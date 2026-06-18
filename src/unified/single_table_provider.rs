use std::any::Any;
use std::fmt;
use std::ops::Bound;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use arrow::array::{ArrayRef, Float32Array, RecordBatch, StringBuilder, UInt32Array};
use arrow::compute::SortOptions;
use arrow::compute::{concat_batches, take};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use arrow::record_batch::RecordBatchOptions;
use async_trait::async_trait;
use datafusion::catalog::Session;
use datafusion::common::config::ConfigOptions;
use datafusion::common::ScalarValue;
use datafusion::common::{Result, Statistics};
use datafusion::datasource::{TableProvider, TableType};
use datafusion::error::DataFusionError;
use datafusion::logical_expr::{Expr, Operator, TableProviderFilterPushDown};
use datafusion::physical_plan::ExecutionPlan;
use datafusion_datasource::source::{DataSource, DataSourceExec};
use datafusion_physical_expr::expressions::Column as PhysicalColumn;
use datafusion_physical_expr::{
    EquivalenceProperties, LexOrdering, PhysicalExpr, PhysicalSortExpr,
};
use datafusion_physical_plan::filter_pushdown::{FilterPushdownPropagation, PushedDown};
use datafusion_physical_plan::metrics::{BaselineMetrics, ExecutionPlanMetricsSet};
use datafusion_physical_plan::projection::ProjectionExprs;
use datafusion_physical_plan::stream::RecordBatchStreamAdapter;
use datafusion_physical_plan::{DisplayFormatType, Partitioning, SendableRecordBatchStream};
use futures::stream::StreamExt;
use tantivy::collector::TopDocs;
use tantivy::query::RangeQuery;
use tantivy::schema::{FieldType, IndexRecordOption, Schema as TantivySchema, Term};
use tantivy::{DateTime, DocAddress, Document, Index};
use tracing::debug;

use crate::fast_field_reader::{read_segment_fast_fields_to_batch, DictCache};
use crate::full_text_udf::{extract_full_text_filter, extract_full_text_or_group};
use crate::index_opener::{DirectIndexOpener, IndexOpener, OpenerSplitRuntimeFactory};
use crate::schema_mapping::tantivy_schema_to_arrow_with_multi_valued;
use crate::split_runtime::{
    PreparedSplit, SplitDescriptor, SplitRuntimeFactoryExt, SplitRuntimeFactoryRef,
};
use crate::type_coercion::{
    apply_fast_field_projection, infer_canonical_fast_field_schema, plan_fast_field_projection,
    FastFieldProjectionPlan,
};
use crate::util::{build_combined_query, for_each_matching_doc_chunks, MatchingDocChunksConfig};

/// Guard that calls `BaselineMetrics::done()` on drop so elapsed time is
/// recorded even when the stream is cancelled.
struct MetricsGuard(BaselineMetrics);
impl Drop for MetricsGuard {
    fn drop(&mut self) {
        self.0.done();
    }
}

/// Guard that aborts a spawned task when dropped.
struct AbortOnDrop {
    handle: tokio::task::JoinHandle<()>,
    cancelled: Arc<AtomicBool>,
}

impl Drop for AbortOnDrop {
    fn drop(&mut self) {
        self.cancelled.store(true, Ordering::Relaxed);
        self.handle.abort();
    }
}

// ---------------------------------------------------------------------------
// Optional per-partition statistics for partition pruning
// ---------------------------------------------------------------------------

/// Per-partition statistics derived from planner-provided field metadata.
///
/// Used by `partition_statistics()` to report column min/max values so that
/// DataFusion's partition pruning can skip partitions whose value ranges do not
/// overlap the query's WHERE clause.
#[derive(Debug, Clone)]
struct PartitionStat {
    num_rows: usize,
    /// Whether this segment has deleted documents. When true, `num_rows` is
    /// an estimate (alive count) rather than an exact value.
    has_deletes: bool,
    /// Column name -> (min, max) as `ScalarValue`.
    column_stats: Vec<(String, Option<ScalarValue>, Option<ScalarValue>)>,
}

#[derive(Debug, Clone)]
struct PlannedSplit {
    descriptor: SplitDescriptor,
    fast_field_schema: SchemaRef,
    partition_stat: Option<PartitionStat>,
    needs_warmup: bool,
}

#[derive(Debug, Clone)]
pub(crate) struct SplitExecutionPlan {
    pub(crate) descriptor: SplitDescriptor,
    pub(crate) needs_warmup: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct PartitionSpec {
    pub(crate) split_idx: usize,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum FilterPushdownSupport {
    Query,
    MissingField,
    Unsupported,
}

fn fast_field_schema_for_opener(opener: &Arc<dyn IndexOpener>) -> SchemaRef {
    tantivy_schema_to_arrow_with_multi_valued(&opener.schema(), &opener.multi_valued_fields())
}

fn split_descriptor_from_opener(
    opener: &Arc<dyn IndexOpener>,
    fallback_id: impl Into<String>,
) -> SplitDescriptor {
    SplitDescriptor::new_with_fast_field_schema(
        fallback_id.into(),
        Vec::new(),
        opener.schema(),
        opener.multi_valued_fields(),
        fast_field_schema_for_opener(opener),
    )
}

fn build_planned_split(
    opener: Arc<dyn IndexOpener>,
    fallback_id: impl Into<String>,
) -> PlannedSplit {
    let fast_field_schema = fast_field_schema_for_opener(&opener);
    PlannedSplit {
        descriptor: split_descriptor_from_opener(&opener, fallback_id),
        fast_field_schema,
        partition_stat: None,
        needs_warmup: opener.needs_warmup(),
    }
}

fn build_unified_schema(fast_field_schema: &SchemaRef) -> (SchemaRef, usize, usize) {
    let mut unified_fields: Vec<Arc<Field>> = fast_field_schema.fields().to_vec();
    let score_column_idx = unified_fields.len();
    unified_fields.push(Arc::new(Field::new("_score", DataType::Float32, true)));
    let document_column_idx = unified_fields.len();
    unified_fields.push(Arc::new(Field::new("_document", DataType::Utf8, true)));
    (
        Arc::new(Schema::new(unified_fields)),
        score_column_idx,
        document_column_idx,
    )
}

fn normalize_canonical_fast_field_schema(schema: &SchemaRef) -> SchemaRef {
    let mut fields = Vec::new();

    if schema
        .fields()
        .iter()
        .all(|field| field.name() != "_doc_id")
    {
        fields.push(Field::new("_doc_id", DataType::UInt32, false));
    }
    if schema
        .fields()
        .iter()
        .all(|field| field.name() != "_segment_ord")
    {
        fields.push(Field::new("_segment_ord", DataType::UInt32, false));
    }

    fields.extend(schema.fields().iter().map(|field| field.as_ref().clone()));
    Arc::new(Schema::new(fields))
}

fn analyze_fast_field_filter_support(
    expr: &Expr,
    tantivy_schema: &TantivySchema,
) -> FilterPushdownSupport {
    let Expr::BinaryExpr(binary) = expr else {
        return FilterPushdownSupport::Unsupported;
    };

    match binary.op {
        Operator::And => {
            return combine_fast_field_filter_support_for_and(
                analyze_fast_field_filter_support(binary.left.as_ref(), tantivy_schema),
                analyze_fast_field_filter_support(binary.right.as_ref(), tantivy_schema),
            );
        }
        Operator::Or => {
            return combine_fast_field_filter_support_for_or(
                analyze_fast_field_filter_support(binary.left.as_ref(), tantivy_schema),
                analyze_fast_field_filter_support(binary.right.as_ref(), tantivy_schema),
            );
        }
        _ => {}
    }

    let column_name = match (binary.left.as_ref(), binary.right.as_ref()) {
        (Expr::Column(col), Expr::Literal(_, _)) => Some(col.name.as_str()),
        (Expr::Literal(_, _), Expr::Column(col)) => Some(col.name.as_str()),
        _ => None,
    };
    let Some(column_name) = column_name else {
        return FilterPushdownSupport::Unsupported;
    };

    if tantivy_schema.get_field(column_name).is_err() {
        return FilterPushdownSupport::MissingField;
    }

    if logical_expr_to_tantivy_query(expr, tantivy_schema).is_some() {
        FilterPushdownSupport::Query
    } else {
        FilterPushdownSupport::Unsupported
    }
}

fn combine_fast_field_filter_support_for_and(
    left: FilterPushdownSupport,
    right: FilterPushdownSupport,
) -> FilterPushdownSupport {
    match (left, right) {
        (FilterPushdownSupport::Unsupported, _) | (_, FilterPushdownSupport::Unsupported) => {
            FilterPushdownSupport::Unsupported
        }
        (FilterPushdownSupport::MissingField, _) | (_, FilterPushdownSupport::MissingField) => {
            FilterPushdownSupport::MissingField
        }
        (FilterPushdownSupport::Query, FilterPushdownSupport::Query) => {
            FilterPushdownSupport::Query
        }
    }
}

fn combine_fast_field_filter_support_for_or(
    left: FilterPushdownSupport,
    right: FilterPushdownSupport,
) -> FilterPushdownSupport {
    match (left, right) {
        (FilterPushdownSupport::Unsupported, _) | (_, FilterPushdownSupport::Unsupported) => {
            FilterPushdownSupport::Unsupported
        }
        (FilterPushdownSupport::Query, _) | (_, FilterPushdownSupport::Query) => {
            FilterPushdownSupport::Query
        }
        (FilterPushdownSupport::MissingField, FilterPushdownSupport::MissingField) => {
            FilterPushdownSupport::MissingField
        }
    }
}

fn filter_is_pushdown_safe(expr: &Expr, splits: &[PlannedSplit]) -> bool {
    let mut any_query = false;

    for split in splits {
        match analyze_fast_field_filter_support(expr, &split.descriptor.tantivy_schema) {
            FilterPushdownSupport::Query => any_query = true,
            FilterPushdownSupport::MissingField => {}
            FilterPushdownSupport::Unsupported => return false,
        }
    }

    any_query
}

pub(crate) fn build_split_fast_field_query(
    exprs: &[Expr],
    tantivy_schema: &TantivySchema,
) -> Option<Arc<dyn tantivy::query::Query>> {
    let mut queries: Vec<Box<dyn tantivy::query::Query>> = Vec::new();

    for expr in exprs {
        match analyze_fast_field_filter_support(expr, tantivy_schema) {
            FilterPushdownSupport::Query => {
                if let Some(query) = logical_expr_to_tantivy_query(expr, tantivy_schema) {
                    queries.push(query);
                }
            }
            FilterPushdownSupport::MissingField => {
                return Some(Arc::new(tantivy::query::EmptyQuery));
            }
            FilterPushdownSupport::Unsupported => return None,
        }
    }

    match queries.len() {
        0 => None,
        1 => queries.into_iter().next().map(Arc::from),
        _ => Some(Arc::new(tantivy::query::BooleanQuery::intersection(
            queries,
        ))),
    }
}

fn translate_partition_stat(
    stat: Option<&PartitionStat>,
    projection: &FastFieldProjectionPlan,
) -> Option<PartitionStat> {
    let stat = stat?;
    let mut column_stats = Vec::new();

    for column in &projection.columns {
        let [source] = column.sources.as_slice() else {
            continue;
        };
        if !matches!(
            source.coercion,
            crate::type_coercion::FastFieldCoercion::Exact
        ) {
            continue;
        }
        if let Some((_, min_val, max_val)) = stat
            .column_stats
            .iter()
            .find(|(name, _, _)| name == &source.source_name)
        {
            column_stats.push((
                column.output_field.name().to_string(),
                min_val.clone(),
                max_val.clone(),
            ));
        }
    }

    Some(PartitionStat {
        num_rows: stat.num_rows,
        has_deletes: stat.has_deletes,
        column_stats,
    })
}

/// A single-table DataFusion provider for tantivy indexes.
///
/// Unlike earlier decomposed providers that joined separate data sources with
/// `HashJoinExec`, this provider handles FTS queries, fast field reading,
/// scoring, and document retrieval in a single pass per split partition.
///
/// The schema is identical: `[_doc_id, _segment_ord, fast_field_1, ..., fast_field_n, _score, _document]`
///
/// Optimizations:
/// - Skips scoring when `_score` is not projected and no FTS query is used
/// - Skips document store reads when `_document` is not projected
/// - Returns `_score` as null when no FTS query is active
///
/// # Example
/// ```sql
/// SELECT id, price, _score, _document
/// FROM my_index
/// WHERE full_text(category, 'books') AND price > 2
/// ORDER BY _score DESC LIMIT 10
/// ```
pub struct SingleTableProvider {
    splits: Vec<PlannedSplit>,
    unified_schema: SchemaRef,
    fast_field_schema: SchemaRef,
    score_column_idx: usize,
    document_column_idx: usize,
    local_runtime_factory: Option<SplitRuntimeFactoryRef>,
}

impl SingleTableProvider {
    /// Create a provider from an already-opened tantivy index.
    #[must_use]
    pub fn new(index: Index) -> Self {
        Self::from_local_splits(vec![index])
            .expect("self-derived canonical schema should always be executable")
    }

    /// Create a provider spanning multiple already-opened local indexes.
    ///
    /// The canonical fast field schema is inferred by strict union on field
    /// names, with one promotion rule: if any split exposes a field as
    /// `List<T>` and another exposes the same field as scalar `T`, the
    /// canonical schema uses `List<T>`.
    pub fn from_local_splits(local_indexes: Vec<Index>) -> Result<Self> {
        let split_openers: Vec<Arc<dyn IndexOpener>> = local_indexes
            .into_iter()
            .map(|index| Arc::new(DirectIndexOpener::new(index)) as Arc<dyn IndexOpener>)
            .collect();
        Self::from_local_split_openers(split_openers)
    }

    /// Create a provider spanning multiple already-opened local indexes with
    /// an explicit canonical fast field schema.
    pub fn from_local_splits_with_fast_field_schema(
        local_indexes: Vec<Index>,
        fast_field_schema: SchemaRef,
    ) -> Result<Self> {
        let split_openers: Vec<Arc<dyn IndexOpener>> = local_indexes
            .into_iter()
            .map(|index| Arc::new(DirectIndexOpener::new(index)) as Arc<dyn IndexOpener>)
            .collect();
        Self::from_local_split_openers_with_fast_field_schema(split_openers, fast_field_schema)
    }

    fn from_local_split_openers(split_openers: Vec<Arc<dyn IndexOpener>>) -> Result<Self> {
        if split_openers.is_empty() {
            return Err(DataFusionError::Plan(
                "SingleTableProvider requires at least one local split".into(),
            ));
        }

        let split_schemas: Vec<SchemaRef> = split_openers
            .iter()
            .map(fast_field_schema_for_opener)
            .collect();
        let canonical_ff_schema = infer_canonical_fast_field_schema(&split_schemas)?;
        Self::from_local_split_openers_with_fast_field_schema(split_openers, canonical_ff_schema)
    }

    fn from_local_split_openers_with_fast_field_schema(
        split_openers: Vec<Arc<dyn IndexOpener>>,
        fast_field_schema: SchemaRef,
    ) -> Result<Self> {
        if split_openers.is_empty() {
            return Err(DataFusionError::Plan(
                "SingleTableProvider requires at least one local split".into(),
            ));
        }

        let fast_field_schema = normalize_canonical_fast_field_schema(&fast_field_schema);
        let opener_map: std::collections::HashMap<String, Arc<dyn IndexOpener>> = split_openers
            .iter()
            .enumerate()
            .map(|(idx, opener)| (format!("local-split-{idx}"), Arc::clone(opener)))
            .collect();

        let splits: Vec<PlannedSplit> = split_openers
            .into_iter()
            .enumerate()
            .map(|(idx, opener)| build_planned_split(opener, format!("local-split-{idx}")))
            .collect();

        for split in &splits {
            plan_fast_field_projection(&split.fast_field_schema, &fast_field_schema)?;
        }

        Ok(Self::from_planned_splits(
            splits,
            fast_field_schema,
            Some(Arc::new(OpenerSplitRuntimeFactory::new(opener_map))),
        ))
    }

    pub fn from_split_descriptors(split_descriptors: Vec<SplitDescriptor>) -> Result<Self> {
        if split_descriptors.is_empty() {
            return Err(DataFusionError::Plan(
                "SingleTableProvider requires at least one split descriptor".into(),
            ));
        }

        let split_schemas: Vec<SchemaRef> = split_descriptors
            .iter()
            .map(SplitDescriptor::fast_field_schema)
            .collect();
        let canonical_ff_schema = infer_canonical_fast_field_schema(&split_schemas)?;
        Self::from_split_descriptors_with_fast_field_schema(split_descriptors, canonical_ff_schema)
    }

    pub fn from_split_descriptors_with_fast_field_schema(
        split_descriptors: Vec<SplitDescriptor>,
        fast_field_schema: SchemaRef,
    ) -> Result<Self> {
        if split_descriptors.is_empty() {
            return Err(DataFusionError::Plan(
                "SingleTableProvider requires at least one split descriptor".into(),
            ));
        }

        let fast_field_schema = normalize_canonical_fast_field_schema(&fast_field_schema);
        let splits: Vec<PlannedSplit> = split_descriptors
            .into_iter()
            .map(|descriptor| PlannedSplit {
                fast_field_schema: descriptor.fast_field_schema(),
                descriptor,
                partition_stat: None,
                needs_warmup: true,
            })
            .collect();

        Ok(Self::from_planned_splits(splits, fast_field_schema, None))
    }

    fn from_planned_splits(
        splits: Vec<PlannedSplit>,
        fast_field_schema: SchemaRef,
        local_runtime_factory: Option<SplitRuntimeFactoryRef>,
    ) -> Self {
        let (unified_schema, score_column_idx, document_column_idx) =
            build_unified_schema(&fast_field_schema);

        Self {
            splits,
            unified_schema,
            fast_field_schema,
            score_column_idx,
            document_column_idx,
            local_runtime_factory,
        }
    }

    fn plan_scan_schema(&self, projection: Option<&Vec<usize>>) -> Result<ScanSchema> {
        let projected_indices: Vec<usize> = match projection {
            Some(indices) => indices.clone(),
            None => (0..self.unified_schema.fields().len()).collect(),
        };

        let mut needs_score = false;
        let mut needs_document = false;
        let mut ff_indices = Vec::new();

        for &idx in &projected_indices {
            if idx == self.score_column_idx {
                needs_score = true;
            } else if idx == self.document_column_idx {
                needs_document = true;
            } else {
                ff_indices.push(idx);
            }
        }

        ff_indices.sort_unstable();
        ff_indices.dedup();
        add_required_fast_field_indices(&mut ff_indices, &self.fast_field_schema, needs_document)?;

        let ff_projected = {
            let fields: Vec<Field> = ff_indices
                .iter()
                .map(|&i| self.fast_field_schema.field(i).clone())
                .collect();
            Arc::new(Schema::new(fields))
        };
        let projected = {
            let fields: Vec<Field> = projected_indices
                .iter()
                .map(|&i| self.unified_schema.field(i).clone())
                .collect();
            Arc::new(Schema::new(fields))
        };

        Ok(ScanSchema {
            unified: self.unified_schema.clone(),
            projected,
            ff_projected,
            projection: projection.cloned(),
            score_idx: self.score_column_idx,
            document_idx: self.document_column_idx,
            needs_score,
            needs_document,
        })
    }
}

fn add_required_fast_field_indices(
    ff_indices: &mut Vec<usize>,
    fast_field_schema: &SchemaRef,
    needs_document: bool,
) -> Result<()> {
    let doc_id_idx = fast_field_schema.index_of("_doc_id").map_err(|_| {
        DataFusionError::Internal("fast field schema missing required _doc_id column".into())
    })?;
    let segment_ord_idx = fast_field_schema.index_of("_segment_ord").map_err(|_| {
        DataFusionError::Internal("fast field schema missing required _segment_ord column".into())
    })?;
    if ff_indices.is_empty() || (needs_document && !ff_indices.contains(&doc_id_idx)) {
        ff_indices.push(doc_id_idx);
    }
    if needs_document && !ff_indices.contains(&segment_ord_idx) {
        ff_indices.push(segment_ord_idx);
    }
    ff_indices.sort_unstable();
    ff_indices.dedup();
    Ok(())
}

struct PushedDownFilters {
    raw_queries: Vec<(String, String)>,
    raw_not_queries: Vec<(String, String)>,
    raw_query_groups: Vec<Vec<(String, String)>>,
    fast_field_filter_exprs: Vec<Expr>,
}

fn collect_pushed_down_filters(filters: &[Expr], splits: &[PlannedSplit]) -> PushedDownFilters {
    let mut pushed = PushedDownFilters {
        raw_queries: Vec::new(),
        raw_not_queries: Vec::new(),
        raw_query_groups: Vec::new(),
        fast_field_filter_exprs: Vec::new(),
    };

    for filter in filters {
        if let Some(full_text_filter) = extract_full_text_filter(filter) {
            let query = (full_text_filter.field_name, full_text_filter.query_string);
            if full_text_filter.negated {
                pushed.raw_not_queries.push(query);
            } else {
                pushed.raw_queries.push(query);
            }
        } else if let Some(group) = extract_full_text_or_group(filter) {
            pushed
                .raw_query_groups
                .push(existing_full_text_group(group, splits));
        } else if filter_is_pushdown_safe(filter, splits) {
            pushed.fast_field_filter_exprs.push(filter.clone());
        }
    }

    pushed
}

fn existing_full_text_group(
    group: Vec<(String, String)>,
    splits: &[PlannedSplit],
) -> Vec<(String, String)> {
    group
        .into_iter()
        .filter(|(field_name, _)| {
            splits.iter().any(|split| {
                split
                    .descriptor
                    .tantivy_schema
                    .get_field(field_name)
                    .is_ok()
            })
        })
        .collect()
}

fn cached_pre_built_query(
    splits: &[PlannedSplit],
    fast_field_filter_exprs: &[Expr],
) -> Option<Arc<dyn tantivy::query::Query>> {
    if splits.len() == 1 {
        build_split_fast_field_query(
            fast_field_filter_exprs,
            &splits[0].descriptor.tantivy_schema,
        )
    } else {
        None
    }
}

fn split_execution_plans(splits: &[PlannedSplit]) -> Vec<SplitExecutionPlan> {
    splits
        .iter()
        .map(|split| SplitExecutionPlan {
            descriptor: split.descriptor.clone(),
            needs_warmup: split.needs_warmup,
        })
        .collect()
}

fn scan_partition_stats(
    splits: &[PlannedSplit],
    ff_projected_schema: &SchemaRef,
) -> Vec<Option<PartitionStat>> {
    splits
        .iter()
        .map(|split| {
            let projection =
                plan_fast_field_projection(&split.fast_field_schema, ff_projected_schema).ok()?;
            translate_partition_stat(split.partition_stat.as_ref(), &projection)
        })
        .collect()
}

impl fmt::Debug for SingleTableProvider {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SingleTableProvider")
            .field("splits", &self.splits.len())
            .field("unified_schema", &self.unified_schema)
            .field("fast_field_schema", &self.fast_field_schema)
            .field("score_column_idx", &self.score_column_idx)
            .field("document_column_idx", &self.document_column_idx)
            .finish_non_exhaustive()
    }
}

#[async_trait]
impl TableProvider for SingleTableProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.unified_schema.clone()
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    fn supports_filters_pushdown(
        &self,
        filters: &[&Expr],
    ) -> Result<Vec<TableProviderFilterPushDown>> {
        Ok(filters
            .iter()
            .map(|f| {
                if extract_full_text_filter(f).is_some() || extract_full_text_or_group(f).is_some()
                {
                    TableProviderFilterPushDown::Exact
                } else if filter_is_pushdown_safe(f, &self.splits) {
                    TableProviderFilterPushDown::Inexact
                } else {
                    TableProviderFilterPushDown::Unsupported
                }
            })
            .collect())
    }

    async fn scan(
        &self,
        _state: &dyn Session,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let pushed = collect_pushed_down_filters(filters, &self.splits);
        let cached_pre_built_query =
            cached_pre_built_query(&self.splits, &pushed.fast_field_filter_exprs);
        let schema = self.plan_scan_schema(projection)?;
        let partition_map: Vec<PartitionSpec> = (0..self.splits.len())
            .map(|split_idx| PartitionSpec { split_idx })
            .collect();
        let partition_stats = scan_partition_stats(&self.splits, &schema.ff_projected);

        let data_source = SingleTableDataSource {
            splits: split_execution_plans(&self.splits),
            schema,
            raw_queries: pushed.raw_queries,
            raw_not_queries: pushed.raw_not_queries,
            raw_query_groups: pushed.raw_query_groups,
            pre_built_query: cached_pre_built_query,
            fast_field_filter_exprs: pushed.fast_field_filter_exprs,
            topk: None,
            row_limit: limit,
            partition_map,
            partition_stats,
            local_runtime_factory: self.local_runtime_factory.clone(),
            warmup_done: self
                .splits
                .iter()
                .map(|_| Arc::new(tokio::sync::OnceCell::new()))
                .collect(),
            metrics: ExecutionPlanMetricsSet::new(),
        };

        Ok(Arc::new(DataSourceExec::new(Arc::new(data_source))))
    }
}

// ---------------------------------------------------------------------------
// DataSource implementation
// ---------------------------------------------------------------------------

/// Bundles the eight schema-related fields that travel together.
#[derive(Debug, Clone)]
pub(crate) struct ScanSchema {
    pub(crate) unified: SchemaRef,
    pub(crate) projected: SchemaRef,
    pub(crate) ff_projected: SchemaRef,
    pub(crate) projection: Option<Vec<usize>>,
    pub(crate) score_idx: usize,
    pub(crate) document_idx: usize,
    pub(crate) needs_score: bool,
    pub(crate) needs_document: bool,
}

pub(crate) struct SingleTableCodecFields {
    pub(crate) splits: Vec<SplitExecutionPlan>,
    pub(crate) schema: ScanSchema,
    pub(crate) raw_queries: Vec<(String, String)>,
    pub(crate) raw_not_queries: Vec<(String, String)>,
    pub(crate) raw_query_groups: Vec<Vec<(String, String)>>,
    pub(crate) fast_field_filter_exprs: Vec<Expr>,
    pub(crate) topk: Option<usize>,
    pub(crate) row_limit: Option<usize>,
    pub(crate) partition_map: Vec<PartitionSpec>,
}

pub struct SingleTableDataSource {
    splits: Vec<SplitExecutionPlan>,
    schema: ScanSchema,
    raw_queries: Vec<(String, String)>,
    raw_not_queries: Vec<(String, String)>,
    raw_query_groups: Vec<Vec<(String, String)>>,
    /// Cached fast field query for the single-split case.
    pre_built_query: Option<Arc<dyn tantivy::query::Query>>,
    /// Source logical `Expr`s that were successfully converted to tantivy
    /// queries. Stored for serialization and for split-specific query
    /// reconstruction at execution time.
    fast_field_filter_exprs: Vec<Expr>,
    pub(crate) topk: Option<usize>,
    row_limit: Option<usize>,
    partition_map: Vec<PartitionSpec>,
    /// Per-partition (split) statistics for partition pruning.
    /// Indexed by partition number. `None` means stats are unavailable for
    /// that partition (e.g. remote opener without metadata).
    partition_stats: Vec<Option<PartitionStat>>,
    local_runtime_factory: Option<SplitRuntimeFactoryRef>,
    /// Ensures warmup runs at most once per split.
    warmup_done: Vec<Arc<tokio::sync::OnceCell<()>>>,
    /// Shared metrics set for all partitions.
    metrics: ExecutionPlanMetricsSet,
}

type BatchSender = tokio::sync::mpsc::Sender<Result<RecordBatch>>;

struct SingleTableOpenInput {
    context: Arc<datafusion::execution::TaskContext>,
    sync_pool: crate::sync_exec::SyncExecutionPoolRef,
    split: SplitExecutionPlan,
    local_runtime_factory: Option<SplitRuntimeFactoryRef>,
    warmup_done: Arc<tokio::sync::OnceCell<()>>,
    needs_warmup: bool,
    batch_size: usize,
    raw_queries: Vec<(String, String)>,
    raw_not_queries: Vec<(String, String)>,
    raw_query_groups: Vec<Vec<(String, String)>>,
    fast_field_filter_exprs: Vec<Expr>,
    pre_built_query: Option<Arc<dyn tantivy::query::Query>>,
    ff_projected_schema: SchemaRef,
    projected_schema: SchemaRef,
    unified_schema: SchemaRef,
    projection: Option<Vec<usize>>,
    needs_score: bool,
    needs_document: bool,
    score_column_idx: usize,
    document_column_idx: usize,
    topk: Option<usize>,
    row_limit: Option<usize>,
    cancelled: Arc<AtomicBool>,
}

struct SingleTableWarmupInput {
    searcher: tantivy::Searcher,
    tantivy_schema: TantivySchema,
    source_ff_schema: SchemaRef,
    raw_queries: Vec<(String, String)>,
    raw_not_queries: Vec<(String, String)>,
    raw_query_groups: Vec<Vec<(String, String)>>,
    fast_field_filter_exprs: Vec<Expr>,
}

struct BlockingScanInput {
    sync_pool: crate::sync_exec::SyncExecutionPoolRef,
    prepared: Arc<PreparedSplit>,
    batch_size: usize,
    source_ff_schema: SchemaRef,
    fast_field_projection: FastFieldProjectionPlan,
    unified_schema: SchemaRef,
    projection: Option<Vec<usize>>,
    score_column_idx: usize,
    document_column_idx: usize,
    needs_score: bool,
    needs_document: bool,
    topk: Option<usize>,
    row_limit: Option<usize>,
    raw_queries: Vec<(String, String)>,
    raw_not_queries: Vec<(String, String)>,
    raw_query_groups: Vec<Vec<(String, String)>>,
    fast_field_filter_exprs: Vec<Expr>,
    pre_built_query: Option<Arc<dyn tantivy::query::Query>>,
    cancelled: Arc<AtomicBool>,
    projected_schema: SchemaRef,
}

impl fmt::Debug for SingleTableDataSource {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SingleTableDataSource")
            .field("splits", &self.splits.len())
            .field("schema", &self.schema.projected)
            .field("topk", &self.topk)
            .field("row_limit", &self.row_limit)
            .field("partitions", &self.partition_map.len())
            .finish_non_exhaustive()
    }
}

impl SingleTableDataSource {
    /// Construct a `SingleTableDataSource` directly from deserialized codec
    /// fields, bypassing `TableProvider::scan` and `SessionContext`. Used by
    /// `TantivyCodec::try_decode` to reconstruct a `DataSourceExec` on workers.
    pub(crate) fn new_from_codec(fields: SingleTableCodecFields) -> Self {
        let warmup_done = fields
            .splits
            .iter()
            .map(|_| Arc::new(tokio::sync::OnceCell::new()))
            .collect();
        Self {
            pre_built_query: if fields.splits.len() == 1 {
                build_split_fast_field_query(
                    &fields.fast_field_filter_exprs,
                    &fields.splits[0].descriptor.tantivy_schema,
                )
            } else {
                None
            },
            partition_stats: vec![None; fields.partition_map.len()],
            splits: fields.splits,
            schema: fields.schema,
            raw_queries: fields.raw_queries,
            raw_not_queries: fields.raw_not_queries,
            raw_query_groups: fields.raw_query_groups,
            fast_field_filter_exprs: fields.fast_field_filter_exprs,
            topk: fields.topk,
            row_limit: fields.row_limit,
            partition_map: fields.partition_map,
            local_runtime_factory: None,
            warmup_done,
            metrics: ExecutionPlanMetricsSet::new(),
        }
    }

    fn clone_with(&self, f: impl FnOnce(&mut Self)) -> Self {
        let mut new = SingleTableDataSource {
            splits: self.splits.clone(),
            schema: self.schema.clone(),
            raw_queries: self.raw_queries.clone(),
            raw_not_queries: self.raw_not_queries.clone(),
            raw_query_groups: self.raw_query_groups.clone(),
            pre_built_query: self.pre_built_query.clone(),
            fast_field_filter_exprs: self.fast_field_filter_exprs.clone(),
            topk: self.topk,
            row_limit: self.row_limit,
            partition_map: self.partition_map.clone(),
            partition_stats: self.partition_stats.clone(),
            local_runtime_factory: self.local_runtime_factory.clone(),
            warmup_done: self.warmup_done.clone(),
            metrics: self.metrics.clone(),
        };
        f(&mut new);
        new
    }

    fn open_input(
        &self,
        partition: usize,
        context: Arc<datafusion::execution::TaskContext>,
        cancelled: Arc<AtomicBool>,
    ) -> Result<SingleTableOpenInput> {
        let partition_spec = *self.partition_map.get(partition).ok_or_else(|| {
            DataFusionError::Internal(format!("invalid partition index {partition}"))
        })?;
        let split = self
            .splits
            .get(partition_spec.split_idx)
            .ok_or_else(|| DataFusionError::Internal("invalid split index".to_string()))?
            .clone();
        let warmup_done = Arc::clone(
            self.warmup_done
                .get(partition_spec.split_idx)
                .ok_or_else(|| DataFusionError::Internal("invalid warmup split index".into()))?,
        );
        let pre_built_query = if self.splits.len() == 1 {
            self.pre_built_query
                .as_ref()
                .map(|q| Arc::from(q.box_clone()))
        } else {
            None
        };
        let needs_warmup = split_needs_warmup(&split);

        Ok(SingleTableOpenInput {
            sync_pool: crate::sync_exec::get_or_default_pool(context.as_ref()),
            split,
            local_runtime_factory: self.local_runtime_factory.clone(),
            warmup_done,
            needs_warmup,
            batch_size: context.session_config().batch_size(),
            raw_queries: self.raw_queries.clone(),
            raw_not_queries: self.raw_not_queries.clone(),
            raw_query_groups: self.raw_query_groups.clone(),
            fast_field_filter_exprs: self.fast_field_filter_exprs.clone(),
            pre_built_query,
            ff_projected_schema: self.schema.ff_projected.clone(),
            projected_schema: self.schema.projected.clone(),
            unified_schema: self.schema.unified.clone(),
            projection: self.schema.projection.clone(),
            needs_score: self.schema.needs_score,
            needs_document: self.schema.needs_document,
            score_column_idx: self.schema.score_idx,
            document_column_idx: self.schema.document_idx,
            topk: self.topk,
            row_limit: self.row_limit,
            cancelled,
            context,
        })
    }

    /// Access the split descriptors.
    pub fn split_descriptors(&self) -> Vec<SplitDescriptor> {
        self.splits
            .iter()
            .map(|split| split.descriptor.clone())
            .collect()
    }

    pub(crate) fn split_descriptor_refs(&self) -> impl Iterator<Item = &SplitDescriptor> + '_ {
        self.splits.iter().map(|split| &split.descriptor)
    }

    pub fn local_runtime_factory(&self) -> Option<SplitRuntimeFactoryRef> {
        self.local_runtime_factory.clone()
    }

    /// Access the raw full-text queries.
    pub fn raw_queries(&self) -> &[(String, String)] {
        &self.raw_queries
    }

    /// Access the negated raw full-text queries.
    pub fn raw_not_queries(&self) -> &[(String, String)] {
        &self.raw_not_queries
    }

    /// Access OR-groups of raw full-text queries.
    pub fn raw_query_groups(&self) -> &[Vec<(String, String)>] {
        &self.raw_query_groups
    }

    /// The number of partitions this data source is partitioned over.
    pub fn num_partitions(&self) -> usize {
        self.partition_map.len()
    }

    /// Access the topk limit.
    pub fn topk(&self) -> Option<usize> {
        self.topk
    }

    /// Access the per-partition scan row limit derived from planner hints.
    pub fn row_limit(&self) -> Option<usize> {
        self.row_limit
    }

    /// Whether this data source has an active query.
    pub fn has_query(&self) -> bool {
        !self.raw_queries.is_empty()
            || !self.raw_not_queries.is_empty()
            || !self.raw_query_groups.is_empty()
            || self.pre_built_query.is_some()
            || !self.fast_field_filter_exprs.is_empty()
    }

    /// Create a copy with the topk limit set.
    #[must_use]
    pub fn with_topk(&self, topk: usize) -> Self {
        self.clone_with(|s| s.topk = Some(topk))
    }

    /// Access the pre-built tantivy query from fast field filters.
    pub fn pre_built_query(&self) -> Option<&Arc<dyn tantivy::query::Query>> {
        self.pre_built_query.as_ref()
    }

    /// Access the canonical fast field schema for this scan.
    pub fn canonical_fast_field_schema(&self) -> SchemaRef {
        let fields: Vec<Field> = self.schema.unified.fields()[..self.schema.score_idx]
            .iter()
            .map(|field| field.as_ref().clone())
            .collect();
        Arc::new(Schema::new(fields))
    }

    /// Access the source logical `Expr`s that produced `pre_built_query`.
    /// Used by the codec for serialization.
    pub fn fast_field_filter_exprs(&self) -> &[Expr] {
        &self.fast_field_filter_exprs
    }

    /// Aggregate per-partition statistics into overall table statistics.
    ///
    /// - `num_rows` = sum of all partition row counts
    /// - column `min_value` = minimum across all partition minimums
    /// - column `max_value` = maximum across all partition maximums
    fn aggregate_statistics(&self) -> Result<Statistics> {
        use datafusion::common::stats::Precision;
        use datafusion::common::ColumnStatistics;

        // Collect only the partitions that have stats.
        let known: Vec<&PartitionStat> = self
            .partition_stats
            .iter()
            .filter_map(|s| s.as_ref())
            .collect();

        if known.is_empty() {
            return Ok(Statistics::new_unknown(&self.schema.projected));
        }

        let total_rows: usize = known.iter().map(|s| s.num_rows).sum();
        let any_deletes = known.iter().any(|s| s.has_deletes);
        let num_rows = if any_deletes {
            Precision::Inexact(total_rows)
        } else {
            Precision::Exact(total_rows)
        };

        let column_statistics: Vec<ColumnStatistics> = self
            .schema
            .projected
            .fields()
            .iter()
            .map(|field| {
                let name = field.name();
                let mut overall_min: Precision<ScalarValue> = Precision::Absent;
                let mut overall_max: Precision<ScalarValue> = Precision::Absent;

                for stat in &known {
                    if let Some((_, min_val, max_val)) =
                        stat.column_stats.iter().find(|(n, _, _)| n == name)
                    {
                        if let Some(min_v) = min_val {
                            let p = Precision::Inexact(min_v.clone());
                            overall_min = match overall_min {
                                Precision::Absent => p,
                                prev => prev.min(&p),
                            };
                        }
                        if let Some(max_v) = max_val {
                            let p = Precision::Inexact(max_v.clone());
                            overall_max = match overall_max {
                                Precision::Absent => p,
                                prev => prev.max(&p),
                            };
                        }
                    }
                }

                ColumnStatistics {
                    null_count: Precision::Absent,
                    max_value: overall_max,
                    min_value: overall_min,
                    sum_value: Precision::Absent,
                    distinct_count: Precision::Absent,
                    byte_size: Precision::Absent,
                }
            })
            .collect();

        Ok(Statistics {
            num_rows,
            total_byte_size: Precision::Absent,
            column_statistics,
        })
    }

    /// Access the projection indices.
    pub fn projection(&self) -> Option<&[usize]> {
        self.schema.projection.as_deref()
    }

    /// Whether _score is needed.
    pub fn needs_score(&self) -> bool {
        self.schema.needs_score
    }

    /// Whether _document is needed.
    pub fn needs_document(&self) -> bool {
        self.schema.needs_document
    }

    fn output_orderings(&self) -> Vec<LexOrdering> {
        let schema = Arc::clone(&self.schema.projected);

        if self.topk.is_some() && self.schema.needs_score {
            if let Ok(score_idx) = schema.index_of("_score") {
                if let Some(ordering) = LexOrdering::new([PhysicalSortExpr::new(
                    Arc::new(PhysicalColumn::new("_score", score_idx)),
                    SortOptions {
                        descending: true,
                        nulls_first: false,
                    },
                )]) {
                    return vec![ordering];
                }
            }
            return Vec::new();
        }
        Vec::new()
    }
}

async fn prepare_split(
    split: &SplitExecutionPlan,
    local_runtime_factory: Option<&SplitRuntimeFactoryRef>,
    context: &datafusion::execution::TaskContext,
) -> Result<Arc<PreparedSplit>> {
    let factory = context
        .session_config()
        .get_split_runtime_factory()
        .or_else(|| local_runtime_factory.cloned())
        .ok_or_else(|| {
            DataFusionError::Internal(
                "no SplitRuntimeFactory registered on session config; \
                 remote split execution requires config.set_split_runtime_factory(...)"
                    .into(),
            )
        })?;
    factory.prepare_split(&split.descriptor).await
}

async fn resolve_split_fast_field_schema(
    split: &SplitExecutionPlan,
    requested_schema: &SchemaRef,
    prepared: Arc<PreparedSplit>,
    local_runtime_factory: Option<&SplitRuntimeFactoryRef>,
    context: &datafusion::execution::TaskContext,
) -> Result<SchemaRef> {
    let factory = context
        .session_config()
        .get_split_runtime_factory()
        .or_else(|| local_runtime_factory.cloned())
        .ok_or_else(|| {
            DataFusionError::Internal(
                "no SplitRuntimeFactory registered on session config; \
                 remote split execution requires config.set_split_runtime_factory(...)"
                    .into(),
            )
        })?;
    factory
        .resolve_fast_field_schema(&split.descriptor, Arc::clone(requested_schema), prepared)
        .await
}

fn split_needs_warmup(split: &SplitExecutionPlan) -> bool {
    split.needs_warmup
}

fn spawn_single_table_open_task(
    input: SingleTableOpenInput,
    tx: BatchSender,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        if let Err(err) = run_single_table_open_task(input, tx.clone()).await {
            let _ = tx.send(Err(err)).await;
        }
    })
}

async fn run_single_table_open_task(input: SingleTableOpenInput, tx: BatchSender) -> Result<()> {
    let prepared = prepare_split(
        &input.split,
        input.local_runtime_factory.as_ref(),
        input.context.as_ref(),
    )
    .await?;
    let source_ff_schema = resolve_split_fast_field_schema(
        &input.split,
        &input.ff_projected_schema,
        Arc::clone(&prepared),
        input.local_runtime_factory.as_ref(),
        input.context.as_ref(),
    )
    .await?;
    debug!(
        split_id = %input.split.descriptor.split_id,
        source_fast_field_schema = ?source_ff_schema,
        projected_fast_field_schema = ?input.ff_projected_schema,
        "planning split fast-field projection"
    );
    let fast_field_projection =
        plan_fast_field_projection(&source_ff_schema, &input.ff_projected_schema)?;

    if input.needs_warmup {
        warmup_single_table_split(
            Arc::clone(&input.warmup_done),
            SingleTableWarmupInput {
                searcher: prepared.searcher().clone(),
                tantivy_schema: prepared.index().schema(),
                source_ff_schema: source_ff_schema.clone(),
                raw_queries: input.raw_queries.clone(),
                raw_not_queries: input.raw_not_queries.clone(),
                raw_query_groups: input.raw_query_groups.clone(),
                fast_field_filter_exprs: input.fast_field_filter_exprs.clone(),
            },
        )
        .await?;
    }

    let scan_input = BlockingScanInput {
        sync_pool: input.sync_pool,
        prepared,
        batch_size: input.batch_size,
        source_ff_schema,
        fast_field_projection,
        unified_schema: input.unified_schema,
        projection: input.projection,
        score_column_idx: input.score_column_idx,
        document_column_idx: input.document_column_idx,
        needs_score: input.needs_score,
        needs_document: input.needs_document,
        topk: input.topk,
        row_limit: input.row_limit,
        raw_queries: input.raw_queries,
        raw_not_queries: input.raw_not_queries,
        raw_query_groups: input.raw_query_groups,
        fast_field_filter_exprs: input.fast_field_filter_exprs,
        pre_built_query: input.pre_built_query,
        cancelled: input.cancelled,
        projected_schema: input.projected_schema,
    };
    run_blocking_scan_and_forward(scan_input, tx).await
}

async fn warmup_single_table_split(
    warmup_done: Arc<tokio::sync::OnceCell<()>>,
    input: SingleTableWarmupInput,
) -> Result<()> {
    warmup_done
        .get_or_try_init(|| async move {
            let mut ff_names: std::collections::BTreeSet<String> = input
                .source_ff_schema
                .fields()
                .iter()
                .filter_map(|field| {
                    let name = field.name();
                    if name == "_doc_id" || name == "_segment_ord" {
                        None
                    } else {
                        Some(crate::fast_field_read_name(field).to_string())
                    }
                })
                .collect();
            ff_names.extend(crate::warmup::fast_field_filter_field_names(
                &input.tantivy_schema,
                &input.fast_field_filter_exprs,
            )?);

            if !ff_names.is_empty() {
                let ff_names: Vec<String> = ff_names.into_iter().collect();
                let ff_name_refs: Vec<&str> = ff_names.iter().map(String::as_str).collect();
                crate::warmup::warmup_fast_fields_by_name(&input.searcher, &ff_name_refs).await?;
            }

            let queried_fields: Vec<tantivy::schema::Field> = input
                .raw_queries
                .iter()
                .chain(input.raw_not_queries.iter())
                .chain(input.raw_query_groups.iter().flatten())
                .filter_map(|(field_name, _)| input.tantivy_schema.get_field(field_name).ok())
                .collect();
            if !queried_fields.is_empty() {
                crate::warmup::warmup_inverted_index(&input.searcher, &queried_fields).await?;
            }

            Ok::<(), DataFusionError>(())
        })
        .await
        .map(|_| ())
}

async fn run_blocking_scan_and_forward(input: BlockingScanInput, tx: BatchSender) -> Result<()> {
    let (raw_tx, mut raw_rx) = tokio::sync::mpsc::channel::<Result<RecordBatch>>(2);
    let BlockingScanInput {
        sync_pool,
        prepared,
        batch_size,
        source_ff_schema,
        fast_field_projection,
        unified_schema,
        projection,
        score_column_idx,
        document_column_idx,
        needs_score,
        needs_document,
        topk,
        row_limit,
        raw_queries,
        raw_not_queries,
        raw_query_groups,
        fast_field_filter_exprs,
        pre_built_query,
        cancelled,
        projected_schema,
    } = input;
    let prepared_for_docs = Arc::clone(&prepared);
    let output_schema_for_docs = projected_schema;
    let tantivy_schema = prepared_for_docs.index().schema();

    let blocking_handle = tokio::spawn(async move {
        sync_pool
            .run_boxed(Box::new(move || {
                let split_fast_field_query = match pre_built_query {
                    Some(query) => Some(query),
                    None => build_split_fast_field_query(
                        &fast_field_filter_exprs,
                        &prepared.index().schema(),
                    ),
                };
                let query = build_combined_query(
                    prepared.index(),
                    split_fast_field_query.as_ref(),
                    &raw_queries,
                    &raw_not_queries,
                    &raw_query_groups,
                )?;
                let cfg = ScanConfig {
                    prepared,
                    batch_size,
                    source_ff_schema,
                    fast_field_projection,
                    unified_schema,
                    projection,
                    score_column_idx,
                    document_column_idx,
                    needs_score,
                    needs_document,
                    topk,
                    row_limit,
                    query,
                    cancelled,
                };
                generate_single_table_batch_streaming(&cfg, |batch| {
                    raw_tx.blocking_send(Ok(batch)).is_ok()
                })?;
                Ok(Box::new(()) as Box<dyn std::any::Any + Send>)
            }))
            .await
    });

    while let Some(result) = raw_rx.recv().await {
        let to_send = match result {
            Ok(batch) if needs_document => {
                fill_document_column_async(
                    batch,
                    &prepared_for_docs,
                    &output_schema_for_docs,
                    &tantivy_schema,
                )
                .await
            }
            other => other,
        };
        if tx.send(to_send).await.is_err() {
            break;
        }
    }

    match blocking_handle.await {
        Ok(Ok(_)) => Ok(()),
        Ok(Err(e)) => Err(e),
        Err(e) => Err(DataFusionError::Internal(format!(
            "sync pool task join: {e}"
        ))),
    }
}

impl DataSource for SingleTableDataSource {
    fn open(
        &self,
        partition: usize,
        context: Arc<datafusion::execution::TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        let metrics_guard = MetricsGuard(BaselineMetrics::new(&self.metrics, partition));
        let schema = self.schema.projected.clone();
        let cancelled = Arc::new(AtomicBool::new(false));
        let (tx, rx) = tokio::sync::mpsc::channel::<Result<RecordBatch>>(2);
        let input = self.open_input(partition, context, Arc::clone(&cancelled))?;
        let handle = spawn_single_table_open_task(input, tx);
        let guard = AbortOnDrop { handle, cancelled };

        let stream = futures::stream::unfold((rx, guard), |(mut rx, guard)| async move {
            rx.recv().await.map(|batch| (batch, (rx, guard)))
        });

        let tracked = stream.map(move |result| {
            if let Ok(ref batch) = result {
                metrics_guard.0.record_output(batch.num_rows());
            }
            result
        });

        Ok(Box::pin(RecordBatchStreamAdapter::new(schema, tracked)))
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "SingleTableDataSource(partitions={}, query={}, score={}, document={}, topk={:?})",
            self.partition_map.len(),
            self.has_query(),
            self.schema.needs_score,
            self.schema.needs_document,
            self.topk,
        )
    }

    fn output_partitioning(&self) -> Partitioning {
        Partitioning::UnknownPartitioning(self.partition_map.len())
    }

    fn eq_properties(&self) -> EquivalenceProperties {
        let orderings = self.output_orderings();
        if orderings.is_empty() {
            EquivalenceProperties::new(Arc::clone(&self.schema.projected))
        } else {
            EquivalenceProperties::new_with_orderings(Arc::clone(&self.schema.projected), orderings)
        }
    }

    fn partition_statistics(&self, partition: Option<usize>) -> Result<Statistics> {
        use datafusion::common::stats::Precision;
        use datafusion::common::ColumnStatistics;

        let partition = match partition {
            Some(p) => p,
            None => {
                // Aggregate across all partitions.
                return self.aggregate_statistics();
            }
        };

        let stat = match self.partition_stats.get(partition) {
            Some(Some(s)) => s,
            _ => return Ok(Statistics::new_unknown(&self.schema.projected)),
        };

        let num_rows = if stat.has_deletes {
            Precision::Inexact(stat.num_rows)
        } else {
            Precision::Exact(stat.num_rows)
        };

        let column_statistics: Vec<ColumnStatistics> = self
            .schema
            .projected
            .fields()
            .iter()
            .map(|field| {
                let name = field.name();
                if let Some((_, min_val, max_val)) =
                    stat.column_stats.iter().find(|(n, _, _)| n == name)
                {
                    ColumnStatistics {
                        null_count: Precision::Absent,
                        // Inexact because deleted docs may have held the
                        // actual min/max — the surviving range could be
                        // narrower than what columnar metadata reports.
                        max_value: max_val
                            .clone()
                            .map_or(Precision::Absent, Precision::Inexact),
                        min_value: min_val
                            .clone()
                            .map_or(Precision::Absent, Precision::Inexact),
                        sum_value: Precision::Absent,
                        distinct_count: Precision::Absent,
                        byte_size: Precision::Absent,
                    }
                } else {
                    ColumnStatistics::new_unknown()
                }
            })
            .collect();

        Ok(Statistics {
            num_rows,
            total_byte_size: Precision::Absent,
            column_statistics,
        })
    }

    fn with_fetch(&self, fetch: Option<usize>) -> Option<Arc<dyn DataSource>> {
        let topk = fetch?;
        if !self.schema.needs_score {
            return None; // Only for scored queries (Block-WAND)
        }
        Some(Arc::new(self.clone_with(|ds| {
            ds.topk = Some(topk);
        })))
    }

    fn fetch(&self) -> Option<usize> {
        // Only report fetch guarantee when single partition — per-segment TopK
        // cannot guarantee a global row limit across multiple partitions.
        if self.partition_map.len() <= 1 {
            self.topk
        } else {
            None
        }
    }

    fn metrics(&self) -> ExecutionPlanMetricsSet {
        self.metrics.clone()
    }

    fn try_swapping_with_projection(
        &self,
        _projection: &ProjectionExprs,
    ) -> Result<Option<Arc<dyn DataSource>>> {
        Ok(None)
    }

    fn try_pushdown_filters(
        &self,
        filters: Vec<Arc<dyn PhysicalExpr>>,
        _config: &ConfigOptions,
    ) -> Result<FilterPushdownPropagation<Arc<dyn DataSource>>> {
        // Don't claim to handle physical filters — let DataFusion keep
        // its FilterExec as a safety net. Tantivy-convertible predicates
        // are already pushed at the logical level via supports_filters_pushdown.
        let results: Vec<PushedDown> = filters.iter().map(|_| PushedDown::No).collect();
        Ok(FilterPushdownPropagation::with_parent_pushdown_result(
            results,
        ))
    }
}

// ---------------------------------------------------------------------------
// Async document column fill
// ---------------------------------------------------------------------------

/// Add the `_document` column to a batch that was produced without it.
///
/// Reads `_doc_id` from the batch, fetches each document via
/// `Searcher::doc_async` (per-block async I/O), and returns a new batch
/// with `_document` inserted at the correct position per the output schema.
async fn fill_document_column_async(
    batch: RecordBatch,
    prepared: &PreparedSplit,
    output_schema: &SchemaRef,
    tantivy_schema: &tantivy::schema::Schema,
) -> Result<RecordBatch> {
    let intermediate_schema = batch.schema();

    // Find _doc_id to get document addresses.
    let doc_id_idx = intermediate_schema.index_of("_doc_id").map_err(|_| {
        DataFusionError::Internal(
            "_doc_id column required for document fetch but not in batch".into(),
        )
    })?;
    let doc_ids = batch
        .column(doc_id_idx)
        .as_any()
        .downcast_ref::<arrow::array::UInt32Array>()
        .ok_or_else(|| DataFusionError::Internal("_doc_id column is not UInt32".into()))?;
    let segment_ord_idx = intermediate_schema.index_of("_segment_ord").map_err(|_| {
        DataFusionError::Internal(
            "_segment_ord column required for document fetch but not in batch".into(),
        )
    })?;
    let segment_ords = batch
        .column(segment_ord_idx)
        .as_any()
        .downcast_ref::<arrow::array::UInt32Array>()
        .ok_or_else(|| DataFusionError::Internal("_segment_ord column is not UInt32".into()))?;

    // Fetch documents async — each call reads only the specific store block needed.
    let mut doc_builder = StringBuilder::with_capacity(doc_ids.len(), doc_ids.len() * 256);
    for idx in 0..doc_ids.len() {
        let doc_id = doc_ids.value(idx);
        let segment_ord = segment_ords.value(idx);
        let doc_addr = DocAddress::new(segment_ord, doc_id);
        let doc: tantivy::TantivyDocument =
            prepared.searcher().doc_async(doc_addr).await.map_err(|e| {
                DataFusionError::Internal(format!(
                    "async doc fetch segment={segment_ord} doc={doc_id}: {e}"
                ))
            })?;
        doc_builder.append_value(doc.to_json(tantivy_schema));
    }
    let doc_array: Arc<dyn arrow::array::Array> = Arc::new(doc_builder.finish());

    // Build output columns in the order defined by output_schema,
    // pulling from the intermediate batch or inserting _document.
    let mut output_columns: Vec<Arc<dyn arrow::array::Array>> =
        Vec::with_capacity(output_schema.fields().len());

    for field in output_schema.fields() {
        if field.name() == "_document" {
            output_columns.push(Arc::clone(&doc_array));
        } else {
            let col_idx = intermediate_schema.index_of(field.name()).map_err(|_| {
                DataFusionError::Internal(format!(
                    "column '{}' not found in intermediate batch",
                    field.name()
                ))
            })?;
            output_columns.push(batch.column(col_idx).clone());
        }
    }

    RecordBatch::try_new(Arc::clone(output_schema), output_columns)
        .map_err(|e| DataFusionError::Internal(format!("build batch with docs: {e}")))
}

// ---------------------------------------------------------------------------
// Core batch generation
// ---------------------------------------------------------------------------

/// Holds immutable context for assembling one `RecordBatch` from a chunk of
/// `doc_ids` plus optional scores. Created once per segment and reused for every
/// chunk, avoiding 14-parameter function signatures.
struct ChunkBuilder<'a> {
    segment_reader: &'a tantivy::SegmentReader,
    source_ff_schema: &'a SchemaRef,
    fast_field_projection: &'a FastFieldProjectionPlan,
    unified_schema: &'a SchemaRef,
    projected_indices: Vec<usize>,
    score_column_idx: usize,
    document_column_idx: usize,
    needs_score: bool,
    needs_document: bool,
    segment_ord: u32,
    dict_cache: DictCache,
}

impl ChunkBuilder<'_> {
    /// Assemble a `RecordBatch` from a chunk of `doc_ids` and optional scores.
    ///
    /// Produces fast fields and scores only — `_document` is excluded here and
    /// added asynchronously by `fill_document_column_async` after this batch
    /// exits `the sync execution pool`. The returned schema is `intermediate_schema`
    /// (projected schema minus `_document`).
    fn build(&self, chunk_ids: &[u32], chunk_scores: Option<&[f32]>) -> Result<RecordBatch> {
        let source_ff_batch = read_segment_fast_fields_to_batch(
            self.segment_reader,
            self.source_ff_schema,
            Some(chunk_ids),
            None,
            None,
            self.segment_ord,
            Some(&self.dict_cache),
        )?;
        let ff_batch = apply_fast_field_projection(&source_ff_batch, self.fast_field_projection)?;
        let chunk_rows = ff_batch.num_rows();

        let score_array: Option<Arc<dyn arrow::array::Array>> = if self.needs_score {
            match chunk_scores {
                Some(sc) => Some(Arc::new(Float32Array::from_iter_values(sc.iter().copied()))),
                None => Some(arrow::array::new_null_array(&DataType::Float32, chunk_rows)),
            }
        } else {
            None
        };

        let mut output_columns: Vec<Arc<dyn arrow::array::Array>> = Vec::new();
        let mut output_fields: Vec<arrow::datatypes::Field> = Vec::new();

        // When _document is needed, always include _doc_id and _segment_ord in
        // the intermediate batch so fill_document_column_async can find
        // document addresses across split-local segments.
        let doc_id_already_projected = self
            .projected_indices
            .iter()
            .any(|&idx| self.unified_schema.field(idx).name() == "_doc_id");
        if self.needs_document && !doc_id_already_projected {
            let doc_id_name = "_doc_id";
            if let Ok(ff_idx) = ff_batch.schema().index_of(doc_id_name) {
                output_columns.push(ff_batch.column(ff_idx).clone());
                output_fields.push(arrow::datatypes::Field::new(
                    doc_id_name,
                    DataType::UInt32,
                    false,
                ));
            }
        }
        let segment_ord_already_projected = self
            .projected_indices
            .iter()
            .any(|&idx| self.unified_schema.field(idx).name() == "_segment_ord");
        if self.needs_document && !segment_ord_already_projected {
            let segment_ord_name = "_segment_ord";
            if let Ok(ff_idx) = ff_batch.schema().index_of(segment_ord_name) {
                output_columns.push(ff_batch.column(ff_idx).clone());
                output_fields.push(arrow::datatypes::Field::new(
                    segment_ord_name,
                    DataType::UInt32,
                    false,
                ));
            }
        }

        for &unified_idx in &self.projected_indices {
            if unified_idx == self.document_column_idx {
                // Skip _document — it's added async after the sync execution pool.
                continue;
            }
            if unified_idx == self.score_column_idx {
                output_columns.push(score_array.clone().unwrap_or_else(|| {
                    arrow::array::new_null_array(&DataType::Float32, chunk_rows)
                }));
                output_fields.push(self.unified_schema.field(unified_idx).clone());
            } else {
                let col_name = self.unified_schema.field(unified_idx).name();
                let ff_col_idx = ff_batch.schema().index_of(col_name).map_err(|_| {
                    DataFusionError::Internal(format!(
                        "fast field column '{col_name}' not found in ff_batch"
                    ))
                })?;
                output_columns.push(ff_batch.column(ff_col_idx).clone());
                output_fields.push(self.unified_schema.field(unified_idx).clone());
            }
        }

        let intermediate_schema = Arc::new(Schema::new(output_fields));
        if output_columns.is_empty() {
            let options = RecordBatchOptions::new().with_row_count(Some(chunk_rows));
            RecordBatch::try_new_with_options(intermediate_schema, output_columns, &options)
                .map_err(|e| DataFusionError::Internal(format!("build output batch: {e}")))
        } else {
            RecordBatch::try_new(intermediate_schema, output_columns)
                .map_err(|e| DataFusionError::Internal(format!("build output batch: {e}")))
        }
    }
}

/// Generate batches for a single segment, streaming each batch through the
/// `emit` callback as it is produced. Returns `Ok(())` when all batches have
/// been emitted. If `emit` returns `false` (receiver dropped), production
/// stops early.
///
/// Configuration for a single-segment batch generation pass.
/// Constructed in `open()` and moved into `the sync execution pool`.
struct ScanConfig {
    prepared: Arc<PreparedSplit>,
    batch_size: usize,
    source_ff_schema: SchemaRef,
    fast_field_projection: FastFieldProjectionPlan,
    unified_schema: SchemaRef,
    projection: Option<Vec<usize>>,
    score_column_idx: usize,
    document_column_idx: usize,
    needs_score: bool,
    needs_document: bool,
    topk: Option<usize>,
    row_limit: Option<usize>,
    query: Option<Arc<dyn tantivy::query::Query>>,
    cancelled: Arc<AtomicBool>,
}

/// Thin orchestrator: query execution is delegated to the streaming helpers in
/// [`crate::util`] and batch assembly to [`ChunkBuilder::build`].
fn generate_single_table_batch_streaming(
    cfg: &ScanConfig,
    mut emit: impl FnMut(RecordBatch) -> bool,
) -> Result<()> {
    let projected_indices: Vec<usize> = match &cfg.projection {
        Some(indices) => indices.clone(),
        None => (0..cfg.unified_schema.fields().len()).collect(),
    };
    let mut remaining = cfg.row_limit.unwrap_or(usize::MAX);

    if let Some(topk) = cfg.topk {
        let effective_topk = remaining.min(topk);
        return emit_topk_batches(cfg, &projected_indices, effective_topk, &mut emit);
    }

    emit_segment_scan_batches(cfg, &projected_indices, &mut remaining, &mut emit)
}

fn build_chunk_builder<'a>(
    cfg: &'a ScanConfig,
    segment_reader: &'a tantivy::SegmentReader,
    segment_ord: u32,
    projected_indices: &[usize],
    dict_cache: DictCache,
) -> ChunkBuilder<'a> {
    ChunkBuilder {
        segment_reader,
        source_ff_schema: &cfg.source_ff_schema,
        fast_field_projection: &cfg.fast_field_projection,
        unified_schema: &cfg.unified_schema,
        projected_indices: projected_indices.to_vec(),
        score_column_idx: cfg.score_column_idx,
        document_column_idx: cfg.document_column_idx,
        needs_score: cfg.needs_score,
        needs_document: cfg.needs_document,
        segment_ord,
        dict_cache,
    }
}

fn emit_topk_batches(
    cfg: &ScanConfig,
    projected_indices: &[usize],
    effective_topk: usize,
    emit: &mut impl FnMut(RecordBatch) -> bool,
) -> Result<()> {
    if effective_topk == 0 {
        return Ok(());
    }
    if effective_topk > u32::MAX as usize {
        return Err(DataFusionError::Internal(format!(
            "topk {effective_topk} exceeds UInt32 take index capacity"
        )));
    }

    let query = cfg.query.as_ref().ok_or_else(|| {
        DataFusionError::Internal("topk collection requires an active query".into())
    })?;
    let hits = cfg
        .prepared
        .searcher()
        .search(
            query.as_ref(),
            &TopDocs::with_limit(effective_topk).order_by_score(),
        )
        .map_err(|e| DataFusionError::Internal(format!("topk search: {e}")))?;
    if hits.is_empty() {
        return Ok(());
    }

    let (segment_batches, take_positions) = build_topk_segment_batches(
        cfg,
        projected_indices,
        group_topk_hits(hits),
        effective_topk,
    )?;
    let reordered = reorder_topk_batches(segment_batches, &take_positions)?;
    emit_sliced_batch(&reordered, cfg.batch_size, cfg.cancelled.as_ref(), emit)
}

fn group_topk_hits(
    hits: Vec<(f32, tantivy::DocAddress)>,
) -> std::collections::BTreeMap<u32, Vec<(u32, f32, usize)>> {
    let mut grouped: std::collections::BTreeMap<u32, Vec<(u32, f32, usize)>> =
        std::collections::BTreeMap::new();
    for (original_pos, (score, doc_addr)) in hits.into_iter().enumerate() {
        grouped.entry(doc_addr.segment_ord).or_default().push((
            doc_addr.doc_id,
            score,
            original_pos,
        ));
    }
    grouped
}

fn build_topk_segment_batches(
    cfg: &ScanConfig,
    projected_indices: &[usize],
    grouped: std::collections::BTreeMap<u32, Vec<(u32, f32, usize)>>,
    effective_topk: usize,
) -> Result<(Vec<RecordBatch>, Vec<u32>)> {
    let mut segment_batches = Vec::new();
    let mut take_positions = vec![0u32; effective_topk];
    let mut concat_offset = 0usize;

    for (segment_ord, rows) in grouped {
        let segment_reader = cfg.prepared.searcher().segment_reader(segment_ord);
        let dict_cache = DictCache::build(segment_reader, &cfg.source_ff_schema)?;
        let builder = build_chunk_builder(
            cfg,
            segment_reader,
            segment_ord,
            projected_indices,
            dict_cache,
        );
        let (doc_ids, scores) =
            collect_topk_rows(rows, concat_offset, take_positions.as_mut_slice())?;
        let batch = builder.build(&doc_ids, Some(&scores))?;
        concat_offset += batch.num_rows();
        segment_batches.push(batch);
    }

    Ok((segment_batches, take_positions))
}

fn collect_topk_rows(
    rows: Vec<(u32, f32, usize)>,
    concat_offset: usize,
    take_positions: &mut [u32],
) -> Result<(Vec<u32>, Vec<f32>)> {
    let mut doc_ids = Vec::with_capacity(rows.len());
    let mut scores = Vec::with_capacity(rows.len());
    for (local_idx, (doc_id, score, original_pos)) in rows.into_iter().enumerate() {
        doc_ids.push(doc_id);
        scores.push(score);
        take_positions[original_pos] = u32::try_from(concat_offset + local_idx).map_err(|_| {
            DataFusionError::Internal(
                "topk concatenated row offset exceeds UInt32 take index capacity".into(),
            )
        })?;
    }
    Ok((doc_ids, scores))
}

fn reorder_topk_batches(
    mut segment_batches: Vec<RecordBatch>,
    take_positions: &[u32],
) -> Result<RecordBatch> {
    if segment_batches.len() == 1 {
        return segment_batches
            .pop()
            .ok_or_else(|| DataFusionError::Internal("missing topk batch".into()));
    }

    let schema = segment_batches
        .first()
        .map(RecordBatch::schema)
        .ok_or_else(|| DataFusionError::Internal("missing topk batches".into()))?;
    let concatenated = concat_batches(&schema, &segment_batches)
        .map_err(|e| DataFusionError::ArrowError(Box::new(e), None))?;
    let take_indices = UInt32Array::from_iter_values(take_positions.iter().copied());
    let columns: Vec<ArrayRef> = concatenated
        .columns()
        .iter()
        .map(|column| {
            take(column.as_ref(), &take_indices, None)
                .map_err(|e| DataFusionError::ArrowError(Box::new(e), None))
        })
        .collect::<Result<_>>()?;
    RecordBatch::try_new(concatenated.schema(), columns)
        .map_err(|e| DataFusionError::ArrowError(Box::new(e), None))
}

fn emit_sliced_batch(
    batch: &RecordBatch,
    batch_size: usize,
    cancelled: &AtomicBool,
    emit: &mut impl FnMut(RecordBatch) -> bool,
) -> Result<()> {
    let total_rows = batch.num_rows();
    let mut offset = 0usize;
    while offset < total_rows {
        if cancelled.load(Ordering::Relaxed) {
            return Ok(());
        }
        let len = (total_rows - offset).min(batch_size);
        let batch = batch.slice(offset, len);
        if batch.num_rows() > 0 && !emit(batch) {
            return Ok(());
        }
        offset += len;
    }
    Ok(())
}

fn emit_segment_scan_batches(
    cfg: &ScanConfig,
    projected_indices: &[usize],
    remaining: &mut usize,
    emit: &mut impl FnMut(RecordBatch) -> bool,
) -> Result<()> {
    let index = cfg.prepared.index();
    let searcher = cfg.prepared.searcher();

    for (segment_ord, segment_reader) in searcher.segment_readers().iter().enumerate() {
        if *remaining == 0 || cfg.cancelled.load(Ordering::Relaxed) {
            break;
        }

        let dict_cache = DictCache::build(segment_reader, &cfg.source_ff_schema)?;
        let segment_ord = u32::try_from(segment_ord).map_err(|_| {
            DataFusionError::Internal(format!("segment ordinal {segment_ord} exceeds u32"))
        })?;
        let builder = build_chunk_builder(
            cfg,
            segment_reader,
            segment_ord,
            projected_indices,
            dict_cache,
        );

        for_each_matching_doc_chunks(
            MatchingDocChunksConfig {
                segment_reader,
                searcher,
                query: cfg.query.as_ref(),
                index_schema: &index.schema(),
                needs_score: cfg.needs_score,
                batch_size: cfg.batch_size,
                cancelled: cfg.cancelled.as_ref(),
            },
            |chunk_ids, chunk_scores| {
                if *remaining == 0 || cfg.cancelled.load(Ordering::Relaxed) {
                    return Ok(false);
                }

                let take = (*remaining).min(chunk_ids.len());
                let batch = builder.build(
                    &chunk_ids[..take],
                    chunk_scores.map(|scores| &scores[..take]),
                )?;
                *remaining -= take;
                if batch.num_rows() > 0 && !emit(batch) {
                    return Ok(false);
                }
                Ok(*remaining > 0)
            },
        )?;
    }

    Ok(())
}

// ---------------------------------------------------------------------------
// Logical Expr → tantivy query conversion for fast field filters
// ---------------------------------------------------------------------------

/// Try to convert a logical `Expr` (column op literal) to a tantivy query.
///
/// Handles simple comparisons where the column is a tantivy FAST field and
/// the operator is one of `=`, `>`, `>=`, `<`, `<=`.
fn logical_expr_to_tantivy_query(
    expr: &Expr,
    tantivy_schema: &TantivySchema,
) -> Option<Box<dyn tantivy::query::Query>> {
    let Expr::BinaryExpr(binary) = expr else {
        return None;
    };

    match binary.op {
        Operator::And => {
            let left = logical_expr_to_tantivy_query(binary.left.as_ref(), tantivy_schema)?;
            let right = logical_expr_to_tantivy_query(binary.right.as_ref(), tantivy_schema)?;
            return Some(Box::new(tantivy::query::BooleanQuery::intersection(vec![
                left, right,
            ])));
        }
        Operator::Or => {
            let left = logical_expr_to_tantivy_query(binary.left.as_ref(), tantivy_schema)?;
            let right = logical_expr_to_tantivy_query(binary.right.as_ref(), tantivy_schema)?;
            return Some(Box::new(tantivy::query::BooleanQuery::union(vec![
                left, right,
            ])));
        }
        _ => {}
    }

    let (col_name, scalar, col_on_left) = match (binary.left.as_ref(), binary.right.as_ref()) {
        (Expr::Column(col), Expr::Literal(sv, _)) => (col.name.clone(), sv.clone(), true),
        (Expr::Literal(sv, _), Expr::Column(col)) => (col.name.clone(), sv.clone(), false),
        _ => return None,
    };

    let field = tantivy_schema.get_field(&col_name).ok()?;
    let field_entry = tantivy_schema.get_field_entry(field);
    if !field_entry.is_fast() {
        return None;
    }

    let op = if col_on_left {
        binary.op
    } else {
        logical_flip_operator(binary.op)?
    };

    let term = logical_scalar_to_term(field, field_entry.field_type(), &scalar)?;

    match op {
        Operator::Eq => {
            // For indexed non-text fields, use TermQuery (posting list lookup, O(matches)).
            // For text fields or non-indexed fields, use RangeQuery (fast field scan).
            // TermQuery on tokenized text fields would search for individual tokens,
            // not the original value — causing silent false negatives.
            if field_entry.is_indexed() && !matches!(field_entry.field_type(), FieldType::Str(_)) {
                Some(Box::new(tantivy::query::TermQuery::new(
                    term,
                    IndexRecordOption::Basic,
                )))
            } else {
                Some(Box::new(RangeQuery::new(
                    Bound::Included(term.clone()),
                    Bound::Included(term),
                )))
            }
        }
        Operator::Gt => Some(Box::new(RangeQuery::new(
            Bound::Excluded(term),
            Bound::Unbounded,
        ))),
        Operator::GtEq => Some(Box::new(RangeQuery::new(
            Bound::Included(term),
            Bound::Unbounded,
        ))),
        Operator::Lt => Some(Box::new(RangeQuery::new(
            Bound::Unbounded,
            Bound::Excluded(term),
        ))),
        Operator::LtEq => Some(Box::new(RangeQuery::new(
            Bound::Unbounded,
            Bound::Included(term),
        ))),
        Operator::NotEq => {
            // NotEq as union of (< term) OR (> term)
            let lt = Box::new(RangeQuery::new(
                Bound::Unbounded,
                Bound::Excluded(term.clone()),
            ));
            let gt = Box::new(RangeQuery::new(Bound::Excluded(term), Bound::Unbounded));
            Some(Box::new(tantivy::query::BooleanQuery::union(vec![lt, gt])))
        }
        _ => None,
    }
}

/// Flip a comparison operator when the column is on the right side.
fn logical_flip_operator(op: Operator) -> Option<Operator> {
    match op {
        Operator::Eq => Some(Operator::Eq),
        Operator::NotEq => Some(Operator::NotEq),
        Operator::Gt => Some(Operator::Lt),
        Operator::GtEq => Some(Operator::LtEq),
        Operator::Lt => Some(Operator::Gt),
        Operator::LtEq => Some(Operator::GtEq),
        _ => None,
    }
}

/// Convert a DataFusion `ScalarValue` to a tantivy `Term` for the given field.
fn logical_scalar_to_term(
    field: tantivy::schema::Field,
    field_type: &FieldType,
    scalar: &ScalarValue,
) -> Option<Term> {
    match field_type {
        FieldType::I64(_) => {
            let v = match scalar {
                ScalarValue::Int64(Some(v)) => *v,
                ScalarValue::Int32(Some(v)) => i64::from(*v),
                ScalarValue::Int16(Some(v)) => i64::from(*v),
                ScalarValue::Int8(Some(v)) => i64::from(*v),
                ScalarValue::UInt64(Some(v)) => i64::try_from(*v).ok()?,
                ScalarValue::UInt32(Some(v)) => i64::from(*v),
                ScalarValue::UInt16(Some(v)) => i64::from(*v),
                ScalarValue::UInt8(Some(v)) => i64::from(*v),
                ScalarValue::TimestampMillisecond(Some(v), _) => *v,
                ScalarValue::Float64(Some(v)) => {
                    if v.fract() != 0.0 || *v > i64::MAX as f64 || *v < i64::MIN as f64 {
                        return None;
                    }
                    *v as i64
                }
                _ => return None,
            };
            return Some(Term::from_field_i64(field, v));
        }
        FieldType::U64(_) => {
            let v = match scalar {
                ScalarValue::UInt64(Some(v)) => *v,
                ScalarValue::UInt32(Some(v)) => u64::from(*v),
                ScalarValue::UInt16(Some(v)) => u64::from(*v),
                ScalarValue::UInt8(Some(v)) => u64::from(*v),
                ScalarValue::Int64(Some(v)) => u64::try_from(*v).ok()?,
                ScalarValue::Int32(Some(v)) => u64::try_from(*v).ok()?,
                ScalarValue::Int16(Some(v)) => u64::try_from(*v).ok()?,
                ScalarValue::Int8(Some(v)) => u64::try_from(*v).ok()?,
                ScalarValue::Float64(Some(v)) => {
                    if v.fract() != 0.0 || *v < 0.0 || *v > u64::MAX as f64 {
                        return None;
                    }
                    *v as u64
                }
                _ => return None,
            };
            return Some(Term::from_field_u64(field, v));
        }
        FieldType::F64(_) => {
            let v = match scalar {
                ScalarValue::Float64(Some(v)) => *v,
                ScalarValue::Float32(Some(v)) => f64::from(*v),
                ScalarValue::Int64(Some(v)) => *v as f64,
                ScalarValue::Int32(Some(v)) => f64::from(*v),
                ScalarValue::Int16(Some(v)) => f64::from(*v),
                ScalarValue::Int8(Some(v)) => f64::from(*v),
                ScalarValue::UInt64(Some(v)) => *v as f64,
                ScalarValue::UInt32(Some(v)) => f64::from(*v),
                ScalarValue::UInt16(Some(v)) => f64::from(*v),
                ScalarValue::UInt8(Some(v)) => f64::from(*v),
                _ => return None,
            };
            return Some(Term::from_field_f64(field, v));
        }
        _ => {}
    }
    match (field_type, scalar) {
        (FieldType::Bool(_), ScalarValue::Boolean(Some(v))) => {
            Some(Term::from_field_bool(field, *v))
        }
        (FieldType::Str(_), ScalarValue::Utf8(Some(s))) => Some(Term::from_field_text(field, s)),
        // Date — tantivy DateTime stores nanoseconds internally.
        (FieldType::Date(_), ScalarValue::TimestampMicrosecond(Some(v), _)) => Some(
            Term::from_field_date(field, DateTime::from_timestamp_micros(*v)),
        ),
        (FieldType::Date(_), ScalarValue::TimestampSecond(Some(v), _)) => Some(
            Term::from_field_date(field, DateTime::from_timestamp_secs(*v)),
        ),
        (FieldType::Date(_), ScalarValue::TimestampMillisecond(Some(v), _)) => Some(
            Term::from_field_date(field, DateTime::from_timestamp_millis(*v)),
        ),
        (FieldType::Date(_), ScalarValue::TimestampNanosecond(Some(v), _)) => Some(
            Term::from_field_date(field, DateTime::from_timestamp_nanos(*v)),
        ),
        // IpAddr — mapped to Utf8 in schema_mapping, tantivy stores as Ipv6Addr.
        (FieldType::IpAddr(_), ScalarValue::Utf8(Some(s))) => {
            let ip: std::net::IpAddr = s.parse().ok()?;
            let ipv6 = match ip {
                std::net::IpAddr::V4(v4) => v4.to_ipv6_mapped(),
                std::net::IpAddr::V6(v6) => v6,
            };
            Some(Term::from_field_ip_addr(field, ipv6))
        }
        // Bytes — mapped to Binary in schema_mapping.
        (FieldType::Bytes(_), ScalarValue::Binary(Some(b))) => {
            Some(Term::from_field_bytes(field, b))
        }
        _ => None,
    }
}

// ---------------------------------------------------------------------------
// Serializable representation of fast field filters for the codec
// ---------------------------------------------------------------------------

/// A simple, serializable representation of a `column op literal` filter
/// expression. Used by the codec to serialize fast field filters that were
/// converted to tantivy queries so workers can re-derive `pre_built_query`.
#[derive(serde::Serialize, serde::Deserialize, Debug)]
pub(crate) struct FastFieldFilter {
    field: String,
    op: String,
    value: String,
    value_type: String,
}

/// Serialize a slice of logical `Expr`s (each `column op literal`) to JSON.
pub(crate) fn serialize_fast_field_filters(exprs: &[Expr]) -> Result<String> {
    let filters: Vec<FastFieldFilter> = exprs
        .iter()
        .filter_map(|expr| {
            let Expr::BinaryExpr(binary) = expr else {
                return None;
            };
            let (col_name, scalar, col_on_left) =
                match (binary.left.as_ref(), binary.right.as_ref()) {
                    (Expr::Column(col), Expr::Literal(sv, _)) => {
                        (col.name.clone(), sv.clone(), true)
                    }
                    (Expr::Literal(sv, _), Expr::Column(col)) => {
                        (col.name.clone(), sv.clone(), false)
                    }
                    _ => return None,
                };
            let op = if col_on_left {
                binary.op
            } else {
                logical_flip_operator(binary.op)?
            };
            let op_str = match op {
                Operator::Eq => "eq",
                Operator::NotEq => "neq",
                Operator::Gt => "gt",
                Operator::GtEq => "gte",
                Operator::Lt => "lt",
                Operator::LtEq => "lte",
                _ => return None,
            };
            let (value, value_type) = scalar_to_json_pair(&scalar)?;
            Some(FastFieldFilter {
                field: col_name,
                op: op_str.to_string(),
                value,
                value_type,
            })
        })
        .collect();
    serde_json::to_string(&filters)
        .map_err(|e| DataFusionError::Internal(format!("serialize fast field filters: {e}")))
}

/// Deserialize fast field filter JSON and reconstruct tantivy queries.
///
/// Returns the reconstructed tantivy queries; the caller combines them with
/// `BooleanQuery::intersection` as usual.
pub(crate) fn deserialize_fast_field_filters(
    json: &str,
    tantivy_schema: &TantivySchema,
) -> Result<Vec<Box<dyn tantivy::query::Query>>> {
    if json.is_empty() {
        return Ok(Vec::new());
    }
    let filters: Vec<FastFieldFilter> = serde_json::from_str(json)
        .map_err(|e| DataFusionError::Internal(format!("deserialize fast field filters: {e}")))?;
    let mut queries = Vec::with_capacity(filters.len());
    for f in &filters {
        let scalar = json_pair_to_scalar(&f.value, &f.value_type)?;
        let op = match f.op.as_str() {
            "eq" => Operator::Eq,
            "neq" => Operator::NotEq,
            "gt" => Operator::Gt,
            "gte" => Operator::GtEq,
            "lt" => Operator::Lt,
            "lte" => Operator::LtEq,
            other => {
                return Err(DataFusionError::Internal(format!(
                    "unknown fast field filter op: {other}"
                )));
            }
        };
        let expr = Expr::BinaryExpr(datafusion::logical_expr::BinaryExpr {
            left: Box::new(Expr::Column(datafusion::common::Column::new_unqualified(
                &f.field,
            ))),
            op,
            right: Box::new(Expr::Literal(scalar, None)),
        });
        let query = logical_expr_to_tantivy_query(&expr, tantivy_schema).ok_or_else(|| {
            DataFusionError::Internal(format!(
                "failed to reconstruct fast field filter '{}' {} during codec decode",
                f.field, f.op
            ))
        })?;
        queries.push(query);
    }
    Ok(queries)
}

/// Deserialize fast field filter JSON back into logical `Expr`s.
pub(crate) fn deserialize_fast_field_filter_exprs(json: &str) -> Result<Vec<Expr>> {
    if json.is_empty() {
        return Ok(Vec::new());
    }

    let filters: Vec<FastFieldFilter> = serde_json::from_str(json)
        .map_err(|e| DataFusionError::Internal(format!("deserialize fast field filters: {e}")))?;

    filters
        .into_iter()
        .map(|filter| {
            let scalar = json_pair_to_scalar(&filter.value, &filter.value_type)?;
            let op = match filter.op.as_str() {
                "eq" => Operator::Eq,
                "neq" => Operator::NotEq,
                "gt" => Operator::Gt,
                "gte" => Operator::GtEq,
                "lt" => Operator::Lt,
                "lte" => Operator::LtEq,
                other => {
                    return Err(DataFusionError::Internal(format!(
                        "unknown fast field filter op: {other}"
                    )));
                }
            };

            Ok(Expr::BinaryExpr(datafusion::logical_expr::BinaryExpr {
                left: Box::new(Expr::Column(datafusion::common::Column::new_unqualified(
                    filter.field,
                ))),
                op,
                right: Box::new(Expr::Literal(scalar, None)),
            }))
        })
        .collect()
}

/// Encode a `ScalarValue` as a `(value_string, type_tag)` pair for JSON.
fn scalar_to_json_pair(scalar: &ScalarValue) -> Option<(String, String)> {
    let timestamp_tag = |prefix: &str, tz: &Option<Arc<str>>| match tz {
        Some(tz) => format!("{prefix}:{tz}"),
        None => prefix.to_string(),
    };

    match scalar {
        ScalarValue::Int8(Some(v)) => Some((v.to_string(), "i8".into())),
        ScalarValue::Int16(Some(v)) => Some((v.to_string(), "i16".into())),
        ScalarValue::Int32(Some(v)) => Some((v.to_string(), "i32".into())),
        ScalarValue::Int64(Some(v)) => Some((v.to_string(), "i64".into())),
        ScalarValue::UInt8(Some(v)) => Some((v.to_string(), "u8".into())),
        ScalarValue::UInt16(Some(v)) => Some((v.to_string(), "u16".into())),
        ScalarValue::UInt32(Some(v)) => Some((v.to_string(), "u32".into())),
        ScalarValue::UInt64(Some(v)) => Some((v.to_string(), "u64".into())),
        ScalarValue::Float32(Some(v)) => Some((v.to_string(), "f32".into())),
        ScalarValue::Float64(Some(v)) => Some((v.to_string(), "f64".into())),
        ScalarValue::Boolean(Some(v)) => Some((v.to_string(), "bool".into())),
        ScalarValue::Utf8(Some(v)) => Some((v.clone(), "utf8".into())),
        ScalarValue::Binary(Some(v)) => {
            use base64::Engine;
            Some((
                base64::engine::general_purpose::STANDARD.encode(v),
                "binary".into(),
            ))
        }
        ScalarValue::TimestampSecond(Some(v), tz) => {
            Some((v.to_string(), timestamp_tag("ts_s", tz)))
        }
        ScalarValue::TimestampMillisecond(Some(v), tz) => {
            Some((v.to_string(), timestamp_tag("ts_ms", tz)))
        }
        ScalarValue::TimestampMicrosecond(Some(v), tz) => {
            Some((v.to_string(), timestamp_tag("ts_us", tz)))
        }
        ScalarValue::TimestampNanosecond(Some(v), tz) => {
            Some((v.to_string(), timestamp_tag("ts_ns", tz)))
        }
        _ => None,
    }
}

/// Decode a `(value_string, type_tag)` pair back to a `ScalarValue`.
fn json_pair_to_scalar(value: &str, value_type: &str) -> Result<ScalarValue> {
    let parse_timestamp_tag = |prefix: &str| -> Option<Option<Arc<str>>> {
        if value_type == prefix {
            Some(None)
        } else {
            value_type
                .strip_prefix(&format!("{prefix}:"))
                .map(|tz| Some(Arc::<str>::from(tz)))
        }
    };
    let parse_err = |e: std::num::ParseIntError| {
        DataFusionError::Internal(format!("parse {value_type} '{value}': {e}"))
    };
    let parse_float_err = |e: std::num::ParseFloatError| {
        DataFusionError::Internal(format!("parse {value_type} '{value}': {e}"))
    };
    match value_type {
        "i8" => Ok(ScalarValue::Int8(Some(value.parse().map_err(parse_err)?))),
        "i16" => Ok(ScalarValue::Int16(Some(value.parse().map_err(parse_err)?))),
        "i32" => Ok(ScalarValue::Int32(Some(value.parse().map_err(parse_err)?))),
        "i64" => Ok(ScalarValue::Int64(Some(value.parse().map_err(parse_err)?))),
        "u8" => Ok(ScalarValue::UInt8(Some(value.parse().map_err(parse_err)?))),
        "u16" => Ok(ScalarValue::UInt16(Some(value.parse().map_err(parse_err)?))),
        "u32" => Ok(ScalarValue::UInt32(Some(value.parse().map_err(parse_err)?))),
        "u64" => Ok(ScalarValue::UInt64(Some(value.parse().map_err(parse_err)?))),
        "f32" => Ok(ScalarValue::Float32(Some(
            value.parse().map_err(parse_float_err)?,
        ))),
        "f64" => Ok(ScalarValue::Float64(Some(
            value.parse().map_err(parse_float_err)?,
        ))),
        "bool" => Ok(ScalarValue::Boolean(Some(value == "true"))),
        "utf8" => Ok(ScalarValue::Utf8(Some(value.to_string()))),
        "binary" => {
            use base64::Engine;
            let bytes = base64::engine::general_purpose::STANDARD
                .decode(value)
                .map_err(|e| DataFusionError::Internal(format!("decode base64 binary: {e}")))?;
            Ok(ScalarValue::Binary(Some(bytes)))
        }
        _ => {
            if let Some(tz) = parse_timestamp_tag("ts_s") {
                return Ok(ScalarValue::TimestampSecond(
                    Some(value.parse().map_err(parse_err)?),
                    tz,
                ));
            }
            if let Some(tz) = parse_timestamp_tag("ts_ms") {
                return Ok(ScalarValue::TimestampMillisecond(
                    Some(value.parse().map_err(parse_err)?),
                    tz,
                ));
            }
            if let Some(tz) = parse_timestamp_tag("ts_us") {
                return Ok(ScalarValue::TimestampMicrosecond(
                    Some(value.parse().map_err(parse_err)?),
                    tz,
                ));
            }
            if let Some(tz) = parse_timestamp_tag("ts_ns") {
                return Ok(ScalarValue::TimestampNanosecond(
                    Some(value.parse().map_err(parse_err)?),
                    tz,
                ));
            }

            Err(DataFusionError::Internal(format!(
                "unknown scalar type tag: {value_type}"
            )))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{
        analyze_fast_field_filter_support, json_pair_to_scalar, logical_expr_to_tantivy_query,
        logical_scalar_to_term, scalar_to_json_pair, FilterPushdownSupport,
    };
    use datafusion::common::ScalarValue;
    use datafusion::prelude::{col, lit};
    use std::sync::Arc;
    use tantivy::schema::{Schema as TantivySchema, FAST, INDEXED, STRING};

    #[test]
    fn test_timestamp_scalar_json_roundtrip_preserves_timezone() {
        let scalar =
            ScalarValue::TimestampMicrosecond(Some(1_234_567), Some(Arc::<str>::from("UTC")));
        let (value, tag) = scalar_to_json_pair(&scalar).unwrap();
        let decoded = json_pair_to_scalar(&value, &tag).unwrap();

        assert_eq!(decoded, scalar);
    }

    #[test]
    fn i64_fast_field_accepts_timestamp_millis_filter_literal() {
        let mut builder = TantivySchema::builder();
        let field = builder.add_i64_field("timestamp", FAST | INDEXED);
        let schema = builder.build();
        let field_type = schema.get_field_entry(field).field_type();

        assert!(logical_scalar_to_term(
            field,
            field_type,
            &ScalarValue::TimestampMillisecond(Some(1_779_287_400_123), None),
        )
        .is_some());
    }

    #[test]
    fn ored_string_fast_field_filters_build_union_query() {
        let mut builder = TantivySchema::builder();
        builder.add_text_field("__encoded_id__", STRING | FAST);
        let schema = builder.build();
        let expr = col("__encoded_id__")
            .eq(lit("event-1"))
            .or(col("__encoded_id__").eq(lit("event-2")));

        assert_eq!(
            analyze_fast_field_filter_support(&expr, &schema),
            FilterPushdownSupport::Query
        );
        assert!(logical_expr_to_tantivy_query(&expr, &schema).is_some());
    }
}
