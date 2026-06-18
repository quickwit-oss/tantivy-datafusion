use std::sync::Arc;

use arrow::array::{
    ArrayRef, BooleanBuilder, Float64Array, Float64Builder, Int64Array, Int64Builder, ListBuilder,
    RecordBatch, StringBuilder, UInt64Array, UInt64Builder,
};
use arrow::datatypes::{DataType, SchemaRef};
use datafusion::common::Result;
use datafusion::error::DataFusionError;
use tantivy::aggregation::agg_req::{Aggregation, AggregationVariants, Aggregations};
use tantivy::aggregation::agg_result::{
    AggregationResult, AggregationResults, BucketEntries, BucketEntry, BucketResult, MetricResult,
    RangeBucketEntry,
};
use tantivy::aggregation::intermediate_agg_result::IntermediateAggregationResults;
use tantivy::aggregation::metric::{PercentilesMetricResult, SingleMetricResult};
use tantivy::aggregation::DistributedAggregationCollector;
use tantivy::aggregation::Key;
use tantivy::query::Query;
use tantivy::Index;

// ---------------------------------------------------------------------------
// Core tantivy aggregation execution
// ---------------------------------------------------------------------------

pub(crate) fn execute_tantivy_agg_with_reader(
    index: &Index,
    aggs: &Aggregations,
    query: Option<&Arc<dyn Query>>,
    output_schema: &SchemaRef,
    existing_reader: Option<&tantivy::IndexReader>,
) -> Result<RecordBatch> {
    let agg_results = execute_tantivy_agg_results_with_reader(index, aggs, query, existing_reader)?;
    agg_results_to_batch(&agg_results, aggs, output_schema)
}

pub(crate) fn execute_tantivy_agg_results_with_reader(
    index: &Index,
    aggs: &Aggregations,
    query: Option<&Arc<dyn Query>>,
    existing_reader: Option<&tantivy::IndexReader>,
) -> Result<AggregationResults> {
    let owned_reader;
    let reader = match existing_reader {
        Some(r) => r,
        None => {
            owned_reader = index
                .reader()
                .map_err(|e| DataFusionError::Internal(format!("open reader: {e}")))?;
            &owned_reader
        }
    };
    let searcher = reader.searcher();

    // Use tantivy's native Searcher::search() which parallelizes across
    // segments using Rayon. Our previous manual segment loop was serial,
    // causing a 3x regression on 3-segment indexes.
    let collector =
        tantivy::aggregation::AggregationCollector::from_aggs(aggs.clone(), Default::default());

    let effective_query: Box<dyn Query> = match query {
        Some(q) => q.box_clone(),
        None => Box::new(tantivy::query::AllQuery),
    };

    let agg_results = searcher
        .search(effective_query.as_ref(), &collector)
        .map_err(|e| DataFusionError::Internal(format!("aggregation search: {e}")))?;

    Ok(agg_results)
}

pub(crate) fn execute_tantivy_intermediate_agg_with_reader(
    index: &Index,
    aggs: &Aggregations,
    query: Option<&Arc<dyn Query>>,
    existing_reader: Option<&tantivy::IndexReader>,
) -> Result<IntermediateAggregationResults> {
    let owned_reader;
    let reader = match existing_reader {
        Some(r) => r,
        None => {
            owned_reader = index
                .reader()
                .map_err(|e| DataFusionError::Internal(format!("open reader: {e}")))?;
            &owned_reader
        }
    };
    let searcher = reader.searcher();
    let collector = DistributedAggregationCollector::from_aggs(aggs.clone(), Default::default());

    let effective_query: Box<dyn Query> = match query {
        Some(q) => q.box_clone(),
        None => Box::new(tantivy::query::AllQuery),
    };

    searcher
        .search(effective_query.as_ref(), &collector)
        .map_err(|e| DataFusionError::Internal(format!("distributed aggregation search: {e}")))
}

pub(crate) fn merge_intermediate_agg_results(
    mut partials: Vec<IntermediateAggregationResults>,
    aggs: &Aggregations,
) -> Result<AggregationResults> {
    let mut merged = partials.pop().unwrap_or_default();
    for partial in partials {
        merged
            .merge_fruits(partial)
            .map_err(|e| DataFusionError::Internal(format!("merge aggregation results: {e}")))?;
    }

    merged
        .into_final_result(aggs.clone(), Default::default())
        .map_err(|e| DataFusionError::Internal(format!("finalize aggregation results: {e}")))
}

pub(crate) fn agg_results_to_output_batch(
    results: &AggregationResults,
    aggs: &Aggregations,
    schema: &SchemaRef,
) -> Result<RecordBatch> {
    agg_results_to_batch(results, aggs, schema)
}

pub(crate) fn agg_results_to_partial_state_batch(
    results: &AggregationResults,
    aggs: &Aggregations,
    schema: &SchemaRef,
) -> Result<RecordBatch> {
    if results.0.len() != 1 || aggs.len() != 1 {
        return Err(DataFusionError::NotImplemented(
            "tantivy partial agg pushdown supports only a single top-level aggregation".into(),
        ));
    }

    let (agg_name, agg_def) = aggs
        .iter()
        .next()
        .ok_or_else(|| DataFusionError::Internal("empty aggregations".into()))?;

    let agg_result = results
        .0
        .get(agg_name)
        .ok_or_else(|| DataFusionError::Internal(format!("missing result for '{agg_name}'")))?;

    match agg_result {
        AggregationResult::BucketResult(BucketResult::Terms {
            buckets,
            sum_other_doc_count: _,
            ..
        }) => terms_bucket_to_partial_state_batch(buckets, agg_def, schema),
        _ => Err(DataFusionError::Internal(
            "partial agg pushdown supports only terms bucket aggregations".into(),
        )),
    }
}

// ---------------------------------------------------------------------------
// AggregationResults → Arrow RecordBatch conversion
// ---------------------------------------------------------------------------

/// Convert tantivy `AggregationResults` into an Arrow `RecordBatch`
/// matching the schema produced by `translate_aggregations`.
fn agg_results_to_batch(
    results: &AggregationResults,
    aggs: &Aggregations,
    schema: &SchemaRef,
) -> Result<RecordBatch> {
    // The schema has columns from `translate_aggregations`.
    // We need to map the tantivy result structure to those columns.
    //
    // For a single top-level agg (the common case when optimizer replaces
    // one AggregateExec), the schema columns come directly from that agg.

    // Determine the agg type. Since this exec replaces a single DataFrame's
    // AggregateExec, we operate on a single aggregation key.
    // The optimizer should only replace plans for a single top-level agg.
    if results.0.len() != 1 || aggs.len() != 1 {
        return Err(DataFusionError::NotImplemented(
            "tantivy agg pushdown supports only a single top-level aggregation".into(),
        ));
    }

    let (agg_name, agg_def) = aggs
        .iter()
        .next()
        .ok_or_else(|| DataFusionError::Internal("empty aggregations".into()))?;

    let agg_result = results
        .0
        .get(agg_name)
        .ok_or_else(|| DataFusionError::Internal(format!("missing result for '{agg_name}'")))?;

    match (&agg_def.agg, agg_result) {
        // Metric-only aggregations → 1 row
        (_, AggregationResult::MetricResult(metric)) => {
            metric_to_batch(metric, agg_name, agg_def, schema)
        }
        // Bucket aggregations → N rows
        (_, AggregationResult::BucketResult(bucket)) => bucket_to_batch(bucket, agg_def, schema),
    }
}

/// Convert a metric result to a single-row RecordBatch.
fn metric_to_batch(
    metric: &MetricResult,
    name: &str,
    _agg_def: &Aggregation,
    schema: &SchemaRef,
) -> Result<RecordBatch> {
    let mut columns: Vec<ArrayRef> = Vec::with_capacity(schema.fields().len());

    for field in schema.fields() {
        let col_name = field.name();
        let value = extract_metric_value(metric, name, col_name);
        let array = scalar_to_array(value, field.data_type());
        columns.push(array);
    }

    RecordBatch::try_new(schema.clone(), columns)
        .map_err(|e| DataFusionError::Internal(format!("build metric batch: {e}")))
}

/// Extract a single f64 value from a MetricResult for a given column name.
fn extract_metric_value(metric: &MetricResult, agg_name: &str, col_name: &str) -> Option<f64> {
    match metric {
        MetricResult::Average(SingleMetricResult { value })
        | MetricResult::Sum(SingleMetricResult { value })
        | MetricResult::Min(SingleMetricResult { value })
        | MetricResult::Max(SingleMetricResult { value })
        | MetricResult::Count(SingleMetricResult { value })
        | MetricResult::Cardinality(SingleMetricResult { value }) => *value,

        MetricResult::Stats(stats) => {
            // Schema columns: {name}_min, {name}_max, {name}_sum, {name}_count, {name}_avg
            let suffix = col_name
                .strip_prefix(&format!("{agg_name}_"))
                .unwrap_or(col_name);
            match suffix {
                "min" => stats.min,
                "max" => stats.max,
                "sum" => Some(stats.sum),
                "count" => Some(stats.count as f64),
                "avg" => stats.avg,
                _ => None,
            }
        }

        MetricResult::ExtendedStats(es) => {
            let suffix = col_name
                .strip_prefix(&format!("{agg_name}_"))
                .unwrap_or(col_name);
            match suffix {
                "min" => es.min,
                "max" => es.max,
                "sum" => Some(es.sum),
                "count" => Some(es.count as f64),
                "avg" => es.avg,
                "variance_population" => es.variance_population,
                "std_deviation_population" => es.std_deviation_population,
                _ => None,
            }
        }

        MetricResult::Percentiles(p) => extract_percentile_value(p, agg_name, col_name),

        _ => None,
    }
}

fn extract_percentile_value(
    p: &PercentilesMetricResult,
    agg_name: &str,
    col_name: &str,
) -> Option<f64> {
    // Column names like "{name}_p1", "{name}_p50", etc.
    let suffix = col_name.strip_prefix(&format!("{agg_name}_p"))?;
    match &p.values {
        tantivy::aggregation::metric::PercentileValues::Vec(entries) => {
            for entry in entries {
                let key_str = if entry.key == entry.key.floor() {
                    format!("{}", entry.key as i64)
                } else {
                    format!("{}", entry.key)
                };
                if key_str == suffix {
                    return if entry.value.is_nan() {
                        None
                    } else {
                        Some(entry.value)
                    };
                }
            }
            None
        }
        tantivy::aggregation::metric::PercentileValues::HashMap(map) => map.get(suffix).copied(),
    }
}

/// Convert an Option<f64> to a single-element Arrow array of the target type.
fn scalar_to_array(value: Option<f64>, data_type: &DataType) -> ArrayRef {
    match data_type {
        DataType::Float64 => Arc::new(Float64Array::from(vec![value])),
        DataType::Int64 => Arc::new(Int64Array::from(vec![value.map(|v| v as i64)])),
        DataType::UInt64 => Arc::new(UInt64Array::from(vec![value.map(|v| v as u64)])),
        // Fallback: use Float64
        _ => Arc::new(Float64Array::from(vec![value])),
    }
}

/// Iterate over the entries in a `BucketEntries` enum.
///
/// `BucketEntries::iter()` is `pub(crate)` in upstream tantivy, so we
/// pattern-match on the public enum variants directly.
fn bucket_entries_iter<T>(entries: &BucketEntries<T>) -> Box<dyn Iterator<Item = &T> + '_> {
    match entries {
        BucketEntries::Vec(vec) => Box::new(vec.iter()),
        BucketEntries::HashMap(map) => Box::new(map.values()),
    }
}

/// Convert a bucket result to an N-row RecordBatch.
fn bucket_to_batch(
    bucket: &BucketResult,
    agg_def: &Aggregation,
    schema: &SchemaRef,
) -> Result<RecordBatch> {
    match bucket {
        BucketResult::Terms {
            buckets,
            sum_other_doc_count: _,
            ..
        } => terms_bucket_to_batch(buckets, agg_def, schema),
        BucketResult::Histogram { buckets } => {
            let entries: Vec<&BucketEntry> = bucket_entries_iter(buckets).collect();
            histogram_bucket_to_batch(&entries, agg_def, schema)
        }
        BucketResult::Range { buckets } => {
            let entries: Vec<&RangeBucketEntry> = bucket_entries_iter(buckets).collect();
            range_bucket_to_batch(&entries, agg_def, schema)
        }
        _ => Err(DataFusionError::Internal(
            "unsupported bucket type for agg pushdown".into(),
        )),
    }
}

fn terms_bucket_to_batch(
    buckets: &[BucketEntry],
    agg_def: &Aggregation,
    schema: &SchemaRef,
) -> Result<RecordBatch> {
    let mut columns: Vec<ArrayRef> = Vec::with_capacity(schema.fields().len());
    let row_count = buckets.len();

    for field in schema.fields() {
        let col_name = field.name().as_str();

        if is_group_key_column(col_name, agg_def) {
            columns.push(key_column_from_iter(
                buckets.iter().map(|bucket| Some(&bucket.key)),
                row_count,
                field.data_type(),
            ));
        } else if is_doc_count_column(col_name, &agg_def.sub_aggregation) {
            // doc_count: maps to the bucket's document count.
            // Matches explicit "doc_count" or COUNT(*) columns (e.g.
            // "count(Int64(1))") that have no corresponding sub-aggregation.
            columns.push(doc_count_column(
                buckets.iter().map(|bucket| bucket.doc_count),
                row_count,
            )?);
        } else {
            columns.push(typed_f64_column_from_iter(
                buckets.iter().map(|bucket| {
                    extract_sub_agg_value(
                        &bucket.sub_aggregation,
                        col_name,
                        &agg_def.sub_aggregation,
                    )
                }),
                row_count,
                field.data_type(),
            ));
        }
    }

    RecordBatch::try_new(schema.clone(), columns)
        .map_err(|e| DataFusionError::Internal(format!("build terms batch: {e}")))
}

fn terms_bucket_to_partial_state_batch(
    buckets: &[BucketEntry],
    agg_def: &Aggregation,
    schema: &SchemaRef,
) -> Result<RecordBatch> {
    if schema.fields().is_empty() {
        return Err(DataFusionError::Internal(
            "partial state schema must contain at least the group key".into(),
        ));
    }

    let mut columns: Vec<ArrayRef> = Vec::with_capacity(schema.fields().len());
    let row_count = buckets.len();
    columns.push(key_column_from_iter(
        buckets.iter().map(|bucket| Some(&bucket.key)),
        row_count,
        schema.fields()[0].data_type(),
    ));

    for field in schema.fields().iter().skip(1) {
        let col_name = field.name().as_str();
        if is_doc_count_column(col_name, &agg_def.sub_aggregation) {
            columns.push(typed_f64_column_from_iter(
                buckets.iter().map(|bucket| Some(bucket.doc_count as f64)),
                row_count,
                field.data_type(),
            ));
            continue;
        }

        columns.push(typed_f64_column_from_iter(
            buckets.iter().map(|bucket| {
                extract_sub_agg_value(&bucket.sub_aggregation, col_name, &agg_def.sub_aggregation)
            }),
            row_count,
            field.data_type(),
        ));
    }

    RecordBatch::try_new(schema.clone(), columns)
        .map_err(|e| DataFusionError::Internal(format!("build partial terms batch: {e}")))
}

fn histogram_bucket_to_batch(
    buckets: &[&BucketEntry],
    agg_def: &Aggregation,
    schema: &SchemaRef,
) -> Result<RecordBatch> {
    let mut columns: Vec<ArrayRef> = Vec::with_capacity(schema.fields().len());
    let row_count = buckets.len();

    for field in schema.fields() {
        let col_name = field.name().as_str();

        if col_name == "bucket" {
            columns.push(typed_f64_column_from_iter(
                buckets.iter().map(|bucket| key_to_f64(&bucket.key)),
                row_count,
                field.data_type(),
            ));
        } else if is_doc_count_column(col_name, &agg_def.sub_aggregation) {
            columns.push(doc_count_column(
                buckets.iter().map(|bucket| bucket.doc_count),
                row_count,
            )?);
        } else {
            columns.push(typed_f64_column_from_iter(
                buckets.iter().map(|bucket| {
                    extract_sub_agg_value(
                        &bucket.sub_aggregation,
                        col_name,
                        &agg_def.sub_aggregation,
                    )
                }),
                row_count,
                field.data_type(),
            ));
        }
    }

    RecordBatch::try_new(schema.clone(), columns)
        .map_err(|e| DataFusionError::Internal(format!("build histogram batch: {e}")))
}

fn range_bucket_to_batch(
    buckets: &[&RangeBucketEntry],
    agg_def: &Aggregation,
    schema: &SchemaRef,
) -> Result<RecordBatch> {
    let mut columns: Vec<ArrayRef> = Vec::with_capacity(schema.fields().len());
    let row_count = buckets.len();

    for field in schema.fields() {
        let col_name = field.name().as_str();

        if col_name == "bucket" {
            columns.push(string_key_column_from_iter(
                buckets.iter().map(|bucket| Some(key_as_str(&bucket.key))),
                row_count,
                field.data_type(),
            ));
        } else if is_doc_count_column(col_name, &agg_def.sub_aggregation) {
            columns.push(doc_count_column(
                buckets.iter().map(|bucket| bucket.doc_count),
                row_count,
            )?);
        } else {
            columns.push(typed_f64_column_from_iter(
                buckets.iter().map(|bucket| {
                    extract_sub_agg_value(
                        &bucket.sub_aggregation,
                        col_name,
                        &agg_def.sub_aggregation,
                    )
                }),
                row_count,
                field.data_type(),
            ));
        }
    }

    RecordBatch::try_new(schema.clone(), columns)
        .map_err(|e| DataFusionError::Internal(format!("build range batch: {e}")))
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Check if a column should be filled with the bucket's `doc_count`.
///
/// Returns true when:
/// - `col_name` is literally `"doc_count"`, or
/// - `col_name` looks like a `COUNT(*)` / `COUNT(1)` expression (starts with
///   `"count("` case-insensitively) and there is no sub-aggregation with that
///   exact name.
///
/// This bridges the naming mismatch between tantivy's `doc_count` and
/// DataFusion's generated column names like `count(Int64(1))`.
fn is_doc_count_column(col_name: &str, sub_aggs: &Aggregations) -> bool {
    if col_name == "doc_count" {
        return true;
    }
    // COUNT(*) in DataFusion becomes "count(Int64(1))" or similar.
    // If it starts with "count(" and is not a named sub-aggregation, treat it
    // as doc_count.
    if col_name.starts_with("count(") || col_name.starts_with("COUNT(") {
        return sub_aggs.get(col_name).is_none();
    }
    false
}

/// Check if a column name is the GROUP BY key for this aggregation.
fn is_group_key_column(col_name: &str, agg_def: &Aggregation) -> bool {
    match &agg_def.agg {
        AggregationVariants::Terms(t) => col_name == t.field,
        AggregationVariants::Histogram(_) => col_name == "bucket",
        AggregationVariants::DateHistogram(_) => col_name == "bucket",
        AggregationVariants::Range(_) => col_name == "bucket",
        _ => false,
    }
}

fn key_to_f64(key: &Key) -> Option<f64> {
    match key {
        Key::F64(v) => Some(*v),
        Key::I64(v) => Some(*v as f64),
        Key::U64(v) => Some(*v as f64),
        Key::Str(s) => s.parse::<f64>().ok(),
    }
}

fn key_to_i64(key: &Key) -> Option<i64> {
    match key {
        Key::I64(v) => Some(*v),
        Key::U64(v) => i64::try_from(*v).ok(),
        Key::F64(v) if v.fract() == 0.0 && *v >= i64::MIN as f64 && *v <= i64::MAX as f64 => {
            Some(*v as i64)
        }
        Key::F64(_) => None,
        Key::Str(s) => s.parse::<i64>().ok(),
    }
}

fn key_to_u64(key: &Key) -> Option<u64> {
    match key {
        Key::U64(v) => Some(*v),
        Key::I64(v) => u64::try_from(*v).ok(),
        Key::F64(v) if v.fract() == 0.0 && *v >= 0.0 && *v <= u64::MAX as f64 => Some(*v as u64),
        Key::F64(_) => None,
        Key::Str(s) => s.parse::<u64>().ok(),
    }
}

fn key_to_bool(key: &Key) -> Option<bool> {
    match key {
        Key::Str(s) => Some(s == "true" || s == "1"),
        Key::I64(v) => Some(*v == 1),
        Key::U64(v) => Some(*v == 1),
        Key::F64(v) => Some(*v == 1.0),
    }
}

fn key_as_str(key: &Key) -> &str {
    match key {
        Key::Str(s) => s.as_str(),
        _ => "",
    }
}

fn append_key_string(builder: &mut StringBuilder, key: &Key) {
    match key {
        Key::Str(s) => builder.append_value(s),
        Key::F64(v) => builder.append_value(v.to_string()),
        Key::I64(v) => builder.append_value(v.to_string()),
        Key::U64(v) => builder.append_value(v.to_string()),
    }
}

fn key_string_array_from_iter<'a>(
    values: impl Iterator<Item = Option<&'a Key>>,
    row_count: usize,
) -> ArrayRef {
    let mut builder = StringBuilder::with_capacity(row_count, row_count * 16);
    for value in values {
        match value {
            Some(key) => append_key_string(&mut builder, key),
            None => builder.append_null(),
        }
    }
    Arc::new(builder.finish())
}

fn string_array_from_iter<'a>(
    values: impl Iterator<Item = Option<&'a str>>,
    row_count: usize,
) -> ArrayRef {
    let mut builder = StringBuilder::with_capacity(row_count, row_count * 16);
    for value in values {
        match value {
            Some(value) => builder.append_value(value),
            None => builder.append_null(),
        }
    }
    Arc::new(builder.finish())
}

fn key_column_from_iter<'a>(
    values: impl Iterator<Item = Option<&'a Key>>,
    row_count: usize,
    data_type: &DataType,
) -> ArrayRef {
    match data_type {
        DataType::Utf8 => key_string_array_from_iter(values, row_count),
        DataType::Utf8View => {
            let string_arr = key_string_array_from_iter(values, row_count);
            arrow::compute::cast(&string_arr, data_type).unwrap_or(string_arr)
        }
        DataType::Dictionary(_, _) => {
            let string_arr = key_string_array_from_iter(values, row_count);
            arrow::compute::cast(&string_arr, data_type).unwrap_or(string_arr)
        }
        DataType::List(inner)
            if matches!(inner.data_type(), DataType::Utf8 | DataType::Utf8View) =>
        {
            let list_arr = keys_to_list(values, row_count, inner);
            if list_arr.data_type() == data_type {
                list_arr
            } else {
                arrow::compute::cast(&list_arr, data_type).unwrap_or(list_arr)
            }
        }
        DataType::Float64 => {
            let mut builder = Float64Builder::with_capacity(row_count);
            for value in values {
                append_optional_f64(&mut builder, value.and_then(key_to_f64));
            }
            Arc::new(builder.finish())
        }
        DataType::Int64 => {
            let mut builder = Int64Builder::with_capacity(row_count);
            for value in values {
                append_optional_i64(&mut builder, value.and_then(key_to_i64));
            }
            Arc::new(builder.finish())
        }
        DataType::UInt64 => {
            let mut builder = UInt64Builder::with_capacity(row_count);
            for value in values {
                append_optional_u64(&mut builder, value.and_then(key_to_u64));
            }
            Arc::new(builder.finish())
        }
        DataType::Boolean => {
            let mut builder = BooleanBuilder::with_capacity(row_count);
            for value in values {
                append_optional_bool(&mut builder, value.and_then(key_to_bool));
            }
            Arc::new(builder.finish())
        }
        _ => key_string_array_from_iter(values, row_count),
    }
}

fn string_key_column_from_iter<'a>(
    values: impl Iterator<Item = Option<&'a str>>,
    row_count: usize,
    data_type: &DataType,
) -> ArrayRef {
    match data_type {
        DataType::Utf8 => string_array_from_iter(values, row_count),
        DataType::Utf8View => {
            let string_arr = string_array_from_iter(values, row_count);
            arrow::compute::cast(&string_arr, data_type).unwrap_or(string_arr)
        }
        DataType::Dictionary(_, _) => {
            let string_arr = string_array_from_iter(values, row_count);
            arrow::compute::cast(&string_arr, data_type).unwrap_or(string_arr)
        }
        DataType::List(inner)
            if matches!(inner.data_type(), DataType::Utf8 | DataType::Utf8View) =>
        {
            let list_arr = strings_to_list(values, row_count, inner);
            if list_arr.data_type() == data_type {
                list_arr
            } else {
                arrow::compute::cast(&list_arr, data_type).unwrap_or(list_arr)
            }
        }
        DataType::Float64 => {
            let mut builder = Float64Builder::with_capacity(row_count);
            for value in values {
                append_optional_f64(&mut builder, value.and_then(|s| s.parse::<f64>().ok()));
            }
            Arc::new(builder.finish())
        }
        DataType::Int64 => {
            let mut builder = Int64Builder::with_capacity(row_count);
            for value in values {
                append_optional_i64(&mut builder, value.and_then(|s| s.parse::<i64>().ok()));
            }
            Arc::new(builder.finish())
        }
        DataType::UInt64 => {
            let mut builder = UInt64Builder::with_capacity(row_count);
            for value in values {
                append_optional_u64(&mut builder, value.and_then(|s| s.parse::<u64>().ok()));
            }
            Arc::new(builder.finish())
        }
        DataType::Boolean => {
            let mut builder = BooleanBuilder::with_capacity(row_count);
            for value in values {
                append_optional_bool(&mut builder, value.map(|s| s == "true" || s == "1"));
            }
            Arc::new(builder.finish())
        }
        _ => string_array_from_iter(values, row_count),
    }
}

fn keys_to_list<'a>(
    values: impl Iterator<Item = Option<&'a Key>>,
    row_count: usize,
    item_field: &Arc<arrow::datatypes::Field>,
) -> ArrayRef {
    let mut builder = ListBuilder::new(StringBuilder::with_capacity(row_count, row_count * 16));
    if matches!(item_field.data_type(), DataType::Utf8) {
        builder = builder.with_field(Arc::clone(item_field));
    }

    for value in values {
        match value {
            Some(key) => {
                append_key_string(builder.values(), key);
                builder.append(true);
            }
            None => builder.append(false),
        }
    }
    Arc::new(builder.finish())
}

fn strings_to_list<'a>(
    values: impl Iterator<Item = Option<&'a str>>,
    row_count: usize,
    item_field: &Arc<arrow::datatypes::Field>,
) -> ArrayRef {
    let mut builder = ListBuilder::new(StringBuilder::with_capacity(row_count, row_count * 16));
    if matches!(item_field.data_type(), DataType::Utf8) {
        builder = builder.with_field(Arc::clone(item_field));
    }

    for value in values {
        match value {
            Some(value) => {
                builder.values().append_value(value);
                builder.append(true);
            }
            None => builder.append(false),
        }
    }
    Arc::new(builder.finish())
}

fn doc_count_column(counts: impl Iterator<Item = u64>, row_count: usize) -> Result<ArrayRef> {
    let mut builder = Int64Builder::with_capacity(row_count);
    for count in counts {
        let count = i64::try_from(count)
            .map_err(|_| DataFusionError::Internal(format!("doc_count {count} exceeds i64")))?;
        builder.append_value(count);
    }
    Ok(Arc::new(builder.finish()))
}

fn append_optional_f64(builder: &mut Float64Builder, value: Option<f64>) {
    match value {
        Some(value) => builder.append_value(value),
        None => builder.append_null(),
    }
}

fn append_optional_i64(builder: &mut Int64Builder, value: Option<i64>) {
    match value {
        Some(value) => builder.append_value(value),
        None => builder.append_null(),
    }
}

fn append_optional_u64(builder: &mut UInt64Builder, value: Option<u64>) {
    match value {
        Some(value) => builder.append_value(value),
        None => builder.append_null(),
    }
}

fn append_optional_bool(builder: &mut BooleanBuilder, value: Option<bool>) {
    match value {
        Some(value) => builder.append_value(value),
        None => builder.append_null(),
    }
}

fn typed_f64_column_from_iter(
    values: impl Iterator<Item = Option<f64>>,
    row_count: usize,
    data_type: &DataType,
) -> ArrayRef {
    match data_type {
        DataType::Float64 => {
            let mut builder = Float64Builder::with_capacity(row_count);
            for value in values {
                append_optional_f64(&mut builder, value);
            }
            Arc::new(builder.finish())
        }
        DataType::Int64 => {
            let mut builder = Int64Builder::with_capacity(row_count);
            for value in values {
                append_optional_i64(&mut builder, value.map(|f| f as i64));
            }
            Arc::new(builder.finish())
        }
        DataType::UInt64 => {
            let mut builder = UInt64Builder::with_capacity(row_count);
            for value in values {
                append_optional_u64(&mut builder, value.map(|f| f as u64));
            }
            Arc::new(builder.finish())
        }
        _ => {
            let mut builder = Float64Builder::with_capacity(row_count);
            for value in values {
                append_optional_f64(&mut builder, value);
            }
            Arc::new(builder.finish())
        }
    }
}

/// Extract a metric value from sub-aggregation results for a given column name.
fn extract_sub_agg_value(
    sub_agg_results: &AggregationResults,
    col_name: &str,
    sub_agg_defs: &Aggregations,
) -> Option<f64> {
    // Try direct match: col_name is a sub-agg key
    if let Some(AggregationResult::MetricResult(metric)) = sub_agg_results.0.get(col_name) {
        return extract_simple_metric_value(metric);
    }

    // Try prefix match for stats-like aggs: col_name = "{sub_agg_name}_{suffix}"
    for (sub_name, _sub_def) in sub_agg_defs.iter() {
        if let Some(suffix) = col_name.strip_prefix(&format!("{sub_name}_")) {
            if let Some(AggregationResult::MetricResult(metric)) = sub_agg_results.0.get(sub_name) {
                return extract_stats_metric_value(metric, suffix);
            }
        }
    }

    None
}

fn extract_simple_metric_value(metric: &MetricResult) -> Option<f64> {
    match metric {
        MetricResult::Average(m)
        | MetricResult::Sum(m)
        | MetricResult::Min(m)
        | MetricResult::Max(m)
        | MetricResult::Count(m)
        | MetricResult::Cardinality(m) => m.value,
        MetricResult::Stats(s) => s.avg, // fallback for direct access
        _ => None,
    }
}

fn extract_stats_metric_value(metric: &MetricResult, suffix: &str) -> Option<f64> {
    match metric {
        MetricResult::Stats(s) => match suffix {
            "min" => s.min,
            "max" => s.max,
            "sum" => Some(s.sum),
            "count" => Some(s.count as f64),
            "avg" => s.avg,
            _ => None,
        },
        MetricResult::ExtendedStats(es) => match suffix {
            "min" => es.min,
            "max" => es.max,
            "sum" => Some(es.sum),
            "count" => Some(es.count as f64),
            "avg" => es.avg,
            "variance_population" => es.variance_population,
            "std_deviation_population" => es.std_deviation_population,
            _ => None,
        },
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Array, ListArray, StringArray};

    #[test]
    fn cast_key_column_respects_utf8_view_schema() {
        let values = [Some("api".to_string()), Some("web".to_string())];
        let array = string_key_column_from_iter(
            values.iter().map(|value| value.as_deref()),
            values.len(),
            &DataType::Utf8View,
        );
        assert_eq!(array.data_type(), &DataType::Utf8View);
    }

    #[test]
    fn cast_key_column_wraps_strings_for_list_schema() {
        let values = [Some("api".to_string()), None, Some("web".to_string())];
        let data_type = DataType::new_list(DataType::Utf8, true);
        let array = string_key_column_from_iter(
            values.iter().map(|value| value.as_deref()),
            values.len(),
            &data_type,
        );
        assert_eq!(array.data_type(), &data_type);

        let list = array.as_any().downcast_ref::<ListArray>().unwrap();
        let strings = list
            .values()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(list.value_offsets(), &[0, 1, 1, 2]);
        assert_eq!(strings.value(0), "api");
        assert!(list.is_null(1));
        assert_eq!(strings.value(1), "web");
    }
}
