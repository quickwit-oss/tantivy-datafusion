# Nested Top-K Aggregations

This document defines a `NestedTopKAgg` operator for DataFusion that
handles ES-style nested bucket sub-aggregation trees, with distributed
partial/final execution and optional tantivy pushdown.

Companion document: `ESCompatDF.md` describes the ES-compatible
semantic contract in detail. This document describes the operator
implementation.

## Terminology

Elasticsearch uses "nested aggregation" to mean aggregation over
`nested` documents via a `path`. That is not what this operator does.

This operator models nested bucket sub-aggregation trees — what ES
calls sub-aggregations. "Top K services, within each top N endpoints,
within each top M error types" with metrics at every level.

This document uses "nested bucket tree" or "sub-aggregation tree"
throughout.

## Problem

ES nested bucket sub-aggregation trees have no equivalent in SQL
engines. The standard SQL approach — flat GROUP BY on the composite
key, then window functions for per-level top-K pruning — requires
multiple passes, transfers the full cartesian product of group keys
between workers and coordinator, and cannot compute parent-level
metrics accurately (parent metrics cover ALL children, not just
surviving ones).

No open-source SQL/DataFrame engine has a nested top-K aggregation
operator. DataFusion, ClickHouse, DuckDB, Velox, CockroachDB, TiDB,
and Polars all have single-level top-K only. The only open-source
implementations of nested bucket aggregation live in search engines
(Elasticsearch, tantivy/Quickwit). Kusto's `top-nested` operator has
the right semantics but the execution engine is proprietary.

## Goals

- A generic DataFusion operator that works on any Arrow input (parquet,
  CSV, tantivy, anything).
- Two explicit execution contracts: `Exact` for generic sources and
  `EsCompatApproxLeaf` for tantivy pushdown. See Semantic Contracts.
- Tantivy pushdown as an optimization: tantivy's
  `DistributedAggregationCollector` provides the ES-compatible leaf
  partial. Its `IntermediateAggregationResults` tree is flattened into
  the shared intermediate RecordBatch schema. The flattening copies key
  strings and serializes metric state into Arrow buffers — O(tree size),
  not O(data size).
- Performance matching or beating Elasticsearch on equivalent queries.
- Output structurally matching Elasticsearch's nested JSON aggregation
  response.
- Distributed execution with bounded intermediate transfer.

## Non-Goals

- SQL syntax for nested aggregations. This operator is reached via
  Substrait extensions or direct logical plan construction.
- Replacing tantivy's existing `AggregationCollector`. The pushdown
  delegates to it.
- Mid-stream lossy eviction during accumulation. See Memory Model.

## Semantic Contracts

The operator supports two execution contracts. The choice is explicit
in the spec and planner-visible.

```rust
pub enum AggSemantics {
    /// Full bucket membership is exact. No candidate pruning during
    /// accumulation. Algebraic metrics are exact. Sketch metrics
    /// (percentiles, cardinality) remain approximate by construction.
    /// No ES error metadata is emitted.
    Exact,
    /// ES-compatible leaf approximation. Candidate-based levels (terms)
    /// prune to shard_size at the leaf emit boundary. Exhaustive levels
    /// (date_histogram, range) remain exhaustive. ES error metadata
    /// (sum_other_doc_count, doc_count_error_upper_bound) is carried
    /// in the intermediate format and merged by the final reducer.
    EsCompatApproxLeaf,
}
```

### Why two contracts

ES aggregations are not uniformly approximate. Approximation has two
independent sources:

1. **Bucket selection.** `terms` is candidate-based: each leaf keeps
   its local top `shard_size` and ships those. The coordinator sees
   the union of candidates, not the full set. `date_histogram` and
   `range` are exhaustive. This is per-level, not per-query.

2. **Metric computation.** `sum`, `avg`, `min`, `max`, `count` are
   algebraic: partial states merge exactly. `percentiles` (t-digest)
   and `cardinality` (HLL) are sketch-based: inherently approximate
   regardless of bucket membership.

These are orthogonal. Even with exhaustive bucketing, percentiles are
approximate. Even with candidate-based bucketing, sum is exact within
the surviving buckets.

The Exact contract eliminates source 1 (all buckets are kept). The
EsCompatApproxLeaf contract matches ES's distributed behavior where
each shard independently decides which terms buckets to keep.

### What "ES approximate" actually means

ES is not globally approximate. Within each shard, the aggregation is
exact over the documents it sees. The approximation enters at the
shard boundary:

```
Shard 1: A=100, X=99   → sends A (X pruned)
Shard 2: B=100, X=99   → sends B (X pruned)
Shard 3: X=99,  C=98   → sends X (C pruned)

Coordinator sees: A=100, B=100, X=99
True global:      X=297
```

This is why ES exposes:
- `shard_size` (default: `size * 1.5 + 10`): how many candidates each
  shard keeps.
- `sum_other_doc_count`: total count of buckets discarded at this
  level. Tracked from discarded buckets, not inferred from parent count,
  because docs missing the field are excluded and some bucket types can
  count a doc in multiple buckets.
- `doc_count_error_upper_bound`: for `_count desc` ordering only, the
  sum across shards of the count of each shard's `(shard_size + 1)`-th
  bucket (the largest bucket each shard pruned). Not defined for other
  orderings.

Sub-aggregations under an approximate terms level inherit that
approximation: if a parent bucket was omitted by some shards, the
child metrics are computed only over the shards that kept the parent.

### Approximation is per-level

In a nested tree, level 0 might be `terms` (candidate-based) and
level 1 might be `date_histogram` (exhaustive). The intermediate
format carries per-level metadata so the final reducer knows which
levels have error bounds.

## Data Model

### Spec

```rust
pub struct NestedTopKAggSpec {
    pub semantics: AggSemantics,
    pub levels: Vec<AggLevel>,
}

pub struct AggLevel {
    /// ES aggregation name (e.g., "top_services"). Used to reconstruct
    /// the ES JSON response tree and to reference metrics in AggOrder.
    pub name: String,
    pub group_expr: Expr,
    pub bucket_type: BucketType,
    pub top_k: usize,
    /// Only meaningful for EsCompatApproxLeaf on candidate-based levels.
    /// Ignored for Exact semantics (all buckets kept).
    /// Ignored for exhaustive levels (date_histogram, range).
    /// Default: top_k * 1.5 + 10 (matches ES default).
    pub shard_size: Option<usize>,
    pub order_by: AggOrder,
    pub metrics: Vec<MetricSpec>,
}

pub enum BucketType {
    /// Candidate-based: each leaf keeps top shard_size.
    Terms,
    /// Exhaustive: every matched doc maps to exactly one bucket.
    DateHistogram { interval: Duration },
    /// Exhaustive: every matched doc maps to exactly one bucket.
    Range { ranges: Vec<(Option<f64>, Option<f64>)> },
}

pub enum AggOrder {
    /// Normal top-k mode. Bounded error in distributed mode.
    CountDesc,
    /// Discouraged by ES: often gives inaccurate results in
    /// distributed mode. Supported but error metadata is not defined.
    CountAsc,
    /// Safe only for max-desc and min-asc per ES docs. Other metric
    /// orderings are supported but error metadata is not defined.
    Metric { name: String, dir: SortDir },
    /// Exact and safe. Buckets ordered by key value.
    KeyAsc,
    KeyDesc,
}

pub struct MetricSpec {
    pub name: String,
    pub func: MetricFunc,
    pub field: Expr,
}

pub enum MetricFunc {
    Sum,
    Avg,
    Min,
    Max,
    Count,
    Cardinality,
    Percentiles { percents: Vec<f64> },
}
```

### Intermediate RecordBatch Schema

This is the contract between Partial and Final. Both the Exact generic
path and the EsCompatApproxLeaf tantivy path produce batches in this
schema. The schema is derived deterministically from the spec.

For a spec with levels `[service(top50, avg(latency)), endpoint(top20,
p99(latency)), error_type(top10)]`:

```
__level:                    UInt8     — which level this row represents (0, 1, 2)
__count:                    UInt64    — doc count for this bucket
__bucket_semantics:         UInt8     — 0=exact, 1=candidate_terms
__sum_other_doc_count:      UInt64    — nullable. NULL for exact/exhaustive levels.
__doc_count_error:          UInt64    — nullable. NULL for exact/exhaustive levels
                                        and for orderings where it is not defined.
service:                    Utf8      — set for all rows
endpoint:                   Utf8      — NULL for level=0 rows
error_type:                 Utf8      — NULL for level=0 and level=1 rows
__m0_avg_sum:               Float64   — level-0 metric partial state (nullable)
__m0_avg_count:             UInt64    — level-0 metric partial state (nullable)
__m1_p99_digest:            Binary    — level-1 metric partial state (nullable)
```

The `__level` column is critical. A level-0 row for `service=auth`
carries `__count=50000` and `__m0_avg_sum/count` computed over ALL
50000 docs in that service — not just docs in surviving child buckets.
Parent-level metrics are accurate over the full partition data even
though child buckets may be pruned at the leaf boundary.

`__bucket_semantics` is set per level:
- `exact` for Exact semantics or exhaustive bucket types
  (date_histogram, range).
- `candidate_terms` for EsCompatApproxLeaf on terms levels.

`__sum_other_doc_count` and `__doc_count_error` are populated only
for `candidate_terms` rows. The final reducer sums `__sum_other_doc_count`
across partitions and computes the final `doc_count_error_upper_bound`
per ES rules. Both are NULL for exact/exhaustive levels.

Example intermediate batch from one partition (EsCompatApproxLeaf):

```
__level  __bucket_sem  __sum_other  __doc_err  service   endpoint  error_type  __count  ...
0        candidate     1200         50         auth      NULL      NULL        50000    ...
0        candidate     800          30         billing   NULL      NULL        40000    ...
1        candidate     500          20         auth      /login    NULL        30000    ...
2        exact         NULL         NULL       auth      /login    timeout     8000     ...
```

Row count per partition is bounded by:
- **EsCompatApproxLeaf:** sum across levels of shard_size products.
  With [75+10, 30+10, 15+10] (ES defaults for top_k [50, 20, 10]):
  `85 + 85*40 + 85*40*25 = 85 + 3,400 + 85,000 ≈ 89K` rows.
- **Exact:** sum across levels of actual cardinality products. Bounded
  by partition data, not by spec. Could be large for high-cardinality
  terms.

### Output RecordBatch Schema

The output schema replaces partial state columns with resolved final
value columns:

- `__level`, `__count`, group key columns: same as intermediate.
- `__sum_other_doc_count`, `__doc_count_error`: carried through for ES
  output formatting (NULL for exact levels).
- Metric columns resolved:

| MetricFunc    | Intermediate Columns          | Output Columns                          |
|---------------|-------------------------------|-----------------------------------------|
| Sum           | `_sum: Float64`               | `Float64`                               |
| Avg           | `_sum: Float64, _count: U64`  | `Float64` (the resolved average)        |
| Min           | `_min: Float64`               | `Float64`                               |
| Max           | `_max: Float64`               | `Float64`                               |
| Count         | `_count: UInt64`              | `UInt64`                                |
| Cardinality   | `_hll: Binary`                | `UInt64` (estimated distinct count)     |
| Percentiles   | `_digest: Binary`             | One `Float64` column per requested      |
|               |                               | percentile, named `_p{N}` (e.g.,       |
|               |                               | `__m1_p99_latency_p99: Float64`)        |

Column naming: `__m{L}_{metric_name}_{state_field}` for intermediate,
`__m{L}_{metric_name}` or `__m{L}_{metric_name}_p{N}` for output.

Pruned to `top_k` at each level. `__level` retained for API-layer
tree reconstruction.

## Memory Model

There is no mid-stream lossy eviction in either contract.

### Exact contract

The accumulator builds the full tree for the partition. Memory is
O(actual cardinality product). For a 10M-doc split with typical
observability data (1K services × 50 endpoints/service × 20 error
types/endpoint = 1M nodes), this is manageable.

If the tree exceeds a configurable memory budget, the query fails with
a clear error. This is the same class of tradeoff as DataFusion's hash
aggregation: succeed if memory fits, fail if it doesn't. Spill to disk
can be added later as an optimization without changing semantics.

### EsCompatApproxLeaf contract

When tantivy pushdown fires, tantivy's collector manages its own
memory. Each split builds the full tree per-segment, prunes to
`shard_size` at segment boundaries, and returns
`IntermediateAggregationResults`. Memory is bounded by the shard_size
product.

When tantivy pushdown does not fire (generic Arrow source with
EsCompatApproxLeaf semantics), the generic accumulator builds the full
tree and prunes candidate-based levels to `shard_size` at emit time.
Same memory profile as the Exact contract during accumulation, bounded
at emit.

### Coordinator memory

The streaming final merge folds each intermediate batch into one
accumulator tree. Peak memory is one merged tree plus one intermediate
batch in flight.

The merged tree is bounded by `min(N_partitions * shard_size,
total_distinct_keys)` at each level. With 100 partitions each sending
85 distinct services (ES default shard_size for top_k=50), the merged
tree could have up to 8,500 service entries if all are disjoint.
Realistic data has heavy overlap (the top services appear in every
partition), so the tree is typically close to shard_size. Worst case
for disjoint data with 100 partitions and default shard_size [85, 40,
25]:

- 8,500 level-0 nodes × ~300 bytes = 2.5MB
- 8,500 × 40 level-1 nodes × ~300 bytes = 100MB (worst case)
- 8,500 × 40 × 25 level-2 nodes × ~60 bytes = 510MB (worst case)

This worst case is pathological (every partition has completely
different services). For real observability data, the merged tree is
~10-100MB.

## NestedAccumulator

The in-memory tree used by the generic Partial executor (both Exact
and EsCompatApproxLeaf when pushdown does not fire). One per partition.

```rust
pub struct NestedAccumulator {
    spec: Arc<NestedTopKAggSpec>,
    root: HashMap<BucketKey, AccNode>,
}

pub struct AccNode {
    count: u64,
    metrics: Vec<MetricAccumulator>,
    children: Option<HashMap<BucketKey, AccNode>>,
}

pub enum BucketKey {
    Str(CompactString),
    I64(i64),
    U64(u64),
    F64(OrderedFloat<f64>),
    Bool(bool),
    Bytes(Vec<u8>),
}

pub enum MetricAccumulator {
    Sum { sum: f64 },
    Avg { sum: f64, count: u64 },
    Min { min: f64 },
    Max { max: f64 },
    Count { count: u64 },
    Percentiles { digest: TDigest },
    Cardinality { hll: HyperLogLog },
}
```

### Accumulation

For each input RecordBatch:

1. Pre-resolve column references: extract the Arrow array for each
   level's `group_expr` and each metric's `field`. O(1) per batch.
2. For each row, walk the tree:
   - Level 0: hash lookup on `group_expr_0(row)`, increment count,
     update level-0 metrics.
   - Level 1: hash lookup on `group_expr_1(row)` in level-0's children,
     increment count, update level-1 metrics.
   - Level N: same pattern.
3. `BucketType` determines how the key is derived:
   - `Terms`: raw value from the column.
   - `DateHistogram`: `truncate_to_interval(timestamp_value, interval)`.
   - `Range`: binary search in the range list to find the bucket index.

No pruning during accumulation. The tree grows to the full cardinality
of the partition. Memory checks happen per-batch against the configured
budget.

### Emit to Intermediate RecordBatch

After consuming all input:

1. For **EsCompatApproxLeaf**: prune candidate-based levels (terms) to
   `shard_size`. Track `sum_other_doc_count` (sum of counts of pruned
   buckets) and `doc_count_error` (count of the `shard_size + 1`-th
   bucket, when ordering is `_count desc`). Exhaustive levels
   (date_histogram, range) are not pruned.
   For **Exact**: no pruning at emit. All buckets emitted.
2. Walk tree depth-first. At each node, emit a row at that node's level
   with the accumulated keys, count, metric state, and per-level
   metadata (`__bucket_semantics`, `__sum_other_doc_count`,
   `__doc_count_error`).
3. Level-0 rows carry accurate counts and metrics over ALL children,
   including those pruned. This is because `AccNode.count` and
   `AccNode.metrics` were updated for every row during accumulation,
   before any pruning.

### Ordering and Tie-Breaking

`compare_acc_nodes` implements bucket ordering for both the emit prune
and the final prune:

```rust
fn compare_acc_nodes(
    a: &AccNode,
    a_key: &BucketKey,
    b: &AccNode,
    b_key: &BucketKey,
    order_by: &AggOrder,
) -> std::cmp::Ordering {
    let primary = match order_by {
        AggOrder::CountDesc => b.count.cmp(&a.count),
        AggOrder::CountAsc => a.count.cmp(&b.count),
        AggOrder::Metric { name, dir } => {
            let a_val = a.metric_sort_value(name);
            let b_val = b.metric_sort_value(name);
            let ord = a_val.partial_cmp(&b_val)
                .unwrap_or(std::cmp::Ordering::Equal);
            match dir {
                SortDir::Asc => ord,
                SortDir::Desc => ord.reverse(),
            }
        }
        AggOrder::KeyAsc => a_key.cmp(b_key),
        AggOrder::KeyDesc => b_key.cmp(a_key),
    };
    // ES tie-breaking: secondary sort by key ascending.
    if primary == std::cmp::Ordering::Equal {
        a_key.cmp(b_key)
    } else {
        primary
    }
}
```

`metric_sort_value(name)` looks up the named metric and returns its
current scalar value (sum for Sum, sum/count for Avg, etc.). Percentiles
cannot be used as an ordering target — the spec should reference a
specific single-valued metric instead.

## Physical Executors

### NestedTopKAggExec

```rust
pub struct NestedTopKAggExec {
    input: Arc<dyn ExecutionPlan>,
    spec: Arc<NestedTopKAggSpec>,
    mode: AggMode,
}

pub enum AggMode {
    Partial,
    Final,
    Single,
}
```

The `spec.semantics` field determines behavior within each mode.

### Partial Mode

```
execute(partition) -> SendableRecordBatchStream:
    let input_stream = self.input.execute(partition);
    let mut acc = NestedAccumulator::new(&self.spec);

    while let Some(batch) = input_stream.next().await:
        acc.accumulate(&batch);
        acc.check_memory_budget()?;

    let intermediate = acc.emit_intermediate();
    yield intermediate;
```

Output: one intermediate RecordBatch per partition.

Output partitioning: same count as input. Each partition produces one
batch. `CoalescePartitionsExec` feeds all partitions into Final.

For EsCompatApproxLeaf: `emit_intermediate` prunes candidate-based
levels to `shard_size` and populates ES error metadata columns.

For Exact: `emit_intermediate` emits all buckets, error metadata
columns are NULL.

### Final Mode

The Final executor uses a streaming merge: it folds each incoming
intermediate batch into a single accumulator tree, then emits the
final output.

```
execute(0) -> SendableRecordBatchStream:
    let input_stream = self.input.execute(0);
    let mut acc = NestedAccumulator::new_for_merge(&self.spec);

    while let Some(batch) = input_stream.next().await:
        acc.merge_intermediate_batch(&batch);

    let output = acc.emit_final();
    yield output;
```

`merge_intermediate_batch` processes one intermediate batch:

1. Iterate rows. Read `__level` to determine which level each row
   belongs to.
2. For each row: walk down the tree to the appropriate node, merge
   `__count` (sum), merge metric partial states, merge
   `__sum_other_doc_count` (sum), merge `__doc_count_error` (sum for
   `_count desc` ordering).
3. Discard the batch.

Peak coordinator memory: one merged tree. See Memory Model for bounds.

`emit_final`:

1. Prune each level to `top_k` using `compare_acc_nodes` with
   tie-breaking.
2. Resolve final metric values: `avg = sum / count`,
   `percentile = digest.quantile(percents)`, `cardinality = hll.count()`.
3. Compute final ES metadata:
   - `sum_other_doc_count`: accumulated from leaf partials.
   - `doc_count_error_upper_bound`: accumulated from leaf partials,
     valid only for `_count desc`.
4. Walk tree depth-first, emitting rows in the output schema.

### Single Mode

For non-distributed execution. Same as Partial + Final in one pass:
accumulate, prune to `top_k`, resolve metrics. No intermediate format.

## Plan Shapes

### ES-compatible mode (tantivy pushdown fires)

```
NestedTopKAgg(spec, semantics=EsCompatApproxLeaf)
  → DataSourceExec(NestedAggTantivyDataSource)     // tantivy leaf
  → CoalescePartitions
  → NestedTopKAggExec(Final)
```

The tantivy data source uses `DistributedAggregationCollector` (not
`AggregationCollector`) to match Quickwit's split-level semantics.

### ES-compatible mode (pushdown does not fire)

```
NestedTopKAgg(spec, semantics=EsCompatApproxLeaf)
  → NestedTopKAggExec(Partial)                     // generic Arrow
  → CoalescePartitions
  → NestedTopKAggExec(Final)
```

The generic Partial executor builds the full tree and prunes
candidate-based levels to `shard_size` at emit.

### Exact mode

```
NestedTopKAgg(spec, semantics=Exact)
  → NestedTopKAggExec(Partial)                     // generic Arrow
  → CoalescePartitions
  → NestedTopKAggExec(Final)
```

No pruning at the partial. All buckets emitted. Final prunes to
`top_k`. No ES error metadata.

## Tantivy Pushdown

### Optimizer Rule: `NestedAggTantivyPushdown`

Fires after physical planning, before distributed partitioning. Only
fires when:
- `spec.semantics == EsCompatApproxLeaf`
- All `group_expr` are bare `Column` references (tantivy requires
  field names, not computed expressions)
- All metric `field` are bare `Column` references
- A `SingleTableDataSource` is beneath

Pattern:

```
NestedTopKAggExec(Partial)
  └── [Filter, Projection — safe intermediate ops]
        └── DataSourceExec(SingleTableDataSource)
```

Rewrites to:

```
DataSourceExec(NestedAggTantivyDataSource)
```

### Spec → Tantivy Aggregations Conversion

```rust
fn spec_to_tantivy_aggregations(spec: &NestedTopKAggSpec) -> Option<Aggregations> {
    fn convert(levels: &[AggLevel], depth: usize) -> Option<Aggregations> {
        if depth >= levels.len() {
            return Some(Aggregations::default());
        }
        let level = &levels[depth];

        let field_name = match &level.group_expr {
            Expr::Column(col) => col.name().to_string(),
            _ => return None,
        };

        let mut sub_aggs = convert(levels, depth + 1)?;
        for metric in &level.metrics {
            sub_aggs.insert(metric.name.clone(), metric_to_tantivy(metric)?);
        }

        let shard_size = level.shard_size
            .unwrap_or((level.top_k as f64 * 1.5 + 10.0) as usize);

        let bucket_agg = match &level.bucket_type {
            BucketType::Terms => AggregationVariants::Terms(TermsAggregation {
                field: field_name,
                size: Some(shard_size as u32),
                shard_size: Some(shard_size as u32),
                order: order_to_tantivy(&level.order_by),
                ..Default::default()
            }),
            BucketType::DateHistogram { interval } =>
                AggregationVariants::DateHistogram(DateHistogramAggregation {
                    field: field_name,
                    fixed_interval: Some(interval_to_string(interval)),
                    ..Default::default()
                }),
            BucketType::Range { ranges } =>
                AggregationVariants::Range(RangeAggregation {
                    field: field_name,
                    ranges: ranges_to_tantivy(ranges),
                    ..Default::default()
                }),
        };

        let mut aggs = Aggregations::default();
        aggs.insert(level.name.clone(), Aggregation {
            agg: bucket_agg,
            sub_aggregation: sub_aggs,
        });
        Some(aggs)
    }

    convert(&spec.levels, 0)
}
```

### Tantivy Output → Intermediate RecordBatch

After `DistributedAggregationCollector` runs on a split, it returns
`IntermediateAggregationResults`. The conversion walks the tree
depth-first, emitting rows at each level and preserving ES error
metadata:

```rust
fn walk(
    results: &IntermediateAggregationResults,
    spec: &NestedTopKAggSpec,
    depth: usize,
    parent_keys: &[BucketKey],
    builders: &mut IntermediateBatchBuilders,
) {
    let level = &spec.levels[depth];
    let level_result = results.aggs.get(&level.name);

    match level_result {
        Some(IntermediateBucketResult::Terms(terms)) => {
            // Populate per-level ES error metadata from tantivy
            let sum_other = terms.sum_other_doc_count;
            let doc_count_error = terms.doc_count_error_upper_bound;

            for (key, entry) in &terms.entries {
                let mut keys = parent_keys.to_vec();
                keys.push(key.clone());

                let metrics = extract_intermediate_metrics(
                    &entry.sub_aggregation, &level.metrics);
                builders.push_row(
                    depth, &keys, entry.doc_count, &metrics,
                    BucketSemantics::CandidateTerms,
                    Some(sum_other), doc_count_error,
                );

                if depth + 1 < spec.levels.len() {
                    walk(&entry.sub_aggregation, spec, depth + 1, &keys, builders);
                }
            }
        }
        // DateHistogram, Range: BucketSemantics::Exact, NULL error metadata
        _ => { /* ... */ }
    }
}
```

The resulting RecordBatch has the exact same schema as the generic
Partial path. The Final executor is source-agnostic.

## Substrait Extension

### Protobuf Message

```protobuf
message NestedTopKAggRel {
    substrait.Rel input = 1;
    repeated AggLevel levels = 2;
    AggSemantics semantics = 3;
}

enum AggSemantics {
    EXACT = 0;
    ES_COMPAT_APPROX_LEAF = 1;
}

message AggLevel {
    string name = 1;
    substrait.Expression group_expr = 2;
    BucketType bucket_type = 3;
    uint32 top_k = 4;
    optional uint32 shard_size = 5;
    AggOrder order_by = 6;
    repeated MetricSpec metrics = 7;
}

message BucketType {
    oneof type {
        TermsBucket terms = 1;
        DateHistogramBucket date_histogram = 2;
        RangeBucket range = 3;
    }
}

message DateHistogramBucket {
    string fixed_interval = 1;
}

message RangeBucket {
    repeated Range ranges = 1;
    message Range {
        optional double from = 1;
        optional double to = 2;
    }
}

message AggOrder {
    oneof order {
        bool count_desc = 1;
        bool count_asc = 2;
        MetricOrder metric = 3;
        bool key_asc = 4;
        bool key_desc = 5;
    }
    message MetricOrder {
        string metric_name = 1;
        bool ascending = 2;
    }
}

message MetricSpec {
    string name = 1;
    MetricFunc func = 2;
    substrait.Expression field = 3;
}

enum MetricFunc {
    SUM = 0;
    AVG = 1;
    MIN = 2;
    MAX = 3;
    COUNT = 4;
    PERCENTILES = 5;
    CARDINALITY = 6;
}
```

### ES DSL → Substrait Mapping

```
ES agg JSON key (e.g., "top_services") → AggLevel.name
ES "terms" agg              → AggLevel with BucketType::Terms
ES "date_histogram" agg     → AggLevel with BucketType::DateHistogram
ES "range" agg              → AggLevel with BucketType::Range
ES sub-agg that is a bucket → next AggLevel (recurse into levels)
ES sub-agg that is a metric → MetricSpec on current AggLevel
ES metric sub-agg JSON key  → MetricSpec.name
ES "size" field              → top_k
ES "shard_size" field        → shard_size (default: top_k * 1.5 + 10)
ES "order" field             → AggOrder
Always                       → semantics = ES_COMPAT_APPROX_LEAF
```

### Codec Serialization

`NestedTopKAggExec` serializes via `TantivyCodec`. The spec is the
protobuf above. The mode (Partial/Final/Single) is a single enum byte.
The intermediate schema is derived deterministically from the spec.

## Distributed Execution

End-to-end flow through Quickwit:

```
1. Client sends ES aggregation JSON to quickwit-serve.

2. quickwit-serve: ES DSL → Substrait NestedTopKAggRel
   (semantics = ES_COMPAT_APPROX_LEAF).

3. quickwit-datafusion Substrait consumer: → NestedTopKAgg logical plan.

4. DataFusion physical planner: → NestedTopKAggExec(Single).

5. Physical optimizer (two-phase split):
   NestedTopKAggExec(Partial) → CoalescePartitions → NestedTopKAggExec(Final)

6. Physical optimizer (tantivy pushdown):
   Replaces Partial with NestedAggTantivyDataSource (uses
   DistributedAggregationCollector, not AggregationCollector).

7. Physical optimizer (distributed):
   Serializes leaf plan via TantivyCodec. Assigns splits to workers.

8. Workers (parallel, one per split):
   - Open split, warmup fast fields for aggregation columns only.
   - Run DistributedAggregationCollector with nested Aggregations.
   - Flatten IntermediateAggregationResults to intermediate RecordBatch,
     preserving sum_other_doc_count and doc_count_error.
   - Serialize as Arrow IPC, stream to coordinator.

9. Coordinator:
   - Receive intermediate batches from all workers.
   - NestedTopKAggExec(Final): streaming merge → prune → resolve → emit.
   - Output RecordBatch.

10. quickwit-serve: output RecordBatch → ES-compatible nested JSON tree
    → HTTP response.
```

For parquet-backed data: step 6 does not fire. The Partial executor
reads Arrow batches from `ParquetExec`, builds `NestedAccumulator`,
prunes candidate-based levels to `shard_size` at emit, emits the same
intermediate format. Steps 9-10 are identical.

## ES Output Compatibility

The API layer (quickwit-serve) converts the output RecordBatch to ES's
nested JSON tree by walking rows grouped by `__level`. Each level's
`AggLevel.name` provides the JSON key for the sub-aggregation object.

1. Collect level=0 rows. Each becomes a top-level bucket with `key`,
   `doc_count`, and resolved metrics. The bucket array is keyed by
   `spec.levels[0].name`.
2. For each level=0 bucket, collect level=1 rows matching its key.
   Each becomes a child bucket under `spec.levels[1].name`.
3. Recurse for deeper levels.

ES formatting details handled in this layer:

- `sum_other_doc_count`: carried from the output RecordBatch. Sum of
  counts of buckets discarded at each level. Per-level, not global.
- `doc_count_error_upper_bound`: carried from the output RecordBatch.
  Valid only for `_count desc` ordering on terms levels.
- Bucket key formatting: epoch millis for date_histogram, `"*-100.0"`
  / `"100.0-200.0"` for range buckets, keyed vs array format.
- Tie-breaking: handled by `compare_acc_nodes` (secondary sort by key
  ascending), consistent with ES.

## IO Analysis

Query: top 50 services, top 20 endpoints, top 10 error types,
avg(latency) at every level. 100 splits, 10 segments each, 4 fast
field columns. EsCompatApproxLeaf semantics.

### NestedTopKAgg with tantivy pushdown

| Phase                      | S3 GETs | S3 Bytes | Worker → Coord | Worker Memory     |
|----------------------------|---------|----------|----------------|-------------------|
| Footer (cached)            | 0–100   | 0–100MB  | —              | —                 |
| Warmup: 4 fields × 10 seg | 4,000   | 320GB    | —              | 3.2GB cache       |
| Accumulation               | 0       | 0        | —              | tantivy-managed   |
| Intermediate transfer      | —       | —        | ~100MB total   | —                 |
| Final merge (streaming)    | 0       | 0        | —              | 10–100MB tree (*) |
| **Total**                  | **4,000** | **320GB** | **~100MB**   | **~100MB worst**  |

### NestedTopKAgg generic path (parquet, Exact)

| Phase                      | S3 GETs | S3 Bytes | Worker → Coord | Worker Memory        |
|----------------------------|---------|----------|----------------|----------------------|
| Column reads               | ~4,000  | ~320GB   | —              | —                    |
| Accumulation               | 0       | 0        | —              | O(cardinality) (**)  |
| Intermediate transfer      | —       | —        | varies         | —                    |
| Final merge (streaming)    | 0       | 0        | —              | ~merged tree         |
| **Total**                  | **~4,000** | **~320GB** | **varies** | **data-dependent**   |

### Flat GROUP BY fallback (no operator)

| Phase                      | S3 GETs | S3 Bytes | Worker → Coord | Worker Memory  |
|----------------------------|---------|----------|----------------|----------------|
| Column reads               | ~4,000  | ~320GB   | —              | —              |
| Hash GROUP BY              | 0       | 0        | —              | 1GB hash table |
| Intermediate transfer      | —       | —        | 50GB           | —              |
| Window + prune             | 0       | 0        | —              | tens of GB     |
| **Total**                  | **~4,000** | **~320GB** | **50GB**   | **1GB+ peak**  |

(*) Coordinator tree size depends on key overlap across partitions.
Typical observability data: ~10-100MB. Pathological disjoint data:
could be N_partitions × shard_size × node_size. See Memory Model.

(**) Exact path worker memory is O(actual cardinality product). For
typical observability data this is manageable. For high-cardinality
fields, it can be large. Fails on memory budget, does not silently
approximate.

### Where S3 calls are actually reduced

The aggregation strategy does not change per-query S3 IO. What does:

1. **Split pruning via timestamp metadata.** "Last 1 hour" queries skip
   99% of splits.
2. **Fast field cache on searcher nodes.** Warm splits have zero S3
   calls.
3. **Batched byte-range GETs.** Combine multiple column reads into fewer
   S3 requests per split.

## Why This Beats Elasticsearch

1. **Columnar execution.** Tantivy fast fields and parquet are columnar.
   ES's doc_values are columnar too, but ES's Java collector has object
   overhead per bucket. Rust's `NestedAccumulator` is compact structs
   with no GC pressure.

2. **Arrow-native intermediate format.** ES serializes intermediate
   results as custom Java serialization. We use Arrow IPC — cache-line
   friendly memory layout.

3. **Streaming merge.** ES's coordinator collects all shard results
   into memory, then merges. Our Final executor folds each intermediate
   batch into the accumulator tree as it arrives and discards the batch.
   Peak coordinator memory is one tree, not all intermediate results.

4. **Shared fast field cache.** Quickwit's searcher nodes cache fast
   field columns across queries. Repeated dashboard refreshes hit cached
   data.

5. **Parquet fallback.** For cold data in parquet, the same operator
   works without tantivy. ES must index everything into Lucene.

6. **No JVM warmup.** Rust is fast from the first query.

## Implementation Components

All components live in tantivy-datafusion. The generic parts (everything
except the tantivy pushdown) do not import tantivy types.

### 1. Core Data Model

- `AggSemantics`, `NestedTopKAggSpec`, `AggLevel`, `BucketType`,
  `AggOrder`, `MetricSpec`, `MetricFunc`.
- `intermediate_schema(spec) -> SchemaRef` — includes `__bucket_semantics`,
  `__sum_other_doc_count`, `__doc_count_error` columns.
- `output_schema(spec) -> SchemaRef` — resolved metric columns.

### 2. NestedAccumulator

- `BucketKey` enum with Hash/Eq/Ord.
- `AccNode` with count, metrics vec, optional children map.
- `MetricAccumulator` enum with all metric types.
- `accumulate(batch: &RecordBatch)` — per-batch ingestion.
- `check_memory_budget()` — fails if tree exceeds budget.
- `emit_intermediate() -> RecordBatch` — for EsCompatApproxLeaf: prune
  candidate levels to shard_size, populate error metadata. For Exact:
  emit all buckets.
- `merge_intermediate_batch(batch: &RecordBatch)` — streaming merge
  for Final executor.
- `emit_final() -> RecordBatch` — prune to top_k, resolve metrics,
  merge error metadata.

### 3. Physical Executors

- `NestedTopKAggExec` implementing `ExecutionPlan`.
- `AggMode::Partial`, `AggMode::Final`, `AggMode::Single`.
- Behavior within each mode determined by `spec.semantics`.

### 4. Logical Plan Node

- `NestedTopKAgg` implementing `UserDefinedLogicalNode`.
- `from_spec(input: LogicalPlan, spec: NestedTopKAggSpec) -> Self`.
- Schema derivation from spec.

### 5. Physical Optimizer Rules

- Two-phase split: converts `NestedTopKAggExec(Single)` to
  `Partial → Coalesce → Final`.
- Tantivy pushdown: replaces `Partial` above `SingleTableDataSource`
  with `NestedAggTantivyDataSource`. Only fires for
  `EsCompatApproxLeaf` and bare column references.
- Rule ordering: two-phase split first, then tantivy pushdown, then
  distributed partitioning.

### 6. Tantivy Bridge

- `spec_to_tantivy_aggregations(spec) -> Option<Aggregations>`.
- `intermediate_agg_results_to_batch(results, spec, schema) -> RecordBatch`.
  Uses `DistributedAggregationCollector` output. Preserves
  `sum_other_doc_count` and `doc_count_error_upper_bound` from tantivy.
- `NestedAggTantivyDataSource` implementing `DataSource`.

### 7. Substrait Extension

- Protobuf definitions for `NestedTopKAggRel` with `AggSemantics`.
- `QuickwitSubstraitConsumer` handling for the extension.
- ES DSL → Substrait converter (always emits `ES_COMPAT_APPROX_LEAF`).

### 8. Codec

- Serialization/deserialization of `NestedTopKAggExec` via
  `TantivyCodec`.
- Round-trip tests.

### 9. ES Output Formatting

- `output_batch_to_es_json(batch, spec) -> serde_json::Value`.
- Handles `sum_other_doc_count`, `doc_count_error_upper_bound`,
  bucket key formatting, tie-breaking.
- Lives in quickwit-serve's API layer.

## Scope for V1

### Must support

- Nested bucket trees of `terms`, `date_histogram`, `range`.
- Metrics: `sum`, `avg`, `min`, `max`, `count`, `percentiles`,
  `cardinality`.
- Two semantics: `Exact`, `EsCompatApproxLeaf`.
- Tantivy pushdown for the ES-compatible path.
- Generic Arrow path for parquet.
- ES error metadata: `sum_other_doc_count`, `doc_count_error_upper_bound`.
- Deterministic tie-breaking (secondary sort by key ascending).

### Ordering guarantees for V1

- `CountDesc`: full ES error metadata, bounded error.
- `KeyAsc`, `KeyDesc`: exact, no error metadata needed.
- `CountAsc`: supported, but error metadata not defined (matches ES).
- `Metric { max, desc }` and `Metric { min, asc }`: safe per ES docs.
- Other metric orderings: supported but no error guarantees.

### Can wait

- Spill to disk for exact generic partials.
- Full ES parameter parity: `min_doc_count`, `shard_min_doc_count`,
  `missing`, `include`/`exclude`, `calendar_interval`, `time_zone`,
  `offset`, `extended_bounds`, `hard_bounds`, keyed range responses,
  custom range keys.
- `rare_terms`, `composite`, `filter`, `pipeline` aggregations.
