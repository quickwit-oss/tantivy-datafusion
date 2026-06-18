# ES-Compatible Aggregations in DataFusion: Problem Statement

## Problem

We need to execute Elasticsearch-style aggregation queries in DataFusion with
three properties:

- correct: match Quickwit/tantivy semantics
- efficient: keep native pruning and avoid large intermediate blowups
- well architected: make the behavior explicit in the DF plan and support
  fallback when pushdown fails

This is hard because ES aggregation semantics are not plain SQL aggregation
semantics.

In this note, "ES-compatible" means matching Quickwit/tantivy semantics for
ES-style aggregations on this path. It does not claim full Elasticsearch parity
for every edge case.

## Background

ES-style aggregations exist because search and observability systems need
low-latency grouped analytics over indexed data: top services, top endpoints,
time histograms, p99 latency per bucket, and nested drill-downs. They are
common because they are operationally useful and cheaper than exact global
group-by over the same data.

The main aggregation kinds are:

- bucket aggregations: `terms`, `date_histogram`, `range`
- metric aggregations: `sum`, `avg`, `min`, `max`, `count`
- sketch metrics: `percentiles`, `cardinality`
- sub-aggregation trees: bucket aggregations nested under bucket aggregations

The key semantic split is:

- `terms` is candidate-based
- `date_histogram` and `range` are exhaustive
- algebraic metrics merge exactly
- sketch metrics remain approximate

## Quickwit and Tantivy

### Data layout

Quickwit stores data in splits. Each split contains segments. Aggregations read
matching documents through tantivy and usually read values from fast fields.

### Query path

The general path is:

1. Quickwit selects matching splits.
2. A leaf searcher runs a tantivy collector on each split.
3. The leaf returns intermediate aggregation results.
4. The coordinator merges those intermediate results.
5. Finalization produces the ES response shape.

Quickwit mainly orchestrates this flow. Tantivy owns the aggregation semantics.

In `tantivy-datafusion`, the relevant building blocks already exist:

- `AggPushdown`: rewrites partial aggregates
- `AggDataSource`: runs tantivy collectors
- `execute_tantivy_intermediate_agg_with_reader`: uses
  `DistributedAggregationCollector`

The current code handles single-level aggregation pushdown. This work extends
that shape to nested sub-aggregation trees.

### Pruning levels

The pruning story matters more than the tree shape.

- segment level:
  `terms` cuts candidates to `segment_size`
- split level:
  tantivy merges segment partials into one split-level intermediate tree
- final level:
  Quickwit merges split intermediates and finalization applies final ordering
  and final `size`

This is why `terms` can be approximate while `date_histogram` and `range`
remain exhaustive.

It is also why `sum_other_doc_count` and `doc_count_error_upper_bound` exist:
they describe buckets lost during candidate selection.

## Why This Is Hard in DataFusion

DataFusion assumes exact relational aggregation:

- group rows by key
- compute algebraic partial state
- merge partial state
- sort and limit

ES-style queries do not fit that model cleanly.

`terms` is not exhaustive. Parent metrics must stay correct when child buckets
are pruned. Approximation lives at the leaf boundary. The final result is a
bucket tree, not one scalar per group.

This is not a good fit for a plain UDAF. A UDAF can carry state, but it still
models one aggregate result per group. Here we need a custom operator that
carries and merges a bucket tree with per-level semantics and error metadata.

This can be written with joins, windows, and exact grouping. That would be
correct for SQL. It would not be correct for ES. It would also be expensive:
too many groups, too many bytes, and no native candidate pruning.

## Rejected Solutions

- Mid-stream eviction: wrong semantics. Evicted buckets lose state and can later
  become winners.
- Denormalized path rows: valid but wasteful. Descendant rows repeat ancestor
  keys.
- Opaque binary state: compact for `DistributedAggregationCollector`, but not a
  good shared contract once generic Arrow partials must be produced too.
- Keep merge outside DF: preserves semantics by passing
  `IntermediateAggregationResults` through Quickwit-style merge/finalize, but
  gives up a DF-visible operator and a shared fallback path.

## Solution Idea

Use a dedicated nested aggregation operator with one shared intermediate
contract.

The contract is a node table:

- one row per bucket node
- parent pointer
- one key column populated for the node's level
- metric partial state on the node row
- ES candidate metadata on the owning parent row

The shared contract is not a tantivy struct and not a SQL row set. It is a
DF-visible tree state.

Concretely, the pushdown path reuses tantivy's existing aggregation APIs:

- `DistributedAggregationCollector::from_aggs(...)`
- leaf output type: `IntermediateAggregationResults`
- leaf merge primitive: `IntermediateAggregationResults::merge_fruits(...)`
- finalization primitive: `IntermediateAggregationResults::into_final_result(...)`

That path is:

```text
Tantivy split
  -> DistributedAggregationCollector
  -> IntermediateAggregationResults
  -> convert to node-table Arrow batches
  -> DF final node
```

The generic fallback reuses the existing fast-field scan path and performs the
aggregation in DF after projection:

```text
DataSourceExec(SingleTableProvider)
  -> ProjectionExec
  -> NestedTopKAggExec(PartialEsCompat or PartialExact)
  -> Coalesce/Repartition
  -> NestedTopKAggExec(Final)
```

This is not a new UDAF. It is a new logical/physical operator:

- logical: `NestedTopKAgg` as a custom logical node
- physical partial:
  - `TantivyNativePartialExec` when full pushdown is possible
  - `NestedTopKAggExec(Partial...)` when expressions force Arrow fallback
- physical final:
  - `NestedTopKAggExec(Final)`

The current single-level formulation in `tantivy-datafusion` is:

```text
AggregateExec(Final)
  <- ... <- AggregateExec(Partial)
  <- DataSourceExec(AggDataSource)
```

driven by `AggPushdown` and `AggDataSource`. The proposed nested formulation is
analogous in shape, but it cannot stay inside plain `AggregateExec` because the
intermediate state is a bucket tree, not one scalar state tuple per SQL group.

The pushdown path uses tantivy's distributed collector and converts tantivy's
intermediate tree into the node-table schema. The generic fallback scans fast
fields into Arrow, evaluates expressions, performs segment-aware partial
aggregation, and emits the same schema. A final DF node merges the partial
trees and produces the ES result.

## Corner Cases

### Segment semantics

Full ES compatibility requires segment-aware pre-aggregation.

The shape is:

```text
segment pre-agg + prune
  -> split-local merge
  -> final cluster merge + final cutoff
```

A generic fallback cannot aggregate a whole split and prune once at the end. It
must preserve segment-local cutoff and error accounting.

Without segment-aware pre-aggregation in the generic fallback,
`sum_other_doc_count` and `doc_count_error_upper_bound` will be wrong for
`terms` levels. Bucket selection can still be made correct by building the full
split tree and pruning at emit. The error metadata will not match tantivy. This
does not matter for `Exact`. It does matter for `EsCompatApproxLeaf`.

### Expressions over fast fields

This architecture exists because not every useful query is fully pushdownable.

Example:

- `terms(service)`
- `range(CASE WHEN latency > 1000 THEN 1 ELSE 0 END)`
- `terms(error_type)`

Tantivy can read the fast fields, but it cannot execute the full expression
tree. In that case the plan must:

1. scan fast fields into Arrow
2. evaluate the expression in DF
3. aggregate generically
4. still preserve ES-compatible segment semantics

### How segment-aware fallback can work

There are two real options:

- expose segments as plan partitions and aggregate per segment
- keep split partitions, but treat hidden `_segment_ord` as an internal
  execution key and do a segment-local pre-aggregation tier inside the partial
  executor

The second option is usually cheaper to plan. The first is simpler to reason
about. Both are more complex than plain DF aggregation.

If this is not handled, the generic fallback will drift from tantivy exactly
where ES compatibility matters most: `terms` cutoff, `sum_other_doc_count`, and
`doc_count_error_upper_bound`.

There is also a scope question for v1. One simplification is:

- tantivy pushdown supports `EsCompatApproxLeaf`
- generic fallback supports only `Exact`

That avoids segment-aware generic approximation entirely. If a query contains
an expression that prevents pushdown, the query runs exact instead. That is
more accurate, but can use more memory. A generic `EsCompatApproxLeaf` fallback
is only required if approximate ES semantics must remain available when pushdown
fails.
