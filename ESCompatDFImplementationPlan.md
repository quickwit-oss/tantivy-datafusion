# ES-Compatible Nested Approx Aggregations in DataFusion: Implementation Plan

## Purpose

This document describes how to implement nested approximate top-k aggregations
in `tantivy-datafusion` with:

- native tantivy leaf execution when pushdown is possible
- a DataFusion fallback when expressions block pushdown
- a shared Arrow intermediate format
- proper scatter/gather with overscan

This plan targets the simplified contract from `ESCompatDFNoMetadata.md`:

- local candidate pruning is preserved
- approximation metadata is not returned

## Scope

This is a plan for `tantivy-datafusion`.

It does not require SQL syntax for nested aggs. It does not require a new UDAF
for v1. It does not require a new logical extension framework in
`quickwit-datafusion` for v1.

The first deliverable is a reusable physical plan builder and execution path.
`quickwit-datafusion` can call that builder from its query translation layer.

## What We Reuse

The existing code already gives us most of the leaf and distributed plumbing.

### Reuse as-is

- `SingleTableProvider` and `SingleTableDataSource`
  - split-aware fast-field scan
  - hidden `_segment_ord`
  - filter conversion and runtime split preparation
- `AggDataSource`
  - per-split execution
  - warmup
  - codec serialization of split descriptors and filters
- `execute_tantivy_intermediate_agg_with_reader`
  - uses `DistributedAggregationCollector`
- `TantivyCodec`
  - distributed codec hook already registered through source contributions
- distributed session wiring in `quickwit-datafusion`

### Reuse with extension

- `AggPushdown`
  - keep for existing single-level SQL `GROUP BY`
  - add a separate nested-agg planner path instead of stretching this rule
- `AggDataSource`
  - add a new partial output mode for node-table partials
- `agg_exec.rs`
  - keep current one-level conversion functions
  - add recursive `IntermediateAggregationResults -> node table` conversion

### Reuse at the expression / metric layer

Do not reuse `AggregateExec` itself for nested agg execution.

Do reuse DataFusion aggregate expression machinery:

- `AggregateExpr`
- `Accumulator`
- `state_fields()`
- `merge_batch()`
- `evaluate()`

That gives us:

- one metric definition path
- one state schema convention
- one generic merge story for exact algebraic metrics

This is the main reuse point with DataFusion.

### Current touch points

The first implementation pass should start from these existing functions.

- `src/unified/agg_data_source.rs`
  - `execute_single_split_partial_state_batch`
  - `execute_split_intermediate_agg`
  - `execute_final_agg_batch`
- `src/unified/agg_exec.rs`
  - `execute_tantivy_intermediate_agg_with_reader`
  - `merge_intermediate_agg_results`
  - `agg_results_to_partial_state_batch`
- `src/unified/agg_pushdown.rs`
  - `derive_tantivy_partial_aggregations`
  - `try_rewrite_two_phase`
- `src/codec.rs`
  - `try_encode`
  - `decode_agg`

These are the current single-level hooks that the nested path should extend, not
replace.

## Why Not a UDAF

Do not implement this as a UDAF in v1.

Reasons:

- a UDAF produces one aggregate value per group
- nested top-k produces a bucket tree, not one scalar
- the current `quickwit-datafusion` contribution API does not register UDAFs
- pushdown still needs a custom leaf rewrite and a custom codec path

UDAF state handling is still useful as inspiration:

- use explicit partial/final state
- use DataFusion metric `state_fields()` conventions

The implementation should borrow that pattern, not the UDAF abstraction.

## Why Not Wrap `AggregateExec`

Do not try to express the core nested-agg algorithm as a wrapper around normal
`AggregateExec`.

`AggregateExec` is still useful for plain SQL and existing single-level
pushdown. It is not the right final merge operator for nested top-k because:

- the intermediate state is a tree, not rows keyed by one flat group key
- final trim is per level, not one global `LIMIT`
- child ranking is parent-dependent
- the result is a multi-row bucket tree

The custom path should instead reuse:

- `ProjectionExec`
- `RepartitionExec`
- `CoalescePartitionsExec`
- `DataSourceExec`

around custom nested-agg exec nodes.

## Proposed Types

### `NestedApproxAggSpec`

Add a new spec type in `tantivy-datafusion`.

Suggested shape:

```rust
pub struct NestedApproxAggSpec {
    pub semantics: NestedAggSemantics,
    pub levels: Vec<BucketLevelSpec>,
    pub metrics: Vec<MetricSpec>,
    pub segment_mode: SegmentExecutionMode,
    pub final_output: FinalOutputMode,
}

pub enum NestedAggSemantics {
    ApproxNoMetadata,
    Exact,
}

pub enum SegmentExecutionMode {
    NativeTantivy,
    SplitLocal,
    SegmentAwareInternal,
    SegmentPartitioned,
}
```

For `terms` levels, each level carries:

- `final_size`
- `fanout`
- ordering

For v1, support:

- `terms`
- `date_histogram`
- `range`
- ordering by `_count desc`
- optional `_key` ordering later

Defer:

- `_count asc`
- arbitrary sub-agg ordering
- ES nested-doc aggregations
- full ES formatting flags

### `MetricSpec`

Metrics should carry DataFusion `AggregateExpr`s or an equivalent wrapper that
can create `Accumulator`s.

This lets the generic path and final merge share the same metric state logic.

### `NodeTableSchema`

The shared partial format is a node table.

Intermediate schema:

```text
__tree_id
__node_id
__parent_id
__level
__count
__key_0 ... __key_n
[metric state fields]
```

Final schema:

```text
__level
__count
__key_0 ... __key_n
[finalized metric fields]
```

Internal transport ids do not appear in final output.

## New Execution Nodes

### 1. Extend `AggDataSource`

Add a new output mode:

```rust
pub enum AggOutputMode {
    FinalMerged,
    PartialStates,
    NodeTablePartial,
}
```

`PartialStates` stays for the current single-level `AggregateExec(Final*)`
integration.

`NodeTablePartial` is the new nested-agg leaf path.

### 2. Add `NestedApproxAggExec`

Add a custom physical exec with modes:

```rust
pub enum NestedApproxAggMode {
    PartialSplitLocal,
    PartialSegmentAware,
    SplitMerge,
    FinalMerge,
}
```

Recommended v1 modes:

- `PartialSplitLocal`
- `PartialSegmentAware`
- `FinalMerge`

`SplitMerge` is only needed if we choose explicit segment partitioning instead
of an internal segment-aware partial.

### 3. Optional builder, not logical extension, for v1

V1 should expose a builder like:

```rust
pub fn build_nested_approx_plan(
    input: Arc<dyn ExecutionPlan>,
    scan_info: Option<&SingleTableDataSource>,
    spec: Arc<NestedApproxAggSpec>,
) -> Result<Arc<dyn ExecutionPlan>>;
```

This avoids depending on a new logical extension framework in
`quickwit-datafusion`.

Later, if we want full logical/physical planning from Substrait or SQL, add a
logical extension node and planner registration.

## Plan Shapes

### Pushdown path

Use when every bucket key and metric input is pushdownable to tantivy.

```text
DataSourceExec(
  AggDataSource {
    output_mode = NodeTablePartial,
    aggregations = tantivy nested agg tree,
    spec = nested approx spec
  }
)
  -> Coalesce / distributed exchange
  -> NestedApproxAggExec(FinalMerge, spec)
```

Properties:

- leaf runs one split at a time
- tantivy handles segment-local pruning
- output is a split-local node table
- final node merges split partials and trims to `final_size`

### Non-pushdown path, v1 simple fallback

Use when any level contains a non-pushdownable expression.

```text
SingleTable scan
  -> ProjectionExec(normalized level keys and metric inputs)
  -> NestedApproxAggExec(PartialSplitLocal, spec)
  -> Coalesce / distributed exchange
  -> NestedApproxAggExec(FinalMerge, spec)
```

Properties:

- simple
- no segment-local parity
- good approximate fallback
- visible winners can differ from tantivy pushdown

### Non-pushdown path, segment-aware fallback

Use when fallback must match tantivy's pruning boundary.

Internal segment-aware version:

```text
SingleTable scan
  -> ProjectionExec(normalized level keys and metric inputs)
  -> NestedApproxAggExec(PartialSegmentAware, spec)
  -> Coalesce / distributed exchange
  -> NestedApproxAggExec(FinalMerge, spec)
```

Explicit segment-partitioned version:

```text
SingleTable scan
  -> ProjectionExec(normalized level keys and metric inputs)
  -> Repartition(hash(split_id, _segment_ord))
  -> NestedApproxAggExec(PartialSplitLocal, spec)
  -> Repartition(hash(split_id))
  -> NestedApproxAggExec(SplitMerge, spec)
  -> Coalesce / distributed exchange
  -> NestedApproxAggExec(FinalMerge, spec)
```

Recommend the internal segment-aware version first. The scan path already
emits `_segment_ord`.

## Fanout and Overscan

Overscan is modeled explicitly as `fanout`.

For each `terms` level:

- `final_size` is the user-visible top-k
- `fanout` is the local candidate limit before final merge

Example:

```text
service:    final_size=50, fanout=200
endpoint:   final_size=20, fanout=80
error_type: final_size=10, fanout=40
```

### Pushdown mapping

For pushdown:

- map `fanout` to tantivy `segment_size`
- keep `final_size` in the spec for final DF trimming

Important detail:

- tantivy trims at the segment boundary
- split-level `IntermediateAggregationResults` are merged across segment
  candidates and are not final-size trimmed yet

This is acceptable for v1. It matches native tantivy split partial semantics.

If transport size later becomes a problem, add an optional split-local trim
stage after node-table conversion.

### Planner responsibility

The planner must set fanout explicitly. Do not hide it inside storage defaults.

V1 recommendation:

- require explicit fanout in the spec, or
- default to tantivy-compatible behavior (`segment_size = size * 10`)

If upstream wants ES-style defaults instead, it can normalize those before
calling the builder.

## `AggDataSource` Changes

### New fields

Add an optional nested spec field:

```rust
nested_spec: Option<Arc<NestedApproxAggSpec>>
```

This is needed for:

- node-table schema derivation
- conversion from tantivy intermediates to node-table rows
- codec roundtrip

### New constructors

Add constructors for node-table partials:

- `from_split_descriptors_node_table_partial(...)`
- `from_local_splits_node_table_partial(...)`

Keep existing constructors unchanged for current SQL agg pushdown.

### New partial execution path

Today `execute_single_split_partial_state_batch` uses:

- `execute_tantivy_agg_results_with_reader`
- `agg_results_to_partial_state_batch`

That is the wrong boundary for nested approx agg.

For `NodeTablePartial`, switch to:

- `execute_tantivy_intermediate_agg_with_reader`
- `intermediate_results_to_node_table_batch`

This is the critical change.

### New conversion function

Add a recursive conversion:

```rust
pub(crate) fn intermediate_results_to_node_table_batch(
    partial: &IntermediateAggregationResults,
    spec: &NestedApproxAggSpec,
    schema: &SchemaRef,
) -> Result<RecordBatch>;
```

Requirements:

- assign transport-local node ids
- emit one row per retained bucket node
- populate one key column for the node's level
- populate metric state fields using DataFusion metric state ordering
- preserve child structure via parent id

### Keep `FinalMerged`

Do not remove `FinalMerged`. It remains useful for:

- current one-level pushdown
- differential tests against native tantivy finalization

## `agg_exec.rs` Changes

Keep current one-level conversions for existing SQL `GROUP BY` pushdown.

Add new nested-specific modules or functions:

- `intermediate_results_to_node_table_batch`
- recursive key extraction for:
  - terms
  - date_histogram
  - range
- metric partial-state extraction from:
  - intermediate count
  - intermediate sum
  - intermediate avg
  - min/max
  - later sketches

Do not overload the existing single-level flat conversion functions.

## `NestedApproxAggExec` Algorithm

### Final merge

Input:

- one or more node-table batches from splits

Algorithm:

1. Reconstruct each batch's tree using `__node_id` / `__parent_id`.
2. Merge into one in-memory tree keyed by:
   - level
   - parent path
   - bucket key
3. Merge metric states per node.
4. After merge, trim `terms` children to `final_size` at each parent.
5. Finalize metric states.
6. Emit final node-table rows.

This merge is custom. It does not go through `AggregateExec(Final)`.

### Generic partial

Input:

- projected Arrow columns
- `_segment_ord` when segment-aware mode is used

Algorithm:

1. Read normalized key columns and metric input columns.
2. Update an in-memory nested tree.
3. For split-local mode:
   - build full split-local tree
   - trim `terms` children to `fanout` before emit
4. For segment-aware mode:
   - maintain a segment-local tree
   - when `_segment_ord` changes:
     - trim `terms` children to `fanout`
     - merge the segment tree into a split tree
     - reset the segment tree
5. Emit a node-table batch.

Use DataFusion `Accumulator`s inside nodes for metric state.

## Pushdownability Rules

Pushdown should fire only when all levels are storage-native:

- every bucket key is a simple column
- every metric input is a simple column
- bucket kinds are supported by tantivy
- ordering is supported

If any level is a non-pushdownable expression:

- v1 fallback: generic exact or split-local approximate
- later: segment-aware approximate fallback

Do not try partial tree pushdown in v1. If one level contains an expression,
keep the whole tree in the generic path.

## Differences With Regular Elasticsearch

This implementation is intentionally narrower.

### V1 differences

- no `sum_other_doc_count`
- no `doc_count_error_upper_bound`
- no ES nested-doc aggregations
- no `_count asc`
- no arbitrary sub-agg ordering
- no full ES response JSON inside DF

### Default fanout

ES and tantivy use different defaults.

V1 should avoid hidden defaults in the engine. The planner should pass explicit
fanout.

If a default is needed inside `tantivy-datafusion`, prefer one of:

- explicit `fanout required`
- tantivy-compatible `size * 10`

Do not silently mix ES and tantivy defaults.

### Non-pushdown parity

If fallback uses split-local trim, it is approximate but not tantivy-equivalent.

If exact fallback is chosen, it is more accurate than pushdown, but results can
differ from the approximate pushdown path.

Only the segment-aware fallback should be expected to match tantivy's visible
winners.

## Codec Changes

`TantivyCodec` currently supports:

- `SingleTableDataSource`
- `AggDataSource`

It must be extended to support:

- `AggDataSource` with `NodeTablePartial`
- `NestedApproxAggExec`

Recommended changes:

1. Extend the agg datasource proto payload with:
   - new output mode value
   - serialized nested spec
2. Add a new provider / exec type for `NestedApproxAggExec`
3. Add roundtrip tests for both

Without codec support, distributed execution will not work.

## Testing Plan

Testing must be differential, not only structural.

### 1. Unit tests

Add unit tests for:

- spec validation
- fanout propagation
- node-table schema derivation
- `IntermediateAggregationResults -> node table` conversion
- node-table merge
- per-level trim logic

### 2. Pushdown correctness tests

Add integration tests with local in-memory indexes:

- single split nested terms with metrics
- multi split nested terms with metrics
- terms + date_histogram
- terms + range

For pushdown correctness, compare:

- direct tantivy final result
- `AggDataSource(NodeTablePartial)` + `NestedApproxAggExec(FinalMerge)`

These should match on:

- returned buckets
- counts
- metric values

### 3. Overscan tests

Add explicit regression tests where:

- a true winner is not top-1 on any segment
- low fanout loses it
- larger fanout recovers it

This proves the plan is actually doing overscan.

### 4. Non-pushdown fallback tests

Add tests where an expression blocks pushdown:

- example: `CASE WHEN latency > 1000 THEN 'slow' ELSE 'fast' END`

Test:

- plan does not use `AggDataSource(NodeTablePartial)`
- partial is produced by `NestedApproxAggExec`
- final output is correct for the selected fallback mode

### 5. Segment-aware parity tests

Add tests where:

- pushdown query uses a raw field
- fallback query uses an equivalent expression over the same field

Compare:

- pushdown path result
- segment-aware fallback result

These should match on visible winners and metric values.

### 6. Explain-plan tests

Add `EXPLAIN` tests for:

- pushdown path
- exact fallback
- segment-aware fallback

Assert the plan contains the expected nodes.

### 7. Codec roundtrip tests

Extend existing `codec_roundtrip.rs` to cover:

- `AggDataSource(NodeTablePartial)`
- `NestedApproxAggExec`
- preserved `fanout`
- preserved `segment_mode`

### 8. Distributed / multi-split tests

Extend `multi_split.rs` or add new tests to verify:

- split-local partials are scattered
- final merge happens once
- results are stable across split order

### 9. Performance tests

Add benchmark-style tests for:

- pushdown approx vs full exact flat group-by
- node-table partial size vs current flat partial states

The main metrics are:

- bytes transferred
- rows emitted in the partial stream
- peak memory in final merge

## Recommended Implementation Phases

### Phase 1: Pushdown path only

Deliver:

- `NestedApproxAggSpec`
- `AggOutputMode::NodeTablePartial`
- node-table conversion from tantivy intermediate results
- `NestedApproxAggExec(FinalMerge)`
- codec support
- pushdown correctness tests

This already gives:

- proper scatter/gather
- overscan via fanout
- native tantivy leaf semantics

### Phase 2: Exact generic fallback

Deliver:

- `NestedApproxAggExec(PartialSplitLocal)` in exact mode
- Projection-based expression path
- exact fallback tests

This gives a safe path for non-pushdown queries without yet promising
tantivy-equivalent approximation.

### Phase 3: Segment-aware generic fallback

Deliver:

- `NestedApproxAggExec(PartialSegmentAware)`
- segment-aware parity tests

This is the point where generic fallback can claim parity with tantivy's
pruning boundary.

### Phase 4: Optional split-local transport trim

Only if transport size becomes a problem.

Options:

- trim split-local merged candidates before scatter
- add a dedicated `SplitMerge` / `SplitTrim` stage

Do this only after phase 1-3 correctness is proven.

## File-Level Plan

### New files

- `src/nested_agg/spec.rs`
- `src/nested_agg/node_table.rs`
- `src/nested_agg/exec.rs`
- `tests/nested_agg_pushdown.rs`
- `tests/nested_agg_codec.rs`
- `tests/nested_agg_fallback.rs`

### Existing files to change

- `src/unified/agg_data_source.rs`
- `src/unified/agg_exec.rs`
- `src/codec.rs`
- `src/lib.rs`

Optional later:

- `quickwit-datafusion/src/data_source.rs`
  - only if we add planner registration support beyond physical rules

## Final Recommendation

Implement this as:

- extended `AggDataSource` for pushdown leaf partials
- custom `NestedApproxAggExec` for merge and fallback
- node-table Arrow intermediate
- explicit `fanout` and `final_size` per `terms` level

Do not implement it as:

- a UDAF
- a wrapper around normal `AggregateExec`
- an opaque binary intermediate

That gives the best balance of:

- correctness
- transport efficiency
- reuse of current tantivy and DataFusion code
- a realistic path to segment-aware fallback later
