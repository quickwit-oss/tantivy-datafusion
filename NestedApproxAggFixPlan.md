# Nested Approx Agg Fix Plan

## Goals

We need three things at once:

1. Non-pushdown support for expressions over tantivy fast fields.
2. Proper streaming in the merge path.
3. Semantic correctness across pushdown and fallback.

The current code has a solid pushdown skeleton:

- pushdown leaf via `AggDataSource::NodeTablePartial`
- shared node-table partial format
- custom final merge in `NestedApproxAggExec(FinalMerge)`

The missing pieces are:

- a real generic partial executor
- incremental final merge
- a correct definition of `Count`

## Core Decisions

### Keep this separate from SQL aggregate pushdown

This work must stay separate from the existing flat SQL aggregate pushdown path
in `src/unified/agg_pushdown.rs`.

They solve different problems:

- SQL aggregate pushdown lowers flat `AggregateExec` plans into tantivy-native
  single-level partials and keeps a regular DF final aggregate.
- nested approx aggregation executes a tree-shaped top-k contract with
  per-level `fanout`, node-table partials, and a custom final merge.

Do not extend the old `AggPushdown` rule for this work.

Use a separate planner entry point and separate custom execs:

- `build_nested_approx_plan(...)`
- `NestedApproxAggExec(PartialSplitLocal)`
- `NestedApproxAggExec(FinalMerge)`
- `AggDataSource(NodeTablePartial)` only as the native leaf producer

Shared infrastructure is fine:

- `SingleTableDataSource`
- split runtime factory
- codec wiring
- sync execution pool

But the optimization rule, partial contract, and final merge logic stay
separate.

### Keep the custom operator

Do not force this into `AggregateExec`, a UDAF, or a wrapper over standard DF aggregate nodes.

Reasons:

- nested top-k is tree-shaped, not one scalar per group
- each level has its own `fanout`
- the generic fallback must be segment-aware
- the output contract is a node-table, not a standard aggregate row

Use the existing custom operator path:

- `AggDataSource(NodeTablePartial)` for nested native pushdown
- `NestedApproxAggExec(PartialSplitLocal)` for generic fallback
- `NestedApproxAggExec(FinalMerge)` for shared final merge

### Keep node-table partials

Do not use a shared opaque binary column.

Reasons:

- the generic fallback must consume Arrow rows and evaluate expressions first
- the pushdown and fallback paths need one shared intermediate format
- the current node-table shape is already good enough once we keep it as one row per node with parent pointers

### No approximation metadata in v1

The contract is approximate bucket selection, not error reporting.

This simplifies:

- partial schema
- final output schema
- pushdown conversion
- fallback matching

The remaining semantic requirement is still important:

- pushdown and fallback must return the same visible buckets for the same `fanout`

## Fix 1: Semantic Correctness for `Count`

The current `MetricSpec::Count` is wrong.

Today it is implemented as tantivy `CountAggregation` on the deepest field. That is value-count, not bucket `doc_count`.

### Required change

Redefine `MetricSpec::Count` as bucket document count.

Implementation:

- `MetricSpec::Count.state_field_count()` becomes `0`
- `MetricSpec::Count` is not emitted into tantivy sub-aggregations
- node-table partial rows carry no metric-state column for `Count`
- final output for `MetricSpec::Count` is derived from `__count`

This makes `Count` semantically correct for nested buckets.

If fielded value-count is needed later, add a separate metric:

- `MetricSpec::ValueCount { field: String }`

### Files

- `src/nested_agg/spec.rs`
- `src/nested_agg/node_table.rs`
- `src/nested_agg/exec.rs`
- tests that currently assume one count state column

## Fix 2: Proper Streaming in Final Merge

`FinalMerge` currently buffers every partial batch before ingesting it.

That is unnecessary.

### Required change

Change `execute_final_merge` to ingest each batch as it arrives.

Current shape:

```text
collect all batches
build merge tree from all batches
trim
finalize
```

Target shape:

```text
create merge tree
for each incoming partial batch:
  ingest into merge tree
trim
finalize
```

This keeps memory bounded by:

- merge tree
- one in-flight partial batch

### Files

- `src/nested_agg/exec.rs`

### Follow-up

Keep `CoalescePartitionsExec` for now. The final node still needs one logical input stream.

## Fix 3: Real Generic Fallback

The missing feature is not “generic final merge.” That already exists.

The missing feature is a split-local partial executor that:

- reads projected Arrow rows
- groups them into the nested tree
- trims at segment boundaries
- emits a node-table partial

## Target Physical Shapes

### Pushdown path

Keep the current shape:

```text
DataSourceExec(AggDataSource { output_mode: NodeTablePartial })
  -> CoalescePartitionsExec
  -> NestedApproxAggExec(FinalMerge)
```

This remains the fast path when all bucket keys and metric inputs are simple fields supported by tantivy.

### Generic fallback path

Add a real partial mode:

```text
DataSourceExec(SingleTableDataSource)
  -> ProjectionExec(normalize inputs for nested agg)
  -> NestedApproxAggExec(PartialSplitLocal)
  -> CoalescePartitionsExec
  -> NestedApproxAggExec(FinalMerge)
```

This is still split-partitioned.

There is no shuffle between the scan and `PartialSplitLocal`.

That matters because the partial executor relies on split-local segment order.

## Generic Fallback Design

### Child schema contract

The child of `PartialSplitLocal` should be a `ProjectionExec` that emits:

- `__na_key_0 .. __na_key_n`
- `__na_metric_0 .. __na_metric_m`
- `_segment_ord`

The projection is where DataFusion evaluates expressions.

Examples:

- `service`
- `CASE WHEN latency > 100 THEN 'slow' ELSE 'fast' END`
- `date_bin('1m', ts)` if we choose to support that form in the fallback

The partial executor should not evaluate general expressions itself.

### Input normalization

Use the projection to normalize metric inputs to `Float64`.

Do not force key columns to `Utf8` in the projection.

Instead:

- keep the projected key columns in their natural Arrow type
- stringize them inside `PartialSplitLocal` with a shared helper

Reason:

- this gives better control over formatting
- it lets fallback match the pushdown path more closely

## Segment-aware partial execution

The fallback must not aggregate the whole split and trim once.

It must trim at segment boundaries.

### Why

The pushdown path uses tantivy `DistributedAggregationCollector`.

That collector applies `segment_size` locally before split-level merge.

If the fallback trims only once per split, it can return different winners even with the same `fanout`.

### How

`SingleTableDataSource` already scans one split per partition and emits `_segment_ord`.

`ProjectionExec` preserves row order.

That gives the partial executor exactly what it needs:

- one input partition per split
- rows grouped by increasing `_segment_ord`

### Algorithm

`PartialSplitLocal` keeps two trees:

1. `segment_tree`
2. `split_tree`

For each projected row:

- read `_segment_ord`
- if it changed:
  - trim `segment_tree` to per-level `fanout`
  - merge `segment_tree` into `split_tree`
  - reset `segment_tree`
- insert the row into `segment_tree`

At end of input:

- flush the last `segment_tree`
- optionally trim `split_tree` to `fanout`
- emit `split_tree` as a node-table partial batch

That gives:

- segment-local pruning
- one node-table partial per split
- the same scatter/gather contract as pushdown

## Fanout Properties

Each level already has:

- `final_size`
- `fanout`

In v1:

- `fanout` means both:
  - tantivy `size`
  - tantivy `segment_size`

The generic fallback should use the same number:

- trim `segment_tree` to `fanout`
- trim `split_tree` to `fanout` before emitting partials

This keeps pushdown and fallback aligned.

If later we need finer control, split it into:

- `segment_fanout`
- `split_fanout`

Do not do that in the same change unless we hit a real need.

## Partial Node Emission

The current code can build node tables from tantivy intermediate results and from the final merged tree.

We need a third emitter:

- merge tree -> partial node-table batch

This should emit:

- structural columns
- key columns
- metric state columns

It should not finalize metrics.

### Required refactor

Factor the node-table builders so both paths can share them:

- tantivy intermediate -> partial node rows
- merge tree -> partial node rows
- merge tree -> final node rows

### Files

- `src/nested_agg/node_table.rs`
- `src/nested_agg/exec.rs`

## Plan Generation

## Pushdownability check

Add a real pushdownability decision instead of assuming pushdown.

Pushdownable if:

- every bucket key is a bare field
- every metric input is a bare field or bucket `Count`
- every bucket kind is supported by the pushdown conversion

Otherwise build the generic fallback path.

## Builder changes

`build_nested_approx_plan(...)` should branch:

### Pushdown

- keep the existing `AggDataSource(NodeTablePartial)` path

### Fallback

1. build a scan over the source with the required physical columns plus `_segment_ord`
2. insert a `ProjectionExec` that computes normalized keys and metric inputs
3. insert `NestedApproxAggExec(PartialSplitLocal)`
4. keep `CoalescePartitionsExec`
5. keep `NestedApproxAggExec(FinalMerge)`

This means `NestedApproxAggExec` needs a real partial mode again.

## `NestedApproxAggExec` mode changes

Add back:

- `PartialSplitLocal`
- `FinalMerge`

`PartialSplitLocal` needs enough config to find its normalized child columns.

Prefer schema-based lookup by reserved alias names:

- `__na_key_i`
- `__na_metric_i`
- `_segment_ord`

That keeps codec state small and avoids serializing expressions into the exec itself.

The child `ProjectionExec` already carries the expressions.

### Files

- `src/nested_agg/exec.rs`
- `src/nested_agg/plan_builder.rs`
- `src/codec.rs`

## `AggDataSource` changes

The pushdown path is close to correct already.

Keep:

- `AggOutputMode::NodeTablePartial`
- `execute_tantivy_intermediate_agg_with_reader(...)`
- conversion via `intermediate_results_to_node_table_batch(...)`

Required cleanup:

1. Remove stale comments that still promise a generic fallback inside this module.
2. Keep `AggDataSource` focused on pushdown partials only.
3. If `fanout`/overscan logic changes, keep it in `NestedApproxAggSpec::to_tantivy_aggregations()`.

`AggDataSource` should not know about generic fallback execution.

That belongs in `NestedApproxAggExec(PartialSplitLocal)`.

## Codec and Distributed Execution

The codec already supports:

- `AggDataSource(NodeTablePartial)`
- `NestedApproxAggExec(FinalMerge)`

Add codec support for:

- `NestedApproxAggExec(PartialSplitLocal)`

The partial mode must roundtrip with its child `ProjectionExec`.

The worker should deserialize:

- scan
- projection
- partial nested agg

and execute split-local partials there.

## Distributed Optimization Follow-up

There is a relevant planning pattern in DataFusion Distributed:

- PR: [datafusion-contrib/datafusion-distributed#396](https://github.com/datafusion-contrib/datafusion-distributed/pull/396)
- Issue: [datafusion-contrib/datafusion-distributed#360](https://github.com/datafusion-contrib/datafusion-distributed/issues/360)

That work adds a planner pass after distribution planning that rewrites:

```text
NetworkShuffleExec
  RepartitionExec(Hash(...))
    AggregateExec(Partial)
```

into:

```text
NetworkShuffleExec
  AggregateExec(PartialReduce)
    RepartitionExec(Hash(...))
      AggregateExec(Partial)
```

The important idea is not the exact `AggregateExec(PartialReduce)` operator.
That operator is for exact flat aggregate states.

The important idea is the planner pattern:

- distribute first
- identify a hash-partitioned pre-network stage
- insert a local reduce step above the repartition and below the network
- reduce duplicate partial state rows before they cross the network

That pattern is relevant here.

It does **not** replace the nested approx leaf operator.

The nested approx leaf still owns:

- segment-local trimming
- split-local merge
- `fanout`
- node-table partial emission

But once we have node-table partials, we can apply the same planning idea with
our own custom reduce mode:

```text
... split-local nested partials ...
  -> RepartitionExec(Hash(bucket_path_hash))
  -> NestedApproxAggExec(PartialReduce)
  -> NetworkShuffleExec
  -> NestedApproxAggExec(FinalMerge)
```

Where `NestedApproxAggExec(PartialReduce)` would:

- merge duplicate node-table paths exactly
- merge metric state columns
- not apply final trimming
- not introduce any new approximation

This should be treated as a later optimization phase, not part of the core v1
correctness work.

## Testing Plan

## 1. Semantic correctness

Add tests for:

- `MetricSpec::Count` equals bucket `__count`
- missing deepest field does not change `Count`
- multi-valued deepest field does not inflate `Count`
- if `ValueCount` is later added, it differs from `Count` in the expected way

## 2. Pushdown and fallback equivalence

Add a new test file:

- `tests/nested_agg_equivalence.rs`

Cases:

- `terms -> terms -> avg`
- `terms(expr) -> terms -> avg`
- `terms -> expr(metric) -> avg`
- `date_histogram -> terms`

For each case:

- run pushdown when possible
- force fallback on the same logical query
- compare final node-table output

This is the main semantic lock.

## 3. Segment-boundary correctness

Add a dedicated test file:

- `tests/nested_agg_segment_semantics.rs`

Construct data where:

- `fanout = 1` loses a global winner
- `fanout = 2` recovers it

Run both:

- pushdown
- generic fallback

and assert the same visible winners.

## 4. Streaming behavior

Add a test that feeds many partial batches into `FinalMerge` and asserts:

- batches are ingested incrementally
- no `Vec<RecordBatch>` buffering remains

This can be unit-tested by factoring the merge loop into a helper and feeding a synthetic stream.

Also add a partial executor test that spans multiple `_segment_ord` values across multiple input batches.

## 5. Codec coverage

Extend `tests/codec_roundtrip.rs` to cover:

- `NestedApproxAggExec(PartialSplitLocal)`
- `ProjectionExec -> NestedApproxAggExec(PartialSplitLocal)`
- full partial/final nested plan roundtrip

## 6. DateHistogram coverage

Add end-to-end tests for:

- pushed-down date histogram
- fallback date histogram
- mixed `date_histogram -> terms`

Right now the code path exists but is under-tested.

## 7. Fuzz / randomized cross-check

Add a small randomized test that:

- generates a few splits
- generates a few segments per split
- generates categorical data
- compares pushdown and fallback outputs

This should be small and deterministic but will catch path-merging bugs.

## Rollout Order

### Phase 1

- fix `Count`
- make `FinalMerge` streaming
- add missing tests

This is low risk and improves current pushdown correctness immediately.

### Phase 2

- implement `PartialSplitLocal`
- add projection normalization in the builder
- add codec support
- add pushdown/fallback equivalence tests

This unlocks non-pushdown support.

### Phase 3

- add `DateHistogram` fallback coverage
- harden type handling
- consider explicit `segment_fanout` vs `split_fanout` only if needed

## Definition of Done

The feature is in a good v1 state when:

1. `MetricSpec::Count` is bucket `doc_count`, not fielded value-count.
2. `FinalMerge` ingests partial batches incrementally.
3. Non-pushdown queries over fast-field expressions run through `PartialSplitLocal`.
4. Pushdown and fallback return the same visible buckets for the same `fanout`.
5. Codec roundtrip covers the full partial/final plan shape.
6. `terms` and `date_histogram` both have end-to-end coverage.
