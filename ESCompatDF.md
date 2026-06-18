# ES-Compatible Nested Top-K Aggregations in DataFusion

## Purpose

This document defines the conceptual model for supporting Elasticsearch-style
nested bucket aggregation trees in `quickwit-datafusion`, while preserving the
distributed semantics Quickwit uses today.

The goal is not "make SQL do ES aggregations." The goal is:

1. Represent ES aggregation semantics explicitly in a DataFusion plan.
2. Reuse Quickwit's existing split-level approximation behavior where it already
   exists today.
3. Support an exact generic path for non-tantivy sources.
4. Make the semantic boundary clear: approximation happens at the leaf partial,
   not as an accidental side effect of streaming execution.

This is a companion to `NestedTopKAgg.MD`. That document describes the operator.
This document describes the ES-compatible contract the operator must support.

## Terminology

Elasticsearch already uses "nested aggregation" to mean aggregation over
`nested` documents via a `path`.

That is not what this operator is about.

This operator models:

- ES sub-aggregation trees
- nested bucket trees
- "top K within top K within top K" bucket structures

When discussing the ES-compatible mode, this document uses "nested bucket tree"
or "sub-aggregation tree" to avoid confusion with ES `nested` document aggs.

## Why ES Aggregations Do Not Map Cleanly to SQL / DataFusion

SQL engines and DataFusion are built around exact relational aggregation:

- group rows by a key
- compute algebraic partial states
- merge partial states
- optionally sort and limit

ES aggregations are different in three important ways.

### 1. Bucket selection can be candidate-based

`terms` is not an exhaustive global group-by. Each leaf collects its local top
`shard_size` terms and sends only those candidates upward. The coordinator sees
the union of candidates, not the full set of distinct terms.

This is why ES and Quickwit expose:

- `shard_size`
- `sum_other_doc_count`
- `doc_count_error_upper_bound`

By contrast:

- `date_histogram` is exhaustive for the matched docs
- `range` is exhaustive for the configured ranges

So "approximate vs exact" is not a property of the whole query. It is a
property of each bucketed level.

### 2. Metrics are a mix of exact and sketch-based

Some metrics merge exactly:

- `sum`
- `avg`
- `min`
- `max`
- `count`

Some metrics are sketches and remain approximate even with exhaustive buckets:

- `percentiles` via t-digest
- `cardinality` via HLL

So there are two independent approximation sources:

- candidate pruning from bucket selection
- sketch error from metric representation

### 3. Parent metrics in nested bucket trees are not "post-prune" metrics

In ES-style nested bucket trees:

- level-0 service metrics are computed over all docs in the service
- level-1 endpoint metrics are computed over all docs in the endpoint within
  that service
- child pruning does not change parent metrics

This is why a flat SQL rewrite is not enough. A query like:

```text
top 50 services
  -> within each, top 20 endpoints
  -> within each, top 10 error types
  -> with avg(latency) at every level
```

cannot be expressed as "group by all keys, then prune" without either:

- shipping the full cartesian key space, or
- losing correct parent-level metrics.

## The Three Semantic Axes

The design becomes simpler if we name the axes explicitly.

### Axis 1: Bucket semantics

- `Terms`: candidate-based
- `DateHistogram`: exhaustive
- `Range`: exhaustive

Future bucket types should be classified the same way.

### Axis 2: Metric semantics

- algebraic: exact merge
- sketch-based: approximate merge

### Axis 3: Memory boundary

There are only two honest places to put the memory boundary:

1. At the emit boundary:
   Build the full tree for the partition, then prune candidate-based levels when
   emitting the partial result.
2. In a lossy mid-stream eviction algorithm:
   Drop keys before the partition is finished.

For ES-compatible semantics, only option 1 is valid.

If a parent bucket is evicted mid-scan and later rows for that key appear, the
previous state is gone. That breaks both:

- exact semantics
- ES semantics

ES and Quickwit do not define that third behavior. We should not invent it.

## Core Contracts

The operator needs to support two execution contracts.

## Contract A: `Exact`

This is the generic DataFusion path for parquet or any Arrow source.

Properties:

- full bucket membership is exact
- parent metrics are exact
- no candidate pruning during accumulation
- prune only at the partial emit boundary or final emit boundary
- algebraic metrics are exact
- sketch metrics remain approximate by construction
- no ES bucket-error metadata is emitted because no candidate buckets were
  dropped

This path may use large memory if the partition cardinality is large. That is
acceptable. It is the same class of tradeoff as DataFusion hash aggregation:

- succeed if memory fits
- fail if memory does not fit
- add spill later as an optimization, not as a semantic change

## Contract B: `EsCompatApproxLeaf`

This is the ES-compatible path for tantivy / Quickwit.

Properties:

- approximation lives at the leaf partial
- candidate-based levels prune to `shard_size` at the leaf boundary
- exhaustive levels remain exhaustive
- parent metrics are exact only within the surviving leaf buckets that were
  emitted
- global parent metrics may be approximate if the bucket itself was omitted by
  some leaves
- ES metadata is carried and merged:
  - `sum_other_doc_count`
  - `doc_count_error_upper_bound` when defined

This matches how Quickwit terms aggregation works today at the split level.

## Why Mid-Stream Pruning Must Be Rejected

The temptation is to bound memory by pruning while accumulating. That is not
compatible with either contract.

Example:

- top-50 services requested
- current accumulator has seen 1,000 rows for `service=foo`
- `foo` is evicted because it is currently below the cut line
- 10,000 more `foo` rows arrive later

Now:

- the count for `foo` is wrong
- the metrics for `foo` are wrong
- if `foo` should have entered the true top-50, the result is wrong

So the rule is:

- no lossy eviction inside the exact partial executor
- no lossy eviction inside the ES-compatible partial executor beyond the
  semantics already defined by the leaf collector API

## DF Architecture

The real execution has two distinct boundaries:

1. row-oriented execution for scan and expression evaluation
2. tree-oriented execution for partial/final aggregation

That decomposition is:

```text
Scan
  -> Expression evaluation
  -> Nested accumulation
  -> Emit partial tree state
  -> Final merge / finalize
```

This matters because tantivy pushdown is only one implementation of the first
three steps. If every grouping expression and metric input is pushdownable,
tantivy can collapse scan + accumulation into one native collector. If any level
depends on a DataFusion-only expression, the whole nested tree must fall back to
the generic Arrow accumulation path after projection.

There is no useful "push down level 0 and level 2 but not level 1" execution
for a nested bucket tree. The partial state therefore has to be something both
paths can produce.

There is one more semantic boundary that matters for ES-compatible `terms`:
tantivy cuts off candidate buckets at the segment boundary, not only at the
split boundary. A generic `EsCompatApproxLeaf` executor therefore cannot
accumulate an entire split and prune once at the end if it wants to match
tantivy exactly. It must preserve segment-local pruning and error accounting.

This is a major implementation constraint. It means the generic ES-compatible
path must do one of the following:

1. expose segments as execution partitions and aggregate per segment, or
2. keep split-level partitions in the DF plan, but execute segment-aware
   partial aggregation internally inside the split executor

Option 2 is usually the better v1 trade because it avoids exploding the
planning fan-out. But either way, segment boundaries are part of the semantic
contract, not an implementation detail.

Practically, this can be modeled as a hidden execution key rather than a
user-visible grouping level. The existing tantivy scan path already carries
`_segment_ord`, so the generic fallback can preserve segment-local aggregation
semantics without necessarily re-planning the whole distributed query around
segment partitions.

The DataFusion boundary needs to satisfy three things at once:

- correctness:
  the leaf partial must mean the same thing as a Quickwit / ES split partial
- efficiency:
  moving partials into DataFusion must not expand them into a much larger shape
- planner visibility:
  the partial/final boundary should be explicit in the DataFusion plan

This leads to three architectural options.

### Option 1: Flatten the tree into denormalized bucket rows

Example shape:

```text
__level
__count
[ancestor keys repeated on every descendant row]
[metric partials]
```

Advantages:

- relational and easy to inspect
- straightforward to emit from either path

Disadvantages:

- repeats ancestor keys on descendant rows
- expands a tree into a wider denormalized table than necessary
- makes transport cost depend on path repetition instead of node count

This works, but it pays a real space penalty at deeper levels.

### Option 2: Opaque binary aggregate state

Example shape:

```text
__agg_state: Binary
```

Advantages:

- compact
- close to native tantivy / Quickwit partials

Disadvantages:

- only a natural fit for the fully pushdownable tantivy path
- generic Arrow accumulation would need to invent its own opaque encoding
- hides too much structure from the shared final reducer

This is a good physical trick for some cases, but it is not the right shared
contract.

### Option 3: Columnar tree encoding

Encode one row per bucket node, with explicit parent pointers and only the
current node's key populated.

Example shape:

```text
__tree_id
__node_id
__parent_id
__level
__count
__bucket_semantics
__key_0 ... __key_n
[metric partial state columns]
[nullable ES metadata columns]
```

Advantages:

- one row per node, not one row per repeated path segment
- no ancestor-key repetition
- Arrow-native and inspectable
- naturally produced by both the generic accumulator and tantivy conversion
- keeps the partial/final boundary explicit inside DF

Disadvantages:

- still requires a custom final reducer
- node ids are transport-local and must be rebuilt per incoming partial

### UDAF Inspiration

DataFusion UDAFs are still the right precedent, but for the boundary rather than
for the exact wire shape.

The useful idea is:

- partial aggregation emits explicit aggregate state columns
- final aggregation consumes those state columns

The chosen design follows that pattern, but uses structured Arrow state instead
of a single opaque binary value.

### Chosen Architecture

Use option 3 for both `Exact` and `EsCompatApproxLeaf`.

The shared intermediate contract is a columnar tree encoding. The partial
executor emits one row per bucket node, with parent/child relationships encoded
via ids, and only the key for that row's level populated.

Recommended shape:

```text
__tree_id:                  UInt32
__node_id:                  UInt32
__parent_id:                UInt32
__level:                    UInt8 nullable
__count:                    UInt64 nullable
__bucket_semantics:         UInt8 nullable
__key_0 ... __key_n:        typed, nullable
[metric partial state columns]
__child_sum_other_doc_count: UInt64 nullable
__child_doc_count_error:     UInt64 nullable
```

Semantics:

- each non-root row is one bucket node
- `__node_id` is unique only within one emitted tree
- `__parent_id` points to the parent node in that same tree
- only `__key_<level>` is populated for a given row
- `__child_sum_other_doc_count` and `__child_doc_count_error` are owned by the
  row whose children they describe
- a synthetic root row may be emitted for top-level candidate metadata

This keeps the tree structure explicit without repeating ancestor keys.

### Why This Is Correct

It is correct for ES-compatible semantics because:

- the generic path evaluates expressions first, then accumulates the same tree
  the final reducer expects
- the generic `EsCompatApproxLeaf` path is segment-aware, so its cutoffs and
  error bookkeeping happen at the same boundary tantivy uses
- the tantivy path converts `IntermediateAggregationResults` into that same tree
  encoding
- approximation still happens only at the leaf boundary where ES defines it
- the final reducer reconstructs each incoming partial tree from
  `(__tree_id, __node_id, __parent_id)` and merges nodes by logical identity:
  `(level, merged_parent, node_key)`
- ES metadata stays attached to the relevant aggregation context and is merged
  explicitly rather than inferred

In short:

- approximation happens exactly where ES already defines it
- final reduction inside DF adds no extra approximation

### Why This Is Efficient

It is efficient because:

- the intermediate size is proportional to node count, not ancestor-path
  repetition
- each row carries only one key, not the full key path
- both the generic accumulator and the tantivy converter can emit it in `O(n)`
  over the number of nodes
- the final reducer can merge it with normal hash/tree logic without first
  expanding an opaque blob or re-parsing a denormalized path table

This is the best trade for the shared path:

- correctness from a single semantic contract
- efficiency from compact node-oriented transport
- acceptable code quality because the boundary remains explicit and Arrow-native

The main cost is complexity in the generic ES-compatible path: segment-local
pruning means the partial executor cannot be a naive split-wide Arrow group-by.
That is the real bottleneck for full semantic parity.

## Intermediate State Contract

The shared intermediate contract is a node table, not a denormalized path table
and not an opaque binary blob.

Required properties:

- one row per bucket node
- parent/child relationships encoded via ids
- one typed key column per level, with exactly one populated per row
- metric partial states carried on the node row
- ES candidate metadata carried on the owning parent row, with a synthetic root
  row for top-level candidate metadata if needed

The final API output does not have to match this schema. The final reducer can
consume the node table, merge the tree, and emit:

- final bucket rows
- a nested object/JSON-like structure
- any other stable output contract the API layer expects

The important point is that both the generic Arrow path and the tantivy
pushdown path can produce this contract efficiently.

## What Must Be Preserved from ES / Quickwit Semantics

For ES-compatible mode, the following behaviors matter.

### Terms semantics

- each leaf emits only local top `shard_size` candidates
- final result may be approximate
- `sum_other_doc_count` must be preserved
- `doc_count_error_upper_bound` must be preserved when valid
- tie-breaking must match ES

Important caveat:

- `_count desc` is the normal, bounded-error case
- `_key asc|desc` is safe
- `_count asc` is discouraged by ES and effectively approximate in a different
  way
- general sub-aggregation ordering is not universally safe

For v1, the operator should be explicit about which orderings it treats as
ES-compatible and which ones it rejects or marks as unsupported.

### Date histogram semantics

These are exhaustive bucket levels, but still require ES-compatible formatting
and parameters:

- fixed vs calendar interval
- timezone
- offset
- key formatting
- `min_doc_count`
- bounds

These are not candidate pruning problems. They are bucketing and formatting
problems.

### Range semantics

These are also exhaustive levels, but need ES-compatible handling for:

- `from` inclusive
- `to` exclusive
- bucket key formatting
- keyed responses

## Mapping to Quickwit and Tantivy

The Quickwit scatter/gather path already has the semantic boundary we need.

### What Quickwit does today

At the leaf:

- build tantivy aggregation partials per split
- serialize intermediate aggregation results

At the root:

- merge `IntermediateAggregationResults`
- finalize to ES-compatible output

Relevant existing hooks:

- `quickwit-search/src/collector.rs`
  - merges `IntermediateAggregationResults` across leaf responses
- `quickwit-search/src/root.rs`
  - finalizes aggregation results at the root
- `quickwit-proto/protos/quickwit/search.proto`
  - `skip_aggregation_finalization` already allows returning raw intermediate
    bytes instead of finalized output

That is exactly the seam needed for `quickwit-datafusion`.

### Important API distinction in `tantivy-datafusion`

Today there are two tantivy aggregation APIs in play:

1. `AggregationCollector`
   - returns finalized `AggregationResults` for that searcher
2. `DistributedAggregationCollector`
   - returns `IntermediateAggregationResults`

This distinction matters.

If the DataFusion leaf node is supposed to represent the same semantics as a
Quickwit split partial, then the ES-compatible leaf path should be defined in
terms of:

- `DistributedAggregationCollector`
- `IntermediateAggregationResults`

and not in terms of:

- split-local finalized `AggregationResults`

unless we can prove those are semantically equivalent for the supported bucket
types and metadata.

### Current state in `tantivy-datafusion`

There is already useful infrastructure:

- `AggDataSource`
  - supports `FinalMerged` vs `PartialStates`
- `AggPushdown`
  - already rewrites two-phase aggregates by replacing only the partial side
- `execute_tantivy_intermediate_agg_with_reader`
  - already uses `DistributedAggregationCollector`

But the current `PartialStates` path uses `AggregationCollector` and converts
split-local `AggregationResults` into partial rows.

That may still be useful for a tabular exact path, but it is not the right
boundary for the ES-compatible path.

For ES-compatible nested top-k, that should be revisited. The new leaf node
should align with Quickwit's split partial semantics, which means using the
distributed collector API and carrying the right metadata forward.

## Plan Shapes

### ES-compatible mode

```text
NestedTopKAgg(spec, semantics=EsCompatApproxLeaf)
  -> TantivyNativePartialExec
       or ProjectionExec -> NestedTopKLeafApproxExec
  -> CoalescePartitions / distributed gather
  -> NestedTopKAggExec(Final)
```

Leaf responsibilities:

- either:
  - run tantivy distributed collector on each split and convert the result
  - or accumulate from Arrow rows after expression evaluation, but still prune
    and account for errors per segment
- emit the same node-table partial format in both cases
- preserve ES error metadata on the owning rows

Final responsibilities:

- reconstruct and merge incoming partial trees
- apply final `top_k`
- finalize to ES-compatible structural output

### Exact mode

```text
NestedTopKAgg(spec, semantics=Exact)
  -> NestedTopKAggExec(PartialExact)
  -> CoalescePartitions / distributed gather
  -> NestedTopKAggExec(Final)
```

Partial responsibilities:

- build full tree for the partition
- do not prune candidate levels mid-stream
- emit the same node-table state shape

Final responsibilities:

- merge exact node-table partials
- finalize metrics
- emit no ES error metadata for exact levels

## The Final Reducer Contract

The final reducer is shared structurally, but not semantically blind.

It must know:

- which semantics produced the incoming node rows
- which bucket levels are candidate-based or exhaustive
- whether ES error metadata is valid
- whether the metric states are exact algebraic states or sketches
- how to map each incoming node to its logical parent/key identity

The reducer is therefore:

- shared code
- different partial-production paths
- same merge/finalization responsibility

## Recommended Operator Model

Use one logical operator with an explicit semantics field:

```rust
enum AggSemantics {
    Exact,
    EsCompatApproxLeaf,
}
```

Why this is better than separate unrelated operators:

- the user-facing query shape is one concept: nested bucket tree aggregation
- semantics are planner-visible
- physical planning can pick different partial executors
- testing can compare both contracts directly

Separate physical executors are still appropriate.

## What This Means for Quickwit-DataFusion

`quickwit-datafusion` should not try to force ES semantics into regular
`AggregateExec`.

Instead it should:

1. Parse ES aggregation trees into a dedicated logical node.
2. Carry semantics explicitly in that node.
3. Let physical optimization decide whether the leaf partial can be pushed down
   to tantivy.
4. Keep the final merge in DataFusion so nested tree reconstruction and
   non-tantivy execution share one path.

This gives a clean division of labor:

- tantivy / Quickwit leaf collector:
  owns ES-compatible candidate semantics
- DataFusion final reducer:
  owns tree merge, final ordering, and unified output shape

And it gives a clean expression fallback:

- if all levels are pushdownable, use tantivy native partials
- if any level depends on a DF-only expression, evaluate it in Arrow and emit
  the same node-table partial format from the generic partial executor

However, full ES compatibility in that fallback path still requires
segment-aware execution. This is the main architectural bottleneck: the DF plan
can remain split-partitioned, but the partial executor must internally preserve
tantivy's segment cutoff semantics. A more DF-native alternative would expose
segments as plan partitions, but that is a much larger planning change.

The existing hidden `_segment_ord` scan column gives a concrete hook for this:
the generic partial executor can treat it as an internal execution key and run a
segment-local pre-aggregation tier before producing the split-local partial
tree.

This also fits the current `quickwit-datafusion` planning model: source-specific
optimizer rules already run before the distributed rule inspects the physical
plan, so the nested top-k pushdown can choose its leaf partial form before task
partitioning is fixed.

## Scope for V1

The first implementation should be narrow and honest.

### Must support

- nested bucket trees made of:
  - `terms`
  - `date_histogram`
  - `range`
- metrics:
  - `sum`
  - `avg`
  - `min`
  - `max`
  - `count`
  - `percentiles`
  - `cardinality`
- two semantics:
  - `Exact`
  - `EsCompatApproxLeaf`
- segment-aware generic execution for `EsCompatApproxLeaf` fallback, so `terms`
  cutoff and error bookkeeping happen at tantivy's semantic boundary

### Must not do

- mid-stream lossy pruning in the generic path
- hide approximation behind a generic "bounded memory" story
- pretend all orderings have the same guarantees

### Can wait

- spill for exact generic partials
- full ES parameter parity in the generic path
- rare terms
- composite aggregation
- pipeline aggs
- every ES formatting knob

## Concrete Implementation Plan

### Phase 1: Semantics and logical planning

- add `AggSemantics` to the nested top-k spec
- define per-level bucket semantics in the spec or derived metadata
- add a new design note in `quickwit-datafusion` explaining that this operator
  models ES sub-aggregation trees, not ES `nested` document aggs

### Phase 2: Intermediate schema

- define the node-table schema
- define parent-owned ES metadata fields and the root-sentinel rule
- define per-level key columns and metric state columns

### Phase 3: Tantivy leaf path

- create a dedicated ES-compatible partial leaf executor/data source
- base it on `DistributedAggregationCollector`
- preserve the same leaf semantics Quickwit uses today
- convert `IntermediateAggregationResults` into node-table batches
- use ES-compatible `size` / `shard_size`, not "keep everything" sizing

### Phase 4: Generic ES-compatible fallback

- implement a segment-aware Arrow partial executor for `EsCompatApproxLeaf`
- evaluate DF expressions before accumulation
- prune `terms` and compute error metadata per segment, then merge to the
  split-local partial tree
- emit the same node-table schema as the tantivy path

### Phase 5: Final reducer

- reconstruct incoming partial trees from node rows
- merge nodes by `(level, parent, key)`
- support final `top_k` and deterministic tie-breaking
- emit a stable final output shape for the API layer

### Phase 6: Quickwit integration

- map ES query parsing to the new logical node
- ensure pushdown runs before distributed partitioning
- use the existing Quickwit scatter/gather seam where
  `skip_aggregation_finalization` already exposes intermediate bytes

### Phase 7: Exact generic path

- implement full-tree generic partial executor
- fail on memory pressure rather than silently approximating
- add spill later if needed

## Decision Summary

The design should commit to the following:

- approximation is a leaf contract, not a streaming heuristic
- bucket semantics are per level, not global
- exact and ES-compatible are both first-class semantics
- the ES-compatible leaf path should align with Quickwit's existing split
  semantics
- full semantic parity for `EsCompatApproxLeaf` also requires respecting
  tantivy's segment-local cutoff boundary in the generic fallback path
- for that path, `DistributedAggregationCollector` /
  `IntermediateAggregationResults` is the correct tantivy API boundary
- the shared DF partial/final boundary should use a columnar node-table
  encoding, not a denormalized path table or an opaque blob

That gives a clear path to support ES queries in `quickwit-datafusion` the way
Quickwit supports them today, while still allowing a generic exact path for
non-tantivy sources.
