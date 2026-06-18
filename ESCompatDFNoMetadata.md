# ES-Compatible Nested Aggregations in DataFusion Without Approximation Metadata

## Goal

Support search-style nested top-k aggregations in DataFusion without carrying
approximation metadata such as `sum_other_doc_count` or
`doc_count_error_upper_bound`.

This keeps the visible bucket semantics of local candidate pruning, but drops
uncertainty reporting. The result is simpler than full ES-compatible metadata,
while still preserving the main execution shape used by tantivy and Quickwit.

## Vocabulary

### Query tree

The aggregation query itself is a tree.

Example:

```text
top 50 services
  -> within each, top 20 endpoints
  -> within each, top 10 error types
```

That is not a storage artifact. It is the query shape.

### Bucket level

One grouping step in the tree.

In the example above:

- level 0: `service`
- level 1: `endpoint`
- level 2: `error_type`

### `terms`

Group by distinct field values and keep only the top buckets. This is the
candidate-pruned bucket type.

### `date_histogram`

Group by time interval. This is exhaustive for matched documents.

### `range`

Group by configured numeric or date ranges. This is exhaustive for the defined
ranges.

### Segment

The lowest storage unit relevant to tantivy aggregation. `terms` pruning
happens here.

### Split

A Quickwit storage unit containing one or more segments. Reader execution works
at the split level.

### Leaf

The lowest aggregation stage in distributed execution. In the pushdown path,
the leaf is tantivy running on one split. In the generic path, the leaf is the
DF partial executor operating over split-local Arrow batches.

### Final

The coordinator-side merge and final trim.

### Local trim

Keep only the top candidate buckets at a local boundary. In tantivy `terms`,
the critical local boundary is the segment.

### Final trim

After all local partials merge, keep only the user-requested top buckets.

### Final size

The user-visible top-k for a `terms` level.

Example:

```text
terms(service, size = 50)
```

`50` is the final size.

### Fanout

The local candidate limit used before final merge. Fanout is larger than final
size.

Example:

```text
final size = 50
fanout = 200
```

The leaf keeps up to 200 service buckets. The final stage keeps 50.

Every `terms` level has two independent planning properties:

- `final_size`
- `fanout`

`date_histogram` and `range` do not use fanout because they are exhaustive.

### Pushdown

The storage engine performs the leaf aggregation itself.

### Non-pushdown

DataFusion scans Arrow rows, evaluates expressions, and performs the leaf
aggregation itself.

### Node table

The shared Arrow intermediate format.

One row is one bucket node:

- node id
- parent id
- level
- this node's key
- this node's count
- this node's metric partial state

This is a tree encoded as a table. It is not a denormalized path table.

## The No-Metadata Contract

The system still does local candidate pruning. It simply does not report
uncertainty metadata in the final result.

That means:

- local fanout still affects which buckets survive
- final results may still be approximate
- final output contains buckets and metrics only
- no uncertainty fields need to travel through DF

This is the key simplification. The shared intermediate no longer needs:

- discarded count totals
- cutoff counts
- count uncertainty bounds

It only needs the retained partial tree.

That removes the main reason to use an opaque binary state column as the shared
contract. The shared contract can instead be Arrow rows in node-table form.

## Shared Intermediate

The shared intermediate is a node table.

Suggested shape:

```text
__tree_id
__node_id
__parent_id
__level
__count
__key_0 ... __key_n
[metric partial state columns]
```

Properties:

- one row per retained bucket node
- no ancestor-key repetition
- no approximation metadata columns
- usable by both pushdown and non-pushdown paths

`__node_id` and `__parent_id` are transport-only. They do not appear in the
final user-facing Arrow output.

## Pushdown Path

The pushdown path is the simplest case.

1. The planner sees that all bucket keys and metrics can run in tantivy.
2. For each `terms` level, the planner sets:
   - `final_size`
   - `fanout`
3. The leaf uses tantivy's `DistributedAggregationCollector`.
4. Tantivy performs:
   - segment-local `terms` trim to fanout
   - split-local merge of segment partials
5. The leaf converts the split-local intermediate tree into the node table.
6. A DF final node merges split partials and trims to `final_size`.

Conceptually:

```text
split scan
  -> tantivy distributed collector
  -> node table
  -> DF final merge
  -> final trim to user size
```

This path preserves tantivy's candidate-pruned semantics without requiring DF
to reproduce them from raw rows.

## Non-Pushdown Path

The non-pushdown path exists for the queries tantivy cannot evaluate directly.

Example:

```text
terms(service)
  -> terms(CASE WHEN latency_ms > 1000 THEN 'slow' ELSE 'fast' END)
```

The scan can still come from tantivy fast fields. The expression cannot.

So the path becomes:

```text
split scan to Arrow
  -> ProjectionExec
  -> DF partial nested agg
  -> node table
  -> DF final merge
```

This path must still respect the same `fanout` properties at each `terms`
level.

The hard part is the local pruning boundary.

If the goal is only "approximate trimmed group-by", DF can aggregate the whole
split and trim once before emit.

If the goal is "match tantivy's visible winners", DF must respect tantivy's
segment-local pruning boundary, because local trim changes which buckets
survive, not just metadata.

## Segment-Level Trimming

Tantivy `terms` does not prune once per split. It prunes once per segment, then
merges segment partials into a split-local intermediate tree.

To match that shape in DF, the non-pushdown path needs three stages:

1. segment partial
2. split merge
3. final merge

Conceptually:

```text
docs in segment
  -> segment partial tree
  -> trim to fanout

segment partials in split
  -> split merge

split partials across query
  -> final merge
  -> final trim to final_size
```

Without uncertainty metadata this is still approximate. It is simply approximate
without explicit error reporting.

## Two Ways To Do Segment-Level Trimming In DF

### Option 1: Explicit segment partitioning

Make segment the physical partition boundary for the segment partial stage.

Conceptually:

```text
Scan
  -> ProjectionExec
  -> Repartition by (split_id, segment_ord)
  -> NestedTopKAggExec(SegmentPartial, fanout)
  -> Repartition by split_id
  -> NestedTopKAggExec(SplitMerge)
  -> Coalesce / Repartition
  -> NestedTopKAggExec(Final, final_size)
```

This is the most explicit formulation. It makes the semantic boundary visible in
the plan.

It also has costs:

- more planning work
- more shuffling
- segment identity must be available to the plan

### Option 2: Segment-aware split partial

Keep the plan split-partitioned. Make the DF partial executor segment-aware.

Conceptually:

```text
Scan split
  -> ProjectionExec
  -> NestedTopKAggExec(SegmentAwareSplitPartial, fanout)
  -> Coalesce / Repartition
  -> NestedTopKAggExec(Final, final_size)
```

Inside `SegmentAwareSplitPartial`:

- batches are read in segment order
- the executor resets its segment-local partial on segment boundaries
- it trims to fanout at each segment boundary
- it merges segment partials into one split-local node table

This is simpler operationally. It avoids exposing segment partitions to the full
DF distributed plan.

It still matches the same semantic shape.

## Which Option To Prefer

Both should remain valid.

Use explicit segment partitioning when:

- the plan must expose segment-local execution directly
- streaming and memory behavior are easier to reason about as separate stages
- the optimizer can tolerate the extra repartitioning

Use a segment-aware split partial when:

- the scan already yields data segment-by-segment
- split is the natural DF partition
- the extra segment shuffle would be wasteful

The codebase already has a hidden `_segment_ord` scan column. That makes the
second option practical even when the plan is not explicitly segment
partitioned.

## Fanout Properties

Fanout must be explicit in the plan or in the generated operator spec.

For each `terms` level:

- `final_size` is the user-visible top-k
- `fanout` is the local retained bucket limit before merge

Example:

```text
level 0 service:    final_size = 50, fanout = 200
level 1 endpoint:   final_size = 20, fanout = 80
level 2 error_type: final_size = 10, fanout = 40
```

Pushdown and non-pushdown paths must use the same fanout values if they are
expected to return the same visible winners.

Fanout is a property of the plan, not a hidden storage default.

## What This Buys Us

Dropping approximation metadata gives a simpler and cleaner architecture:

- no uncertainty fields in the result
- no uncertainty fields in the intermediate schema
- no need for a shared opaque binary state column
- one Arrow node-table intermediate for both paths

The remaining hard problem is not metadata. It is pruning boundary:

- pushdown path gets segment-local pruning from tantivy
- non-pushdown path must either model segment-local pruning too or accept a
  different approximation behavior

That is the real semantic choice.

## Recommended Shape

For now, the cleanest shape is:

- shared node-table intermediate
- explicit `final_size` and `fanout` per `terms` level
- pushdown path uses tantivy's distributed collector
- non-pushdown path supports:
  - split-local trim for a simpler approximate fallback
  - segment-local trim when semantic parity with tantivy matters

That keeps the final architecture flexible. It does not force the segment
decision into one implementation path too early.
