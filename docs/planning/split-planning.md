# Split Planning Architecture

This document defines the planner/worker boundary for Quickwit-backed
`tantivy-datafusion` execution.

The main goal is to make distributed execution work cleanly when:

- planning and execution do not happen on the same node
- the planner must not open splits
- workers must reuse split runtime state
- DataFusion partitions are split-based
- the execution path uses Tantivy's async APIs where they exist, similar to
  Quickwit's searchers

## Design Goals

- Match Quickwit's root/leaf architecture as closely as possible.
- Keep split opening and cache population on workers only.
- Serialize only immutable split descriptors across the plan boundary.
- Reuse worker-local split runtime across scan, `_document` fetch, and native
  aggregation.
- Partition by split, not by segment.
- Keep Tantivy collector execution synchronous, but move split opening,
  warmup, and document fetch onto async paths.

## Non-Goals

- Segment-level DataFusion partitioning in the initial distributed design.
- Planner-time split opening to discover segment metadata.
- Shipping `Index`, `IndexReader`, `Searcher`, or directory handles across the
  codec boundary.

## Quickwit Parity

Quickwit root planning does not open splits. It:

- resolves index metadata and doc-mapper state
- lists relevant `SplitMetadata` from the metastore
- builds jobs carrying split footer offsets
- sends those split descriptors to searcher nodes

Quickwit leaf execution opens the split locally, warms it locally, and then
executes Tantivy search over the opened split.

`tantivy-datafusion` should follow the same pattern.

## Core Rule

Planning must operate on split descriptors.

Execution must operate on prepared split runtime state.

That is the entire boundary.

## Planner-Side Responsibilities

The planner, or coordinator, is allowed to do only the following:

- resolve index metadata
- choose the canonical output schema
- list relevant `SplitMetadata`
- prune splits using split-level metadata
- construct one DataFusion partition per split
- serialize split descriptors and query/agg information

The planner must not:

- open splits
- inspect segment readers
- build `IndexReader`
- build `Searcher`
- read fast-field column metadata from storage-backed splits

## Worker-Side Responsibilities

The worker, or searcher, is responsible for:

- reconstructing split runtime from serialized split descriptors
- resolving storage from split metadata
- opening the split with Quickwit cache layers
- building `IndexReader` and `Searcher`
- warming the split based on the actual query shape
- reusing that prepared split runtime for scan, `_document`, and aggregation

## Partitioning Model

Partitioning is by split.

One split maps to one DataFusion partition.

Inside a split partition, the worker may iterate multiple Tantivy segments, but
those segments are internal execution details and are not separate DataFusion
partitions.

This matches Quickwit root planning, which has split metadata but not segment
inventory.

## Why Not Partition By Segment

Quickwit root planning does not know segment layout before leaf execution.
Segment ordinals appear only after the split is opened on the worker.

Therefore segment-level DataFusion partitioning would require extra serialized
metadata beyond what Quickwit root currently uses.

The initial distributed architecture should not depend on that.

## Data Model

There are three important runtime/planning types.

### 1. Index Resolution

This belongs in `quickwit-datafusion`.

It resolves:

- index UID
- index URI
- canonical schema inputs
- split metadata source

Suggested shape:

```rust
pub struct ResolvedIndex {
    pub index_uid: IndexUid,
    pub index_uri: quickwit_common::uri::Uri,
}
```

This should no longer return an already-resolved storage handle for planner-time
construction of openers.

### 2. Split Descriptor

This is the planner-to-worker serialized unit.

Suggested shape:

```rust
pub struct SplitDescriptor {
    pub split_id: String,
    pub num_docs: u64,
    pub payload: Vec<u8>,
}
```

For Quickwit, `payload` should serialize the worker reconstruction data:

```rust
struct QuickwitSplitPayload {
    index_uri: String,
    split_id: String,
    footer_start: u64,
    footer_end: u64,
    num_docs: u64,
    timestamp_start: Option<i64>,
    timestamp_end: Option<i64>,
}
```

The payload must be opaque to `tantivy-datafusion`.

### 3. Prepared Split

This is the worker-local reusable runtime object.

Suggested shape:

```rust
pub struct PreparedSplit {
    pub index: tantivy::Index,
    pub reader: tantivy::IndexReader,
    pub searcher: tantivy::Searcher,
    keepalive: Arc<dyn Any + Send + Sync>,
}
```

`keepalive` keeps the hot directory, ephemeral cache, or any other split-local
runtime state alive for as long as the prepared split is in use.

## Runtime Factory

The distributed runtime boundary should be a split runtime factory, not an
index opener.

Suggested trait:

```rust
#[async_trait]
pub trait SplitRuntimeFactory: Send + Sync {
    async fn prepare_split(&self, split: &SplitDescriptor) -> Result<Arc<PreparedSplit>>;
}
```

This factory lives on the worker via a `SessionConfig` extension, similar to the
current opener factory, but it should reconstruct prepared split runtime
directly.

## Why The Current IndexOpener Contract Is Wrong

The current `IndexOpener` contract assumes:

- planning can synchronously ask for schema metadata
- planning can synchronously ask for segment sizes
- execution later calls `open() -> Index`

That contract is acceptable for local `DirectIndexOpener`, but it is the wrong
abstraction for Quickwit distributed execution.

It causes three bad outcomes:

1. Planner-time split opening to discover metadata.
2. Reopening split state on workers even when the same split should be reused.
3. Encoding worker reconstruction identity into `identifier` hacks.

The distributed architecture should replace opener-based planning with
descriptor-based planning.

## Quickwit Mapping

The intended mapping from Quickwit to DataFusion is:

- Quickwit root `SplitMetadata` -> `SplitDescriptor`
- Quickwit leaf `open_index_with_caches(...)` -> `SplitRuntimeFactory::prepare_split(...)`
- Quickwit leaf `warmup(&searcher, &warmup_info)` -> worker-side query-shaped
  warmup on `PreparedSplit`
- Quickwit fetch-docs `searcher.doc_async(...)` -> async `_document` fill using
  `PreparedSplit.searcher`

## Scan Execution Flow

For a `SingleTableDataSource` partition:

1. Decode plan and obtain the split descriptor for this partition.
2. Call `prepare_split(split).await`.
3. Run query-shaped warmup once for that prepared split.
4. Launch blocking Tantivy scan work over all segment readers in the split.
5. Emit intermediate batches without `_document`.
6. If `_document` is required, fill it asynchronously using
   `PreparedSplit.searcher.doc_async(...)`.
7. Return final batches to DataFusion.

## Document Fetch Contract

`_document` fetch must not open a fresh `IndexReader` per batch.

Instead it must:

- reuse the `PreparedSplit.searcher`
- use row-level `_segment_ord` and `_doc_id`
- fetch documents asynchronously with bounded concurrency

Because partitioning is by split, one batch may contain documents from multiple
segments within that split. `_document` fill must therefore use the row's
`_segment_ord`, not a fixed partition segment.

## Aggregation Execution Flow

Aggregation should reuse the same prepared split model.

For native agg pushdown:

- one DataFusion partition per split
- worker prepares the split once
- worker performs query-shaped warmup once
- worker runs sync Tantivy aggregation against the prepared reader/searcher

For distributed aggregation:

- each split partition emits native partial aggregation state
- DataFusion keeps the final aggregate above it
- no worker should merge all splits into a monolithic one-node final aggregate

## Warmup Model

Warmup should be query-shaped and `Searcher`-based.

It should be driven by:

- full-text query fields
- pushed fast-field filter columns
- projected fast fields
- aggregation fields
- scoring requirements

It should not require a top-level boolean like "warm everything".

The warmup surface should logically mirror Quickwit's leaf warmup.

## Worker Reuse

Reuse exists at two levels.

### Mandatory

Within one partition execution:

- a split is prepared once
- the same prepared split is reused for scan/agg/document fetch

### Recommended

Across queries on a worker:

- maintain a worker-local prepared split cache
- key it by immutable split identity

Suggested cache key:

- index URI
- split ID
- footer start
- footer end

Because published splits are immutable, this cache is safe and valuable.

## Schema Boundary

Canonical schema selection belongs above `tantivy-datafusion`, in
`quickwit-datafusion`.

`tantivy-datafusion` should accept:

- canonical output schema
- split descriptors
- pushed filter/query info
- pushed aggregation info

It should not decide schema by opening remote splits during planning.

For Quickwit logs queries, planner-time schema sources should be:

- explicit DDL schema
- index/doc-mapper metadata
- future manifest merge logic

Not "open the newest split".

## Codec Boundary

The codec must serialize:

- canonical schema
- split descriptors
- pushed raw/full-text query info
- pushed fast-field filter expressions
- aggregation specification and output mode when relevant

The codec must not serialize:

- `Index`
- `IndexReader`
- `Searcher`
- `HotDirectory`
- local cache handles

## `quickwit-datafusion` Changes

The Quickwit side should:

1. Resolve index metadata without opening splits.
2. List published splits from the metastore.
3. Convert `SplitMetadata` into `SplitDescriptor`.
4. Construct a `SingleTableProvider` or `AggDataSource` from split
   descriptors.
5. Register a `SplitRuntimeFactory` on the session config for workers.

It should stop:

- opening every split at `scan()` time just to build openers
- overloading opener identifiers with `{index_uri}\\0{split_id}`
- resolving worker storage via `block_in_place` inside an opener factory

## `tantivy-datafusion` Changes

The Tantivy side should:

1. Introduce `SplitDescriptor`.
2. Introduce `PreparedSplit`.
3. Introduce `SplitRuntimeFactory`.
4. Refactor `SingleTableDataSource` to partition by split.
5. Refactor `AggDataSource` to operate on prepared splits.
6. Refactor warmup helpers to be `Searcher`-based.
7. Refactor `_document` fill to use shared prepared split searchers.
8. Remove planner-time dependence on sync opener metadata for distributed use.

## Trait and Function Mapping

### Current

- `IndexOpener::open()` -> opens an index too late and with too little shared
  runtime state
- `IndexOpener::schema()` / `segment_sizes()` -> forces planner-time sync
  metadata assumptions
- `OpenerFactoryExt` -> worker rebuilds openers, not prepared split runtime

### Target

- `SplitRuntimeFactory::prepare_split(&SplitDescriptor)` -> worker-local split
  preparation
- `PreparedSplit` -> shared runtime for one split
- `SplitRuntimeFactoryExt` -> session config extension for distributed workers

### Scan

- current `SingleTableProvider::from_splits(openers)` -> target
  `SingleTableProvider::from_split_descriptors(descriptors, canonical_schema)`

### Aggregation

- current `AggDataSource::from_split_openers(...)` -> target
  `AggDataSource::from_split_descriptors(...)`

## Recommended Implementation Order

1. Add `SplitDescriptor` and `PreparedSplit`.
2. Add `SplitRuntimeFactory` and session-config extension support.
3. Teach the codec to serialize split descriptors instead of opener metadata.
4. Refactor `quickwit-datafusion` to build split descriptors from metastore
   data only.
5. Refactor scan execution to prepare one split per partition and reuse it.
6. Refactor `_document` fill to use the prepared split searcher with
   `_segment_ord`.
7. Refactor native aggregation paths to use prepared splits.
8. Add worker-local prepared split caching.

## Testing Requirements

Tests should prove:

- planning does not open splits
- planning and execution can occur on different nodes
- one DataFusion partition is created per split
- worker runtime is reused across scan and `_document` fetch
- worker runtime is reused across native agg execution
- `_document` fetch uses async document retrieval
- codec roundtrips work with split descriptors only
- repeated queries on the same worker hit the prepared split cache

## Summary

The final distributed architecture should mirror Quickwit:

- root plans with index metadata and split metadata only
- leaves open splits locally
- leaves warm locally
- leaves search locally
- document fetch uses async APIs on the prepared split searcher

The core correction is to replace opener-based distributed planning with
descriptor-based planning and worker-local prepared split runtime.
