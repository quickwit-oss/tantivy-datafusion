# Runtime Alignment Plan

Date: 2026-04-14

## Goal

Align `tantivy-datafusion` execution with how Quickwit actually runs Tantivy work today, while keeping the current planner/worker split and leaving room for a more explicit `dd-datafusion`-style runtime model later.

This plan is specifically for the current Quickwit PR. It is not the long-term "perfect" runtime architecture. The target for this step is:

- async split preparation on Tokio
- async warmup on Tokio
- async document fetch with `doc_async`
- bounded CPU execution for sync Tantivy scan / aggregation / merge work
- no reliance on Tokio's generic `spawn_blocking` pool for the hot search path

## What Quickwit Does Today

Quickwit's search runtime is already split into clear lanes.

Async lane:

- open split bundles with caches
- build readers and searchers
- warm up dictionaries, postings, and fast fields
- fetch documents with `Searcher::doc_async`

CPU lane:

- run Tantivy collectors
- run split-local search
- run CPU-heavy merge/finalize work

That CPU lane is not `tokio::spawn_blocking`. It is a bounded Rayon-backed `quickwit_common::thread_pool::ThreadPool`.

Relevant code:

- `quickwit-search/src/lib.rs`: `search_thread_pool()`
- `quickwit-search/src/leaf.rs`: split-local search runs through `search_thread_pool().run_cpu_intensive(...)`
- `quickwit-search/src/root.rs`: merge work also runs through the same pool
- `quickwit-search/src/fetch_docs.rs`: document fetch stays async, but Tantivy's internal executor is explicitly set from the search thread pool
- `quickwit-common/src/thread_pool.rs`: bounded Rayon-backed executor

## What `tantivy-datafusion` Does Today

The current execution split is only partially aligned.

Already aligned:

- split preparation is async via `SplitRuntimeFactory`
- warmup is mostly `Searcher`-based and async
- `_document` fetch uses `Searcher::doc_async`
- planner sends `SplitDescriptor`, workers build `PreparedSplit`

Still misaligned:

- normal scan execution uses `tokio::task::spawn_blocking`
- aggregation execution uses `tokio::task::spawn_blocking`
- aggregation merge/finalize uses `tokio::task::spawn_blocking`
- one warmup helper still opens a reader through `spawn_blocking`
- prepared splits do not currently set Tantivy's internal executor

Relevant code:

- `src/unified/single_table_provider.rs`
- `src/unified/agg_data_source.rs`
- `src/warmup.rs`

## What `dd-datafusion` Does

`dd-datafusion` has a more explicit runtime model:

- a CPU Tokio runtime
- an IO Tokio runtime
- execution-plan wrappers that force work onto the appropriate runtime

Relevant code:

- `dd-datafusion/runtime/src/tokio_runtimes.rs`
- `dd-datafusion/runtime/src/io_executor.rs`
- `dd-datafusion/runtime/src/io_table_provider.rs`

This is a good long-term reference model, but it is not how Quickwit search currently executes Tantivy collectors. For the current PR, we should follow Quickwit's existing model first.

## Decision

For this PR, `tantivy-datafusion` should match Quickwit's runtime model, not `dd-datafusion`'s full runtime segregation model.

That means:

- keep async prep, warmup, and `doc_async`
- replace `spawn_blocking` for sync Tantivy work with a bounded CPU executor
- in Quickwit integration, back that executor with `quickwit_common::thread_pool::ThreadPool`
- set Tantivy's internal executor from that same pool when preparing a split

## Architecture

### Planner / Worker Boundary

This does not change.

Planner:

- resolves canonical schema
- resolves split metadata
- serializes `SplitDescriptor`

Worker:

- reconstructs `PreparedSplit` through `SplitRuntimeFactory`
- executes scan / agg against the prepared split

The runtime change is inside worker-side execution only.

### Execution Lanes

Async lane:

- `SplitRuntimeFactory::prepare_split`
- split open with caches
- warmup
- `_document` fetch via `doc_async`

CPU lane:

- scan batch generation over Tantivy segment readers
- split-local aggregation execution
- intermediate aggregation merge
- final aggregation materialization

### New Abstraction

Add a worker-session injectable sync execution abstraction next to `SplitRuntimeFactory`.

The generic crate should not depend directly on `quickwit_common`.

`tantivy-datafusion` should define the interface.

Quickwit should provide the implementation.

## Proposed API Shape

Add a new module, tentatively:

- `src/sync_exec.rs`

Expose a session extension similar to `SplitRuntimeFactoryExt`.

Suggested surface:

```rust
pub trait SyncExecutionPool: Send + Sync {
    async fn run_boxed(
        &self,
        task: Box<dyn FnOnce() -> datafusion::common::Result<Box<dyn Any + Send>> + Send>,
    ) -> datafusion::common::Result<Box<dyn Any + Send>>;
}
```

That exact signature may be refined. The important constraints are:

- object-safe
- session-config injectable
- usable from both scan and agg paths
- default fallback implementation available in `tantivy-datafusion`

The generic crate should also expose:

- `SyncExecutionPoolExt` for `SessionConfig`

Default fallback:

- a Tokio-based implementation using `spawn_blocking`

Quickwit implementation:

- a bounded Rayon-backed implementation using `quickwit_common::thread_pool::ThreadPool`

## Why Not Use `quickwit_common::RuntimeType::Blocking`

Because that is not how Quickwit search currently runs Tantivy collectors.

Quickwit search uses:

- Tokio async for prep and warmup
- `quickwit_common::thread_pool::ThreadPool` for CPU-heavy search and merge work

`RuntimeType::Blocking` is a separate Tokio runtime used elsewhere in Quickwit. It is not the search execution model we are trying to match.

## Why Not Keep `spawn_blocking`

`spawn_blocking` works as a bridge, but it is not the right long-term or even short-term search execution substrate here.

Problems:

- Tokio's blocking pool can expand under load
- it gives weaker admission control than Quickwit's bounded search pool
- it does not match Quickwit's search runtime
- it makes the hot search path less predictable operationally

## Implementation Plan

### Phase 1: Introduce sync execution injection in `tantivy-datafusion`

Files:

- `src/sync_exec.rs`
- `src/lib.rs`

Work:

- add a session extension for sync execution
- add a default `spawn_blocking`-backed implementation
- make the abstraction available to scan and agg execution code

Acceptance:

- no behavior change yet
- tests still pass

### Phase 2: Route scan execution through the new abstraction

Files:

- `src/unified/single_table_provider.rs`

Work:

- replace direct `tokio::task::spawn_blocking` usage in scan batch generation
- keep async batch forwarding and async `_document` fill unchanged
- preserve cancellation checks

Acceptance:

- scan path no longer directly calls `spawn_blocking`
- behavior unchanged under default fallback implementation

### Phase 3: Route aggregation execution through the new abstraction

Files:

- `src/unified/agg_data_source.rs`

Work:

- replace direct `spawn_blocking` in:
  - split-local intermediate aggregation
  - single-split final aggregation
  - partial-state batch production
  - merge/finalize work

Acceptance:

- agg path no longer directly calls `spawn_blocking`
- distributed partial-state plan shape is unchanged

### Phase 4: Finish the warmup runtime cleanup

Files:

- `src/warmup.rs`

Work:

- remove or demote index-based warmup helpers that reopen readers with `spawn_blocking`
- keep warmup on `Searcher`-based async APIs
- make any remaining reader-open helper explicit as transitional if it must remain

Acceptance:

- no hot-path warmup dependency on `spawn_blocking`

### Phase 5: Add Quickwit sync executor implementation

Files:

- `quickwit-datafusion/src/sources/tantivy/mod.rs`
- new file, likely `quickwit-datafusion/src/sources/tantivy/search_executor.rs`

Work:

- create a dedicated `quickwit_common::thread_pool::ThreadPool` for DataFusion Tantivy search work
- inject it through `SessionConfig` in `configure_session`
- do not reuse the legacy `quickwit-search` global pool directly

Reason:

- avoids coupling DataFusion query load to existing search-service pool behavior
- keeps the execution model the same while isolating operational load

Acceptance:

- worker sessions install both:
  - `SplitRuntimeFactory`
  - sync execution pool

### Phase 6: Set Tantivy's internal executor on prepared splits

Files:

- `quickwit-datafusion/src/sources/tantivy/prepared_split_factory.rs`

Work:

- after opening the split and before constructing `PreparedSplit`, call `index.set_executor(...)`
- derive the executor from the same Quickwit DF search thread pool

Reason:

- matches Quickwit's own fetch-docs behavior
- avoids nested/default executor mismatches inside Tantivy

Acceptance:

- prepared splits consistently use the intended CPU pool for Tantivy internal work

### Phase 7: Tighten tests so they run the real path

Files:

- `tantivy-datafusion/tests/...`
- `quickwit-integration-tests/src/tests/tantivy_datafusion_tests.rs`

Work:

- add coverage that local tests can run via injected sync executor
- ensure Quickwit integration tests exercise:
  - `SplitDescriptor`
  - `SplitRuntimeFactory`
  - Quickwit sync execution pool
  - distributed worker decode path

Required integration coverage:

- full-text query with `_document` and `_score`
- top-k query path
- distributed grouped aggregation pushdown
- plain full-text scan path

## Missing Gaps To Watch

### Cancellation Semantics

Quickwit's `ThreadPool::run_cpu_intensive` only avoids starting work if the caller drops interest before execution begins.

It does not preempt work once it is already running.

This is still better than unbounded `spawn_blocking`, but it does not solve true mid-query preemption.

We should preserve internal cancellation checks where possible.

### Query Duration vs Pool Choice

`quickwit_common::run_cpu_intensive(...)` is documented for small tasks.

That is not the API we should use.

We should instantiate a dedicated `ThreadPool::new("tantivy-df-search", Some(...))` or similar, because split-local search and aggregation can be long-running.

### Cache Interaction

This plan does not solve prepared-split cache policy.

It only ensures the execution lane is bounded and Quickwit-aligned.

### Search Semantics

This plan does not solve:

- deterministic tie-breaking
- `search_after`
- late document fetch
- global top-k semantics beyond existing behavior

Those remain separate work.

## What This Leaves For Later

This plan intentionally does not implement the full `dd-datafusion` runtime model.

Later work can move toward:

- explicit CPU vs IO runtimes
- execution-plan wrappers similar to `IOExec`
- richer runtime control at the DataFusion layer

That is a valid long-term direction, but it should not block aligning this PR to Quickwit's actual current search runtime.

## Success Criteria

We should consider this runtime alignment complete for the PR when:

- scan path no longer uses direct `spawn_blocking`
- agg path no longer uses direct `spawn_blocking`
- Quickwit installs a bounded CPU search executor for Tantivy work
- prepared splits set Tantivy's internal executor from that same pool
- async prep, warmup, and `doc_async` remain unchanged
- existing Quickwit integration tests still pass
- new integration tests prove the real worker path is exercised

## Summary

The correct short-term move is:

- do what Quickwit search already does
- not what `dd-datafusion` might do long-term

That means:

- keep Tokio for async orchestration
- use a bounded Rayon-backed Quickwit pool for sync Tantivy execution
- inject that through `tantivy-datafusion` as a generic execution abstraction
- leave the broader runtime-segregation redesign for later
