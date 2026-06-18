# tantivy-datafusion

`tantivy-datafusion` exposes Tantivy indexes as DataFusion tables so callers can
run SQL over search indexes.

The crate is aimed at query engines that already own Tantivy indexes or split
metadata and want DataFusion execution, projection, filtering, aggregation, and
distributed-plan serialization around that data. It is not an Elasticsearch API
compatibility layer.

Status: alpha. The core execution path is tested, but public APIs may still
change as the Quickwit integration settles.

## What It Provides

- A `TantivyTableProvider` that presents one or more Tantivy indexes as a single
  DataFusion table.
- SQL projection and filtering over Tantivy fast fields.
- A `full_text(column, query)` SQL UDF that pushes Tantivy query parsing and
  full-text search into the table scan.
- Optional `_score` and `_document` columns for scored search results and stored
  document retrieval.
- `AggPushdown`, a DataFusion physical optimizer rule that pushes supported
  aggregations into Tantivy's native aggregation engine.
- Split/runtime abstractions for distributed execution:
  `SplitDescriptor`, `SplitRuntimeFactory`, `SyncExecutionPool`, and
  `TantivyCodec`.

## Data Model

The provider builds a DataFusion schema from Tantivy fast fields. It also adds
internal/search columns:

- `_doc_id`: Tantivy document id within a segment.
- `_segment_ord`: Tantivy segment ordinal.
- `_score`: `Float32`, populated when a scored full-text query is active.
- `_document`: stored Tantivy document serialized as JSON.

Fast fields are the main SQL column surface. Text fields can also participate in
`full_text(...)` predicates when they are indexed in Tantivy.

## Basic Usage

```rust
use std::sync::Arc;

use datafusion::prelude::*;
use tantivy::Index;
use tantivy_datafusion::{full_text_udf, TantivyTableProvider};

async fn query_index(index: Index) -> datafusion::common::Result<()> {
    let ctx = SessionContext::new();

    ctx.register_udf(full_text_udf());
    ctx.register_table("docs", Arc::new(TantivyTableProvider::new(index)))?;

    let batches = ctx
        .sql(
            "SELECT id, price, _score
             FROM docs
             WHERE full_text(category, 'electronics') AND price > 2.0
             ORDER BY _score DESC
             LIMIT 10",
        )
        .await?
        .collect()
        .await?;

    for batch in batches {
        println!("{batch:?}");
    }

    Ok(())
}
```

For aggregation pushdown, register `AggPushdown` in the DataFusion session state:

```rust
use std::sync::Arc;

use datafusion::execution::SessionStateBuilder;
use datafusion::prelude::*;
use tantivy_datafusion::{full_text_udf, AggPushdown, TantivyTableProvider};

let state = SessionStateBuilder::new()
    .with_config(SessionConfig::new())
    .with_default_features()
    .with_physical_optimizer_rule(Arc::new(AggPushdown::new()))
    .build();

let ctx = SessionContext::new_with_state(state);
ctx.register_udf(full_text_udf());
ctx.register_table("docs", Arc::new(TantivyTableProvider::new(index)))?;
```

Supported pushdowns include common grouped and ungrouped aggregations that can be
represented by Tantivy aggregation requests. Unsupported aggregate shapes fall
back to normal DataFusion execution.

## Multi-Split Execution

For local multi-index execution, use:

```rust
let provider = TantivyTableProvider::from_local_splits(indexes)?;
ctx.register_table("docs", Arc::new(provider))?;
```

For distributed execution, integrations provide split metadata and runtime
resolution:

- `SplitDescriptor` carries serializable split metadata.
- `SplitRuntimeFactory` prepares a worker-local `PreparedSplit`.
- `TantivyCodec` serializes DataFusion physical plans containing Tantivy data
  sources.
- `SyncExecutionPool` lets the embedding runtime choose where synchronous
  Tantivy query work runs.

These hooks are intended for Quickwit-style split scheduling where planning and
execution happen in different processes.

## Current Scope

This repository is focused on SQL execution over Tantivy:

- fast-field scans;
- full-text predicates through `full_text(...)`;
- score/document projection when requested;
- schema evolution across splits;
- aggregation pushdown where Tantivy can execute the aggregation directly;
- distributed physical-plan serialization.

Out of scope for this crate:

- implementing the Elasticsearch REST API;
- preserving Elasticsearch response formats;
- serving as a general ES compatibility layer.

Historical design notes and integration plans live under `docs/`; they are not
API contracts.

## Development

Run the standard checks:

```bash
cargo fmt --check
cargo test
cargo clippy --all-targets -- -D warnings
RUSTDOCFLAGS="-D warnings" cargo doc --no-deps
```

The benchmark suite includes aggregation scenarios:

```bash
cargo bench --bench agg_bench
```

## License

MIT
