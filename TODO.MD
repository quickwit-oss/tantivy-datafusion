1. split the codec per struct
2. pull our the connector trait to it's own repo so it can be defined here
3. make sure everything works together with how rules are registered
4. check all the enums make sure we are serializing multiple splits/everything works properly
5. check the type information given some substrait plans.:
6. rename singletabledatasource
7. Singleton `SplitRuntimeFactoryExt`/`SyncExecutionPoolExt` prevent multiple tantivy-like sources** — `src/lib.rs` - this is part of the quickwit-datafusion refactor
8. output_orderings
9. `derive_tantivy_aggregations` silently drops unsupported aggregation families** — agg derivation module


**10. Agg result conversion allocates `Vec<Option<T>>` per column** — agg result conversion
Builds an intermediate `Vec<Option<T>>` per column before constructing the Arrow array. Per CLAUDE.md, allocations must be O(record_batch), not O(row × columns). Use Arrow builders directly.

**Fix:**
```rust
// Replace per-column Vec<Option<T>> with Arrow builders that append directly:
let mut builder = PrimitiveBuilder::<Int64Type>::with_capacity(row_count);
for row in rows {
    match row.get(col_idx) {
        Some(v) => builder.append_value(v),
        None    => builder.append_null(),
    }
}
let array: ArrayRef = Arc::new(builder.finish());
// No intermediate Vec. One allocation per column, sized up-front.
```


- `AggDataSource` has six constructors — collapse into one + builder when doing Finding 3.
- Planning docs (`ESCompatDF.md`, `MULTI_SPLIT_PLAN.md`, `NestedApproxAggFixPlan.md`, etc.) are untracked; decide whether to commit them or move to a `docs/` directory so they don't clutter `git status`.


- The crate already has `MetricsGuard` and `AbortOnDrop` patterns in place — the foundations for proper metrics and task lifecycle are there; they just need to be lifted into a shared core (Finding 3) rather than copy-pasted.
