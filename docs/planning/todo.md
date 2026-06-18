1. split the codec per struct
2. pull our the connector trait to it's own repo so it can be defined here
3. make sure everything works together with how rules are registered
4. check all the enums make sure we are serializing multiple splits/everything works properly
5. check the type information given some substrait plans.:
6. rename singletabledatasource
7. Singleton `SplitRuntimeFactoryExt`/`SyncExecutionPoolExt` prevent multiple tantivy-like sources** — `src/lib.rs` - this is part of the quickwit-datafusion refactor
8. output_orderings
9. `derive_tantivy_aggregations` silently drops unsupported aggregation families** — agg derivation module


- `TantivyAggDataSource` has six constructors — collapse into one + builder when doing Finding 3.
- The crate already has `MetricsGuard` and `AbortOnDrop` patterns in place — the foundations for proper metrics and task lifecycle are there; they just need to be lifted into a shared core (Finding 3) rather than copy-pasted.
