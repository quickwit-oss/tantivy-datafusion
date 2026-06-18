# Multi-Split SingleTableProvider for Quickwit Integration

## Problem

Quickwit queries span multiple splits (each split = one tantivy index with 1+ segments). Currently:
- `SingleTableProvider` accepts one `IndexOpener` (one split)
- Quickwit PR #6160 creates a per-split provider and UNION ALLs them
- UNION ALL means N copies of the same plan pattern — optimizer can't reason globally
- The PR itself notes this should be a single logical table

## Goal

`SingleTableProvider` accepts `Vec<Arc<dyn IndexOpener>>` — one per split. Partitions span all segments across all splits. One plan, one table, global optimization.

## API

```rust
// Multi-split (new):
let provider = SingleTableProvider::from_openers(vec![opener_a, opener_b, opener_c]);

// Single-split (unchanged):
let provider = SingleTableProvider::new(index);
let provider = SingleTableProvider::from_opener(opener);  // wraps in vec![opener]
```

## Partition Model

Each opener reports `segment_sizes() -> Vec<u32>`. Partitions assigned sequentially:

```
Opener A: 2 segments → partitions 0, 1
Opener B: 1 segment  → partition 2
Opener C: 3 segments → partitions 3, 4, 5
Total: 6 partitions
```

`SingleTableDataSource` stores a mapping:
```rust
struct PartitionMapping {
    opener: Arc<dyn IndexOpener>,
    segment_idx: usize,
}
partition_map: Vec<PartitionMapping>  // indexed by partition number
```

In `open(partition)`:
```rust
let mapping = &self.partition_map[partition];
let index = mapping.opener.open().await?;
let segment_reader = searcher.segment_reader(mapping.segment_idx);
// ... process this segment
```

## Schema Resolution

Quickwit's DocMapper defines the canonical schema per index — all splits share it. The provider uses the first opener's schema.

For future schema evolution (different splits have different fields): merge schemas at construction time, rely on null-padding in `fast_field_reader.rs` for missing fields per segment. This already works.

## Warmup

Per-opener, not per-partition and not global. Each split's storage needs independent warmup.

```rust
warmup_cells: Vec<Arc<OnceCell<()>>>  // one per opener
```

In `open(partition)`:
```rust
let opener_idx = self.partition_to_opener_idx[partition];
let warmup_cell = &self.warmup_cells[opener_idx];
warmup_cell.get_or_init(|| async { warmup(&index).await }).await;
```

First partition for each split triggers warmup. Other partitions from the same split skip it.

## Partition Statistics

`compute_partition_stats` runs per opener. Stats concatenated:
```rust
let mut all_stats = Vec::new();
for opener in &openers {
    if let Some(direct) = opener.as_any().downcast_ref::<DirectIndexOpener>() {
        all_stats.extend(compute_partition_stats(direct.index(), &ff_schema)?);
    } else {
        // Remote opener: stats unknown until execution
        let n = opener.segment_sizes().len();
        all_stats.extend(vec![None; n]);
    }
}
```

DataFusion can prune entire splits via timestamp min/max per partition.

## Aggregation (AggDataSource)

`AggDataSource` accepts `Vec<Arc<dyn IndexOpener>>`. Aggregation runs per-split, intermediates merged:

```rust
let mut merged = None;
for opener in &openers {
    let index = opener.open().await?;
    let result = execute_tantivy_agg(&index, &aggs, query, &schema)?;
    // merge intermediate results across splits
}
```

Or: each split is a separate partition in AggDataSource. DataFusion merges.

## Codec (Distributed Execution)

### Serialization
```protobuf
message TantivyScanProto {
    // ... existing fields ...
    repeated OpenerProto openers = 19;        // one per split
    repeated PartitionMap partition_map = 20;  // partition → opener index + segment index
}
```

### On Workers
The distributed optimizer assigns partitions to workers by split affinity. Each worker's deserialized DataSource only has the openers for its assigned splits. The `OpenerFactory` reconstructs each opener from its metadata.

## Quickwit Integration

`QuickwitTableProvider::scan()` becomes:
```rust
async fn scan(&self, state, projection, filters, limit) -> Result<Arc<dyn ExecutionPlan>> {
    // 1. Query metastore for matching splits
    let splits = self.metastore.list_splits(index_uid, time_filter, tag_filter).await?;

    // 2. Create an opener per split
    let openers: Vec<Arc<dyn IndexOpener>> = splits.iter()
        .map(|s| Arc::new(StorageSplitOpener::new(s, storage.clone())) as _)
        .collect();

    // 3. Single provider, all splits
    let provider = SingleTableProvider::from_openers(openers);
    provider.scan(state, projection, filters, limit).await
}
```

No UNION ALL. One plan. DataFusion sees all partitions globally.

## What This Enables

1. **Global LIMIT pushdown** — `LIMIT 10` across all splits, not `LIMIT 10` per split then merge
2. **Global TopK** — Block-WAND across all segments from all splits
3. **Partition pruning** — skip entire splits via timestamp stats without scanning
4. **Split-affinity scheduling** — distributed optimizer assigns partitions to workers by split locality
5. **Single plan tree** — optimizer rules fire once, not N times per UNION branch

## Files to Modify

| File | Change |
|------|--------|
| `src/unified/single_table_provider.rs` | Accept `Vec<Arc<dyn IndexOpener>>`, partition mapping, per-opener warmup |
| `src/unified/agg_data_source.rs` | Accept `Vec<Arc<dyn IndexOpener>>`, per-split aggregation |
| `src/codec.rs` | Serialize/deserialize multi-opener + partition mapping |
| `src/index_opener.rs` | No changes to trait — `OpenerMetadata` unchanged |

## Testing

1. Multi-opener with 2-3 RAM indexes — SELECT spans all splits
2. Aggregation across splits — GROUP BY returns merged results
3. FTS across splits — full_text() finds docs in any split
4. Deleted docs in one split — correctly excluded
5. Different segment counts per split — partition mapping correct
6. Codec roundtrip with multi-opener
7. Partition stats span all openers — pruning works
8. Empty split (0 segments) — handled gracefully
