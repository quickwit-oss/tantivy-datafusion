
Review complete — 35 findings from specialists

Confirmed. Both AggregateExec::filter_expr() and AggregateFunctionExpr::is_distinct() exist
in the DataFusion API. The agg_pushdown rule checks neither. Now I have everything needed for
the synthesis.

---------------------------------------------------------------------------------------------

DataFusion Review Summary

Overall Assessment: The crate has a solid architecture — the plan-node-based design, correct
alive_bitset handling, proper BM25 scoring, and the spawn_blocking+MPSC channel bridge are
all well-executed. Two correctness bugs in the aggregation pushdown rule will produce silent
wrong results for queries using COUNT(DISTINCT ...) or COUNT(*) FILTER (WHERE ...). The
primary memory concern — full segment materialization before batching — affects every
non-TopK scan path.

---------------------------------------------------------------------------------------------

Critical / High Findings

1. Missing filter_expr guard in AggPushdown — silent wrong results —
src/unified/agg_pushdown.rs:94,124

AggPushdown never checks whether AggregateExec has per-aggregate FILTER clauses (e.g.,
COUNT(*) FILTER (WHERE x > 5)). Tantivy's aggregation API has no concept of per-aggregate
filters. If present, the filter is silently dropped — tantivy counts all matching docs,
producing wrong results. DataFusion's own aggregate_statistics.rs guards against this.

Fix:


 // Add at the top of try_rewrite_single (line ~98) and try_rewrite_two_phase (line ~130),
 // right after the has_group_by check:

 if agg.filter_expr().iter().any(|e| e.is_some()) {
     return Ok(Transformed::no(plan.clone()));
 }


In try_rewrite_two_phase, also check the partial aggregate:


 if partial_agg.filter_expr().iter().any(|e| e.is_some()) {
     return Ok(Transformed::no(plan.clone()));
 }


---------------------------------------------------------------------------------------------

2. Missing is_distinct() guard in AggPushdown — silent wrong results —
src/unified/agg_pushdown.rs:191

The derive_tantivy_aggregations loop never checks agg_fn.is_distinct(). COUNT(DISTINCT price)
pushes down to tantivy's CountAggregation, which counts all values, not distinct values.
Silent wrong results.

Fix:


 // In derive_tantivy_aggregations, inside the for loop at line 191:
 for agg_fn in agg.aggr_expr() {
     if agg_fn.is_distinct() {
         return None; // Cannot push down DISTINCT aggregations to tantivy
     }
     // ... rest of existing code


---------------------------------------------------------------------------------------------

3. Full segment materialization before batching — O(segment) memory — src/util.rs:56-176

collect_matching_docs() collects ALL matching doc_ids (and scores) into unbounded
Vec<u32>/Vec<f32> before returning. For a 10M-match segment: ~80MB allocated before the first
batch is emitted. This violates the allocation guidance in `AGENTS.md`.

The TopK path (lines 72-110) is bounded by K and is fine. Three paths need fixing:

 • Full scoring without TopK (lines 112-139): unbounded Vec::new()
 • No scoring (lines 149-162): unbounded Vec::new()
 • No query (lines 170-172): (0..max_doc).filter(...).collect()

The debate confirmed this is not a tantivy API limitation — for_each_no_score delivers docs
in 64-doc blocks, and ChunkBuilder::build takes &self (shared borrow), so inline batch
emission from within the callback is feasible with no borrow conflicts.

Prompt to address this:

▌ You are refactoring collect_matching_docs in src/util.rs and
▌ generate_single_table_batch_streaming in src/unified/single_table_provider.rs.
▌ Currently, collect_matching_docs returns (Vec<u32>, Option<Vec<f32>>), materializing all
▌ matching doc_ids for an entire segment before the batching loop in
▌ generate_single_table_batch_streaming (line 1035-1042) chunks them.
▌ Goal: Bound memory to O(batch_size) instead of O(segment) for all non-TopK paths.
▌ Approach: Replace collect_matching_docs with two functions:
▌  1 collect_topk_docs(...) — keep the existing TopK path (lines 72-110), already bounded
▌    by K. Returns (Vec<u32>, Vec<f32>).
▌  2 for_each_matching_doc_chunks(segment_reader, searcher, query, index_schema,
▌    needs_score, batch_size, on_chunk) — streams matching docs through a callback in
▌    batch_size chunks:
▌
▌  pub(crate) fn for_each_matching_doc_chunks(
▌      segment_reader: &SegmentReader,
▌      searcher: &Searcher,
▌      query: Option<&Arc<dyn tantivy::query::Query>>,
▌      index_schema: &tantivy::schema::Schema,
▌      needs_score: bool,
▌      batch_size: usize,
▌      mut on_chunk: impl FnMut(&[DocId], Option<&[Score]>) -> Result<bool>,
▌  ) -> Result<()>
▌
▌ Inside the callback-based paths:
▌  • No-score path: accumulate doc_ids from for_each_no_score's &[DocId] slices (64-doc
▌    blocks from tantivy) into a Vec<DocId> with capacity=batch_size. When the buffer
▌    reaches batch_size, call on_chunk(&buffer, None) and buffer.clear(). Use a stopped
▌    flag + captured Option<DataFusionError> for error/cancellation since the callback
▌    returns ().
▌  • Scoring path (no TopK): same pattern but buffer both Vec<DocId> and Vec<Score>, using
▌    weight.for_each(segment_reader, &mut |doc, score| { ... }).
▌  • No-query path: iterate (0..max_doc) with alive_bitset filtering, emitting every
▌    batch_size docs. No tantivy callback involved — straightforward loop.
▌ In generate_single_table_batch_streaming, wire the streaming function directly to
▌ ChunkBuilder:
▌
▌  for_each_matching_doc_chunks(
▌      segment_reader, &searcher, cfg.query.as_ref(),
▌      &index.schema(), cfg.needs_score, cfg.batch_size,
▌      |chunk_ids, chunk_scores| {
▌          let batch = builder.build(chunk_ids, chunk_scores)?;
▌          if batch.num_rows() > 0 { Ok(emit(batch)) } else { Ok(true) }
▌      },
▌  )
▌
▌ Do NOT add a crossbeam channel — the existing tokio::sync::mpsc::channel(2) at line 640
▌ already provides the blocking-to-async bridge and backpressure. Do NOT modify the TopK
▌ path.
▌ Files to modify: src/util.rs, src/unified/single_table_provider.rs
▌ Memory profile after: ~64KB per partition (batch_size=8192) instead of ~80MB for 10M
▌ matches — a 1250x reduction.

---------------------------------------------------------------------------------------------

4. Fast field filter codec path is completely untested — silent filter loss on workers —
src/unified/single_table_provider.rs:1316-1401

The codec serializes fast field filters as JSON via serialize_fast_field_filters() /
deserialize_fast_field_filters(). Zero codec roundtrip tests exercise this path. If
serialization breaks (e.g., a type tag mismatch in scalar_to_json_pair /
json_pair_to_scalar), the filter is silently dropped — logical_expr_to_tantivy_query()
returns None for unrecognized expressions, and the codec ignores it (line 1396). Workers then
scan all docs without the tantivy-level filter.

Correctness is preserved because supports_filters_pushdown returns Inexact for these filters,
so DataFusion keeps a FilterExec above the scan. But the tantivy-level optimization (segment
pruning via RangeQuery) is silently lost — workers scan all docs and filter in Arrow.

Fix: Add codec roundtrip tests:


 #[tokio::test]
 async fn test_codec_roundtrip_with_fast_field_filters() {
     // 1. Create SingleTableDataSource with a numeric fast field filter
     //    (e.g., price > 2.0 as Expr::BinaryExpr)
     // 2. Round-trip through the codec
     // 3. Assert the decoded plan's pre_built_query is Some, not None
     // 4. Cover scalar types: i64, u64, f64, bool, TimestampMicrosecond, Utf8
 }

 #[tokio::test]
 async fn test_codec_roundtrip_fts_plus_fast_field_filter() {
     // Combine FTS query + fast field filter, verify both survive
 }

 #[tokio::test]
 async fn test_codec_roundtrip_agg_with_fast_field_filters() {
     // AggDataSource with fast field filters
 }


---------------------------------------------------------------------------------------------

Medium Findings

5. Timestamp timezone dropped in codec serialization —
src/unified/single_table_provider.rs:1425-1428

scalar_to_json_pair discards the timezone from all Timestamp variants (the _ in
TimestampMicrosecond(Some(v), _)). On deserialization, json_pair_to_scalar always
reconstructs with None timezone. The codec round-trip is not faithful for timezone-aware
timestamps. While tantivy stores absolute microseconds (unaffected), the logical Expr is
altered.

Fix:


 // In scalar_to_json_pair, preserve timezone:
 ScalarValue::TimestampMicrosecond(Some(v), tz) => {
     let tag = match tz {
         Some(tz) => format!("ts_us:{tz}"),
         None => "ts_us".into(),
     };
     Some((v.to_string(), tag))
 }

 // In json_pair_to_scalar, restore timezone:
 tag if tag.starts_with("ts_us") => {
     let tz = tag.strip_prefix("ts_us:")
         .map(|s| Arc::from(s) as Arc<str>);
     Ok(ScalarValue::TimestampMicrosecond(
         Some(value.parse().map_err(parse_err)?), tz,
     ))
 }


Apply the same pattern for ts_s, ts_ms, ts_ns.

---------------------------------------------------------------------------------------------

6. eq_properties() does not declare output ordering —
src/unified/single_table_provider.rs:779-781

Returns bare EquivalenceProperties with no ordering. When no TopK is active, docs are emitted
in doc_id order within each segment. Declaring _doc_id ASC lets the optimizer skip redundant
sorts and use SortPreservingMergeExec. When TopK is active, docs come in score-descending
order.

Fix:


 fn eq_properties(&self) -> EquivalenceProperties {
     let mut props = EquivalenceProperties::new(self.schema.projected.clone());
     // When no TopK, docs are emitted in doc_id order within each partition.
     // When TopK+scoring, docs come in score-descending order.
     // Declare this so the optimizer can skip redundant sorts.
     //
     // Implementation: check if self.topk.is_some() && self.schema.needs_score,
     // declare _score DESC; otherwise if _doc_id is projected, declare _doc_id ASC.
     props
 }


This requires constructing PhysicalSortExpr for the relevant column — conditionally based on
self.topk and self.schema.needs_score. The schema's projected field must include the ordering
column for the declaration to be meaningful.

---------------------------------------------------------------------------------------------

7. Synchronous IndexReader construction in async warmup functions — src/warmup.rs:19-23,
54-58, 84-88

warmup_inverted_index, warmup_fast_fields_by_name, and warmup_fast_fields construct
IndexReader synchronously (file I/O) inside async fn. These run on the tokio executor via
tokio::spawn. warmup_document_store (line 131) correctly wraps the same construction in
spawn_blocking. The inconsistency is an oversight.

For local/mmap openers, needs_warmup is false, so these never run. For storage-backed
openers, the reader construction may involve real I/O.

Fix:


 // In warmup_inverted_index (and similarly for the other two):
 pub async fn warmup_inverted_index(index: &Index, query_fields: &[Field]) -> Result<()> {
     let index = index.clone();
     let fields: Vec<Field> = query_fields.to_vec();
     let (reader, searcher) = tokio::task::spawn_blocking(move || {
         let reader = index
             .reader_builder()
             .reload_policy(ReloadPolicy::Manual)
             .try_into()
             .map_err(|e| DataFusionError::Internal(format!("open reader for warmup:
 {e}")))?;
         let searcher = reader.searcher();
         Ok::<_, DataFusionError>((reader, searcher))
     }).await.map_err(|e| DataFusionError::Internal(format!("spawn: {e}")))??;

     // ... rest of async warmup using searcher ...


Alternatively, factor out a shared open_reader_blocking(index) -> Result<(IndexReader,
Searcher)> helper.

---------------------------------------------------------------------------------------------

8. Double .to_vec() copies doc IDs per batch — src/fast_field_reader.rs:82,106

When called with doc_ids: Some(ids), the slice is .to_vec()'d at line 82. Then at line 106,
docs.to_vec() copies the same data again to construct _doc_id UInt32Array. Two O(batch_size)
copies per batch.

Fix:


 // At line 106, replace:
 let array: ArrayRef = Arc::new(UInt32Array::from(docs.to_vec()));

 // With (docs is already a Vec<u32>):
 let array: ArrayRef = Arc::new(UInt32Array::from(docs.clone()));


Or better, build the _doc_id array before consuming docs for field reads, then pass &docs for
both paths. The first .to_vec() at line 82 is needed for mutability (alive_bitset filtering
in the None branch), but the second copy at line 106 is pure waste since docs is already an
owned Vec<u32>.

---------------------------------------------------------------------------------------------

9. No codec roundtrip test for multi-valued fields — tests/codec_roundtrip.rs

No roundtrip test exercises multi-valued fast fields (List<T> schema). If multi_valued_fields
serialization breaks, workers would use scalar types instead of List<T>, causing Arrow schema
mismatch errors — but only for indexes with multi-valued fields.

Fix: Add a codec roundtrip test with a tantivy index containing a multi-valued fast field.
Verify the decoded plan's schema includes List<T> for the multi-valued field.

---------------------------------------------------------------------------------------------

Architectural Findings

10. AggPushdown collapses distributed two-phase aggregation — rule ordering determines
behavior — src/unified/agg_pushdown.rs:124-159

AggPushdown rewrites AggregateExec(Final) → ... → AggregateExec(Partial) → DataSourceExec
into a single DataSourceExec(AggDataSource) with 1 partition. When this rule runs before a
distributed physical optimizer, the aggregation is collapsed before the distributed planner
inserts network boundaries. When reversed, the distributed planner inserts
NetworkCoalesceExec which AggPushdown can't see through, so it doesn't fire.

Neither order is documented. This is a footgun for distributed deployments.

Prompt to address this:

▌ You are documenting the rule ordering contract for AggPushdown in
▌ src/unified/agg_pushdown.rs.
▌ Currently the struct docstring explains what it does but not when it should run relative
▌ to other optimizer rules.
▌ Add a doc comment on AggPushdown explaining:
▌  1 This rule must be registered before any distributed physical optimizer rule (e.g.,
▌    DistributedPhysicalOptimizerRule)
▌  2 Tantivy's native aggregation accesses all segments from a single node — it cannot be
▌    partitioned across workers
▌  3 If registered after a distributed optimizer, NetworkCoalesceExec boundaries will
▌    prevent the rule from firing
▌ Also add a ## Rule Ordering section to the crate-level or module-level documentation.
▌ Files to modify: src/unified/agg_pushdown.rs (struct and module docs)

---------------------------------------------------------------------------------------------

11. No schema reconciliation layer between tantivy types and downstream consumers —
src/schema_mapping.rs, src/unified/single_table_provider.rs

SpiceAI uses SchemaCastScanExec as a wrapper that handles nullability adjustments, type
coercion, and column stripping between the source and downstream consumers.
tantivy-datafusion has no equivalent — type mismatches between tantivy's fast field types and
what downstream consumers expect would surface as runtime errors.

Prompt to address this:

▌ You are evaluating whether SingleTableDataSource needs a schema reconciliation layer,
▌ inspired by SpiceAI's SchemaCastScanExec.
▌ Currently tantivy-datafusion trusts that tantivy_schema_to_arrow produces a schema
▌ compatible with all downstream consumers. Consider adding a lightweight SchemaCastExec
▌ wrapper that:
▌  1 Adjusts nullability (non-nullable tantivy fields → nullable output for safety)
▌  2 Strips internal columns (_doc_id, _segment_ord) from the output if not projected
▌  3 Handles type coercion between tantivy's native types and what consumers expect
▌ This is a defense-in-depth measure. Evaluate whether existing Arrow errors from schema
▌ mismatches are sufficient or whether silent type coercion issues exist.
▌ Reference: SpiceAI's SchemaCastScanExec at
▌ crates/runtime-datafusion/src/execution_plan/schema_cast.rs
▌ Files to consider: src/unified/single_table_provider.rs (the open() method), new file
▌ src/schema_cast.rs

---------------------------------------------------------------------------------------------

Low / Info

 • Arc::clone style: The codebase uses .clone() on Arc types in ~20+ locations instead of
   Arc::clone(&x). `AGENTS.md` prefers the explicit form for clarity. Low priority but a
   cleanup candidate.
 • full_text() UDF docstring/impl mismatch (src/full_text_udf.rs:62): The docstring says
   "returns true for every row as a safe fallback" but invoke_with_args() returns Err(...).
   Either change the implementation to return
   ColumnarValue::Scalar(ScalarValue::Boolean(Some(true))) or update the docstring. The
   current Exact pushdown means DataFusion will never call invoke in practice, but the
   doc/code mismatch is a latent bug if the pushdown behavior ever changes.
 • Facet and JsonObject fields silently omitted (src/schema_mapping.rs:9-24): These return
   None from scalar_arrow_type, so they don't appear in the Arrow schema. Defensible
   simplification, but a tracing::warn! for unrecognized field types would help users who
   expect these fields to appear.
 • No double-roundtrip test for AggDataSource codec (tests/codec_roundtrip.rs):
   SingleTableDataSource has one but AggDataSource does not.
 • Score .to_vec() per batch (single_table_provider.rs:934): Float32Array::from(sc.to_vec())
   copies the score slice. This is O(batch_size) and acceptable in isolation, but becomes
   redundant after the streaming refactor from finding #3 (scores can be fed directly into
   the builder).

---------------------------------------------------------------------------------------------

Conflicts Resolved

Materialization approach (distributed_expert vs quickwit_tantivy_expert): distributed_expert
proposed a bounded crossbeam channel within the blocking code; quickwit_tantivy_expert
proposed inline buffer emission inside the callback. Resolution: The inline buffer approach
is correct. The existing tokio::sync::mpsc::channel(2) already provides the blocking→async
bridge and backpressure. Adding a crossbeam channel inside the blocking code duplicates this
with no benefit and adds an unnecessary OS thread. This matches the pattern in DataFusion's
own StreamRead, InfluxDB's cross_rt_stream.rs, and dd-datafusion's io_executor.rs — all use a
single channel layer at the blocking→async boundary.

Severity of materialization (influx_expert: critical vs others: high): influx_expert rated
the materialization issue as Critical. The other agents rated it High. Resolution: High. The
issue causes excess memory usage but not data corruption or panics — it won't cause incorrect
results, just potential OOM under heavy load. The TopK path (most common for search use
cases) is already bounded.

Build artifact in diff (simplifier): The simplifier flagged that the diff contains only
target/debug/build/typenum-*/out/tests.rs. All agents correctly identified this as a build
artifact and reviewed the actual source code instead. No action needed — the file is already
.gitignored.

---------------------------------------------------------------------------------------------

What's Done Well

 • alive_bitset handling: All four code paths in collect_matching_docs correctly filter
   deleted documents, including the for_each_pruning path which skips deleted docs without
   updating the TopN threshold. The no-query path also filters correctly. This is a common
   source of bugs in tantivy integrations and it's handled thoroughly here.
 • BM25 scoring: Uses EnableScoring::enabled_from_searcher(searcher) for index-wide IDF
   (scores comparable across segments) and EnableScoring::disabled_from_schema when scoring
   is disabled. Both are the correct API choices.
 • Equality on string fast fields: Correctly uses RangeQuery(Included, Included) instead of
   TermQuery for string fields, avoiding the tokenization trap where TermQuery on tokenized
   fields searches for individual tokens, not the original value.
 • spawn_blocking + MPSC channel bridge: The tokio::spawn → spawn_blocking →
   tx_blocking.blocking_send() pattern with channel(2) is textbook correct for bridging
   synchronous tantivy I/O with async DataFusion streams, matching patterns in DataFusion,
   InfluxDB, and dd-datafusion.
 • Selective warmup: Only warms fast fields that are actually projected and inverted index
   fields that are queried. Warmup is correctly skipped for local/mmap openers. The OnceCell
   ensures warmup runs at most once across partitions.
 • Aggregation uses Searcher::search(): Correctly relies on tantivy's built-in alive_bitset
   filtering in the collector framework rather than reimplementing it.
