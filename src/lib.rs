// ---------------------------------------------------------------------------
// Shared infrastructure
// ---------------------------------------------------------------------------
pub mod codec;
pub mod fast_field_reader;
pub mod full_text_udf;
pub(crate) mod index_opener;
pub mod schema_mapping;
pub mod split_runtime;
pub mod sync_exec;
pub(crate) mod type_coercion;
pub(crate) mod util;
pub mod warmup;

use arrow::datatypes::Field;

/// Arrow field metadata key used when a split-local fast-field source column has
/// a different physical Tantivy name than the logical DataFusion column.
pub const FAST_FIELD_LOGICAL_NAME_METADATA_KEY: &str = "tantivy_datafusion.logical_name";

/// Arrow field metadata key used when an internal source column alias should
/// read from a different Tantivy fast-field name.
pub const FAST_FIELD_READ_NAME_METADATA_KEY: &str = "tantivy_datafusion.read_name";

pub fn fast_field_read_name(field: &Field) -> &str {
    field
        .metadata()
        .get(FAST_FIELD_READ_NAME_METADATA_KEY)
        .map(String::as_str)
        .unwrap_or_else(|| field.name().as_str())
}

// ---------------------------------------------------------------------------
// Unified Tantivy table approach
// ---------------------------------------------------------------------------
pub mod unified;

// ---------------------------------------------------------------------------
// Re-exports
// ---------------------------------------------------------------------------
pub use codec::TantivyCodec;
pub use full_text_udf::{
    extract_full_text_call, extract_full_text_filter, extract_full_text_or_group, full_text_udf,
    FullTextFilter,
};
pub use schema_mapping::{
    tantivy_schema_to_arrow, tantivy_schema_to_arrow_from_index,
    tantivy_schema_to_arrow_with_multi_valued,
};
pub use split_runtime::{
    PreparedSplit, SplitDescriptor, SplitRuntimeFactory, SplitRuntimeFactoryExt,
};
pub use sync_exec::{SyncExecutionPool, SyncExecutionPoolExt, SyncExecutionPoolRef};
pub use unified::agg_pushdown::AggPushdown;
pub use unified::tantivy_table_provider::TantivyTableProvider;
