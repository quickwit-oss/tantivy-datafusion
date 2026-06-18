use std::sync::Arc;

use arrow::array::{
    Array, ArrayRef, BinaryArray, BinaryBuilder, BooleanArray, BooleanBuilder, Float64Array,
    Float64Builder, Int64Array, Int64Builder, ListArray, ListBuilder, RecordBatch, StringArray,
    StringBuilder, StringViewArray, TimestampMicrosecondArray, TimestampMicrosecondBuilder,
    UInt64Array, UInt64Builder,
};
use arrow::compute::kernels::zip::zip;
use arrow::compute::{cast, is_not_null};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef, TimeUnit};
use datafusion::common::Result;
use datafusion::error::DataFusionError;

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum FastFieldCoercion {
    Exact,
    Cast(DataType),
    EpochMillisToTimestampMicros,
    TimestampMicrosToMillis,
    TimestampMicrosToMillisList,
    TryCastUtf8ToNumeric(DataType),
    TryCastUtf8ToNumericList(DataType),
    TryCastUtf8ListToNumericList(DataType),
    ScalarToList {
        item_type: DataType,
        cast_scalar_to: Option<DataType>,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct FastFieldColumnPlan {
    pub(crate) output_field: Arc<Field>,
    pub(crate) sources: Vec<FastFieldSourcePlan>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct FastFieldSourcePlan {
    pub(crate) source_name: String,
    pub(crate) coercion: FastFieldCoercion,
}

#[derive(Debug, Clone)]
pub(crate) struct FastFieldProjectionPlan {
    pub(crate) output_schema: SchemaRef,
    pub(crate) columns: Vec<FastFieldColumnPlan>,
}

pub(crate) fn infer_canonical_fast_field_schema(split_schemas: &[SchemaRef]) -> Result<SchemaRef> {
    if split_schemas.is_empty() {
        return Err(DataFusionError::Plan(
            "at least one split schema is required".to_string(),
        ));
    }

    let mut fields = vec![
        Field::new("_doc_id", DataType::UInt32, false),
        Field::new("_segment_ord", DataType::UInt32, false),
    ];

    for schema in split_schemas {
        for field in schema.fields() {
            let name = field.name();
            if name == "_doc_id" || name == "_segment_ord" {
                continue;
            }

            let Some(existing_idx) = fields.iter().position(|candidate| candidate.name() == name)
            else {
                fields.push(field.as_ref().clone());
                continue;
            };

            let merged = merge_field_types(fields[existing_idx].data_type(), field.data_type())
                .ok_or_else(|| {
                    DataFusionError::Plan(format!(
                        "conflicting fast field types for '{name}': {:?} vs {:?}; \
                         provide an explicit canonical schema",
                        fields[existing_idx].data_type(),
                        field.data_type()
                    ))
                })?;

            fields[existing_idx] = Field::new(name, merged, true);
        }
    }

    Ok(Arc::new(Schema::new(fields)))
}

pub(crate) fn plan_fast_field_projection(
    split_schema: &SchemaRef,
    canonical_schema: &SchemaRef,
) -> Result<FastFieldProjectionPlan> {
    let mut column_plans = Vec::with_capacity(canonical_schema.fields().len());

    for output_field in canonical_schema.fields() {
        let output_name = output_field.name();
        let matching_sources = split_schema
            .fields()
            .iter()
            .filter(|candidate| field_matches_output_name(candidate, output_name))
            .cloned()
            .map(|source_field| {
                let coercion =
                    plan_fast_field_coercion(source_field.data_type(), output_field.data_type())
                        .map_err(|e| {
                            DataFusionError::Plan(format!(
                                "cannot coerce split field '{output_name}' from {:?} to {:?}: {e}",
                                source_field.data_type(),
                                output_field.data_type()
                            ))
                        })?;

                Ok(FastFieldSourcePlan {
                    source_name: source_field.name().to_string(),
                    coercion,
                })
            })
            .collect::<Result<Vec<_>>>()?;

        column_plans.push(FastFieldColumnPlan {
            output_field: Arc::clone(output_field),
            sources: matching_sources,
        });
    }

    Ok(FastFieldProjectionPlan {
        output_schema: Arc::clone(canonical_schema),
        columns: column_plans,
    })
}

fn field_matches_output_name(field: &Field, output_name: &str) -> bool {
    field.name() == output_name
        || field
            .metadata()
            .get(crate::FAST_FIELD_LOGICAL_NAME_METADATA_KEY)
            .is_some_and(|logical_name| logical_name == output_name)
}

pub(crate) fn apply_fast_field_projection(
    source_batch: &RecordBatch,
    projection_plan: &FastFieldProjectionPlan,
) -> Result<RecordBatch> {
    let num_rows = source_batch.num_rows();
    let mut columns = Vec::with_capacity(projection_plan.columns.len());

    for column_plan in &projection_plan.columns {
        let array = match column_plan.sources.as_slice() {
            [] => arrow::array::new_null_array(column_plan.output_field.data_type(), num_rows),
            [source] => project_source_array(source_batch, source)?,
            sources => {
                let arrays = sources
                    .iter()
                    .map(|source| project_source_array(source_batch, source))
                    .collect::<Result<Vec<_>>>()?;
                coalesce_projected_arrays(arrays)?
            }
        };
        columns.push(array);
    }

    RecordBatch::try_new(Arc::clone(&projection_plan.output_schema), columns)
        .map_err(|e| DataFusionError::ArrowError(Box::new(e), None))
}

fn project_source_array(
    source_batch: &RecordBatch,
    source: &FastFieldSourcePlan,
) -> Result<ArrayRef> {
    let source_idx = source_batch
        .schema()
        .index_of(&source.source_name)
        .map_err(|_| {
            DataFusionError::Internal(format!(
                "source fast field '{}' missing from batch",
                source.source_name
            ))
        })?;
    let source_array = source_batch.column(source_idx);
    coerce_array(source_array, &source.coercion)
}

fn coalesce_projected_arrays(arrays: Vec<ArrayRef>) -> Result<ArrayRef> {
    let mut arrays = arrays.into_iter();
    let mut merged = arrays.next().ok_or_else(|| {
        DataFusionError::Internal("coalesce requires at least one source array".to_string())
    })?;
    for next in arrays {
        let mask = source_value_present_mask(&merged)?;
        let merged_array = merged.as_ref();
        let next_array = next.as_ref();
        merged = zip(&mask, &merged_array, &next_array)
            .map_err(|e| DataFusionError::ArrowError(Box::new(e), None))?;
    }
    Ok(merged)
}

fn source_value_present_mask(array: &ArrayRef) -> Result<BooleanArray> {
    if matches!(array.data_type(), DataType::List(_)) {
        let list = array.as_any().downcast_ref::<ListArray>().ok_or_else(|| {
            DataFusionError::Internal(format!(
                "expected ListArray for coalesce mask, got {:?}",
                array.data_type()
            ))
        })?;
        let mut builder = BooleanBuilder::with_capacity(list.len());
        let values = list.values();
        let offsets = list.value_offsets();
        for row in 0..list.len() {
            let present = if list.is_null(row) {
                false
            } else {
                let start = offsets[row] as usize;
                let end = offsets[row + 1] as usize;
                (start..end).any(|idx| !values.is_null(idx))
            };
            builder.append_value(present);
        }
        return Ok(builder.finish());
    }

    is_not_null(array.as_ref()).map_err(|e| DataFusionError::ArrowError(Box::new(e), None))
}

fn coerce_array(array: &ArrayRef, coercion: &FastFieldCoercion) -> Result<ArrayRef> {
    match coercion {
        FastFieldCoercion::Exact => Ok(Arc::clone(array)),
        FastFieldCoercion::Cast(target_type) => {
            cast(array, target_type).map_err(|e| DataFusionError::ArrowError(Box::new(e), None))
        }
        FastFieldCoercion::EpochMillisToTimestampMicros => epoch_millis_to_timestamp_micros(array),
        FastFieldCoercion::TimestampMicrosToMillis => timestamp_micros_to_millis(array),
        FastFieldCoercion::TimestampMicrosToMillisList => {
            let millis = timestamp_micros_to_millis(array)?;
            wrap_scalar_array_in_list(&millis, &DataType::Int64)
        }
        FastFieldCoercion::TryCastUtf8ToNumeric(target_type) => {
            try_cast_utf8_to_numeric(array, target_type)
        }
        FastFieldCoercion::TryCastUtf8ToNumericList(item_type) => {
            let numeric = try_cast_utf8_to_numeric(array, item_type)?;
            wrap_scalar_array_in_list(&numeric, item_type)
        }
        FastFieldCoercion::TryCastUtf8ListToNumericList(item_type) => {
            try_cast_utf8_list_to_numeric_list(array, item_type)
        }
        FastFieldCoercion::ScalarToList {
            item_type,
            cast_scalar_to,
        } => {
            let scalar = match cast_scalar_to {
                Some(target_type) => cast(array, target_type)
                    .map_err(|e| DataFusionError::ArrowError(Box::new(e), None))?,
                None => Arc::clone(array),
            };
            wrap_scalar_array_in_list(&scalar, item_type)
        }
    }
}

fn plan_fast_field_coercion(source: &DataType, target: &DataType) -> Result<FastFieldCoercion> {
    if source == target {
        return Ok(FastFieldCoercion::Exact);
    }

    if let DataType::List(inner) = target {
        if let DataType::List(source_inner) = source {
            if utf8_to_numeric_supported(source_inner.data_type(), inner.data_type()) {
                return Ok(FastFieldCoercion::TryCastUtf8ListToNumericList(
                    inner.data_type().clone(),
                ));
            }
            if scalar_cast_supported(source_inner.data_type(), inner.data_type()) {
                return Ok(FastFieldCoercion::Cast(target.clone()));
            }
            return Err(DataFusionError::Plan(
                "list coercions require an exact list type match".to_string(),
            ));
        }

        let item_type = inner.data_type().clone();
        if matches!(source, DataType::Timestamp(TimeUnit::Microsecond, None))
            && item_type == DataType::Int64
        {
            return Ok(FastFieldCoercion::TimestampMicrosToMillisList);
        }
        if source == &item_type {
            return Ok(FastFieldCoercion::ScalarToList {
                item_type,
                cast_scalar_to: None,
            });
        }
        if utf8_to_numeric_supported(source, &item_type) {
            return Ok(FastFieldCoercion::TryCastUtf8ToNumericList(item_type));
        }
        if scalar_cast_supported(source, &item_type) {
            return Ok(FastFieldCoercion::ScalarToList {
                item_type: item_type.clone(),
                cast_scalar_to: Some(item_type),
            });
        }

        return Err(DataFusionError::Plan(format!(
            "unsupported scalar-to-list coercion from {source:?} to {target:?}"
        )));
    }

    if matches!(source, DataType::Timestamp(TimeUnit::Microsecond, None))
        && target == &DataType::Int64
    {
        return Ok(FastFieldCoercion::TimestampMicrosToMillis);
    }
    if source == &DataType::Int64 && target == &DataType::Timestamp(TimeUnit::Microsecond, None) {
        return Ok(FastFieldCoercion::EpochMillisToTimestampMicros);
    }

    if utf8_to_numeric_supported(source, target) {
        return Ok(FastFieldCoercion::TryCastUtf8ToNumeric(target.clone()));
    }

    if scalar_cast_supported(source, target) {
        return Ok(FastFieldCoercion::Cast(target.clone()));
    }

    Err(DataFusionError::Plan(format!(
        "unsupported fast field coercion from {source:?} to {target:?}"
    )))
}

fn scalar_cast_supported(source: &DataType, target: &DataType) -> bool {
    if source == target {
        return true;
    }

    match (source, target) {
        (DataType::Dictionary(_, value_type), target)
            if value_type.as_ref() == &DataType::Utf8 && is_utf8_like(target) =>
        {
            true
        }
        (source, target)
            if is_utf8_like(target) && matches!(source, DataType::Utf8 | DataType::Utf8View) =>
        {
            true
        }
        (source, target) if is_utf8_like(target) && is_numeric(source) => true,
        (DataType::Boolean, target) if is_utf8_like(target) => true,
        (DataType::Timestamp(_, _), target) if is_utf8_like(target) => true,
        (source, target) if is_numeric(source) && is_numeric(target) => true,
        (DataType::Timestamp(_, _), DataType::Timestamp(_, _)) => true,
        _ => false,
    }
}

fn is_utf8_like(data_type: &DataType) -> bool {
    matches!(data_type, DataType::Utf8 | DataType::Utf8View)
}

fn is_numeric(data_type: &DataType) -> bool {
    matches!(
        data_type,
        DataType::Int8
            | DataType::Int16
            | DataType::Int32
            | DataType::Int64
            | DataType::UInt8
            | DataType::UInt16
            | DataType::UInt32
            | DataType::UInt64
            | DataType::Float16
            | DataType::Float32
            | DataType::Float64
    )
}

fn utf8_to_numeric_supported(source: &DataType, target: &DataType) -> bool {
    is_utf8_like(source)
        && matches!(
            target,
            DataType::Int64 | DataType::UInt64 | DataType::Float64
        )
}

fn try_cast_utf8_to_numeric(array: &ArrayRef, target_type: &DataType) -> Result<ArrayRef> {
    match target_type {
        DataType::Int64 => {
            try_cast_utf8_to_numeric_array::<Int64Builder, i64>(array, target_type, |value| {
                value.parse::<i64>().ok()
            })
        }
        DataType::UInt64 => {
            try_cast_utf8_to_numeric_array::<UInt64Builder, u64>(array, target_type, |value| {
                value.parse::<u64>().ok()
            })
        }
        DataType::Float64 => {
            try_cast_utf8_to_numeric_array::<Float64Builder, f64>(array, target_type, |value| {
                value.parse::<f64>().ok()
            })
        }
        other => Err(DataFusionError::Plan(format!(
            "unsupported UTF8-to-numeric coercion target: {other:?}"
        ))),
    }
}

fn try_cast_utf8_to_numeric_array<Builder, Value>(
    array: &ArrayRef,
    target_type: &DataType,
    parse: impl Fn(&str) -> Option<Value>,
) -> Result<ArrayRef>
where
    Builder: arrow::array::ArrayBuilder + Default + ValueAppender<Value>,
    Value: Copy,
{
    let mut builder = Builder::default();
    for row in 0..array.len() {
        match utf8_value_at(array, row)? {
            Some(value) => {
                if let Some(parsed) = parse(value) {
                    builder.append_value(parsed);
                } else {
                    builder.append_null();
                }
            }
            None => builder.append_null(),
        }
    }
    let result = builder.finish();
    if result.data_type() != target_type {
        return Err(DataFusionError::Internal(format!(
            "UTF8 numeric coercion produced {:?}, expected {:?}",
            result.data_type(),
            target_type
        )));
    }
    Ok(result)
}

fn try_cast_utf8_list_to_numeric_list(array: &ArrayRef, item_type: &DataType) -> Result<ArrayRef> {
    match item_type {
        DataType::Int64 => try_cast_utf8_list_to_numeric_list_array::<Int64Builder, i64>(
            array,
            item_type,
            |value| value.parse::<i64>().ok(),
        ),
        DataType::UInt64 => try_cast_utf8_list_to_numeric_list_array::<UInt64Builder, u64>(
            array,
            item_type,
            |value| value.parse::<u64>().ok(),
        ),
        DataType::Float64 => try_cast_utf8_list_to_numeric_list_array::<Float64Builder, f64>(
            array,
            item_type,
            |value| value.parse::<f64>().ok(),
        ),
        other => Err(DataFusionError::Plan(format!(
            "unsupported UTF8-list-to-numeric-list coercion target: {other:?}"
        ))),
    }
}

fn try_cast_utf8_list_to_numeric_list_array<Builder, Value>(
    array: &ArrayRef,
    item_type: &DataType,
    parse: impl Fn(&str) -> Option<Value>,
) -> Result<ArrayRef>
where
    Builder: arrow::array::ArrayBuilder + Default + ValueAppender<Value>,
    Value: Copy,
{
    let list = array.as_any().downcast_ref::<ListArray>().ok_or_else(|| {
        DataFusionError::Internal(format!(
            "expected ListArray for UTF8-list-to-numeric-list coercion, got {:?}",
            array.data_type()
        ))
    })?;
    let values = list.values();
    let offsets = list.value_offsets();
    let mut builder = ListBuilder::new(Builder::default());
    for row in 0..list.len() {
        if list.is_null(row) {
            builder.append(false);
            continue;
        }
        let start = offsets[row] as usize;
        let end = offsets[row + 1] as usize;
        for idx in start..end {
            match utf8_value_at(values, idx)? {
                Some(value) => {
                    if let Some(parsed) = parse(value) {
                        builder.values().append_value(parsed);
                    } else {
                        builder.values().append_null();
                    }
                }
                None => builder.values().append_null(),
            }
        }
        builder.append(true);
    }
    let result = builder.finish();
    let expected = DataType::new_list(item_type.clone(), true);
    if result.data_type() != &expected {
        return Err(DataFusionError::Internal(format!(
            "UTF8 list numeric coercion produced {:?}, expected {:?}",
            result.data_type(),
            expected
        )));
    }
    Ok(Arc::new(result))
}

fn utf8_value_at(array: &ArrayRef, row: usize) -> Result<Option<&str>> {
    if array.is_null(row) {
        return Ok(None);
    }
    if let Some(strings) = array.as_any().downcast_ref::<StringArray>() {
        return Ok(Some(strings.value(row)));
    }
    if let Some(strings) = array.as_any().downcast_ref::<StringViewArray>() {
        return Ok(Some(strings.value(row)));
    }
    Err(DataFusionError::Internal(format!(
        "expected UTF8 array for string numeric coercion, got {:?}",
        array.data_type()
    )))
}

fn timestamp_micros_to_millis(array: &ArrayRef) -> Result<ArrayRef> {
    let typed = array
        .as_any()
        .downcast_ref::<TimestampMicrosecondArray>()
        .ok_or_else(|| {
            DataFusionError::Plan(format!(
                "expected TimestampMicrosecondArray for timestamp-to-i64 coercion, got {:?}",
                array.data_type()
            ))
        })?;

    let mut builder = Int64Builder::with_capacity(typed.len());
    for row in 0..typed.len() {
        if typed.is_null(row) {
            builder.append_null();
        } else {
            builder.append_value(typed.value(row) / 1_000);
        }
    }
    Ok(Arc::new(builder.finish()))
}

fn epoch_millis_to_timestamp_micros(array: &ArrayRef) -> Result<ArrayRef> {
    let typed = array.as_any().downcast_ref::<Int64Array>().ok_or_else(|| {
        DataFusionError::Plan(format!(
            "expected Int64Array for i64-to-timestamp coercion, got {:?}",
            array.data_type()
        ))
    })?;

    let mut builder = TimestampMicrosecondBuilder::with_capacity(typed.len());
    for row in 0..typed.len() {
        if typed.is_null(row) {
            builder.append_null();
        } else {
            builder.append_value(typed.value(row) * 1_000);
        }
    }
    Ok(Arc::new(builder.finish()))
}

fn merge_field_types(left: &DataType, right: &DataType) -> Option<DataType> {
    if left == right {
        return Some(left.clone());
    }

    let left_promoted = promotable_scalar_type(left)?;
    let right_promoted = promotable_scalar_type(right)?;
    if left_promoted == right_promoted {
        return Some(DataType::List(Arc::new(Field::new(
            "item",
            left_promoted,
            true,
        ))));
    }

    if heterogeneous_string_fallback_supported(&left_promoted, &right_promoted) {
        return Some(DataType::List(Arc::new(Field::new(
            "item",
            DataType::Utf8,
            true,
        ))));
    }

    None
}

fn heterogeneous_string_fallback_supported(left: &DataType, right: &DataType) -> bool {
    string_fallback_supported(left) && string_fallback_supported(right)
}

fn string_fallback_supported(data_type: &DataType) -> bool {
    matches!(
        data_type,
        DataType::UInt64
            | DataType::Int64
            | DataType::Float64
            | DataType::Boolean
            | DataType::Utf8
            | DataType::Utf8View
            | DataType::Timestamp(TimeUnit::Microsecond, None)
    )
}

fn promotable_scalar_type(data_type: &DataType) -> Option<DataType> {
    match data_type {
        DataType::UInt64
        | DataType::Int64
        | DataType::Float64
        | DataType::Boolean
        | DataType::Utf8
        | DataType::Utf8View
        | DataType::Binary
        | DataType::Timestamp(TimeUnit::Microsecond, None) => Some(data_type.clone()),
        DataType::Dictionary(_, value_type) if value_type.as_ref() == &DataType::Utf8 => {
            Some(DataType::Utf8)
        }
        DataType::List(inner) => Some(inner.data_type().clone()),
        _ => None,
    }
}

fn wrap_scalar_array_in_list(array: &ArrayRef, item_type: &DataType) -> Result<ArrayRef> {
    match item_type {
        DataType::UInt64 => wrap_typed_array::<UInt64Array, UInt64Builder, u64>(
            array,
            item_type,
            |typed: &UInt64Array, row| typed.value(row),
        ),
        DataType::Int64 => wrap_typed_array::<Int64Array, Int64Builder, i64>(
            array,
            item_type,
            |typed: &Int64Array, row| typed.value(row),
        ),
        DataType::Float64 => wrap_typed_array::<Float64Array, Float64Builder, f64>(
            array,
            item_type,
            |typed: &Float64Array, row| typed.value(row),
        ),
        DataType::Boolean => wrap_typed_array::<BooleanArray, BooleanBuilder, bool>(
            array,
            item_type,
            |typed: &BooleanArray, row| typed.value(row),
        ),
        DataType::Utf8 => wrap_string_array_in_list(array, item_type),
        DataType::Binary => wrap_binary_array_in_list(array, item_type),
        DataType::Timestamp(TimeUnit::Microsecond, None) => {
            wrap_typed_array::<TimestampMicrosecondArray, TimestampMicrosecondBuilder, i64>(
                array,
                item_type,
                |typed: &TimestampMicrosecondArray, row| typed.value(row),
            )
        }
        other => Err(DataFusionError::Plan(format!(
            "unsupported scalar-to-list inner type: {other:?}"
        ))),
    }
}

trait ValueAppender<T> {
    fn append_value(&mut self, value: T);
    fn append_null(&mut self);
}

impl ValueAppender<u64> for UInt64Builder {
    fn append_value(&mut self, value: u64) {
        UInt64Builder::append_value(self, value);
    }

    fn append_null(&mut self) {
        UInt64Builder::append_null(self);
    }
}

impl ValueAppender<i64> for Int64Builder {
    fn append_value(&mut self, value: i64) {
        Int64Builder::append_value(self, value);
    }

    fn append_null(&mut self) {
        Int64Builder::append_null(self);
    }
}

impl ValueAppender<f64> for Float64Builder {
    fn append_value(&mut self, value: f64) {
        Float64Builder::append_value(self, value);
    }

    fn append_null(&mut self) {
        Float64Builder::append_null(self);
    }
}

impl ValueAppender<bool> for BooleanBuilder {
    fn append_value(&mut self, value: bool) {
        BooleanBuilder::append_value(self, value);
    }

    fn append_null(&mut self) {
        BooleanBuilder::append_null(self);
    }
}

impl ValueAppender<i64> for TimestampMicrosecondBuilder {
    fn append_value(&mut self, value: i64) {
        TimestampMicrosecondBuilder::append_value(self, value);
    }

    fn append_null(&mut self) {
        TimestampMicrosecondBuilder::append_null(self);
    }
}

fn wrap_string_array_in_list(array: &ArrayRef, item_type: &DataType) -> Result<ArrayRef> {
    let typed = array
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or_else(|| {
            DataFusionError::Internal(format!(
                "expected array {:?}, got {:?}",
                item_type,
                array.data_type()
            ))
        })?;

    let mut builder = ListBuilder::new(StringBuilder::default());
    for row in 0..typed.len() {
        if typed.is_null(row) {
            builder.append(false);
            continue;
        }
        builder.values().append_value(typed.value(row));
        builder.append(true);
    }
    Ok(Arc::new(builder.finish()))
}

fn wrap_binary_array_in_list(array: &ArrayRef, item_type: &DataType) -> Result<ArrayRef> {
    let typed = array
        .as_any()
        .downcast_ref::<BinaryArray>()
        .ok_or_else(|| {
            DataFusionError::Internal(format!(
                "expected array {:?}, got {:?}",
                item_type,
                array.data_type()
            ))
        })?;

    let mut builder = ListBuilder::new(BinaryBuilder::default());
    for row in 0..typed.len() {
        if typed.is_null(row) {
            builder.append(false);
            continue;
        }
        builder.values().append_value(typed.value(row));
        builder.append(true);
    }
    Ok(Arc::new(builder.finish()))
}

fn wrap_typed_array<ArrayType, ValueBuilder, Value>(
    array: &ArrayRef,
    item_type: &DataType,
    value_at: impl Fn(&ArrayType, usize) -> Value,
) -> Result<ArrayRef>
where
    ArrayType: Array + 'static,
    ValueBuilder: arrow::array::ArrayBuilder + Default + ValueAppender<Value>,
    Value: Copy,
{
    let typed = array.as_any().downcast_ref::<ArrayType>().ok_or_else(|| {
        DataFusionError::Internal(format!(
            "expected array {:?}, got {:?}",
            item_type,
            array.data_type()
        ))
    })?;

    let mut builder = ListBuilder::new(ValueBuilder::default());
    for row in 0..typed.len() {
        if typed.is_null(row) {
            builder.append(false);
            continue;
        }
        builder.values().append_value(value_at(typed, row));
        builder.append(true);
    }
    Ok(Arc::new(builder.finish()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Int64Array, ListArray, StringArray};

    #[test]
    fn scalar_cast_supported_accepts_numeric_to_utf8_view() {
        assert!(scalar_cast_supported(
            &DataType::Float64,
            &DataType::Utf8View
        ));
        assert!(scalar_cast_supported(
            &DataType::UInt64,
            &DataType::Utf8View
        ));
    }

    #[test]
    fn scalar_cast_supported_accepts_utf8_dictionary_to_utf8_view() {
        let dict_type = DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8));
        assert!(scalar_cast_supported(&dict_type, &DataType::Utf8View));
    }

    #[test]
    fn mixed_fast_field_types_infer_list_utf8() {
        let left = Arc::new(Schema::new(vec![Field::new(
            "mixed",
            DataType::Int64,
            true,
        )]));
        let right = Arc::new(Schema::new(vec![Field::new("mixed", DataType::Utf8, true)]));

        let schema = infer_canonical_fast_field_schema(&[left, right]).unwrap();

        assert_eq!(
            schema.field_with_name("mixed").unwrap().data_type(),
            &DataType::new_list(DataType::Utf8, true)
        );
    }

    #[test]
    fn mixed_list_and_scalar_fast_field_types_infer_list_utf8() {
        let left = Arc::new(Schema::new(vec![Field::new(
            "mixed",
            DataType::new_list(DataType::Int64, true),
            true,
        )]));
        let right = Arc::new(Schema::new(vec![Field::new("mixed", DataType::Utf8, true)]));

        let schema = infer_canonical_fast_field_schema(&[left, right]).unwrap();

        assert_eq!(
            schema.field_with_name("mixed").unwrap().data_type(),
            &DataType::new_list(DataType::Utf8, true)
        );
    }

    #[test]
    fn list_i64_coerces_to_list_utf8() {
        let mut builder = ListBuilder::new(Int64Builder::new());
        builder.values().append_value(12);
        builder.append(true);
        builder.append(false);
        let source = Arc::new(builder.finish()) as ArrayRef;
        let target = DataType::new_list(DataType::Utf8, true);

        let coercion = plan_fast_field_coercion(source.data_type(), &target).unwrap();
        let coerced = coerce_array(&source, &coercion).unwrap();
        let list = coerced.as_any().downcast_ref::<ListArray>().unwrap();
        let values = list
            .values()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();

        assert_eq!(list.value_offsets(), &[0, 1, 1]);
        assert_eq!(values.value(0), "12");
        assert!(list.is_null(1));
    }

    #[test]
    fn list_utf8_try_coerces_to_list_i64() {
        let mut builder = ListBuilder::new(StringBuilder::new());
        builder.values().append_value("101");
        builder.values().append_value("not-a-number");
        builder.values().append_null();
        builder.append(true);
        builder.append(false);
        builder.values().append_value("202");
        builder.append(true);
        let source = Arc::new(builder.finish()) as ArrayRef;
        let target = DataType::new_list(DataType::Int64, true);

        let coercion = plan_fast_field_coercion(source.data_type(), &target).unwrap();
        assert_eq!(
            coercion,
            FastFieldCoercion::TryCastUtf8ListToNumericList(DataType::Int64)
        );
        let coerced = coerce_array(&source, &coercion).unwrap();
        let list = coerced.as_any().downcast_ref::<ListArray>().unwrap();
        let values = list.values().as_any().downcast_ref::<Int64Array>().unwrap();

        assert_eq!(list.value_offsets(), &[0, 3, 3, 1 + 3]);
        assert_eq!(values.value(0), 101);
        assert!(values.is_null(1));
        assert!(values.is_null(2));
        assert!(list.is_null(1));
        assert_eq!(values.value(3), 202);
    }

    #[test]
    fn scalar_utf8_try_coerces_to_list_f64() {
        let source = Arc::new(StringArray::from(vec![
            Some("1.5"),
            Some("bad"),
            None,
            Some("3"),
        ])) as ArrayRef;
        let target = DataType::new_list(DataType::Float64, true);

        let coercion = plan_fast_field_coercion(source.data_type(), &target).unwrap();
        assert_eq!(
            coercion,
            FastFieldCoercion::TryCastUtf8ToNumericList(DataType::Float64)
        );
        let coerced = coerce_array(&source, &coercion).unwrap();
        let list = coerced.as_any().downcast_ref::<ListArray>().unwrap();
        let values = list
            .values()
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();

        assert_eq!(list.value_offsets(), &[0, 1, 1, 1, 2]);
        assert_eq!(values.value(0), 1.5);
        assert!(list.is_null(1));
        assert!(list.is_null(2));
        assert_eq!(values.value(1), 3.0);
    }

    #[test]
    fn list_presence_mask_treats_all_null_list_as_absent() {
        let mut builder = ListBuilder::new(Int64Builder::new());
        builder.values().append_null();
        builder.append(true);
        builder.append(true);
        builder.values().append_value(5);
        builder.append(true);
        builder.append(false);
        let source = Arc::new(builder.finish()) as ArrayRef;

        let mask = source_value_present_mask(&source).unwrap();

        assert!(!mask.value(0));
        assert!(!mask.value(1));
        assert!(mask.value(2));
        assert!(!mask.value(3));
    }

    #[test]
    fn timestamp_micros_coerces_to_epoch_millis_i64() {
        let source = Arc::new(TimestampMicrosecondArray::from(vec![
            Some(1_779_287_400_123_000),
            None,
        ])) as ArrayRef;

        let coerced = coerce_array(&source, &FastFieldCoercion::TimestampMicrosToMillis).unwrap();
        let millis = coerced.as_any().downcast_ref::<Int64Array>().unwrap();

        assert_eq!(millis.value(0), 1_779_287_400_123);
        assert!(millis.is_null(1));
    }

    #[test]
    fn epoch_millis_i64_coerces_to_timestamp_micros() {
        let source = Arc::new(Int64Array::from(vec![Some(1_779_287_400_123), None])) as ArrayRef;
        let target = DataType::Timestamp(TimeUnit::Microsecond, None);

        let coercion = plan_fast_field_coercion(source.data_type(), &target).unwrap();
        let coerced = coerce_array(&source, &coercion).unwrap();
        let timestamp = coerced
            .as_any()
            .downcast_ref::<TimestampMicrosecondArray>()
            .unwrap();

        assert_eq!(timestamp.value(0), 1_779_287_400_123_000);
        assert!(timestamp.is_null(1));
    }

    #[test]
    fn timestamp_micros_coerces_to_epoch_millis_list() {
        let source = Arc::new(TimestampMicrosecondArray::from(vec![
            Some(1_779_287_400_123_000),
            None,
        ])) as ArrayRef;

        let coerced =
            coerce_array(&source, &FastFieldCoercion::TimestampMicrosToMillisList).unwrap();
        let list = coerced.as_any().downcast_ref::<ListArray>().unwrap();
        let values = list.values().as_any().downcast_ref::<Int64Array>().unwrap();

        assert_eq!(list.value_offsets(), &[0, 1, 1]);
        assert_eq!(values.value(0), 1_779_287_400_123);
        assert!(list.is_null(1));
    }

    #[test]
    fn projection_matches_physical_field_by_logical_metadata() {
        let source_schema = Arc::new(Schema::new(vec![
            Field::new("_doc_id", DataType::UInt32, false),
            Field::new(
                "_dynamic.custom.mixed",
                DataType::new_list(DataType::Utf8, true),
                true,
            )
            .with_metadata(std::collections::HashMap::from([(
                crate::FAST_FIELD_LOGICAL_NAME_METADATA_KEY.to_string(),
                "custom.mixed".to_string(),
            )])),
        ]));
        let canonical_schema = Arc::new(Schema::new(vec![
            Field::new("_doc_id", DataType::UInt32, false),
            Field::new(
                "custom.mixed",
                DataType::new_list(DataType::Utf8, true),
                true,
            ),
        ]));

        let plan = plan_fast_field_projection(&source_schema, &canonical_schema).unwrap();

        assert_eq!(
            plan.columns[1]
                .sources
                .first()
                .map(|source| source.source_name.as_str()),
            Some("_dynamic.custom.mixed")
        );
        assert_eq!(plan.columns[1].output_field.name(), "custom.mixed");
    }

    #[test]
    fn projection_coalesces_multiple_physical_lanes() {
        let source_schema = Arc::new(Schema::new(vec![
            Field::new(
                "custom.mixed__qw_lane_00_str",
                DataType::new_list(DataType::Utf8, true),
                true,
            )
            .with_metadata(std::collections::HashMap::from([(
                crate::FAST_FIELD_LOGICAL_NAME_METADATA_KEY.to_string(),
                "custom.mixed".to_string(),
            )])),
            Field::new(
                "custom.mixed__qw_lane_01_i64",
                DataType::new_list(DataType::Int64, true),
                true,
            )
            .with_metadata(std::collections::HashMap::from([(
                crate::FAST_FIELD_LOGICAL_NAME_METADATA_KEY.to_string(),
                "custom.mixed".to_string(),
            )])),
        ]));
        let canonical_schema = Arc::new(Schema::new(vec![Field::new(
            "custom.mixed",
            DataType::new_list(DataType::Utf8, true),
            true,
        )]));
        let mut strings = ListBuilder::new(StringBuilder::new());
        strings.values().append_value("green");
        strings.append(true);
        strings.append(true);
        let mut ints = ListBuilder::new(Int64Builder::new());
        ints.append(false);
        ints.values().append_value(101);
        ints.append(true);
        let source_batch = RecordBatch::try_new(
            Arc::clone(&source_schema),
            vec![Arc::new(strings.finish()), Arc::new(ints.finish())],
        )
        .unwrap();

        let plan = plan_fast_field_projection(&source_schema, &canonical_schema).unwrap();
        let projected = apply_fast_field_projection(&source_batch, &plan).unwrap();
        let list = projected
            .column(0)
            .as_any()
            .downcast_ref::<ListArray>()
            .unwrap();
        let values = list
            .values()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();

        assert_eq!(plan.columns[0].sources.len(), 2);
        assert_eq!(list.value_offsets(), &[0, 1, 2]);
        assert_eq!(values.value(0), "green");
        assert_eq!(values.value(1), "101");
    }

    #[test]
    fn projection_coalesces_numeric_string_lanes_for_merge_integer() {
        let source_schema = Arc::new(Schema::new(vec![
            Field::new(
                "custom.mixed__qw_lane_00_i64",
                DataType::new_list(DataType::Int64, true),
                true,
            )
            .with_metadata(std::collections::HashMap::from([(
                crate::FAST_FIELD_LOGICAL_NAME_METADATA_KEY.to_string(),
                "custom.mixed".to_string(),
            )])),
            Field::new(
                "custom.mixed__qw_lane_02_str",
                DataType::new_list(DataType::Utf8, true),
                true,
            )
            .with_metadata(std::collections::HashMap::from([(
                crate::FAST_FIELD_LOGICAL_NAME_METADATA_KEY.to_string(),
                "custom.mixed".to_string(),
            )])),
        ]));
        let canonical_schema = Arc::new(Schema::new(vec![Field::new(
            "custom.mixed",
            DataType::new_list(DataType::Int64, true),
            true,
        )]));
        let mut ints = ListBuilder::new(Int64Builder::new());
        ints.append(false);
        ints.values().append_value(200);
        ints.append(true);
        ints.append(false);
        let mut strings = ListBuilder::new(StringBuilder::new());
        strings.values().append_value("101");
        strings.append(true);
        strings.values().append_value("999");
        strings.append(true);
        strings.values().append_value("bad");
        strings.append(true);
        let source_batch = RecordBatch::try_new(
            Arc::clone(&source_schema),
            vec![Arc::new(ints.finish()), Arc::new(strings.finish())],
        )
        .unwrap();

        let plan = plan_fast_field_projection(&source_schema, &canonical_schema).unwrap();
        let projected = apply_fast_field_projection(&source_batch, &plan).unwrap();
        let list = projected
            .column(0)
            .as_any()
            .downcast_ref::<ListArray>()
            .unwrap();
        let values = list.values().as_any().downcast_ref::<Int64Array>().unwrap();

        assert_eq!(plan.columns[0].sources.len(), 2);
        assert_eq!(list.value_offsets(), &[0, 1, 2, 3]);
        assert_eq!(values.value(0), 101);
        assert_eq!(values.value(1), 200);
        assert!(values.is_null(2));
    }
}
