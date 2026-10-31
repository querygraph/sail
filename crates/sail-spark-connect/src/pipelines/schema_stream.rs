//! The schema a streaming read of a virtual table exposes.
//!
//! Spark loads a streaming virtual input as `MemoryStream(ExpressionEncoder(schema))`,
//! so the read exposes the encoder's serializer schema instead of the input schema.
//! That differs in ways a batch read does not, as measured against Spark 4.2.1:
//!
//! - Field metadata is dropped at every depth, because `ExpressionEncoder.schema`
//!   and `CreateNamedStruct` take no metadata from the serializer expressions.
//!   Field names and nullability are retained at every depth.
//! - Year-month and day-time intervals widen to their full families.
//! - TIME widens to TIME(6) unless the types framework is enabled.
//! - Declared CHAR and VARCHAR have no encoder unless their type information is
//!   preserved, and the read fails during the consuming flow's analysis.
//!
//! This is the single definition of that contract. Batch reads, final inferred
//! schemas, publication and command reanalysis do not go through it.
use std::collections::HashMap;
use std::sync::Arc;

use sail_common::spec;

use super::schema_merge::quoted_type;
use crate::config::EncoderSchemaSettings;
use crate::error::{SparkResult, StructuredSparkError};

pub(super) fn streaming_read_fields(
    fields: &spec::Fields,
    settings: EncoderSchemaSettings,
) -> SparkResult<spec::Fields> {
    fields
        .iter()
        .map(|field| {
            let mut field = field.as_ref().clone();
            field.data_type = streaming_read_type(&field.data_type, settings)?;
            field
                .metadata
                .retain(|(key, _)| key != spec::SPARK_METADATA_JSON_KEY);
            Ok(Arc::new(field))
        })
        .collect()
}

fn streaming_read_type(
    data_type: &spec::DataType,
    settings: EncoderSchemaSettings,
) -> SparkResult<spec::DataType> {
    let mut data_type = data_type.clone();
    match &mut data_type {
        spec::DataType::Struct { fields } => *fields = streaming_read_fields(fields, settings)?,
        spec::DataType::List { data_type, .. }
        | spec::DataType::LargeList { data_type, .. }
        | spec::DataType::FixedSizeList { data_type, .. } => {
            **data_type = streaming_read_type(data_type, settings)?;
        }
        spec::DataType::Map {
            key_type,
            value_type,
            ..
        } => {
            **key_type = streaming_read_type(key_type, settings)?;
            **value_type = streaming_read_type(value_type, settings)?;
        }
        spec::DataType::Interval {
            interval_unit: spec::IntervalUnit::YearMonth,
            start_field,
            end_field,
        } => {
            *start_field = Some(spec::IntervalFieldType::Year);
            *end_field = Some(spec::IntervalFieldType::Month);
        }
        spec::DataType::Interval {
            interval_unit: spec::IntervalUnit::DayTime,
            start_field,
            end_field,
        } => {
            *start_field = Some(spec::IntervalFieldType::Day);
            *end_field = Some(spec::IntervalFieldType::Second);
        }
        spec::DataType::Time { precision } if !settings.types_framework_enabled => {
            *precision = 6;
        }
        spec::DataType::ConfiguredUtf8 {
            utf8_type: spec::Utf8Type::Char { .. } | spec::Utf8Type::VarChar { .. },
        } if !settings.preserve_char_varchar_type_info => {
            return Err(unsupported_encoder(&data_type)?);
        }
        _ => {}
    }
    Ok(data_type)
}

fn unsupported_encoder(data_type: &spec::DataType) -> SparkResult<crate::error::SparkError> {
    let quoted = quoted_type(data_type)?;
    Ok(Box::new(StructuredSparkError {
        class: "org.apache.spark.sql.AnalysisException".into(),
        condition: "UNSUPPORTED_DATA_TYPE_FOR_ENCODER".into(),
        sql_state: "0A000".into(),
        message: format!(
            "Cannot create encoder for {quoted}. Please use a different output data type for your UDF or DataFrame."
        ),
        parameters: HashMap::from([("dataType".into(), quoted)]),
    })
    .into())
}

#[cfg(test)]
#[path = "schema_stream_tests.rs"]
mod tests;
