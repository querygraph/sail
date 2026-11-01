//! Messages into a record batch, following the sail-iggy reader
//! (`formats/iggy/reader.rs` at `5816b9b`). Any message that does not decode
//! fails the whole range: skipping one would still advance progress past it.

use std::sync::Arc;

use datafusion::arrow::array::{
    ArrayRef, BinaryArray, BooleanArray, Float64Array, Int32Array, Int64Array, LargeBinaryBuilder,
    RecordBatch, StringArray, TimestampMicrosecondArray, TimestampNanosecondArray,
};
use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef, TimeUnit};
use datafusion::common::{DataFusionError, Result, exec_datafusion_err, plan_err};
use iggy::prelude::IggyMessage;

use crate::config::{IggyBoundedConfig, PayloadFormat};
use crate::envelope::{IggyEnvelopeContext, envelope_batch, validate_envelope_schema};

/// The schema of a raw read.
pub fn raw_schema() -> Schema {
    Schema::new(vec![
        Arc::new(Field::new("offset", DataType::Int64, false)),
        Arc::new(Field::new(
            "timestamp",
            DataType::Timestamp(TimeUnit::Nanosecond, Some("UTC".into())),
            false,
        )),
        Arc::new(Field::new("id", DataType::Binary, false)),
        Arc::new(Field::new("payload", DataType::LargeBinary, false)),
        Arc::new(Field::new("user_headers", DataType::LargeBinary, true)),
    ])
}

/// Whether rows of this format can fill this schema. Checked when a source
/// is built, so that a type mismatch is a planning error and not a corrupt
/// message found in the middle of a run.
pub fn validate_schema(config: &IggyBoundedConfig, schema: &Schema) -> Result<()> {
    match config.format {
        PayloadFormat::Raw => {
            if schema.fields() != raw_schema().fields() {
                return plan_err!("invalid raw Iggy schema");
            }
        }
        PayloadFormat::Envelope => {
            validate_envelope_schema(schema)?;
            if config.source_id.is_none() {
                return plan_err!("Iggy envelope reads require source_id");
            }
        }
        PayloadFormat::Json => {
            for field in schema.fields() {
                if !json_supports(field.data_type()) {
                    return plan_err!(
                        "unsupported Iggy JSON field type for {}: {}",
                        field.name(),
                        field.data_type()
                    );
                }
            }
        }
    }
    Ok(())
}

fn json_supports(data_type: &DataType) -> bool {
    matches!(
        data_type,
        DataType::Utf8
            | DataType::Boolean
            | DataType::Int32
            | DataType::Int64
            | DataType::Float64
            | DataType::Timestamp(TimeUnit::Microsecond | TimeUnit::Nanosecond, _)
    )
}

pub fn decode_messages(
    config: &IggyBoundedConfig,
    partition: u32,
    schema: &SchemaRef,
    messages: &[IggyMessage],
) -> Result<RecordBatch> {
    match config.format {
        PayloadFormat::Raw => decode_raw(schema, messages),
        PayloadFormat::Json => decode_json(schema, messages),
        PayloadFormat::Envelope => envelope_batch(
            Arc::clone(schema),
            IggyEnvelopeContext {
                source_id: config
                    .source_id
                    .as_deref()
                    .ok_or_else(|| exec_datafusion_err!("Iggy envelope reads require source_id"))?,
                stream: &config.stream,
                topic: &config.topic,
                partition,
            },
            messages,
        ),
    }
}

fn decode_raw(schema: &SchemaRef, messages: &[IggyMessage]) -> Result<RecordBatch> {
    let offsets = messages
        .iter()
        .map(|message| {
            i64::try_from(message.header.offset)
                .map_err(|_| exec_datafusion_err!("Iggy offset exceeds Int64"))
        })
        .collect::<Result<Vec<_>>>()?;
    let timestamps = messages
        .iter()
        .map(|message| {
            i64::try_from(message.header.timestamp)
                .ok()
                .and_then(|value| value.checked_mul(1_000))
                .ok_or_else(|| exec_datafusion_err!("Iggy timestamp exceeds nanosecond range"))
        })
        .collect::<Result<Vec<_>>>()?;
    let ids = BinaryArray::from_iter_values(
        messages
            .iter()
            .map(|message| message.header.id.to_le_bytes()),
    );
    let mut payloads = LargeBinaryBuilder::new();
    let mut headers = LargeBinaryBuilder::new();
    for message in messages {
        payloads.append_value(message.payload.as_ref());
        headers.append_option(message.user_headers.as_deref());
    }
    let columns: Vec<ArrayRef> = vec![
        Arc::new(Int64Array::from(offsets)),
        Arc::new(TimestampNanosecondArray::from(timestamps).with_timezone("UTC")),
        Arc::new(ids),
        Arc::new(payloads.finish()),
        Arc::new(headers.finish()),
    ];
    RecordBatch::try_new(Arc::clone(schema), columns).map_err(DataFusionError::from)
}

fn decode_json(schema: &SchemaRef, messages: &[IggyMessage]) -> Result<RecordBatch> {
    let values = messages
        .iter()
        .map(|message| {
            serde_json::from_slice::<serde_json::Value>(&message.payload)
                .map_err(|e| exec_datafusion_err!("invalid Iggy JSON payload: {e}"))
        })
        .collect::<Result<Vec<_>>>()?;
    let columns = schema
        .fields()
        .iter()
        .map(|field| json_column(field, &values))
        .collect::<Result<Vec<_>>>()?;
    RecordBatch::try_new(Arc::clone(schema), columns).map_err(DataFusionError::from)
}

/// One field across every message, each value converted by `convert`.
fn field_values<T>(
    field: &Field,
    values: &[serde_json::Value],
    convert: impl Fn(&serde_json::Value) -> Result<T>,
) -> Result<Vec<Option<T>>> {
    values
        .iter()
        .map(|value| {
            required_field_value(value, field.name(), field.is_nullable())?
                .map(&convert)
                .transpose()
        })
        .collect()
}

fn json_column(field: &Field, values: &[serde_json::Value]) -> Result<ArrayRef> {
    let name = field.name();
    Ok(match field.data_type() {
        DataType::Utf8 => {
            let column = field_values(field, values, |value| {
                value
                    .as_str()
                    .map(str::to_owned)
                    .ok_or_else(|| exec_datafusion_err!("Iggy JSON field {name} must be a string"))
            })?;
            Arc::new(StringArray::from(column))
        }
        DataType::Boolean => {
            let column = field_values(field, values, |value| {
                value
                    .as_bool()
                    .ok_or_else(|| exec_datafusion_err!("Iggy JSON field {name} must be a boolean"))
            })?;
            Arc::new(BooleanArray::from(column))
        }
        DataType::Int32 => {
            let column = field_values(field, values, |value| {
                value
                    .as_i64()
                    .and_then(|value| i32::try_from(value).ok())
                    .ok_or_else(|| {
                        exec_datafusion_err!("Iggy JSON field {name} must be a 32-bit integer")
                    })
            })?;
            Arc::new(Int32Array::from(column))
        }
        DataType::Int64 => {
            let column = field_values(field, values, |value| {
                value.as_i64().ok_or_else(|| {
                    exec_datafusion_err!("Iggy JSON field {name} must be a 64-bit integer")
                })
            })?;
            Arc::new(Int64Array::from(column))
        }
        DataType::Float64 => {
            let column = field_values(field, values, |value| {
                value.as_f64().ok_or_else(|| {
                    exec_datafusion_err!("Iggy JSON field {name} must be a floating point number")
                })
            })?;
            Arc::new(Float64Array::from(column))
        }
        DataType::Timestamp(TimeUnit::Nanosecond, timezone) => {
            let column = field_values(field, values, |value| parse_json_timestamp(name, value))?;
            Arc::new(TimestampNanosecondArray::from(column).with_timezone_opt(timezone.clone()))
        }
        DataType::Timestamp(TimeUnit::Microsecond, timezone) => {
            let column = field_values(field, values, |value| {
                Ok(parse_json_timestamp(name, value)?.div_euclid(1_000))
            })?;
            Arc::new(TimestampMicrosecondArray::from(column).with_timezone_opt(timezone.clone()))
        }
        other => return plan_err!("unsupported Iggy JSON field type for {name}: {other}"),
    })
}

fn required_field_value<'a>(
    value: &'a serde_json::Value,
    name: &str,
    nullable: bool,
) -> Result<Option<&'a serde_json::Value>> {
    let object = value
        .as_object()
        .ok_or_else(|| exec_datafusion_err!("Iggy JSON payload must be an object"))?;
    match object.get(name) {
        Some(value) if !value.is_null() => Ok(Some(value)),
        _ if nullable => Ok(None),
        _ => Err(exec_datafusion_err!(
            "Iggy JSON field {name} is required but missing or null"
        )),
    }
}

/// An RFC 3339 string or a nanosecond epoch integer, in the fork's terms.
fn parse_json_timestamp(name: &str, value: &serde_json::Value) -> Result<i64> {
    if let Some(value) = value.as_i64() {
        return Ok(value);
    }
    let Some(value) = value.as_str() else {
        return Err(exec_datafusion_err!(
            "Iggy JSON field {name} must be an RFC 3339 timestamp string or nanosecond epoch integer"
        ));
    };
    let timestamp = chrono::DateTime::parse_from_rfc3339(value)
        .map_err(|e| exec_datafusion_err!("invalid Iggy JSON timestamp field {name}: {e}"))?;
    timestamp.timestamp_nanos_opt().ok_or_else(|| {
        exec_datafusion_err!("Iggy JSON timestamp field {name} is out of nanosecond range")
    })
}
