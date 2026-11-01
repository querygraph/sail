use std::collections::HashMap;
use std::sync::Arc;

use arrow::datatypes::{DataType, Field, Fields, Schema, SchemaRef, TimeUnit};
use datafusion_common::{Result, not_impl_err, plan_datafusion_err, plan_err};
use orc_rust::arrow_writer::ORC_TYPE_ATTRIBUTE_PREFIX;
use sail_common::spec;
use sail_common_datafusion::variant::is_marked_variant_storage_type;

use super::schema::{CATALYST_TYPE, CHAR_VARCHAR_TYPE};

/// Schema admitted for ORC writing, and how each column is stored.
///
/// Admission inspects logical metadata as well as storage types, at every depth,
/// before any encoder or output is opened. Native physical support alone is
/// insufficient: TIME/interval/UDT/extension semantics must not disappear into
/// primitive ORC columns.
///
/// The stored schema follows Spark 4.2.1's `OrcUtils.orcTypeDescription` with one
/// difference. A TIMESTAMP_NTZ is a LONG of microseconds, and a TIMESTAMP is an
/// ORC TIMESTAMP_INSTANT, each with a Catalyst annotation. Spark's own TIMESTAMP
/// kind holds a wall clock that readers keep across zones. It only returns the
/// instant to a reader in the writer's JVM zone, which a Sail worker cannot know.
#[derive(Debug, Clone)]
pub struct OrcWriteSchema {
    schema: SchemaRef,
    file_schema: SchemaRef,
}

impl OrcWriteSchema {
    pub fn try_new(schema: SchemaRef) -> Result<Self> {
        if schema.fields().is_empty() {
            return plan_err!("ORC writing requires a nonempty schema");
        }
        let fields = schema
            .fields()
            .iter()
            .map(|field| stored_field(field))
            .collect::<Result<Vec<_>>>()?;
        let file_schema = Arc::new(Schema::new_with_metadata(fields, schema.metadata().clone()));
        Ok(Self {
            schema,
            file_schema,
        })
    }

    /// The original schema, including nullability and non-type annotations.
    /// Keeping annotations here does not promise their persistence in ORC files.
    pub fn schema(&self) -> &SchemaRef {
        &self.schema
    }

    /// The schema the native writer receives.
    pub fn file_schema(&self) -> &SchemaRef {
        &self.file_schema
    }
}

/// The ORC columns below the root of a stored schema.
pub fn column_count(fields: &Fields) -> usize {
    fields
        .iter()
        .map(|field| {
            1 + match field.data_type() {
                DataType::Struct(fields) => column_count(fields),
                DataType::List(child) | DataType::LargeList(child) => {
                    column_count(&Fields::from(vec![child.clone()]))
                }
                DataType::Map(entries, _) => match entries.data_type() {
                    DataType::Struct(fields) => column_count(fields),
                    _ => 0,
                },
                _ => 0,
            }
        })
        .sum()
}

fn validate_metadata(field: &Field) -> Result<()> {
    // Reject unknown members of the semantic namespaces too. Checking only a
    // recognized extension name would admit orphaned/malformed extension metadata.
    if let Some(key) = field
        .metadata()
        .keys()
        .filter(|key| key.starts_with("ARROW:extension:") || key.starts_with("SAIL::spark::"))
        .min()
    {
        return not_impl_err!(
            "ORC writing does not support logical metadata {key:?} for field {:?}",
            field.name()
        );
    }
    if let Some(json) = field.metadata().get(spec::SPARK_METADATA_JSON_KEY) {
        let metadata: serde_json::Map<String, serde_json::Value> = serde_json::from_str(json)
            .map_err(|error| {
                plan_datafusion_err!(
                    "invalid Spark metadata for ORC write field {:?}: {error}",
                    field.name()
                )
            })?;
        for key in [CHAR_VARCHAR_TYPE, "__COLLATIONS"] {
            if metadata.contains_key(key) {
                return not_impl_err!(
                    "ORC writing does not support Spark type metadata {key:?} for field {:?}",
                    field.name()
                );
            }
        }
    }
    Ok(())
}

fn stored_fields(fields: &Fields) -> Result<Fields> {
    fields
        .iter()
        .map(|field| stored_field(field).map(Arc::new))
        .collect()
}

fn stored_field(field: &Field) -> Result<Field> {
    validate_metadata(field)?;
    // A variant nested by a function keeps only the marker inside its storage,
    // and would otherwise be written as an ordinary struct of two binaries.
    if is_marked_variant_storage_type(field.data_type()) {
        return not_impl_err!(
            "ORC writing does not support type VARIANT for field {:?}",
            field.name()
        );
    }
    let unsupported = || {
        not_impl_err!(
            "ORC writing does not support type {:?} for field {:?}",
            field.data_type(),
            field.name()
        )
    };
    let (data_type, catalyst_type) = match field.data_type() {
        DataType::Boolean
        | DataType::Int8
        | DataType::Int16
        | DataType::Int32
        | DataType::Int64
        | DataType::Float32
        | DataType::Float64
        | DataType::Date32
        | DataType::Utf8
        | DataType::LargeUtf8
        | DataType::Binary
        | DataType::LargeBinary => (field.data_type().clone(), None),
        DataType::Utf8View => (DataType::Utf8, None),
        DataType::BinaryView => (DataType::Binary, None),
        // Storage is in microseconds, which cannot hold every nanosecond value.
        DataType::Timestamp(TimeUnit::Nanosecond, _) => return unsupported(),
        DataType::Timestamp(_, None) => (DataType::Int64, Some("timestamp_ntz")),
        // The values are the instants, whatever zone labels them.
        DataType::Timestamp(_, Some(_)) => (
            DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
            Some("timestamp"),
        ),
        DataType::Decimal128(precision, scale) if (1..=38).contains(precision) && *scale >= 0 => {
            (field.data_type().clone(), None)
        }
        DataType::Struct(fields) if !fields.is_empty() => {
            (DataType::Struct(stored_fields(fields)?), None)
        }
        DataType::List(child) => (DataType::List(Arc::new(stored_field(child)?)), None),
        DataType::LargeList(child) => (DataType::LargeList(Arc::new(stored_field(child)?)), None),
        DataType::Map(entries, sorted) => match entries.data_type() {
            DataType::Struct(fields) if fields.len() == 2 => {
                let fields = DataType::Struct(stored_fields(fields)?);
                let entries = entries.as_ref().clone().with_data_type(fields);
                (DataType::Map(Arc::new(entries), *sorted), None)
            }
            _ => return unsupported(),
        },
        _ => return unsupported(),
    };
    let mut metadata: HashMap<_, _> = field.metadata().clone();
    if let Some(value) = catalyst_type {
        metadata.insert(
            format!("{ORC_TYPE_ATTRIBUTE_PREFIX}{CATALYST_TYPE}"),
            value.to_string(),
        );
    }
    Ok(field
        .clone()
        .with_data_type(data_type)
        .with_metadata(metadata))
}

#[cfg(test)]
#[path = "write_schema_tests.rs"]
mod tests;
