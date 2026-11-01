use arrow::datatypes::{DataType, Field, SchemaRef};
use datafusion_common::{Result, not_impl_err, plan_datafusion_err, plan_err};
use sail_common::spec;

use super::schema::CHAR_VARCHAR_TYPE;

/// Schema admitted for the initial primitive ORC writer.
///
/// Admission inspects logical metadata as well as storage types, before any
/// encoder or output is opened. Native physical support alone is insufficient:
/// TIME/interval/UDT/extension semantics must not disappear into primitive ORC
/// columns. Temporal, nested and decimal types need separate writer support.
/// This component does not activate the public writer or guarantee upload cleanup.
#[derive(Debug, Clone)]
pub struct OrcWriteSchema {
    schema: SchemaRef,
}

impl OrcWriteSchema {
    pub fn try_new(schema: SchemaRef) -> Result<Self> {
        if schema.fields().is_empty() {
            return plan_err!("ORC writing requires a nonempty schema");
        }
        for field in schema.fields() {
            validate_field(field)?;
        }
        Ok(Self { schema })
    }

    /// The original schema, including nullability and non-type annotations.
    /// Keeping annotations here does not promise their persistence in ORC files.
    pub fn schema(&self) -> &SchemaRef {
        &self.schema
    }
}

fn validate_field(field: &Field) -> Result<()> {
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
    match field.data_type() {
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
        | DataType::LargeBinary => Ok(()),
        data_type => not_impl_err!(
            "ORC writing does not support type {data_type:?} for field {:?}",
            field.name()
        ),
    }
}

#[cfg(test)]
#[path = "write_schema_tests.rs"]
mod tests;
