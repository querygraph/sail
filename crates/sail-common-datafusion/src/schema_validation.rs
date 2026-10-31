//! Spark schema name validation, shared by readers and schema changes.
use std::collections::{HashMap, HashSet};

use datafusion::arrow::datatypes::{DataType, FieldRef};
use datafusion_common::{DataFusionError, Result};
use sail_common::utils::root_lowercase::spark_root_lowercase;

use crate::error::{StructuredSparkError, quote_sql_identifier};

/// Check each struct's names before descending into its children, as SchemaUtils
/// does. Report the UTF-16 lexical minimum duplicate at that level.
pub fn validate_field_names(fields: &[FieldRef], case_sensitive: bool) -> Result<()> {
    let mut names = HashSet::new();
    let mut duplicate: Option<String> = None;
    for field in fields {
        let name = if case_sensitive {
            field.name().clone()
        } else {
            spark_root_lowercase(field.name())
        };
        if !names.insert(name.clone())
            && duplicate
                .as_ref()
                .is_none_or(|previous| name.encode_utf16().cmp(previous.encode_utf16()).is_lt())
        {
            duplicate = Some(name);
        }
    }
    if let Some(name) = duplicate {
        let column = quote_sql_identifier(&name)?;
        return Err(structured(
            "COLUMN_ALREADY_EXISTS",
            "42711",
            format!(
                "The column {column} already exists. Choose another name or rename the existing column."
            ),
            "columnName",
            column,
        ));
    }
    for field in fields {
        validate_type(field.data_type(), case_sensitive)?;
    }
    Ok(())
}

fn validate_type(ty: &DataType, case_sensitive: bool) -> Result<()> {
    match ty {
        DataType::Struct(fields) => validate_field_names(fields, case_sensitive),
        DataType::List(field)
        | DataType::LargeList(field)
        | DataType::FixedSizeList(field, _)
        | DataType::Map(field, _) => validate_type(field.data_type(), case_sensitive),
        _ => Ok(()),
    }
}

fn structured(
    condition: &str,
    state: &str,
    message: String,
    key: &str,
    value: String,
) -> DataFusionError {
    DataFusionError::External(Box::new(StructuredSparkError {
        class: "org.apache.spark.sql.AnalysisException".into(),
        condition: condition.into(),
        sql_state: state.into(),
        message,
        parameters: HashMap::from([(key.into(), value)]),
    }))
}

#[cfg(test)]
#[path = "schema_validation_tests.rs"]
mod tests;
