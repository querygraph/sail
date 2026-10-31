//! Spark schema name validation, shared by readers and schema changes.
use std::collections::{HashMap, HashSet};

use datafusion::arrow::datatypes::{DataType, FieldRef};
use datafusion_common::{DataFusionError, Result};
use sail_common::utils::root_lowercase::spark_root_lowercase;

use crate::error::StructuredSparkError;

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
        let column = sql_identifier(&name)?;
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

/// SchemaUtils passes the raw duplicate name to toSQLId(String), which interprets
/// dots/backticks as AttributeNameParser syntax. It does not quote it as one part.
fn sql_identifier(name: &str) -> Result<String> {
    let invalid = || {
        structured(
            "INVALID_ATTRIBUTE_NAME_SYNTAX",
            "42601",
            format!(
                "Syntax error in the attribute name: {name}. Check that backticks appear in pairs, a quoted string is a complete name part and use a backtick only inside quoted name parts."
            ),
            "name",
            name.into(),
        )
    };
    let mut parts = vec![];
    let mut part = String::new();
    let mut quoted = false;
    let mut previous = None;
    let mut chars = name.chars().peekable();
    while let Some(c) = chars.next() {
        if quoted {
            if c == '`' {
                if chars.peek() == Some(&'`') {
                    part.push('`');
                    chars.next();
                } else {
                    quoted = false;
                    if chars.peek().is_some_and(|next| *next != '.') {
                        return Err(invalid());
                    }
                }
            } else {
                part.push(c);
            }
        } else if c == '`' {
            if !part.is_empty() {
                return Err(invalid());
            }
            quoted = true;
        } else if c == '.' {
            if previous.is_none() || previous == Some('.') || chars.peek().is_none() {
                return Err(invalid());
            }
            parts.push(std::mem::take(&mut part));
        } else {
            part.push(c);
        }
        previous = Some(c);
    }
    if quoted {
        return Err(invalid());
    }
    parts.push(part);
    if parts.len() > 1 && parts[0] == "__auto_generated_subquery_name" {
        parts.remove(0);
    }
    Ok(parts
        .iter()
        .map(|p| format!("`{}`", p.replace('`', "``")))
        .collect::<Vec<_>>()
        .join("."))
}

#[cfg(test)]
#[path = "schema_validation_tests.rs"]
mod tests;
