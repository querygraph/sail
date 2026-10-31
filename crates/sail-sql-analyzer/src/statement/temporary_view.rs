use std::collections::HashMap;

use sail_common::error::{StructuredSparkError, quote_sql_identifier};
use sail_common::spec;
use sail_common::utils::string::spark_case_insensitive_name_eq;

use crate::error::{SqlError, SqlResult};

/// SparkSqlParser.normalizeTempViewIdentifier for query-defined temporary views.
pub(super) fn normalize_query_name(name: spec::ObjectName) -> SqlResult<spec::Identifier> {
    let parts: Vec<String> = name.into();
    let Some(last) = parts.last() else {
        return Err(SqlError::invalid("expected a temporary view name"));
    };
    let allowed = match parts.as_slice() {
        [_] => true,
        [namespace, _] => spark_case_insensitive_name_eq(namespace, "session"),
        [catalog, namespace, _] => {
            spark_case_insensitive_name_eq(catalog, "system")
                && spark_case_insensitive_name_eq(namespace, "session")
        }
        _ => false,
    };
    if allowed {
        return Ok(last.clone().into());
    }
    let object_name = quote_in_parser(last)?;
    let qualifier = quote_in_parser(&parts[..parts.len() - 1].join("."))?;
    Err(Box::new(StructuredSparkError {
        class: "org.apache.spark.sql.catalyst.parser.ParseException".into(),
        condition: "INVALID_TEMP_OBJ_QUALIFIER".into(),
        sql_state: "42602".into(),
        message: format!(
            "Temporary VIEW {object_name} cannot be qualified with {qualifier}. Temporary objects can only be qualified with SESSION or SYSTEM.SESSION."
        ),
        parameters: HashMap::from([
            ("objectType".into(), "VIEW".into()),
            ("objectName".into(), object_name),
            ("qualifier".into(), qualifier),
        ]),
    }).into())
}

// Spark's parser converts an AnalysisException raised by diagnostic formatting
// into a ParseException, retaining its condition/state/parameters.
fn quote_in_parser(name: &str) -> SqlResult<String> {
    quote_sql_identifier(name).map_err(|mut error| {
        error.class = "org.apache.spark.sql.catalyst.parser.ParseException".into();
        SqlError::Structured(error)
    })
}

#[cfg(test)]
mod tests;
