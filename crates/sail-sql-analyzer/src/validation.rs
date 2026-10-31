//! Semantic INSERT syntax shared by ordinary SQL and pipeline definitions.

use either::Either;
use sail_common::error::StructuredSparkError;
use sail_sql_parser::ast::statement::Statement;

use crate::error::SqlResult;

/// Spark's AstBuilder rejects this INTO clause before registration or analysis.
/// OVERWRITE retains its separate IF NOT EXISTS semantics.
pub(crate) fn validate_insert_modes(statement: &Statement) -> SqlResult<()> {
    match statement {
        Statement::InsertInto {
            into_or_overwrite: Either::Left(_),
            if_not_exists: Some(_),
            ..
        } => Err(Box::new(StructuredSparkError {
            class: "org.apache.spark.sql.catalyst.parser.ParseException".into(),
            condition: "INVALID_STATEMENT_OR_CLAUSE".into(),
            sql_state: "42601".into(),
            message: "The statement or clause: INSERT INTO ... IF NOT EXISTS is not valid.".into(),
            parameters: std::collections::HashMap::from([(
                "operation".into(),
                "INSERT INTO ... IF NOT EXISTS".into(),
            )]),
        })
        .into()),
        Statement::CreatePipelineFlow { statement, .. } | Statement::Explain { statement, .. } => {
            validate_insert_modes(statement)
        }
        _ => Ok(()),
    }
}

#[cfg(test)]
mod tests;
