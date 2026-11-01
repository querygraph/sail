//! Spark's pipeline dataset restrictions apply before either lowering path.

use either::Either;
use sail_common::error::StructuredSparkError;
use sail_sql_parser::ast::pipeline::CreatePipelineDataset;
use sail_sql_parser::ast::statement::{ColumnDefinitionOption, CreateTableClause};

use crate::error::SqlResult;

pub(crate) fn validate(dataset: &CreatePipelineDataset) -> SqlResult<()> {
    let kind = match dataset.kind {
        Either::Left(_) => "MATERIALIZED VIEW",
        Either::Right(_) => "STREAMING TABLE",
    };
    // Match SparkSqlParser's validation precedence, independently of clause order.
    let constrained = dataset.columns.as_ref().is_some_and(|list| {
        list.columns.clone().into_items().any(|column| {
            column.options.iter().any(|option| {
                matches!(
                    option,
                    ColumnDefinitionOption::Check(..)
                        | ColumnDefinitionOption::Unique(..)
                        | ColumnDefinitionOption::PrimaryKey(..)
                        | ColumnDefinitionOption::References(..)
                )
            })
        })
    });
    let message = if constrained {
        Some(
            "Pipeline datasets do not currently support column constraints. Please remove any \
             CHECK, UNIQUE, PK, and FK constraints specified on the pipeline dataset."
                .to_string(),
        )
    } else if dataset
        .clauses
        .iter()
        .any(|c| matches!(c, CreateTableClause::ClusteredBy(..)))
    {
        Some(format!(
            "Bucketing is not supported for CREATE {kind} statements. Please remove any bucket spec specified in the statement."
        ))
    } else if dataset
        .clauses
        .iter()
        .any(|c| matches!(c, CreateTableClause::Options(..)))
    {
        Some(format!(
            "Options are not supported for CREATE {kind} statements. Please remove any OPTIONS lists specified in the statement."
        ))
    } else if dataset
        .clauses
        .iter()
        .any(|c| matches!(c, CreateTableClause::StoredAs(..)))
    {
        Some(format!(
            "The STORED AS syntax is not supported for CREATE {kind} statements. Consider using the Data Source based USING clause instead."
        ))
    } else if dataset
        .clauses
        .iter()
        .any(|c| matches!(c, CreateTableClause::RowFormat(..)))
    {
        Some(format!(
            "Hive SerDe format options are not supported for CREATE {kind} statements."
        ))
    } else if dataset
        .clauses
        .iter()
        .any(|c| matches!(c, CreateTableClause::Location(..)))
    {
        Some(format!(
            "Specifying location is not supported for CREATE {kind} statements. The storage location for a pipeline dataset is managed by the pipeline itself."
        ))
    } else if matches!(dataset.kind, Either::Left(_)) && dataset.r#as.is_none() {
        Some("Unable to find query for CREATE MATERIALIZED VIEW statement.".into())
    } else {
        None
    };
    match message {
        Some(message) => Err(Box::new(StructuredSparkError {
            class: "org.apache.spark.sql.catalyst.parser.ParseException".into(),
            condition: "_LEGACY_ERROR_TEMP_0035".into(),
            sql_state: String::new(),
            message: format!("Operation not allowed: {message}."),
            parameters: std::collections::HashMap::from([("message".into(), message)]),
        })
        .into()),
        None => Ok(()),
    }
}
