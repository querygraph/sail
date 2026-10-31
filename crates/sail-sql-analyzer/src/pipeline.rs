//! Pipeline-specific lowering reuses the SQL grammar without executing DDL.

mod dataset;

pub(crate) use dataset::validate as validate_dataset;
use either::Either;
use sail_common::spec;
use sail_sql_parser::ast::keywords::Table;
use sail_sql_parser::ast::statement::Statement;

use crate::error::{SqlError, SqlResult};
use crate::expression::from_ast_object_name;
use crate::query::from_ast_query;
use crate::source::SqlSource;
use crate::statement::{from_ast_statement, from_ast_statement_with_source};

pub enum PipelineStatement {
    Dataset {
        streaming: bool,
        table: spec::ObjectName,
        definition: spec::TableDefinition,
        query: Option<Box<spec::QueryPlan>>,
    },
    Flow {
        name: spec::ObjectName,
        target: spec::ObjectName,
        query: spec::QueryPlan,
    },
    Command(spec::CommandNode),
}

pub fn from_ast_pipeline_statement(statement: Statement) -> SqlResult<PipelineStatement> {
    from_ast_pipeline_statement_impl(statement, None)
}

/// Lowers a pipeline declaration parsed from the same original SQL source.
pub fn from_ast_pipeline_statement_with_source(
    statement: Statement,
    source: &SqlSource<'_>,
) -> SqlResult<PipelineStatement> {
    from_ast_pipeline_statement_impl(statement, Some(source))
}

fn from_ast_pipeline_statement_impl(
    statement: Statement,
    source: Option<&SqlSource<'_>>,
) -> SqlResult<PipelineStatement> {
    match statement {
        Statement::CreatePipelineDataset(dataset) => {
            validate_dataset(&dataset)?;
            let streaming = matches!(dataset.kind, Either::Right(_));
            let table = match dataset.kind {
                Either::Left((materialized, _)) => Table::new(materialized.span),
                Either::Right((_, table)) => table,
            };
            let command = from_ast_statement(Statement::CreateTable {
                create: dataset.create,
                or_replace: None,
                temporary: None,
                external: None,
                table,
                if_not_exists: dataset.if_not_exists,
                name: dataset.name,
                columns: dataset.columns,
                like: None,
                using: dataset.using,
                clauses: dataset.clauses,
                r#as: dataset.r#as,
            })?;
            match command {
                spec::Plan::Command(spec::CommandPlan {
                    node:
                        spec::CommandNode::CreateTableAsSelect {
                            table,
                            definition,
                            query,
                        },
                    ..
                }) => Ok(PipelineStatement::Dataset {
                    streaming,
                    table,
                    definition,
                    query: Some(query),
                }),
                spec::Plan::Command(spec::CommandPlan {
                    node: spec::CommandNode::CreateTable { table, definition },
                    ..
                }) if streaming => Ok(PipelineStatement::Dataset {
                    streaming,
                    table,
                    definition,
                    query: None,
                }),
                _ => Err(SqlError::invalid("a materialized view requires a query")),
            }
        }
        Statement::CreatePipelineFlow {
            name, statement, ..
        } => {
            let Statement::InsertInto {
                with: None,
                into_or_overwrite: Either::Left(_),
                name: target,
                partition: None,
                if_not_exists: None,
                columns: Some(Either::Left(_)),
                query,
                ..
            } = *statement
            else {
                return Err(SqlError::analysis(
                    "[PIPELINE_SQL_GRAPH_ELEMENT_REGISTRATION_ERROR] Only INSERT INTO by name flows supported; partition specifications and target column lists are not allowed.",
                ));
            };
            Ok(PipelineStatement::Flow {
                name: from_ast_object_name(name)?,
                target: from_ast_object_name(target)?,
                query: from_ast_query(query)?,
            })
        }
        statement => match match source {
            Some(source) => from_ast_statement_with_source(statement, source),
            None => from_ast_statement(statement),
        }? {
            spec::Plan::Command(plan) => Ok(PipelineStatement::Command(plan.node)),
            _ => Err(SqlError::analysis(
                "[PIPELINE_SQL_GRAPH_ELEMENT_REGISTRATION_ERROR] Unsupported query statement in pipeline definitions",
            )),
        },
    }
}

/// Spark rejects direct execution of a pipeline dataset while registration uses
/// the separate lowering path above. Keep the error's client-visible metadata.
pub(crate) fn direct_dataset_execution_error(streaming: bool) -> SqlError {
    let dataset_type = if streaming {
        "STREAMING TABLE"
    } else {
        "MATERIALIZED VIEW"
    };
    Box::new(sail_common::error::StructuredSparkError {
        class: "org.apache.spark.SparkUnsupportedOperationException".into(),
        condition: "UNSUPPORTED_FEATURE.CREATE_PIPELINE_DATASET_QUERY_EXECUTION".into(),
        sql_state: "0A000".into(),
        message: format!(
            "The feature is not supported: Direct execution of a CREATE ... {dataset_type} query. \
             To create a pipeline dataset, create and execute a pipeline with a SQL file containing \
             your query attached in the pipeline definition."
        ),
        parameters: std::collections::HashMap::from([("pipelineDatasetType".into(), dataset_type.into())]),
    }).into()
}

#[cfg(test)]
mod tests;

pub(crate) fn direct_flow_execution_error() -> SqlError {
    Box::new(sail_common::error::StructuredSparkError {
        class: "org.apache.spark.SparkUnsupportedOperationException".into(),
        condition: "UNSUPPORTED_FEATURE.CREATE_FLOW_QUERY_EXECUTION".into(),
        sql_state: "0A000".into(),
        message: "The feature is not supported: Direct execution of a CREATE FLOW statement is not supported. \
                  To create a flow, create and execute a pipeline with a SQL file containing your statement \
                  attached in the pipeline spec.".into(),
        parameters: std::collections::HashMap::new(),
    }).into()
}
