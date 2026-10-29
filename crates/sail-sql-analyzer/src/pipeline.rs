//! Pipeline-specific lowering reuses the SQL grammar without executing DDL.

use either::Either;
use sail_common::spec;
use sail_sql_parser::ast::keywords::Table;
use sail_sql_parser::ast::statement::Statement;

use crate::error::{SqlError, SqlResult};
use crate::expression::from_ast_object_name;
use crate::query::from_ast_query;
use crate::statement::from_ast_statement;

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
    match statement {
        Statement::CreatePipelineDataset(dataset) => {
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
        statement => match from_ast_statement(statement)? {
            spec::Plan::Command(plan) => Ok(PipelineStatement::Command(plan.node)),
            _ => Err(SqlError::analysis(
                "[PIPELINE_SQL_GRAPH_ELEMENT_REGISTRATION_ERROR] Unsupported query statement in pipeline definitions",
            )),
        },
    }
}
