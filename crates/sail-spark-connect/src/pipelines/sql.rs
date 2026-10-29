use std::collections::HashMap;
use std::sync::Arc;

use sail_common::spec;
use sail_sql_analyzer::pipeline::{PipelineStatement, from_ast_pipeline_statement_with_source};
use sail_sql_analyzer::source::{SqlSource, StatementOrigin};

use super::graph::{Flow, PipelineGraph, analysis, quoted};
use crate::error::{ProtoFieldExt, SparkError, SparkResult, StructuredSparkError};
use crate::spark::connect::{OutputType, pipeline_command as pc};

pub(super) fn register(
    graph: &mut PipelineGraph,
    command: pc::DefineSqlGraphElements,
) -> SparkResult<()> {
    graph.ensure_idle()?;
    let text = command.sql_text.required("pipeline SQL text")?;
    let mut draft = PipelineGraph::new(
        graph.definition.catalog.clone(),
        graph.definition.database.clone(),
        graph.definition.sql_conf.clone(),
    );
    draft.definition = graph.definition.clone();
    let source = SqlSource::new(&text)?;
    for (statement, span) in source.parse_statements_with_spans()? {
        let result = from_ast_pipeline_statement_with_source(statement, &source)
            .map_err(SparkError::from)
            .and_then(|statement| register_statement(&mut draft, statement));
        if let Err(error) = result {
            return Err(Box::new(registration_error(
                error.to_string(),
                source.statement_origin(span)?,
                command.sql_file_path.as_deref(),
            ))
            .into());
        }
    }
    // USE and SET apply to this SQL file. Each flow keeps its own captured
    // namespace/configuration; subsequent files start from pipeline defaults.
    draft
        .definition
        .catalog
        .clone_from(&graph.definition.catalog);
    draft
        .definition
        .database
        .clone_from(&graph.definition.database);
    draft
        .definition
        .sql_conf
        .clone_from(&graph.definition.sql_conf);
    graph.definition = draft.definition;
    Ok(())
}

fn registration_error(
    message: String,
    origin: StatementOrigin<'_>,
    file: Option<&str>,
) -> StructuredSparkError {
    let offending_query = format!("\nOffending query:\n{}\n", origin.text);
    let code_location = file
        .map(|file| format!("Query defined at {file}:{}", origin.line))
        .unwrap_or_default();
    StructuredSparkError {
        class: "org.apache.spark.sql.AnalysisException",
        condition: "PIPELINE_SQL_GRAPH_ELEMENT_REGISTRATION_ERROR",
        sql_state: "42000",
        message: format!("{message}\n{offending_query}\n{code_location}"),
        parameters: HashMap::from([
            ("message".into(), message),
            ("offendingQuery".into(), offending_query),
            ("codeLocation".into(), code_location),
        ]),
    }
}

fn register_statement(graph: &mut PipelineGraph, statement: PipelineStatement) -> SparkResult<()> {
    match statement {
        PipelineStatement::Dataset {
            streaming,
            table,
            definition,
            query,
        } => {
            if definition.location.is_some()
                || spec::has_path_or_location(None, &definition.options)
            {
                return Err(SparkError::invalid(
                    "Setting dataset location is disallowed",
                ));
            }
            let mut partition_cols = vec![];
            for partition in definition.partition_by {
                match partition {
                    spec::PartitionColumn::Expression(spec::Expr::UnresolvedAttribute {
                        name,
                        ..
                    }) if name.parts().len() == 1 => {
                        partition_cols.push(name.parts()[0].as_ref().to_string())
                    }
                    _ => {
                        return Err(SparkError::invalid(
                            "Only identity partition columns are supported in pipelines",
                        ));
                    }
                }
            }
            let format = match definition.file_format {
                None => None,
                Some(spec::TableFileFormat::General { format }) => Some(format),
                _ => return Err(SparkError::unsupported("pipeline storage format")),
            };
            let schema = if definition.columns.is_empty() {
                None
            } else {
                Some(spec::Schema {
                    fields: definition
                        .columns
                        .into_iter()
                        .map(|column| {
                            Arc::new(spec::Field {
                                name: column.name,
                                data_type: column.data_type,
                                nullable: column.nullable,
                                metadata: column
                                    .comment
                                    .map(|comment| {
                                        vec![(
                                            spec::SPARK_METADATA_JSON_KEY.to_string(),
                                            serde_json::json!({"comment": comment}).to_string(),
                                        )]
                                    })
                                    .unwrap_or_default(),
                            })
                        })
                        .collect::<Vec<_>>()
                        .into(),
                })
            };
            let table: Vec<String> = table.into();
            let name = graph.define_output(pc::DefineOutput {
                output_name: Some(quoted(&table)),
                output_type: Some(if streaming {
                    OutputType::Table
                } else {
                    OutputType::MaterializedView
                } as i32),
                comment: definition.comment,
                details: Some(pc::define_output::Details::TableDetails(
                    pc::define_output::TableDetails {
                        format,
                        partition_cols,
                        table_properties: definition.properties.into_iter().collect(),
                        clustering_columns: definition
                            .cluster_by
                            .into_iter()
                            .map(|name| quoted(&Vec::<String>::from(name)))
                            .collect(),
                        schema: None,
                    },
                )),
                ..Default::default()
            })?;
            if let Some(output) = graph.definition.outputs.last_mut() {
                output.schema = schema;
            }
            if let Some(query) = query {
                add_flow(graph, name.clone(), name, *query);
            }
        }
        PipelineStatement::Flow {
            name,
            target,
            query,
        } => {
            let raw_name: Vec<String> = name.into();
            if raw_name.len() != 1 {
                return Err(analysis(
                    "MULTIPART_FLOW_NAME_NOT_SUPPORTED",
                    quoted(&raw_name),
                ));
            }
            let name = graph.definition.qualify(&raw_name)?;
            let target = graph
                .definition
                .resolve_name(&Vec::<String>::from(target))?;
            add_flow(graph, name, target, query);
        }
        PipelineStatement::Command(spec::CommandNode::SetVariable { variable, value }) => {
            graph.definition.sql_conf.insert(variable, value);
        }
        PipelineStatement::Command(spec::CommandNode::SetCurrentDatabase { database }) => {
            let parts: Vec<String> = database.into();
            match parts.as_slice() {
                [database] => graph.definition.database.clone_from(database),
                [catalog, database] => {
                    graph.definition.catalog.clone_from(catalog);
                    graph.definition.database.clone_from(database);
                }
                _ => {
                    return Err(SparkError::invalid(
                        "pipeline database must have one or two components",
                    ));
                }
            }
        }
        PipelineStatement::Command(spec::CommandNode::SetCurrentCatalog { catalog }) => {
            graph.definition.catalog = catalog.as_ref().to_string();
            graph.definition.database.clear();
        }
        PipelineStatement::Command(spec::CommandNode::CreateTemporaryView {
            view,
            is_global: false,
            definition,
        }) => {
            let name = graph.define_output(pc::DefineOutput {
                output_name: Some(quoted(&[view.as_ref().to_string()])),
                output_type: Some(OutputType::TemporaryView as i32),
                comment: definition.comment,
                ..Default::default()
            })?;
            let query = with_view_columns(*definition.input, definition.columns);
            add_flow(graph, name.clone(), name, query);
        }
        PipelineStatement::Command(spec::CommandNode::CreateView {
            view,
            mut definition,
        }) => {
            let name = graph.define_output(pc::DefineOutput {
                output_name: Some(quoted(&Vec::<String>::from(view))),
                output_type: Some(OutputType::MaterializedView as i32),
                ..Default::default()
            })?;
            definition.input = Box::new(with_view_columns(
                *definition.input,
                definition.columns.clone(),
            ));
            add_flow(graph, name.clone(), name, *definition.input.clone());
            if let Some(output) = graph.definition.outputs.last_mut() {
                output.view = Some(definition);
            }
        }
        _ => {
            return Err(SparkError::unsupported(
                "SQL statement in pipeline definitions",
            ));
        }
    }
    Ok(())
}

fn add_flow(
    graph: &mut PipelineGraph,
    name: Vec<String>,
    target: Vec<String>,
    query: spec::QueryPlan,
) {
    graph.definition.flows.push(Flow {
        name,
        target,
        query,
        sql_conf: graph.definition.sql_conf.clone(),
        catalog: graph.definition.catalog.clone(),
        database: graph.definition.database.clone(),
    });
}

fn with_view_columns(
    query: spec::QueryPlan,
    columns: Option<Vec<spec::ViewColumnDefinition>>,
) -> spec::QueryPlan {
    match columns {
        Some(columns) => spec::QueryPlan::new(spec::QueryNode::ToDf {
            input: Box::new(query),
            column_names: columns
                .into_iter()
                .map(|column| column.name.into())
                .collect(),
        }),
        None => query,
    }
}

#[cfg(test)]
mod tests;
