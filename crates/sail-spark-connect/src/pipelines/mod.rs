mod analysis;
mod autocdc;
mod config;
mod execution;
mod graph;
mod materialize;
mod properties;
mod reanalysis;
mod schema;
mod schema_merge;
mod selection;
mod sql;
mod streaming;
mod topology;

use std::collections::HashMap;
use std::sync::Arc;
use std::time::SystemTime;

use datafusion::prelude::SessionContext;
pub(crate) use graph::PipelineGraph;
use sail_catalog::manager::CatalogManager;
use sail_common_datafusion::extension::SessionExtensionAccessor;
use sail_sql_analyzer::expression::from_ast_object_name;
use sail_sql_analyzer::parser::parse_object_name;

use crate::error::{ProtoFieldExt, SparkError, SparkResult, StructuredSparkError};
use crate::executor::{Executor, ExecutorBatch, ExecutorMetadata, ExecutorOutput};
use crate::service::ExecutePlanResponseStream;
use crate::session::SparkSession;
use crate::spark::connect::{self as sc, pipeline_command as pc, pipeline_command_result as pr};

fn result(value: sc::PipelineCommandResult) -> ExecutorOutput {
    ExecutorOutput::new(ExecutorBatch::PipelineCommandResult(Box::new(value)))
}

fn event(message: &str) -> ExecutorOutput {
    let timestamp = SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .unwrap_or_default();
    ExecutorOutput::new(ExecutorBatch::PipelineEventResult(Box::new(
        sc::PipelineEventResult {
            event: Some(sc::PipelineEvent {
                timestamp: Some(pbjson_types::Timestamp {
                    seconds: timestamp.as_secs() as i64,
                    nanos: timestamp.subsec_nanos() as i32,
                }),
                message: Some(message.to_string()),
            }),
        },
    )))
}

pub(crate) async fn handle_command(
    ctx: &SessionContext,
    command: sc::PipelineCommand,
    metadata: ExecutorMetadata,
) -> SparkResult<ExecutePlanResponseStream> {
    let spark = ctx.extension::<SparkSession>()?;
    let response = match command.command_type.required("pipeline command")? {
        pc::CommandType::CreateDataflowGraph(command) => {
            let catalog = ctx.extension::<CatalogManager>()?;
            let name = command.default_catalog.unwrap_or(
                catalog
                    .default_catalog()
                    .map_err(|e| SparkError::invalid(e.to_string()))?
                    .to_string(),
            );
            let database = command.default_database.unwrap_or(
                Vec::<String>::from(
                    catalog
                        .default_database()
                        .map_err(|e| SparkError::invalid(e.to_string()))?,
                )
                .join("."),
            );
            catalog
                .set_default_catalog(name.clone())
                .map_err(|e| SparkError::invalid(e.to_string()))?;
            catalog
                .set_default_database(from_ast_object_name(parse_object_name(&database)?)?.into())
                .await
                .map_err(|e| SparkError::invalid(e.to_string()))?;
            let id = spark.add_pipeline(PipelineGraph::new(name, database, command.sql_conf))?;
            sc::PipelineCommandResult {
                result_type: Some(pr::ResultType::CreateDataflowGraphResult(
                    pr::CreateDataflowGraphResult {
                        dataflow_graph_id: Some(id),
                    },
                )),
            }
        }
        pc::CommandType::DefineOutput(command) => {
            let id = command
                .dataflow_graph_id
                .as_deref()
                .required("dataflow graph id")?;
            let graph = spark.pipeline(id)?;
            let name = graph.lock()?.define_output(command)?;
            sc::PipelineCommandResult {
                result_type: Some(pr::ResultType::DefineOutputResult(pr::DefineOutputResult {
                    resolved_identifier: Some(graph::identifier(&name)),
                })),
            }
        }
        pc::CommandType::DefineFlow(command) => {
            // Spark rejects an explicitly supplied once option before graph lookup.
            if command.once.is_some() {
                let name = command.flow_name.as_deref().unwrap_or_default();
                return Err(Box::new(StructuredSparkError {
                    class: "org.apache.spark.sql.AnalysisException".into(),
                    condition: "DEFINE_FLOW_ONCE_OPTION_NOT_SUPPORTED".into(),
                    sql_state: "0A000".into(),
                    message: format!(
                        "Defining a one-time flow {name} with the 'once' option is not supported."
                    ),
                    parameters: HashMap::from([("flowName".into(), name.into())]),
                })
                .into());
            }
            let id = command
                .dataflow_graph_id
                .as_deref()
                .required("dataflow graph id")?;
            let graph = spark.pipeline(id)?;
            let name = graph.lock()?.define_flow(command)?;
            sc::PipelineCommandResult {
                result_type: Some(pr::ResultType::DefineFlowResult(pr::DefineFlowResult {
                    resolved_identifier: Some(graph::identifier(&name)),
                })),
            }
        }
        pc::CommandType::DropDataflowGraph(command) => {
            spark.drop_pipeline(
                command
                    .dataflow_graph_id
                    .as_deref()
                    .required("dataflow graph id")?,
            )?;
            sc::PipelineCommandResult::default()
        }
        pc::CommandType::StartRun(command) => {
            let graph_id = command
                .dataflow_graph_id
                .as_deref()
                .required("dataflow graph id")?;
            let graph = spark.pipeline(graph_id)?;
            let (definition, mut cancelled, selection) = {
                let mut graph = graph.lock()?;
                // Command validation precedes reserving the execution entry,
                // so invalid options can be corrected on the same graph.
                let selection = selection::Selection::new(&graph.definition, &command)?;
                if command.storage.as_deref().is_none_or(str::is_empty) {
                    return Err(SparkError::invalid(
                        "Storage must be specified to start a run.",
                    ));
                }
                let (definition, cancelled) = graph.start(graph_id)?;
                (definition, cancelled, selection)
            };
            let ctx = ctx.clone();
            let inner = async_stream::try_stream! {
                yield event("Starting pipeline analysis");
                let run = async {
                    let analyzed = analysis::analyze(&ctx, &definition).await?;
                    if !command.dry.unwrap_or(false) {
                        execution::execute(&ctx, &definition, analyzed, selection).await?;
                    }
                    Ok::<_, SparkError>(())
                };
                let outcome = tokio::select! {
                    value = run => value,
                    _ = cancelled.changed() => Err(SparkError::invalid("pipeline run cancelled")),
                };
                outcome?;
                yield event("Pipeline run completed");
                yield result(sc::PipelineCommandResult::default());
            };
            let operation_id = metadata.operation_id.clone();
            let executor = Executor::with_outputs(
                metadata,
                Box::pin(inner),
                spark.options().execution_heartbeat_interval,
            );
            let rx = executor.start()?;
            spark.add_executor(executor)?;
            return Ok(ExecutePlanResponseStream::new(
                spark.session_id().to_string(),
                operation_id,
                rx,
            ));
        }
        pc::CommandType::DefineSqlGraphElements(command) => {
            let id = command
                .dataflow_graph_id
                .as_deref()
                .required("dataflow graph id")?
                .to_string();
            let mut config = None;
            let preparation = sql::prepare(command, |expression| {
                // Capture session planning configuration lazily, only when a catalog
                // expression requires it. File/graph overrides belong to flows.
                let config = match &config {
                    Some(config) => Ok(Arc::clone(config)),
                    None => spark
                        .plan_config()
                        .inspect(|value| config = Some(value.clone())),
                };
                async move {
                    let resolver = sail_plan::resolver::PlanResolver::new(ctx, config?);
                    resolver
                        .resolve_catalog_name(expression)
                        .await
                        .map_err(SparkError::from)
                }
            });
            sql::register(&spark, &id, preparation).await?;
            sc::PipelineCommandResult::default()
        }
        _ => return Err(SparkError::unsupported("pipeline command")),
    };
    let operation_id = metadata.operation_id.clone();
    let mut outputs = vec![result(response)];
    if metadata.reattachable {
        outputs.push(ExecutorOutput::complete());
    }
    let executor = Executor::completed(metadata, outputs);
    let rx = executor.start()?;
    spark.add_executor(executor)?;
    Ok(ExecutePlanResponseStream::new(
        spark.session_id().to_string(),
        operation_id,
        rx,
    ))
}

#[cfg(test)]
mod catalog_preparation_tests;
