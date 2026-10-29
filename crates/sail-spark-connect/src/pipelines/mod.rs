mod analysis;
mod execution;
mod graph;
mod materialize;
mod properties;
mod schema;
mod schema_merge;
mod selection;
mod sql;
mod topology;

use std::time::SystemTime;

use datafusion::prelude::SessionContext;
pub(crate) use graph::PipelineGraph;
use sail_catalog::manager::CatalogManager;
use sail_common_datafusion::extension::SessionExtensionAccessor;

use crate::error::{ProtoFieldExt, SparkError, SparkResult};
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
                .set_default_database(vec![database.clone()])
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
                    let selection = selection.with_dependencies(&definition, &analyzed);
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
            let graph = spark.pipeline(
                command
                    .dataflow_graph_id
                    .as_deref()
                    .required("dataflow graph id")?,
            )?;
            sql::register(&mut *graph.lock()?, command)?;
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
