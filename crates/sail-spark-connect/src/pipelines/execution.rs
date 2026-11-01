use std::collections::{BTreeSet, HashMap};
use std::sync::Arc;

use datafusion::physical_plan::ExecutionPlan;
use datafusion::prelude::SessionContext;
use futures::TryStreamExt;
use sail_catalog::command::CatalogCommand;
use sail_common::spec;
use sail_common_datafusion::extension::SessionExtensionAccessor;
use sail_common_datafusion::session::job::JobService;
use sail_plan::config::PlanConfig;
use sail_plan::resolve_and_execute_plan_with_scope;
use sail_plan::resolver::scope::RelationScope;
use tokio::sync::mpsc;

use super::analysis::{AnalyzedGraph, GraphScope};
use super::graph::{Flow, GraphDefinition, Name, Output, analysis};
use super::selection::Selection;
use crate::error::{SparkError, SparkResult};
use crate::session::SparkSession;
use crate::spark::connect::{OutputType, pipeline_command as pc};

/// Progress messages for the client, in `FlowProgressEventLogger`'s words.
pub(super) type RunEvents = mpsc::UnboundedSender<String>;

const MAX_FLOW_RETRY_ATTEMPTS: &str = "spark.sql.pipelines.maxFlowRetryAttempts";
const DEFAULT_MAX_FLOW_RETRY_ATTEMPTS: u32 = 2;

pub(super) async fn execute(
    ctx: &SessionContext,
    graph: &GraphDefinition,
    analyzed: AnalyzedGraph,
    selection: Selection,
    storage: String,
    events: &RunEvents,
) -> SparkResult<()> {
    // Reject unsupported modes before the first write, rather than partially
    // publishing a graph that contains an output this run cannot produce.
    if graph.flows.iter().any(|flow| flow.auto_cdc.is_some()) {
        if super::autocdc::admitted(ctx, graph)? {
            super::autocdc::execute_graph(ctx, graph, &analyzed, &selection).await?;
            return Ok(());
        }
        return Err(SparkError::unsupported("incremental pipeline execution"));
    }
    super::materialize::tables(ctx, graph, &analyzed.schemas, &selection).await?;
    super::materialize::views(ctx, graph, &analyzed).await?;
    let runnable =
        |output: &Output| output.view.is_none() && output.kind != OutputType::TemporaryView;
    for name in &analyzed.order {
        let output = find_output(graph, name)?;
        if runnable(output) {
            for flow in flows_of(graph, name) {
                let flow = flow.name.join(".");
                emit(
                    events,
                    if selection.runs(name) {
                        format!("Flow {flow} is QUEUED.")
                    } else {
                        format!("Flow '{flow}' is EXCLUDED.")
                    },
                );
            }
        }
    }
    let run = FlowRun {
        ctx,
        graph,
        analyzed: &analyzed,
        selection: &selection,
        storage: &storage,
        events,
    };
    // A failed flow does not stop independent flows, only what reads from it.
    // The first failure is the run's error.
    let mut failed = BTreeSet::new();
    let mut first_failure = None;
    for name in &analyzed.order {
        let output = find_output(graph, name)?;
        if !runnable(output) || !selection.runs(name) {
            continue;
        }
        let flows = flows_of(graph, name).collect::<Vec<_>>();
        if flows.is_empty() {
            return Err(analysis("PIPELINE_DATASET_WITHOUT_FLOW", name.join(".")));
        }
        if reads_failed_output(&analyzed, name, &failed) {
            failed.insert(name.clone());
            for flow in flows {
                let flow = flow.name.join(".");
                emit(
                    events,
                    format!("Flow '{flow}' SKIPPED due to upstream failure(s)."),
                );
            }
            continue;
        }
        for flow in flows {
            if let Err(error) = run.flow(output, flow).await {
                failed.insert(name.clone());
                first_failure.get_or_insert(error);
            }
        }
    }
    first_failure.map_or(Ok(()), Err)
}

fn flows_of<'a>(graph: &'a GraphDefinition, name: &'a Name) -> impl Iterator<Item = &'a Flow> {
    graph.flows.iter().filter(move |flow| &flow.target == name)
}

fn find_output<'a>(graph: &'a GraphDefinition, name: &Name) -> SparkResult<&'a Output> {
    graph
        .outputs
        .iter()
        .find(|output| &output.name == name)
        .ok_or_else(|| SparkError::internal("analyzed output missing"))
}

fn emit(events: &RunEvents, message: String) {
    // A closed channel means the client went away; the run still finishes.
    let _ = events.send(message);
}

/// Whether `name` reads a failed output, directly or through views.
fn reads_failed_output(analyzed: &AnalyzedGraph, name: &Name, failed: &BTreeSet<Name>) -> bool {
    let mut pending = vec![name];
    let mut visited = BTreeSet::new();
    while let Some(next) = pending.pop() {
        for input in analyzed.dependencies.get(next).into_iter().flatten() {
            if failed.contains(input) {
                return true;
            }
            if visited.insert(input) {
                pending.push(input);
            }
        }
    }
    false
}

struct FlowRun<'a> {
    ctx: &'a SessionContext,
    graph: &'a GraphDefinition,
    analyzed: &'a AnalyzedGraph,
    selection: &'a Selection,
    storage: &'a str,
    events: &'a RunEvents,
}

impl FlowRun<'_> {
    /// One flow, with `TriggeredGraphExecution`'s retries.
    async fn flow(&self, output: &Output, flow: &Flow) -> SparkResult<()> {
        let accepted = self
            .analyzed
            .accepted_confs
            .get(&flow.name)
            .ok_or_else(|| SparkError::internal("accepted flow configuration missing"))?;
        let retries = accepted
            .get(MAX_FLOW_RETRY_ATTEMPTS)
            .map(|value| {
                value.parse::<u32>().map_err(|_| {
                    SparkError::invalid(format!("invalid {MAX_FLOW_RETRY_ATTEMPTS}: {value}"))
                })
            })
            .transpose()?
            .unwrap_or(DEFAULT_MAX_FLOW_RETRY_ATTEMPTS);
        let display = flow.name.join(".");
        let batch = output.kind == OutputType::MaterializedView;
        let mut attempt = 0;
        loop {
            if batch {
                emit(self.events, format!("Flow {display} is PLANNING."));
            }
            emit(self.events, format!("Flow {display} is STARTING."));
            emit(self.events, format!("Flow {display} is RUNNING."));
            match self
                .attempt(output, flow, accepted, batch && attempt > 0)
                .await
            {
                Ok(()) => {
                    emit(self.events, format!("Flow {display} has COMPLETED."));
                    return Ok(());
                }
                Err(error) => {
                    emit(
                        self.events,
                        format!("Flow '{display}' has FAILED.\nError: {error}"),
                    );
                    if attempt >= retries {
                        return Err(error);
                    }
                    attempt += 1;
                }
            }
        }
    }

    async fn attempt(
        &self,
        output: &Output,
        flow: &Flow,
        accepted: &HashMap<String, String>,
        truncate: bool,
    ) -> SparkResult<()> {
        let spark = self.ctx.extension::<SparkSession>()?;
        let config = spark.plan_config_with_overrides(accepted.clone())?;
        let name = &output.name;
        let resolved = super::reanalysis::resolve_inputs(
            self.ctx,
            self.graph,
            self.analyzed,
            name,
            config.clone(),
            accepted,
        )
        .await?;
        let write = match output.kind {
            OutputType::Sink => sink_write_command(output, flow)?,
            _ => table_write_command(output, flow),
        };
        if output.kind != OutputType::MaterializedView {
            let run = super::append::AppendFlow {
                graph: self.graph,
                flow,
                destination: name,
                inputs: &resolved,
                write,
                storage: self.storage,
                reset: super::materialize::resets(output, self.selection),
            };
            return super::append::run(self.ctx, config, run).await;
        }
        // A failed attempt may have left files behind.
        if truncate {
            let table: spec::ObjectName = name.clone().into();
            run_command(
                self.ctx,
                config.clone(),
                spec::CommandNode::TruncateTable { table },
                None,
            )
            .await?;
        }
        let scope = GraphScope::new(self.graph, &resolved, flow);
        run_command(self.ctx, config, write, Some(&scope)).await
    }
}

pub(super) async fn run_command(
    ctx: &SessionContext,
    config: Arc<PlanConfig>,
    command: spec::CommandNode,
    scope: Option<&dyn RelationScope>,
) -> SparkResult<()> {
    let (physical, _) = resolve_and_execute_plan_with_scope(
        ctx,
        config,
        spec::Plan::Command(spec::CommandPlan::new(command)),
        scope,
    )
    .await?;
    execute_command_plan(ctx, physical).await
}

pub(super) async fn run_catalog_command(
    ctx: &SessionContext,
    command: CatalogCommand,
) -> SparkResult<()> {
    let (physical, _) = sail_plan::prepare_catalog_command(ctx, command).await?;
    execute_command_plan(ctx, physical).await
}

async fn execute_command_plan(
    ctx: &SessionContext,
    physical: Arc<dyn ExecutionPlan>,
) -> SparkResult<()> {
    let service = ctx.extension::<JobService>()?;
    let mut stream = service.runner().execute(ctx, physical).await?;
    while stream.try_next().await?.is_some() {}
    Ok(())
}

/// A streaming write to a sink's path, in the sink's format.
fn sink_write_command(output: &Output, flow: &Flow) -> SparkResult<spec::CommandNode> {
    let Some(pc::define_output::Details::SinkDetails(details)) = &output.definition.details else {
        return Err(SparkError::internal("sink output without sink details"));
    };
    let format = details
        .format
        .clone()
        .ok_or_else(|| SparkError::invalid("a pipeline sink needs a format"))?;
    let path = details
        .options
        .get("path")
        .cloned()
        .ok_or_else(|| SparkError::unsupported(format!("a {format} sink without a path")))?;
    Ok(spec::CommandNode::Write(spec::Write {
        input: Box::new(flow.query.clone()),
        source: Some(format),
        save_type: spec::SaveType::Path(path),
        mode: Some(spec::SaveMode::Append),
        sort_columns: vec![],
        partitioning_columns: vec![],
        clustering_columns: vec![],
        bucket_by: None,
        options: details
            .options
            .iter()
            .filter(|(key, _)| key.as_str() != "path")
            .map(|(key, value)| (key.clone(), value.clone()))
            .collect(),
    }))
}

fn table_write_command(output: &Output, flow: &Flow) -> spec::CommandNode {
    let details = match &output.definition.details {
        Some(pc::define_output::Details::TableDetails(details)) => details.clone(),
        _ => pc::define_output::TableDetails::default(),
    };
    spec::CommandNode::WriteTo(spec::WriteTo {
        input: Box::new(flow.query.clone()),
        provider: details.format,
        table: output.name.clone().into(),
        mode: spec::WriteToMode::Append,
        partitioning_columns: details
            .partition_cols
            .into_iter()
            .map(|name| spec::Expr::UnresolvedAttribute {
                name: spec::ObjectName::bare(name),
                plan_id: None,
                is_metadata_column: false,
            })
            .collect(),
        clustering_columns: details
            .clustering_columns
            .into_iter()
            .map(Into::into)
            .collect(),
        options: vec![],
        table_properties: vec![],
    })
}
