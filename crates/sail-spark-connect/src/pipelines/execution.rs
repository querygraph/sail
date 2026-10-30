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

use super::analysis::{AnalyzedGraph, GraphScope};
use super::graph::{Flow, GraphDefinition, Output, analysis};
use super::selection::Selection;
use crate::error::{SparkError, SparkResult};
use crate::session::SparkSession;
use crate::spark::connect::{OutputType, pipeline_command as pc};

pub(super) async fn execute(
    ctx: &SessionContext,
    graph: &GraphDefinition,
    analyzed: AnalyzedGraph,
    selection: Selection,
) -> SparkResult<()> {
    // Reject unsupported modes before the first write, rather than partially
    // publishing a batch graph that contains an incremental or sink output.
    if graph
        .outputs
        .iter()
        .any(|x| matches!(x.kind, OutputType::Table | OutputType::Sink))
    {
        return Err(SparkError::unsupported("incremental pipeline execution"));
    }
    let spark = ctx.extension::<SparkSession>()?;
    super::materialize::tables(ctx, graph, &analyzed, &selection).await?;
    super::materialize::views(ctx, graph, &analyzed).await?;
    for name in &analyzed.order {
        let output = graph
            .outputs
            .iter()
            .find(|output| &output.name == name)
            .ok_or_else(|| SparkError::internal("analyzed output missing"))?;
        if output.view.is_some()
            || output.kind == OutputType::TemporaryView
            || !selection.runs(name)
        {
            continue;
        }
        let flow = graph
            .flows
            .iter()
            .find(|x| &x.target == name)
            .ok_or_else(|| analysis("PIPELINE_DATASET_WITHOUT_FLOW", name.join(".")))?;
        let config = spark.plan_config_with_overrides(flow.sql_conf.clone())?;
        let resolved =
            super::reanalysis::resolve_inputs(ctx, graph, &analyzed, name, config.clone()).await?;
        let scope = GraphScope::new(graph, &resolved, flow);
        run_command(ctx, config, table_write_command(output, flow), Some(&scope)).await?;
    }
    Ok(())
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
