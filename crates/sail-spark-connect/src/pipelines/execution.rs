use std::collections::BTreeMap;
use std::sync::Mutex;

use datafusion::prelude::SessionContext;
use futures::TryStreamExt;
use sail_common::spec;
use sail_common_datafusion::extension::SessionExtensionAccessor;
use sail_common_datafusion::rename::logical_plan::rename_logical_plan;
use sail_common_datafusion::session::job::JobService;
use sail_plan::resolve_and_execute_plan_with_scope;
use sail_plan::resolver::PlanResolver;

use super::analysis::{AnalyzedGraph, GraphScope};
use super::graph::{Flow, GraphDefinition, Output, analysis};
use super::selection::Selection;
use crate::error::{SparkError, SparkResult};
use crate::session::SparkSession;
use crate::spark::connect::{OutputType, pipeline_command as pc};

pub(super) async fn validate_publication(
    ctx: &SessionContext,
    graph: &GraphDefinition,
    analyzed: &AnalyzedGraph,
) -> SparkResult<()> {
    let spark = ctx.extension::<SparkSession>()?;
    for output in &graph.outputs {
        if output.kind != OutputType::MaterializedView {
            continue;
        }
        let flow = graph
            .flows
            .iter()
            .find(|x| x.target == output.name)
            .ok_or_else(|| analysis("PIPELINE_DATASET_WITHOUT_FLOW", output.name.join(".")))?;
        let scope = GraphScope {
            graph,
            resolved: &analyzed.relations,
            reads: Mutex::default(),
            flow,
        };
        let config = spark.plan_config_with_overrides(flow.sql_conf.clone())?;
        // Resolve catalog preconditions and writer options against analyzed
        // placeholders. Executing these plans here would publish output during
        // dry runs or before the remaining writers have been validated.
        PlanResolver::new(ctx, config)
            .with_relation_scope(&scope)
            .resolve_named_plan(spec::Plan::Command(spec::CommandPlan::new(
                publication_command(output, flow),
            )))
            .await?;
    }
    Ok(())
}

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
    let service = ctx.extension::<JobService>()?;
    let mut resolved = BTreeMap::new();
    for output in &graph.outputs {
        if output.schema.is_some() || output.definition.comment.is_some() {
            return Err(SparkError::unsupported(
                "declared pipeline schema or comment publication",
            ));
        }
        if output.kind != OutputType::TemporaryView
            && !selection.runs(&output.name)
            && selection.required.contains(&output.name)
        {
            resolved.insert(
                output.name.clone(),
                read_output(ctx, output.name.clone()).await?,
            );
        }
    }
    for name in analyzed.order {
        let output = graph
            .outputs
            .iter()
            .find(|x| x.name == name)
            .ok_or_else(|| SparkError::internal("analyzed output missing"))?;
        if !selection.required.contains(&name)
            || (output.kind != OutputType::TemporaryView && !selection.runs(&name))
        {
            continue;
        }
        let flow = graph
            .flows
            .iter()
            .find(|x| x.target == name)
            .ok_or_else(|| analysis("PIPELINE_DATASET_WITHOUT_FLOW", name.join(".")))?;
        let config = spark.plan_config_with_overrides(flow.sql_conf.clone())?;
        let scope = GraphScope {
            graph,
            resolved: &resolved,
            reads: Mutex::default(),
            flow,
        };
        let resolver = PlanResolver::new(ctx, config.clone()).with_relation_scope(&scope);
        if output.kind == OutputType::TemporaryView {
            let plan = resolver
                .resolve_named_plan(spec::Plan::Query(flow.query.clone()))
                .await?;
            let plan = match plan.fields {
                Some(fields) => rename_logical_plan(plan.plan, &fields)?,
                None => plan.plan,
            };
            resolved.insert(name, plan);
            continue;
        }
        let (physical, _) = resolve_and_execute_plan_with_scope(
            ctx,
            config,
            spec::Plan::Command(spec::CommandPlan::new(publication_command(output, flow))),
            Some(&scope),
        )
        .await?;
        let mut stream = service.runner().execute(ctx, physical).await?;
        while stream.try_next().await?.is_some() {}
        // Downstream flows scan the newly committed table. They must not inline
        // and recompute the producer's definition, especially for nondeterminism.
        resolved.insert(name.clone(), read_output(ctx, name).await?);
    }
    Ok(())
}

fn publication_command(output: &Output, flow: &Flow) -> spec::CommandNode {
    if let Some(mut definition) = output.view.clone() {
        definition.replace = true;
        definition.if_not_exists = false;
        return spec::CommandNode::CreateView {
            view: output.name.clone().into(),
            definition,
        };
    }
    let details = match &output.definition.details {
        Some(pc::define_output::Details::TableDetails(details)) => details.clone(),
        _ => pc::define_output::TableDetails::default(),
    };
    spec::CommandNode::WriteTo(spec::WriteTo {
        input: Box::new(flow.query.clone()),
        provider: details.format,
        table: output.name.clone().into(),
        mode: spec::WriteToMode::CreateOrReplace,
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
        table_properties: details.table_properties.into_iter().collect(),
    })
}

async fn read_output(
    ctx: &SessionContext,
    name: super::graph::Name,
) -> SparkResult<datafusion::logical_expr::LogicalPlan> {
    let read = spec::QueryPlan::new(spec::QueryNode::Read {
        read_type: spec::ReadType::NamedTable(Box::new(spec::ReadNamedTable {
            name: name.clone().into(),
            temporal: None,
            sample: None,
            options: vec![],
        })),
        is_streaming: false,
    });
    let plan = PlanResolver::new(ctx, ctx.extension::<SparkSession>()?.plan_config()?)
        .resolve_named_plan(spec::Plan::Query(read))
        .await?;
    let plan = match plan.fields {
        Some(fields) => rename_logical_plan(plan.plan, &fields)?,
        None => plan.plan,
    };
    Ok(plan)
}
