//! Resolve inputs afresh for one batch command, stopping at committed tables.
use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

use datafusion::logical_expr::LogicalPlan;
use datafusion::prelude::SessionContext;
use sail_common::spec;
use sail_common_datafusion::extension::SessionExtensionAccessor;
use sail_common_datafusion::rename::logical_plan::rename_logical_plan;
use sail_plan::config::PlanConfig;
use sail_plan::resolver::PlanResolver;

use super::analysis::{AnalyzedGraph, GraphScope};
use super::graph::{GraphDefinition, Name, analysis, quoted};
use crate::error::{SparkError, SparkResult};
use crate::session::SparkSession;
use crate::spark::connect::OutputType;

/// Local views are definitions; tables are materialization boundaries. External
/// relations stay on the ordinary resolver path and are not graph dependencies.
fn inputs(
    graph: &GraphDefinition,
    analyzed: &AnalyzedGraph,
    target: &Name,
) -> SparkResult<BTreeSet<Name>> {
    let mut pending = analyzed
        .dependencies
        .get(target)
        .into_iter()
        .flatten()
        .cloned()
        .collect::<Vec<_>>();
    let mut required = BTreeSet::new();
    while let Some(name) = pending.pop() {
        if !required.insert(name.clone()) {
            continue;
        }
        let output = graph
            .outputs
            .iter()
            .find(|output| output.name == name)
            .ok_or_else(|| SparkError::internal("analyzed dependency output missing"))?;
        if output.kind == OutputType::TemporaryView || output.view.is_some() {
            pending.extend(
                analyzed
                    .dependencies
                    .get(&name)
                    .into_iter()
                    .flatten()
                    .cloned(),
            );
        }
    }
    Ok(required)
}

/// The returned provider plans belong to this command only. This does not admit
/// an owner, pin a backend version, or implement upstream configuration fusion.
/// View bodies keep their existing flow config; committed reads use the caller's
/// command config. Later ownership activation must pass its captured Session too.
pub(super) async fn resolve_inputs(
    ctx: &SessionContext,
    graph: &GraphDefinition,
    analyzed: &AnalyzedGraph,
    target: &Name,
    command_config: Arc<PlanConfig>,
) -> SparkResult<BTreeMap<Name, LogicalPlan>> {
    let required = inputs(graph, analyzed, target)?;
    let spark = ctx.extension::<SparkSession>()?;
    let mut resolved = BTreeMap::new();
    for name in analyzed
        .order
        .iter()
        .filter(|name| required.contains(*name))
    {
        let output = graph
            .outputs
            .iter()
            .find(|output| &output.name == name)
            .ok_or_else(|| SparkError::internal("analyzed input output missing"))?;
        let (query, config, flow) =
            if output.kind == OutputType::TemporaryView || output.view.is_some() {
                let flow = graph
                    .flows
                    .iter()
                    .find(|flow| &flow.target == name)
                    .ok_or_else(|| analysis("PIPELINE_DATASET_WITHOUT_FLOW", quoted(name)))?;
                (
                    flow.query.clone(),
                    spark.plan_config_with_overrides(flow.sql_conf.clone())?,
                    Some(flow),
                )
            } else {
                let query = spec::QueryPlan::new(spec::QueryNode::Read {
                    read_type: spec::ReadType::NamedTable(Box::new(spec::ReadNamedTable {
                        name: name.clone().into(),
                        temporal: None,
                        sample: None,
                        options: vec![],
                    })),
                    is_streaming: false,
                });
                (query, command_config.clone(), None)
            };
        let resolver = PlanResolver::new(ctx, config);
        let scope = flow.map(|flow| GraphScope::new(graph, &resolved, flow));
        let resolver = match &scope {
            Some(scope) => resolver.with_relation_scope(scope),
            None => resolver,
        };
        let named = resolver
            .resolve_named_plan(spec::Plan::Query(query))
            .await?;
        let plan = match named.fields {
            Some(fields) => rename_logical_plan(named.plan, &fields)?,
            None => named.plan,
        };
        resolved.insert(name.clone(), plan);
    }
    Ok(resolved)
}

#[cfg(test)]
mod tests;
