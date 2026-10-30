use std::sync::Arc;

use datafusion::dataframe::DataFrame;
use datafusion::physical_plan::{ExecutionPlan, displayable};
use datafusion::prelude::SessionContext;
use datafusion_common::Result;
use datafusion_common::display::{PlanType, StringifiedPlan, ToStringifiedPlan};
use datafusion_expr::{Extension, LogicalPlan};
use sail_catalog::command::CatalogCommand;
use sail_common::spec;
use sail_common_datafusion::rename::physical_plan::rename_physical_plan;

use crate::catalog::CatalogCommandNode;
use crate::config::PlanConfig;
use crate::error::PlanResult;
use crate::resolver::PlanResolver;
use crate::resolver::plan::NamedPlan;
use crate::resolver::scope::RelationScope;
use crate::streaming::rewriter::{is_streaming_plan, rewrite_streaming_plan};

pub mod catalog;
pub mod config;
pub mod error;
pub mod explain;
pub mod formatter;
pub mod function;
pub mod procedure;
pub mod resolver;
mod streaming;

/// Executes a logical plan.
/// Catalog commands and barrier nodes are handled by the physical planner.
pub async fn execute_logical_plan(ctx: &SessionContext, plan: LogicalPlan) -> Result<DataFrame> {
    let df = ctx.execute_logical_plan(plan).await?;
    Ok(df)
}

pub async fn resolve_and_execute_plan(
    ctx: &SessionContext,
    config: Arc<PlanConfig>,
    plan: spec::Plan,
) -> PlanResult<(Arc<dyn ExecutionPlan>, Vec<StringifiedPlan>)> {
    resolve_and_execute_plan_with_scope(ctx, config, plan, None).await
}

/// Resolve with an optional caller-local namespace, then use the same optimizer,
/// streaming rewrite, physical planner, and field-renaming path as other queries.
pub async fn resolve_and_execute_plan_with_scope(
    ctx: &SessionContext,
    config: Arc<PlanConfig>,
    plan: spec::Plan,
    scope: Option<&dyn RelationScope>,
) -> PlanResult<(Arc<dyn ExecutionPlan>, Vec<StringifiedPlan>)> {
    let resolver = PlanResolver::new(ctx, config);
    let resolver = match scope {
        Some(scope) => resolver.with_relation_scope(scope),
        None => resolver,
    };
    let named = resolver.resolve_named_plan(plan).await?;
    prepare_named_plan(ctx, named).await
}

/// Prepare catalog metadata through Sail's ordinary planning stages without
/// resolving SQL or its source query again. This does not admit a job or execute
/// the returned stream; callers retain those responsibilities.
pub async fn prepare_catalog_command(
    ctx: &SessionContext,
    command: CatalogCommand,
) -> PlanResult<(Arc<dyn ExecutionPlan>, Vec<StringifiedPlan>)> {
    let plan = LogicalPlan::Extension(Extension {
        node: Arc::new(CatalogCommandNode::try_new(ctx, command)?),
    });
    prepare_named_plan(ctx, NamedPlan { plan, fields: None }).await
}

async fn prepare_named_plan(
    ctx: &SessionContext,
    NamedPlan { plan, fields }: NamedPlan,
) -> PlanResult<(Arc<dyn ExecutionPlan>, Vec<StringifiedPlan>)> {
    let mut info = vec![];
    info.push(plan.to_stringified(PlanType::InitialLogicalPlan));
    let df = execute_logical_plan(ctx, plan).await?;
    let (session_state, plan) = df.into_parts();
    let plan = session_state.optimize(&plan)?;
    let plan = if is_streaming_plan(&plan)? {
        rewrite_streaming_plan(plan)?
    } else {
        plan
    };
    info.push(plan.to_stringified(PlanType::FinalLogicalPlan));
    let plan = session_state
        .query_planner()
        .create_physical_plan(&plan, &session_state)
        .await?;
    let plan = if let Some(fields) = fields {
        rename_physical_plan(plan, &fields)?
    } else {
        plan
    };
    info.push(StringifiedPlan::new(
        PlanType::FinalPhysicalPlan,
        displayable(plan.as_ref()).indent(true).to_string(),
    ));
    Ok((plan, info))
}
