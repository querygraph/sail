use std::sync::Arc;

use datafusion::dataframe::DataFrame;
use datafusion::physical_plan::{ExecutionPlan, displayable};
use datafusion::prelude::SessionContext;
use datafusion_common::Result;
use datafusion_common::display::{PlanType, StringifiedPlan, ToStringifiedPlan};
use datafusion_expr::LogicalPlan;
use sail_common::spec;
use sail_common::telemetry::c2::{Guard, Identity, Phase};
use sail_common_datafusion::rename::physical_plan::rename_physical_plan;

use crate::config::PlanConfig;
use crate::error::PlanResult;
use crate::resolver::PlanResolver;
use crate::resolver::plan::NamedPlan;
use crate::streaming::rewriter::{is_streaming_plan, rewrite_streaming_plan};

pub mod catalog;
pub mod config;
pub mod error;
pub mod explain;
pub mod formatter;
pub mod function;
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
    let mut info = vec![];
    let resolver = PlanResolver::new(ctx, config);
    let observation = Guard::start(Phase::Resolve, || Identity::CausalContext);
    let resolved = resolver.resolve_named_plan(plan).await;
    observation.finish_result(&resolved);
    let NamedPlan { plan, fields } = resolved?;
    info.push(plan.to_stringified(PlanType::InitialLogicalPlan));
    let observation = Guard::start(Phase::LogicalExecute, || Identity::CausalContext);
    let executed = execute_logical_plan(ctx, plan).await;
    observation.finish_result(&executed);
    let df = executed?;
    let (session_state, plan) = df.into_parts();
    let observation = Guard::start(Phase::Optimize, || Identity::CausalContext);
    let optimized = session_state.optimize(&plan);
    observation.finish_result(&optimized);
    let plan = optimized?;
    let plan = if is_streaming_plan(&plan)? {
        rewrite_streaming_plan(plan)?
    } else {
        plan
    };
    info.push(plan.to_stringified(PlanType::FinalLogicalPlan));
    let observation = Guard::start(Phase::PhysicalPlan, || Identity::CausalContext);
    let planned = session_state
        .query_planner()
        .create_physical_plan(&plan, &session_state)
        .await;
    observation.finish_result(&planned);
    let plan = planned?;
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
