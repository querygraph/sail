use arrow::array::RecordBatch;
use datafusion::physical_plan::collect;
use datafusion_expr::LogicalPlan;

use super::PlanResolver;
use crate::error::PlanResult;

impl PlanResolver<'_> {
    /// Execute a query used by resolution, preserving the legacy path unless
    /// the caller supplied its admitted query Session. This never dispatches
    /// authoritative session commands through a private state snapshot.
    pub(super) async fn collect_query(&self, plan: LogicalPlan) -> PlanResult<Vec<RecordBatch>> {
        match self.query_session {
            Some(session) => {
                let physical = session.create_physical_plan(&plan).await?;
                Ok(collect(physical, session.task_ctx()).await?)
            }
            None => Ok(self.ctx.execute_logical_plan(plan).await?.collect().await?),
        }
    }
}
