use arrow::array::RecordBatch;
use datafusion::catalog::Session;
use datafusion::execution::SessionState;
use datafusion::physical_plan::collect;
use datafusion_expr::LogicalPlan;

use super::PlanResolver;
use crate::error::PlanResult;

/// Keep a legacy snapshot alive across a factory call, or borrow the caller's
/// captured view. Only the legacy branch captures state, at the old call phase.
pub(super) enum ProviderSession<'a> {
    Captured(&'a dyn Session),
    Legacy(Box<SessionState>),
}

impl ProviderSession<'_> {
    pub(super) fn as_session(&self) -> &dyn Session {
        match self {
            Self::Captured(session) => *session,
            Self::Legacy(state) => state.as_ref(),
        }
    }
}

impl PlanResolver<'_> {
    pub(super) fn provider_session(&self) -> ProviderSession<'_> {
        match self.query_session {
            Some(session) => ProviderSession::Captured(session),
            None => ProviderSession::Legacy(Box::new(self.ctx.state())),
        }
    }

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
