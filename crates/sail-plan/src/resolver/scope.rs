use datafusion_expr::LogicalPlan;

use crate::error::PlanResult;

/// A private relation namespace for one planning request. Returned plans must
/// expose user-facing field names, as stored temporary-view plans do.
/// CTEs take precedence over this scope; the catalog is consulted on `None`.
pub trait RelationScope: Send + Sync {
    fn resolve(&self, name: &[String]) -> PlanResult<Option<LogicalPlan>>;
}
