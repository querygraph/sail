//! A query snapshot for provider/physical-planner calls through `Session`.
//!
//! Session commands still belong to the authoritative SessionContext. Callers
//! choose the snapshot boundary after those commands and retain the admitted
//! resource owner through planning, execution and cleanup.

use std::any::Any;
use std::collections::HashMap;
use std::sync::Arc;

use datafusion::catalog::{CatalogProviderList, Session};
use datafusion::execution::object_store::ObjectStoreRegistry;
use datafusion::execution::runtime_env::{RuntimeEnv, RuntimeEnvBuilder};
use datafusion::execution::{SessionState, TaskContext};
use datafusion::physical_optimizer::PhysicalOptimizerRule;
use datafusion::physical_plan::operator_statistics::StatisticsRegistry;
use datafusion::physical_plan::{ExecutionPlan, PhysicalExpr};
use datafusion::prelude::SessionConfig;
use datafusion_common::config::TableOptions;
use datafusion_common::{DFSchema, Result};
use datafusion_expr::execution_props::ExecutionProps;
use datafusion_expr::registry::ExtensionTypeRegistryRef;
use datafusion_expr::{AggregateUDF, Expr, HigherOrderUDF, LogicalPlan, ScalarUDF, WindowUDF};

use crate::task_resources::TaskResourceScope;

/// Preserve the captured SessionState without rebuilding its registries. The
/// optional registry overlay is visible to Session consumers and derived tasks.
/// This does not create admission, run session commands, or install routing in
/// the parent. Providers requiring a SessionState downcast need a separate seam.
pub struct ScopedSession {
    state: SessionState,
    runtime: Arc<RuntimeEnv>,
    resources: Arc<TaskResourceScope>,
}

impl ScopedSession {
    pub fn new(mut state: SessionState, resources: Arc<TaskResourceScope>) -> Self {
        state.config_mut().set_extension(resources.clone());
        let runtime = state.runtime_env().clone();
        Self {
            state,
            runtime,
            resources,
        }
    }

    pub fn resources(&self) -> &Arc<TaskResourceScope> {
        &self.resources
    }

    /// Supply an already-qualified registry; this helper does not define its
    /// provider ownership, admission bounds or effect-outcome policy.
    pub fn with_object_store_registry(
        mut self,
        registry: Arc<dyn ObjectStoreRegistry>,
    ) -> Result<Self> {
        self.runtime = RuntimeEnvBuilder::from_runtime_env(&self.runtime)
            .with_object_store_registry(registry)
            .build_arc()?;
        Ok(self)
    }
}

#[tonic::async_trait]
impl Session for ScopedSession {
    fn session_id(&self) -> &str {
        self.state.session_id()
    }
    fn config(&self) -> &SessionConfig {
        self.state.config()
    }
    fn catalog_list(&self) -> Arc<dyn CatalogProviderList> {
        // Required DataFusion Session interface; Sail name resolution continues
        // to use CatalogManager rather than this native catalog list.
        Session::catalog_list(&self.state)
    }
    fn query_planner(&self) -> Arc<dyn datafusion::execution::context::QueryPlanner + Send + Sync> {
        self.state.query_planner().clone()
    }
    fn optimize(&self, plan: &LogicalPlan) -> Result<LogicalPlan> {
        self.state.optimize(plan)
    }
    fn physical_optimizers(&self) -> &[Arc<dyn PhysicalOptimizerRule + Send + Sync>] {
        self.state.physical_optimizers()
    }
    fn statistics_registry(&self) -> Option<&StatisticsRegistry> {
        self.state.statistics_registry()
    }
    async fn create_physical_plan(&self, plan: &LogicalPlan) -> Result<Arc<dyn ExecutionPlan>> {
        let plan = self.optimize(plan)?;
        self.query_planner().create_physical_plan(&plan, self).await
    }
    fn create_physical_expr(&self, expr: Expr, schema: &DFSchema) -> Result<Arc<dyn PhysicalExpr>> {
        self.state.create_physical_expr(expr, schema)
    }
    fn scalar_functions(&self) -> &HashMap<String, Arc<ScalarUDF>> {
        self.state.scalar_functions()
    }
    fn higher_order_functions(&self) -> &HashMap<String, Arc<HigherOrderUDF>> {
        self.state.higher_order_functions()
    }
    fn aggregate_functions(&self) -> &HashMap<String, Arc<AggregateUDF>> {
        self.state.aggregate_functions()
    }
    fn window_functions(&self) -> &HashMap<String, Arc<WindowUDF>> {
        self.state.window_functions()
    }
    fn extension_type_registry(&self) -> &ExtensionTypeRegistryRef {
        Session::extension_type_registry(&self.state)
    }
    fn runtime_env(&self) -> &Arc<RuntimeEnv> {
        &self.runtime
    }
    fn execution_props(&self) -> &ExecutionProps {
        self.state.execution_props()
    }
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn table_options(&self) -> &TableOptions {
        self.state.table_options()
    }
    fn table_options_mut(&mut self) -> &mut TableOptions {
        self.state.table_options_mut()
    }
    fn task_ctx(&self) -> Arc<TaskContext> {
        Arc::new(TaskContext::from(self as &dyn Session))
    }
}

#[cfg(test)]
mod tests;
