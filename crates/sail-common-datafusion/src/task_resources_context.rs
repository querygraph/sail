use std::sync::Arc;

use datafusion::execution::TaskContext;
use datafusion::execution::object_store::ObjectStoreRegistry;
use datafusion::execution::runtime_env::{RuntimeEnv, RuntimeEnvBuilder};
use datafusion::prelude::SessionConfig;
use datafusion_common::{Result, internal_datafusion_err};
use tokio::runtime::Handle;

use super::TaskResourceScope;
use crate::extension::SessionExtensionAccessor;

/// A fresh resource scope and a context preserving the parent's execution state.
/// Creating this value does not install or drain it in a job/task runner.
pub struct ScopedTaskContext {
    pub context: Arc<TaskContext>,
    pub resources: Arc<TaskResourceScope>,
}

impl ScopedTaskContext {
    pub fn new(parent: &TaskContext, runtime: Handle) -> Self {
        let resources = Arc::new(TaskResourceScope::new(runtime));
        let config = parent
            .session_config()
            .clone()
            .with_extension(resources.clone());
        let context = copy_context(parent, config, parent.runtime_env());
        Self { context, resources }
    }

    /// Derive a context with an operation-specific store registry while keeping
    /// this exact resource owner. Pool/disk/caches and feature-dependent runtime
    /// state are inherited via DataFusion's builder; no fresh scope is created.
    /// The caller supplies a registry whose ownership/routing is already valid.
    /// This method alone does not intercept or qualify storage effects.
    pub fn with_object_store_registry(
        &self,
        registry: Arc<dyn ObjectStoreRegistry>,
    ) -> Result<Self> {
        let resources = self.context.extension::<TaskResourceScope>()?;
        if !Arc::ptr_eq(&resources, &self.resources) {
            return Err(internal_datafusion_err!(
                "task context resource scope does not match its owner"
            ));
        }
        let runtime = RuntimeEnvBuilder::from_runtime_env(self.context.runtime_env().as_ref())
            .with_object_store_registry(registry)
            .build_arc()?;
        let context = copy_context(
            &self.context,
            self.context.session_config().clone(),
            runtime,
        );
        Ok(Self { context, resources })
    }
}

fn copy_context(
    parent: &TaskContext,
    config: SessionConfig,
    runtime: Arc<RuntimeEnv>,
) -> Arc<TaskContext> {
    Arc::new(TaskContext::new(
        parent.task_id(),
        parent.session_id(),
        config,
        parent.scalar_functions().clone(),
        parent.higher_order_functions().clone(),
        parent.aggregate_functions().clone(),
        parent.window_functions().clone(),
        runtime,
    ))
}

#[cfg(test)]
#[path = "task_resources_context_tests.rs"]
mod tests;
