use std::sync::Arc;

use datafusion::common::{DataFusionError, Result};
use datafusion::execution::SendableRecordBatchStream;
use datafusion::execution::object_store::ObjectStoreRegistry;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::prelude::SessionContext;

use crate::extension::SessionExtension;
use crate::managed_stream::ManagedStreamControl;
use crate::session::scoped::ScopedSession;
use crate::task_resources::TaskResourceScope;

/// An admitted, move-only preparation bound to the runner that created it.
/// Planning remains on the caller, which retains its typed result and borrows
/// this scope. Finish/cancel expose cleanup independently of that result.
#[tonic::async_trait]
pub trait JobPreparation: Send + Sync {
    fn resources(&self) -> &Arc<TaskResourceScope>;
    fn control(&self) -> ManagedStreamControl;
    fn finish(self: Box<Self>) -> ManagedStreamControl;
    fn cancel(self: Box<Self>) -> ManagedStreamControl;

    /// Consume admission into a query snapshot after authority mutations.
    /// The optional registry must already have qualified ownership/routing.
    /// It replaces the registry in the captured runtime; supply all required
    /// stores or an explicitly composed registry, not a partial registration.
    /// Retain control before this call to observe cleanup if capture fails.
    /// The default cancels admission and returns the typed capability error to
    /// the caller; the control reports cancellation and cleanup separately.
    /// Backends must opt in; legacy execute keeps its existing capture boundary.
    fn capture<'a>(
        self: Box<Self>,
        _registry: Option<Arc<dyn ObjectStoreRegistry>>,
    ) -> Result<Box<dyn CapturedJobPreparation + 'a>>
    where
        Self: 'a,
    {
        self.cancel();
        Err(DataFusionError::NotImplemented(
            "captured query preparation is not supported by this runner".into(),
        ))
    }

    async fn execute(
        self: Box<Self>,
        plan: Arc<dyn ExecutionPlan>,
    ) -> Result<SendableRecordBatchStream>;
}

/// An admitted executor whose query snapshot is fixed at capture.
/// The borrowed view and consuming execute belong to the same preparation;
/// callers cannot supply a different Session at the execution boundary.
/// This does not make retained providers reusable after their owner closes.
/// Calling inherited `capture` again is unsupported: like any failed consuming
/// transition it consumes this guard, and the default cancels its admission.
/// Keep the control to observe cleanup; a failed second capture cannot recover
/// the original guard for execution.
pub trait CapturedJobPreparation: JobPreparation {
    fn session(&self) -> &ScopedSession;
}

#[tonic::async_trait]
pub trait JobRunner: Send + Sync + 'static {
    /// Admit preparation before the caller invokes planning. The object borrows
    /// this runner and cannot be handed to another runner for execution.
    /// Legacy implementations retain execute; migrate planning callers only
    /// after the corresponding runner's preparation path is qualified.
    fn begin_preparation(&self, _ctx: &SessionContext) -> Result<Box<dyn JobPreparation + '_>> {
        Err(DataFusionError::NotImplemented(
            "pre-planning preparation is not supported by this runner".into(),
        ))
    }

    /// Executes a plan.
    async fn execute(
        &self,
        ctx: &SessionContext,
        plan: Arc<dyn ExecutionPlan>,
    ) -> Result<SendableRecordBatchStream>;

    /// Returns the distributed execution plan for EXPLAIN output.
    fn explain(&self, plan: Arc<dyn ExecutionPlan>) -> Result<String>;

    /// Stop admission/execution and report shutdown or resource-cleanup errors.
    /// Implementations specify which local or remote cleanup they can confirm.
    async fn stop(&self) -> Result<()>;

    /// Whether a successful [`Self::stop`] means that no checkpoint write can still be running.
    /// Without this, it is not safe to delete the checkpoints of the session, since an object
    /// could be published after the deletion. A runner must opt in, based on what it owns.
    fn confirms_checkpoint_effects(&self) -> bool {
        false
    }
}

pub struct JobService {
    runner: Box<dyn JobRunner>,
}

impl JobService {
    pub fn new(runner: Box<dyn JobRunner>) -> Self {
        Self { runner }
    }

    pub fn runner(&self) -> &dyn JobRunner {
        self.runner.as_ref()
    }
}

impl SessionExtension for JobService {
    fn name() -> &'static str {
        "JobService"
    }
}
