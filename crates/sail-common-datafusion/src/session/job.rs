use std::sync::Arc;

use datafusion::common::Result;
use datafusion::execution::SendableRecordBatchStream;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::prelude::SessionContext;

use crate::extension::SessionExtension;

#[tonic::async_trait]
pub trait JobRunner: Send + Sync + 'static {
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
