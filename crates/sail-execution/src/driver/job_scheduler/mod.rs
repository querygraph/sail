mod core;
mod options;
mod state;
mod topology;

use std::sync::Arc;
use std::sync::atomic::AtomicBool;

use datafusion::arrow::datatypes::SchemaRef;
use datafusion::execution::TaskContext;
use datafusion_proto::physical_plan::PhysicalExtensionCodec;
use indexmap::IndexMap;
pub use options::JobSchedulerOptions;
use sail_common_datafusion::error::CommonErrorCause;
use sail_telemetry::events::SystemEventReporter;
pub use state::{JobState, TaskState};

use crate::driver::job_scheduler::state::JobDescriptor;
use crate::driver::output::JobOutputHandle;
use crate::id::{IdGenerator, JobId, TaskKey, TaskStreamKey};
use crate::proto::RemoteExecutionCodec;
use crate::task::scheduling::TaskRegion;

pub struct JobScheduler {
    options: JobSchedulerOptions,
    jobs: IndexMap<JobId, JobDescriptor>,
    job_id_generator: IdGenerator<JobId>,
    codec: Box<dyn PhysicalExtensionCodec>,
    event_reporter: SystemEventReporter,
    /// Set when the driver stops, so that the output of an abandoned job is not
    /// taken for the output of a finished one.
    stopping: Arc<AtomicBool>,
}

impl JobScheduler {
    pub fn new(options: JobSchedulerOptions, event_reporter: SystemEventReporter) -> Self {
        Self {
            options,
            jobs: IndexMap::new(),
            job_id_generator: IdGenerator::new(),
            codec: Box::new(RemoteExecutionCodec),
            event_reporter,
            stopping: Arc::new(AtomicBool::new(false)),
        }
    }
}

#[derive(Debug)]
pub enum JobAction {
    ScheduleTaskRegion {
        region: TaskRegion,
    },
    CancelTask {
        key: TaskKey,
    },
    ExtendJobOutput {
        handle: JobOutputHandle,
        key: TaskStreamKey,
        schema: SchemaRef,
        retrying: Option<CommonErrorCause>,
    },
    FailJobOutput {
        handle: JobOutputHandle,
        cause: CommonErrorCause,
    },
    CleanUpJob {
        job_id: JobId,
        stage: Option<usize>,
        context: Arc<TaskContext>,
    },
}
