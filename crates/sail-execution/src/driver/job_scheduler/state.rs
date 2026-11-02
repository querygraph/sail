use std::sync::Arc;

use datafusion::execution::TaskContext;
use datafusion::physical_plan::ExecutionPlanProperties;
use sail_common_datafusion::error::CommonErrorCause;
use tokio::task::AbortHandle;

use crate::driver::job_scheduler::topology::JobTopology;
use crate::driver::output::{JobOutputManager, JobOutputOutcome};
use crate::error::ExecutionResult;
use crate::job_graph::JobGraph;

/// Tracks graph/topology and runtime state for a single job.
pub struct JobDescriptor {
    pub context: Arc<TaskContext>,
    pub graph: JobGraph,
    pub topology: JobTopology,
    pub stages: Vec<StageDescriptor>,
    pub regions: Vec<TaskRegionDescriptor>,
    pub state: JobState,
    /// The task forwarding the job's output to its consumer. It outlives the
    /// `Running` state: a draining job's forwarder still holds the workers'
    /// streams until they end.
    pub forwarder: Option<AbortHandle>,
}

pub enum JobState {
    Running { output: JobOutputManager },
    Draining,
    Succeeded,
    Failed,
    Canceled,
}

impl JobState {
    pub fn finish_output(&mut self, outcome: JobOutputOutcome) {
        match self {
            JobState::Running { .. } | JobState::Draining => {
                *self = match outcome {
                    JobOutputOutcome::Completed => JobState::Succeeded,
                    JobOutputOutcome::Failed => JobState::Failed,
                    JobOutputOutcome::Canceled => JobState::Canceled,
                };
            }
            JobState::Succeeded | JobState::Failed | JobState::Canceled => {}
        }
    }

    pub fn status(&self) -> &'static str {
        match self {
            JobState::Running { .. } => "RUNNING",
            JobState::Draining => "DRAINING",
            JobState::Succeeded => "SUCCEEDED",
            JobState::Failed => "FAILED",
            JobState::Canceled => "CANCELED",
        }
    }
}

impl JobDescriptor {
    /// Stop forwarding the job's output. The forwarder's streams from the
    /// workers are dropped, so the workers' servers can stop; the consumer
    /// sees the output end without its terminal item, which the output's
    /// truncation guard reports as a canceled job.
    pub fn abandon_output(&mut self) {
        if let Some(forwarder) = self.forwarder.take() {
            forwarder.abort();
        }
    }

    pub fn try_new(
        graph: JobGraph,
        state: JobState,
        context: Arc<TaskContext>,
    ) -> ExecutionResult<Self> {
        let mut stages = vec![];
        for stage in graph.stages().iter() {
            let mut descriptor = StageDescriptor {
                tasks: vec![],
                state: StageState::Active,
            };
            for _ in 0..stage.plan.output_partitioning().partition_count() {
                descriptor.tasks.push(TaskDescriptor { attempts: vec![] });
            }
            stages.push(descriptor);
        }
        let topology = JobTopology::try_new(&graph)?;
        let regions = (0..topology.regions.len())
            .map(|_| TaskRegionDescriptor {
                state: TaskRegionState::Running,
            })
            .collect();
        Ok(Self {
            context,
            graph,
            forwarder: None,
            topology,
            stages,
            regions,
            state,
        })
    }
}

#[derive(Debug)]
pub struct StageDescriptor {
    /// A list of tasks for each partition of the stage.
    pub tasks: Vec<TaskDescriptor>,
    pub state: StageState,
}

#[derive(Debug, Clone, Copy)]
pub enum StageState {
    /// The tasks in the stage are not yet completed,
    /// or the task streams are still being consumed.
    Active,
    /// The tasks in the stage will not be scheduled anymore,
    /// and the task streams are no longer being consumed.
    Inactive,
}

impl StageState {
    pub fn status(&self) -> &'static str {
        match self {
            StageState::Active => "ACTIVE",
            StageState::Inactive => "INACTIVE",
        }
    }
}

#[derive(Debug)]
pub struct TaskRegionDescriptor {
    pub state: TaskRegionState,
}

#[derive(Debug, Clone, Copy)]
pub enum TaskRegionState {
    Running,
    Failed,
    Succeeded,
}

#[derive(Debug)]
pub struct TaskDescriptor {
    pub attempts: Vec<TaskAttemptDescriptor>,
}

#[derive(Debug)]
pub struct TaskAttemptDescriptor {
    pub state: TaskState,
    pub messages: Vec<String>,
    pub cause: Option<CommonErrorCause>,
    /// Whether the task streams are fetched for the job output.
    /// This will always be false if the task does not belong to
    /// the final stages of the job.
    pub job_output_fetched: bool,
}

#[derive(Debug, Clone, Copy)]
pub enum TaskState {
    /// The task attempt has been created, but is not assigned to any worker.
    /// A task attempt is only created when the task region is eligible for scheduling.
    Created,
    /// The task attempt is scheduled to a worker, but its status is unknown.
    Scheduled,
    /// The task attempt is running on a worker.
    Running,
    /// The task attempt has succeeded.
    Succeeded,
    /// The task attempt has failed.
    Failed,
    /// The task attempt has been canceled.
    Canceled,
}

impl TaskState {
    pub fn status(&self) -> &'static str {
        match self {
            TaskState::Created => "CREATED",
            TaskState::Scheduled => "SCHEDULED",
            TaskState::Running => "RUNNING",
            TaskState::Succeeded => "SUCCEEDED",
            TaskState::Failed => "FAILED",
            TaskState::Canceled => "CANCELED",
        }
    }

    pub fn consolidate(&self, next: Self) -> Self {
        match (self, next) {
            (TaskState::Created, x) => x,
            (TaskState::Scheduled, TaskState::Created) => *self,
            (TaskState::Scheduled, x) => x,
            (TaskState::Running, TaskState::Created | TaskState::Scheduled) => *self,
            (TaskState::Running, x) => x,
            (TaskState::Succeeded | TaskState::Failed | TaskState::Canceled, _) => *self,
        }
    }

    pub fn is_terminal(&self) -> bool {
        match self {
            TaskState::Succeeded | TaskState::Failed | TaskState::Canceled => true,
            TaskState::Created | TaskState::Scheduled | TaskState::Running => false,
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use datafusion::arrow::datatypes::Schema;
    use datafusion::datasource::memory::MemorySourceConfig;
    use datafusion::execution::TaskContext;
    use tokio::task::JoinSet;

    use super::{JobDescriptor, JobState};
    use crate::error::ExecutionResult;
    use crate::job_graph::{JobGraph, JobGraphOptions};
    use crate::shuffle::{ShuffleBackendKind, ShuffleCompression};

    #[tokio::test]
    async fn abandoning_the_output_ends_the_forwarder() -> ExecutionResult<()> {
        let graph = JobGraph::try_new(
            MemorySourceConfig::try_new_exec(&[vec![]], Arc::new(Schema::empty()), None)?,
            JobGraphOptions {
                shuffle_backend: ShuffleBackendKind::Flight {
                    compression: ShuffleCompression::None,
                    connection_count: std::num::NonZeroUsize::MIN,
                    initial_stream_window_size: None,
                    initial_connection_window_size: None,
                },
            },
        )?;
        let mut job =
            JobDescriptor::try_new(graph, JobState::Draining, Arc::new(TaskContext::default()))?;
        let mut tasks = JoinSet::new();
        // A forwarder that would otherwise run for as long as a worker's
        // response stays open.
        job.forwarder = Some(tasks.spawn(std::future::pending::<()>()));

        job.abandon_output();

        let joined = tasks.join_next().await;
        assert!(joined.is_some_and(|result| result.is_err_and(|e| e.is_cancelled())));
        assert!(job.forwarder.is_none());
        // Abandoning twice is harmless.
        job.abandon_output();
        Ok(())
    }
}
