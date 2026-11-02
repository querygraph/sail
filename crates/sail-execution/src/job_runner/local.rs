use std::any::Any;
use std::collections::BTreeMap;
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::sync::{Arc, Mutex, MutexGuard};

use datafusion::common::{DataFusionError, Result, internal_datafusion_err};
use datafusion::execution::SendableRecordBatchStream;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::prelude::SessionContext;
use futures::future::join_all;
use sail_common_datafusion::error::CommonErrorCause;
use sail_common_datafusion::managed_stream::{ManagedStreamControl, StreamPreparation};
use sail_common_datafusion::session::job::{JobPreparation, JobRunner};
use sail_common_datafusion::task_resources::TaskResourceScope;
use sail_python_udf::error::PyErrExtractor;
use tokio::runtime::Handle;

use super::explain_job_graph;
use crate::shuffle::{ShuffleBackendKind, ShuffleCompression};

#[derive(Clone)]
struct Job {
    control: ManagedStreamControl,
    resources: Arc<TaskResourceScope>,
}

struct Failure {
    error: Arc<DataFusionError>,
    nonretryable: bool,
    commit_unknown: bool,
}

impl Failure {
    fn new(error: DataFusionError) -> Self {
        // Cause extraction can call foreign error/Python code. Never do this
        // while holding the job registry mutex.
        let cause = CommonErrorCause::new::<PyErrExtractor>(&error);
        Self {
            nonretryable: !cause.is_retryable(),
            commit_unknown: matches!(cause, CommonErrorCause::CommitStateUnknown(_)),
            error: Arc::new(error),
        }
    }

    /// The cleanup failure of a job whose supervisors were all joined. Nothing
    /// of the job is still writing, and the failure was part of the error its
    /// query received, so it does not fail the runner. Only an unknown commit
    /// state is still kept, since checkpoint cleanup must not follow it.
    fn settled(error: DataFusionError, job: u64) -> Option<Self> {
        let failure = Self::new(error);
        if failure.commit_unknown {
            return Some(failure);
        }
        log::warn!(
            "local job {job} failed after its query was told; its resources are settled: {}",
            failure.error
        );
        None
    }
}

#[derive(Default)]
struct Failures {
    first: Option<Arc<DataFusionError>>,
    nonretryable: Option<Arc<DataFusionError>>,
}

impl Failures {
    fn retain(&mut self, failure: &Failure) {
        if self.first.is_none() {
            self.first = Some(failure.error.clone());
        }
        if failure.nonretryable && self.nonretryable.is_none() {
            self.nonretryable = Some(failure.error.clone());
        }
    }

    fn errors(&self) -> Vec<Arc<DataFusionError>> {
        let mut errors: Vec<_> = self.first.iter().cloned().collect();
        if let Some(error) = &self.nonretryable
            && !errors.iter().any(|existing| Arc::ptr_eq(existing, error))
        {
            errors.push(error.clone());
        }
        errors
    }
}

struct Jobs {
    stopped: bool,
    next_id: u64,
    active: BTreeMap<u64, Job>,
    failures: Failures,
}

pub struct LocalJobRunner {
    session_id: String,
    jobs: Mutex<Jobs>,
}

impl LocalJobRunner {
    pub fn new(session_id: String) -> Self {
        Self {
            session_id,
            jobs: Mutex::new(Jobs {
                stopped: false,
                next_id: 1,
                active: BTreeMap::new(),
                failures: Failures::default(),
            }),
        }
    }

    /// Publish the preparation to stop before any caller-side planning. Keep
    /// the authoritative context shared; no execution snapshot is frozen here.
    pub fn begin_preparation(&self, ctx: &SessionContext) -> Result<LocalJobPreparation<'_>> {
        let runtime =
            Handle::try_current().map_err(|error| DataFusionError::External(Box::new(error)))?;
        let resources = Arc::new(TaskResourceScope::new(runtime.clone()));
        let (preparation, control) = StreamPreparation::new(resources.clone(), runtime);
        let id = self.admit(Job {
            control: control.clone(),
            resources: resources.clone(),
        })?;
        Ok(LocalJobPreparation {
            runner: self,
            id,
            context: ctx.clone(),
            resources,
            preparation,
            control,
        })
    }

    fn lock(&self) -> MutexGuard<'_, Jobs> {
        // No execution, error classification or foreign destruction occurs
        // under this mutex. Keep ownership reachable if it is poisoned.
        self.jobs.lock().unwrap_or_else(|error| error.into_inner())
    }

    fn snapshot(&self) -> Vec<(u64, Job)> {
        self.lock()
            .active
            .iter()
            .map(|(id, job)| (*id, job.clone()))
            .collect()
    }

    /// Reclaim completed history on later admission/stop. Interrupted drainage
    /// remains owned; there is no completion-forwarding task to lose on shutdown.
    fn reap(&self) {
        let completed: Vec<_> = self
            .snapshot()
            .into_iter()
            .filter_map(|(id, job)| {
                let report = job.control.completed()?;
                report.resources_drained.then(|| {
                    let failure = report
                        .cleanup_result()
                        .err()
                        .and_then(|error| Failure::settled(error, id));
                    (id, failure)
                })
            })
            .collect();
        let mut removed = Vec::with_capacity(completed.len());
        {
            let mut state = self.lock();
            for (id, failure) in &completed {
                if let Some(job) = state.active.remove(id) {
                    if let Some(failure) = failure {
                        state.failures.retain(failure);
                    }
                    removed.push(job);
                }
            }
        }
        // Last control/resource/error references may run destructors.
        drop(removed);
    }

    fn admit(&self, job: Job) -> Result<u64> {
        self.reap();
        let mut state = self.lock();
        if state.stopped {
            return Err(internal_datafusion_err!("job runner is stopped"));
        }
        let id = state.next_id;
        state.next_id = id
            .checked_add(1)
            .ok_or_else(|| internal_datafusion_err!("local job identity exhausted"))?;
        state.active.insert(id, job);
        Ok(id)
    }

    async fn stop_jobs(&self) -> Result<()> {
        let jobs: Vec<_> = {
            let mut state = self.lock();
            state.stopped = true;
            state
                .active
                .iter()
                .map(|(id, job)| (*id, job.clone()))
                .collect()
        };
        let mut cancellation_errors = vec![];
        // Seal and cancel every admitted preparation/stream before waiting for
        // any one job. Dropping the later waiter cannot undo these requests.
        for (_, job) in &jobs {
            let result = catch_unwind(AssertUnwindSafe(|| job.control.cancel()))
                .unwrap_or_else(|payload| Err(panic_error("cancellation", payload)));
            if let Err(error) = result {
                cancellation_errors.push(error);
            }
        }
        let failures: Vec<_> = cancellation_errors.into_iter().map(Failure::new).collect();
        {
            let mut state = self.lock();
            for failure in &failures {
                state.failures.retain(failure);
            }
        }
        let reports = join_all(jobs.iter().map(|(id, job)| async move {
            let report = job.control.completion().await;
            let mut failures = vec![];
            if let Err(error) = report.cleanup_result() {
                if report.resources_drained {
                    failures.extend(Failure::settled(error, *id));
                } else {
                    failures.push(Failure::new(error));
                }
            }
            let drained = if report.resources_drained {
                true
            } else {
                // The immutable interrupted report remains a failure even if
                // this later join confirms all supervisors have now finished.
                match job.resources.drain().await {
                    Ok(report) => {
                        failures.extend(report.errors().into_iter().map(Failure::new));
                        true
                    }
                    Err(error) => {
                        failures.push(Failure::new(error));
                        false
                    }
                }
            };
            (*id, drained, failures)
        }))
        .await;
        let mut removed = Vec::with_capacity(reports.len());
        let errors = {
            let mut state = self.lock();
            for (id, drained, failures) in &reports {
                // Inspect every snapshotted report, even if another admission
                // sweep or stop waiter has already retired its registry entry.
                for failure in failures {
                    state.failures.retain(failure);
                }
                if *drained && let Some(job) = state.active.remove(id) {
                    removed.push(job);
                }
            }
            state.failures.errors()
        };
        drop(removed);
        match errors.len() {
            0 => Ok(()),
            1 => Err(DataFusionError::Shared(errors[0].clone())),
            _ => Err(DataFusionError::Collection(
                errors.into_iter().map(DataFusionError::Shared).collect(),
            )),
        }
    }
}

impl Default for LocalJobRunner {
    fn default() -> Self {
        Self::new(String::new())
    }
}

impl Drop for LocalJobRunner {
    fn drop(&mut self) {
        let jobs = std::mem::take(
            &mut self
                .jobs
                .get_mut()
                .unwrap_or_else(|error| error.into_inner())
                .active,
        );
        for (_, job) in jobs {
            // Best effort only. An explicit stop awaits registered cleanup.
            let _ = catch_unwind(AssertUnwindSafe(|| job.control.cancel()));
        }
    }
}

#[tonic::async_trait]
impl JobRunner for LocalJobRunner {
    fn begin_preparation(&self, ctx: &SessionContext) -> Result<Box<dyn JobPreparation + '_>> {
        Ok(Box::new(LocalJobRunner::begin_preparation(self, ctx)?))
    }

    fn explain(&self, plan: Arc<dyn ExecutionPlan>) -> Result<String> {
        explain_job_graph(
            plan,
            ShuffleBackendKind::Flight {
                compression: ShuffleCompression::None,
                connection_count: std::num::NonZeroUsize::MIN,
                initial_stream_window_size: None,
                initial_connection_window_size: None,
            },
        )
    }

    async fn execute(
        &self,
        ctx: &SessionContext,
        plan: Arc<dyn ExecutionPlan>,
    ) -> Result<SendableRecordBatchStream> {
        self.begin_preparation(ctx)?.execute(plan).await
    }

    /// Await owned preparation, stream disposal and registered supervisors.
    /// Unregistered operator tasks and remote effects retain their own contracts.
    async fn stop(&self) -> Result<()> {
        self.stop_jobs().await
    }

    /// Checkpoint writers run as jobs of this runner, with their storage
    /// operations registered in the job's resource scope.
    fn confirms_checkpoint_effects(&self) -> bool {
        true
    }
}

fn panic_error(operation: &str, payload: Box<dyn Any + Send>) -> DataFusionError {
    let message = payload
        .downcast_ref::<&str>()
        .copied()
        .or_else(|| payload.downcast_ref::<String>().map(String::as_str))
        .unwrap_or("unknown panic");
    internal_datafusion_err!("local job {operation} panicked: {message}")
}

#[cfg(test)]
mod tests;

mod checkpoint_effects;
mod preparation;
pub use preparation::LocalJobPreparation;
