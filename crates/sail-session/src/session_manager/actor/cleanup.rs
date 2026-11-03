use std::any::Any;
use std::fmt;
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::sync::{Arc, Mutex, MutexGuard};

use datafusion::common::{DataFusionError, Result, internal_datafusion_err};
use datafusion::execution::runtime_env::RuntimeEnv;
use datafusion::prelude::SessionContext;
use futures::FutureExt;
use sail_cache::remote_checkpoint::RemoteCheckpointRegistry;
use sail_common_datafusion::extension::SessionExtensionAccessor;
use sail_common_datafusion::native_resource::NativeResourceTracker;
use sail_common_datafusion::session::job::JobRunner;
use sail_common_datafusion::session::lifecycle::{SessionOperations, SessionOperationsService};
use sail_execution::DriverId;
use tokio::sync::watch;

use crate::extensions::graph_utils::GraphRuns;
use crate::session_manager::session::SessionOwner;

/// The immutable result of a cleanup attempt.
pub(crate) type CleanupOutcome = std::result::Result<(), Arc<DataFusionError>>;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum CleanupPhase {
    NotStarted,
    StoppingRunner,
    RemovingCheckpoints,
    CleaningExtensionState,
    ReleasingNativeMemory,
}

impl fmt::Display for CleanupPhase {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            CleanupPhase::NotStarted => "starting cleanup",
            CleanupPhase::StoppingRunner => "stopping the job runner",
            CleanupPhase::RemovingCheckpoints => "removing checkpoints",
            CleanupPhase::CleaningExtensionState => "cleaning extension state",
            CleanupPhase::ReleasingNativeMemory => "waiting for native memory to be released",
        })
    }
}

struct CleanupResources {
    owner: SessionOwner,
    context: Option<SessionContext>,
}

/// The single record that owns the end of a session. Explicit deletion, idle deletion, session
/// failure and manager shutdown all share it, so a session cannot be cleaned up twice.
///
/// The outcome is published by the driver and is observed without going through the actor.
/// RPC callers are observers only: a caller that disappears cannot affect the cleanup.
/// A failed cleanup keeps its resources reachable from here.
pub struct SessionCleanup {
    session_id: String,
    attempt: u64,
    driver_id: Option<DriverId>,
    phase: Mutex<CleanupPhase>,
    resources: Mutex<Option<CleanupResources>>,
    outcome: watch::Sender<Option<CleanupOutcome>>,
}

impl SessionCleanup {
    pub(super) fn new(
        session_id: String,
        attempt: u64,
        owner: SessionOwner,
        context: Option<SessionContext>,
    ) -> Arc<Self> {
        Arc::new(Self {
            session_id,
            attempt,
            driver_id: owner.driver_id,
            phase: Mutex::new(CleanupPhase::NotStarted),
            resources: Mutex::new(Some(CleanupResources { owner, context })),
            outcome: watch::channel(None).0,
        })
    }

    pub(super) fn attempt(&self) -> u64 {
        self.attempt
    }

    pub(super) fn driver_id(&self) -> Option<DriverId> {
        self.driver_id
    }

    /// The published outcome, if the cleanup has finished.
    pub(crate) fn outcome(&self) -> Option<CleanupOutcome> {
        self.outcome.borrow().clone()
    }

    /// Wait for the cleanup to finish. Dropping this future has no effect on the cleanup.
    pub(crate) async fn wait(&self) -> CleanupOutcome {
        let mut outcome = self.outcome.subscribe();
        match outcome.wait_for(Option::is_some).await {
            Ok(value) => value.clone().unwrap_or_else(|| Err(interrupted(self))),
            // The sender lives in this record, so this cannot be reached while `self` is alive.
            Err(_) => Err(interrupted(self)),
        }
    }

    /// Create the driver, which must be done before spawning anything.
    /// If the spawned task never runs, dropping the driver reports the interruption.
    pub(super) fn driver(self: &Arc<Self>) -> CleanupDriver {
        CleanupDriver {
            record: self.clone(),
        }
    }

    fn lock_resources(&self) -> MutexGuard<'_, Option<CleanupResources>> {
        // No foreign code runs under this mutex.
        self.resources
            .lock()
            .unwrap_or_else(|error| error.into_inner())
    }

    fn set_phase(&self, phase: CleanupPhase) {
        *self.phase.lock().unwrap_or_else(|error| error.into_inner()) = phase;
    }

    fn phase(&self) -> CleanupPhase {
        *self.phase.lock().unwrap_or_else(|error| error.into_inner())
    }

    async fn clean(&self) -> Result<()> {
        let (service, context) = {
            let resources = self.lock_resources();
            let Some(resources) = resources.as_ref() else {
                return Err(internal_datafusion_err!(
                    "session cleanup resources are missing"
                ));
            };
            (resources.owner.service.clone(), resources.context.clone())
        };
        let checkpoints = context
            .as_ref()
            .and_then(|context| context.extension::<RemoteCheckpointRegistry>().ok());
        let operations = context
            .as_ref()
            .and_then(|context| context.extension::<SessionOperationsService>().ok());
        let runtime = context
            .as_ref()
            .map(|context| context.runtime_env())
            .unwrap_or_default();
        let graph_runs = context
            .as_ref()
            .and_then(|context| context.extension::<GraphRuns>().ok());
        let native_resources = context
            .as_ref()
            .and_then(|context| context.extension::<NativeResourceTracker>().ok());
        stop_and_clean_checkpoints(
            operations.as_ref().map(|x| x.operations()),
            service.runner(),
            checkpoints.as_deref(),
            runtime.as_ref(),
            |phase| self.set_phase(phase),
        )
        .await?;
        // Extension state lives outside the job runner and the checkpoints. It is
        // cleaned only after both have stopped, since a native region can still be
        // producing until then; and the memory native producers hold is waited for,
        // not reclaimed: a cleanup that reports success has nothing left in flight.
        if let Some(graph_runs) = graph_runs {
            self.set_phase(CleanupPhase::CleaningExtensionState);
            graph_runs.cleanup().await?;
        }
        if let Some(native_resources) = native_resources {
            self.set_phase(CleanupPhase::ReleasingNativeMemory);
            // The packages' admission leases are owned by the session's own extensions
            // (a bound package holds its lease in a capsule), so the count cannot reach
            // zero while a session context is alive. Everything that needed the context
            // has finished above; release every handle to it before waiting.
            drop(context);
            drop(operations);
            drop(checkpoints);
            let retained = self
                .lock_resources()
                .as_mut()
                .and_then(|resources| resources.context.take());
            drop(retained);
            native_resources.wait_for_release().await;
        }
        Ok(())
    }

    fn publish(&self, outcome: CleanupOutcome) -> bool {
        self.outcome.send_if_modified(|current| {
            if current.is_some() {
                return false;
            }
            *current = Some(outcome);
            true
        })
    }
}

fn interrupted(record: &SessionCleanup) -> Arc<DataFusionError> {
    Arc::new(internal_datafusion_err!(
        "cleanup of session {} was interrupted while {}; its outcome is unknown",
        record.session_id,
        record.phase()
    ))
}

fn panicked(operation: &str, payload: &(dyn Any + Send)) -> DataFusionError {
    let message = payload
        .downcast_ref::<&str>()
        .copied()
        .or_else(|| payload.downcast_ref::<String>().map(String::as_str))
        .unwrap_or("unknown panic");
    internal_datafusion_err!("{operation} panicked: {message}")
}

/// The unique right to run a cleanup. If this is dropped before the cleanup finishes (the task
/// was aborted or never started), the record reports interruption instead of leaving observers
/// waiting. An interruption is never reported as success.
pub(super) struct CleanupDriver {
    record: Arc<SessionCleanup>,
}

impl CleanupDriver {
    pub(super) async fn run(self) {
        let record = &self.record;
        let result = AssertUnwindSafe(record.clean())
            .catch_unwind()
            .await
            .unwrap_or_else(|payload| Err(panicked("session cleanup", payload.as_ref())));
        match result {
            Ok(()) => {
                // Publish first: a resource destructor must not delay or prevent the outcome.
                let resources = record.lock_resources().take();
                record.publish(Ok(()));
                drop(resources);
            }
            Err(error) => {
                record.publish(Err(Arc::new(DataFusionError::Context(
                    format!(
                        "failed to clean up session {} while {}",
                        record.session_id,
                        record.phase()
                    ),
                    Box::new(error),
                ))));
            }
        }
    }
}

impl Drop for CleanupDriver {
    fn drop(&mut self) {
        // This has no effect if the outcome has already been published.
        self.record.publish(Err(interrupted(&self.record)));
    }
}

/// Close the operations of the session and stop its runner, and then remove its checkpoints.
///
/// Closing is requested before anything is awaited, and the runner is stopped concurrently with
/// waiting for the operations. An operation can be waiting for a job, and joining it before the
/// job was stopped would never finish. Waiting for the runner alone is not enough either, since
/// operations can start jobs until they have finished.
///
/// Do not invoke checkpoint cleanup when any of this reports an error: the error can mean that
/// something is still writing.
pub(super) async fn stop_and_clean_checkpoints(
    operations: Option<&dyn SessionOperations>,
    runner: &dyn JobRunner,
    checkpoints: Option<&RemoteCheckpointRegistry>,
    runtime: &RuntimeEnv,
    on_phase: impl Fn(CleanupPhase),
) -> Result<()> {
    on_phase(CleanupPhase::StoppingRunner);
    let mut errors = vec![];
    if let Some(operations) = operations {
        match catch_unwind(AssertUnwindSafe(|| operations.begin_close())) {
            Ok(Ok(())) => {}
            Ok(Err(error)) => errors.push(error),
            Err(payload) => errors.push(panicked("closing operations", payload.as_ref())),
        }
    }
    let wait_closed = async {
        let Some(operations) = operations else {
            return Ok(());
        };
        AssertUnwindSafe(operations.wait_closed())
            .catch_unwind()
            .await
            .unwrap_or_else(|payload| Err(panicked("waiting for operations", payload.as_ref())))
    };
    let (stopped, closed) = tokio::join!(runner.stop(), wait_closed);
    errors.extend(stopped.err());
    errors.extend(closed.err());
    if let Some(error) = errors.pop() {
        return Err(if errors.is_empty() {
            error
        } else {
            errors.push(error);
            DataFusionError::Collection(errors)
        });
    }
    if let Some(checkpoints) = checkpoints {
        if checkpoints.may_have_storage() && !runner.confirms_checkpoint_effects() {
            // The objects stay in place, and the record retains what is needed to find them.
            return Err(DataFusionError::Execution(
                "session checkpoints were not removed, since the job runner cannot confirm \
                 that their writers have stopped"
                    .to_string(),
            ));
        }
        on_phase(CleanupPhase::RemovingCheckpoints);
        checkpoints.cleanup_session(runtime).await?;
    }
    Ok(())
}

#[cfg(test)]
#[path = "cleanup_tests.rs"]
mod tests;
