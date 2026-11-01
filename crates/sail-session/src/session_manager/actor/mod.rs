mod cleanup;
mod core;
mod handler;
mod message;
mod options;
mod shutdown;

use std::sync::Arc;

pub(crate) use cleanup::CleanupOutcome;
pub use cleanup::SessionCleanup;
use indexmap::IndexMap;
pub(crate) use message::{SessionManagerMessage, SessionRelease};
pub use options::{SessionManagerComponents, SessionManagerOptions};
use sail_execution::driver::{DriverGateway, DriverRegistry};
use sail_execution::{DriverId, IdGenerator};
use sail_telemetry::events::SystemEventReporter;

use crate::session_factory::{ServerSessionInfo, SessionFactory, SessionJobRunnerFactory};
use crate::session_manager::session::ServerSession;
use crate::session_manager::shared::ManagerShared;

/// The lifecycle of the session manager.
enum ManagerState {
    Open,
    /// Session admission is sealed and the shutdown is in progress.
    /// The actor keeps serving the messages that the shutdown depends on.
    Closing,
    /// Everything has stopped. This holds the outcome until the actor publishes it.
    Stopped(CleanupOutcome),
}

pub struct SessionManagerActor {
    state: ManagerState,
    options: SessionManagerOptions,
    session_factory: Box<dyn SessionFactory<ServerSessionInfo>>,
    job_runner_factory: Box<dyn SessionJobRunnerFactory>,
    sessions: IndexMap<String, ServerSession>,
    drivers: DriverRegistry,
    driver_gateway: Option<DriverGateway>,
    driver_id_generator: IdGenerator<DriverId>,
    next_cleanup_attempt: u64,
    shared: Arc<ManagerShared>,
    event_reporter: SystemEventReporter,
}

impl Drop for SessionManagerActor {
    fn drop(&mut self) {
        // This has no effect after a normal stop. Otherwise, the actor panicked or was aborted,
        // and observers must learn about that rather than wait forever.
        self.shared.interrupt_shutdown();
    }
}

#[cfg(test)]
mod lifecycle_tests;
