mod cleanup;
mod core;
mod handler;
mod message;
mod options;

use std::sync::Arc;

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

pub struct SessionManagerActor {
    options: SessionManagerOptions,
    session_factory: Box<dyn SessionFactory<ServerSessionInfo>>,
    job_runner_factory: Box<dyn SessionJobRunnerFactory>,
    sessions: IndexMap<String, ServerSession>,
    drivers: DriverRegistry,
    driver_gateway: Option<DriverGateway>,
    driver_id_generator: IdGenerator<DriverId>,
    next_cleanup_attempt: u64,
    shared: Arc<ManagerShared>,
    shutdown_notifier: Option<tokio::sync::oneshot::Sender<()>>,
    event_reporter: SystemEventReporter,
}

#[cfg(test)]
mod lifecycle_tests;
