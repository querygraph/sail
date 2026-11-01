use std::sync::Arc;

use datafusion::prelude::SessionContext;
use sail_common_datafusion::session::job::JobService;
use sail_execution::DriverId;
use tokio::sync::oneshot;

use crate::error::{SessionError, SessionResult};
use crate::session_manager::actor::SessionCleanup;

pub struct ServerSession {
    pub state: ServerSessionState,
}

/// What must be stopped when a session ends.
/// This is retained from the moment the job runner is allocated, so no later step of session
/// creation can be the only owner of the runner.
pub struct SessionOwner {
    pub service: Arc<JobService>,
    /// Set only when the driver is in the session manager's driver registry.
    pub driver_id: Option<DriverId>,
}

pub enum ServerSessionState {
    Creating {
        user_id: String,
        context: SessionContext,
        owner: SessionOwner,
        waiters: Vec<oneshot::Sender<SessionResult<SessionContext>>>,
    },
    Running {
        context: SessionContext,
        owner: SessionOwner,
    },
    /// The session is unavailable and its cleanup record owns the context.
    Deleting { cleanup: Arc<SessionCleanup> },
    /// Cleanup failed. The record retains the error and the resources that were not released.
    /// There is no implicit retry, since the failure may mean that effects are still in flight.
    DeletionFailed { cleanup: Arc<SessionCleanup> },
    /// Cleanup succeeded. This is a tombstone, so the session ID can never be reused.
    Deleted,
    Failed {
        /// The cleanup of whatever the session had acquired before it failed.
        cleanup: Option<Arc<SessionCleanup>>,
        /// Whether the client has released this session.
        released: bool,
    },
}

impl ServerSessionState {
    pub fn status(&self) -> &'static str {
        match self {
            ServerSessionState::Creating { .. } => "CREATING",
            ServerSessionState::Running { .. } => "RUNNING",
            ServerSessionState::Deleting { .. } => "DELETING",
            ServerSessionState::DeletionFailed { .. } => "DELETION_FAILED",
            ServerSessionState::Deleted => "DELETED",
            ServerSessionState::Failed { .. } => "FAILED",
        }
    }

    /// The error for a request on a session in a state that cannot serve it.
    pub fn unavailable(&self, session_id: &str) -> SessionError {
        match self {
            ServerSessionState::Failed {
                released: false, ..
            } => SessionError::invalid(format!("session {session_id} is not running")),
            _ => SessionError::SessionClosed(session_id.to_string()),
        }
    }
}
