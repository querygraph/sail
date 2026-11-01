use std::borrow::Cow;
use std::sync::Arc;

use datafusion::prelude::SessionContext;
use sail_common::telemetry::{SpanAssociation, SpanAttribute};
use sail_execution::DriverId;
use sail_execution::driver::DriverHandle;
use sail_execution::error::ExecutionResult;
use tokio::sync::oneshot;
use tokio::time::Instant;

use crate::error::SessionResult;
use crate::session_manager::actor::SessionCleanup;

/// The result of accepting a session release.
pub enum SessionRelease {
    /// There was nothing to release: the session is unknown, or was already deleted.
    Nothing,
    /// The release is complete once this cleanup is.
    Cleanup(Arc<SessionCleanup>),
}

pub enum SessionManagerMessage {
    GetOrCreateSession {
        session_id: String,
        user_id: String,
        result: oneshot::Sender<SessionResult<SessionContext>>,
    },
    CompleteSessionCreation {
        session_id: String,
        activation: ExecutionResult<()>,
    },
    /// Sent after the cleanup has published its outcome.
    /// This updates the session state and is not needed to observe the outcome.
    CompleteSessionCleanup {
        session_id: String,
        attempt: u64,
    },
    ProbeIdleSession {
        session_id: String,
        /// The time when the session was known to be active.
        instant: Instant,
    },
    DeleteSession {
        session_id: String,
        result: oneshot::Sender<SessionResult<SessionRelease>>,
    },
    /// Check that a request may use a session, without creating the session.
    CheckSession {
        session_id: String,
        result: oneshot::Sender<SessionResult<()>>,
    },
    SetSessionFailure {
        session_id: String,
    },
    GetDriver {
        driver_id: DriverId,
        result: oneshot::Sender<ExecutionResult<DriverHandle>>,
    },
    Shutdown {
        result: oneshot::Sender<()>,
    },
}

impl SpanAssociation for SessionManagerMessage {
    fn name(&self) -> Cow<'static, str> {
        let name = match self {
            SessionManagerMessage::GetOrCreateSession { .. } => "GetOrCreateSession",
            SessionManagerMessage::CompleteSessionCreation { .. } => "CompleteSessionCreation",
            SessionManagerMessage::CompleteSessionCleanup { .. } => "CompleteSessionCleanup",
            SessionManagerMessage::ProbeIdleSession { .. } => "ProbeIdleSession",
            SessionManagerMessage::DeleteSession { .. } => "DeleteSession",
            SessionManagerMessage::CheckSession { .. } => "CheckSession",
            SessionManagerMessage::SetSessionFailure { .. } => "SetSessionFailure",
            SessionManagerMessage::GetDriver { .. } => "GetDriver",
            SessionManagerMessage::Shutdown { .. } => "Shutdown",
        };
        name.into()
    }

    fn properties(&self) -> impl IntoIterator<Item = (Cow<'static, str>, Cow<'static, str>)> {
        let mut p: Vec<(&'static str, String)> = vec![];
        match self {
            SessionManagerMessage::GetOrCreateSession {
                session_id,
                user_id: _,
                result: _,
            }
            | SessionManagerMessage::CompleteSessionCreation {
                session_id,
                activation: _,
            }
            | SessionManagerMessage::CompleteSessionCleanup {
                session_id,
                attempt: _,
            }
            | SessionManagerMessage::ProbeIdleSession {
                session_id,
                instant: _,
            }
            | SessionManagerMessage::DeleteSession {
                session_id,
                result: _,
            }
            | SessionManagerMessage::CheckSession {
                session_id,
                result: _,
            }
            | SessionManagerMessage::SetSessionFailure { session_id } => {
                p.push((SpanAttribute::SESSION_ID, session_id.to_string()));
            }
            SessionManagerMessage::GetDriver {
                driver_id,
                result: _,
            } => {
                p.push((SpanAttribute::CLUSTER_DRIVER_ID, driver_id.to_string()));
            }
            SessionManagerMessage::Shutdown { .. } => {}
        }
        p.into_iter().map(|(k, v)| (k.into(), v.into()))
    }
}
