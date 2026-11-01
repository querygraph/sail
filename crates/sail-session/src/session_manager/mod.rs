mod actor;
mod session;
mod shared;

use std::fmt;
use std::sync::Arc;
use std::time::Duration;

use datafusion::common::DataFusionError;
use datafusion::prelude::SessionContext;
use sail_common::actor::{ActorHandle, ActorSystem};
use sail_common::config::{AppConfig, ExecutionMode};
use sail_common::runtime::RuntimeHandle;
use sail_execution::driver::{DriverGateway, DriverGatewayOptions};
use sail_telemetry::telemetry::global_system_event_reporter;
use tokio::sync::oneshot;

use crate::error::{SessionError, SessionResult};
use crate::session_factory::{
    ServerSessionInfo, ServerSessionJobRunnerFactory, SessionFactory, SessionJobRunnerFactory,
};
use crate::session_manager::actor::SessionRelease;
pub(crate) use crate::session_manager::actor::{SessionManagerActor, SessionManagerMessage};
pub use crate::session_manager::actor::{SessionManagerComponents, SessionManagerOptions};
use crate::session_manager::shared::ManagerShared;

pub type ServerSessionFactoryFn =
    fn(Arc<AppConfig>, RuntimeHandle) -> Box<dyn SessionFactory<ServerSessionInfo>>;

#[derive(Clone)]
pub struct SessionManager {
    handle: ActorHandle<SessionManagerActor>,
    shared: Arc<ManagerShared>,
}

impl fmt::Debug for SessionManager {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SessionManager").finish()
    }
}

impl SessionManager {
    pub fn try_new(
        options: SessionManagerOptions,
        components: SessionManagerComponents,
        system: &mut ActorSystem,
    ) -> SessionResult<Self> {
        let shared = Arc::new(ManagerShared::default());
        let handle = system.spawn::<SessionManagerActor>((options, components, shared.clone()));
        Ok(Self { handle, shared })
    }

    pub async fn get_or_create_session_context(
        &self,
        session_id: String,
        user_id: String,
    ) -> SessionResult<SessionContext> {
        let (tx, rx) = oneshot::channel();
        let message = SessionManagerMessage::GetOrCreateSession {
            session_id: session_id.clone(),
            user_id,
            result: tx,
        };
        self.handle
            .send(message)
            .await
            .map_err(|_| self.stopped_error(&session_id))?;
        rx.await.map_err(|_| self.stopped_error(&session_id))?
    }

    /// Check that a request may use a session, without creating the session.
    /// A session that has not been seen is accepted.
    pub async fn check_session(&self, session_id: String) -> SessionResult<()> {
        let (tx, rx) = oneshot::channel();
        let message = SessionManagerMessage::CheckSession {
            session_id: session_id.clone(),
            result: tx,
        };
        self.handle
            .send(message)
            .await
            .map_err(|_| self.stopped_error(&session_id))?;
        rx.await.map_err(|_| self.stopped_error(&session_id))?
    }

    /// Delete a session and wait for the outcome of its cleanup.
    /// Returns whether a session was closed by a release that has now completed.
    /// Releasing a session that was never seen, or was already deleted, does nothing.
    ///
    /// Dropping the returned future stops waiting, but it does not affect the cleanup.
    /// A failed cleanup keeps reporting the same error: it is not retried.
    pub async fn delete_session(&self, session_id: String) -> SessionResult<bool> {
        let (tx, rx) = oneshot::channel();
        let message = SessionManagerMessage::DeleteSession {
            session_id: session_id.clone(),
            result: tx,
        };
        let release = match self.handle.send(message).await {
            Ok(()) => rx.await.ok(),
            Err(_) => None,
        };
        let cleanup = match release {
            Some(release) => match release? {
                SessionRelease::Nothing => return Ok(false),
                SessionRelease::Cleanup(cleanup) => cleanup,
            },
            // The actor has stopped, but what it retained can still be observed.
            None => self
                .shared
                .cleanup(&session_id)
                .ok_or_else(|| self.stopped_error(&session_id))?,
        };
        match cleanup.wait().await {
            Ok(()) => Ok(true),
            Err(error) => Err(SessionError::DataFusionError(DataFusionError::Shared(
                error,
            ))),
        }
    }

    fn stopped_error(&self, session_id: &str) -> SessionError {
        if self.shared.cleanup(session_id).is_some() {
            SessionError::SessionClosed(session_id.to_string())
        } else {
            SessionError::internal("session manager has stopped")
        }
    }

    /// Shut down the session manager and all resources it owns.
    pub async fn shutdown(&self) -> SessionResult<()> {
        let (tx, rx) = oneshot::channel();
        self.handle
            .send(SessionManagerMessage::Shutdown { result: tx })
            .await?;
        rx.await.map_err(|e| {
            SessionError::internal(format!("failed to shut down session manager: {e}"))
        })?;
        Ok(())
    }
}

pub async fn create_session_manager(
    config: Arc<AppConfig>,
    runtime: RuntimeHandle,
    session_factory_fn: ServerSessionFactoryFn,
    session_timeout: Duration,
    system: &mut ActorSystem,
) -> SessionResult<SessionManager> {
    let session_factory = session_factory_fn(config.clone(), runtime.clone());
    let job_runner_factory = Box::new(ServerSessionJobRunnerFactory::new(
        config.clone(),
        runtime.clone(),
    )) as Box<dyn SessionJobRunnerFactory>;
    let driver_gateway = if matches!(&config.mode, ExecutionMode::Local) {
        None
    } else {
        Some(
            DriverGateway::try_new(DriverGatewayOptions::new(&config))
                .await
                .map_err(|e| {
                    SessionError::internal(format!("failed to create driver gateway: {e}"))
                })?,
        )
    };
    let options = SessionManagerOptions::new(runtime)
        .with_session_timeout(session_timeout)
        .with_options(
            config
                .raw()
                .map_err(|e| SessionError::internal(e.to_string()))?,
        );
    let components = SessionManagerComponents {
        session_factory,
        job_runner_factory,
        driver_gateway,
        event_reporter: global_system_event_reporter().ok_or_else(|| {
            SessionError::internal("telemetry is not initialized for system store")
        })?,
    };
    SessionManager::try_new(options, components, system)
}
