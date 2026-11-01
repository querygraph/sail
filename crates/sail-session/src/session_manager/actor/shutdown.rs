use std::mem;
use std::panic::AssertUnwindSafe;
use std::sync::Arc;

use datafusion::common::{DataFusionError, internal_datafusion_err};
use futures::FutureExt;
use futures::future::join_all;
use log::{info, warn};
use sail_common::actor::{ActorAction, ActorContext, ActorSystem};
use sail_execution::DriverId;
use sail_execution::driver::{DriverGateway, DriverHandle};

use crate::session_manager::SessionManagerMessage;
use crate::session_manager::actor::cleanup::{CleanupOutcome, SessionCleanup};
use crate::session_manager::actor::{ManagerState, SessionManagerActor};
use crate::session_manager::session::ServerSessionState;

/// What the coordinator takes over, so that the actor stays free to answer the messages
/// that the shutdown itself depends on.
struct ShutdownCoordinator {
    cleanups: Vec<(String, Arc<SessionCleanup>)>,
    drivers: Vec<(DriverId, DriverHandle)>,
    children: ActorSystem,
    gateway: Option<DriverGateway>,
}

impl ShutdownCoordinator {
    /// Wait for everything the manager owns. Every session has already been asked to stop, each
    /// by its own task, so one blocked session cannot delay the stop request of another.
    async fn run(self) -> CleanupOutcome {
        let Self {
            cleanups,
            drivers,
            mut children,
            gateway,
        } = self;
        let outcomes = join_all(cleanups.iter().map(|(_, x)| x.wait())).await;
        let mut errors = cleanups
            .iter()
            .zip(outcomes)
            .filter_map(|((session_id, _), outcome)| {
                outcome.err().map(|error| {
                    DataFusionError::Context(
                        format!("session {session_id}"),
                        Box::new(DataFusionError::Shared(error)),
                    )
                })
            })
            .collect::<Vec<_>>();
        // Keep the gateway available while drivers stop. Graceful gateway shutdown waits for
        // active task stream connections, which are owned by the drivers.
        let stopped = join_all(drivers.iter().map(|(driver_id, driver)| async move {
            (*driver_id, driver.shutdown_and_wait().await)
        }))
        .await;
        for (driver_id, result) in stopped {
            if let Err(e) = result {
                errors.push(DataFusionError::Context(
                    format!("failed to shut down driver {driver_id}"),
                    Box::new(DataFusionError::External(Box::new(e))),
                ));
            }
        }
        children.join().await;
        if let Some(mut gateway) = gateway {
            gateway.stop().await;
            info!("driver server has stopped");
        }
        match errors.len() {
            0 => Ok(()),
            1 => Err(Arc::new(errors.remove(0))),
            _ => Err(Arc::new(DataFusionError::Collection(errors))),
        }
    }
}

impl SessionManagerActor {
    /// Seal session admission and start shutting down. The actor keeps running so that it can
    /// answer driver lookups, which the gateway needs while it stops, and late lifecycle
    /// messages. It stops when the coordinator reports back.
    pub(super) fn handle_shutdown(&mut self, ctx: &mut ActorContext<Self>) -> ActorAction {
        if !matches!(self.state, ManagerState::Open) {
            return ActorAction::Continue;
        }
        info!("shutting down the session manager");
        self.state = ManagerState::Closing;
        let session_ids = self.sessions.keys().cloned().collect::<Vec<_>>();
        for session_id in &session_ids {
            self.begin_session_deletion(ctx, session_id);
        }
        let cleanups = self
            .sessions
            .iter()
            .filter_map(|(session_id, session)| {
                let cleanup = match &session.state {
                    ServerSessionState::Deleting { cleanup }
                    | ServerSessionState::DeletionFailed { cleanup }
                    | ServerSessionState::Failed {
                        cleanup: Some(cleanup),
                        ..
                    } => cleanup.clone(),
                    ServerSessionState::Creating { .. }
                    | ServerSessionState::Running { .. }
                    | ServerSessionState::Deleted
                    | ServerSessionState::Failed { cleanup: None, .. } => return None,
                };
                Some((session_id.clone(), cleanup))
            })
            .collect();
        let coordinator = ShutdownCoordinator {
            cleanups,
            // The handles stay registered, since the gateway looks drivers up until it stops.
            drivers: self.drivers.handles(),
            children: mem::take(ctx.children_mut()),
            gateway: self.driver_gateway.take(),
        };
        let handle = ctx.handle().clone();
        ctx.spawn(async move {
            let outcome = AssertUnwindSafe(coordinator.run())
                .catch_unwind()
                .await
                .unwrap_or_else(|_| {
                    Err(Arc::new(internal_datafusion_err!(
                        "session manager shutdown panicked"
                    )))
                });
            let message = SessionManagerMessage::CompleteShutdown { outcome };
            if handle.send(message).await.is_err() {
                warn!("failed to complete session manager shutdown");
            }
        });
        ActorAction::Continue
    }

    pub(super) fn handle_complete_shutdown(&mut self, outcome: CleanupOutcome) -> ActorAction {
        self.state = ManagerState::Stopped(outcome);
        ActorAction::Stop
    }
}
