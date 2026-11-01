use std::sync::Arc;

use indexmap::IndexMap;
use log::info;
use sail_common::actor::{Actor, ActorAction, ActorContext, ActorHandle};
use sail_execution::driver::{DriverHandle, DriverRegistryAccessor};
use sail_execution::error::{ExecutionError, ExecutionResult};
use sail_execution::{DriverId, IdGenerator};
use sail_system_store::SystemEvent;

use crate::session_manager::actor::{ManagerState, SessionManagerActor};
use crate::session_manager::shared::ManagerShared;
use crate::session_manager::{
    SessionManagerComponents, SessionManagerMessage, SessionManagerOptions,
};

struct SessionDriverRegistry {
    handle: ActorHandle<SessionManagerActor>,
}

#[tonic::async_trait]
impl DriverRegistryAccessor for SessionDriverRegistry {
    async fn get(&self, driver_id: DriverId) -> ExecutionResult<DriverHandle> {
        let (tx, rx) = tokio::sync::oneshot::channel();
        self.handle
            .send(SessionManagerMessage::GetDriver {
                driver_id,
                result: tx,
            })
            .await
            .map_err(ExecutionError::from)?;
        rx.await.map_err(ExecutionError::from)?
    }
}

impl Actor for SessionManagerActor {
    type Message = SessionManagerMessage;
    type Options = (
        SessionManagerOptions,
        SessionManagerComponents,
        Arc<ManagerShared>,
    );

    fn name() -> &'static str {
        "SessionManagerActor"
    }

    fn new(options: Self::Options) -> Self {
        let (options, components, shared) = options;
        let SessionManagerComponents {
            session_factory,
            job_runner_factory,
            driver_gateway,
            event_reporter,
        } = components;
        Self {
            state: ManagerState::Open,
            options,
            session_factory,
            job_runner_factory,
            sessions: IndexMap::new(),
            drivers: Default::default(),
            driver_gateway,
            driver_id_generator: IdGenerator::new(),
            next_cleanup_attempt: 1,
            shared,
            event_reporter,
        }
    }

    async fn start(&mut self, ctx: &mut ActorContext<Self>) {
        for (key, value) in &self.options.options {
            self.event_reporter.report(SystemEvent::OptionCreated {
                key: key.clone(),
                value: value.clone(),
            });
        }
        let Some(driver_gateway) = &mut self.driver_gateway else {
            return;
        };
        driver_gateway.start(Arc::new(SessionDriverRegistry {
            handle: ctx.handle().clone(),
        }));
        info!("driver server is ready on port {}", driver_gateway.port());
    }

    async fn receive(
        &mut self,
        ctx: &mut ActorContext<Self>,
        message: Self::Message,
    ) -> ActorAction {
        match message {
            SessionManagerMessage::GetOrCreateSession {
                session_id,
                user_id,
                result,
            } => self.handle_get_or_create_session(ctx, session_id, user_id, result),
            SessionManagerMessage::CompleteSessionCreation {
                session_id,
                activation,
            } => self.handle_complete_session_creation(ctx, session_id, activation),
            SessionManagerMessage::CompleteSessionCleanup {
                session_id,
                attempt,
            } => self.handle_complete_session_cleanup(session_id, attempt),
            SessionManagerMessage::ProbeIdleSession {
                session_id,
                instant,
            } => self.handle_probe_idle_session(ctx, session_id, instant),
            SessionManagerMessage::DeleteSession { session_id, result } => {
                self.handle_delete_session(ctx, session_id, result)
            }
            SessionManagerMessage::CheckSession { session_id, result } => {
                self.handle_check_session(session_id, result)
            }
            SessionManagerMessage::SetSessionFailure { session_id } => {
                self.handle_set_session_failure(ctx, session_id)
            }
            SessionManagerMessage::GetDriver { driver_id, result } => {
                self.handle_get_driver(driver_id, result)
            }
            SessionManagerMessage::Shutdown => self.handle_shutdown(ctx),
            SessionManagerMessage::CompleteShutdown { outcome } => {
                self.handle_complete_shutdown(outcome)
            }
        }
    }

    async fn stop(self, _: &mut ActorContext<Self>) {
        // Publish from the final stop path, which is reached only through the shutdown.
        // Everything the manager owns was stopped before this, while the actor was serving.
        if let ManagerState::Stopped(outcome) = &self.state {
            self.shared.complete_shutdown(outcome.clone());
        }
    }
}
