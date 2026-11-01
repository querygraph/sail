use std::panic::{AssertUnwindSafe, catch_unwind};
use std::sync::Arc;

use chrono::Utc;
use datafusion::common::internal_datafusion_err;
use datafusion::prelude::SessionContext;
use fastrace::Span;
use fastrace::collector::SpanContext;
use log::{info, warn};
use sail_common::actor::{ActorAction, ActorContext};
use sail_common::telemetry::SpanAttribute;
use sail_common_datafusion::extension::SessionExtensionAccessor;
use sail_common_datafusion::session::activity::ActivityTracker;
use sail_common_datafusion::session::job::JobService;
use sail_execution::DriverId;
use sail_execution::driver::DriverHandle;
use sail_execution::error::ExecutionResult;
use sail_system_store::SystemEvent;
use tokio::sync::oneshot;
use tokio::time::Instant;

use crate::error::{SessionError, SessionResult};
use crate::session_factory::{ServerSessionInfo, SessionJobRunnerInfo};
use crate::session_manager::SessionManagerMessage;
use crate::session_manager::actor::cleanup::SessionCleanup;
use crate::session_manager::actor::{SessionManagerActor, SessionRelease};
use crate::session_manager::session::{ServerSession, ServerSessionState, SessionOwner};

impl SessionManagerActor {
    pub(super) fn handle_get_driver(
        &self,
        driver_id: DriverId,
        result: oneshot::Sender<ExecutionResult<DriverHandle>>,
    ) -> ActorAction {
        let _ = result.send(self.drivers.get(driver_id));
        ActorAction::Continue
    }

    pub(super) fn handle_get_or_create_session(
        &mut self,
        ctx: &mut ActorContext<Self>,
        session_id: String,
        user_id: String,
        result: oneshot::Sender<SessionResult<SessionContext>>,
    ) -> ActorAction {
        if let Some(session) = self.sessions.get_mut(&session_id) {
            let context = match &mut session.state {
                ServerSessionState::Running { context, .. } => Some(context.clone()),
                ServerSessionState::Creating { waiters, .. } => {
                    waiters.push(result);
                    return ActorAction::Continue;
                }
                ServerSessionState::Deleting { .. }
                | ServerSessionState::DeletionFailed { .. }
                | ServerSessionState::Deleted
                | ServerSessionState::Failed { .. } => None,
            };
            if let Some(context) = context {
                self.schedule_idle_session_probe(ctx, session_id, &context);
                let _ = result.send(Ok(context));
            } else {
                let _ = result.send(Err(session.state.unavailable(&session_id)));
            }
            return ActorAction::Continue;
        }

        // TODO: The session ID is used in various storage paths, so it is assumed to be unique
        //   across all session managers, and it should contain only valid characters for a
        //   path segment. Right now the session ID is generated as a UUID by the Spark client,
        //   so this is true in practice, but we may still want some validation here.
        info!("creating session {session_id}");
        let span = Span::root(
            "SessionManagerActor::create_session_context",
            SpanContext::random(),
        )
        .with_property(|| (SpanAttribute::SESSION_ID, session_id.clone()));
        let _guard = span.set_local_parent();
        let driver_id = match self.driver_id_generator.generate() {
            Ok(driver_id) => driver_id,
            Err(e) => {
                let _ = result.send(Err(SessionError::internal(e.to_string())));
                return ActorAction::Continue;
            }
        };
        let runner = match self.job_runner_factory.create(
            ctx.children_mut(),
            SessionJobRunnerInfo {
                session_id: session_id.clone(),
                driver_id,
                driver_server_port: self.driver_gateway.as_ref().map(|x| x.port()),
                event_reporter: self.event_reporter.clone(),
            },
        ) {
            Ok(runner) => runner,
            Err(e) => {
                let _ = result.send(Err(e));
                return ActorAction::Continue;
            }
        };
        let (runner, driver, activation) = runner.into_parts();
        // Own the runner before anything else can fail or panic.
        let service = Arc::new(JobService::new(runner));
        let registered_driver_id = driver.as_ref().map(|_| driver_id);
        let mut owner = SessionOwner {
            service: service.clone(),
            driver_id: registered_driver_id,
        };
        if let Some(driver) = driver
            && let Err(e) = self.drivers.insert(driver_id, driver)
        {
            // The registered driver with this ID belongs to another session.
            owner.driver_id = None;
            self.fail_session_creation(ctx, session_id, user_id, owner, e.into(), result);
            return ActorAction::Continue;
        }
        let info = ServerSessionInfo {
            session_id: session_id.clone(),
            user_id: user_id.clone(),
            session_manager: ctx.handle().clone(),
            job_service: Some(service),
        };
        let session_factory = &mut self.session_factory;
        let created = catch_unwind(AssertUnwindSafe(|| session_factory.create(info)))
            .unwrap_or_else(|_| Err(internal_datafusion_err!("session factory panicked")));
        let context = match created {
            Ok(context) => context,
            Err(e) => {
                self.fail_session_creation(ctx, session_id, user_id, owner, e.into(), result);
                return ActorAction::Continue;
            }
        };
        // The context is owned here, not by the message that completes the creation.
        self.sessions.insert(
            session_id.clone(),
            ServerSession {
                state: ServerSessionState::Creating {
                    user_id,
                    context,
                    owner,
                    waiters: vec![result],
                },
            },
        );
        let message = move |activation| SessionManagerMessage::CompleteSessionCreation {
            session_id,
            activation,
        };
        if let Some(activation) = activation {
            let session_manager = ctx.handle().clone();
            ctx.spawn(async move {
                let message = message(activation.await);
                if session_manager.send(message).await.is_err() {
                    warn!("failed to complete session creation");
                }
            });
        } else {
            ctx.send(message(Ok(())));
        }
        ActorAction::Continue
    }

    fn fail_session_creation(
        &mut self,
        ctx: &mut ActorContext<Self>,
        session_id: String,
        user_id: String,
        owner: SessionOwner,
        error: SessionError,
        result: oneshot::Sender<SessionResult<SessionContext>>,
    ) {
        let cleanup = self.start_cleanup(ctx, &session_id, owner, None);
        let state = ServerSessionState::Failed {
            cleanup: Some(cleanup),
            released: false,
        };
        let status = state.status().to_string();
        self.sessions
            .insert(session_id.clone(), ServerSession { state });
        self.event_reporter.report(SystemEvent::SessionCreated {
            session_id,
            user_id,
            status,
            created_at: Utc::now(),
        });
        let _ = result.send(Err(error));
    }

    pub(super) fn handle_complete_session_creation(
        &mut self,
        ctx: &mut ActorContext<Self>,
        session_id: String,
        activation: ExecutionResult<()>,
    ) -> ActorAction {
        let Some(session) = self.sessions.get_mut(&session_id) else {
            warn!("session {session_id} disappeared during creation");
            return ActorAction::Continue;
        };
        if !matches!(session.state, ServerSessionState::Creating { .. }) {
            // The session has been closed, and its cleanup owns what the creation left.
            info!("session {session_id} creation is no longer pending");
            return ActorAction::Continue;
        }
        let ServerSessionState::Creating {
            user_id,
            context,
            owner,
            waiters,
        } = std::mem::replace(&mut session.state, ServerSessionState::Deleted)
        else {
            return ActorAction::Continue;
        };
        match activation {
            Ok(()) => {
                session.state = ServerSessionState::Running {
                    context: context.clone(),
                    owner,
                };
                self.event_reporter.report(SystemEvent::SessionCreated {
                    session_id: session_id.clone(),
                    user_id,
                    status: session.state.status().to_string(),
                    created_at: Utc::now(),
                });
                self.schedule_idle_session_probe(ctx, session_id, &context);
                for waiter in waiters {
                    let _ = waiter.send(Ok(context.clone()));
                }
            }
            Err(e) => {
                let cleanup = self.start_cleanup(ctx, &session_id, owner, Some(context));
                let state = ServerSessionState::Failed {
                    cleanup: Some(cleanup),
                    released: false,
                };
                let status = state.status().to_string();
                self.sessions
                    .insert(session_id.clone(), ServerSession { state });
                self.event_reporter.report(SystemEvent::SessionCreated {
                    session_id,
                    user_id,
                    status,
                    created_at: Utc::now(),
                });
                let message = e.to_string();
                for waiter in waiters {
                    let _ = waiter.send(Err(SessionError::internal(message.clone())));
                }
            }
        }
        ActorAction::Continue
    }

    fn schedule_idle_session_probe(
        &self,
        ctx: &mut ActorContext<Self>,
        session_id: String,
        context: &SessionContext,
    ) {
        if let Ok(active_at) = context
            .extension::<ActivityTracker>()
            .and_then(|tracker| tracker.track_activity())
        {
            ctx.send_with_delay(
                SessionManagerMessage::ProbeIdleSession {
                    session_id: session_id.clone(),
                    instant: active_at,
                },
                self.options.session_timeout,
            );
        }
    }

    pub(super) fn handle_probe_idle_session(
        &mut self,
        ctx: &mut ActorContext<Self>,
        session_id: String,
        instant: Instant,
    ) -> ActorAction {
        let idle = self
            .sessions
            .get(&session_id)
            .is_some_and(|session| match &session.state {
                ServerSessionState::Running { context, .. } => context
                    .extension::<ActivityTracker>()
                    .is_ok_and(|tracker| tracker.active_at().is_ok_and(|x| x <= instant)),
                _ => false,
            });
        if idle {
            info!("removing idle session {session_id}");
            self.begin_session_deletion(ctx, &session_id);
        }
        ActorAction::Continue
    }

    pub(super) fn handle_delete_session(
        &mut self,
        ctx: &mut ActorContext<Self>,
        session_id: String,
        result: oneshot::Sender<SessionResult<SessionRelease>>,
    ) -> ActorAction {
        let release = match self.sessions.get_mut(&session_id).map(|x| &mut x.state) {
            // Releasing a session that was never seen is a no-op, and it must not leave a
            // tombstone, since that would prevent the session from being created later.
            None | Some(ServerSessionState::Deleted) => SessionRelease::Nothing,
            Some(
                ServerSessionState::Deleting { cleanup }
                | ServerSessionState::DeletionFailed { cleanup },
            ) => SessionRelease::Cleanup(cleanup.clone()),
            Some(ServerSessionState::Failed { cleanup, released }) => {
                *released = true;
                cleanup
                    .clone()
                    .map_or(SessionRelease::Nothing, SessionRelease::Cleanup)
            }
            Some(ServerSessionState::Creating { .. } | ServerSessionState::Running { .. }) => {
                info!("removing session {session_id}");
                self.begin_session_deletion(ctx, &session_id)
                    .map_or(SessionRelease::Nothing, SessionRelease::Cleanup)
            }
        };
        let _ = result.send(Ok(release));
        ActorAction::Continue
    }

    pub(super) fn handle_check_session(
        &self,
        session_id: String,
        result: oneshot::Sender<SessionResult<()>>,
    ) -> ActorAction {
        let output = match self.sessions.get(&session_id).map(|x| &x.state) {
            None
            | Some(ServerSessionState::Creating { .. } | ServerSessionState::Running { .. }) => {
                Ok(())
            }
            Some(state) => Err(state.unavailable(&session_id)),
        };
        let _ = result.send(output);
        ActorAction::Continue
    }

    pub(super) fn handle_set_session_failure(
        &mut self,
        ctx: &mut ActorContext<Self>,
        session_id: String,
    ) -> ActorAction {
        let Some(session) = self.sessions.get_mut(&session_id) else {
            warn!("session not found: {session_id}");
            return ActorAction::Continue;
        };
        if !matches!(
            session.state,
            ServerSessionState::Creating { .. } | ServerSessionState::Running { .. }
        ) {
            // A session that is already ending keeps its state and the ownership it retains.
            info!("ignoring failure of session {session_id}, which is no longer active");
            return ActorAction::Continue;
        }
        let (owner, context, waiters) =
            match std::mem::replace(&mut session.state, ServerSessionState::Deleted) {
                ServerSessionState::Creating {
                    context,
                    owner,
                    waiters,
                    ..
                } => (owner, context, waiters),
                ServerSessionState::Running { context, owner } => (owner, context, vec![]),
                _ => return ActorAction::Continue,
            };
        let cleanup = self.start_cleanup(ctx, &session_id, owner, Some(context));
        let state = ServerSessionState::Failed {
            cleanup: Some(cleanup),
            released: false,
        };
        let status = state.status().to_string();
        self.sessions
            .insert(session_id.clone(), ServerSession { state });
        self.event_reporter.report(SystemEvent::SessionUpdated {
            session_id,
            status,
            updated_at: Utc::now(),
        });
        for waiter in waiters {
            let _ = waiter.send(Err(SessionError::internal(
                "session failed during creation",
            )));
        }
        ActorAction::Continue
    }

    /// Move a live session out of service and start its cleanup. The state changes first, so no
    /// request can obtain the session once its cleanup has started.
    /// A session being created has its waiters told that the session was closed.
    pub(super) fn begin_session_deletion(
        &mut self,
        ctx: &mut ActorContext<Self>,
        session_id: &str,
    ) -> Option<Arc<SessionCleanup>> {
        let session = self.sessions.get_mut(session_id)?;
        if !matches!(
            session.state,
            ServerSessionState::Creating { .. } | ServerSessionState::Running { .. }
        ) {
            return None;
        }
        let (owner, context, waiters) =
            match std::mem::replace(&mut session.state, ServerSessionState::Deleted) {
                ServerSessionState::Running { context, owner } => (owner, context, vec![]),
                ServerSessionState::Creating {
                    context,
                    owner,
                    waiters,
                    ..
                } => (owner, context, waiters),
                _ => return None,
            };
        let cleanup = self.start_cleanup(ctx, session_id, owner, Some(context));
        let state = ServerSessionState::Deleting {
            cleanup: cleanup.clone(),
        };
        let status = state.status().to_string();
        self.sessions
            .insert(session_id.to_string(), ServerSession { state });
        self.event_reporter.report(SystemEvent::SessionUpdated {
            session_id: session_id.to_string(),
            status,
            updated_at: Utc::now(),
        });
        for waiter in waiters {
            let _ = waiter.send(Err(SessionError::SessionClosed(session_id.to_string())));
        }
        Some(cleanup)
    }

    /// Record the cleanup, then start it. The cleanup runs in a task since it calls foreign code
    /// and waits for operations that may need this actor.
    fn start_cleanup(
        &mut self,
        ctx: &mut ActorContext<Self>,
        session_id: &str,
        owner: SessionOwner,
        context: Option<SessionContext>,
    ) -> Arc<SessionCleanup> {
        let attempt = self.next_cleanup_attempt;
        // The attempt only fences stale completions of the same session.
        self.next_cleanup_attempt = attempt.saturating_add(1);
        let cleanup = SessionCleanup::new(session_id.to_string(), attempt, owner, context);
        self.shared.retain_cleanup(session_id, cleanup.clone());
        let driver = cleanup.driver();
        let handle = ctx.handle().clone();
        let session_id = session_id.to_string();
        ctx.spawn(async move {
            driver.run().await;
            let message = SessionManagerMessage::CompleteSessionCleanup {
                session_id,
                attempt,
            };
            let _ = handle.send(message).await;
        });
        cleanup
    }

    pub(super) fn handle_complete_session_cleanup(
        &mut self,
        session_id: String,
        attempt: u64,
    ) -> ActorAction {
        let Some(session) = self.sessions.get_mut(&session_id) else {
            warn!("session {session_id} disappeared during cleanup");
            return ActorAction::Continue;
        };
        let (cleanup, deleting) = match &session.state {
            ServerSessionState::Deleting { cleanup } => (cleanup.clone(), true),
            ServerSessionState::Failed {
                cleanup: Some(cleanup),
                ..
            } => (cleanup.clone(), false),
            _ => {
                warn!("ignoring stale cleanup completion for session {session_id}");
                return ActorAction::Continue;
            }
        };
        if cleanup.attempt() != attempt {
            warn!("ignoring stale cleanup completion for session {session_id}");
            return ActorAction::Continue;
        }
        let Some(outcome) = cleanup.outcome() else {
            warn!("session {session_id} cleanup has not published its outcome");
            return ActorAction::Continue;
        };
        if let Err(error) = &outcome {
            warn!("{error}");
        }
        if deleting {
            session.state = match outcome {
                Ok(()) => ServerSessionState::Deleted,
                Err(_) => ServerSessionState::DeletionFailed {
                    cleanup: cleanup.clone(),
                },
            };
            self.event_reporter.report(SystemEvent::SessionUpdated {
                session_id,
                status: session.state.status().to_string(),
                updated_at: Utc::now(),
            });
        }
        // A failed cleanup keeps its driver registered, so manager shutdown can still stop it.
        if cleanup.outcome().is_some_and(|x| x.is_ok())
            && let Some(driver_id) = cleanup.driver_id()
        {
            self.drivers.remove(driver_id);
        }
        ActorAction::Continue
    }
}
