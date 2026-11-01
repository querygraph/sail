//! Session deletion through the real actor, with a scripted job runner.
//!
//! Ordering comes from gates and channels. Timeouts only bound a test that fails.

use std::collections::HashMap;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use datafusion::arrow::datatypes::Schema;
use datafusion::common::{DataFusionError, Result, internal_datafusion_err};
use datafusion::execution::SendableRecordBatchStream;
use datafusion::execution::object_store::ObjectStoreUrl;
use datafusion::physical_expr::Partitioning;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::prelude::{SessionConfig, SessionContext};
use futures::FutureExt;
use futures::future::{BoxFuture, join_all};
use object_store::path::Path;
use opentelemetry::logs::{AnyValue, LoggerProvider};
use opentelemetry_sdk::logs::{InMemoryLogExporter, SdkLoggerProvider};
use sail_cache::remote_checkpoint::{RemoteCheckpointDescriptor, RemoteCheckpointRegistry};
use sail_common::actor::ActorSystem;
use sail_common::runtime::RuntimeHandle;
use sail_common_datafusion::error::{
    CommitStateUnknownError, CommonErrorCause, PythonErrorCause, PythonErrorCauseExtractor,
};
use sail_common_datafusion::extension::SessionExtensionAccessor;
use sail_common_datafusion::session::activity::ActivityTracker;
use sail_common_datafusion::session::job::JobRunner;
use sail_execution::error::{ExecutionError, ExecutionResult};
use sail_telemetry::events::SystemEventReporter;
use tokio::runtime::Handle;
use tokio::sync::{Notify, oneshot};
use tokio::time::timeout;

use crate::error::SessionError;
use crate::session_factory::{
    ServerSessionInfo, SessionFactory, SessionJobRunner, SessionJobRunnerFactory,
    SessionJobRunnerInfo,
};
use crate::session_manager::{
    ServerStopError, SessionManager, SessionManagerComponents, SessionManagerMessage,
    SessionManagerOptions, serve_until_stopped,
};

type TestResult = std::result::Result<(), Box<dyn std::error::Error>>;

const BOUND: Duration = Duration::from_secs(10);

/// How the fake components of one session behave.
#[derive(Default)]
struct Script {
    stop_error: Option<Arc<DataFusionError>>,
    stop_panics: bool,
    hold_stop: Option<oneshot::Receiver<()>>,
    hold_activation: Option<oneshot::Receiver<ExecutionResult<()>>>,
    factory_error: bool,
    factory_panics: bool,
}

#[derive(Default)]
struct Harness {
    scripts: Mutex<HashMap<String, Script>>,
    stops: Mutex<HashMap<String, Arc<AtomicUsize>>>,
    stopped: Notify,
}

impl Harness {
    fn script(&self, session_id: &str, script: Script) {
        self.scripts
            .lock()
            .unwrap()
            .insert(session_id.to_string(), script);
    }

    fn take_script(&self, session_id: &str) -> Script {
        self.scripts
            .lock()
            .unwrap()
            .remove(session_id)
            .unwrap_or_default()
    }

    fn stop_counter(&self, session_id: &str) -> Arc<AtomicUsize> {
        self.stops
            .lock()
            .unwrap()
            .entry(session_id.to_string())
            .or_default()
            .clone()
    }

    fn stop_calls(&self, session_id: &str) -> usize {
        self.stop_counter(session_id).load(Ordering::SeqCst)
    }

    async fn stop_started(&self, session_id: &str) {
        let wait = async {
            loop {
                let notified = self.stopped.notified();
                if self.stop_calls(session_id) > 0 {
                    return;
                }
                notified.await;
            }
        };
        assert!(
            timeout(BOUND, wait).await.is_ok(),
            "{session_id} not stopped"
        );
    }
}

struct ScriptedRunner {
    harness: Arc<Harness>,
    calls: Arc<AtomicUsize>,
    error: Option<Arc<DataFusionError>>,
    panics: bool,
    hold: Mutex<Option<oneshot::Receiver<()>>>,
}

#[async_trait::async_trait]
impl JobRunner for ScriptedRunner {
    async fn execute(
        &self,
        _: &SessionContext,
        _: Arc<dyn ExecutionPlan>,
    ) -> Result<SendableRecordBatchStream> {
        Err(internal_datafusion_err!("unused test execution"))
    }

    fn explain(&self, _: Arc<dyn ExecutionPlan>) -> Result<String> {
        Err(internal_datafusion_err!("unused test explanation"))
    }

    async fn stop(&self) -> Result<()> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        self.harness.stopped.notify_waiters();
        let hold = self.hold.lock().unwrap().take();
        if let Some(hold) = hold {
            let _ = hold.await;
        }
        assert!(!self.panics, "scripted stop panic");
        match &self.error {
            Some(error) => Err(DataFusionError::Shared(error.clone())),
            None => Ok(()),
        }
    }
}

struct ScriptedRunnerFactory {
    harness: Arc<Harness>,
}

impl SessionJobRunnerFactory for ScriptedRunnerFactory {
    fn create(
        &mut self,
        _: &mut ActorSystem,
        info: SessionJobRunnerInfo,
    ) -> crate::error::SessionResult<SessionJobRunner> {
        let mut scripts = self.harness.scripts.lock().unwrap();
        let script = scripts.entry(info.session_id.clone()).or_default();
        let runner = ScriptedRunner {
            harness: self.harness.clone(),
            calls: self.harness.stop_counter(&info.session_id),
            error: script.stop_error.clone(),
            panics: script.stop_panics,
            hold: Mutex::new(script.hold_stop.take()),
        };
        let activation = script.hold_activation.take().map(|hold| {
            async move { hold.await.unwrap_or(Ok(())) }.boxed()
                as BoxFuture<'static, ExecutionResult<()>>
        });
        Ok(SessionJobRunner::with_activation(
            Box::new(runner),
            activation,
        ))
    }
}

struct ScriptedSessionFactory {
    harness: Arc<Harness>,
}

impl SessionFactory<ServerSessionInfo> for ScriptedSessionFactory {
    fn create(&mut self, mut info: ServerSessionInfo) -> Result<SessionContext> {
        let script = self.harness.take_script(&info.session_id);
        // Like a real factory, consume the runner's service before failing.
        let service = info
            .job_service
            .take()
            .ok_or_else(|| internal_datafusion_err!("missing job service"))?;
        assert!(!script.factory_panics, "scripted factory panic");
        if script.factory_error {
            return Err(DataFusionError::Plan("scripted factory error".into()));
        }
        let config = SessionConfig::new()
            .with_extension(service)
            .with_extension(Arc::new(ActivityTracker::new()))
            .with_extension(Arc::new(RemoteCheckpointRegistry::new(
                None,
                info.session_id.clone(),
            )));
        Ok(SessionContext::new_with_config(config))
    }
}

struct Fixture {
    system: ActorSystem,
    manager: SessionManager,
    harness: Arc<Harness>,
    events: InMemoryLogExporter,
    // Retained so that events are exported.
    _provider: SdkLoggerProvider,
}

impl Fixture {
    fn new(harness: Arc<Harness>, session_timeout: Duration) -> Self {
        let events = InMemoryLogExporter::default();
        let provider = SdkLoggerProvider::builder()
            .with_simple_exporter(events.clone())
            .build();
        let handle = RuntimeHandle::new(Handle::current(), Handle::current());
        let options = SessionManagerOptions::new(handle).with_session_timeout(session_timeout);
        let components = SessionManagerComponents {
            session_factory: Box::new(ScriptedSessionFactory {
                harness: harness.clone(),
            }),
            job_runner_factory: Box::new(ScriptedRunnerFactory {
                harness: harness.clone(),
            }),
            driver_gateway: None,
            event_reporter: SystemEventReporter::new(provider.logger("lifecycle-test")),
        };
        let mut system = ActorSystem::new();
        let manager = SessionManager::try_new(options, components, &mut system).unwrap();
        Self {
            system,
            manager,
            harness,
            events,
            _provider: provider,
        }
    }

    fn with_scripts(scripts: Vec<(&str, Script)>) -> Self {
        let harness = Arc::new(Harness::default());
        for (session_id, script) in scripts {
            harness.script(session_id, script);
        }
        Self::new(harness, Duration::MAX)
    }

    async fn session(&self, session_id: &str) -> Result<SessionContext, SessionError> {
        self.manager
            .get_or_create_session_context(session_id.to_string(), "user".to_string())
            .await
    }

    fn session_owned(
        &self,
        session_id: String,
    ) -> impl Future<Output = Result<SessionContext, SessionError>> + use<> {
        let manager = self.manager.clone();
        async move {
            manager
                .get_or_create_session_context(session_id, "user".to_string())
                .await
        }
    }

    async fn send(&self, message: SessionManagerMessage) {
        assert!(self.manager.handle.send(message).await.is_ok());
    }

    fn delete(&self, session_id: &str) -> tokio::task::JoinHandle<Result<bool, SessionError>> {
        let manager = self.manager.clone();
        let session_id = session_id.to_string();
        tokio::spawn(async move { manager.delete_session(session_id).await })
    }

    fn statuses(&self, session_id: &str) -> Vec<String> {
        self.events
            .get_emitted_logs()
            .unwrap()
            .iter()
            .filter_map(|log| match log.record.body() {
                Some(AnyValue::String(body)) => {
                    serde_json::from_str::<serde_json::Value>(body.as_str()).ok()
                }
                _ => None,
            })
            .filter_map(|event| {
                (event.get("session_id")?.as_str()? == session_id)
                    .then(|| event.get("status")?.as_str().map(str::to_string))?
            })
            .collect()
    }
}

fn is_closed<T>(result: &Result<T, SessionError>, session_id: &str) -> bool {
    matches!(result, Err(SessionError::SessionClosed(id)) if id == session_id)
}

fn unknown_commit() -> Arc<DataFusionError> {
    Arc::new(DataFusionError::External(Box::new(
        CommitStateUnknownError("stop outcome unknown".into()),
    )))
}

struct NoPython;

impl PythonErrorCauseExtractor for NoPython {
    fn extract(_: &(dyn std::error::Error + 'static)) -> Option<PythonErrorCause> {
        None
    }
}

fn descriptor(name: &str) -> Result<RemoteCheckpointDescriptor> {
    let schema = Arc::new(Schema::empty());
    Ok(RemoteCheckpointDescriptor {
        relation_id: name.into(),
        object_store_url: ObjectStoreUrl::parse("memory://")?,
        prefix: Path::from("checkpoint"),
        logical_schema: schema.clone(),
        storage_schema: schema,
        output_partitioning: Partitioning::UnknownPartitioning(1),
        output_ordering: None,
        partitions: vec![],
    })
}

#[tokio::test]
async fn release_is_acknowledged_after_cleanup_and_concurrent_callers_share_it() -> TestResult {
    let (release, hold_stop) = oneshot::channel();
    let fixture = Fixture::with_scripts(vec![(
        "a",
        Script {
            hold_stop: Some(hold_stop),
            ..Default::default()
        },
    )]);
    fixture.session("a").await?;
    let first = fixture.delete("a");
    let second = fixture.delete("a");
    fixture.harness.stop_started("a").await;
    // The session left Running before its cleanup finished.
    assert!(is_closed(&fixture.session("a").await, "a"));
    assert!(!first.is_finished() && !second.is_finished());
    // The manager is not blocked by the cleanup of another session.
    timeout(BOUND, fixture.session("other")).await??;
    assert_eq!(fixture.statuses("a"), ["RUNNING", "DELETING"]);

    release.send(()).map_err(|_| "stop was abandoned")?;
    assert!(timeout(BOUND, first).await???);
    assert!(timeout(BOUND, second).await???);
    assert_eq!(fixture.harness.stop_calls("a"), 1);
    // The tombstone acknowledges a repeated release, without any further cleanup.
    assert!(!fixture.delete("a").await??);
    assert_eq!(fixture.harness.stop_calls("a"), 1);
    assert!(is_closed(&fixture.session("a").await, "a"));
    Ok(())
}

#[tokio::test]
async fn deleted_status_is_only_reported_after_the_cleanup_succeeded() -> TestResult {
    let fixture = Fixture::with_scripts(vec![]);
    fixture.session("a").await?;
    assert!(fixture.delete("a").await??);
    // The status is reported by the actor once the outcome has been published,
    // and this request is ordered after that notification in the worst case.
    let wait = async {
        loop {
            if fixture.statuses("a").last().map(String::as_str) == Some("DELETED") {
                return;
            }
            tokio::task::yield_now().await;
        }
    };
    timeout(BOUND, wait).await?;
    assert_eq!(fixture.statuses("a"), ["RUNNING", "DELETING", "DELETED"]);
    Ok(())
}

#[tokio::test]
async fn failed_cleanup_is_reported_to_every_caller_and_retains_its_owner() -> TestResult {
    let failure = unknown_commit();
    let (release, hold_stop) = oneshot::channel();
    let fixture = Fixture::with_scripts(vec![(
        "a",
        Script {
            stop_error: Some(failure.clone()),
            hold_stop: Some(hold_stop),
            ..Default::default()
        },
    )]);
    let context = fixture.session("a").await?;
    let checkpoints = context.extension::<RemoteCheckpointRegistry>()?;
    checkpoints.insert(descriptor("existing")?)?;
    drop(context);
    let first = fixture.delete("a");
    let second = fixture.delete("a");
    fixture.harness.stop_started("a").await;
    release.send(()).map_err(|_| "stop was abandoned")?;
    for caller in [first, second] {
        let error = timeout(BOUND, caller)
            .await??
            .err()
            .ok_or("cleanup succeeded")?;
        let SessionError::DataFusionError(error) = error else {
            return Err("cleanup error lost its type".into());
        };
        let cause = CommonErrorCause::new::<NoPython>(&error);
        assert!(matches!(cause, CommonErrorCause::CommitStateUnknown(_)));
        assert!(!cause.is_retryable());
    }
    // A repeated release reports the same failure. It neither retries nor resurrects.
    let error = fixture.delete("a").await?.err().ok_or("repeat succeeded")?;
    assert!(error.to_string().contains("stop outcome unknown"));
    assert_eq!(fixture.harness.stop_calls("a"), 1);
    assert!(is_closed(&fixture.session("a").await, "a"));
    // The checkpoint metadata was not cleared, and the cleanup record keeps it alive.
    assert!(checkpoints.get("existing")?.is_some());
    assert_eq!(Arc::strong_count(&checkpoints), 2);
    let wait = async {
        loop {
            if fixture.statuses("a").last().map(String::as_str) == Some("DELETION_FAILED") {
                return;
            }
            tokio::task::yield_now().await;
        }
    };
    timeout(BOUND, wait).await?;
    Ok(())
}

#[tokio::test]
async fn a_panicking_cleanup_reports_a_failure() -> TestResult {
    let fixture = Fixture::with_scripts(vec![(
        "a",
        Script {
            stop_panics: true,
            ..Default::default()
        },
    )]);
    fixture.session("a").await?;
    let error = fixture
        .delete("a")
        .await?
        .err()
        .ok_or("cleanup succeeded")?;
    assert!(error.to_string().contains("panicked"), "{error}");
    // The actor is still serving.
    fixture.session("b").await?;
    Ok(())
}

#[tokio::test]
async fn a_dropped_caller_does_not_cancel_the_cleanup() -> TestResult {
    let (release, hold_stop) = oneshot::channel();
    let fixture = Fixture::with_scripts(vec![(
        "a",
        Script {
            hold_stop: Some(hold_stop),
            ..Default::default()
        },
    )]);
    fixture.session("a").await?;
    let caller = fixture.delete("a");
    fixture.harness.stop_started("a").await;
    caller.abort();
    assert!(caller.await.is_err_and(|e| e.is_cancelled()));
    // A later caller finds the cleanup still running, and sees its result.
    let later = fixture.delete("a");
    tokio::task::yield_now().await;
    assert!(!later.is_finished());
    release.send(()).map_err(|_| "stop was abandoned")?;
    assert!(timeout(BOUND, later).await???);
    assert_eq!(fixture.harness.stop_calls("a"), 1);
    Ok(())
}

#[tokio::test]
async fn releasing_an_unknown_session_leaves_no_tombstone() -> TestResult {
    let fixture = Fixture::with_scripts(vec![]);
    assert!(!fixture.delete("a").await??);
    assert!(!fixture.delete("a").await??);
    fixture.manager.check_session("a".to_string()).await?;
    fixture.session("a").await?;
    assert!(fixture.delete("a").await??);
    Ok(())
}

#[tokio::test]
async fn check_session_reports_a_closed_session_without_creating_one() -> TestResult {
    let fixture = Fixture::with_scripts(vec![]);
    fixture.manager.check_session("a".to_string()).await?;
    assert_eq!(fixture.harness.stop_calls("a"), 0);
    assert!(fixture.delete("a").await.is_ok());
    fixture.session("b").await?;
    fixture.delete("b").await??;
    let result = fixture.manager.check_session("b".to_string()).await;
    assert!(is_closed(&result, "b"));
    Ok(())
}

#[tokio::test]
async fn delete_during_creating_owns_the_context_and_a_late_completion_is_ignored() -> TestResult {
    let (activate, hold_activation) = oneshot::channel();
    let fixture = Fixture::with_scripts(vec![(
        "a",
        Script {
            hold_activation: Some(hold_activation),
            ..Default::default()
        },
    )]);
    let waiter = {
        let manager = fixture.manager.clone();
        tokio::spawn(async move {
            manager
                .get_or_create_session_context("a".to_string(), "user".to_string())
                .await
        })
    };
    // This is served after the session has entered Creating.
    let wait = async {
        while fixture.harness.scripts.lock().unwrap().contains_key("a") {
            tokio::task::yield_now().await;
        }
    };
    timeout(BOUND, wait).await?;
    assert!(fixture.delete("a").await??);
    assert!(is_closed(
        &timeout(BOUND, waiter).await?.map_err(|_| "join")?,
        "a"
    ));
    assert_eq!(fixture.harness.stop_calls("a"), 1);
    // The activation finishes after the session was deleted: it must not come back.
    activate
        .send(Ok(()))
        .map_err(|_| "activation was abandoned")?;
    fixture.delete("a").await??;
    assert!(is_closed(&fixture.session("a").await, "a"));
    assert_eq!(fixture.harness.stop_calls("a"), 1);
    Ok(())
}

#[tokio::test]
async fn failed_activation_stops_the_runner_and_keeps_the_session_unavailable() -> TestResult {
    let (activate, hold_activation) = oneshot::channel();
    let fixture = Fixture::with_scripts(vec![(
        "a",
        Script {
            hold_activation: Some(hold_activation),
            ..Default::default()
        },
    )]);
    activate
        .send(Err(ExecutionError::InternalError("no workers".into())))
        .map_err(|_| "activation was abandoned")?;
    let error = fixture.session("a").await.err().ok_or("session created")?;
    assert!(error.to_string().contains("no workers"), "{error}");
    fixture.harness.stop_started("a").await;
    assert!(matches!(
        fixture.session("a").await,
        Err(SessionError::InvalidArgument(_))
    ));
    // Release observes the cleanup of what the failed session had acquired.
    assert!(fixture.delete("a").await??);
    assert!(is_closed(&fixture.session("a").await, "a"));
    assert_eq!(fixture.harness.stop_calls("a"), 1);
    Ok(())
}

#[tokio::test]
async fn a_failed_or_panicking_factory_does_not_lose_the_runner() -> TestResult {
    let fixture = Fixture::with_scripts(vec![
        (
            "failed",
            Script {
                factory_error: true,
                ..Default::default()
            },
        ),
        (
            "panicked",
            Script {
                factory_panics: true,
                ..Default::default()
            },
        ),
    ]);
    for session_id in ["failed", "panicked"] {
        fixture.session(session_id).await.err().ok_or("created")?;
        fixture.harness.stop_started(session_id).await;
        assert!(fixture.delete(session_id).await??);
        assert_eq!(fixture.harness.stop_calls(session_id), 1);
    }
    fixture.session("next").await?;
    Ok(())
}

#[tokio::test]
async fn session_failure_is_cleaned_up_once_and_does_not_overwrite_a_deletion() -> TestResult {
    let (release, hold_stop) = oneshot::channel();
    let fixture = Fixture::with_scripts(vec![(
        "deleting",
        Script {
            hold_stop: Some(hold_stop),
            ..Default::default()
        },
    )]);
    fixture.session("running").await?;
    fixture.session("deleting").await?;
    let failure = |session_id: &str| SessionManagerMessage::SetSessionFailure {
        session_id: session_id.to_string(),
    };

    fixture.send(failure("running")).await;
    fixture.harness.stop_started("running").await;
    assert!(matches!(
        fixture.session("running").await,
        Err(SessionError::InvalidArgument(_))
    ));
    assert!(fixture.delete("running").await??);
    assert_eq!(fixture.harness.stop_calls("running"), 1);

    let deletion = fixture.delete("deleting");
    fixture.harness.stop_started("deleting").await;
    fixture.send(failure("deleting")).await;
    fixture.session("barrier").await?;
    assert!(is_closed(&fixture.session("deleting").await, "deleting"));
    assert!(!deletion.is_finished());
    release.send(()).map_err(|_| "stop was abandoned")?;
    assert!(timeout(BOUND, deletion).await???);
    // Neither is a deleted session overwritten.
    fixture.send(failure("deleting")).await;
    fixture.session("barrier").await?;
    assert!(!fixture.delete("deleting").await??);
    assert!(is_closed(&fixture.session("deleting").await, "deleting"));
    Ok(())
}

#[tokio::test]
async fn a_stale_cleanup_completion_does_not_change_the_session() -> TestResult {
    let (release, hold_stop) = oneshot::channel();
    let fixture = Fixture::with_scripts(vec![(
        "a",
        Script {
            hold_stop: Some(hold_stop),
            ..Default::default()
        },
    )]);
    fixture.session("a").await?;
    let deletion = fixture.delete("a");
    fixture.harness.stop_started("a").await;
    fixture
        .send(SessionManagerMessage::CompleteSessionCleanup {
            session_id: "a".to_string(),
            attempt: u64::MAX,
        })
        .await;
    fixture.session("barrier").await?;
    assert!(!deletion.is_finished());
    assert_eq!(
        fixture.statuses("a").last().map(String::as_str),
        Some("DELETING")
    );
    release.send(()).map_err(|_| "stop was abandoned")?;
    assert!(timeout(BOUND, deletion).await???);
    Ok(())
}

#[tokio::test]
async fn an_explicit_delete_joins_an_idle_deletion() -> TestResult {
    let (release, hold_stop) = oneshot::channel();
    let harness = Arc::new(Harness::default());
    harness.script(
        "a",
        Script {
            hold_stop: Some(hold_stop),
            ..Default::default()
        },
    );
    let fixture = Fixture::new(harness, Duration::from_millis(1));
    fixture.session("a").await?;
    fixture.harness.stop_started("a").await;
    let deletion = fixture.delete("a");
    assert!(is_closed(&fixture.session("a").await, "a"));
    assert!(!deletion.is_finished());
    release.send(()).map_err(|_| "stop was abandoned")?;
    assert!(timeout(BOUND, deletion).await???);
    assert_eq!(fixture.harness.stop_calls("a"), 1);
    Ok(())
}

#[tokio::test]
async fn destroying_the_actor_is_an_interruption_and_never_a_success() -> TestResult {
    let (_release, hold_stop) = oneshot::channel();
    let fixture = Fixture::with_scripts(vec![(
        "a",
        Script {
            hold_stop: Some(hold_stop),
            ..Default::default()
        },
    )]);
    fixture.session("a").await?;
    let deletion = fixture.delete("a");
    fixture.harness.stop_started("a").await;
    let Fixture {
        system,
        manager,
        _provider,
        ..
    } = fixture;
    drop(system);
    let error = timeout(BOUND, deletion)
        .await??
        .err()
        .ok_or("reported success")?;
    assert!(error.to_string().contains("interrupted"), "{error}");
    // The retained record stays reachable through the manager handle.
    let error = manager
        .delete_session("a".to_string())
        .await
        .err()
        .ok_or("reported success")?;
    assert!(error.to_string().contains("interrupted"), "{error}");
    let lookup = manager
        .get_or_create_session_context("a".to_string(), "user".to_string())
        .await;
    assert!(is_closed(&lookup, "a"));
    Ok(())
}

fn shutdown(fixture: &Fixture) -> tokio::task::JoinHandle<Result<(), SessionError>> {
    let manager = fixture.manager.clone();
    tokio::spawn(async move { manager.shutdown().await })
}

#[tokio::test]
async fn shutdown_cleans_every_session_concurrently_and_aggregates_failures() -> TestResult {
    let failure = unknown_commit();
    let (release, hold_stop) = oneshot::channel();
    let fixture = Fixture::with_scripts(vec![
        (
            "blocked",
            Script {
                hold_stop: Some(hold_stop),
                ..Default::default()
            },
        ),
        (
            "failing",
            Script {
                stop_error: Some(failure.clone()),
                ..Default::default()
            },
        ),
        (
            "panicking",
            Script {
                stop_panics: true,
                ..Default::default()
            },
        ),
    ]);
    for session_id in ["blocked", "failing", "panicking", "clean"] {
        fixture.session(session_id).await?;
    }
    let Fixture {
        mut system,
        manager,
        harness,
        _provider,
        ..
    } = fixture;
    let observer = {
        let manager = manager.clone();
        tokio::spawn(async move { manager.shutdown().await })
    };
    // Every session is stopped, while the first one is still blocked.
    for session_id in ["blocked", "failing", "panicking", "clean"] {
        harness.stop_started(session_id).await;
    }
    assert!(!observer.is_finished());
    release.send(()).map_err(|_| "stop was abandoned")?;
    let error = timeout(BOUND, observer)
        .await??
        .err()
        .ok_or("shutdown succeeded")?;
    let SessionError::DataFusionError(error) = error else {
        return Err("shutdown error lost its type".into());
    };
    let DataFusionError::Shared(error) = &error else {
        return Err("shutdown error is not the shared outcome".into());
    };
    let DataFusionError::Collection(errors) = error.as_ref() else {
        return Err("sibling failures were not aggregated".into());
    };
    let errors = errors.iter().map(|x| x.to_string()).collect::<Vec<_>>();
    assert_eq!(errors.len(), 2, "{errors:?}");
    assert!(errors.iter().any(|x| x.contains("failing")), "{errors:?}");
    assert!(errors.iter().any(|x| x.contains("panicking")), "{errors:?}");
    // The nonretryable classification survives the aggregation.
    let cause = CommonErrorCause::new::<NoPython>(error);
    assert!(!cause.is_retryable());
    timeout(BOUND, system.join()).await?;
    Ok(())
}

#[tokio::test]
async fn shutdown_racing_a_delete_shares_its_cleanup() -> TestResult {
    let (release, hold_stop) = oneshot::channel();
    let fixture = Fixture::with_scripts(vec![(
        "a",
        Script {
            hold_stop: Some(hold_stop),
            ..Default::default()
        },
    )]);
    fixture.session("a").await?;
    let deletion = fixture.delete("a");
    fixture.harness.stop_started("a").await;
    let observer = shutdown(&fixture);
    let late = {
        // This is ordered after Shutdown only if the manager is closing, which either way
        // joins the same cleanup rather than starting another one.
        fixture.delete("a")
    };
    tokio::task::yield_now().await;
    assert!(!observer.is_finished());
    release.send(()).map_err(|_| "stop was abandoned")?;
    assert!(timeout(BOUND, deletion).await???);
    assert!(timeout(BOUND, late).await???);
    timeout(BOUND, observer).await???;
    assert_eq!(fixture.harness.stop_calls("a"), 1);
    Ok(())
}

#[tokio::test]
async fn shutdown_owns_a_session_in_creating() -> TestResult {
    let (activate, hold_activation) = oneshot::channel();
    let fixture = Fixture::with_scripts(vec![(
        "a",
        Script {
            hold_activation: Some(hold_activation),
            ..Default::default()
        },
    )]);
    let waiter = {
        let manager = fixture.manager.clone();
        tokio::spawn(async move {
            manager
                .get_or_create_session_context("a".to_string(), "user".to_string())
                .await
        })
    };
    let wait = async {
        while fixture.harness.scripts.lock().unwrap().contains_key("a") {
            tokio::task::yield_now().await;
        }
    };
    timeout(BOUND, wait).await?;
    // The activation is still held, and it never finishes before the shutdown does.
    timeout(BOUND, shutdown(&fixture)).await???;
    assert_eq!(fixture.harness.stop_calls("a"), 1);
    assert!(timeout(BOUND, waiter).await?.map_err(|_| "join")?.is_err());
    let _ = activate.send(Ok(()));
    assert!(is_closed(&fixture.session("a").await, "a"));
    Ok(())
}

#[tokio::test]
async fn closing_seals_admission_and_keeps_serving_the_messages_shutdown_needs() -> TestResult {
    let (release, hold_stop) = oneshot::channel();
    let fixture = Fixture::with_scripts(vec![(
        "a",
        Script {
            hold_stop: Some(hold_stop),
            ..Default::default()
        },
    )]);
    fixture.session("a").await?;
    let observer = shutdown(&fixture);
    fixture.harness.stop_started("a").await;

    // An existing session is not available, and neither is a new one.
    assert!(is_closed(&fixture.session("a").await, "a"));
    let error = fixture
        .session("new")
        .await
        .err()
        .ok_or("session created")?;
    assert!(error.to_string().contains("shutting down"), "{error}");

    // A driver lookup is answered. The gateway depends on this while it stops: a lookup that
    // was queued behind the shutdown would otherwise hold a reply that the gateway waits for.
    let (tx, rx) = oneshot::channel();
    fixture
        .send(SessionManagerMessage::GetDriver {
            driver_id: 1.into(),
            result: tx,
        })
        .await;
    let reply = timeout(BOUND, rx).await??;
    assert!(matches!(reply, Err(ExecutionError::InvalidArgument(_))));

    // More messages than the mailbox holds are served while the shutdown is in progress.
    let lookups = (0..32).map(|i| fixture.session_owned(format!("late-{i}")));
    for result in timeout(BOUND, join_all(lookups)).await? {
        assert!(result.is_err());
    }
    assert!(!observer.is_finished());
    release.send(()).map_err(|_| "stop was abandoned")?;
    timeout(BOUND, observer).await???;
    Ok(())
}

#[tokio::test]
async fn every_shutdown_caller_observes_the_same_result() -> TestResult {
    let failure = unknown_commit();
    let (release, hold_stop) = oneshot::channel();
    let fixture = Fixture::with_scripts(vec![(
        "a",
        Script {
            stop_error: Some(failure),
            hold_stop: Some(hold_stop),
            ..Default::default()
        },
    )]);
    fixture.session("a").await?;
    let first = shutdown(&fixture);
    fixture.harness.stop_started("a").await;
    // The caller that started the shutdown goes away, which does not affect it.
    first.abort();
    assert!(first.await.is_err_and(|e| e.is_cancelled()));
    let second = shutdown(&fixture);
    tokio::task::yield_now().await;
    assert!(!second.is_finished());
    release.send(()).map_err(|_| "stop was abandoned")?;
    let shared = |error: SessionError| match error {
        SessionError::DataFusionError(DataFusionError::Shared(error)) => Ok(error),
        _ => Err("shutdown error is not the shared outcome"),
    };
    let second = shared(timeout(BOUND, second).await??.err().ok_or("succeeded")?)?;
    let Fixture {
        mut system,
        manager,
        _provider,
        ..
    } = fixture;
    timeout(BOUND, system.join()).await?;
    // The mailbox has closed by now.
    let third = manager.shutdown().await.err().ok_or("succeeded")?;
    assert!(Arc::ptr_eq(&second, &shared(third)?));
    // What failed stays reachable, and a release reports it.
    let error = manager
        .delete_session("a".to_string())
        .await
        .err()
        .ok_or("succeeded")?;
    assert!(
        error.to_string().contains("stop outcome unknown"),
        "{error}"
    );
    Ok(())
}

#[tokio::test]
async fn a_shutdown_canceled_while_the_mailbox_is_full_can_be_requested_again() -> TestResult {
    let fixture = Fixture::with_scripts(vec![]);
    fixture.session("a").await?;
    // On this single thread, the actor cannot run while the mailbox is being filled.
    let mut filling = vec![];
    loop {
        let (tx, rx) = oneshot::channel();
        let message = SessionManagerMessage::CheckSession {
            session_id: "a".to_string(),
            result: tx,
        };
        if fixture
            .manager
            .handle
            .send(message)
            .now_or_never()
            .is_none()
        {
            break;
        }
        filling.push(rx);
    }
    let mut canceled = Box::pin(fixture.manager.shutdown());
    assert!(futures::poll!(canceled.as_mut()).is_pending());
    drop(canceled);
    assert!(join_all(filling).await.iter().all(Result::is_ok));
    // Nothing was accepted, so the session is still running, and the request can be repeated.
    fixture.session("a").await?;
    assert_eq!(fixture.harness.stop_calls("a"), 0);
    timeout(BOUND, fixture.manager.shutdown()).await??;
    assert_eq!(fixture.harness.stop_calls("a"), 1);
    Ok(())
}

#[tokio::test]
async fn shutdown_does_not_wait_for_a_scheduled_idle_probe() -> TestResult {
    let harness = Arc::new(Harness::default());
    let fixture = Fixture::new(harness, Duration::from_secs(24 * 60 * 60));
    fixture.session("a").await?;
    timeout(BOUND, fixture.manager.shutdown()).await??;
    let Fixture { mut system, .. } = fixture;
    timeout(BOUND, system.join()).await?;
    Ok(())
}

#[tokio::test]
async fn destroying_the_actor_during_shutdown_is_not_a_success() -> TestResult {
    let (_release, hold_stop) = oneshot::channel();
    let fixture = Fixture::with_scripts(vec![(
        "a",
        Script {
            hold_stop: Some(hold_stop),
            ..Default::default()
        },
    )]);
    fixture.session("a").await?;
    let observer = shutdown(&fixture);
    fixture.harness.stop_started("a").await;
    let Fixture {
        system,
        manager,
        _provider,
        ..
    } = fixture;
    drop(system);
    let error = timeout(BOUND, observer).await??.err().ok_or("succeeded")?;
    assert!(error.to_string().contains("interrupted"), "{error}");
    let error = manager.shutdown().await.err().ok_or("succeeded")?;
    assert!(error.to_string().contains("interrupted"), "{error}");
    Ok(())
}

#[derive(Debug, thiserror::Error)]
#[error("transport failed")]
struct TransportError;

#[tokio::test]
async fn the_signal_stops_sessions_while_the_transport_is_still_draining() -> TestResult {
    let (hold_tx, hold_stop) = oneshot::channel();
    let harness = Arc::new(Harness::default());
    harness.script(
        "a",
        Script {
            hold_stop: Some(hold_stop),
            ..Default::default()
        },
    );
    let mut fixture = Fixture::new(harness.clone(), Duration::MAX);
    fixture.session("a").await?;
    let (signal, signaled) = oneshot::channel::<()>();
    let observed = harness.clone();
    let transport = move |drain: BoxFuture<'static, ()>| async move {
        drain.await;
        // An active response ends only when its job is stopped, which must already be happening.
        observed.stop_started("a").await;
        hold_tx.send(()).map_err(|_| TransportError)?;
        Ok::<_, TransportError>(())
    };
    let Fixture {
        ref mut system,
        ref manager,
        ..
    } = fixture;
    signal.send(()).map_err(|_| "signal")?;
    let served = serve_until_stopped(
        manager,
        system,
        async move {
            let _ = signaled.await;
        },
        transport,
    );
    timeout(BOUND, served).await?.map_err(|e| e.to_string())?;
    assert_eq!(fixture.harness.stop_calls("a"), 1);
    Ok(())
}

#[tokio::test]
async fn a_failed_transport_shuts_the_manager_down_and_joins_the_actors() -> TestResult {
    let mut fixture = Fixture::with_scripts(vec![]);
    fixture.session("a").await?;
    let Fixture {
        ref mut system,
        ref manager,
        ..
    } = fixture;
    let served = serve_until_stopped(manager, system, std::future::pending(), |_| async {
        Err::<(), _>(TransportError)
    });
    let error = timeout(BOUND, served).await?.err().ok_or("succeeded")?;
    assert_eq!(error.to_string(), "transport failed");
    assert_eq!(fixture.harness.stop_calls("a"), 1);
    assert!(
        fixture
            .manager
            .handle
            .send(SessionManagerMessage::Shutdown)
            .await
            .is_err()
    );
    Ok(())
}

#[tokio::test]
async fn transport_and_cleanup_failures_are_both_retained() -> TestResult {
    let mut fixture = Fixture::with_scripts(vec![
        (
            "a",
            Script {
                stop_error: Some(unknown_commit()),
                ..Default::default()
            },
        ),
        (
            "b",
            Script {
                stop_error: Some(Arc::new(DataFusionError::Plan("second".into()))),
                ..Default::default()
            },
        ),
    ]);
    fixture.session("a").await?;
    fixture.session("b").await?;
    let Fixture {
        ref mut system,
        ref manager,
        ..
    } = fixture;
    let served = serve_until_stopped(manager, system, std::future::pending(), |_| async {
        Err::<(), _>(TransportError)
    });
    let error = timeout(BOUND, served).await?.err().ok_or("succeeded")?;
    let error = error
        .downcast_ref::<ServerStopError>()
        .ok_or("failures were not combined")?;
    assert_eq!(error.transport.to_string(), "transport failed");
    let message = error.to_string();
    assert!(message.contains("transport failed"), "{message}");
    assert!(message.contains("session a"), "{message}");
    assert!(message.contains("session b"), "{message}");
    Ok(())
}

#[tokio::test]
async fn a_cleanup_failure_alone_is_reported_and_the_actors_are_joined() -> TestResult {
    let mut fixture = Fixture::with_scripts(vec![(
        "a",
        Script {
            stop_error: Some(unknown_commit()),
            ..Default::default()
        },
    )]);
    fixture.session("a").await?;
    let Fixture {
        ref mut system,
        ref manager,
        ..
    } = fixture;
    let served = serve_until_stopped(manager, system, std::future::ready(()), |drain| async {
        drain.await;
        Ok::<_, TransportError>(())
    });
    let error = timeout(BOUND, served).await?.err().ok_or("succeeded")?;
    assert!(error.to_string().contains("session a"), "{error}");
    assert!(
        fixture
            .manager
            .handle
            .send(SessionManagerMessage::Shutdown)
            .await
            .is_err()
    );
    Ok(())
}
