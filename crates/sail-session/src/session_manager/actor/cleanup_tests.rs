use std::collections::HashMap;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use datafusion::arrow::datatypes::Schema;
use datafusion::common::DataFusionError;
use datafusion::execution::SendableRecordBatchStream;
use datafusion::execution::object_store::ObjectStoreUrl;
use datafusion::physical_expr::Partitioning;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::prelude::SessionContext;
use object_store::ObjectStoreExt;
use object_store::path::Path;
use sail_cache::remote_checkpoint::RemoteCheckpointDescriptor;
use sail_common_datafusion::error::{
    CommitStateUnknownError, CommonErrorCause, PythonErrorCause, PythonErrorCauseExtractor,
    StructuredSparkError,
};
use tokio::sync::oneshot;

use super::*;

type TestResult = std::result::Result<(), Box<dyn std::error::Error>>;

struct TestRunner {
    error: Option<Arc<DataFusionError>>,
    release: Mutex<Option<oneshot::Receiver<()>>>,
    stop_calls: AtomicUsize,
    confirms_effects: bool,
}

impl TestRunner {
    fn new(error: Option<Arc<DataFusionError>>, release: Option<oneshot::Receiver<()>>) -> Self {
        Self {
            error,
            release: Mutex::new(release),
            stop_calls: AtomicUsize::new(0),
            confirms_effects: true,
        }
    }
}

#[async_trait::async_trait]
impl JobRunner for TestRunner {
    async fn execute(
        &self,
        _: &SessionContext,
        _: Arc<dyn ExecutionPlan>,
    ) -> Result<SendableRecordBatchStream> {
        Err(DataFusionError::Internal("unused test execution".into()))
    }

    fn explain(&self, _: Arc<dyn ExecutionPlan>) -> Result<String> {
        Err(DataFusionError::Internal("unused test explanation".into()))
    }

    async fn stop(&self) -> Result<()> {
        self.stop_calls.fetch_add(1, Ordering::SeqCst);
        let release = self
            .release
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .take();
        if let Some(release) = release {
            release
                .await
                .map_err(|error| DataFusionError::External(Box::new(error)))?;
        }
        match &self.error {
            Some(error) => Err(DataFusionError::Shared(error.clone())),
            None => Ok(()),
        }
    }

    fn confirms_checkpoint_effects(&self) -> bool {
        self.confirms_effects
    }
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
async fn failed_stop_preserves_checkpoint_registry_and_typed_causes() -> TestResult {
    let failures = [
        DataFusionError::External(Box::new(StructuredSparkError {
            class: "java.lang.NumberFormatException".into(),
            condition: "CAST_INVALID_INPUT".into(),
            sql_state: "22018".into(),
            message: "invalid stop value".into(),
            parameters: HashMap::from([("expression".into(), "'bad'".into())]),
        })),
        DataFusionError::External(Box::new(CommitStateUnknownError(
            "checkpoint writer outcome unknown".into(),
        ))),
    ];
    for (index, failure) in failures.into_iter().enumerate() {
        let failure = Arc::new(failure);
        let runner = TestRunner::new(Some(failure.clone()), None);
        let checkpoints = RemoteCheckpointRegistry::new(None, "cleanup-test".into());
        checkpoints.insert(descriptor("existing")?)?;
        let original = checkpoints.get("existing")?.ok_or("missing checkpoint")?;
        let error = stop_and_clean_checkpoints(
            None,
            &runner,
            Some(&checkpoints),
            &RuntimeEnv::default(),
            |_| {},
        )
        .await
        .err()
        .ok_or("failed stop reported success")?;
        let DataFusionError::Shared(retained) = &error else {
            return Err("stop error was replaced".into());
        };
        assert!(Arc::ptr_eq(retained, &failure));
        match (index, CommonErrorCause::new::<NoPython>(&error)) {
            (0, CommonErrorCause::Structured(cause)) => {
                assert_eq!(cause.class, "java.lang.NumberFormatException");
                assert_eq!(cause.condition, "CAST_INVALID_INPUT");
                assert_eq!(cause.sql_state, "22018");
                assert_eq!(
                    cause.parameters.get("expression").map(String::as_str),
                    Some("'bad'"),
                );
            }
            (1, cause @ CommonErrorCause::CommitStateUnknown(_)) => {
                assert!(!cause.is_retryable());
            }
            _ => return Err("typed stop cause was lost".into()),
        }
        assert_eq!(runner.stop_calls.load(Ordering::SeqCst), 1);
        assert!(Arc::ptr_eq(
            &original,
            &checkpoints
                .get("existing")?
                .ok_or("checkpoint was cleared")?,
        ));
        checkpoints.insert(descriptor("after-failed-stop")?)?;
        assert!(checkpoints.get("after-failed-stop")?.is_some());
    }
    Ok(())
}

#[tokio::test]
async fn checkpoint_cleanup_waits_for_successful_stop_then_clears_and_seals() -> TestResult {
    let (release, pending) = oneshot::channel();
    let runner = TestRunner::new(None, Some(pending));
    let checkpoints = RemoteCheckpointRegistry::new(None, "cleanup-test".into());
    checkpoints.insert(descriptor("existing")?)?;
    let runtime = RuntimeEnv::default();
    let cleanup = stop_and_clean_checkpoints(None, &runner, Some(&checkpoints), &runtime, |_| {});
    tokio::pin!(cleanup);
    assert!(futures::poll!(cleanup.as_mut()).is_pending());
    assert_eq!(runner.stop_calls.load(Ordering::SeqCst), 1);
    assert!(checkpoints.get("existing")?.is_some());
    checkpoints.insert(descriptor("while-stopping")?)?;
    release.send(()).map_err(|_| "stop was abandoned")?;
    cleanup.await?;
    assert!(checkpoints.get("existing")?.is_none());
    assert!(checkpoints.get("while-stopping")?.is_none());
    assert!(checkpoints.insert(descriptor("after-cleanup")?).is_err());
    Ok(())
}

#[tokio::test]
async fn missing_checkpoint_registry_does_not_skip_stopping_the_runner() -> TestResult {
    let error = Arc::new(DataFusionError::External(Box::new(
        CommitStateUnknownError("stop outcome unknown".into()),
    )));
    let runner = TestRunner::new(Some(error.clone()), None);
    let result =
        stop_and_clean_checkpoints(None, &runner, None, &RuntimeEnv::default(), |_| {}).await;
    let Err(DataFusionError::Shared(retained)) = result else {
        return Err("missing registry suppressed the stop failure".into());
    };
    assert!(Arc::ptr_eq(&retained, &error));
    assert_eq!(runner.stop_calls.load(Ordering::SeqCst), 1);
    Ok(())
}

/// An operation that finishes only when the job it is waiting for has been stopped.
struct OperationWaitingForJob {
    closing: AtomicUsize,
    stopped: Mutex<Option<oneshot::Receiver<()>>>,
    error: Option<Arc<DataFusionError>>,
}

#[async_trait::async_trait]
impl SessionOperations for OperationWaitingForJob {
    fn begin_close(&self) -> Result<()> {
        self.closing.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }

    async fn wait_closed(&self) -> Result<()> {
        let stopped = self.stopped.lock().ok().and_then(|mut x| x.take());
        if let Some(stopped) = stopped {
            let _ = stopped.await;
        }
        match &self.error {
            Some(error) => Err(DataFusionError::Shared(error.clone())),
            None => Ok(()),
        }
    }
}

/// A runner whose stop requires that closing was requested, and then releases the operation.
struct RunnerReleasingOperation {
    operation: Arc<OperationWaitingForJob>,
    release: Mutex<Option<oneshot::Sender<()>>>,
}

#[async_trait::async_trait]
impl JobRunner for RunnerReleasingOperation {
    async fn execute(
        &self,
        _: &SessionContext,
        _: Arc<dyn ExecutionPlan>,
    ) -> Result<SendableRecordBatchStream> {
        Err(DataFusionError::Internal("unused test execution".into()))
    }

    fn explain(&self, _: Arc<dyn ExecutionPlan>) -> Result<String> {
        Err(DataFusionError::Internal("unused test explanation".into()))
    }

    async fn stop(&self) -> Result<()> {
        assert_eq!(self.operation.closing.load(Ordering::SeqCst), 1);
        if let Ok(Some(release)) = self.release.lock().map(|mut x| x.take()) {
            let _ = release.send(());
        }
        Ok(())
    }

    fn confirms_checkpoint_effects(&self) -> bool {
        true
    }
}

fn operation_waiting_for_job(
    error: Option<Arc<DataFusionError>>,
) -> (Arc<OperationWaitingForJob>, RunnerReleasingOperation) {
    let (release, stopped) = oneshot::channel();
    let operation = Arc::new(OperationWaitingForJob {
        closing: AtomicUsize::new(0),
        stopped: Mutex::new(Some(stopped)),
        error,
    });
    let runner = RunnerReleasingOperation {
        operation: operation.clone(),
        release: Mutex::new(Some(release)),
    };
    (operation, runner)
}

#[tokio::test]
async fn an_operation_waiting_for_its_job_does_not_deadlock_the_cleanup() -> TestResult {
    let (operation, runner) = operation_waiting_for_job(None);
    let checkpoints = RemoteCheckpointRegistry::new(None, "cleanup-test".into());
    checkpoints.insert(descriptor("existing")?)?;
    let runtime = RuntimeEnv::default();
    let cleanup = stop_and_clean_checkpoints(
        Some(operation.as_ref()),
        &runner,
        Some(&checkpoints),
        &runtime,
        |_| {},
    );
    tokio::time::timeout(Duration::from_secs(10), cleanup).await??;
    assert!(checkpoints.get("existing")?.is_none());
    Ok(())
}

#[tokio::test]
async fn a_failed_operation_drain_blocks_checkpoint_removal() -> TestResult {
    let failure = Arc::new(DataFusionError::External(Box::new(
        CommitStateUnknownError("operation outcome unknown".into()),
    )));
    let (operation, runner) = operation_waiting_for_job(Some(failure.clone()));
    let checkpoints = RemoteCheckpointRegistry::new(None, "cleanup-test".into());
    checkpoints.insert(descriptor("existing")?)?;
    let runtime = RuntimeEnv::default();
    let cleanup = stop_and_clean_checkpoints(
        Some(operation.as_ref()),
        &runner,
        Some(&checkpoints),
        &runtime,
        |_| {},
    );
    let error = tokio::time::timeout(Duration::from_secs(10), cleanup)
        .await?
        .err()
        .ok_or("failed drain reported success")?;
    let DataFusionError::Shared(retained) = &error else {
        return Err("drain error was replaced".into());
    };
    assert!(Arc::ptr_eq(retained, &failure));
    assert!(checkpoints.get("existing")?.is_some());
    Ok(())
}

#[tokio::test]
async fn unconfirmed_writers_keep_the_checkpoints_of_a_session_that_used_storage() -> TestResult {
    let mut runner = TestRunner::new(None, None);
    runner.confirms_effects = false;
    let directory = tempfile::tempdir()?;
    let url = format!("file://{}/", directory.path().display());
    let runtime = RuntimeEnv::default();
    let checkpoints = RemoteCheckpointRegistry::new(Some(url.clone()), "cleanup-test".into());

    // Nothing was written, so there is nothing to delete and nothing to fail about.
    let unused = stop_and_clean_checkpoints(None, &runner, Some(&checkpoints), &runtime, |_| {});
    unused.await?;

    let checkpoints = RemoteCheckpointRegistry::new(Some(url.clone()), "cleanup-test".into());
    let (store_url, prefix) = checkpoints.resolve_relation(&runtime, "relation")?;
    let store = runtime.object_store(&store_url)?;
    let object = prefix.clone().join("part");
    store.put(&object, "retained".into()).await?;
    let error = stop_and_clean_checkpoints(None, &runner, Some(&checkpoints), &runtime, |_| {})
        .await
        .err()
        .ok_or("unconfirmed stop removed checkpoints")?;
    assert!(error.to_string().contains("cannot confirm"), "{error}");
    assert!(store.head(&object).await.is_ok());
    // The registry was not sealed, and it can still resolve the retained objects.
    assert!(checkpoints.resolve_relation(&runtime, "relation").is_ok());
    Ok(())
}
