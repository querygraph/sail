use std::collections::HashMap;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use datafusion::arrow::datatypes::Schema;
use datafusion::common::DataFusionError;
use datafusion::execution::SendableRecordBatchStream;
use datafusion::execution::object_store::ObjectStoreUrl;
use datafusion::physical_expr::Partitioning;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::prelude::SessionContext;
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
}

impl TestRunner {
    fn new(error: Option<Arc<DataFusionError>>, release: Option<oneshot::Receiver<()>>) -> Self {
        Self {
            error,
            release: Mutex::new(release),
            stop_calls: AtomicUsize::new(0),
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
        let error = stop_and_clean_checkpoints(&runner, Some(&checkpoints), &RuntimeEnv::default())
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
    let cleanup = stop_and_clean_checkpoints(&runner, Some(&checkpoints), &runtime);
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
    let result = stop_and_clean_checkpoints(&runner, None, &RuntimeEnv::default()).await;
    let Err(DataFusionError::Shared(retained)) = result else {
        return Err("missing registry suppressed the stop failure".into());
    };
    assert!(Arc::ptr_eq(&retained, &error));
    assert_eq!(runner.stop_calls.load(Ordering::SeqCst), 1);
    Ok(())
}
