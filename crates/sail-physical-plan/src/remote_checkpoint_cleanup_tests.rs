//! Exercise the actual commit failure path and compensating cleanup.
use datafusion::prelude::{SessionConfig, SessionContext};
use object_store::memory::InMemory;
use sail_common_datafusion::error::{
    CommitStateUnknownError, CommonErrorCause, PythonErrorCause, PythonErrorCauseExtractor,
};

use super::*;

struct NoPython;
impl PythonErrorCauseExtractor for NoPython {
    fn extract(_: &(dyn std::error::Error + 'static)) -> Option<PythonErrorCause> {
        None
    }
}

async fn failed_commit(cleanup_fails: bool) -> Result<DataFusionError> {
    let registry = Arc::new(RemoteCheckpointRegistry::new(None, "session".into()));
    let context =
        SessionContext::new_with_config(SessionConfig::new().with_extension(registry.clone()));
    let url = ObjectStoreUrl::parse(if cleanup_fails {
        "unregistered-checkpoint://bucket"
    } else {
        "memory://"
    })?;
    if !cleanup_fails {
        context
            .runtime_env()
            .register_object_store(url.as_ref(), Arc::new(InMemory::new()));
    }
    let input = Box::pin(RecordBatchStreamAdapter::new(
        checkpoint_metadata_schema(),
        stream::once(async {
            Err(DataFusionError::External(Box::new(
                CommitStateUnknownError("provider publication result was lost".into()),
            )))
        }),
    ));
    let schema = Arc::new(Schema::empty());
    let result = commit_checkpoint(
        input,
        context.task_ctx(),
        "relation".into(),
        url,
        Path::from("checkpoint/session/relation"),
        schema.clone(),
        schema.clone(),
        Partitioning::UnknownPartitioning(1),
        None,
        schema,
    )
    .await;
    assert!(registry.get("relation")?.is_none());
    match result {
        Ok(_) => Err(internal_datafusion_err!(
            "input failure unexpectedly produced a checkpoint"
        )),
        Err(error) => Ok(error),
    }
}

#[tokio::test]
async fn simultaneous_checkpoint_and_cleanup_failures_preserve_nonretryable_cause() -> Result<()> {
    let error = failed_commit(true).await?;
    let cause = CommonErrorCause::new::<NoPython>(&error);
    assert!(matches!(cause, CommonErrorCause::CommitStateUnknown(_)));
    assert!(!cause.is_retryable());
    let DataFusionError::Collection(errors) = error else {
        return Err(internal_datafusion_err!(
            "both typed errors must be retained"
        ));
    };
    assert_eq!(errors.len(), 2);
    let DataFusionError::External(original) = &errors[0] else {
        return Err(internal_datafusion_err!(
            "original failure must retain its type"
        ));
    };
    assert!(original.downcast_ref::<CommitStateUnknownError>().is_some());
    let DataFusionError::Context(message, cleanup) = &errors[1] else {
        return Err(internal_datafusion_err!("cleanup must carry its path"));
    };
    assert!(message.contains("checkpoint/session/relation"));
    // This fixture fails the actual registry lookup before storage mutation.
    assert!(cleanup.to_string().contains("unregistered-checkpoint"));
    Ok(())
}

#[tokio::test]
async fn successful_cleanup_keeps_the_original_checkpoint_error() -> Result<()> {
    let error = failed_commit(false).await?;
    assert!(!CommonErrorCause::new::<NoPython>(&error).is_retryable());
    let DataFusionError::External(original) = error else {
        return Err(internal_datafusion_err!(
            "successful cleanup must return the original error"
        ));
    };
    assert!(original.downcast_ref::<CommitStateUnknownError>().is_some());
    Ok(())
}
