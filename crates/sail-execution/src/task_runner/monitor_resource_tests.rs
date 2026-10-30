use std::collections::HashMap;
use std::pin::Pin;
use std::task::{Context, Poll};

use datafusion::arrow::datatypes::{Schema, SchemaRef};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::common::Result;
use datafusion::execution::RecordBatchStream;
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use futures::{Stream, stream};
use sail_common_datafusion::error::{CommitStateUnknownError, StructuredSparkError};
use tokio::runtime::Handle;

use super::*;
use crate::id::JobId;

type TestResult = std::result::Result<(), Box<dyn std::error::Error>>;

fn key() -> TaskKey {
    TaskKey {
        job_id: JobId::from(1),
        stage: 2,
        partition: 3,
        attempt: 4,
    }
}

fn batches(
    inner: impl Stream<Item = Result<RecordBatch>> + Send + 'static,
) -> SendableRecordBatchStream {
    Box::pin(RecordBatchStreamAdapter::new(
        Arc::new(Schema::empty()),
        inner,
    ))
}

fn scope() -> TaskResourceScope {
    TaskResourceScope::new(Handle::current())
}

/// A supervisor that acknowledges cancellation separately from completing cleanup.
fn delayed_cleanup(
    resources: &TaskResourceScope,
    result: Result<()>,
) -> Result<(oneshot::Receiver<()>, oneshot::Sender<()>)> {
    let (observed, observation) = oneshot::channel();
    let (complete, completion) = oneshot::channel();
    resources.spawn(|mut canceled| async move {
        canceled
            .wait_for(|value| *value)
            .await
            .map_err(|error| DataFusionError::External(Box::new(error)))?;
        let _ = observed.send(());
        completion
            .await
            .map_err(|error| DataFusionError::External(Box::new(error)))?;
        result
    })?;
    Ok((observation, complete))
}

fn status(message: TaskRunnerMessage) -> (TaskStatus, Option<String>, Option<CommonErrorCause>) {
    let TaskRunnerMessage::ReportTaskStatus {
        key: actual,
        status,
        message,
        cause,
    } = message
    else {
        unreachable!("monitor must return a status report")
    };
    assert_eq!(actual, key());
    (status, message, cause)
}

fn unknown(message: &str) -> DataFusionError {
    DataFusionError::External(Box::new(CommitStateUnknownError(message.into())))
}

#[tokio::test]
async fn success_waits_for_resource_completion_without_canceling_it() -> TestResult {
    let resources = scope();
    let (complete, completion) = oneshot::channel();
    resources.spawn(|canceled| async move {
        completion
            .await
            .map_err(|error| DataFusionError::External(Box::new(error)))?;
        assert!(!*canceled.borrow());
        Ok(())
    })?;
    let (_stop, signal) = oneshot::channel();
    let terminal =
        TaskMonitor::run_to_terminal(key(), batches(stream::empty()), signal, &resources);
    tokio::pin!(terminal);
    assert!(futures::poll!(terminal.as_mut()).is_pending());
    assert!(
        resources.spawn(|_| async { Ok(()) }).is_err(),
        "EOF must seal admission"
    );
    complete.send(()).map_err(|_| "completion receiver lost")?;
    let (state, message, cause) = status(terminal.await);
    assert!(matches!(state, TaskStatus::Succeeded));
    assert!(message.is_none() && cause.is_none());
    assert_eq!(resources.drain().await?.registered, 1);
    Ok(())
}

#[tokio::test]
async fn cancellation_and_closed_signal_wait_for_cleanup() -> TestResult {
    for close_sender in [false, true] {
        let resources = scope();
        let (observation, complete) = delayed_cleanup(&resources, Ok(()))?;
        let (stop, signal) = oneshot::channel();
        if close_sender {
            drop(stop);
        } else {
            stop.send(()).map_err(|_| "stop receiver lost")?;
        }
        let terminal =
            TaskMonitor::run_to_terminal(key(), batches(stream::pending()), signal, &resources);
        tokio::pin!(terminal);
        assert!(futures::poll!(terminal.as_mut()).is_pending());
        observation.await?;
        assert!(futures::poll!(terminal.as_mut()).is_pending());
        complete.send(()).map_err(|_| "cleanup receiver lost")?;
        let (state, message, cause) = status(terminal.await);
        assert!(matches!(state, TaskStatus::Canceled));
        assert!(message.is_some_and(|message| message.contains("canceled")));
        assert!(cause.is_none());
    }
    Ok(())
}

#[tokio::test]
async fn cancellation_after_eof_interrupts_drain_but_waits_for_cleanup() -> TestResult {
    let resources = scope();
    let (observation, complete) = delayed_cleanup(&resources, Ok(()))?;
    let (stop, signal) = oneshot::channel();
    let terminal =
        TaskMonitor::run_to_terminal(key(), batches(stream::empty()), signal, &resources);
    tokio::pin!(terminal);
    assert!(futures::poll!(terminal.as_mut()).is_pending());
    assert!(resources.spawn(|_| async { Ok(()) }).is_err());
    stop.send(())
        .map_err(|_| "stop receiver was dropped at EOF")?;
    assert!(futures::poll!(terminal.as_mut()).is_pending());
    observation.await?;
    assert!(futures::poll!(terminal.as_mut()).is_pending());
    complete.send(()).map_err(|_| "cleanup receiver lost")?;
    assert!(matches!(status(terminal.await).0, TaskStatus::Canceled));
    Ok(())
}

#[tokio::test]
async fn cleanup_failure_preserves_structured_execution_cause_and_waits_for_siblings() -> TestResult
{
    let resources = scope();
    resources.spawn(|_| async { Err(DataFusionError::Execution("cleanup failed".into())) })?;
    let (observation, complete) = delayed_cleanup(&resources, Ok(()))?;
    let error = StructuredSparkError {
        class: "java.lang.NumberFormatException".into(),
        condition: "CAST_INVALID_INPUT".into(),
        sql_state: "22018".into(),
        message: "malformed value".into(),
        parameters: HashMap::from([("expression".into(), "'bad'".into())]),
    };
    let expected = serde_json::to_value(&error)?;
    let (_stop, signal) = oneshot::channel();
    let input = batches(stream::iter([Err(DataFusionError::External(Box::new(
        error,
    )))]));
    let terminal = TaskMonitor::run_to_terminal(key(), input, signal, &resources);
    tokio::pin!(terminal);
    assert!(futures::poll!(terminal.as_mut()).is_pending());
    observation.await?;
    assert!(futures::poll!(terminal.as_mut()).is_pending());
    complete.send(()).map_err(|_| "cleanup receiver lost")?;
    let (state, message, cause) = status(terminal.await);
    assert!(matches!(state, TaskStatus::Failed));
    let Some(CommonErrorCause::Structured(error)) = cause else {
        return Err("structured execution cause lost".into());
    };
    assert_eq!(serde_json::to_value(error)?, expected);
    let message = message.ok_or("missing diagnostics")?;
    assert!(message.contains("malformed value") && message.contains("cleanup failed"));
    assert_eq!(resources.drain().await?.registered, 2);
    Ok(())
}

#[tokio::test]
async fn unknown_cleanup_outcome_prevents_retry_after_execution_failure_or_cancel() -> TestResult {
    for cancel in [false, true] {
        let resources = scope();
        let (observation, complete) =
            delayed_cleanup(&resources, Err(unknown("upload reply lost")))?;
        let (stop, signal) = oneshot::channel();
        let input = if cancel {
            stop.send(()).map_err(|_| "stop receiver lost")?;
            batches(stream::pending())
        } else {
            batches(stream::iter([Err(DataFusionError::Execution(
                "query failed".into(),
            ))]))
        };
        let terminal = TaskMonitor::run_to_terminal(key(), input, signal, &resources);
        tokio::pin!(terminal);
        assert!(futures::poll!(terminal.as_mut()).is_pending());
        observation.await?;
        assert!(futures::poll!(terminal.as_mut()).is_pending());
        complete.send(()).map_err(|_| "cleanup receiver lost")?;
        let (state, message, cause) = status(terminal.await);
        assert!(matches!(state, TaskStatus::Failed));
        let cause = cause.ok_or("unknown commit cause lost")?;
        assert!(!cause.is_retryable());
        assert!(
            matches!(cause, CommonErrorCause::CommitStateUnknown(message) if message.contains("upload reply lost"))
        );
        let message = message.ok_or("missing diagnostics")?;
        assert!(message.contains(if cancel { "canceled" } else { "query failed" }));
        assert!(message.contains("upload reply lost"));
    }
    Ok(())
}

#[tokio::test]
async fn primary_unknown_commit_survives_another_unknown_cleanup_outcome() -> TestResult {
    let resources = scope();
    resources.spawn(|_| async { Err(unknown("secondary upload")) })?;
    let (_stop, signal) = oneshot::channel();
    let input = batches(stream::iter([Err(unknown("primary mutation"))]));
    let (state, message, cause) =
        status(TaskMonitor::run_to_terminal(key(), input, signal, &resources).await);
    assert!(matches!(state, TaskStatus::Failed));
    assert!(
        matches!(cause, Some(CommonErrorCause::CommitStateUnknown(message)) if message.contains("primary mutation"))
    );
    assert!(message.is_some_and(
        |message| message.contains("primary mutation") && message.contains("secondary upload")
    ));
    Ok(())
}

#[tokio::test]
async fn resource_failure_turns_success_into_failed_status() -> TestResult {
    let resources = scope();
    resources.spawn(|_| async { Err(unknown("complete response lost")) })?;
    let (_stop, signal) = oneshot::channel();
    let (state, _, cause) = status(
        TaskMonitor::run_to_terminal(key(), batches(stream::empty()), signal, &resources).await,
    );
    assert!(matches!(state, TaskStatus::Failed));
    assert!(
        matches!(cause, Some(CommonErrorCause::CommitStateUnknown(message)) if message.contains("complete response lost"))
    );
    Ok(())
}

struct PanicOnDrop;

impl Stream for PanicOnDrop {
    type Item = Result<RecordBatch>;

    fn poll_next(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        Poll::Pending
    }
}

impl RecordBatchStream for PanicOnDrop {
    fn schema(&self) -> SchemaRef {
        Arc::new(Schema::empty())
    }
}

impl Drop for PanicOnDrop {
    fn drop(&mut self) {
        std::panic::resume_unwind(Box::new("stream drop panic"));
    }
}

#[tokio::test]
async fn stream_poll_and_canceled_stream_drop_panics_wait_for_cleanup() -> TestResult {
    for drop_panic in [false, true] {
        let resources = scope();
        let (observation, complete) = delayed_cleanup(&resources, Ok(()))?;
        let (stop, signal) = oneshot::channel();
        let input = if drop_panic {
            stop.send(()).map_err(|_| "stop receiver lost")?;
            Box::pin(PanicOnDrop) as SendableRecordBatchStream
        } else {
            batches(stream::poll_fn(|_| {
                std::panic::resume_unwind(Box::new("stream poll panic"))
            }))
        };
        let terminal = TaskMonitor::run_to_terminal(key(), input, signal, &resources);
        tokio::pin!(terminal);
        assert!(futures::poll!(terminal.as_mut()).is_pending());
        observation.await?;
        assert!(futures::poll!(terminal.as_mut()).is_pending());
        complete.send(()).map_err(|_| "cleanup receiver lost")?;
        let (state, message, cause) = status(terminal.await);
        assert!(matches!(state, TaskStatus::Failed));
        let expected = if drop_panic {
            "task panicked: stream drop panic"
        } else {
            "task panicked: stream poll panic"
        };
        assert_eq!(message.as_deref(), Some(expected));
        assert!(matches!(cause, Some(CommonErrorCause::Internal(message)) if message == expected));
    }
    Ok(())
}
