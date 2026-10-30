use std::any::Any;
use std::panic::AssertUnwindSafe;
use std::sync::Arc;

use datafusion::common::DataFusionError;
use datafusion::execution::SendableRecordBatchStream;
use fastrace::Span;
use fastrace::future::FutureExt;
use futures::{FutureExt as _, StreamExt};
use sail_common::actor::ActorHandle;
use sail_common::telemetry::SpanAttribute;
use sail_common_datafusion::error::CommonErrorCause;
use sail_common_datafusion::task_resources::TaskResourceScope;
use sail_python_udf::error::PyErrExtractor;
use tokio::sync::oneshot;
use tokio_util::task::AbortOnDropHandle;

use crate::driver::TaskStatus;
use crate::id::{TaskKey, TaskKeyDisplay};
use crate::task_runner::{TaskRunnerActor, TaskRunnerMessage};

pub struct TaskMonitor {
    handle: ActorHandle<TaskRunnerActor>,
    key: TaskKey,
    stream: SendableRecordBatchStream,
    signal: oneshot::Receiver<()>,
    resources: Arc<TaskResourceScope>,
}

impl TaskMonitor {
    pub fn new(
        handle: ActorHandle<TaskRunnerActor>,
        key: TaskKey,
        stream: SendableRecordBatchStream,
        signal: oneshot::Receiver<()>,
        resources: Arc<TaskResourceScope>,
    ) -> Self {
        Self {
            handle,
            key,
            stream,
            signal,
            resources,
        }
    }

    pub async fn run(self) {
        let Self {
            handle,
            key,
            stream,
            signal,
            resources,
        } = self;
        let _ = handle.send(Self::running(key.clone())).await;
        let message = Self::run_to_terminal(key, stream, signal, &resources).await;
        let _ = handle.send(message).await;
    }

    pub async fn supervise(self) {
        let handle = self.handle.clone();
        let key = self.key.clone();
        let resources = self.resources.clone();
        // Admission now has one span per batch. Preserve task identity on the monitor
        // so preparation and execution spans can still be attributed to each attempt.
        let span = Span::enter_with_local_parent("TaskMonitor::run").with_properties(|| {
            [
                (SpanAttribute::EXECUTION_JOB_ID, key.job_id.to_string()),
                (SpanAttribute::EXECUTION_STAGE, key.stage.to_string()),
                (
                    SpanAttribute::EXECUTION_PARTITION,
                    key.partition.to_string(),
                ),
                (SpanAttribute::EXECUTION_ATTEMPT, key.attempt.to_string()),
            ]
        });
        let monitor = AbortOnDropHandle::new(tokio::spawn(self.run().in_span(span)));
        if let Some(message) = Self::monitor_failure(key, monitor).await {
            // Retain ownership even when the monitor panics outside execution.
            let message = Self::drain_resources(message, &resources).await;
            let _ = handle.send(message).await;
        }
    }

    async fn run_to_terminal(
        key: TaskKey,
        stream: SendableRecordBatchStream,
        mut signal: oneshot::Receiver<()>,
        resources: &TaskResourceScope,
    ) -> TaskRunnerMessage {
        // Catch both stream polling and destruction of the losing select future.
        let message = AssertUnwindSafe(async {
            tokio::select! {
                x = Self::execute(key.clone(), stream) => x,
                x = Self::cancel(key.clone(), &mut signal) => x,
            }
        })
        .catch_unwind()
        .await
        .unwrap_or_else(|payload| Self::panicked(key.clone(), payload));

        if matches!(
            message,
            TaskRunnerMessage::ReportTaskStatus {
                status: TaskStatus::Succeeded,
                ..
            }
        ) {
            // EOF can precede resource completion. StopTask must still interrupt
            // that wait, signal the supervisors, and await their cleanup reports.
            tokio::select! {
                x = Self::drain_resources(message, resources) => x,
                x = Self::cancel(key, &mut signal) => Self::drain_resources(x, resources).await,
            }
        } else {
            Self::drain_resources(message, resources).await
        }
    }

    async fn drain_resources(
        mut message: TaskRunnerMessage,
        resources: &TaskResourceScope,
    ) -> TaskRunnerMessage {
        if !matches!(
            message,
            TaskRunnerMessage::ReportTaskStatus {
                status: TaskStatus::Succeeded,
                ..
            }
        ) && let Err(error) = resources.cancel()
        {
            Self::resource_error(&mut message, "resource cancellation", &error);
        }
        match resources.drain().await {
            Ok(report) => {
                for failure in &report.failures {
                    Self::resource_error(
                        &mut message,
                        &format!("resource {} completion or cleanup", failure.id.0),
                        &failure.error,
                    );
                }
            }
            Err(error) => Self::resource_error(&mut message, "resource drainage", &error),
        }
        message
    }

    fn resource_error(message: &mut TaskRunnerMessage, context: &str, error: &DataFusionError) {
        if let TaskRunnerMessage::ReportTaskStatus {
            status,
            message,
            cause,
            ..
        } = message
        {
            *status = TaskStatus::Failed;
            let secondary = CommonErrorCause::new::<PyErrExtractor>(error);
            // Match error-collection precedence: preserve the primary cause,
            // except that an uncertain commit must prohibit automatic replay.
            if cause
                .as_ref()
                .is_none_or(|primary| primary.is_retryable() && !secondary.is_retryable())
            {
                *cause = Some(secondary);
            }
            let detail = format!("{context}: {error}");
            match message {
                Some(message) => {
                    message.push_str("; ");
                    message.push_str(&detail);
                }
                None => *message = Some(detail),
            }
        }
    }

    async fn monitor_failure(
        key: TaskKey,
        monitor: AbortOnDropHandle<()>,
    ) -> Option<TaskRunnerMessage> {
        match monitor.await {
            Ok(()) => None,
            Err(error) if error.is_cancelled() => None,
            Err(error) if error.is_panic() => Some(Self::panicked(key, error.into_panic())),
            Err(error) => {
                let message = format!("task monitor failed: {error}");
                Some(Self::status(
                    key,
                    TaskStatus::Failed,
                    Some(message.clone()),
                    Some(CommonErrorCause::Internal(message)),
                ))
            }
        }
    }

    /// Builds a "task is running" status message.
    fn running(key: TaskKey) -> TaskRunnerMessage {
        Self::status(key, TaskStatus::Running, None, None)
    }

    fn status(
        key: TaskKey,
        status: TaskStatus,
        message: Option<String>,
        cause: Option<CommonErrorCause>,
    ) -> TaskRunnerMessage {
        TaskRunnerMessage::ReportTaskStatus {
            key,
            status,
            message,
            cause,
        }
    }

    fn panic_message(payload: Box<dyn Any + Send>) -> String {
        if let Some(message) = payload.downcast_ref::<&str>() {
            (*message).to_string()
        } else if let Some(message) = payload.downcast_ref::<String>() {
            message.clone()
        } else {
            "unknown panic".to_string()
        }
    }

    fn panicked(key: TaskKey, payload: Box<dyn Any + Send>) -> TaskRunnerMessage {
        let message = format!("task panicked: {}", Self::panic_message(payload));
        Self::status(
            key,
            TaskStatus::Failed,
            Some(message.clone()),
            Some(CommonErrorCause::Internal(message)),
        )
    }

    /// Waits for a cancellation signal and builds a canceled status message.
    async fn cancel(key: TaskKey, signal: &mut oneshot::Receiver<()>) -> TaskRunnerMessage {
        let _ = signal.await;
        Self::status(
            key.clone(),
            TaskStatus::Canceled,
            Some(format!("{} canceled", TaskKeyDisplay(&key))),
            None,
        )
    }

    async fn execute(key: TaskKey, mut stream: SendableRecordBatchStream) -> TaskRunnerMessage {
        loop {
            let Some(batch) = stream.next().await else {
                break Self::status(key, TaskStatus::Succeeded, None, None);
            };
            if let Err(error) = batch {
                break Self::status(
                    key,
                    TaskStatus::Failed,
                    Some(format!("task error: {error}")),
                    Some(CommonErrorCause::new::<PyErrExtractor>(&error)),
                );
            }
        }
    }
}

#[cfg(test)]
#[path = "monitor_resource_tests.rs"]
mod resource_tests;

#[cfg(test)]
mod tests {
    use futures::future;

    use super::*;
    use crate::id::JobId;

    #[tokio::test]
    async fn preserves_unknown_commit_cause_through_task_status_and_remote_error()
    -> Result<(), Box<dyn std::error::Error>> {
        use std::sync::Arc;

        use datafusion::arrow::datatypes::Schema;
        use datafusion::common::DataFusionError;
        use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
        use sail_catalog::error::CatalogError;
        use sail_common_datafusion::error::RemoteError;

        let error = DataFusionError::Collection(vec![
            DataFusionError::Execution("another failure".into()),
            DataFusionError::Context(
                "commit failed".into(),
                Box::new(DataFusionError::External(Box::new(
                    CatalogError::CommitStateUnknown("response lost".to_string().into()),
                ))),
            ),
        ]);
        let stream = RecordBatchStreamAdapter::new(
            Arc::new(Schema::empty()),
            futures::stream::iter([Err(error)]),
        );
        let message = TaskMonitor::execute(task_key(), Box::pin(stream)).await;
        let TaskRunnerMessage::ReportTaskStatus {
            status: TaskStatus::Failed,
            cause: Some(cause),
            ..
        } = message
        else {
            return Err("expected task failure with cause".into());
        };
        assert!(!cause.is_retryable());
        let cause = serde_json::from_str(&serde_json::to_string(&cause)?)?;
        let error = DataFusionError::External(Box::new(RemoteError { cause }));
        assert!(matches!(
            CommonErrorCause::new::<PyErrExtractor>(&error),
            CommonErrorCause::CommitStateUnknown(message) if message == "commit state unknown: response lost"
        ));
        Ok(())
    }

    #[tokio::test]
    async fn preserves_structured_cause_through_failed_task_status()
    -> Result<(), Box<dyn std::error::Error>> {
        use std::collections::HashMap;
        use std::sync::Arc;

        use datafusion::arrow::datatypes::Schema;
        use datafusion::common::DataFusionError;
        use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
        use sail_common_datafusion::error::{RemoteError, StructuredSparkError};

        let error = StructuredSparkError {
            class: "java.lang.NumberFormatException".into(),
            condition: "CAST_INVALID_INPUT".into(),
            sql_state: "22018".into(),
            message: "malformed value".into(),
            parameters: HashMap::from([("expression".into(), "'bad'".into())]),
        };
        let expected = serde_json::to_value(&error)?;
        let stream = RecordBatchStreamAdapter::new(
            Arc::new(Schema::empty()),
            futures::stream::iter([Err(DataFusionError::External(Box::new(error)))]),
        );
        let message = TaskMonitor::execute(task_key(), Box::pin(stream)).await;
        let TaskRunnerMessage::ReportTaskStatus {
            status: TaskStatus::Failed,
            cause: Some(cause),
            ..
        } = message
        else {
            return Err("expected failed task with structured cause".into());
        };
        let cause = serde_json::from_str(&serde_json::to_string(&cause)?)?;
        let CommonErrorCause::Structured(error) =
            CommonErrorCause::new::<PyErrExtractor>(&RemoteError { cause })
        else {
            return Err("task status lost structured cause".into());
        };
        assert_eq!(serde_json::to_value(error)?, expected);
        Ok(())
    }

    fn task_key() -> TaskKey {
        TaskKey {
            job_id: JobId::from(1),
            stage: 2,
            partition: 3,
            attempt: 4,
        }
    }

    #[tokio::test]
    async fn ignores_successful_monitor_completion() {
        let monitor = AbortOnDropHandle::new(tokio::spawn(async {}));

        let message = TaskMonitor::monitor_failure(task_key(), monitor).await;

        assert!(message.is_none());
    }

    #[tokio::test]
    async fn ignores_cancelled_monitor() {
        let monitor = AbortOnDropHandle::new(tokio::spawn(future::pending()));
        monitor.abort();

        let message = TaskMonitor::monitor_failure(task_key(), monitor).await;

        assert!(message.is_none());
    }

    #[tokio::test]
    async fn reports_monitor_panic_as_task_failure() -> Result<(), String> {
        let monitor = AbortOnDropHandle::new(tokio::spawn(async {
            std::panic::resume_unwind(Box::new("test monitor panic"));
        }));

        let message = TaskMonitor::monitor_failure(task_key(), monitor)
            .await
            .ok_or_else(|| "expected a failed task status".to_string())?;

        let TaskRunnerMessage::ReportTaskStatus {
            key,
            status: TaskStatus::Failed,
            message: Some(message),
            cause: Some(CommonErrorCause::Internal(cause)),
        } = message
        else {
            return Err("expected a failed task status".to_string());
        };
        assert_eq!(key, task_key());
        assert_eq!(message, "task panicked: test monitor panic");
        assert_eq!(cause, message);
        Ok(())
    }

    #[test]
    fn extracts_panic_messages() {
        assert_eq!(
            TaskMonitor::panic_message(Box::new("literal panic")),
            "literal panic"
        );
        assert_eq!(
            TaskMonitor::panic_message(Box::new("owned panic".to_string())),
            "owned panic"
        );
        assert_eq!(TaskMonitor::panic_message(Box::new(42)), "unknown panic");
    }
}
