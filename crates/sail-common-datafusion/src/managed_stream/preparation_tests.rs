use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::task::{Context, Poll, Waker};

use arrow::datatypes::{Schema, SchemaRef};
use arrow::record_batch::RecordBatch;
use datafusion::physical_plan::RecordBatchStream;
use datafusion_common::{DataFusionError, Result};
use futures::{FutureExt, Stream, StreamExt};
use tokio::runtime::Handle;
use tokio::sync::oneshot;

use super::*;
use crate::error::{
    CommonErrorCause, PythonErrorCause, PythonErrorCauseExtractor, StructuredSparkError,
};
use crate::task_resources::TaskResourceScope;

type TestResult = std::result::Result<(), Box<dyn std::error::Error>>;

#[derive(Default)]
struct Counts {
    polls: AtomicUsize,
    drops: AtomicUsize,
}

struct ProbeStream {
    schema: SchemaRef,
    counts: Arc<Counts>,
    disposed: Option<oneshot::Sender<()>>,
}

impl ProbeStream {
    fn new() -> Self {
        Self {
            schema: Arc::new(Schema::empty()),
            counts: Arc::new(Counts::default()),
            disposed: None,
        }
    }
}

impl Stream for ProbeStream {
    type Item = Result<RecordBatch>;

    fn poll_next(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.counts.polls.fetch_add(1, Ordering::SeqCst);
        Poll::Pending
    }
}

impl RecordBatchStream for ProbeStream {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
}

impl Drop for ProbeStream {
    fn drop(&mut self) {
        self.counts.drops.fetch_add(1, Ordering::SeqCst);
        if let Some(disposed) = self.disposed.take() {
            let _ = disposed.send(());
        }
    }
}

struct NoPython;

impl PythonErrorCauseExtractor for NoPython {
    fn extract(_: &(dyn std::error::Error + 'static)) -> Option<PythonErrorCause> {
        None
    }
}

#[tokio::test]
async fn cancellation_before_complete_disposes_a_retained_unpolled_stream() -> TestResult {
    let resources = Arc::new(TaskResourceScope::new(Handle::current()));
    let (preparation, control) = StreamPreparation::new(resources.clone(), Handle::current());
    control.cancel()?;
    assert!(resources.spawn(|_| async { Ok(()) }).is_err());
    assert!(control.completed().is_none());
    assert!(control.completion().now_or_never().is_none());

    let source = ProbeStream::new();
    let schema = source.schema();
    let counts = source.counts.clone();
    let mut stream = preparation.complete(schema.clone(), Box::pin(source));
    assert_eq!(stream.schema(), schema);
    let report = control.completion().await;
    assert!(report.resources_drained);
    report.cleanup_result()?;
    assert!(report.result().is_err());
    assert_eq!(counts.polls.load(Ordering::SeqCst), 0);
    assert_eq!(counts.drops.load(Ordering::SeqCst), 1);
    let completed = control.completed().ok_or("completion was not retained")?;
    assert!(Arc::ptr_eq(&report, &completed));
    assert!(matches!(stream.next().await, Some(Err(_))));
    assert!(stream.next().await.is_none());
    assert_eq!(counts.polls.load(Ordering::SeqCst), 0);
    assert_eq!(counts.drops.load(Ordering::SeqCst), 1);
    Ok(())
}

#[tokio::test]
async fn preparation_failure_preserves_the_query_cause_and_qualifies_resource_drain() -> TestResult
{
    for supervisor_fails in [false, true] {
        let resources = Arc::new(TaskResourceScope::new(Handle::current()));
        let (observed, observation) = oneshot::channel();
        let (release, released) = oneshot::channel();
        resources.spawn(move |mut canceled| async move {
            canceled
                .wait_for(|value| *value)
                .await
                .map_err(|error| DataFusionError::External(Box::new(error)))?;
            let _ = observed.send(());
            released
                .await
                .map_err(|error| DataFusionError::External(Box::new(error)))?;
            if supervisor_fails {
                Err(DataFusionError::Execution("resource cleanup failed".into()))
            } else {
                Ok(())
            }
        })?;
        let (preparation, control) = StreamPreparation::new(resources.clone(), Handle::current());
        preparation.fail(DataFusionError::External(Box::new(StructuredSparkError {
            class: "java.lang.NumberFormatException".into(),
            condition: "CAST_INVALID_INPUT".into(),
            sql_state: "22018".into(),
            message: "invalid value during preparation".into(),
            parameters: HashMap::from([("expression".into(), "'bad'".into())]),
        })));
        assert!(resources.spawn(|_| async { Ok(()) }).is_err());
        observation.await?;
        assert!(control.completed().is_none());
        release
            .send(())
            .map_err(|_| "resource supervisor was lost")?;
        let report = control.completion().await;
        assert!(report.resources_drained);
        let error = report
            .result()
            .err()
            .ok_or("preparation failure was lost")?;
        let CommonErrorCause::Structured(cause) = CommonErrorCause::new::<NoPython>(&error) else {
            return Err("typed preparation cause was lost".into());
        };
        assert_eq!(cause.condition, "CAST_INVALID_INPUT");
        assert_eq!(cause.sql_state, "22018");
        assert_eq!(
            cause.parameters.get("expression").map(String::as_str),
            Some("'bad'")
        );
        assert_eq!(report.cleanup_errors.len(), usize::from(supervisor_fails));
        if supervisor_fails {
            let cleanup = report
                .cleanup_result()
                .err()
                .ok_or("cleanup failure was lost")?;
            assert!(
                matches!(CommonErrorCause::new::<NoPython>(&cleanup), CommonErrorCause::Execution(message) if message == "resource cleanup failed")
            );
        } else {
            report.cleanup_result()?;
        }
    }
    Ok(())
}

#[tokio::test]
async fn abandoned_preparation_keeps_cleanup_owned_after_a_waiter_is_dropped() -> TestResult {
    let resources = Arc::new(TaskResourceScope::new(Handle::current()));
    let (observed, observation) = oneshot::channel();
    let (release, released) = oneshot::channel();
    resources.spawn(move |mut canceled| async move {
        canceled
            .wait_for(|value| *value)
            .await
            .map_err(|error| DataFusionError::External(Box::new(error)))?;
        let _ = observed.send(());
        released
            .await
            .map_err(|error| DataFusionError::External(Box::new(error)))?;
        Ok(())
    })?;
    let (preparation, control) = StreamPreparation::new(resources, Handle::current());
    drop(preparation);
    let mut waiter = Box::pin(control.completion());
    assert!(
        waiter
            .as_mut()
            .poll(&mut Context::from_waker(Waker::noop()))
            .is_pending()
    );
    drop(waiter);
    observation.await?;
    assert!(control.completed().is_none());
    release
        .send(())
        .map_err(|_| "cleanup lost its resource owner")?;
    let (first, second) = tokio::join!(control.completion(), control.completion());
    assert!(first.resources_drained);
    first.cleanup_result()?;
    assert!(first.result().is_err());
    assert!(Arc::ptr_eq(&first, &second));
    let completed = control.completed().ok_or("completion was not retained")?;
    assert!(Arc::ptr_eq(&first, &completed));
    Ok(())
}

#[test]
fn runtime_shutdown_during_preparation_cleanup_does_not_report_drained_resources() -> TestResult {
    for pending_drain in [false, true] {
        let runtime = tokio::runtime::Builder::new_current_thread().build()?;
        let resources = Arc::new(TaskResourceScope::new(runtime.handle().clone()));
        let (started, starting) = oneshot::channel();
        let (release, released) = oneshot::channel::<()>();
        if pending_drain {
            resources.spawn(move |_| async move {
                let _ = started.send(());
                released
                    .await
                    .map_err(|error| DataFusionError::External(Box::new(error)))?;
                Ok(())
            })?;
        }
        let (disposed, disposal) = oneshot::channel();
        let mut source = ProbeStream::new();
        source.disposed = Some(disposed);
        let schema = source.schema();
        let counts = source.counts.clone();
        let (preparation, control) = StreamPreparation::new(resources, runtime.handle().clone());
        control.cancel()?;
        let _stream = preparation.complete(schema, Box::pin(source));
        if pending_drain {
            runtime.block_on(async {
                starting.await?;
                disposal.await?;
                Ok::<_, oneshot::error::RecvError>(())
            })?;
            assert_eq!(counts.drops.load(Ordering::SeqCst), 1);
        } else {
            // The current-thread runtime has never been driven.
            assert_eq!(counts.drops.load(Ordering::SeqCst), 0);
        }
        assert!(control.completed().is_none());
        let mut completion = Box::pin(control.completion());
        assert!(
            completion
                .as_mut()
                .poll(&mut Context::from_waker(Waker::noop()))
                .is_pending()
        );
        drop(runtime);
        let Poll::Ready(report) = completion
            .as_mut()
            .poll(&mut Context::from_waker(Waker::noop()))
        else {
            return Err("runtime shutdown stranded preparation cleanup".into());
        };
        assert!(!report.resources_drained);
        let cleanup = report
            .cleanup_result()
            .err()
            .ok_or("interrupted cleanup reported success")?;
        assert!(
            matches!(CommonErrorCause::new::<NoPython>(&cleanup), CommonErrorCause::Internal(message) if message.contains("cleanup interrupted"))
        );
        assert!(report.result().is_err());
        assert_eq!(counts.polls.load(Ordering::SeqCst), 0);
        assert_eq!(counts.drops.load(Ordering::SeqCst), 1);
        drop(release);
    }
    Ok(())
}
