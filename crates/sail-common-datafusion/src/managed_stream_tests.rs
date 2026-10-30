use std::collections::HashMap;
use std::future::Future;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::mpsc;

use arrow::array::{ArrayRef, Int32Array};
use arrow::datatypes::Schema;
use futures::StreamExt;
use futures::task::{ArcWake, waker};
use tokio::sync::oneshot;

use super::*;
use crate::error::{
    CommitStateUnknownError, CommonErrorCause, PythonErrorCause, PythonErrorCauseExtractor,
    StructuredSparkError,
};

type TestResult = std::result::Result<(), Box<dyn std::error::Error>>;
type Next = dyn FnMut(&mut Context<'_>) -> Poll<Option<Result<RecordBatch>>> + Send;

#[derive(Default)]
struct Counts {
    polls: AtomicUsize,
    drops: AtomicUsize,
}

struct ProbeStream {
    next: Box<Next>,
    counts: Arc<Counts>,
    disposed: Option<oneshot::Sender<()>>,
    panic_drop: bool,
}

impl ProbeStream {
    fn new(
        next: impl FnMut(&mut Context<'_>) -> Poll<Option<Result<RecordBatch>>> + Send + 'static,
    ) -> Self {
        Self {
            next: Box::new(next),
            counts: Arc::new(Counts::default()),
            disposed: None,
            panic_drop: false,
        }
    }
}

impl Stream for ProbeStream {
    type Item = Result<RecordBatch>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();
        this.counts.polls.fetch_add(1, Ordering::SeqCst);
        (this.next)(cx)
    }
}

impl RecordBatchStream for ProbeStream {
    fn schema(&self) -> SchemaRef {
        std::panic::resume_unwind(Box::new("upstream schema must not be called"))
    }
}

impl Drop for ProbeStream {
    fn drop(&mut self) {
        self.counts.drops.fetch_add(1, Ordering::SeqCst);
        if let Some(disposed) = self.disposed.take() {
            let _ = disposed.send(());
        }
        if self.panic_drop {
            std::panic::resume_unwind(Box::new("source drop failure"));
        }
    }
}

fn managed(
    stream: ProbeStream,
    resources: Arc<TaskResourceScope>,
    runtime: Handle,
) -> (ManagedRecordBatchStream, ManagedStreamControl) {
    ManagedRecordBatchStream::new(
        Arc::new(Schema::empty()),
        Box::pin(stream),
        resources,
        runtime,
    )
}

fn poll_stream(stream: &mut ManagedRecordBatchStream) -> Poll<Option<Result<RecordBatch>>> {
    Pin::new(stream).poll_next(&mut Context::from_waker(Waker::noop()))
}

struct NoPython;

impl PythonErrorCauseExtractor for NoPython {
    fn extract(_: &(dyn std::error::Error + 'static)) -> Option<PythonErrorCause> {
        None
    }
}

#[derive(Default)]
struct WakeCount(AtomicUsize);

impl ArcWake for WakeCount {
    fn wake_by_ref(this: &Arc<Self>) {
        this.0.fetch_add(1, Ordering::SeqCst);
    }
}

#[tokio::test]
async fn abandoned_next_and_source_wakeup_do_not_poll_without_demand() -> TestResult {
    let saved = Arc::new(Mutex::new(None::<Waker>));
    let source_waker = saved.clone();
    let source = ProbeStream::new(move |cx| {
        *source_waker
            .lock()
            .unwrap_or_else(|error| error.into_inner()) = Some(cx.waker().clone());
        Poll::Pending
    });
    let counts = source.counts.clone();
    let resources = Arc::new(TaskResourceScope::new(Handle::current()));
    let (mut stream, control) = managed(source, resources, Handle::current());
    assert!(stream.schema().fields().is_empty());
    assert_eq!(counts.polls.load(Ordering::SeqCst), 0);
    let wakes = Arc::new(WakeCount::default());
    let consumer = waker(wakes.clone());
    let mut next = Box::pin(stream.next());
    assert!(
        next.as_mut()
            .poll(&mut Context::from_waker(&consumer))
            .is_pending()
    );
    drop(next);
    let source_waker = saved
        .lock()
        .unwrap_or_else(|error| error.into_inner())
        .take()
        .ok_or("source waker missing")?;
    source_waker.wake();
    tokio::task::yield_now().await;
    assert_eq!(wakes.0.load(Ordering::SeqCst), 1);
    assert_eq!(counts.polls.load(Ordering::SeqCst), 1);
    assert_eq!(counts.drops.load(Ordering::SeqCst), 0);
    control.cancel()?;
    control.completion().await.cleanup_result()?;
    assert_eq!(counts.polls.load(Ordering::SeqCst), 1);
    assert_eq!(counts.drops.load(Ordering::SeqCst), 1);
    Ok(())
}

#[tokio::test]
async fn cancel_disposes_a_retained_unpolled_stream_once() -> TestResult {
    let source = ProbeStream::new(|_| Poll::Pending);
    let counts = source.counts.clone();
    let resources = Arc::new(TaskResourceScope::new(Handle::current()));
    let (mut stream, control) = managed(source, resources, Handle::current());
    control.cancel()?;
    control.cancel()?;
    let completion = control.completion().await;
    completion.cleanup_result()?;
    assert!(completion.result().is_err());
    assert_eq!(counts.polls.load(Ordering::SeqCst), 0);
    assert_eq!(counts.drops.load(Ordering::SeqCst), 1);
    assert!(matches!(stream.next().await, Some(Err(_))));
    assert!(stream.next().await.is_none());
    assert!(stream.next().await.is_none());
    Ok(())
}

#[test]
fn cancellation_while_a_synchronous_poll_is_held_waits_for_its_lease() -> TestResult {
    let runtime = tokio::runtime::Builder::new_current_thread().build()?;
    let (entered, entered_rx) = mpsc::sync_channel(0);
    let (release, release_rx) = mpsc::sync_channel(0);
    let source = ProbeStream::new(move |_| {
        if entered.send(()).is_err() || release_rx.recv().is_err() {
            return Poll::Ready(Some(Err(DataFusionError::Internal(
                "poll latch disconnected".into(),
            ))));
        }
        Poll::Pending
    });
    let counts = source.counts.clone();
    let resources = Arc::new(TaskResourceScope::new(runtime.handle().clone()));
    let (mut stream, control) = managed(source, resources, runtime.handle().clone());
    let consumer = std::thread::spawn(move || {
        assert!(poll_stream(&mut stream).is_pending());
        stream
    });
    entered_rx.recv()?;
    control.cancel()?;
    assert!(control.completion().now_or_never().is_none());
    assert_eq!(counts.drops.load(Ordering::SeqCst), 0);
    release.send(())?;
    let _stream = consumer.join().map_err(|_| "consumer thread panicked")?;
    runtime.block_on(control.completion()).cleanup_result()?;
    assert_eq!(counts.polls.load(Ordering::SeqCst), 1);
    assert_eq!(counts.drops.load(Ordering::SeqCst), 1);
    Ok(())
}

#[tokio::test]
async fn eof_seals_admission_and_waits_without_canceling_until_requested() -> TestResult {
    for cancel_after_eof in [false, true] {
        let resources = Arc::new(TaskResourceScope::new(Handle::current()));
        let (started, starting) = oneshot::channel();
        let (release, released) = oneshot::channel();
        resources.spawn(move |canceled| async move {
            let _ = started.send(canceled);
            released
                .await
                .map_err(|error| DataFusionError::External(Box::new(error)))?;
            Ok(())
        })?;
        let (disposed, disposal) = oneshot::channel();
        let mut source = ProbeStream::new(|_| Poll::Ready(None));
        source.disposed = Some(disposed);
        let (mut stream, control) = managed(source, resources.clone(), Handle::current());
        assert!(poll_stream(&mut stream).is_pending());
        // Current-thread execution has not yielded to the cleanup task yet.
        assert!(resources.spawn(|_| async { Ok(()) }).is_err());
        let canceled = starting.await?;
        disposal.await?;
        assert!(!*canceled.borrow());
        assert!(control.completion().now_or_never().is_none());
        if cancel_after_eof {
            control.cancel()?;
            assert!(*canceled.borrow());
            assert!(control.completion().now_or_never().is_none());
        }
        release.send(()).map_err(|_| "resource completion lost")?;
        let completion = control.completion().await;
        completion.cleanup_result()?;
        assert_eq!(completion.result().is_err(), cancel_after_eof);
        assert_eq!(*canceled.borrow(), cancel_after_eof);
        assert_eq!(stream.next().await.is_some(), cancel_after_eof);
        assert!(stream.next().await.is_none());
    }
    Ok(())
}

#[tokio::test]
async fn consumer_drop_and_abandoned_waiters_leave_shared_cleanup_owned() -> TestResult {
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
    let source = ProbeStream::new(|_| Poll::Pending);
    let counts = source.counts.clone();
    let (stream, control) = managed(source, resources, Handle::current());
    drop(stream);
    observation.await?;
    assert!(control.completion().now_or_never().is_none());
    release
        .send(())
        .map_err(|_| "cleanup was abandoned with its waiter")?;
    let (first, second) = tokio::join!(control.completion(), control.completion());
    first.cleanup_result()?;
    assert!(Arc::ptr_eq(&first, &second));
    assert!(Arc::ptr_eq(&first, &control.completion().await));
    assert_eq!(counts.polls.load(Ordering::SeqCst), 0);
    assert_eq!(counts.drops.load(Ordering::SeqCst), 1);
    Ok(())
}

#[tokio::test]
async fn poll_panic_retains_primary_and_nonretryable_cleanup_cause() -> TestResult {
    let resources = Arc::new(TaskResourceScope::new(Handle::current()));
    resources.spawn(|mut canceled| async move {
        canceled
            .wait_for(|value| *value)
            .await
            .map_err(|error| DataFusionError::External(Box::new(error)))?;
        Err(DataFusionError::External(Box::new(
            CommitStateUnknownError("commit response lost".into()),
        )))
    })?;
    let source = ProbeStream::new(|_| std::panic::resume_unwind(Box::new("source poll failure")));
    let counts = source.counts.clone();
    let (mut stream, control) = managed(source, resources, Handle::current());
    let error = stream
        .next()
        .await
        .ok_or("missing poll failure")?
        .err()
        .ok_or("poll panic was successful")?;
    assert!(error.to_string().contains("source poll failure"));
    let cause = CommonErrorCause::new::<NoPython>(&error);
    assert!(!cause.is_retryable());
    assert!(matches!(cause, CommonErrorCause::CommitStateUnknown(_)));
    let report = control.completion().await;
    let cleanup = report
        .cleanup_result()
        .err()
        .ok_or("uncertain cleanup reported success")?;
    assert!(matches!(
        CommonErrorCause::new::<NoPython>(&cleanup),
        CommonErrorCause::CommitStateUnknown(_)
    ));
    assert_eq!(counts.polls.load(Ordering::SeqCst), 1);
    assert_eq!(counts.drops.load(Ordering::SeqCst), 1);
    assert!(stream.next().await.is_none());
    Ok(())
}

#[tokio::test]
async fn drop_panic_preserves_the_structured_query_error_and_cleanup_failure() -> TestResult {
    let structured = StructuredSparkError {
        class: "java.lang.NumberFormatException".into(),
        condition: "CAST_INVALID_INPUT".into(),
        sql_state: "22018".into(),
        message: "invalid managed stream value".into(),
        parameters: HashMap::from([("expression".into(), "'bad'".into())]),
    };
    let mut error = Some(DataFusionError::External(Box::new(structured)));
    let mut source = ProbeStream::new(move |_| Poll::Ready(error.take().map(Err)));
    source.panic_drop = true;
    let counts = source.counts.clone();
    let resources = Arc::new(TaskResourceScope::new(Handle::current()));
    let (mut stream, control) = managed(source, resources, Handle::current());
    let error = stream
        .next()
        .await
        .ok_or("missing structured failure")?
        .err()
        .ok_or("query failure was successful")?;
    let CommonErrorCause::Structured(cause) = CommonErrorCause::new::<NoPython>(&error) else {
        return Err("structured primary cause lost after drop panic".into());
    };
    assert_eq!(cause.condition, "CAST_INVALID_INPUT");
    assert_eq!(cause.sql_state, "22018");
    assert_eq!(
        cause.parameters.get("expression").map(String::as_str),
        Some("'bad'")
    );
    let cleanup = control
        .completion()
        .await
        .cleanup_result()
        .err()
        .ok_or("drop panic reported clean")?;
    assert!(cleanup.to_string().contains("source drop failure"));
    assert_eq!(counts.drops.load(Ordering::SeqCst), 1);
    assert!(stream.next().await.is_none());
    Ok(())
}

struct PanicOnWakerDrop;

#[tokio::test]
async fn drop_panic_after_eof_cancels_a_supervisor_without_a_prior_query_error() -> TestResult {
    let resources = Arc::new(TaskResourceScope::new(Handle::current()));
    resources.spawn(|mut canceled| async move {
        canceled
            .wait_for(|value| *value)
            .await
            .map_err(|error| DataFusionError::External(Box::new(error)))?;
        Ok(())
    })?;
    let mut source = ProbeStream::new(|_| Poll::Ready(None));
    source.panic_drop = true;
    let counts = source.counts.clone();
    let (mut stream, control) = managed(source, resources, Handle::current());
    let error = stream
        .next()
        .await
        .ok_or("missing Drop panic")?
        .err()
        .ok_or("Drop panic reported success")?;
    assert!(error.to_string().contains("source drop failure"));
    assert!(control.completion().await.cleanup_result().is_err());
    assert_eq!(counts.polls.load(Ordering::SeqCst), 1);
    assert_eq!(counts.drops.load(Ordering::SeqCst), 1);
    assert!(stream.next().await.is_none());
    Ok(())
}

impl ArcWake for PanicOnWakerDrop {
    fn wake_by_ref(_: &Arc<Self>) {}
}

impl Drop for PanicOnWakerDrop {
    fn drop(&mut self) {
        std::panic::resume_unwind(Box::new("old consumer waker drop failure"));
    }
}

#[tokio::test]
async fn replacing_a_panicking_waker_does_not_strand_the_poll_lease() -> TestResult {
    let source = ProbeStream::new(|_| Poll::Pending);
    let counts = source.counts.clone();
    let resources = Arc::new(TaskResourceScope::new(Handle::current()));
    let (mut stream, control) = managed(source, resources, Handle::current());
    let old = waker(Arc::new(PanicOnWakerDrop));
    assert!(
        Pin::new(&mut stream)
            .poll_next(&mut Context::from_waker(&old))
            .is_pending()
    );
    drop(old); // Only the controller retains the old waker now.
    assert!(catch_unwind(AssertUnwindSafe(|| poll_stream(&mut stream))).is_err());
    let completion = control.completion().await;
    completion.cleanup_result()?;
    assert!(completion.result().is_err());
    assert_eq!(counts.polls.load(Ordering::SeqCst), 1);
    assert_eq!(counts.drops.load(Ordering::SeqCst), 1);
    Ok(())
}

#[test]
fn runtime_shutdown_before_cleanup_poll_and_during_drain_reports_uncertainty() -> TestResult {
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
        let mut source = ProbeStream::new(|_| Poll::Pending);
        source.disposed = Some(disposed);
        let counts = source.counts.clone();
        let (_stream, control) = managed(source, resources, runtime.handle().clone());
        control.cancel()?;
        if pending_drain {
            runtime.block_on(async {
                starting.await?;
                disposal.await?;
                Ok::<_, oneshot::error::RecvError>(())
            })?;
            assert_eq!(counts.drops.load(Ordering::SeqCst), 1);
        } else {
            assert_eq!(counts.drops.load(Ordering::SeqCst), 0);
        }
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
            return Err("runtime shutdown stranded cleanup completion".into());
        };
        let cleanup = report
            .cleanup_result()
            .err()
            .ok_or("interrupted cleanup reported success")?;
        assert!(
            matches!(CommonErrorCause::new::<NoPython>(&cleanup), CommonErrorCause::Internal(message) if message.contains("cleanup interrupted"))
        );
        assert!(report.result().is_err());
        assert!(!report.resources_drained);
        assert_eq!(counts.polls.load(Ordering::SeqCst), 0);
        assert_eq!(counts.drops.load(Ordering::SeqCst), 1);
        drop(release);
    }
    Ok(())
}

#[tokio::test]
async fn batches_pass_unchanged_only_on_demand_and_finish_cleanly() -> TestResult {
    let first_array: ArrayRef = Arc::new(Int32Array::from(vec![1, 2]));
    let second_array: ArrayRef = Arc::new(Int32Array::from(vec![3, 4, 5]));
    let first = RecordBatch::try_from_iter([("id", first_array.clone())])?;
    let schema = first.schema();
    let second = RecordBatch::try_new(schema.clone(), vec![second_array.clone()])?;
    let mut batches = vec![Ok(first), Ok(second)].into_iter();
    let source = ProbeStream::new(move |_| Poll::Ready(batches.next()));
    let counts = source.counts.clone();
    let resources = Arc::new(TaskResourceScope::new(Handle::current()));
    let (mut stream, control) = ManagedRecordBatchStream::new(
        schema.clone(),
        Box::pin(source),
        resources,
        Handle::current(),
    );
    assert!(Arc::ptr_eq(&stream.schema(), &schema));
    assert_eq!(counts.polls.load(Ordering::SeqCst), 0);
    for (index, expected) in [first_array, second_array].into_iter().enumerate() {
        let batch = stream.next().await.ok_or("missing batch")??;
        assert!(Arc::ptr_eq(&batch.schema(), &schema));
        assert!(Arc::ptr_eq(batch.column(0), &expected));
        assert_eq!(batch.num_rows(), expected.len());
        assert_eq!(counts.polls.load(Ordering::SeqCst), index + 1);
        assert_eq!(counts.drops.load(Ordering::SeqCst), 0);
        tokio::task::yield_now().await;
        assert_eq!(counts.polls.load(Ordering::SeqCst), index + 1);
    }
    assert!(stream.next().await.is_none());
    let completion = control.completion().await;
    completion.result()?;
    completion.cleanup_result()?;
    assert!(completion.resources_drained);
    assert_eq!(counts.polls.load(Ordering::SeqCst), 3);
    assert_eq!(counts.drops.load(Ordering::SeqCst), 1);
    assert!(stream.next().await.is_none());
    assert_eq!(counts.polls.load(Ordering::SeqCst), 3);
    Ok(())
}

#[tokio::test]
async fn resource_failure_turns_apparent_eof_into_terminal_failure() -> TestResult {
    let resources = Arc::new(TaskResourceScope::new(Handle::current()));
    resources.spawn(|canceled| async move {
        assert!(!*canceled.borrow());
        Err(DataFusionError::IoError(std::io::Error::other(
            "resource completion failed",
        )))
    })?;
    let source = ProbeStream::new(|_| Poll::Ready(None));
    let counts = source.counts.clone();
    let (mut stream, control) = managed(source, resources, Handle::current());
    let error = stream
        .next()
        .await
        .ok_or("EOF hid resource failure")?
        .err()
        .ok_or("resource failure became a batch")?;
    assert!(
        matches!(CommonErrorCause::new::<NoPython>(&error), CommonErrorCause::Io(message) if message == "resource completion failed")
    );
    let report = control.completion().await;
    assert!(report.result().is_err());
    assert!(report.resources_drained);
    let cleanup = report
        .cleanup_result()
        .err()
        .ok_or("resource failure reported clean cleanup")?;
    assert!(
        matches!(CommonErrorCause::new::<NoPython>(&cleanup), CommonErrorCause::Io(message) if message == "resource completion failed")
    );
    assert_eq!(counts.polls.load(Ordering::SeqCst), 1);
    assert_eq!(counts.drops.load(Ordering::SeqCst), 1);
    assert!(stream.next().await.is_none());
    Ok(())
}
