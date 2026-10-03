use std::collections::VecDeque;
use std::sync::atomic::{AtomicUsize, Ordering};

use datafusion::arrow::array::{ArrayRef, Int64Array};
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::common::DataFusionError;
use futures::task::noop_waker;

use super::*;

type Item = Option<Result<RecordBatch>>;

struct Inner {
    schema: SchemaRef,
    polls: Arc<AtomicUsize>,
    outputs: VecDeque<Poll<Item>>,
}

impl Stream for Inner {
    type Item = Result<RecordBatch>;

    fn poll_next(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let inner = self.get_mut();
        inner.polls.fetch_add(1, Ordering::SeqCst);
        inner.outputs.pop_front().unwrap_or(Poll::Ready(None))
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        (0, Some(self.outputs.len()))
    }
}

impl RecordBatchStream for Inner {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
}

fn scope() -> Scope {
    Scope::Job {
        session: Arc::from("session"),
        job: 23,
    }
}

fn schema() -> SchemaRef {
    Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]))
}

#[test]
fn observed_stream_preserves_each_poll_backpressure_batch_error_and_eof() -> Result<()> {
    let schema = schema();
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![Arc::new(Int64Array::from(vec![i64::MIN, 1, i64::MAX])) as ArrayRef],
    )?;
    let polls = Arc::new(AtomicUsize::new(0));
    let inner = Inner {
        schema: schema.clone(),
        polls: polls.clone(),
        outputs: VecDeque::from([
            Poll::Pending,
            Poll::Ready(Some(Ok(batch.clone()))),
            Poll::Ready(Some(Err(DataFusionError::Execution(
                "original sentinel".into(),
            )))),
            Poll::Ready(None),
        ]),
    };
    let inner: SendableRecordBatchStream = Box::pin(inner);
    let mut observed = ObservedStream {
        inner,
        flow: Flow::new(scope(), "test_reader"),
    };
    assert_eq!(observed.schema(), schema);
    assert_eq!(observed.size_hint(), (0, Some(4)));
    let waker = noop_waker();
    let mut context = Context::from_waker(&waker);
    assert!(Pin::new(&mut observed).poll_next(&mut context).is_pending());
    assert_eq!(polls.load(Ordering::SeqCst), 1);
    assert_eq!(observed.size_hint(), (0, Some(3)));
    assert!(observed.flow.polled);
    assert_eq!(observed.flow.batches, 0);
    assert!(matches!(Pin::new(&mut observed).poll_next(&mut context),
        Poll::Ready(Some(Ok(actual))) if actual == batch));
    assert_eq!(observed.flow.rows, 3);
    assert_eq!(observed.flow.batches, 1);
    assert!(matches!(Pin::new(&mut observed).poll_next(&mut context),
        Poll::Ready(Some(Err(error))) if error.to_string().contains("original sentinel")));
    assert!(observed.flow.consumption.is_none());
    assert!(matches!(
        Pin::new(&mut observed).poll_next(&mut context),
        Poll::Ready(None)
    ));
    assert_eq!(polls.load(Ordering::SeqCst), 4);
    Ok(())
}

#[test]
fn pinned_record_batch_wrapper_keeps_sendable_schema_and_unpolled_drop() {
    let original_schema = schema();
    let polls = Arc::new(AtomicUsize::new(0));
    let inner: SendableRecordBatchStream = Box::pin(Inner {
        schema: original_schema.clone(),
        polls: polls.clone(),
        outputs: VecDeque::from([Poll::Pending]),
    });
    let wrapped: SendableRecordBatchStream = Box::pin(ObservedStream {
        inner,
        flow: Flow::new(scope(), "test_pinned_reader"),
    });
    assert!(Arc::ptr_eq(&wrapped.schema(), &original_schema));
    assert_eq!(wrapped.size_hint(), (0, Some(1)));
    drop(wrapped);
    assert_eq!(polls.load(Ordering::SeqCst), 0);
}

#[test]
fn declaration_owner_has_no_consumption_or_first_poll_to_abandon() {
    let flow = Flow::declaration(scope(), "reader_declaration_owner");
    assert!(flow.owner.is_some());
    assert!(flow.consumption.is_none());
    assert!(!flow.polled);
    assert_eq!(flow.batches, 0);
}

#[test]
fn unpolled_stream_drop_does_not_poll_original() {
    let polls = Arc::new(AtomicUsize::new(0));
    let stream = ObservedStream {
        inner: Inner {
            schema: schema(),
            polls: polls.clone(),
            outputs: VecDeque::new(),
        },
        flow: Flow::new(scope(), "test_reader"),
    };
    drop(stream);
    assert_eq!(polls.load(Ordering::SeqCst), 0);
}

#[test]
fn saturating_counts_are_explicitly_incomplete_and_never_wrap() {
    let mut flow = Flow::new(scope(), "test_reader");
    flow.rows = u64::MAX;
    flow.batches = u64::MAX;
    flow.batch(1);
    assert!(!flow.counts_complete);
    assert_eq!(flow.rows, u64::MAX);
    assert_eq!(flow.batches, u64::MAX);
}

struct InnerSink {
    writes: Arc<AtomicUsize>,
    commits: Arc<AtomicUsize>,
    aborts: Arc<AtomicUsize>,
}

#[tonic::async_trait]
impl TaskStreamSink for InnerSink {
    async fn write(&mut self, _: Vec<Option<RecordBatch>>) -> Result<TaskStreamWriteState> {
        self.writes.fetch_add(1, Ordering::SeqCst);
        Err(DataFusionError::Execution("original write error".into()))
    }

    async fn commit(self: Box<Self>) -> Result<()> {
        self.commits.fetch_add(1, Ordering::SeqCst);
        Err(DataFusionError::Execution("original commit error".into()))
    }

    async fn abort(self: Box<Self>) -> Result<()> {
        self.aborts.fetch_add(1, Ordering::SeqCst);
        Err(DataFusionError::Execution("original abort error".into()))
    }
}

#[tokio::test]
async fn observed_sink_preserves_write_error_and_following_abort_once() {
    let writes = Arc::new(AtomicUsize::new(0));
    let commits = Arc::new(AtomicUsize::new(0));
    let aborts = Arc::new(AtomicUsize::new(0));
    let mut sink = ObservedSink {
        inner: Box::new(InnerSink {
            writes: writes.clone(),
            commits: commits.clone(),
            aborts: aborts.clone(),
        }),
        flow: Flow::new(scope(), "opened_shuffle_sink"),
    };
    let result = sink
        .write(vec![Some(RecordBatch::new_empty(schema()))])
        .await;
    assert!(matches!(result, Err(error) if error.to_string().contains("original write error")));
    assert!(sink.flow.consumption.is_none());
    assert_eq!(sink.flow.batches, 1);
    let result = Box::new(sink).abort().await;
    assert!(matches!(result, Err(error) if error.to_string().contains("original abort error")));
    assert_eq!(writes.load(Ordering::SeqCst), 1);
    assert_eq!(commits.load(Ordering::SeqCst), 0);
    assert_eq!(aborts.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn observed_sink_preserves_commit_error_without_adding_abort() {
    let writes = Arc::new(AtomicUsize::new(0));
    let commits = Arc::new(AtomicUsize::new(0));
    let aborts = Arc::new(AtomicUsize::new(0));
    let sink = ObservedSink {
        inner: Box::new(InnerSink {
            writes: writes.clone(),
            commits: commits.clone(),
            aborts: aborts.clone(),
        }),
        flow: Flow::new(scope(), "opened_shuffle_sink"),
    };
    let result = Box::new(sink).commit().await;
    assert!(matches!(result, Err(error) if error.to_string().contains("original commit error")));
    assert_eq!(writes.load(Ordering::SeqCst), 0);
    assert_eq!(commits.load(Ordering::SeqCst), 1);
    assert_eq!(aborts.load(Ordering::SeqCst), 0);
}
