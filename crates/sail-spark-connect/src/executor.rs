use std::collections::VecDeque;
use std::io::Cursor;
use std::mem;
use std::ops::{Deref, DerefMut};
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, OnceLock};
use std::time::Duration;

use datafusion::arrow::array::RecordBatch;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::arrow::ipc::writer::StreamWriter;
use datafusion::execution::SendableRecordBatchStream;
use fastrace::Span;
use fastrace::future::FutureExt;
use futures::stream::StreamExt;
use futures::{Stream, future, stream};
use sail_common_datafusion::schema::{normalize_record_batch, record_batch_has_views};
use tokio::sync::{mpsc, oneshot, watch};
use tokio::task::JoinHandle;
use tonic::codegen::tokio_stream::wrappers::ReceiverStream;
use uuid::Uuid;

use crate::error::{SparkError, SparkResult, session_closed_error};
use crate::schema::to_spark_schema;
use crate::spark::connect::execute_plan_response::{ArrowBatch, SqlCommandResult};
use crate::spark::connect::{
    CheckpointCommandResult, DataType, StreamingQueryCommandResult,
    StreamingQueryManagerCommandResult, WriteStreamOperationStartResult,
};

#[derive(Clone, Debug)]
pub enum ExecutorBatch {
    Heartbeat,
    ArrowBatch(ArrowBatch),
    SqlCommandResult(Box<SqlCommandResult>),
    WriteStreamOperationStartResult(Box<WriteStreamOperationStartResult>),
    StreamingQueryCommandResult(Box<StreamingQueryCommandResult>),
    StreamingQueryManagerCommandResult(Box<StreamingQueryManagerCommandResult>),
    CheckpointCommandResult(Box<CheckpointCommandResult>),
    PipelineCommandResult(Box<crate::spark::connect::PipelineCommandResult>),
    PipelineEventResult(Box<crate::spark::connect::PipelineEventResult>),
    Schema(Box<DataType>),
    Complete,
}

#[derive(Clone, Debug)]
pub struct ExecutorOutput {
    pub(crate) id: String,
    pub(crate) batch: ExecutorBatch,
}

impl ExecutorOutput {
    pub fn new(batch: ExecutorBatch) -> Self {
        Self {
            id: Uuid::new_v4().to_string(),
            batch,
        }
    }

    pub fn complete() -> Self {
        Self::new(ExecutorBatch::Complete)
    }
}

pub type ExecutorOutputStream = Pin<Box<dyn Stream<Item = SparkResult<ExecutorOutput>> + Send>>;

struct ExecutorBuffer {
    capacity: usize,
    inner: VecDeque<ExecutorOutput>,
}

// TODO: use "spark.connect.execute.reattachable.observerRetryBufferSize"
// TODO: limit the size based on serialized message size instead of element count
const EXECUTOR_BUFFER_CAPACITY: usize = 128;

impl ExecutorBuffer {
    fn new(capacity: usize) -> Self {
        Self {
            capacity,
            inner: VecDeque::with_capacity(capacity),
        }
    }

    fn add(&mut self, output: ExecutorOutput) {
        if self.capacity == 0 {
            return;
        }
        if self.inner.len() >= self.capacity {
            self.inner.pop_front();
        }
        self.inner.push_back(output);
    }

    fn remove_until(&mut self, id: &str) {
        let index = self.inner.iter().position(|x| x.id == id);
        if let Some(index) = index {
            self.inner.drain(0..=index);
        }
    }

    fn iter(&self) -> impl Iterator<Item = &ExecutorOutput> {
        self.inner.iter()
    }
}

pub(crate) struct ExecutorMetadata {
    pub(crate) operation_id: String,
    pub(crate) tags: Vec<String>,
    pub(crate) reattachable: bool,
}

pub(crate) struct Executor {
    pub(crate) metadata: ExecutorMetadata,
    state: Mutex<ExecutorState>,
    /// Closed when the most recently started task has finished. Unlike the task handle, this
    /// is not moved out while pausing, so it cannot be lost with a caller that was canceled.
    finished: Mutex<Option<watch::Receiver<()>>>,
    closure: Arc<ExecutorClosure>,
}

enum ExecutorState {
    Idle,
    Pending {
        context: ExecutorTaskContext,
        span: Span,
    },
    Running {
        task: ExecutorTask,
        span: Span,
    },
    Completed {
        buffer: Arc<Mutex<ExecutorBuffer>>,
    },
    Pausing,
    Failed(SparkError),
    /// The session has been closed. The executor can never run again.
    Closed,
}

/// A request for the task of an executor to stop.
enum ExecutorSignal {
    /// Keep the task context, since the client may reattach.
    Pause,
    /// Discard the task context, since the session was closed.
    Close,
}

/// How a client learns that its operation was stopped by closing the session.
///
/// This does not go through the output channel: that channel can be full, and closing the
/// session must not wait for a client to read it. The client receives this after the outputs
/// that were already sent. Without this, it would see the end of the stream, as if the
/// operation had finished.
#[derive(Default)]
struct ExecutorClosure {
    /// The session, which is set when closing is requested.
    session_id: OnceLock<String>,
    /// Whether the task stopped before finishing. A task that finished is not affected.
    interrupted: AtomicBool,
}

impl ExecutorClosure {
    fn is_requested(&self) -> bool {
        self.session_id.get().is_some()
    }

    fn interrupt(&self) {
        self.interrupted.store(true, Ordering::Release);
    }

    fn error(&self) -> Option<SparkError> {
        if !self.interrupted.load(Ordering::Acquire) {
            return None;
        }
        self.session_id.get().map(|x| session_closed_error(x))
    }
}

struct ExecutorTask {
    notifier: oneshot::Sender<ExecutorSignal>,
    handle: JoinHandle<ExecutorTaskResult>,
    buffer: Arc<Mutex<ExecutorBuffer>>,
}

struct ExecutorTaskContext {
    stream: SendableRecordBatchStream,
    heartbeat_interval: Duration,
    state: ExecutorTaskState,
    buffer: Arc<Mutex<ExecutorBuffer>>,
}

pub(crate) enum ExecutorMode {
    Query,
    Outputs(ExecutorOutputStream),
    Command {
        completion: CommandCompletionHandler,
    },
}

impl ExecutorMode {
    pub(crate) fn command() -> Self {
        Self::command_with_completion(|_, _| Ok(None))
    }

    pub(crate) fn command_with_completion(
        completion: impl FnOnce(SchemaRef, Vec<RecordBatch>) -> SparkResult<Option<ExecutorOutput>>
        + Send
        + 'static,
    ) -> Self {
        Self::Command {
            completion: Box::new(completion),
        }
    }
}

pub(crate) enum ExecutorTaskState {
    Query,
    Outputs(ExecutorOutputStream),
    Command {
        batches: Vec<RecordBatch>,
        completion: Option<CommandCompletionHandler>,
    },
    Completed,
}

enum ExecutorTaskItem {
    Batch(Option<RecordBatch>),
    Heartbeat,
}

pub(crate) type CommandCompletionHandler =
    Box<dyn FnOnce(SchemaRef, Vec<RecordBatch>) -> SparkResult<Option<ExecutorOutput>> + Send>;

impl ExecutorTaskContext {
    async fn next(
        stream: &mut SendableRecordBatchStream,
        heartbeat_interval: Duration,
    ) -> SparkResult<ExecutorTaskItem> {
        let span = Span::enter_with_local_parent("ExecutorTaskContext::next");
        tokio::select! {
            batch = stream.next().in_span(span) => Ok(ExecutorTaskItem::Batch(batch.transpose()?)),
            _ = tokio::time::sleep(heartbeat_interval) => {
                // FIXME: non-reattachable clients cannot refresh session activity by releasing heartbeat responses
                Ok(ExecutorTaskItem::Heartbeat)
            }
        }
    }

    async fn send_output(
        buffer: &Arc<Mutex<ExecutorBuffer>>,
        tx: &mpsc::Sender<SparkResult<ExecutorOutput>>,
        output: ExecutorOutput,
    ) -> SparkResult<()> {
        {
            let mut buffer = buffer.lock()?;
            buffer.add(output.clone());
        }
        tx.send(Ok(output)).await?;
        Ok(())
    }
}

enum ExecutorTaskResult {
    Paused(ExecutorTaskContext),
    Failed(SparkError),
    Completed,
}

impl Executor {
    pub(crate) fn new(
        metadata: ExecutorMetadata,
        stream: SendableRecordBatchStream,
        heartbeat_interval: Duration,
        mode: ExecutorMode,
    ) -> Self {
        let state = match mode {
            ExecutorMode::Query => ExecutorTaskState::Query,
            ExecutorMode::Outputs(stream) => ExecutorTaskState::Outputs(stream),
            ExecutorMode::Command { completion } => ExecutorTaskState::Command {
                batches: vec![],
                completion: Some(completion),
            },
        };
        let buffer = if metadata.reattachable {
            ExecutorBuffer::new(EXECUTOR_BUFFER_CAPACITY)
        } else {
            ExecutorBuffer::new(0)
        };
        Self {
            metadata,
            finished: Mutex::new(None),
            closure: Default::default(),
            state: Mutex::new(ExecutorState::Pending {
                context: ExecutorTaskContext {
                    stream,
                    heartbeat_interval,
                    state,
                    buffer: Arc::new(Mutex::new(buffer)),
                },
                span: Span::enter_with_local_parent("Executor::new"),
            }),
        }
    }

    pub(crate) fn completed(metadata: ExecutorMetadata, outputs: Vec<ExecutorOutput>) -> Self {
        let mut buffer = ExecutorBuffer::new(outputs.len());
        for output in outputs {
            buffer.add(output);
        }
        Self {
            metadata,
            finished: Mutex::new(None),
            closure: Default::default(),
            state: Mutex::new(ExecutorState::Completed {
                buffer: Arc::new(Mutex::new(buffer)),
            }),
        }
    }

    /// Execute typed protocol messages with the same replay, heartbeat, and
    /// interruption lifecycle as record batches.
    pub(crate) fn with_outputs(
        metadata: ExecutorMetadata,
        outputs: ExecutorOutputStream,
        heartbeat_interval: Duration,
    ) -> Self {
        let empty = datafusion::physical_plan::stream::RecordBatchStreamAdapter::new(
            Arc::new(datafusion::arrow::datatypes::Schema::empty()),
            stream::empty(),
        );
        Self::new(
            metadata,
            Box::pin(empty),
            heartbeat_interval,
            ExecutorMode::Outputs(outputs),
        )
    }

    async fn run_internal(
        context: &mut ExecutorTaskContext,
        tx: &mpsc::Sender<SparkResult<ExecutorOutput>>,
        reattachable: bool,
    ) -> SparkResult<()> {
        let outputs = {
            let buffer = context.buffer.lock()?;
            buffer.iter().cloned().collect::<Vec<_>>()
        };
        for output in outputs {
            tx.send(Ok(output)).await?;
        }
        match &mut context.state {
            ExecutorTaskState::Outputs(stream) => loop {
                let item = tokio::select! {
                    item = stream.next() => item,
                    _ = tokio::time::sleep(context.heartbeat_interval) => {
                        Some(Ok(ExecutorOutput::new(ExecutorBatch::Heartbeat)))
                    }
                };
                let Some(item) = item else {
                    break;
                };
                ExecutorTaskContext::send_output(&context.buffer, tx, item?).await?;
            },
            ExecutorTaskState::Query => {
                let schema = to_spark_schema(context.stream.schema())?;
                ExecutorTaskContext::send_output(
                    &context.buffer,
                    tx,
                    ExecutorOutput::new(ExecutorBatch::Schema(Box::new(schema))),
                )
                .await?;

                let mut empty = true;
                loop {
                    match ExecutorTaskContext::next(&mut context.stream, context.heartbeat_interval)
                        .await?
                    {
                        ExecutorTaskItem::Batch(Some(batch)) => {
                            let batch = to_arrow_batch(&batch)?;
                            ExecutorTaskContext::send_output(
                                &context.buffer,
                                tx,
                                ExecutorOutput::new(ExecutorBatch::ArrowBatch(batch)),
                            )
                            .await?;
                            empty = false;
                        }
                        ExecutorTaskItem::Batch(None) => break,
                        ExecutorTaskItem::Heartbeat => {
                            ExecutorTaskContext::send_output(
                                &context.buffer,
                                tx,
                                ExecutorOutput::new(ExecutorBatch::Heartbeat),
                            )
                            .await?;
                        }
                    }
                }
                if empty {
                    let batch = RecordBatch::new_empty(context.stream.schema());
                    let batch = to_arrow_batch(&batch)?;
                    ExecutorTaskContext::send_output(
                        &context.buffer,
                        tx,
                        ExecutorOutput::new(ExecutorBatch::ArrowBatch(batch)),
                    )
                    .await?;
                }
            }
            ExecutorTaskState::Command {
                batches,
                completion,
            } => {
                loop {
                    match ExecutorTaskContext::next(&mut context.stream, context.heartbeat_interval)
                        .await?
                    {
                        ExecutorTaskItem::Batch(Some(batch)) => {
                            batches.push(batch);
                        }
                        ExecutorTaskItem::Batch(None) => break,
                        ExecutorTaskItem::Heartbeat => {
                            ExecutorTaskContext::send_output(
                                &context.buffer,
                                tx,
                                ExecutorOutput::new(ExecutorBatch::Heartbeat),
                            )
                            .await?;
                        }
                    }
                }
                let schema = context.stream.schema();
                let output = completion
                    .take()
                    .map(|completion| completion(schema, mem::take(batches)))
                    .transpose()?
                    .flatten();
                if let Some(output) = output {
                    ExecutorTaskContext::send_output(&context.buffer, tx, output).await?;
                }
            }
            ExecutorTaskState::Completed => return Ok(()),
        }
        // A disconnect while sending completion must only replay the buffered response.
        context.state = ExecutorTaskState::Completed;
        if reattachable {
            ExecutorTaskContext::send_output(&context.buffer, tx, ExecutorOutput::complete())
                .await?;
        }
        Ok(())
    }

    async fn run(
        mut context: ExecutorTaskContext,
        listener: oneshot::Receiver<ExecutorSignal>,
        tx: mpsc::Sender<SparkResult<ExecutorOutput>>,
        reattachable: bool,
        closure: Arc<ExecutorClosure>,
    ) -> ExecutorTaskResult {
        let out = tokio::select! {
            x = Executor::run_internal(&mut context, &tx, reattachable) => x,
            signal = listener => {
                return match signal {
                    Ok(ExecutorSignal::Close) => {
                        drop(context);
                        closure.interrupt();
                        ExecutorTaskResult::Completed
                    }
                    Ok(ExecutorSignal::Pause) | Err(_) => ExecutorTaskResult::Paused(context),
                };
            }
        };
        match out {
            Ok(()) => ExecutorTaskResult::Completed,
            // Closing the session cancels the job, so this is a consequence of that.
            Err(_) if closure.is_requested() => {
                drop(context);
                closure.interrupt();
                ExecutorTaskResult::Completed
            }
            Err(SparkError::SendError(_)) => ExecutorTaskResult::Paused(context),
            Err(e) => {
                let _ = tx.send(Err(e)).await;
                // TODO: track the original error in the task result
                ExecutorTaskResult::Failed(SparkError::internal(
                    "task failed while executing the plan",
                ))
            }
        }
    }

    pub(crate) fn start(&self) -> SparkResult<ExecutorOutputStream> {
        let mut state = self.state.lock()?;
        let (context, span) = match mem::replace(state.deref_mut(), ExecutorState::Idle) {
            ExecutorState::Pending { context, span } => (context, span),
            ExecutorState::Completed { buffer } => {
                let outputs = buffer.lock()?.iter().cloned().collect::<Vec<_>>();
                *state = ExecutorState::Completed { buffer };
                return Ok(Box::pin(stream::iter(outputs.into_iter().map(Ok))));
            }
            ExecutorState::Failed(e) => {
                *state = ExecutorState::Failed(SparkError::internal(
                    "task failed due to a previous error",
                ));
                return Err(e);
            }
            x @ ExecutorState::Idle => {
                *state = x;
                return Err(SparkError::internal("task context not found for operation"));
            }
            x @ ExecutorState::Running { .. } => {
                *state = x;
                return Err(SparkError::internal("task is already running"));
            }
            x @ ExecutorState::Pausing => {
                *state = x;
                return Err(SparkError::internal("task is being paused"));
            }
            x @ ExecutorState::Closed => {
                *state = x;
                return Err(SparkError::internal("task has been closed"));
            }
        };
        let (tx, rx) = mpsc::channel(1);
        let (notifier, listener) = oneshot::channel();
        let buffer = Arc::clone(&context.buffer);
        let reattachable = self.metadata.reattachable;
        let (finished_tx, finished) = watch::channel(());
        *self.finished.lock()? = Some(finished);
        let closure = self.closure.clone();
        let handle = {
            let span = { Span::enter_with_parent("Executor::run", &span) };
            tokio::spawn(async move {
                let _finished = finished_tx;
                Executor::run(context, listener, tx, reattachable, closure)
                    .in_span(span)
                    .await
            })
        };
        *state = ExecutorState::Running {
            task: ExecutorTask {
                notifier,
                handle,
                buffer,
            },
            span,
        };
        let closure = self.closure.clone();
        let closed =
            stream::once(async move { closure.error().map(Err) }).filter_map(future::ready);
        Ok(Box::pin(ReceiverStream::new(rx).chain(closed)))
    }

    pub(crate) async fn pause_if_running(&self) -> SparkResult<()> {
        let (task, span) = {
            let mut state = self.state.lock()?;
            match mem::replace(state.deref_mut(), ExecutorState::Idle) {
                ExecutorState::Running { task, span } => {
                    *state = ExecutorState::Pausing;
                    (task, span)
                }
                x => {
                    *state = x;
                    return Ok(());
                }
            }
        };
        let _ = task.notifier.send(ExecutorSignal::Pause);
        let next = match task.handle.await? {
            ExecutorTaskResult::Paused(context) => ExecutorState::Pending { context, span },
            ExecutorTaskResult::Completed => ExecutorState::Completed {
                buffer: task.buffer,
            },
            ExecutorTaskResult::Failed(e) => ExecutorState::Failed(e),
        };
        let mut state = self.state.lock()?;
        if !matches!(*state, ExecutorState::Closed) {
            *state = next;
        }
        Ok(())
    }

    /// Stop the executor permanently, without waiting for its task. See [`Self::closed`].
    pub(crate) fn request_close(&self, session_id: &str) -> SparkResult<()> {
        let _ = self.closure.session_id.set(session_id.to_string());
        let previous = {
            let mut state = self.state.lock()?;
            mem::replace(state.deref_mut(), ExecutorState::Closed)
        };
        match previous {
            ExecutorState::Running { task, .. } => {
                // Dropping the handle does not cancel the task, and `finished` tracks it.
                let _ = task.notifier.send(ExecutorSignal::Close);
            }
            // Dropping a pending context here disposes of its stream outside of the lock.
            ExecutorState::Pending { .. }
            | ExecutorState::Idle
            | ExecutorState::Completed { .. }
            | ExecutorState::Pausing
            | ExecutorState::Failed(_)
            | ExecutorState::Closed => {}
        }
        Ok(())
    }

    /// Wait for the task to finish after [`Self::request_close`].
    pub(crate) async fn closed(&self) -> SparkResult<()> {
        let finished = self.finished.lock()?.take();
        if let Some(mut finished) = finished {
            while finished.changed().await.is_ok() {}
        }
        Ok(())
    }

    pub(crate) fn release(&self, response_id: String) -> SparkResult<()> {
        let state = self.state.lock()?;
        let buffer = match state.deref() {
            ExecutorState::Running { task, span: _ } => &task.buffer,
            ExecutorState::Pending { context, span: _ } => &context.buffer,
            ExecutorState::Completed { buffer } => buffer,
            ExecutorState::Idle
            | ExecutorState::Failed(_)
            | ExecutorState::Pausing
            | ExecutorState::Closed => {
                return Ok(());
            }
        };
        buffer.lock()?.remove_until(&response_id);
        Ok(())
    }
}

pub(crate) fn to_arrow_batch(batch: &RecordBatch) -> SparkResult<ArrowBatch> {
    let mut output = ArrowBatch::default();
    {
        // Fallback incase view types weren't handled in planning/execution.
        let batch = if record_batch_has_views(batch) {
            // TODO: Look into passing the session's `spark.sql.execution.arrow.useLargeVarTypes` value.
            // TODO: Look into making `large_to_utf8_and_binary` configurable for Spark Connect clients that may not support `LargeUtf8` or `LargeBinary`.
            &normalize_record_batch(batch, false, false)?
        } else {
            batch
        };
        let cursor = Cursor::new(&mut output.data);
        let mut writer = StreamWriter::try_new(cursor, batch.schema().as_ref())?;
        writer.write(batch)?;
        output.row_count += batch.num_rows() as i64;
        writer.finish()?;
    }
    Ok(output)
}
