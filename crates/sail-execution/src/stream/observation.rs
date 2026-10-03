//! Default-off observations of named stream owners, never allocator reclamation.
use std::fmt;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use datafusion::arrow::array::RecordBatch;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::common::Result;
use datafusion::execution::SendableRecordBatchStream;
use datafusion::physical_plan::RecordBatchStream;
use futures::Stream;
use sail_common::telemetry::c2::{self, Guard, Identity, Outcome, Phase, Placement};

use crate::id::TaskKey;
use crate::stream::reader::{TaskStreamReader, TaskStreamSource};
use crate::stream::writer::{TaskStreamSink, TaskStreamWriteState, TaskStreamWriter};

#[derive(Clone)]
pub(crate) struct ObserverContext {
    session: Arc<str>,
    placement: Placement,
}

impl ObserverContext {
    pub(crate) fn optional(session: &str, placement: Placement) -> Option<Self> {
        c2::enabled().then(|| Self {
            session: Arc::from(session),
            placement,
        })
    }

    pub(crate) fn task(&self, key: &TaskKey) -> Scope {
        Scope::Task {
            context: self.clone(),
            key: key.clone(),
        }
    }
}

#[derive(Clone)]
pub(crate) enum Scope {
    Task {
        context: ObserverContext,
        key: TaskKey,
    },
    Job {
        session: Arc<str>,
        job: u64,
    },
    Operation {
        session: Arc<str>,
        operation: Arc<str>,
    },
}

impl Scope {
    fn identity(&self) -> Identity<'_> {
        match self {
            Self::Task { context, key } => Identity::Task {
                session: &context.session,
                job: key.job_id.into(),
                stage: key.stage,
                partition: key.partition,
                attempt: key.attempt,
                placement: context.placement,
            },
            Self::Job { session, job } => Identity::Job { session, job: *job },
            Self::Operation { session, operation } => Identity::Operation { session, operation },
        }
    }

    fn start(&self, phase: Phase, role: &'static str) -> Guard<'static> {
        let guard = Guard::start(phase, || self.identity());
        guard.detail("c2.owner_role", || role.into());
        guard
    }

    fn point(&self, phase: Phase, role: &'static str) -> Guard<'static> {
        let guard = Guard::point(phase, || self.identity());
        guard.detail("c2.owner_role", || role.into());
        guard
    }
}

/// Bounded records: one first poll/batch, one consumption terminal and one owner drop.
/// Errors do not replace inner errors; dropped open/commit/abort futures are abandoned.
/// Writes count offered inputs; they make no per-write cancellation assertion.
struct Flow {
    scope: Scope,
    role: &'static str,
    owner: Option<Guard<'static>>,
    consumption: Option<Guard<'static>>,
    polled: bool,
    batches: u64,
    rows: u64,
    counts_complete: bool,
}

impl Flow {
    fn new(scope: Scope, role: &'static str) -> Self {
        Self {
            owner: Some(scope.start(Phase::StreamOwner, role)),
            consumption: Some(scope.start(Phase::StreamConsumption, role)),
            scope,
            role,
            polled: false,
            batches: 0,
            rows: 0,
            counts_complete: true,
        }
    }

    fn declaration(scope: Scope, role: &'static str) -> Self {
        // A declaration is an owner, not a stream that failed to reach EOF.
        Self {
            owner: Some(scope.start(Phase::StreamOwner, role)),
            consumption: None,
            scope,
            role,
            polled: false,
            batches: 0,
            rows: 0,
            counts_complete: true,
        }
    }

    fn first_poll(&mut self) {
        if !self.polled {
            self.polled = true;
            self.scope
                .point(Phase::FirstPoll, self.role)
                .finish(Outcome::Succeeded);
        }
    }

    fn batch(&mut self, rows: usize) {
        if self.batches == 0 {
            self.scope
                .point(Phase::FirstBatch, self.role)
                .finish(Outcome::Succeeded);
        }
        let rows = u64::try_from(rows).unwrap_or(u64::MAX);
        match (self.batches.checked_add(1), self.rows.checked_add(rows)) {
            (Some(batches), Some(rows)) => {
                self.batches = batches;
                self.rows = rows;
            }
            _ => self.counts_complete = false,
        }
    }

    fn counts(&self, guard: &Guard<'_>) {
        guard.detail("c2.count_semantics", || {
            if self.role == "opened_shuffle_sink" {
                "offered_input_before_inner_write_returns".into()
            } else {
                "observed_successful_output_batches".into()
            }
        });
        guard.detail("c2.batches", || self.batches.to_string());
        guard.detail("c2.rows", || self.rows.to_string());
        guard.detail("c2.counts_complete", || self.counts_complete.to_string());
        guard.detail("c2.polled", || self.polled.to_string());
    }

    fn terminal(&mut self, reason: &'static str, outcome: Outcome) {
        if let Some(guard) = self.consumption.take() {
            self.counts(&guard);
            guard.detail("c2.terminal_reason", || reason.into());
            guard.finish(outcome);
        }
    }
}

impl Drop for Flow {
    fn drop(&mut self) {
        self.terminal("owner_drop_before_terminal", Outcome::Abandoned);
        if let Some(guard) = self.owner.take() {
            self.counts(&guard);
            guard.detail("c2.terminal_reason", || "named_wrapper_owner_drop".into());
            guard.finish(Outcome::Succeeded);
        }
    }
}

struct ObservedStream<S> {
    inner: S,
    flow: Flow,
}

impl<S, E> Stream for ObservedStream<S>
where
    S: Stream<Item = std::result::Result<RecordBatch, E>> + Unpin,
{
    type Item = std::result::Result<RecordBatch, E>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();
        this.flow.first_poll();
        // Exactly one original poll per outer poll, including after a terminal result.
        let result = Pin::new(&mut this.inner).poll_next(cx);
        match &result {
            Poll::Ready(Some(Ok(batch))) => this.flow.batch(batch.num_rows()),
            Poll::Ready(Some(Err(_))) => this.flow.terminal("observed_error", Outcome::Failed),
            Poll::Ready(None) => this.flow.terminal("eof", Outcome::Succeeded),
            Poll::Pending => {}
        }
        result
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        self.inner.size_hint()
    }
}

// The public functions receive a pinned boxed trait object. It implements Stream,
// but RecordBatchStream's schema method is available through deref, not a blanket
// RecordBatchStream implementation on Pin<Box<_>>.
impl RecordBatchStream for ObservedStream<SendableRecordBatchStream> {
    fn schema(&self) -> SchemaRef {
        self.inner.schema()
    }
}

pub fn operation_stream(
    inner: SendableRecordBatchStream,
    session: &str,
    operation: &str,
) -> SendableRecordBatchStream {
    if !c2::enabled() {
        return inner;
    }
    Box::pin(ObservedStream {
        inner,
        flow: Flow::new(
            Scope::Operation {
                session: Arc::from(session),
                operation: Arc::from(operation),
            },
            "executor_input_stream",
        ),
    })
}

pub(crate) fn job_stream(
    inner: SendableRecordBatchStream,
    session: &str,
    job: u64,
) -> SendableRecordBatchStream {
    if !c2::enabled() {
        return inner;
    }
    Box::pin(ObservedStream {
        inner,
        flow: Flow::new(
            Scope::Job {
                session: Arc::from(session),
                job,
            },
            "driver_output_receiver",
        ),
    })
}

pub(crate) fn task_stream(
    inner: SendableRecordBatchStream,
    context: Option<ObserverContext>,
    key: &TaskKey,
) -> SendableRecordBatchStream {
    let Some(context) = context else {
        return inner;
    };
    Box::pin(ObservedStream {
        inner,
        flow: Flow::new(context.task(key), "task_completion_stream"),
    })
}

pub(crate) fn reader(
    inner: Arc<dyn TaskStreamReader>,
    scope: Option<Scope>,
) -> Arc<dyn TaskStreamReader> {
    let Some(scope) = scope else {
        return inner;
    };
    let declaration = scope.point(Phase::ReaderDeclaration, "shuffle_reader");
    declaration.finish(Outcome::Succeeded);
    Arc::new(ObservedReader {
        inner,
        owner: Flow::declaration(scope, "reader_declaration_owner"),
    })
}

struct ObservedReader {
    inner: Arc<dyn TaskStreamReader>,
    owner: Flow,
}

impl fmt::Debug for ObservedReader {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ObservedReader")
            .field("inner", &self.inner)
            .finish()
    }
}

#[tonic::async_trait]
impl TaskStreamReader for ObservedReader {
    async fn open(&self, partition: usize) -> Result<TaskStreamSource> {
        let guard = self.owner.scope.start(Phase::ReaderOpen, "reader_open");
        guard.detail("c2.open_partition", || partition.to_string());
        let result = self.inner.open(partition).await;
        guard.finish_result(&result);
        let inner = result?;
        Ok(Box::pin(ObservedStream {
            inner,
            flow: Flow::new(self.owner.scope.clone(), "opened_shuffle_reader"),
        }))
    }
}

pub(crate) fn writer(
    inner: Arc<dyn TaskStreamWriter>,
    scope: Option<Scope>,
) -> Arc<dyn TaskStreamWriter> {
    let Some(scope) = scope else {
        return inner;
    };
    scope
        .point(Phase::WriterDeclaration, "shuffle_writer")
        .finish(Outcome::Succeeded);
    Arc::new(ObservedWriter {
        inner,
        owner: Flow::declaration(scope, "writer_declaration_owner"),
    })
}

struct ObservedWriter {
    inner: Arc<dyn TaskStreamWriter>,
    owner: Flow,
}

impl fmt::Debug for ObservedWriter {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ObservedWriter")
            .field("inner", &self.inner)
            .finish()
    }
}

#[tonic::async_trait]
impl TaskStreamWriter for ObservedWriter {
    async fn open(&self, partition: usize) -> Result<Box<dyn TaskStreamSink>> {
        let guard = self.owner.scope.start(Phase::WriterOpen, "writer_open");
        guard.detail("c2.open_partition", || partition.to_string());
        let result = self.inner.open(partition).await;
        guard.finish_result(&result);
        Ok(Box::new(ObservedSink {
            inner: result?,
            flow: Flow::new(self.owner.scope.clone(), "opened_shuffle_sink"),
        }))
    }
}

struct ObservedSink {
    inner: Box<dyn TaskStreamSink>,
    flow: Flow,
}

#[tonic::async_trait]
impl TaskStreamSink for ObservedSink {
    async fn write(&mut self, batches: Vec<Option<RecordBatch>>) -> Result<TaskStreamWriteState> {
        self.flow.first_poll();
        for batch in batches.iter().flatten() {
            self.flow.batch(batch.num_rows());
        }
        let result = self.inner.write(batches).await;
        match &result {
            Err(_) => self.flow.terminal("write_error", Outcome::Failed),
            Ok(TaskStreamWriteState::Closed) => {
                self.flow.terminal("write_closed", Outcome::Succeeded)
            }
            Ok(TaskStreamWriteState::Active) => {}
        }
        result
    }

    async fn commit(self: Box<Self>) -> Result<()> {
        let Self { inner, mut flow } = *self;
        let guard = flow.scope.start(Phase::SinkCommit, "sink_commit");
        let result = inner.commit().await;
        guard.finish_result(&result);
        flow.terminal(
            "commit_returned",
            if result.is_ok() {
                Outcome::Succeeded
            } else {
                Outcome::Failed
            },
        );
        result
    }

    async fn abort(self: Box<Self>) -> Result<()> {
        let Self { inner, mut flow } = *self;
        let guard = flow.scope.start(Phase::SinkAbort, "sink_abort");
        let result = inner.abort().await;
        guard.finish_result(&result);
        flow.terminal(
            "abort_returned",
            if result.is_ok() {
                Outcome::Succeeded
            } else {
                Outcome::Failed
            },
        );
        result
    }
}

#[cfg(test)]
#[path = "observation_tests.rs"]
mod tests;
