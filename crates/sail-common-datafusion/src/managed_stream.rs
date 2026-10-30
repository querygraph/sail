//! Shared ownership of a prepared stream and its registered resource cleanup.
//!
//! Upstream polling stays on the consumer's thread, with no prefetch. A control
//! handle can cancel even if the consumer retains an unpolled stream. Completion
//! qualifies stream disposal and registered resource drainage only: it does not
//! join unregistered tasks or establish runner admission/session shutdown.

use std::any::Any;
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::pin::Pin;
use std::sync::{Arc, Mutex, MutexGuard};
use std::task::{Context, Poll, Waker};

use arrow::datatypes::SchemaRef;
use arrow::record_batch::RecordBatch;
use datafusion::execution::SendableRecordBatchStream;
use datafusion::physical_plan::RecordBatchStream;
use datafusion_common::{DataFusionError, Result};
use futures::future::BoxFuture;
use futures::{FutureExt, Stream};
use tokio::runtime::Handle;
use tokio::sync::watch;

use crate::task_resources::{ResourceDrainReport, TaskResourceScope};

/// Query outcome and cleanup qualification are deliberately separate.
#[derive(Debug)]
pub struct StreamCompletion {
    pub error: Option<Arc<DataFusionError>>,
    pub cleanup_errors: Vec<Arc<DataFusionError>>,
}

impl StreamCompletion {
    pub fn result(&self) -> Result<()> {
        match &self.error {
            Some(error) => Err(DataFusionError::Shared(error.clone())),
            None => Ok(()),
        }
    }

    pub fn cleanup_result(&self) -> Result<()> {
        match combined_error(self.cleanup_errors.to_vec()) {
            Some(error) => Err(DataFusionError::Shared(error)),
            None => Ok(()),
        }
    }
}

enum Phase {
    Ready(SendableRecordBatchStream),
    Polling,
    Draining,
    Terminal,
}

struct State {
    phase: Phase,
    canceled: bool,
    consumer: Option<Waker>,
}

struct Inner {
    state: Mutex<State>,
    resources: Arc<TaskResourceScope>,
    runtime: Handle,
    completion: watch::Sender<Option<Arc<StreamCompletion>>>,
}

/// Cloning or dropping a control handle does not cancel its stream.
#[derive(Clone)]
pub struct ManagedStreamControl(Arc<Inner>);

impl ManagedStreamControl {
    /// Latch cancellation, seal resource admission, and initiate disposal.
    /// A synchronous upstream poll must return before its lease can be disposed.
    pub fn cancel(&self) -> Result<()> {
        self.0.cancel()
    }

    /// Waiters do not own cleanup. Dropping one leaves cleanup running, and
    /// repeated/concurrent waiters observe the same immutable report.
    pub async fn completion(&self) -> Arc<StreamCompletion> {
        let mut receiver = self.0.completion.subscribe();
        loop {
            if let Some(report) = receiver.borrow_and_update().clone() {
                return report;
            }
            // This control retains the sender throughout the wait.
            let _ = receiver.changed().await;
        }
    }
}

pub struct ManagedRecordBatchStream {
    schema: SchemaRef,
    control: ManagedStreamControl,
    completion: BoxFuture<'static, Arc<StreamCompletion>>,
    terminal: bool,
}

impl ManagedRecordBatchStream {
    /// The caller supplies the already-known schema so construction invokes no
    /// upstream methods. Preparation/admission must be owned by the caller.
    pub fn new(
        schema: SchemaRef,
        stream: SendableRecordBatchStream,
        resources: Arc<TaskResourceScope>,
        runtime: Handle,
    ) -> (Self, ManagedStreamControl) {
        let control = ManagedStreamControl(Arc::new(Inner {
            state: Mutex::new(State {
                phase: Phase::Ready(stream),
                canceled: false,
                consumer: None,
            }),
            resources,
            runtime,
            completion: watch::channel(None).0,
        }));
        let waiter = control.clone();
        (
            Self {
                schema,
                control: control.clone(),
                completion: async move { waiter.completion().await }.boxed(),
                terminal: false,
            },
            control,
        )
    }
}

impl RecordBatchStream for ManagedRecordBatchStream {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
}

impl Stream for ManagedRecordBatchStream {
    type Item = Result<RecordBatch>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();
        if this.terminal {
            return Poll::Ready(None);
        }
        if let Some(mut lease) = this.control.0.lease(cx.waker()) {
            let next = catch_unwind(AssertUnwindSafe(|| lease.poll(cx)))
                .unwrap_or_else(|panic| Poll::Ready(Some(Err(panic_error("poll", panic)))));
            if let Some(next) = lease.release(next) {
                return next;
            }
        }
        match this.completion.as_mut().poll(cx) {
            Poll::Pending => Poll::Pending,
            Poll::Ready(report) => {
                this.terminal = true;
                Poll::Ready(report.result().err().map(Err))
            }
        }
    }
}

impl Drop for ManagedRecordBatchStream {
    fn drop(&mut self) {
        if !self.terminal {
            let _ = self.control.cancel();
        }
    }
}

impl Inner {
    fn lock(&self) -> MutexGuard<'_, State> {
        // No upstream code, wake callbacks, or stream destructors run under
        // this lock. Recover ownership rather than strand a stream on poison.
        self.state.lock().unwrap_or_else(|error| error.into_inner())
    }

    fn lease(self: &Arc<Self>, waker: &Waker) -> Option<PollLease> {
        let waker = waker.clone();
        let (stream, previous) = {
            let mut state = self.lock();
            let previous = state.consumer.replace(waker);
            let stream = if matches!(state.phase, Phase::Ready(_)) {
                match std::mem::replace(&mut state.phase, Phase::Polling) {
                    Phase::Ready(stream) => Some(stream),
                    _ => None,
                }
            } else {
                None
            };
            (stream, previous)
        };
        let lease = stream.map(|stream| PollLease {
            owner: self.clone(),
            stream: Some(stream),
        });
        drop(previous);
        lease
    }

    fn cancel(self: &Arc<Self>) -> Result<()> {
        let (stream, consumer) = {
            let mut state = self.lock();
            if matches!(state.phase, Phase::Terminal) {
                return Ok(());
            }
            state.canceled = true;
            let stream = if matches!(state.phase, Phase::Ready(_)) {
                match std::mem::replace(&mut state.phase, Phase::Draining) {
                    Phase::Ready(stream) => Some(stream),
                    _ => None,
                }
            } else {
                None
            };
            (stream, state.consumer.take())
        };
        let result = match stream {
            Some(stream) => self.drain(stream, None),
            None => self.resources.cancel(),
        };
        if let Some(consumer) = consumer {
            consumer.wake();
        }
        result
    }

    fn drain(
        self: &Arc<Self>,
        stream: SendableRecordBatchStream,
        error: Option<DataFusionError>,
    ) -> Result<()> {
        // Construct the guard before spawning: dropping a never-polled task on
        // runtime shutdown must still dispose and publish unresolved cleanup.
        let mut cleanup = Cleanup {
            owner: self.clone(),
            stream: Some(stream),
            primary: error.map(Arc::new),
            failures: vec![],
            published: false,
        };
        let admission = if cleanup.primary.is_some() || self.lock().canceled {
            self.resources.cancel()
        } else {
            self.resources.seal()
        };
        let error = admission.err().map(Arc::new);
        if let Some(error) = &error {
            cleanup.failures.push(error.clone());
        }
        drop(self.runtime.spawn(cleanup.run()));
        match error {
            Some(error) => Err(DataFusionError::Shared(error)),
            None => Ok(()),
        }
    }
}

struct PollLease {
    owner: Arc<Inner>,
    stream: Option<SendableRecordBatchStream>,
}

impl PollLease {
    fn poll(&mut self, cx: &mut Context<'_>) -> Poll<Option<Result<RecordBatch>>> {
        match &mut self.stream {
            Some(stream) => stream.as_mut().poll_next(cx),
            None => Poll::Ready(None),
        }
    }

    /// None means the consumer must wait for cleanup-qualified completion.
    fn release(
        mut self,
        next: Poll<Option<Result<RecordBatch>>>,
    ) -> Option<Poll<Option<Result<RecordBatch>>>> {
        let mut state = self.owner.lock();
        if !state.canceled && matches!(next, Poll::Pending | Poll::Ready(Some(Ok(_)))) {
            let stream = self.stream.take()?;
            state.phase = Phase::Ready(stream);
            return Some(next);
        }
        state.phase = Phase::Draining;
        drop(state);
        let error = match next {
            Poll::Ready(Some(Err(error))) => Some(error),
            _ => None,
        };
        let stream = self.stream.take()?;
        let _ = self.owner.drain(stream, error);
        None
    }
}

impl Drop for PollLease {
    fn drop(&mut self) {
        if let Some(stream) = self.stream.take() {
            self.owner.lock().phase = Phase::Draining;
            let _ = self.owner.drain(
                stream,
                Some(DataFusionError::Internal(
                    "managed stream poll handoff interrupted".into(),
                )),
            );
        }
    }
}

struct Cleanup {
    owner: Arc<Inner>,
    stream: Option<SendableRecordBatchStream>,
    primary: Option<Arc<DataFusionError>>,
    failures: Vec<Arc<DataFusionError>>,
    published: bool,
}

impl Cleanup {
    fn cancel_resources(&mut self) {
        if let Err(error) = self.owner.resources.cancel() {
            self.failures.push(Arc::new(error));
        }
    }

    fn dispose(&mut self) {
        if self.primary.is_some() || self.owner.lock().canceled {
            self.cancel_resources();
        }
        if let Some(stream) = self.stream.take()
            && let Err(panic) = catch_unwind(AssertUnwindSafe(|| drop(stream)))
        {
            self.failures.push(Arc::new(panic_error("drop", panic)));
            self.cancel_resources();
        }
    }

    async fn run(mut self) {
        self.dispose();
        match self.owner.resources.drain().await {
            Ok(report) => {
                self.failures
                    .extend(report.failures.iter().enumerate().map(|(index, failure)| {
                        Arc::new(DataFusionError::Context(
                            format!("resource {} completion or cleanup", failure.id.0),
                            Box::new(DataFusionError::External(Box::new(ResourceError {
                                report: report.clone(),
                                index,
                            }))),
                        ))
                    }));
            }
            Err(error) => self.failures.push(Arc::new(error)),
        }
        self.publish();
    }

    fn publish(&mut self) {
        let (canceled, consumer) = {
            let mut state = self.owner.lock();
            state.phase = Phase::Terminal;
            (state.canceled, state.consumer.take())
        };
        if canceled && self.primary.is_none() {
            self.primary = Some(Arc::new(DataFusionError::Execution(
                "managed stream canceled".into(),
            )));
        }
        let error = combined_error(
            self.primary
                .iter()
                .chain(self.failures.iter())
                .cloned()
                .collect(),
        );
        self.published = true;
        self.owner
            .completion
            .send_replace(Some(Arc::new(StreamCompletion {
                error,
                cleanup_errors: std::mem::take(&mut self.failures),
            })));
        if let Some(consumer) = consumer {
            consumer.wake();
        }
    }
}

/// Keep the original non-Clone error alive without flattening its typed cause.
#[derive(Debug)]
struct ResourceError {
    report: Arc<ResourceDrainReport>,
    index: usize,
}

impl std::fmt::Display for ResourceError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        std::fmt::Display::fmt(&self.report.failures[self.index].error, formatter)
    }
}

impl std::error::Error for ResourceError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(&self.report.failures[self.index].error)
    }
}

impl Drop for Cleanup {
    fn drop(&mut self) {
        if !self.published {
            self.cancel_resources();
            self.dispose();
            self.failures.push(Arc::new(DataFusionError::Internal(
                "managed stream cleanup interrupted before drainage completed".into(),
            )));
            self.publish();
        }
    }
}

fn combined_error(mut errors: Vec<Arc<DataFusionError>>) -> Option<Arc<DataFusionError>> {
    match errors.len() {
        0 => None,
        1 => errors.pop(),
        _ => Some(Arc::new(DataFusionError::Collection(
            errors.into_iter().map(DataFusionError::Shared).collect(),
        ))),
    }
}

fn panic_error(operation: &str, payload: Box<dyn Any + Send>) -> DataFusionError {
    let message = payload
        .downcast_ref::<&str>()
        .copied()
        .or_else(|| payload.downcast_ref::<String>().map(String::as_str))
        .unwrap_or("unknown panic");
    DataFusionError::Internal(format!("managed stream {operation} panicked: {message}"))
}

#[cfg(test)]
#[path = "managed_stream_tests.rs"]
mod tests;
