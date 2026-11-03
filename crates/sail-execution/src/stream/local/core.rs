use std::collections::HashMap;
use std::collections::hash_map::Entry;
use std::pin::Pin;
use std::task::{Context, Poll};

use datafusion::arrow::array::RecordBatch;
use datafusion::arrow::datatypes::SchemaRef;
use futures::Stream;
use sail_common::actor::ActorContext;
use sail_common_datafusion::error::CommonErrorCause;
use sail_python_udf::error::PyErrExtractor;
use tokio::sync::mpsc;
use tokio_util::sync::{CancellationToken, WaitForCancellationFutureOwned};
use tonic::codegen::tokio_stream::wrappers::ReceiverStream;

use crate::error::{ExecutionError, ExecutionResult};
use crate::id::{JobId, TaskKey, TaskStreamKey, TaskStreamKeyDisplay};
use crate::stream::error::{TaskStreamError, TaskStreamResult};
use crate::stream::local::memory::MemoryStream;
use crate::stream::local::options::LocalStreamManagerOptions;
use crate::stream::local::{LocalStreamManager, LocalStreamState};
use crate::stream::reader::TaskStreamSource;
use crate::stream::writer::TaskStreamChannelSink;
use crate::task_runner::{TaskRunnerActor, TaskRunnerMessage};

const CANCELED: &str = "task or job has been canceled";

/// A subscription to a created stream that ends, with the cancel cause, as
/// soon as the stream's producer is stopped. The producer's sink may still be
/// alive then (its task future dies at its next yield, and a blocking
/// preparation never yields), so the receiver alone would stay pending and a
/// Flight response built on it would hold the worker's server open past its
/// stop bound.
struct EndedStream {
    inner: TaskStreamSource,
    ended: Pin<Box<WaitForCancellationFutureOwned>>,
    done: bool,
}

impl Stream for EndedStream {
    type Item = TaskStreamResult<RecordBatch>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        if self.done {
            return Poll::Ready(None);
        }
        if self.ended.as_mut().poll(cx).is_ready() {
            self.done = true;
            let cause = CommonErrorCause::Execution(CANCELED.to_string());
            return Poll::Ready(Some(Err(TaskStreamError::from(cause))));
        }
        match self.inner.as_mut().poll_next(cx) {
            Poll::Ready(None) => {
                self.done = true;
                Poll::Ready(None)
            }
            other => other,
        }
    }
}

impl LocalStreamManager {
    pub fn new(options: LocalStreamManagerOptions) -> Self {
        Self {
            options,
            streams: HashMap::new(),
        }
    }

    pub fn create_stream(
        &mut self,
        key: TaskStreamKey,
        replicas: usize,
        _schema: SchemaRef,
    ) -> ExecutionResult<Box<dyn TaskStreamChannelSink>> {
        let create = |senders: Vec<_>| -> ExecutionResult<_> {
            let mut stream = Self::create_stream_with_senders(replicas, senders, &self.options);
            let sink = stream.publish()?;
            Ok((stream, sink))
        };

        match self.streams.entry(key.clone()) {
            Entry::Occupied(mut entry) => {
                let senders = match entry.get_mut() {
                    LocalStreamState::Created { .. } => {
                        return Err(ExecutionError::InternalError(format!(
                            "local stream {} is already created",
                            TaskStreamKeyDisplay(&key)
                        )));
                    }
                    LocalStreamState::Pending { senders } => senders,
                    LocalStreamState::Failed { cause } => {
                        return Err(ExecutionError::InternalError(format!(
                            "local stream creation has failed for {}: {}",
                            TaskStreamKeyDisplay(&key),
                            TaskStreamError::from(cause.clone())
                        )));
                    }
                };
                match create(senders.clone()) {
                    Ok((stream, sink)) => {
                        *entry.into_mut() = LocalStreamState::Created {
                            stream,
                            ended: CancellationToken::new(),
                        };
                        Ok(sink)
                    }
                    Err(e) => {
                        let cause = CommonErrorCause::new::<PyErrExtractor>(&e);
                        Self::fail_senders(senders, &cause);
                        *entry.into_mut() = LocalStreamState::Failed { cause };
                        Err(e)
                    }
                }
            }
            Entry::Vacant(entry) => match create(vec![]) {
                Ok((stream, sink)) => {
                    entry.insert(LocalStreamState::Created {
                        stream,
                        ended: CancellationToken::new(),
                    });
                    Ok(sink)
                }
                Err(e) => {
                    let cause = CommonErrorCause::new::<PyErrExtractor>(&e);
                    entry.insert(LocalStreamState::Failed { cause });
                    Err(e)
                }
            },
        }
    }

    pub fn fetch_stream(
        &mut self,
        ctx: &mut ActorContext<TaskRunnerActor>,
        key: &TaskStreamKey,
    ) -> ExecutionResult<TaskStreamSource> {
        if let Some(subscription) = self.subscribe(key) {
            return subscription;
        }
        match self.streams.entry(key.clone()) {
            Entry::Occupied(_) => Err(ExecutionError::InternalError(format!(
                "local stream {} changed state during subscription",
                TaskStreamKeyDisplay(key)
            ))),
            Entry::Vacant(entry) => {
                let (tx, rx) = mpsc::channel(self.options.task_stream_buffer);
                entry.insert(LocalStreamState::Pending { senders: vec![tx] });
                ctx.send_with_delay(
                    TaskRunnerMessage::ProbePendingLocalStream { key: key.clone() },
                    self.options.task_stream_creation_timeout,
                );
                Ok(Box::pin(ReceiverStream::new(rx)))
            }
        }
    }

    /// Subscribes to a stream the manager already knows; `None` for one it
    /// does not, which `fetch_stream` then registers as pending.
    fn subscribe(&mut self, key: &TaskStreamKey) -> Option<ExecutionResult<TaskStreamSource>> {
        let state = self.streams.get_mut(key)?;
        Some(match state {
            LocalStreamState::Created { stream, ended } => stream.subscribe().map(|inner| {
                Box::pin(EndedStream {
                    inner,
                    ended: Box::pin(ended.clone().cancelled_owned()),
                    done: false,
                }) as TaskStreamSource
            }),
            LocalStreamState::Pending { senders } => {
                let (tx, rx) = mpsc::channel(self.options.task_stream_buffer);
                senders.push(tx);
                // There is no need to probe the pending stream again.
                Ok(Box::pin(ReceiverStream::new(rx)))
            }
            LocalStreamState::Failed { cause } => Err(ExecutionError::InternalError(format!(
                "local stream creation has failed for {}: {}",
                TaskStreamKeyDisplay(key),
                TaskStreamError::from(cause.clone())
            ))),
        })
    }

    pub fn remove_streams(&mut self, job_id: JobId, stage: Option<usize>) {
        if let Some(stage) = stage {
            self.streams
                .retain(|key, _| key.job_id != job_id || key.stage != stage);
        } else {
            self.streams.retain(|key, _| key.job_id != job_id);
        }
    }

    pub fn fail_stream_if_pending(&mut self, key: &TaskStreamKey) {
        let Some(value) = self.streams.get_mut(key) else {
            return;
        };
        if let LocalStreamState::Pending { senders } = value {
            let message = "local stream is not created within the expected time".to_string();
            let cause = CommonErrorCause::Execution(message);
            Self::fail_senders(senders, &cause);
            *value = LocalStreamState::Failed { cause };
        }
    }

    /// A stopped task never creates its output streams, and stops feeding the
    /// ones it created. Readers waiting for the former learn that now, with
    /// the right cause, rather than at the creation timeout with the wrong
    /// one; subscribers of the latter end now, with the same cause, rather
    /// than when the task's future gets around to dropping its sink.
    pub fn end_task_streams(&mut self, task: &TaskKey) {
        self.end_streams(|key| {
            key.job_id == task.job_id
                && key.stage == task.stage
                && key.partition == task.partition
                && key.attempt == task.attempt
        });
    }

    /// The runner is shutting down: every stream ends for its subscribers,
    /// whether or not its producer has noticed yet.
    pub fn end_all_streams(&mut self) {
        self.end_streams(|_| true);
    }

    fn end_streams(&mut self, selected: impl Fn(&TaskStreamKey) -> bool) {
        let cause = CommonErrorCause::Execution(CANCELED.to_string());
        for (key, value) in &mut self.streams {
            if !selected(key) {
                continue;
            }
            match value {
                LocalStreamState::Pending { senders } => {
                    Self::fail_senders(senders, &cause);
                    *value = LocalStreamState::Failed {
                        cause: cause.clone(),
                    };
                }
                LocalStreamState::Created { ended, .. } => ended.cancel(),
                LocalStreamState::Failed { .. } => {}
            }
        }
    }

    pub fn fail_senders(
        senders: &[mpsc::Sender<TaskStreamResult<RecordBatch>>],
        cause: &CommonErrorCause,
    ) {
        for tx in senders {
            // `try_send` would not fail due to full buffer because we have
            // never sent any data to the channel.
            // So we do not need to spawn a task to send the error asynchronously.
            let _ = tx.try_send(Err(TaskStreamError::from(cause.clone())));
        }
    }

    fn create_stream_with_senders(
        replicas: usize,
        senders: Vec<mpsc::Sender<TaskStreamResult<RecordBatch>>>,
        options: &LocalStreamManagerOptions,
    ) -> MemoryStream {
        MemoryStream::new(options.task_stream_buffer, replicas, senders)
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::time::Duration;

    use datafusion::arrow::datatypes::Schema;
    use futures::StreamExt;

    use super::*;
    use crate::id::TaskKey;

    fn stream_key(stage: usize, channel: usize) -> TaskStreamKey {
        TaskStreamKey {
            job_id: JobId::from(1u64),
            stage,
            partition: 0,
            attempt: 0,
            channel,
        }
    }

    #[tokio::test]
    async fn a_stopped_task_fails_its_pending_streams_with_the_cancel_cause() {
        let mut manager = LocalStreamManager::new(LocalStreamManagerOptions::for_tests());
        let (tx, mut rx) = mpsc::channel(1);
        let (other_tx, mut other_rx) = mpsc::channel(1);
        let stopped = stream_key(0, 2);
        let other = stream_key(1, 0);
        manager.streams.insert(
            stopped.clone(),
            LocalStreamState::Pending { senders: vec![tx] },
        );
        manager.streams.insert(
            other.clone(),
            LocalStreamState::Pending {
                senders: vec![other_tx],
            },
        );

        manager.end_task_streams(&TaskKey {
            job_id: JobId::from(1u64),
            stage: 0,
            partition: 0,
            attempt: 0,
        });

        let item = rx.recv().await;
        assert!(
            item.as_ref()
                .is_some_and(|item| format!("{item:?}").contains("canceled")),
            "{item:?}"
        );
        assert!(matches!(
            manager.streams.get(&stopped),
            Some(LocalStreamState::Failed { .. })
        ));
        // Another task's stream is untouched and still waits for its creator.
        assert!(matches!(
            manager.streams.get(&other),
            Some(LocalStreamState::Pending { .. })
        ));
        assert!(other_rx.try_recv().is_err());
    }

    fn task(stage: usize) -> TaskKey {
        TaskKey {
            job_id: JobId::from(1u64),
            stage,
            partition: 0,
            attempt: 0,
        }
    }

    async fn next_is_canceled(stream: &mut TaskStreamSource) {
        let item = stream.next().await;
        assert!(
            item.as_ref()
                .is_some_and(|item| format!("{item:?}").contains("canceled")),
            "{item:?}"
        );
        assert!(stream.next().await.is_none());
    }

    #[tokio::test]
    async fn a_stopped_task_ends_its_created_streams_for_their_subscribers()
    -> Result<(), Box<dyn std::error::Error>> {
        let mut manager = LocalStreamManager::new(LocalStreamManagerOptions::for_tests());
        let schema = Arc::new(Schema::empty());
        let stopped = stream_key(0, 0);
        let other = stream_key(1, 0);
        // The sinks stay alive throughout: a producer whose future has not
        // died yet, or never yields, must not keep its subscribers waiting.
        let _stopped_sink = manager.create_stream(stopped.clone(), 2, schema.clone())?;
        let _other_sink = manager.create_stream(other.clone(), 1, schema)?;
        let mut before = manager.subscribe(&stopped).transpose()?.ok_or("stream")?;
        let mut untouched = manager.subscribe(&other).transpose()?.ok_or("stream")?;

        manager.end_task_streams(&task(0));

        next_is_canceled(&mut before).await;
        // A subscriber arriving after the stop ends at once, with the cause.
        let mut after = manager.subscribe(&stopped).transpose()?.ok_or("stream")?;
        next_is_canceled(&mut after).await;
        // Another task's subscribers still wait for their producer.
        assert!(
            tokio::time::timeout(Duration::from_millis(50), untouched.next())
                .await
                .is_err()
        );

        manager.end_all_streams();
        next_is_canceled(&mut untouched).await;
        Ok(())
    }
}
