//! Multipart lifetime supervision for an execution owner, without spawning tasks.
//!
//! Register [`MultipartSupervisor::run`] with a retained owner before awaiting
//! creation. Its runtime must outlive settlement. This component neither installs
//! object-store routing nor qualifies provider-internal detached work. In
//! particular it does not turn the legacy runtime wrapper into a session barrier.

mod supervisor;
#[cfg(test)]
mod tests;

use std::fmt;
use std::num::NonZeroUsize;
use std::sync::{Arc, Mutex};

use futures::FutureExt;
use futures::future::{BoxFuture, Shared};
use object_store::{MultipartUpload, PutPayload, PutResult};
use tokio::sync::{OwnedSemaphorePermit, Semaphore, mpsc, oneshot, watch};

/// Provider operation whose original typed error is retained.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Stage {
    Create,
    Part,
    Complete,
    Abort,
}

#[derive(Clone, Debug)]
pub struct ProviderFailure {
    pub stage: Stage,
    pub error: Arc<object_store::Error>,
}

#[derive(Clone, Debug)]
pub enum MultipartError {
    Closed,
    AtCapacity,
    InvalidCapacity,
    Interrupted,
    /// All observed provider causes, including both part and abort failures.
    /// A Complete cause has uncertain visibility; it is never a clean abort.
    Provider(Arc<Vec<ProviderFailure>>),
}

impl MultipartError {
    pub fn causes(&self) -> &[ProviderFailure] {
        match self {
            Self::Provider(errors) => errors,
            _ => &[],
        }
    }
}

impl fmt::Display for MultipartError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Closed => f.write_str("multipart data admission is closed"),
            Self::AtCapacity => f.write_str("multipart part admission is at capacity"),
            Self::InvalidCapacity => f.write_str("multipart capacity exceeds the semaphore limit"),
            Self::Interrupted => f.write_str("multipart supervisor interrupted"),
            Self::Provider(errors) => {
                for (index, error) in errors.iter().enumerate() {
                    if index != 0 {
                        f.write_str("; ")?;
                    }
                    write!(f, "{:?}: {}", error.stage, error.error)?;
                }
                Ok(())
            }
        }
    }
}

impl std::error::Error for MultipartError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        self.causes().first().map(|cause| cause.error.as_ref() as _)
    }
}

type Result<T> = std::result::Result<T, MultipartError>;

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Disposition {
    Committed(PutResult),
    Aborted,
}

/// Does not keep data admission open. Dropping an observer never cancels work.
pub type Completion = Shared<BoxFuture<'static, Result<Disposition>>>;

#[derive(Debug)]
pub struct Creation(oneshot::Receiver<Result<MultipartHandle>>);

impl Creation {
    pub async fn wait(self) -> Result<MultipartHandle> {
        self.0.await.map_err(|_| MultipartError::Interrupted)?
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Admission {
    Open,
    Completing,
    Closed,
}

enum Command {
    Part(
        PutPayload,
        oneshot::Sender<Result<()>>,
        OwnedSemaphorePermit,
    ),
    Complete,
    Abort,
}

/// A command handle, never the raw provider upload. Parts are admitted in call
/// order, independently of the polling order of their result observers. Each
/// result must be checked; at capacity a part is rejected without being queued.
/// The bound is a part count, not a byte-memory budget.
#[derive(Debug)]
pub struct MultipartHandle {
    commands: mpsc::UnboundedSender<Command>,
    admission: Arc<Mutex<Admission>>,
    capacity: Arc<Semaphore>,
    canceled: watch::Receiver<bool>,
    completion: Completion,
}

impl MultipartHandle {
    // &mut self serializes caller submissions. Notify only after releasing the
    // shared admission lock: channel wakers may reenter the supervisor.
    pub fn put_part(&mut self, payload: PutPayload) -> Result<BoxFuture<'static, Result<()>>> {
        let state = self
            .admission
            .lock()
            .map_err(|_| MultipartError::Interrupted)?;
        if *state != Admission::Open || *self.canceled.borrow() {
            return Err(MultipartError::Closed);
        }
        let permit = self
            .capacity
            .clone()
            .try_acquire_owned()
            .map_err(|_| MultipartError::AtCapacity)?;
        drop(state);
        let (reply, result) = oneshot::channel();
        self.commands
            .send(Command::Part(payload, reply, permit))
            .map_err(|_| MultipartError::Interrupted)?;
        Ok(async move { result.await.map_err(|_| MultipartError::Interrupted)? }.boxed())
    }

    /// Synchronously fence further parts and admit completion. Once admitted,
    /// cancellation/observer loss cannot retract commit. Part failure still
    /// prevents completion and leads to joined abort. Unpolled result observers
    /// do not defer this admission.
    pub fn complete(&mut self) -> Result<Completion> {
        let mut state = self
            .admission
            .lock()
            .map_err(|_| MultipartError::Interrupted)?;
        if *state != Admission::Open || *self.canceled.borrow() {
            return Err(MultipartError::Closed);
        }
        *state = Admission::Completing;
        drop(state);
        self.commands
            .send(Command::Complete)
            .map_err(|_| MultipartError::Interrupted)?;
        Ok(self.completion.clone())
    }

    /// Fence new data and request abort, retaining settlement in the supervisor.
    /// Once completion is admitted, abort observes that same terminal outcome;
    /// it cannot race a second provider disposition against commit.
    pub fn abort(&mut self) -> Result<Completion> {
        let mut state = self
            .admission
            .lock()
            .map_err(|_| MultipartError::Interrupted)?;
        let notify = *state == Admission::Open;
        if notify {
            *state = Admission::Closed;
        }
        drop(state);
        if notify {
            // The supervisor may already have observed Closed and settled.
            // Its shared outcome also reports interruption if its task was lost.
            let _ = self.commands.send(Command::Abort);
        }
        Ok(self.completion.clone())
    }

    /// Observe eventual commit, abort or failure without keeping the handle alive.
    pub fn outcome(&self) -> Completion {
        self.completion.clone()
    }
}

type Factory =
    Box<dyn FnOnce() -> BoxFuture<'static, object_store::Result<Box<dyn MultipartUpload>>> + Send>;

pub struct MultipartSupervisor {
    create: Factory,
    ready: oneshot::Sender<Result<MultipartHandle>>,
    capacity: NonZeroUsize,
}

/// Prepare without invoking the factory or spawning anything. Register the
/// supervisor with the execution owner, adapting its typed error there, then
/// await Creation. If registration fails, drop both values. Losing Creation
/// before or during provider creation cannot lose cleanup ownership.
pub fn prepare<F, Fut>(create: F, capacity: NonZeroUsize) -> (MultipartSupervisor, Creation)
where
    F: FnOnce() -> Fut + Send + 'static,
    Fut: std::future::Future<Output = object_store::Result<Box<dyn MultipartUpload>>>
        + Send
        + 'static,
{
    let (ready, result) = oneshot::channel();
    (
        MultipartSupervisor {
            create: Box::new(move || create().boxed()),
            ready,
            capacity,
        },
        Creation(result),
    )
}
