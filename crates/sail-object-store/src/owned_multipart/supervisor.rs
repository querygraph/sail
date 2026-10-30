use futures::stream::FuturesUnordered;
use futures::{FutureExt, StreamExt};

use super::*;

type Parts = FuturesUnordered<BoxFuture<'static, Option<ProviderFailure>>>;

impl MultipartSupervisor {
    /// Run once inside an already-admitted retained task, on the provider's
    /// required runtime. Never race/drop creation, parts, complete or abort
    /// against caller cancellation. A stalled provider means pending drainage.
    pub async fn run(self, canceled: watch::Receiver<bool>) -> Result<()> {
        if self.capacity.get() > Semaphore::MAX_PERMITS {
            let _ = self.ready.send(Err(MultipartError::InvalidCapacity));
            return Err(MultipartError::InvalidCapacity);
        }
        if *canceled.borrow() || self.ready.is_closed() {
            let _ = self.ready.send(Err(MultipartError::Closed));
            return Ok(());
        }
        let mut upload = match (self.create)().await {
            Ok(upload) => upload,
            Err(error) => {
                let error = failures(vec![cause(Stage::Create, error)]);
                let _ = self.ready.send(Err(error.clone()));
                return Err(error);
            }
        };
        if *canceled.borrow() || self.ready.is_closed() {
            let result = upload
                .abort()
                .await
                .map_err(|error| failures(vec![cause(Stage::Abort, error)]));
            let _ = self
                .ready
                .send(Err(result.clone().err().unwrap_or(MultipartError::Closed)));
            return result;
        }
        let (commands, receiver) = mpsc::unbounded_channel();
        let admission = Arc::new(Mutex::new(Admission::Open));
        let (terminal, outcome) = oneshot::channel();
        let completion = async move { outcome.await.map_err(|_| MultipartError::Interrupted)? }
            .boxed()
            .shared();
        let handle = MultipartHandle {
            commands,
            admission: admission.clone(),
            capacity: Arc::new(Semaphore::new(self.capacity.get())),
            canceled: canceled.clone(),
            completion,
        };
        // If the receiver vanished, send drops the command handle; the loop
        // observes channel closure and aborts the provider before returning.
        let _ = self.ready.send(Ok(handle));
        let result = supervise(upload, receiver, admission, canceled).await;
        let _ = terminal.send(result.clone());
        result.map(|_| ())
    }
}

fn cause(stage: Stage, error: object_store::Error) -> ProviderFailure {
    ProviderFailure {
        stage,
        error: Arc::new(error),
    }
}

fn failures(errors: Vec<ProviderFailure>) -> MultipartError {
    MultipartError::Provider(Arc::new(errors))
}

async fn supervise(
    mut upload: Box<dyn MultipartUpload>,
    mut commands: mpsc::UnboundedReceiver<Command>,
    admission: Arc<Mutex<Admission>>,
    mut canceled: watch::Receiver<bool>,
) -> Result<Disposition> {
    let mut parts = Parts::new();
    let mut errors = vec![];
    let commit = loop {
        let state = *admission.lock().map_err(|_| MultipartError::Interrupted)?;
        if state == Admission::Closed {
            break false;
        }
        let completing = state == Admission::Completing;
        if !completing && *canceled.borrow() {
            if close_if_open(&admission)? {
                break false;
            }
            continue;
        }
        tokio::select! {
            biased;
            _ = canceled.changed(), if !completing => {
                // Sender loss is also a stop request, never proof of cleanup.
                if (canceled.has_changed().is_err() || *canceled.borrow())
                    && close_if_open(&admission)? {
                    break false;
                }
            }
            finished = parts.next(), if !parts.is_empty() => {
                if let Some(Some(error)) = finished {
                    errors.push(error);
                    break false;
                }
            }
            command = commands.recv() => match command {
                Some(Command::Part(payload, reply, permit)) => {
                    // The one supervisor invokes put_part in admitted FIFO order;
                    // only the returned futures are multiplexed concurrently.
                    let part = upload.put_part(payload);
                    parts.push(async move {
                        let result = part.await.map_err(|error| cause(Stage::Part, error));
                        // A ready result must also make capacity reusable before
                        // waking a producer on another executor/thread.
                        drop(permit);
                        let _ = reply.send(result.clone().map_err(|error| failures(vec![error])));
                        result.err()
                    }.boxed());
                }
                Some(Command::Complete) => break true,
                Some(Command::Abort) => break false,
                None => break false,
            }
        }
    };
    *admission.lock().map_err(|_| MultipartError::Interrupted)? = Admission::Closed;
    commands.close();
    // Reject parts that were queued but never invoked. Their permits/payloads
    // are released; no raw provider future was created for them.
    while let Some(command) = commands.recv().await {
        if let Command::Part(_, reply, _) = command {
            let _ = reply.send(Err(MultipartError::Closed));
        }
    }
    // Provider futures may themselves own effects. Join them, even after their
    // result observers vanish. Generic cancellation cannot safely drop them.
    while let Some(error) = parts.next().await {
        errors.extend(error);
    }
    if commit && errors.is_empty() {
        return upload
            .complete()
            .await
            .map(Disposition::Committed)
            .map_err(|error| failures(vec![cause(Stage::Complete, error)]));
    }
    if let Err(error) = upload.abort().await {
        errors.push(cause(Stage::Abort, error));
    }
    if errors.is_empty() {
        Ok(Disposition::Aborted)
    } else {
        Err(failures(errors))
    }
}

// Recheck under the same lock as complete admission. A cancellation observed
// using an older Open snapshot must never retract a now-admitted completion.
pub(super) fn close_if_open(admission: &Mutex<Admission>) -> Result<bool> {
    let mut state = admission.lock().map_err(|_| MultipartError::Interrupted)?;
    if *state == Admission::Open {
        *state = Admission::Closed;
        Ok(true)
    } else {
        Ok(false)
    }
}
