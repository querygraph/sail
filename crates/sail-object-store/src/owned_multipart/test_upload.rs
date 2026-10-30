use std::fmt;

use async_trait::async_trait;
use object_store::{ObjectStore, PutMultipartOptions, UploadPart};

use super::*;

#[derive(Debug)]
pub struct Injected;
impl fmt::Display for Injected {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("injected multipart failure")
    }
}
impl Error for Injected {}
pub fn failure() -> object_store::Error {
    object_store::Error::Generic {
        store: "MultipartFixture",
        source: Box::new(Injected),
    }
}

#[derive(Debug)]
pub struct Gate {
    entered: oneshot::Sender<()>,
    release: oneshot::Receiver<()>,
}
impl Gate {
    pub fn new() -> (Self, oneshot::Receiver<()>, oneshot::Sender<()>) {
        let (entered, wait) = oneshot::channel();
        let (release, pending) = oneshot::channel();
        (
            Self {
                entered,
                release: pending,
            },
            wait,
            release,
        )
    }
    pub async fn wait(self) {
        let _ = self.entered.send(());
        let _ = self.release.await;
    }
}

#[derive(Debug)]
pub struct State {
    pub store: Arc<InMemory>,
    pub path: Path,
    pub order: Mutex<Vec<usize>>,
    pub runtimes: Mutex<Vec<tokio::runtime::Id>>,
    pub complete_count: AtomicUsize,
    pub abort_count: AtomicUsize,
}

#[derive(Debug)]
pub struct TestUpload {
    inner: Box<dyn MultipartUpload>,
    state: Arc<State>,
    next: usize,
    pub part_gates: Vec<Gate>,
    pub complete_gate: Option<Gate>,
    pub abort_gate: Option<Gate>,
    pub fail_part: bool,
    pub fail_complete: bool,
    pub fail_abort: bool,
}
impl TestUpload {
    pub async fn new() -> std::result::Result<(Self, Arc<State>), object_store::Error> {
        let store = Arc::new(InMemory::new());
        let path = Path::from("checkpoint/part");
        let inner = store
            .put_multipart_opts(&path, PutMultipartOptions::default())
            .await?;
        let state = Arc::new(State {
            store,
            path,
            order: Mutex::new(vec![]),
            runtimes: Mutex::new(vec![tokio::runtime::Handle::current().id()]),
            complete_count: AtomicUsize::new(0),
            abort_count: AtomicUsize::new(0),
        });
        Ok((
            Self {
                inner,
                state: state.clone(),
                next: 0,
                part_gates: vec![],
                complete_gate: None,
                abort_gate: None,
                fail_part: false,
                fail_complete: false,
                fail_abort: false,
            },
            state,
        ))
    }
}
#[async_trait]
impl MultipartUpload for TestUpload {
    fn put_part(&mut self, payload: PutPayload) -> UploadPart {
        let index = self.next;
        self.next += 1;
        self.state
            .order
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .push(index);
        let gate = if self.part_gates.is_empty() {
            None
        } else {
            Some(self.part_gates.remove(0))
        };
        let part = self.inner.put_part(payload);
        let fail = self.fail_part;
        let state = self.state.clone();
        state
            .runtimes
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .push(tokio::runtime::Handle::current().id());
        async move {
            state
                .runtimes
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .push(tokio::runtime::Handle::current().id());
            if let Some(gate) = gate {
                gate.wait().await;
            }
            if fail { Err(failure()) } else { part.await }
        }
        .boxed()
    }
    async fn complete(&mut self) -> object_store::Result<PutResult> {
        self.state
            .runtimes
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .push(tokio::runtime::Handle::current().id());
        self.state.complete_count.fetch_add(1, Ordering::SeqCst);
        if let Some(gate) = self.complete_gate.take() {
            gate.wait().await;
        }
        if self.fail_complete {
            return Err(failure());
        }
        self.inner.complete().await
    }
    async fn abort(&mut self) -> object_store::Result<()> {
        self.state
            .runtimes
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .push(tokio::runtime::Handle::current().id());
        self.state.abort_count.fetch_add(1, Ordering::SeqCst);
        if let Some(gate) = self.abort_gate.take() {
            gate.wait().await;
        }
        if self.fail_abort {
            return Err(failure());
        }
        self.inner.abort().await
    }
}
