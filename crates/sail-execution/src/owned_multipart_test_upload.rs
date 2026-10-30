use std::fmt;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use futures::FutureExt;
use object_store::memory::InMemory;
use object_store::path::Path;
use object_store::{
    MultipartUpload, ObjectStore, PutMultipartOptions, PutPayload, PutResult, UploadPart,
};
use tokio::runtime::Handle;
use tokio::sync::oneshot;

#[derive(Debug)]
pub(super) struct Injected;
impl fmt::Display for Injected {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("lost multipart completion response")
    }
}
impl std::error::Error for Injected {}

#[derive(Debug)]
pub(super) struct Gate {
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
pub(super) struct State {
    pub store: Arc<InMemory>,
    pub path: Path,
    pub aborts: AtomicUsize,
    pub runtimes: Mutex<Vec<tokio::runtime::Id>>,
}
impl State {
    pub fn new() -> Arc<Self> {
        Self::at(Arc::new(InMemory::new()), Path::from("checkpoint/part"))
    }
    pub fn at(store: Arc<InMemory>, path: Path) -> Arc<Self> {
        Arc::new(Self {
            store,
            path,
            aborts: AtomicUsize::new(0),
            runtimes: Mutex::new(vec![]),
        })
    }
    fn record(&self) {
        self.runtimes
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .push(Handle::current().id());
    }
}

#[derive(Debug)]
pub(super) struct Upload {
    inner: Box<dyn MultipartUpload>,
    state: Arc<State>,
    pub complete: Option<Gate>,
    pub abort: Option<Gate>,
    pub lose_complete_response: bool,
}
impl Upload {
    pub async fn new(state: Arc<State>) -> object_store::Result<Self> {
        state.record();
        let inner = state
            .store
            .put_multipart_opts(&state.path, PutMultipartOptions::default())
            .await?;
        Ok(Self {
            inner,
            state,
            complete: None,
            abort: None,
            lose_complete_response: false,
        })
    }
}
#[tonic::async_trait]
impl MultipartUpload for Upload {
    fn put_part(&mut self, payload: PutPayload) -> UploadPart {
        self.state.record();
        let state = self.state.clone();
        let part = self.inner.put_part(payload);
        async move {
            state.record();
            part.await
        }
        .boxed()
    }
    async fn complete(&mut self) -> object_store::Result<PutResult> {
        self.state.record();
        if let Some(gate) = self.complete.take() {
            gate.wait().await;
        }
        let result = self.inner.complete().await?;
        if self.lose_complete_response {
            Err(object_store::Error::Generic {
                store: "OwnedMultipartFixture",
                source: Box::new(Injected),
            })
        } else {
            Ok(result)
        }
    }
    async fn abort(&mut self) -> object_store::Result<()> {
        self.state.record();
        self.state.aborts.fetch_add(1, Ordering::SeqCst);
        if let Some(gate) = self.abort.take() {
            gate.wait().await;
        }
        self.inner.abort().await
    }
}
