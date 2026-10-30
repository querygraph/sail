use std::fmt;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use futures::StreamExt;
use futures::stream::BoxStream;
use object_store::memory::InMemory;
use object_store::path::Path;
use object_store::{
    CopyOptions, GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta, ObjectStore,
    PutMultipartOptions, PutOptions, PutPayload, PutResult,
};

use crate::owned_multipart_test_upload::{Gate, State, Upload};

#[derive(Debug, Default)]
pub(super) struct Gates {
    pub put: Option<Gate>,
    pub create: Option<Gate>,
    pub complete: Option<Gate>,
    pub abort: Option<Gate>,
    pub copy: Option<Gate>,
    pub delete: Option<Gate>,
}
#[derive(Debug, Default)]
pub(super) struct Provider {
    pub inner: Arc<InMemory>,
    pub calls: AtomicUsize,
    pub gates: Mutex<Gates>,
    pub uploads: Mutex<Vec<Arc<State>>>,
    pub lose_complete_response: bool,
}
impl Provider {
    pub fn aborts(&self) -> usize {
        self.uploads
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .iter()
            .map(|state| state.aborts.load(Ordering::SeqCst))
            .sum()
    }
}
impl fmt::Display for Provider {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("OwnedStoreProvider")
    }
}
#[tonic::async_trait]
impl ObjectStore for Provider {
    async fn put_opts(
        &self,
        path: &Path,
        payload: PutPayload,
        opts: PutOptions,
    ) -> object_store::Result<PutResult> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        let gate = self
            .gates
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .put
            .take();
        if let Some(gate) = gate {
            gate.wait().await;
        }
        self.inner.put_opts(path, payload, opts).await
    }
    async fn put_multipart_opts(
        &self,
        path: &Path,
        _: PutMultipartOptions,
    ) -> object_store::Result<Box<dyn MultipartUpload>> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        let (create, complete, abort) = {
            let mut gates = self.gates.lock().unwrap_or_else(|e| e.into_inner());
            (
                gates.create.take(),
                gates.complete.take(),
                gates.abort.take(),
            )
        };
        if let Some(gate) = create {
            gate.wait().await;
        }
        let state = State::at(self.inner.clone(), path.clone());
        self.uploads
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .push(state.clone());
        let mut upload = Upload::new(state).await?;
        upload.complete = complete;
        upload.abort = abort;
        upload.lose_complete_response = self.lose_complete_response;
        Ok(Box::new(upload))
    }
    async fn get_opts(&self, path: &Path, opts: GetOptions) -> object_store::Result<GetResult> {
        self.inner.get_opts(path, opts).await
    }
    fn delete_stream(
        &self,
        paths: BoxStream<'static, object_store::Result<Path>>,
    ) -> BoxStream<'static, object_store::Result<Path>> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        let inner = self.inner.clone();
        let mut gate = self
            .gates
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .delete
            .take();
        paths
            .then(move |path| {
                let inner = inner.clone();
                let gate = gate.take();
                async move {
                    let path = path?;
                    if let Some(gate) = gate {
                        gate.wait().await;
                    }
                    object_store::ObjectStoreExt::delete(inner.as_ref(), &path).await?;
                    Ok(path)
                }
            })
            .boxed()
    }
    fn list(&self, prefix: Option<&Path>) -> BoxStream<'static, object_store::Result<ObjectMeta>> {
        self.inner.list(prefix)
    }
    async fn list_with_delimiter(&self, prefix: Option<&Path>) -> object_store::Result<ListResult> {
        self.inner.list_with_delimiter(prefix).await
    }
    async fn copy_opts(
        &self,
        from: &Path,
        to: &Path,
        opts: CopyOptions,
    ) -> object_store::Result<()> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        let gate = self
            .gates
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .copy
            .take();
        if let Some(gate) = gate {
            gate.wait().await;
        }
        self.inner.copy_opts(from, to, opts).await
    }
}
