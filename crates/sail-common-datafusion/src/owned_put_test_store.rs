//! Private forwarding store, using the existing checkpoint-test fixture pattern.
use std::fmt;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use futures::stream::BoxStream;
use object_store::memory::InMemory;
use object_store::path::Path;
use object_store::{
    CopyOptions, GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta, ObjectStore,
    PutMultipartOptions, PutOptions, PutPayload, PutResult,
};
use tokio::sync::oneshot;

#[derive(Debug)]
pub(super) struct PutFailure;
impl fmt::Display for PutFailure {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "injected PUT failure")
    }
}
impl std::error::Error for PutFailure {}

#[derive(Debug)]
pub(super) struct PublishedResponse {
    pub entered: oneshot::Sender<()>,
    pub release: oneshot::Receiver<()>,
}

#[derive(Debug)]
pub(super) struct HeldStore {
    pub inner: Arc<InMemory>,
    pub calls: AtomicUsize,
    pub fail: bool,
    pub after_publish: Mutex<Option<PublishedResponse>>,
    pub entered: Mutex<Option<oneshot::Sender<()>>>,
    pub release: Mutex<Option<oneshot::Receiver<()>>>,
}
impl fmt::Display for HeldStore {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "HeldPutStore")
    }
}
#[async_trait::async_trait]
impl ObjectStore for HeldStore {
    async fn put_opts(
        &self,
        path: &Path,
        payload: PutPayload,
        options: PutOptions,
    ) -> object_store::Result<PutResult> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        let entered = self
            .entered
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .take();
        let release = self
            .release
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .take();
        if let Some(entered) = entered {
            let _ = entered.send(());
        }
        if let Some(release) = release {
            release
                .await
                .map_err(|error| object_store::Error::Generic {
                    store: "HeldPutStore",
                    source: Box::new(error),
                })?;
        }
        if self.fail {
            return Err(object_store::Error::PermissionDenied {
                path: path.to_string(),
                source: Box::new(PutFailure),
            });
        }
        let result = self.inner.put_opts(path, payload, options).await?;
        let response = self
            .after_publish
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .take();
        if let Some(response) = response {
            let _ = response.entered.send(());
            response
                .release
                .await
                .map_err(|error| object_store::Error::Generic {
                    store: "HeldPutStore",
                    source: Box::new(error),
                })?;
        }
        Ok(result)
    }
    async fn put_multipart_opts(
        &self,
        path: &Path,
        options: PutMultipartOptions,
    ) -> object_store::Result<Box<dyn MultipartUpload>> {
        self.inner.put_multipart_opts(path, options).await
    }
    async fn get_opts(&self, path: &Path, options: GetOptions) -> object_store::Result<GetResult> {
        self.inner.get_opts(path, options).await
    }
    fn delete_stream(
        &self,
        paths: BoxStream<'static, object_store::Result<Path>>,
    ) -> BoxStream<'static, object_store::Result<Path>> {
        self.inner.delete_stream(paths)
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
        options: CopyOptions,
    ) -> object_store::Result<()> {
        self.inner.copy_opts(from, to, options).await
    }
}
