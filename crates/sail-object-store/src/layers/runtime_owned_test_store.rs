use std::fmt;
use std::sync::Mutex;

use futures::stream::BoxStream;
use object_store::{
    CopyOptions, GetOptions, GetResult, ListResult, ObjectMeta, ObjectStore, PutMultipartOptions,
    PutOptions,
};

use super::*;

#[derive(Debug)]
pub(super) struct PublicationStore {
    pub memory: Arc<InMemory>,
    pub disposed: Mutex<Option<oneshot::Sender<()>>>,
    pub aborts: Arc<AtomicUsize>,
    pub created_on: Mutex<Option<tokio::runtime::Id>>,
}
impl fmt::Display for PublicationStore {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("PublicationStore")
    }
}
#[async_trait::async_trait]
impl ObjectStore for PublicationStore {
    async fn put_opts(
        &self,
        path: &Path,
        payload: PutPayload,
        options: PutOptions,
    ) -> Result<PutResult> {
        self.memory.put_opts(path, payload, options).await
    }
    async fn put_multipart_opts(
        &self,
        path: &Path,
        _options: PutMultipartOptions,
    ) -> Result<Box<dyn MultipartUpload>> {
        *self.created_on.lock().unwrap_or_else(|e| e.into_inner()) = Some(Handle::current().id());
        let disposed = self
            .disposed
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .take();
        Ok(Box::new(HeldPublication {
            store: self.memory.clone(),
            path: path.clone(),
            started: None,
            release: None,
            disposed,
            aborts: self.aborts.clone(),
            fail_after_publish: true,
        }))
    }
    async fn get_opts(&self, path: &Path, options: GetOptions) -> Result<GetResult> {
        self.memory.get_opts(path, options).await
    }
    fn delete_stream(
        &self,
        paths: BoxStream<'static, Result<Path>>,
    ) -> BoxStream<'static, Result<Path>> {
        self.memory.delete_stream(paths)
    }
    fn list(&self, prefix: Option<&Path>) -> BoxStream<'static, Result<ObjectMeta>> {
        self.memory.list(prefix)
    }
    async fn list_with_delimiter(&self, prefix: Option<&Path>) -> Result<ListResult> {
        self.memory.list_with_delimiter(prefix).await
    }
    async fn copy_opts(&self, from: &Path, to: &Path, options: CopyOptions) -> Result<()> {
        self.memory.copy_opts(from, to, options).await
    }
}
