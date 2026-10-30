use std::fmt;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use bytes::Bytes;
use datafusion::common::Result;
use datafusion::datasource::listing::PartitionedFile;
use futures::stream::BoxStream;
use object_store::memory::InMemory;
use object_store::path::Path;
use object_store::{
    CopyOptions, GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta, ObjectStore,
    ObjectStoreExt, PutMultipartOptions, PutOptions, PutPayload, PutResult,
};

#[derive(Debug)]
pub(super) struct ObservedStore {
    inner: InMemory,
    gets: AtomicUsize,
}

impl ObservedStore {
    pub(super) async fn fixture(bytes: &'static [u8]) -> Result<(Arc<Self>, PartitionedFile)> {
        let inner = InMemory::new();
        let path = Path::from("data.orc");
        inner.put(&path, Bytes::from_static(bytes).into()).await?;
        let file = PartitionedFile::new_from_meta(inner.head(&path).await?);
        Ok((
            Arc::new(Self {
                inner,
                gets: AtomicUsize::new(0),
            }),
            file,
        ))
    }

    pub(super) fn gets(&self) -> usize {
        self.gets.load(Ordering::SeqCst)
    }
}

impl fmt::Display for ObservedStore {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "ORC codec test store")
    }
}

#[tonic::async_trait]
impl ObjectStore for ObservedStore {
    async fn put_opts(
        &self,
        path: &Path,
        payload: PutPayload,
        options: PutOptions,
    ) -> object_store::Result<PutResult> {
        self.inner.put_opts(path, payload, options).await
    }

    async fn put_multipart_opts(
        &self,
        path: &Path,
        options: PutMultipartOptions,
    ) -> object_store::Result<Box<dyn MultipartUpload>> {
        self.inner.put_multipart_opts(path, options).await
    }

    async fn get_opts(&self, path: &Path, options: GetOptions) -> object_store::Result<GetResult> {
        self.gets.fetch_add(1, Ordering::SeqCst);
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
