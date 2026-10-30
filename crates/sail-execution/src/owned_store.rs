//! Operation-local storage effects retained by an existing task resource owner.
//!
//! This facade is not installed in a registry by construction. The caller must
//! propagate the same owner through planning/execution and drain it before
//! cleanup. Provider-internal work, runtime affinity and durability need their
//! own qualification. Reads retain the underlying provider's behavior.

use std::fmt;
use std::num::NonZeroUsize;
use std::ops::Range;
use std::sync::Arc;

use bytes::Bytes;
use datafusion::common::DataFusionError;
use futures::stream::BoxStream;
use futures::{FutureExt, StreamExt};
use object_store::path::Path;
use object_store::{
    CopyOptions, GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta, ObjectStore,
    ObjectStoreExt, PutMultipartOptions, PutOptions, PutPayload, PutResult, RenameOptions,
    UploadPart,
};
use sail_common_datafusion::owned_put;
use sail_common_datafusion::task_resources::TaskResourceScope;
use sail_object_store::owned_multipart::{Disposition, MultipartError, MultipartHandle};

use crate::owned_multipart;

pub struct OwnedObjectStore {
    inner: Arc<dyn ObjectStore>,
    resources: Arc<TaskResourceScope>,
    max_in_flight_parts: NonZeroUsize,
}

impl OwnedObjectStore {
    /// Attach this immutable facade to an already-existing operation owner.
    /// The explicit part-count bound must cover the writer's concurrent part
    /// submissions, including synchronous bursts within one writer call (which
    /// can exceed its nominal concurrency). Excess submissions fail without
    /// queuing. It is not a byte budget. No mutable owner is added to a shared/cached provider handle.
    pub fn new(
        inner: Arc<dyn ObjectStore>,
        resources: Arc<TaskResourceScope>,
        max_in_flight_parts: NonZeroUsize,
    ) -> Self {
        Self {
            inner,
            resources,
            max_in_flight_parts,
        }
    }
}

impl fmt::Debug for OwnedObjectStore {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("OwnedObjectStore")
            .field("inner", &self.inner)
            .field("max_in_flight_parts", &self.max_in_flight_parts)
            .finish_non_exhaustive()
    }
}
impl fmt::Display for OwnedObjectStore {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "OwnedObjectStore({})", self.inner)
    }
}

#[path = "owned_store_error.rs"]
mod error;
use error::store_error;

fn provider_error(error: object_store::Error) -> DataFusionError {
    DataFusionError::ObjectStore(Box::new(error))
}
fn multipart_error(error: MultipartError) -> object_store::Error {
    store_error(owned_multipart::execution_error(error))
}

#[tonic::async_trait]
#[warn(clippy::missing_trait_methods)]
impl ObjectStore for OwnedObjectStore {
    async fn put_opts(
        &self,
        location: &Path,
        payload: PutPayload,
        opts: PutOptions,
    ) -> object_store::Result<PutResult> {
        owned_put::put(
            &self.resources,
            self.inner.clone(),
            location.clone(),
            payload,
            opts,
        )
        .await
        .map_err(store_error)
    }

    async fn put_multipart_opts(
        &self,
        location: &Path,
        opts: PutMultipartOptions,
    ) -> object_store::Result<Box<dyn MultipartUpload>> {
        let inner = self.inner.clone();
        let location = location.clone();
        let handle = owned_multipart::create(
            &self.resources,
            move || async move { inner.put_multipart_opts(&location, opts).await },
            self.max_in_flight_parts,
        )
        .await
        .map_err(store_error)?;
        Ok(Box::new(OwnedUpload(handle)))
    }

    async fn get_opts(&self, location: &Path, opts: GetOptions) -> object_store::Result<GetResult> {
        self.inner.get_opts(location, opts).await
    }
    async fn get_ranges(
        &self,
        location: &Path,
        ranges: &[Range<u64>],
    ) -> object_store::Result<Vec<Bytes>> {
        self.inner.get_ranges(location, ranges).await
    }
    fn list(&self, prefix: Option<&Path>) -> BoxStream<'static, object_store::Result<ObjectMeta>> {
        self.inner.list(prefix)
    }
    fn list_with_offset(
        &self,
        prefix: Option<&Path>,
        offset: &Path,
    ) -> BoxStream<'static, object_store::Result<ObjectMeta>> {
        self.inner.list_with_offset(prefix, offset)
    }
    async fn list_with_delimiter(&self, prefix: Option<&Path>) -> object_store::Result<ListResult> {
        self.inner.list_with_delimiter(prefix).await
    }

    fn delete_stream(
        &self,
        paths: BoxStream<'static, object_store::Result<Path>>,
    ) -> BoxStream<'static, object_store::Result<Path>> {
        let inner = self.inner.clone();
        let resources = self.resources.clone();
        // Admit one location only when requested by the consumer. The retained
        // singleton deletion finishes even if that consumer drops the stream.
        // Do not collect an unbounded input stream into an owned task.
        paths
            .then(move |path| {
                let inner = inner.clone();
                let resources = resources.clone();
                async move {
                    let path = path?;
                    resources
                        .run_mutation(move |_| async move {
                            inner.delete(&path).await.map_err(provider_error)?;
                            Ok(path)
                        })
                        .await
                        .map_err(store_error)
                }
            })
            .boxed()
    }

    async fn copy_opts(
        &self,
        from: &Path,
        to: &Path,
        opts: CopyOptions,
    ) -> object_store::Result<()> {
        let inner = self.inner.clone();
        let from = from.clone();
        let to = to.clone();
        self.resources
            .run_mutation(move |_| async move {
                inner
                    .copy_opts(&from, &to, opts)
                    .await
                    .map_err(provider_error)
            })
            .await
            .map_err(store_error)
    }
    async fn rename_opts(
        &self,
        from: &Path,
        to: &Path,
        opts: RenameOptions,
    ) -> object_store::Result<()> {
        let inner = self.inner.clone();
        let from = from.clone();
        let to = to.clone();
        self.resources
            .run_mutation(move |_| async move {
                inner
                    .rename_opts(&from, &to, opts)
                    .await
                    .map_err(provider_error)
            })
            .await
            .map_err(store_error)
    }
}

// The returned trait object contains only a command handle, never the raw upload.
// Dropping it closes command admission; the registered supervisor owns settlement.
#[derive(Debug)]
struct OwnedUpload(MultipartHandle);
#[tonic::async_trait]
impl MultipartUpload for OwnedUpload {
    fn put_part(&mut self, payload: PutPayload) -> UploadPart {
        match self.0.put_part(payload) {
            Ok(part) => async move { part.await.map_err(multipart_error) }.boxed(),
            Err(error) => futures::future::ready(Err(multipart_error(error))).boxed(),
        }
    }
    async fn complete(&mut self) -> object_store::Result<PutResult> {
        match self
            .0
            .complete()
            .map_err(multipart_error)?
            .await
            .map_err(multipart_error)?
        {
            Disposition::Committed(result) => Ok(result),
            Disposition::Aborted => Err(multipart_error(MultipartError::Closed)),
        }
    }
    async fn abort(&mut self) -> object_store::Result<()> {
        self.0
            .abort()
            .map_err(multipart_error)?
            .await
            .map_err(multipart_error)?;
        Ok(())
    }
}

#[cfg(test)]
#[path = "owned_store_tests.rs"]
mod tests;
