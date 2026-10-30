//! Real checkpoint Parquet writes that publish, then lose the provider response.
use std::fmt;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use datafusion::arrow::array::{Int32Array, RecordBatch};
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::datasource::memory::MemorySourceConfig;
use datafusion::execution::object_store::ObjectStoreUrl;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::prelude::{SessionConfig, SessionContext};
use futures::TryStreamExt;
use futures::stream::BoxStream;
use object_store::memory::InMemory;
use object_store::path::Path;
use object_store::{
    CopyOptions, GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta, ObjectStore,
    ObjectStoreExt, PutMultipartOptions, PutOptions, PutPayload, PutResult, UploadPart,
};
use sail_common_datafusion::error::{
    CommitStateUnknownError, CommonErrorCause, PythonErrorCause, PythonErrorCauseExtractor,
};
use sail_physical_plan::remote_checkpoint::RemoteCheckpointWriteExec;

type TestResult = Result<(), Box<dyn std::error::Error>>;
struct NoPython;
impl PythonErrorCauseExtractor for NoPython {
    fn extract(_: &(dyn std::error::Error + 'static)) -> Option<PythonErrorCause> {
        None
    }
}

#[derive(Debug)]
struct LostResponse;
impl fmt::Display for LostResponse {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("published response lost")
    }
}
impl std::error::Error for LostResponse {}

fn uncertain() -> object_store::Error {
    object_store::Error::Generic {
        store: "checkpoint-test",
        source: Box::new(datafusion::common::DataFusionError::Collection(vec![
            datafusion::common::DataFusionError::External(Box::new(LostResponse)),
            datafusion::common::DataFusionError::External(Box::new(CommitStateUnknownError(
                "published response lost".into(),
            ))),
        ])),
    }
}

#[derive(Debug, Default)]
struct PublishedThenLost {
    inner: Arc<InMemory>,
    puts: AtomicUsize,
    completes: Arc<AtomicUsize>,
}
impl fmt::Display for PublishedThenLost {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("published-then-lost")
    }
}
#[tonic::async_trait]
impl ObjectStore for PublishedThenLost {
    async fn put_opts(
        &self,
        location: &Path,
        payload: PutPayload,
        opts: PutOptions,
    ) -> object_store::Result<PutResult> {
        self.inner.put_opts(location, payload, opts).await?;
        self.puts.fetch_add(1, Ordering::SeqCst);
        Err(uncertain())
    }
    async fn put_multipart_opts(
        &self,
        location: &Path,
        opts: PutMultipartOptions,
    ) -> object_store::Result<Box<dyn MultipartUpload>> {
        Ok(Box::new(PublishedUpload {
            inner: self.inner.put_multipart_opts(location, opts).await?,
            completes: self.completes.clone(),
        }))
    }
    async fn get_opts(&self, location: &Path, opts: GetOptions) -> object_store::Result<GetResult> {
        self.inner.get_opts(location, opts).await
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
        opts: CopyOptions,
    ) -> object_store::Result<()> {
        self.inner.copy_opts(from, to, opts).await
    }
}
#[derive(Debug)]
struct PublishedUpload {
    inner: Box<dyn MultipartUpload>,
    completes: Arc<AtomicUsize>,
}
#[tonic::async_trait]
impl MultipartUpload for PublishedUpload {
    fn put_part(&mut self, payload: PutPayload) -> UploadPart {
        self.inner.put_part(payload)
    }
    async fn complete(&mut self) -> object_store::Result<PutResult> {
        self.inner.complete().await?;
        self.completes.fetch_add(1, Ordering::SeqCst);
        Err(uncertain())
    }
    async fn abort(&mut self) -> object_store::Result<()> {
        self.inner.abort().await
    }
}

async fn checkpoint_error(parallel: bool, multipart: bool) -> TestResult {
    let store = Arc::new(PublishedThenLost::default());
    let url = ObjectStoreUrl::parse("memory://checkpoint-errors")?;
    let mut config = SessionConfig::new();
    config
        .options_mut()
        .execution
        .parquet
        .allow_single_file_parallelism = parallel;
    config
        .options_mut()
        .execution
        .objectstore_writer_buffer_size = if multipart { 1 } else { 1024 * 1024 };
    let session = SessionContext::new_with_config(config);
    session
        .runtime_env()
        .register_object_store(url.as_ref(), store.clone());
    let schema = Arc::new(Schema::new(vec![Field::new(
        "value",
        DataType::Int32,
        false,
    )]));
    let batch = RecordBatch::try_new(schema.clone(), vec![Arc::new(Int32Array::from(vec![1, 2]))])?;
    let input = MemorySourceConfig::try_new_exec(&[vec![batch]], schema.clone(), None)?;
    let prefix = Path::from("checkpoint/actual-writer");
    let plan = RemoteCheckpointWriteExec::try_new(input, url, prefix.clone(), schema, None)?;
    let error = match plan
        .execute(0, session.task_ctx())?
        .try_collect::<Vec<_>>()
        .await
    {
        Ok(_) => return Err("lost publication response unexpectedly succeeded".into()),
        Err(error) => error,
    };
    let objects = store
        .inner
        .list(Some(&prefix))
        .try_collect::<Vec<_>>()
        .await?;
    assert_eq!(objects.len(), 1, "provider must actually publish");
    let bytes = store.inner.get(&objects[0].location).await?.bytes().await?;
    assert!(bytes.starts_with(b"PAR1") && bytes.ends_with(b"PAR1"));
    assert_eq!(store.puts.load(Ordering::SeqCst), usize::from(!multipart));
    assert_eq!(
        store.completes.load(Ordering::SeqCst),
        usize::from(multipart)
    );
    let cause = CommonErrorCause::new::<NoPython>(&error);
    assert!(
        !cause.is_retryable(),
        "published checkpoint became retryable: {error:?}; {cause:?}"
    );
    assert!(matches!(cause, CommonErrorCause::CommitStateUnknown(_)));
    Ok(())
}

#[tokio::test]
async fn sequential_put_keeps_unknown_commit() -> TestResult {
    checkpoint_error(false, false).await
}
#[tokio::test]
async fn sequential_multipart_keeps_unknown_commit() -> TestResult {
    checkpoint_error(false, true).await
}
#[tokio::test]
async fn parallel_put_keeps_unknown_commit() -> TestResult {
    checkpoint_error(true, false).await
}
#[tokio::test]
async fn parallel_multipart_keeps_unknown_commit() -> TestResult {
    checkpoint_error(true, true).await
}
