#![expect(clippy::unwrap_used)]

use std::fmt::{self, Debug, Display};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use arrow::array::{
    Decimal128Array, Int32Array, Int64Array, ListArray, StringArray, StringViewArray,
    TimestampMicrosecondArray, TimestampSecondArray,
};
use arrow::buffer::{NullBuffer, OffsetBuffer};
use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
use datafusion::datasource::listing::ListingTableUrl;
use datafusion::datasource::object_store::ObjectStoreUrl;
use datafusion::datasource::physical_plan::{FileGroup, FileOutputMode, FileSinkConfig};
use datafusion::execution::memory_pool::{GreedyMemoryPool, MemoryPool};
use datafusion::execution::runtime_env::RuntimeEnvBuilder;
use datafusion::logical_expr::dml::InsertOp;
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::prelude::{SessionConfig, SessionContext};
use futures::future::BoxFuture;
use futures::stream::BoxStream;
use futures::{FutureExt, TryStreamExt, stream};
use object_store::memory::InMemory;
use object_store::{
    CopyOptions, GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta,
    PutMultipartOptions, PutOptions, PutResult, UploadPart,
};
use orc_rust::ArrowReaderBuilder;
use orc_rust::reader::metadata::read_metadata;
use tokio::runtime::Handle;
use tokio::time::timeout;
use url::Url;

use super::*;

const URL: &str = "mem://";

#[derive(Debug)]
struct Injected;
impl Display for Injected {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("injected part failure")
    }
}
impl std::error::Error for Injected {}

/// A store that counts upload outcomes and can fail or stall a part.
#[derive(Debug, Default)]
struct FaultStore {
    inner: Arc<InMemory>,
    fail_part: Option<usize>,
    fail_abort: bool,
    /// Parts wait until this turns true.
    stall_parts: Option<watch::Receiver<bool>>,
    created: AtomicUsize,
    parts: Arc<AtomicUsize>,
    aborts: Arc<AtomicUsize>,
    completes: Arc<AtomicUsize>,
}

struct FaultUpload {
    inner: Box<dyn MultipartUpload>,
    fail_part: Option<usize>,
    fail_abort: bool,
    stall_parts: Option<watch::Receiver<bool>>,
    parts: Arc<AtomicUsize>,
    aborts: Arc<AtomicUsize>,
    completes: Arc<AtomicUsize>,
}

impl Debug for FaultUpload {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("FaultUpload")
    }
}

fn injected() -> object_store::Error {
    object_store::Error::Generic {
        store: "FaultStore",
        source: Box::new(Injected),
    }
}

#[async_trait]
impl MultipartUpload for FaultUpload {
    fn put_part(&mut self, payload: PutPayload) -> UploadPart {
        let index = self.parts.fetch_add(1, Ordering::SeqCst);
        if self.fail_part == Some(index) {
            return async { Err(injected()) }.boxed();
        }
        let part = self.inner.put_part(payload);
        let Some(mut stalled) = self.stall_parts.clone() else {
            return part;
        };
        async move {
            let _ = stalled.wait_for(|released| *released).await;
            part.await
        }
        .boxed()
    }

    async fn complete(&mut self) -> object_store::Result<PutResult> {
        self.completes.fetch_add(1, Ordering::SeqCst);
        self.inner.complete().await
    }

    async fn abort(&mut self) -> object_store::Result<()> {
        self.aborts.fetch_add(1, Ordering::SeqCst);
        if self.fail_abort {
            return Err(injected());
        }
        self.inner.abort().await
    }
}

impl Display for FaultStore {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("FaultStore")
    }
}

#[async_trait]
impl ObjectStore for FaultStore {
    async fn put_opts(
        &self,
        path: &Path,
        payload: PutPayload,
        opts: PutOptions,
    ) -> object_store::Result<PutResult> {
        self.inner.put_opts(path, payload, opts).await
    }

    async fn put_multipart_opts(
        &self,
        path: &Path,
        opts: PutMultipartOptions,
    ) -> object_store::Result<Box<dyn MultipartUpload>> {
        self.created.fetch_add(1, Ordering::SeqCst);
        Ok(Box::new(FaultUpload {
            inner: self.inner.put_multipart_opts(path, opts).await?,
            fail_part: self.fail_part,
            fail_abort: self.fail_abort,
            stall_parts: self.stall_parts.clone(),
            parts: self.parts.clone(),
            aborts: self.aborts.clone(),
            completes: self.completes.clone(),
        }))
    }

    async fn get_opts(&self, path: &Path, opts: GetOptions) -> object_store::Result<GetResult> {
        self.inner.get_opts(path, opts).await
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

fn config(schema: SchemaRef, partitions: &[&str]) -> Result<FileSinkConfig> {
    Ok(FileSinkConfig {
        original_url: URL.into(),
        object_store_url: ObjectStoreUrl::parse(URL)?,
        file_group: FileGroup::default(),
        table_paths: vec![ListingTableUrl::parse(URL)?],
        output_schema: schema,
        table_partition_cols: partitions
            .iter()
            .map(|name| ((*name).to_string(), DataType::Null))
            .collect(),
        insert_op: InsertOp::Append,
        keep_partition_by_columns: false,
        file_extension: "orc".into(),
        file_output_mode: FileOutputMode::Automatic,
    })
}

fn long_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, true)]))
}

fn longs(range: std::ops::Range<i64>) -> Result<RecordBatch> {
    Ok(RecordBatch::try_new(
        long_schema(),
        vec![Arc::new(Int64Array::from_iter_values(range))],
    )?)
}

fn sink(schema: SchemaRef, partitions: &[&str]) -> Result<OrcSink> {
    OrcSink::try_new(
        config(schema, partitions)?,
        OrcWriterOptions {
            compression: OrcWriteCompression::Zstd,
        },
    )
}

struct Harness {
    context: Arc<TaskContext>,
    resources: Arc<TaskResourceScope>,
}

/// A greedy pool that remembers its peak.
#[derive(Debug)]
struct PeakPool {
    inner: GreedyMemoryPool,
    peak: AtomicUsize,
}

impl PeakPool {
    fn new(bytes: usize) -> Self {
        Self {
            inner: GreedyMemoryPool::new(bytes),
            peak: AtomicUsize::new(0),
        }
    }

    fn record(&self) {
        self.peak.fetch_max(self.inner.reserved(), Ordering::SeqCst);
    }

    fn peak(&self) -> usize {
        self.peak.load(Ordering::SeqCst)
    }
}

impl Display for PeakPool {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("PeakPool")
    }
}

impl MemoryPool for PeakPool {
    fn name(&self) -> &str {
        "peak"
    }

    fn grow(&self, reservation: &MemoryReservation, additional: usize) {
        self.inner.grow(reservation, additional);
        self.record();
    }

    fn shrink(&self, reservation: &MemoryReservation, shrink: usize) {
        self.inner.shrink(reservation, shrink);
    }

    fn try_grow(&self, reservation: &MemoryReservation, additional: usize) -> Result<()> {
        self.inner.try_grow(reservation, additional)?;
        self.record();
        Ok(())
    }

    fn reserved(&self) -> usize {
        self.inner.reserved()
    }
}

const UNLIMITED: usize = usize::MAX / 2;

fn harness(store: Arc<FaultStore>, pool_bytes: Option<usize>) -> Result<Harness> {
    let pool = Arc::new(PeakPool::new(pool_bytes.unwrap_or(UNLIMITED)));
    harness_with_pool(store, pool)
}

fn harness_with_pool(store: Arc<FaultStore>, pool: Arc<PeakPool>) -> Result<Harness> {
    let runtime = RuntimeEnvBuilder::new()
        .with_memory_pool(pool)
        .build_arc()?;
    runtime.register_object_store(&Url::parse(URL).unwrap(), store);
    let resources = Arc::new(TaskResourceScope::new(Handle::current()));
    // A single output file keeps each scenario's upload count exact.
    let config = SessionConfig::new()
        .with_extension(resources.clone())
        .set_usize("datafusion.execution.minimum_parallel_output_files", 1);
    let session = SessionContext::new_with_config_rt(config, runtime);
    Ok(Harness {
        context: session.task_ctx(),
        resources,
    })
}

fn input(schema: SchemaRef, batches: Vec<Result<RecordBatch>>) -> SendableRecordBatchStream {
    Box::pin(RecordBatchStreamAdapter::new(schema, stream::iter(batches)))
}

async fn published(store: &FaultStore) -> Result<Vec<Path>> {
    let listed: Vec<_> = store.inner.list(None).try_collect().await?;
    Ok(listed.into_iter().map(|meta| meta.location).collect())
}

async fn rows_of(store: &FaultStore, path: &Path) -> Result<Vec<RecordBatch>> {
    let bytes = store.inner.get(path).await?.bytes().await?;
    let reader = ArrowReaderBuilder::try_new(bytes)
        .map_err(native_error)?
        .build();
    Ok(reader.collect::<std::result::Result<Vec<_>, _>>()?)
}

/// Whether this error, or any cause inside it, is the injected store failure.
fn is_injected(error: &DataFusionError) -> bool {
    format!("{error:?}").contains("Injected")
}

#[tokio::test]
async fn batches_share_one_file_with_one_footer() -> Result<()> {
    let store = Arc::new(FaultStore::default());
    let harness = harness(store.clone(), None)?;
    let sink = sink(long_schema(), &[])?;
    let rows = FileSink::write_all(
        &sink,
        input(long_schema(), vec![longs(0..3), longs(3..7), longs(7..10)]),
        &harness.context,
    )
    .await?;
    assert_eq!(rows, 10);
    let paths = published(&store).await?;
    assert_eq!(paths.len(), 1);
    assert!(paths[0].as_ref().ends_with(".orc"));
    let batches = rows_of(&store, &paths[0]).await?;
    let values: Vec<i64> = batches
        .iter()
        .flat_map(|batch| {
            let column = batch.column(0).as_any().downcast_ref::<Int64Array>();
            column.unwrap().values().to_vec()
        })
        .collect();
    assert_eq!(values, (0..10).collect::<Vec<_>>());
    assert_eq!(store.completes.load(Ordering::SeqCst), 1);
    assert_eq!(store.aborts.load(Ordering::SeqCst), 0);
    assert!(harness.resources.drain().await?.is_clean());
    Ok(())
}

#[tokio::test]
async fn view_arrays_are_stored_as_strings() -> Result<()> {
    let schema: SchemaRef = Arc::new(Schema::new(vec![Field::new("s", DataType::Utf8View, true)]));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![Arc::new(StringViewArray::from(vec![
            Some("a"),
            None,
            Some("a string long enough to be stored out of line"),
        ]))],
    )?;
    let store = Arc::new(FaultStore::default());
    let harness = harness(store.clone(), None)?;
    let sink = sink(schema.clone(), &[])?;
    FileSink::write_all(&sink, input(schema, vec![Ok(batch)]), &harness.context).await?;
    let paths = published(&store).await?;
    let batches = rows_of(&store, &paths[0]).await?;
    let column = batches[0].column(0);
    let strings = column.as_any().downcast_ref::<StringArray>().unwrap();
    assert_eq!(strings.value(0), "a");
    assert!(strings.is_null(1));
    Ok(())
}

#[test]
fn partition_columns_are_not_stored_in_the_file() -> Result<()> {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, true),
        Field::new("p", DataType::Utf8, true),
    ]));
    let sink = sink(schema, &["p"])?;
    assert_eq!(sink.file_schema.fields().len(), 1);
    assert_eq!(sink.file_schema.field(0).name(), "id");
    Ok(())
}

#[test]
fn an_unsupported_schema_is_refused_before_any_output() {
    let schema = Arc::new(Schema::new(vec![Field::new(
        "d",
        DataType::Decimal256(40, 2),
        true,
    )]));
    assert!(matches!(
        sink(schema, &[]),
        Err(DataFusionError::NotImplemented(_))
    ));
}

#[tokio::test]
async fn an_empty_input_leaves_one_schema_only_file() -> Result<()> {
    let store = Arc::new(FaultStore::default());
    let harness = harness(store.clone(), None)?;
    let sink = sink(long_schema(), &[])?;
    let rows = FileSink::write_all(&sink, input(long_schema(), vec![]), &harness.context).await?;
    assert_eq!(rows, 0);
    let paths = published(&store).await?;
    assert_eq!(paths.len(), 1);
    assert_eq!(rows_of(&store, &paths[0]).await?.len(), 0);
    Ok(())
}

#[tokio::test]
async fn an_empty_partitioned_input_leaves_nothing() -> Result<()> {
    let schema: SchemaRef = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, true),
        Field::new("p", DataType::Int64, true),
    ]));
    let store = Arc::new(FaultStore::default());
    let harness = harness(store.clone(), None)?;
    let sink = sink(schema.clone(), &["p"])?;
    FileSink::write_all(&sink, input(schema, vec![]), &harness.context).await?;
    assert!(published(&store).await?.is_empty());
    assert_eq!(store.created.load(Ordering::SeqCst), 0);
    Ok(())
}

#[tokio::test]
async fn a_failed_input_publishes_nothing() -> Result<()> {
    let store = Arc::new(FaultStore::default());
    let harness = harness(store.clone(), None)?;
    let sink = sink(long_schema(), &[])?;
    let error = FileSink::write_all(
        &sink,
        input(
            long_schema(),
            vec![longs(0..3), exec_err!("the input failed")],
        ),
        &harness.context,
    )
    .await
    .unwrap_err();
    assert!(error.to_string().contains("the input failed"), "{error}");
    assert!(published(&store).await?.is_empty());
    assert_eq!(store.completes.load(Ordering::SeqCst), 0);
    assert_eq!(store.aborts.load(Ordering::SeqCst), 1);
    assert!(harness.resources.drain().await?.is_clean());
    Ok(())
}

/// Enough incompressible rows for several parts.
fn large() -> Result<Vec<Result<RecordBatch>>> {
    let schema = Arc::new(Schema::new(vec![Field::new("s", DataType::Utf8, true)]));
    let value = |seed: u64| {
        let mut state = seed.wrapping_mul(0x9E37_79B9_7F4A_7C15) | 1;
        (0..1024)
            .map(|_| {
                state ^= state << 13;
                state ^= state >> 7;
                state ^= state << 17;
                char::from(33 + (state % 90) as u8)
            })
            .collect::<String>()
    };
    (0..4)
        .map(|batch| {
            let values = (0..8192).map(|row| value(batch * 8192 + row + 1));
            let array = StringArray::from_iter_values(values);
            Ok(Ok(RecordBatch::try_new(
                schema.clone(),
                vec![Arc::new(array)],
            )?))
        })
        .collect()
}

fn string_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![Field::new("s", DataType::Utf8, true)]))
}

#[tokio::test]
async fn a_failed_part_keeps_its_cause_and_aborts() -> Result<()> {
    let store = Arc::new(FaultStore {
        fail_part: Some(0),
        ..Default::default()
    });
    let harness = harness(store.clone(), None)?;
    let sink = sink(string_schema(), &[])?;
    let error = FileSink::write_all(&sink, input(string_schema(), large()?), &harness.context)
        .await
        .unwrap_err();
    assert!(is_injected(&error), "{error:?}");
    assert!(published(&store).await?.is_empty());
    assert_eq!(store.completes.load(Ordering::SeqCst), 0);
    assert_eq!(store.aborts.load(Ordering::SeqCst), 1);
    Ok(())
}

#[tokio::test]
async fn a_failed_abort_is_context_and_never_the_cause() -> Result<()> {
    let store = Arc::new(FaultStore {
        fail_abort: true,
        ..Default::default()
    });
    let harness = harness(store.clone(), None)?;
    let sink = sink(long_schema(), &[])?;
    let error = FileSink::write_all(
        &sink,
        input(
            long_schema(),
            vec![longs(0..3), exec_err!("the input failed")],
        ),
        &harness.context,
    )
    .await
    .unwrap_err();
    let DataFusionError::Collection(errors) = &error else {
        return exec_err!("expected both failures, got {error:?}");
    };
    assert!(
        errors[0].to_string().contains("the input failed"),
        "{error}"
    );
    assert!(is_injected(&errors[1]), "{error:?}");
    Ok(())
}

fn assert_nothing_reserved(harness: &Harness) {
    assert_eq!(harness.context.memory_pool().reserved(), 0);
}

/// The bytes of the strings in `large`.
const LARGE_BYTES: usize = 4 * 8192 * 1024;

#[tokio::test]
async fn memory_denial_is_a_resource_error_and_aborts() -> Result<()> {
    // The fixed state fits, and then the first slice of rows does not.
    let pool_bytes = COMPRESSOR_BYTES + COLUMN_BYTES + 64 * 1024;
    let store = Arc::new(FaultStore::default());
    let harness = harness(store.clone(), Some(pool_bytes))?;
    let sink = sink(string_schema(), &[])?;
    let error = FileSink::write_all(&sink, input(string_schema(), large()?), &harness.context)
        .await
        .unwrap_err();
    assert!(
        format!("{error:?}").contains("ResourcesExhausted"),
        "{error:?}"
    );
    assert!(published(&store).await?.is_empty());
    assert_eq!(store.aborts.load(Ordering::SeqCst), 1);
    assert_nothing_reserved(&harness);
    Ok(())
}

#[tokio::test]
async fn a_pool_too_small_for_the_writers_fixed_state_refuses_the_file() -> Result<()> {
    let store = Arc::new(FaultStore::default());
    let harness = harness(store.clone(), Some(64 * 1024))?;
    let sink = sink(long_schema(), &[])?;
    let error = FileSink::write_all(
        &sink,
        input(long_schema(), vec![longs(0..3)]),
        &harness.context,
    )
    .await
    .unwrap_err();
    assert!(
        format!("{error:?}").contains("ResourcesExhausted"),
        "{error:?}"
    );
    assert!(published(&store).await?.is_empty());
    assert_eq!(store.parts.load(Ordering::SeqCst), 0);
    assert_nothing_reserved(&harness);
    Ok(())
}

#[tokio::test]
async fn encoder_buffers_are_reserved_while_nothing_has_been_written() -> Result<()> {
    let store = Arc::new(FaultStore::default());
    let harness = harness(store.clone(), None)?;
    let sink = Arc::new(sink(string_schema(), &[])?);
    let write = {
        let (sink, context) = (sink.clone(), harness.context.clone());
        tokio::spawn(async move {
            let data = unending(large()?, string_schema());
            FileSink::write_all(sink.as_ref(), data, &context).await
        })
    };
    // The native encoder keeps every row of a stripe in memory, so before any
    // output all of the input is held only in buffers that this pool must see.
    let pool = harness.context.memory_pool().clone();
    timeout(Duration::from_secs(60), async {
        while pool.reserved() < LARGE_BYTES {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();
    assert_eq!(store.parts.load(Ordering::SeqCst), 0);
    harness.resources.cancel()?;
    let error = timeout(Duration::from_secs(30), write)
        .await
        .unwrap()
        .unwrap()
        .unwrap_err();
    assert!(error.to_string().contains("canceled"), "{error}");
    timeout(Duration::from_secs(30), harness.resources.drain())
        .await
        .unwrap()?;
    assert_eq!(store.aborts.load(Ordering::SeqCst), 1);
    assert_nothing_reserved(&harness);
    Ok(())
}

#[tokio::test]
async fn a_slice_is_reserved_before_it_is_encoded() -> Result<()> {
    let schema = string_schema();
    let value = "x".repeat(1024 * 1024);
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![Arc::new(StringArray::from_iter_values([value]))],
    )?;
    let pool = Arc::new(PeakPool::new(UNLIMITED));
    let store = Arc::new(FaultStore::default());
    let harness = harness_with_pool(store, pool.clone())?;
    // No compressor, so that the only state this large is the reserved slice: its
    // single megabyte of output and the fixed state could not explain this peak.
    let sink = OrcSink::try_new(
        config(schema.clone(), &[])?,
        OrcWriterOptions {
            compression: OrcWriteCompression::None,
        },
    )?;
    FileSink::write_all(&sink, input(schema, vec![Ok(batch)]), &harness.context).await?;
    assert!(pool.peak() >= 2 * 1024 * 1024, "peak {}", pool.peak());
    assert_nothing_reserved(&harness);
    Ok(())
}

#[tokio::test]
async fn memory_pressure_flushes_stripes_instead_of_failing() -> Result<()> {
    // Too small to hold the input in one stripe, as this writer otherwise would.
    let pool_bytes = LARGE_BYTES;
    let store = Arc::new(FaultStore::default());
    let harness = harness(store.clone(), Some(pool_bytes))?;
    let sink = sink(string_schema(), &[])?;
    let rows =
        FileSink::write_all(&sink, input(string_schema(), large()?), &harness.context).await?;
    assert_eq!(rows, 4 * 8192);
    let paths = published(&store).await?;
    let bytes = store.inner.get(&paths[0]).await?.bytes().await?;
    let metadata = read_metadata(&mut bytes.clone()).map_err(native_error)?;
    assert!(metadata.stripe_metadatas().len() > 1);
    let total: usize = rows_of(&store, &paths[0])
        .await?
        .iter()
        .map(RecordBatch::num_rows)
        .sum();
    assert_eq!(total, 4 * 8192);
    assert_nothing_reserved(&harness);
    Ok(())
}

#[tokio::test]
async fn a_successful_write_releases_everything() -> Result<()> {
    let pool = Arc::new(PeakPool::new(UNLIMITED));
    let store = Arc::new(FaultStore::default());
    let harness = harness_with_pool(store, pool.clone())?;
    let sink = sink(string_schema(), &[])?;
    FileSink::write_all(&sink, input(string_schema(), large()?), &harness.context).await?;
    assert!(pool.peak() >= LARGE_BYTES, "peak {}", pool.peak());
    assert_nothing_reserved(&harness);
    Ok(())
}

#[tokio::test]
async fn a_failed_input_releases_everything() -> Result<()> {
    let pool = Arc::new(PeakPool::new(UNLIMITED));
    let store = Arc::new(FaultStore::default());
    let harness = harness_with_pool(store, pool.clone())?;
    let sink = sink(long_schema(), &[])?;
    FileSink::write_all(
        &sink,
        input(
            long_schema(),
            vec![longs(0..3), exec_err!("the input failed")],
        ),
        &harness.context,
    )
    .await
    .unwrap_err();
    assert!(pool.peak() > 0);
    assert_nothing_reserved(&harness);
    Ok(())
}

#[tokio::test]
async fn a_failed_part_releases_everything() -> Result<()> {
    let store = Arc::new(FaultStore {
        fail_part: Some(0),
        ..Default::default()
    });
    let harness = harness(store.clone(), None)?;
    let sink = sink(string_schema(), &[])?;
    FileSink::write_all(&sink, input(string_schema(), large()?), &harness.context)
        .await
        .unwrap_err();
    assert_nothing_reserved(&harness);
    Ok(())
}

fn single_column(name: &str, array: ArrayRef) -> Result<(SchemaRef, RecordBatch)> {
    let schema = Arc::new(Schema::new(vec![Field::new(
        name,
        array.data_type().clone(),
        true,
    )]));
    let batch = RecordBatch::try_new(schema.clone(), vec![array])?;
    Ok((schema, batch))
}

async fn write_one(schema: SchemaRef, batch: RecordBatch) -> Result<(Arc<FaultStore>, Vec<Path>)> {
    let store = Arc::new(FaultStore::default());
    let harness = harness(store.clone(), None)?;
    let sink = sink(schema.clone(), &[])?;
    FileSink::write_all(&sink, input(schema, vec![Ok(batch)]), &harness.context).await?;
    assert_nothing_reserved(&harness);
    let paths = published(&store).await?;
    Ok((store, paths))
}

#[tokio::test]
async fn an_instant_keeps_its_value_whatever_zone_labels_it() -> Result<()> {
    let values = vec![Some(1_709_209_801_123_456), None, Some(-1_000_000)];
    let array = TimestampMicrosecondArray::from(values.clone()).with_timezone("Asia/Tokyo");
    let (schema, batch) = single_column("t", Arc::new(array))?;
    let (store, paths) = write_one(schema, batch).await?;
    let bytes = store.inner.get(&paths[0]).await?.bytes().await?;
    let schema = Arc::new(Schema::new(vec![Field::new(
        "t",
        DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
        true,
    )]));
    let batches = ArrowReaderBuilder::try_new(bytes)
        .map_err(native_error)?
        .with_schema(schema)
        .build()
        .collect::<std::result::Result<Vec<_>, _>>()?;
    let column = batches[0]
        .column(0)
        .as_primitive::<TimestampMicrosecondType>();
    assert_eq!(column.iter().collect::<Vec<_>>(), values);
    Ok(())
}

#[tokio::test]
async fn a_wall_clock_timestamp_is_stored_as_its_microseconds() -> Result<()> {
    let values = vec![Some(-30_610_223_999_500_000), None, Some(1)];
    let array = TimestampMicrosecondArray::from(values.clone());
    let (schema, batch) = single_column("t", Arc::new(array))?;
    let (store, paths) = write_one(schema, batch).await?;
    let batches = rows_of(&store, &paths[0]).await?;
    let column = batches[0].column(0).as_any().downcast_ref::<Int64Array>();
    assert_eq!(column.unwrap().iter().collect::<Vec<_>>(), values);
    Ok(())
}

#[tokio::test]
async fn a_coarser_timestamp_is_scaled_to_microseconds() -> Result<()> {
    let array = TimestampSecondArray::from(vec![Some(2), None]);
    let (schema, batch) = single_column("t", Arc::new(array))?;
    let (store, paths) = write_one(schema, batch).await?;
    let batches = rows_of(&store, &paths[0]).await?;
    let column = batches[0].column(0).as_any().downcast_ref::<Int64Array>();
    assert_eq!(
        column.unwrap().iter().collect::<Vec<_>>(),
        vec![Some(2_000_000), None]
    );
    Ok(())
}

#[tokio::test]
async fn a_timestamp_that_overflows_microseconds_fails_and_is_not_null() -> Result<()> {
    let array = TimestampSecondArray::from(vec![Some(i64::MAX)]);
    let (schema, batch) = single_column("t", Arc::new(array))?;
    let error = write_one(schema, batch).await.unwrap_err();
    assert!(error.to_string().contains("Overflow"), "{error}");
    Ok(())
}

#[tokio::test]
async fn an_instant_orc_cannot_represent_fails_and_publishes_nothing() -> Result<()> {
    // ORC-763: half a second before the epoch reads back half a second after it.
    let array = TimestampMicrosecondArray::from(vec![Some(-500_000)]).with_timezone("UTC");
    let (schema, batch) = single_column("t", Arc::new(array))?;
    let store = Arc::new(FaultStore::default());
    let harness = harness(store.clone(), None)?;
    let sink = sink(schema.clone(), &[])?;
    let error = FileSink::write_all(&sink, input(schema, vec![Ok(batch)]), &harness.context)
        .await
        .unwrap_err();
    assert!(
        format!("{error:?}").contains("EncodeTimestamp"),
        "{error:?}"
    );
    assert!(published(&store).await?.is_empty());
    assert_eq!(store.aborts.load(Ordering::SeqCst), 1);
    assert_nothing_reserved(&harness);
    Ok(())
}

#[tokio::test]
async fn nested_and_decimal_columns_reach_the_file() -> Result<()> {
    let decimals = Decimal128Array::from(vec![Some(10_i128.pow(38) - 1), None])
        .with_precision_and_scale(38, 0)?;
    let (schema, batch) = single_column("d", Arc::new(decimals.clone()))?;
    let (store, paths) = write_one(schema, batch).await?;
    assert_eq!(
        rows_of(&store, &paths[0]).await?[0].column(0).as_ref(),
        &decimals
    );

    // A view, a timestamp and a container all change, and the nulls must not.
    let views = StringViewArray::from(vec![Some("a"), None, Some("b")]);
    let field = Arc::new(Field::new("item", DataType::Utf8View, true));
    let list = ListArray::new(
        field,
        OffsetBuffer::new(vec![0, 2, 2, 3].into()),
        Arc::new(views),
        Some(NullBuffer::from(vec![true, false, true])),
    );
    let (schema, batch) = single_column("l", Arc::new(list))?;
    let (store, paths) = write_one(schema, batch).await?;
    let batches = rows_of(&store, &paths[0]).await?;
    let read = batches[0].column(0).as_list::<i32>();
    assert_eq!(read.value_offsets(), &[0, 2, 2, 3]);
    assert!(read.is_null(1));
    let strings = read.values().as_string::<i32>();
    assert_eq!(strings.value(0), "a");
    assert!(strings.is_null(1));
    assert_eq!(strings.value(2), "b");
    Ok(())
}

#[test]
fn a_slice_bound_counts_only_the_slices_own_child_rows() -> Result<()> {
    let values = Int32Array::from_iter_values(0..1000);
    let field = Arc::new(Field::new("item", DataType::Int32, false));
    let offsets = OffsetBuffer::from_lengths([1, 999]);
    let list = ListArray::new(field, offsets, Arc::new(values), None);
    assert_eq!(encoded_bound(&list.slice(0, 1))?, 8 + 4);
    assert_eq!(encoded_bound(&list)?, 2 * 8 + 4000);
    Ok(())
}

/// A stream that yields its batches and then never ends.
fn unending(batches: Vec<Result<RecordBatch>>, schema: SchemaRef) -> SendableRecordBatchStream {
    let rest = stream::iter(batches).chain(stream::pending());
    Box::pin(RecordBatchStreamAdapter::new(schema, rest))
}

async fn wait_for_parts(store: &FaultStore) {
    while store.parts.load(Ordering::SeqCst) == 0 {
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

#[tokio::test]
async fn cancellation_stops_a_stalled_upload_and_aborts() -> Result<()> {
    let (release, stalled) = watch::channel(false);
    let store = Arc::new(FaultStore {
        stall_parts: Some(stalled),
        ..Default::default()
    });
    let harness = harness(store.clone(), None)?;
    let sink = Arc::new(sink(string_schema(), &[])?);
    let write = {
        let (sink, context) = (sink.clone(), harness.context.clone());
        tokio::spawn(async move {
            // The native writer emits nothing until a stripe or the end of input.
            let data = input(string_schema(), large()?);
            FileSink::write_all(sink.as_ref(), data, &context).await
        })
    };
    wait_for_parts(&store).await;
    harness.resources.cancel()?;
    // The provider's in-flight part stays owned until it returns.
    release.send_replace(true);
    let error = timeout(Duration::from_secs(30), write)
        .await
        .unwrap()
        .unwrap()
        .unwrap_err();
    assert!(error.to_string().contains("canceled"), "{error}");
    timeout(Duration::from_secs(30), harness.resources.drain())
        .await
        .unwrap()?;
    assert!(published(&store).await?.is_empty());
    assert_eq!(store.completes.load(Ordering::SeqCst), 0);
    assert_eq!(store.aborts.load(Ordering::SeqCst), 1);
    Ok(())
}

#[tokio::test]
async fn a_dropped_sink_future_aborts_and_stays_owned() -> Result<()> {
    let store = Arc::new(FaultStore::default());
    let harness = harness(store.clone(), None)?;
    let sink = sink(long_schema(), &[])?;
    {
        let mut write = FileSink::write_all(
            &sink,
            unending(vec![longs(0..3)], long_schema()),
            &harness.context,
        );
        // Run the write until its file exists, then lose the future.
        let polled: BoxFuture<'_, ()> = async {
            tokio::select! {
                _ = &mut write => {}
                () = async {
                    while store.created.load(Ordering::SeqCst) == 0 {
                        tokio::task::yield_now().await;
                    }
                } => {}
            }
        }
        .boxed();
        polled.await;
    }
    harness.resources.seal()?;
    assert!(harness.resources.drain().await?.is_clean());
    assert!(published(&store).await?.is_empty());
    assert_eq!(store.completes.load(Ordering::SeqCst), 0);
    assert_eq!(store.aborts.load(Ordering::SeqCst), 1);
    Ok(())
}
