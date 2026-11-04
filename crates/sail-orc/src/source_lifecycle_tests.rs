use std::fmt;
use std::sync::atomic::{AtomicUsize, Ordering};

use async_trait::async_trait;
use datafusion::execution::memory_pool::GreedyMemoryPool;
use futures::StreamExt;
use futures::stream::BoxStream;
use object_store::{
    CopyOptions, GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta,
    PutMultipartOptions, PutOptions, PutPayload, PutResult,
};
use orc_rust::proto;
use prost013::Message;

use super::*;

#[derive(Debug)]
struct ObservedStore {
    inner: Arc<InMemory>,
    requests: AtomicUsize,
    active: Arc<AtomicUsize>,
    pending_request: Option<usize>,
}

impl fmt::Display for ObservedStore {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "ORC source test store")
    }
}

struct PendingRequest(Arc<AtomicUsize>);

impl Drop for PendingRequest {
    fn drop(&mut self) {
        self.0.fetch_sub(1, Ordering::SeqCst);
    }
}

#[async_trait]
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
        let request = self.requests.fetch_add(1, Ordering::SeqCst) + 1;
        if self.pending_request == Some(request) {
            self.active.fetch_add(1, Ordering::SeqCst);
            let _pending = PendingRequest(self.active.clone());
            return futures::future::pending().await;
        }
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

async fn observed(bytes: Bytes, pending_request: Option<usize>) -> (Scan, Arc<ObservedStore>) {
    let scan = Scan::new(bytes).await;
    let store = Arc::new(ObservedStore {
        inner: scan.store.clone(),
        requests: AtomicUsize::new(0),
        active: Arc::new(AtomicUsize::new(0)),
        pending_request,
    });
    scan.context.runtime_env().register_object_store(
        &url::Url::parse("memory://orc-source").unwrap(),
        store.clone(),
    );
    (scan, store)
}

#[tokio::test]
async fn dropping_file_stream_cancels_pending_tail_or_stripe_get_and_releases_pool() {
    for pending_request in [1, 2] {
        let (bytes, schema) = fixture();
        let (scan, store) = observed(bytes, Some(pending_request)).await;
        let config = scan.builder(Arc::new(scan.source(schema))).build();
        let mut output = config.open(0, scan.context.clone()).unwrap();
        assert!(futures::poll!(output.next()).is_pending());
        assert_eq!(store.requests.load(Ordering::SeqCst), pending_request);
        assert_eq!(store.active.load(Ordering::SeqCst), 1);
        assert!(scan.context.memory_pool().reserved() > 0);
        drop(output);
        assert_eq!(store.active.load(Ordering::SeqCst), 0);
        assert_eq!(scan.context.memory_pool().reserved(), 0);
        // No detached task survives to start another source-driven request.
        tokio::task::yield_now().await;
        assert_eq!(store.requests.load(Ordering::SeqCst), pending_request);
    }
}

#[tokio::test]
async fn dropping_decoder_releases_ranges_without_invalidating_a_returned_batch() {
    let (bytes, schema) = fixture();
    let (scan, store) = observed(bytes, None).await;
    let config = scan
        .builder(Arc::new(scan.source(schema)))
        .with_batch_size(Some(1))
        .build();
    let mut output = config.open(0, scan.context.clone()).unwrap();
    let batch = output.next().await.unwrap().unwrap();
    assert_eq!(ids(std::slice::from_ref(&batch)), vec![Some(0)]);
    assert!(scan.context.memory_pool().reserved() > 0);
    let requests = store.requests.load(Ordering::SeqCst);
    drop(output);
    assert_eq!(scan.context.memory_pool().reserved(), 0);
    assert_eq!(ids(std::slice::from_ref(&batch)), vec![Some(0)]);
    tokio::task::yield_now().await;
    assert_eq!(store.requests.load(Ordering::SeqCst), requests);
}

#[tokio::test]
async fn actual_file_stream_applies_local_pool_and_each_metadata_budget() {
    let (bytes, schema) = fixture();
    // The uncompressed decoder charges the stream directly, rather than
    // rejecting a compressed block against its allowed output size.
    assert!(metadata(&bytes).compression().is_none());
    let (scan, store) = observed(bytes, None).await;
    let mut source = scan.source(schema.clone());
    source.pool = Arc::new(GreedyMemoryPool::new(0));
    let error = scan
        .read(scan.builder(Arc::new(source)).build())
        .await
        .unwrap_err();
    assert!(error.to_string().contains("memory"), "{error}");
    assert_eq!(store.requests.load(Ordering::SeqCst), 0);
    for (index, expected) in [
        "Encoded footer and metadata exceed the configured limit",
        "Decoded stream exceeds the configured output limit",
        "Decoded stream exceeds the configured output limit",
        "exceeding maximum schema depth 0",
    ]
    .into_iter()
    .enumerate()
    {
        let mut source = scan.source(schema.clone());
        match index {
            0 => source.options.max_encoded_metadata_bytes = 0,
            1 => source.options.max_decoded_metadata_bytes = 0,
            2 => source.options.max_decoded_stripe_footer_bytes = 0,
            3 => source.options.max_schema_depth = 0,
            _ => unreachable!(),
        }
        let error = scan
            .read(scan.builder(Arc::new(source)).build())
            .await
            .unwrap_err();
        let message = error.to_string();
        assert!(message.contains(expected), "{message}");
        assert_eq!(scan.context.memory_pool().reserved(), 0);
    }
    let batches = scan
        .read(scan.builder(Arc::new(scan.source(schema))).build())
        .await
        .unwrap();
    assert_eq!(ids(&batches), [0, 1, 2, 3, 4, 5].map(Some));
    assert_eq!(scan.context.memory_pool().reserved(), 0);
}

fn rewrite_footer(file: &Bytes, update: impl FnOnce(&mut proto::Footer)) -> Bytes {
    let postscript_length = usize::from(file[file.len() - 1]);
    let postscript_start = file.len() - 1 - postscript_length;
    let mut postscript =
        proto::PostScript::decode(&file[postscript_start..file.len() - 1]).unwrap();
    assert_eq!(postscript.compression(), proto::CompressionKind::None);
    let footer_start = postscript_start - postscript.footer_length() as usize;
    let mut footer = proto::Footer::decode(&file[footer_start..postscript_start]).unwrap();
    update(&mut footer);
    let footer = footer.encode_to_vec();
    postscript.footer_length = Some(footer.len() as u64);
    let postscript = postscript.encode_to_vec();
    let mut result = file[..footer_start].to_vec();
    result.extend(footer);
    result.extend(&postscript);
    result.push(postscript.len().try_into().unwrap());
    Bytes::from(result)
}

#[tokio::test]
async fn selected_stripe_extent_is_checked_before_stripe_io() {
    let (bytes, schema) = fixture();
    let boundary = metadata(&bytes).stripe_metadatas()[1].offset() as i64;
    let corrupt = rewrite_footer(&bytes, |footer| {
        footer.stripes[1].data_length = Some(bytes.len() as u64 + 1);
    });
    let (scan, store) = observed(corrupt, None).await;
    let config = scan.builder(Arc::new(scan.source(schema.clone()))).build();
    let error = scan.read(config).await.unwrap_err();
    assert!(error.to_string().contains("exceeds object size"), "{error}");
    assert_eq!(store.requests.load(Ordering::SeqCst), 1); // File tail only.
    assert_eq!(scan.context.memory_pool().reserved(), 0);

    // A malformed unselected stripe extent is not an additional range-read
    // validity rule. The first stripe remains readable under its exact range.
    let mut file = scan.file.clone();
    file.range = Some(FileRange {
        start: 0,
        end: boundary,
    });
    let config = scan
        .builder(Arc::new(scan.source(schema)))
        .with_file_groups(vec![FileGroup::from(vec![file])])
        .build();
    assert_eq!(
        ids(&scan.read(config).await.unwrap()),
        vec![Some(0), Some(1)]
    );
}

#[tokio::test]
async fn projected_out_invalid_logical_type_is_rejected_before_stripe_io() {
    let (bytes, schema) = fixture();
    let corrupt = rewrite_footer(&bytes, |footer| {
        footer.types[2].kind = Some(proto::r#type::Kind::Decimal as i32);
        footer.types[2].precision = Some(100);
        footer.types[2].scale = Some(0);
    });
    let (scan, store) = observed(corrupt, None).await;
    let config = scan
        .builder(Arc::new(scan.source(schema)))
        .with_projection_indices(Some(vec![0]))
        .unwrap()
        .build();
    let error = scan.read(config).await.unwrap_err();
    assert!(error.to_string().contains("decimal"), "{error}");
    assert_eq!(store.requests.load(Ordering::SeqCst), 1);
    assert_eq!(scan.context.memory_pool().reserved(), 0);
}

#[tokio::test]
async fn source_passes_case_policy_to_each_file_mapper() {
    let (bytes, _) = fixture();
    let scan = Scan::new(bytes).await;
    let requested = Arc::new(Schema::new(vec![Field::new("ID", DataType::Int64, false)]));
    for case_sensitive in [false, true] {
        let mut source = scan.source(requested.clone());
        source.read_case_sensitive = case_sensitive;
        let batches = scan
            .read(scan.builder(Arc::new(source)).build())
            .await
            .unwrap();
        let expected = if case_sensitive {
            vec![None; 6]
        } else {
            (0..6).map(Some).collect()
        };
        assert_eq!(ids(&batches), expected);
    }
}

#[tokio::test]
async fn full_requested_timestamp_compatibility_precedes_required_projection() {
    let bytes = Bytes::from_static(include_bytes!(
        "../tests/fixtures/orc/timestamps/foreign_submicro.orc"
    ));
    let actual = super::super::super::infer_orc_schema(&metadata(&bytes), "UTC").unwrap();
    let mut fields = actual
        .fields()
        .iter()
        .map(|field| field.as_ref().clone())
        .collect::<Vec<_>>();
    fields[1] = fields[1].clone().with_data_type(DataType::Timestamp(
        arrow::datatypes::TimeUnit::Microsecond,
        None,
    ));
    let (scan, store) = observed(bytes, None).await;
    let config = scan
        .builder(Arc::new(scan.source(Arc::new(Schema::new(fields)))))
        .with_projection_indices(Some(vec![0]))
        .unwrap()
        .build();
    let error = scan.read(config).await.unwrap_err();
    assert!(
        error
            .to_string()
            .contains("cannot convert TIMESTAMP to TIMESTAMP_NTZ in full data schema"),
        "{error}"
    );
    assert_eq!(store.requests.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn empty_physical_root_yields_no_rows_even_when_stripes_declare_rows() {
    let schema = Arc::new(Schema::empty());
    let batch = RecordBatch::try_new_with_options(
        schema.clone(),
        vec![],
        &arrow::record_batch::RecordBatchOptions::new().with_row_count(Some(6)),
    )
    .unwrap();
    let mut bytes = Vec::new();
    let mut writer = ArrowWriterBuilder::new(&mut bytes, schema.clone())
        .try_build()
        .unwrap();
    writer.write(&batch).unwrap();
    writer.close().unwrap();
    let bytes = Bytes::from(bytes);
    let file_metadata = metadata(&bytes);
    assert_eq!(file_metadata.number_of_rows(), 6);
    assert!(!file_metadata.stripe_metadatas().is_empty());
    let (scan, store) = observed(bytes, None).await;
    for requested in [
        schema,
        Arc::new(Schema::new(vec![Field::new(
            "missing",
            DataType::Int64,
            true,
        )])),
    ] {
        let before = store.requests.load(Ordering::SeqCst);
        let batches = scan
            .read(scan.builder(Arc::new(scan.source(requested))).build())
            .await
            .unwrap();
        assert!(batches.is_empty());
        assert_eq!(store.requests.load(Ordering::SeqCst), before + 1); // Tail only.
        assert_eq!(scan.context.memory_pool().reserved(), 0);
    }
}

#[tokio::test]
async fn constructor_rejects_virtual_columns_but_preserves_ordinary_partitions() {
    let (bytes, schema) = fixture();
    let mut scan = Scan::new(bytes).await;
    let partition = Arc::new(Field::new("region", DataType::Utf8, false));
    let virtual_column = Arc::new(
        Field::new("row_number", DataType::Int64, false)
            .with_extension_type(datafusion::parquet::arrow::RowNumber),
    );
    for partitions in [vec![], vec![partition.clone()]] {
        let table = TableSchema::builder(schema.clone())
            .with_table_partition_cols(partitions)
            .with_virtual_columns(vec![virtual_column.clone()])
            .build();
        let error = OrcSource::try_new(
            table,
            options(),
            OrcJavaTimeZoneConfig::for_reader_timezone("UTC".to_string()).unwrap(),
            "UTC".to_string(),
            true,
            scan.context.memory_pool().clone(),
        )
        .unwrap_err();
        assert!(matches!(
            &error,
            datafusion_common::DataFusionError::NotImplemented(_)
        ));
        assert!(
            error
                .to_string()
                .contains("ORC virtual columns are not supported")
        );
    }

    scan.file.partition_values = vec![ScalarValue::Utf8(Some("west".to_string()))];
    let table = TableSchema::builder(schema)
        .with_table_partition_cols(vec![partition.clone()])
        .build();
    let source = scan.source(table);
    assert_eq!(source.table_schema().table_partition_cols().len(), 1);
    assert_eq!(source.table_schema().table_partition_cols()[0], partition);
    let config = scan
        .builder(Arc::new(source))
        .with_projection_indices(Some(vec![2]))
        .unwrap()
        .build();
    let batches = scan.read(config).await.unwrap();
    assert_eq!(batches.iter().map(RecordBatch::num_rows).sum::<usize>(), 6);
    for batch in batches {
        assert_eq!(
            batch
                .column(0)
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap(),
            &StringArray::from(vec!["west"; batch.num_rows()])
        );
    }
}
