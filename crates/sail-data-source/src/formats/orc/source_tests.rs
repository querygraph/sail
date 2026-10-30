#![expect(clippy::unwrap_used)]

use arrow::array::{Array, Int32Array, Int64Array, StringArray, TimestampMicrosecondArray};
use arrow::compute::concat_batches;
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use arrow::record_batch::RecordBatch;
use bytes::Bytes;
use datafusion::execution::TaskContext;
use datafusion::execution::object_store::ObjectStoreUrl;
use datafusion::physical_expr::expressions::{BinaryExpr, Column, Literal};
use datafusion::physical_expr::projection::ProjectionExpr;
use datafusion::prelude::SessionContext;
use datafusion_common::ScalarValue;
use datafusion_datasource::file_groups::FileGroup;
use datafusion_datasource::file_scan_config::FileScanConfigBuilder;
use datafusion_datasource::source::DataSource;
use datafusion_datasource::{FileRange, PartitionedFile};
use datafusion_expr::Operator;
use futures::TryStreamExt;
use object_store::ObjectStoreExt;
use object_store::memory::InMemory;
use object_store::path::Path;
use orc_rust::ArrowWriterBuilder;
use orc_rust::reader::metadata::{FileMetadata, read_metadata_with_options};

use super::*;
use crate::options::ResolveOptions;

#[path = "source_lifecycle_tests.rs"]
mod lifecycle;

fn options() -> OrcReadOptions {
    OrcReadOptions::resolve(&SessionContext::new().state(), vec![]).unwrap()
}

fn metadata(file: &Bytes) -> FileMetadata {
    read_metadata_with_options(
        &mut file.clone(),
        options()
            .resource_limits(&datafusion::execution::memory_pool::UnboundedMemoryPool::default())
            .metadata_options(),
    )
    .unwrap()
}

fn fixture() -> (Bytes, SchemaRef) {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, true),
        Field::new("label", DataType::Utf8, true),
    ]));
    let mut bytes = Vec::new();
    let mut writer = ArrowWriterBuilder::new(&mut bytes, schema.clone())
        .try_build()
        .unwrap();
    for id in [0, 2, 4] {
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int64Array::from(vec![id, id + 1])),
                Arc::new(StringArray::from(vec![Some(format!("row-{id}")), None])),
            ],
        )
        .unwrap();
        writer.write(&batch).unwrap();
        writer.flush_stripe().unwrap();
    }
    writer.close().unwrap();
    let bytes = Bytes::from(bytes);
    assert_eq!(metadata(&bytes).stripe_metadatas().len(), 3);
    (bytes, schema)
}

struct Scan {
    store: Arc<InMemory>,
    context: Arc<TaskContext>,
    file: PartitionedFile,
}

impl Scan {
    async fn new(bytes: Bytes) -> Self {
        let store = Arc::new(InMemory::new());
        let path = Path::from("data.orc");
        store.put(&path, bytes.into()).await.unwrap();
        let file = PartitionedFile::new_from_meta(store.head(&path).await.unwrap());
        let context = Arc::new(TaskContext::default());
        context.runtime_env().register_object_store(
            &url::Url::parse("memory://orc-source").unwrap(),
            store.clone(),
        );
        Self {
            store,
            context,
            file,
        }
    }

    fn source(&self, table_schema: impl Into<TableSchema>) -> OrcSource {
        OrcSource::try_new(
            table_schema,
            options(),
            OrcJavaTimeZoneConfig::for_reader_timezone("UTC".to_string()).unwrap(),
            "UTC".to_string(),
            true,
            self.context.memory_pool().clone(),
        )
        .unwrap()
    }

    fn builder(&self, source: Arc<dyn FileSource>) -> FileScanConfigBuilder {
        FileScanConfigBuilder::new(
            ObjectStoreUrl::parse("memory://orc-source").unwrap(),
            source,
        )
        .with_file_groups(vec![FileGroup::from(vec![self.file.clone()])])
        .with_batch_size(Some(2))
        .with_preserve_order(true)
    }

    async fn read(&self, config: FileScanConfig) -> Result<Vec<RecordBatch>> {
        config.open(0, self.context.clone())?.try_collect().await
    }
}

fn ids(batches: &[RecordBatch]) -> Vec<Option<i64>> {
    batches
        .iter()
        .flat_map(|batch| {
            batch
                .column(0)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .iter()
        })
        .collect()
}

#[tokio::test]
async fn file_stream_restores_expressions_duplicates_missing_fields_and_partitions() {
    let (bytes, _) = fixture();
    let mut scan = Scan::new(bytes).await;
    scan.file.partition_values = vec![ScalarValue::Utf8(Some("west".to_string()))];
    let requested = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("label", DataType::Utf8, false),
        Field::new("missing", DataType::Int32, false),
    ]));
    let table = TableSchema::builder(requested)
        .with_table_partition_cols(vec![Arc::new(Field::new("part", DataType::Utf8, false))])
        .build();
    let source = scan.source(table);
    assert!(
        source
            .table_schema()
            .file_schema()
            .fields()
            .iter()
            .all(|field| field.is_nullable())
    );
    assert!(!source.table_schema().table_partition_cols()[0].is_nullable());
    let projection = ProjectionExprs::new([
        ProjectionExpr::new(Arc::new(Column::new("part", 3)), "partition"),
        ProjectionExpr::new(
            Arc::new(BinaryExpr::new(
                Arc::new(Column::new("id", 0)),
                Operator::Plus,
                Arc::new(Literal::new(ScalarValue::Int64(Some(10)))),
            )),
            "plus_ten",
        ),
        ProjectionExpr::new(Arc::new(Column::new("label", 1)), "label"),
        ProjectionExpr::new(Arc::new(Column::new("id", 0)), "again"),
        ProjectionExpr::new(Arc::new(Column::new("missing", 2)), "missing"),
    ]);
    let source = source
        .try_pushdown_projection(&projection)
        .unwrap()
        .unwrap();
    let config = scan.builder(source).build();
    let output_schema = config.projected_schema().unwrap();
    let batches = scan.read(config).await.unwrap();
    assert_eq!(
        batches
            .iter()
            .map(RecordBatch::num_rows)
            .collect::<Vec<_>>(),
        vec![2, 2, 2]
    );
    let batch = concat_batches(&output_schema, &batches).unwrap();
    assert_eq!(
        batch
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap(),
        &StringArray::from(vec!["west"; 6])
    );
    assert_eq!(
        batch
            .column(1)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap(),
        &Int64Array::from(vec![10, 11, 12, 13, 14, 15])
    );
    assert_eq!(
        batch
            .column(2)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap(),
        &StringArray::from(vec![
            Some("row-0"),
            None,
            Some("row-2"),
            None,
            Some("row-4"),
            None
        ])
    );
    assert_eq!(
        batch
            .column(3)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap(),
        &Int64Array::from(vec![0, 1, 2, 3, 4, 5])
    );
    assert_eq!(
        batch
            .column(4)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap()
            .null_count(),
        6
    );
}

#[tokio::test]
async fn zero_and_partition_only_projections_keep_row_counts_and_file_stream_limits() {
    let (bytes, schema) = fixture();
    let mut scan = Scan::new(bytes).await;
    scan.file.partition_values = vec![ScalarValue::Int32(Some(7))];
    let table = TableSchema::builder(schema)
        .with_table_partition_cols(vec![Arc::new(Field::new("part", DataType::Int32, false))])
        .build();
    for projection in [vec![], vec![2]] {
        let config = scan
            .builder(Arc::new(scan.source(table.clone())))
            .with_projection_indices(Some(projection.clone()))
            .unwrap()
            .with_batch_size(Some(1))
            .with_limit(Some(3))
            .build();
        let batches = scan.read(config).await.unwrap();
        assert_eq!(
            batches
                .iter()
                .map(RecordBatch::num_rows)
                .collect::<Vec<_>>(),
            vec![1, 1, 1]
        );
        assert!(
            batches
                .iter()
                .all(|batch| batch.num_columns() == projection.len())
        );
        if !projection.is_empty() {
            for batch in batches {
                assert_eq!(
                    batch
                        .column(0)
                        .as_any()
                        .downcast_ref::<Int32Array>()
                        .unwrap(),
                    &Int32Array::from(vec![7])
                );
            }
        }
    }
}

#[tokio::test]
async fn file_stream_honors_file_groups_and_limits_across_files() {
    let (bytes, schema) = fixture();
    let scan = Scan::new(bytes.clone()).await;
    let path = Path::from("second.orc");
    scan.store.put(&path, bytes.into()).await.unwrap();
    let second = PartitionedFile::new_from_meta(scan.store.head(&path).await.unwrap());
    let config = scan
        .builder(Arc::new(scan.source(schema)))
        .with_file_groups(vec![
            FileGroup::from(vec![scan.file.clone(), second.clone()]),
            FileGroup::from(vec![second]),
        ])
        .with_limit(Some(8))
        .build();
    let first = scan.read(config.clone()).await.unwrap();
    assert_eq!(ids(&first), [0, 1, 2, 3, 4, 5, 0, 1].map(Some));
    let second = config
        .open(1, scan.context.clone())
        .unwrap()
        .try_collect::<Vec<_>>()
        .await
        .unwrap();
    assert_eq!(ids(&second), [0, 1, 2, 3, 4, 5].map(Some));
}

#[tokio::test]
async fn explicit_ranges_select_stripe_starts_once_with_half_open_boundaries() {
    let (bytes, schema) = fixture();
    let boundary = metadata(&bytes).stripe_metadatas()[1].offset() as i64;
    let scan = Scan::new(bytes).await;
    let source = scan.source(schema);
    assert!(!source.supports_repartitioning());
    let mut combined = Vec::new();
    for (start, end, expected) in [
        (0, boundary, vec![Some(0), Some(1)]),
        (
            boundary,
            scan.file.object_meta.size as i64,
            vec![Some(2), Some(3), Some(4), Some(5)],
        ),
        (boundary, boundary, vec![]),
    ] {
        let mut file = scan.file.clone();
        file.range = Some(FileRange { start, end });
        let config = scan
            .builder(Arc::new(source.clone()))
            .with_file_groups(vec![FileGroup::from(vec![file])])
            .build();
        let values = ids(&scan.read(config).await.unwrap());
        assert_eq!(values, expected);
        combined.extend(values);
    }
    assert_eq!(combined, [0, 1, 2, 3, 4, 5].map(Some));
    for (start, end) in [(-1, 1), (2, 1), (0, scan.file.object_meta.size as i64 + 1)] {
        let mut file = scan.file.clone();
        file.range = Some(FileRange { start, end });
        let config = scan
            .builder(Arc::new(source.clone()))
            .with_file_groups(vec![FileGroup::from(vec![file])])
            .build();
        assert!(
            scan.read(config)
                .await
                .unwrap_err()
                .to_string()
                .contains("ORC file range")
        );
    }
}

#[tokio::test]
async fn file_stream_preserves_captured_reader_identity_separately_from_display_zone() {
    let bytes = Bytes::from_static(include_bytes!(
        "../../../tests/fixtures/orc/timestamps/foreign_submicro.orc"
    ));
    let metadata = metadata(&bytes);
    let scan = Scan::new(bytes).await;
    for (reader, expected) in [
        (
            "UTC",
            vec![Some(1_577_934_245_123_456), Some(-1_876_544), Some(0), None],
        ),
        (
            "America/Los_Angeles",
            vec![
                Some(1_577_963_045_123_456),
                Some(28_798_123_456),
                Some(28_800_000_000),
                None,
            ],
        ),
    ] {
        for display in ["UTC", "America/Los_Angeles"] {
            let requested = super::super::infer_orc_schema(&metadata, display).unwrap();
            let source = OrcSource::try_new(
                requested,
                options(),
                OrcJavaTimeZoneConfig::for_reader_timezone(reader.to_string()).unwrap(),
                display.to_string(),
                true,
                scan.context.memory_pool().clone(),
            )
            .unwrap();
            let batches = scan
                .read(scan.builder(Arc::new(source)).build())
                .await
                .unwrap();
            let values = batches
                .iter()
                .flat_map(|batch| {
                    batch
                        .column(1)
                        .as_any()
                        .downcast_ref::<TimestampMicrosecondArray>()
                        .unwrap()
                        .iter()
                })
                .collect::<Vec<_>>();
            assert_eq!(values, expected);
            assert_eq!(
                batches[0].schema().field(1).data_type(),
                &DataType::Timestamp(
                    arrow::datatypes::TimeUnit::Microsecond,
                    Some(display.into())
                )
            );
        }
    }
}

#[tokio::test]
async fn object_changes_fail_under_planned_identity_and_release_reservations() {
    let (bytes, schema) = fixture();
    let scan = Scan::new(bytes.clone()).await;
    let config = scan.builder(Arc::new(scan.source(schema))).build();
    scan.store
        .put(&scan.file.object_meta.location, bytes.into())
        .await
        .unwrap();
    let error = scan.read(config).await.unwrap_err();
    assert!(error.to_string().contains("precondition"), "{error}");
    assert_eq!(scan.context.memory_pool().reserved(), 0);
}
