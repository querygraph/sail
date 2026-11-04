#![expect(clippy::unwrap_used)]

use std::sync::Arc;

use bytes::Bytes;
use datafusion::arrow::array::{Int32Array, StringArray};
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::common::{Result, ScalarValue};
use datafusion::datasource::listing::PartitionedFile;
use datafusion::datasource::physical_plan::FileGroup;
use datafusion::datasource::table_schema::TableSchema;
use datafusion::execution::config::SessionConfig;
use datafusion::execution::object_store::ObjectStoreUrl;
use object_store::ObjectStoreExt;
use object_store::memory::InMemory;
use object_store::path::Path;
use sail_orc::options::r#gen::OrcReadOptions;
use sail_orc::{OrcJavaTimeZoneConfig, OrcSource};

use super::file_scan_tests::{assert_groups, config};
use super::*;
use crate::proto::{decode_remote_physical_plan, encode_remote_physical_plan};

const ORC: &[u8] = include_bytes!(concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/../sail-orc/tests/fixtures/orc/time_precisions.orc"
));
const STORE_URL: &str = "memory://orc-worker-preparation";

fn worker(store: Arc<InMemory>) -> Arc<TaskContext> {
    let mut session_config = SessionConfig::new();
    session_config
        .options_mut()
        .execution
        .enable_file_stream_work_stealing = true;
    let context = Arc::new(TaskContext::default().with_session_config(session_config));
    context
        .runtime_env()
        .register_object_store(&url::Url::parse(STORE_URL).unwrap(), store);
    context
}

async fn rows(
    plan: &Arc<dyn ExecutionPlan>,
    partition: usize,
    context: Arc<TaskContext>,
) -> Result<Vec<(String, i32)>> {
    let batches = plan
        .execute(partition, context)?
        .try_collect::<Vec<_>>()
        .await?;
    let mut rows = vec![];
    for batch in batches {
        let files = batch
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let ids = batch
            .column(1)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        rows.extend(
            files
                .iter()
                .zip(ids.iter())
                .map(|(file, id)| (file.unwrap().to_string(), id.unwrap())),
        );
    }
    Ok(rows)
}

fn expected_rows(files: &[&str]) -> Vec<(String, i32)> {
    files
        .iter()
        .flat_map(|file| (0..4).map(move |id| (file.to_string(), id)))
        .collect()
}

#[tokio::test]
async fn separately_decoded_orc_tasks_read_only_their_file_groups() -> Result<()> {
    let store = Arc::new(InMemory::new());
    let mut files = vec![];
    for name in ["first.orc", "second.orc", "third.orc"] {
        let path = Path::from(name);
        store.put(&path, Bytes::from_static(ORC).into()).await?;
        let mut file = PartitionedFile::new_from_meta(store.head(&path).await?);
        file.partition_values = vec![ScalarValue::Utf8(Some(name.to_string()))];
        files.push(file);
    }
    let driver = worker(store.clone());
    let schema = TableSchema::builder(Arc::new(Schema::new(vec![Field::new(
        "id",
        DataType::Int32,
        true,
    )])))
    .with_table_partition_cols(vec![Arc::new(Field::new("file", DataType::Utf8, false))])
    .build();
    let source = OrcSource::try_new(
        schema,
        OrcReadOptions {
            max_encoded_metadata_bytes: 64 * 1024 * 1024,
            max_decoded_metadata_bytes: 64 * 1024 * 1024,
            max_decoded_stripe_footer_bytes: 64 * 1024 * 1024,
            max_schema_depth: 64,
        },
        OrcJavaTimeZoneConfig::for_reader_timezone("UTC".to_string())?,
        "UTC".to_string(),
        true,
        driver.memory_pool().clone(),
    )?;
    let original = FileScanConfigBuilder::new(ObjectStoreUrl::parse(STORE_URL)?, Arc::new(source))
        .with_file_groups(vec![
            FileGroup::from(files[..1].to_vec()),
            FileGroup::from(vec![]),
            FileGroup::from(files[1..].to_vec()),
        ])
        .with_projection_indices(Some(vec![1, 0]))?
        .with_batch_size(Some(2))
        .build();
    let encoded = encode_remote_physical_plan(
        &RemoteExecutionCodec,
        DataSourceExec::from_data_source(original.clone()),
    )?;

    // Without task preparation, a fresh process-local queue lets even the
    // empty partition consume every file. Keep this regression control real.
    let context = worker(store.clone());
    let unprepared = decode_remote_physical_plan(&context, &RemoteExecutionCodec, &encoded)?;
    assert!(!config(&unprepared).preserve_order);
    let mut shared_rows = rows(&unprepared, 1, context).await?;
    shared_rows.sort();
    assert_eq!(
        shared_rows,
        expected_rows(&["first.orc", "second.orc", "third.orc"])
    );

    let expected = [
        expected_rows(&["first.orc"]),
        vec![],
        expected_rows(&["second.orc", "third.orc"]),
    ];
    let mut combined = vec![];
    for (partition, expected) in expected.into_iter().enumerate() {
        // Each task decodes independently, as TaskPreparation::execute_plan
        // does. Sharing one decoded plan would hide sibling-queue duplication.
        let context = worker(store.clone());
        let decoded = decode_remote_physical_plan(&context, &RemoteExecutionCodec, &encoded)?;
        let prepared = TaskPreparation::rewrite_file_scans(decoded).unwrap();
        assert!(config(&prepared).preserve_order);
        assert_groups(&original, config(&prepared));
        assert_eq!(prepared.output_partitioning().partition_count(), 3);
        assert_eq!(prepared.schema().field(0).name(), "file");
        assert_eq!(prepared.schema().field(1).name(), "id");
        let actual = rows(&prepared, partition, context.clone()).await?;
        assert_eq!(actual, expected);
        assert_eq!(context.memory_pool().reserved(), 0);
        combined.extend(actual);
    }
    assert_eq!(
        combined,
        expected_rows(&["first.orc", "second.orc", "third.orc"])
    );
    Ok(())
}
