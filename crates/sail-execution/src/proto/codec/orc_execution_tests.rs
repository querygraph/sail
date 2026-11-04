#![expect(clippy::unwrap_used)]

use std::collections::HashMap;
use std::sync::Arc;

use bytes::Bytes;
use datafusion::arrow::array::{ArrayRef, PrimitiveArray, RecordBatch};
use datafusion::arrow::compute::concat_batches;
use datafusion::arrow::datatypes::{
    ArrowPrimitiveType, DataType, DurationMicrosecondType, Field, Int32Type, IntervalUnit,
    IntervalYearMonthType, Schema, SchemaRef, Time64NanosecondType, TimeUnit,
    TimestampMicrosecondType,
};
use datafusion::common::Result;
use datafusion::datasource::listing::PartitionedFile;
use datafusion::datasource::physical_plan::{FileGroup, FileScanConfigBuilder};
use datafusion::execution::TaskContext;
use datafusion::execution::memory_pool::{GreedyMemoryPool, MemoryPool};
use datafusion::execution::object_store::ObjectStoreUrl;
use datafusion::execution::runtime_env::RuntimeEnvBuilder;
use datafusion::physical_plan::ExecutionPlan;
use futures::{StreamExt, TryStreamExt};
use object_store::ObjectStoreExt;
use sail_common::spec;
use sail_orc::{OrcJavaTimeZoneConfig, OrcSource};

use super::{config, decode_node, encode_node, options};

#[path = "orc_test_store.rs"]
mod store;
use store::ObservedStore;

const TIME: &[u8] = include_bytes!("../../../../sail-orc/tests/fixtures/orc/time_precisions.orc");
const INTERVALS: &[u8] = include_bytes!("../../../../sail-orc/tests/fixtures/orc/intervals.orc");
const SUBMICRO: &[u8] =
    include_bytes!("../../../../sail-orc/tests/fixtures/orc/timestamps/foreign_submicro.orc");
const READER_REFERENCE: &str = include_str!(
    "../../../../sail-orc/tests/fixtures/orc/timestamps/explicit-reader-reference.json"
);
const STORE_URL: &str = "memory://orc-codec";
const AMPLE: usize = 64 * 1024 * 1024;

struct Fixture {
    store: Arc<ObservedStore>,
    file: PartitionedFile,
}

impl Fixture {
    async fn new(bytes: &'static [u8]) -> Result<Self> {
        let (store, file) = ObservedStore::fixture(bytes).await?;
        Ok(Self { store, file })
    }

    fn context(&self, pool: Arc<dyn MemoryPool>) -> Result<Arc<TaskContext>> {
        let runtime = RuntimeEnvBuilder::new()
            .with_memory_pool(pool)
            .build_arc()?;
        runtime.register_object_store(&url::Url::parse(STORE_URL).unwrap(), self.store.clone());
        Ok(Arc::new(TaskContext::default().with_runtime(runtime)))
    }

    fn builder(
        &self,
        schema: SchemaRef,
        reader: &str,
        display: &str,
        pool: Arc<dyn MemoryPool>,
    ) -> Result<FileScanConfigBuilder> {
        let source = OrcSource::try_new(
            schema,
            options(),
            OrcJavaTimeZoneConfig::for_reader_timezone(reader.to_string())?,
            display.to_string(),
            true,
            pool,
        )?;
        Ok(
            FileScanConfigBuilder::new(ObjectStoreUrl::parse(STORE_URL)?, Arc::new(source))
                .with_file_groups(vec![FileGroup::from(vec![self.file.clone()])])
                .with_batch_size(Some(1))
                .with_preserve_order(true),
        )
    }
}

fn pool(size: usize) -> Arc<dyn MemoryPool> {
    Arc::new(GreedyMemoryPool::new(size))
}

async fn read(plan: &Arc<dyn ExecutionPlan>, worker: Arc<TaskContext>) -> Result<Vec<RecordBatch>> {
    plan.execute(0, worker)?.try_collect().await
}

fn time_schema() -> SchemaRef {
    let mut fields = vec![Field::new("id", DataType::Int32, true)];
    fields.extend([0, 3, 6].map(|precision| {
        Field::new(
            format!("t{precision}"),
            DataType::Time64(TimeUnit::Nanosecond),
            true,
        )
        .with_metadata(HashMap::from([(
            spec::SAIL_SPARK_TIME_METADATA_KEY.to_string(),
            spec::SparkTimeMetadata::try_new(precision)
                .unwrap()
                .to_json()
                .unwrap(),
        )]))
    }));
    Arc::new(Schema::new(fields))
}

fn interval_schema() -> SchemaRef {
    use spec::IntervalFieldType::{Day, Month, Second, Year};
    let mut fields = vec![Field::new("id", DataType::Int32, true)];
    for (name, start, end, unit, data_type) in [
        (
            "ym",
            Year,
            Month,
            spec::IntervalUnit::YearMonth,
            DataType::Interval(IntervalUnit::YearMonth),
        ),
        (
            "dt",
            Day,
            Second,
            spec::IntervalUnit::DayTime,
            DataType::Duration(TimeUnit::Microsecond),
        ),
    ] {
        fields.push(
            Field::new(name, data_type, true).with_metadata(HashMap::from([(
                spec::SAIL_SPARK_INTERVAL_METADATA_KEY.to_string(),
                spec::SparkIntervalMetadata::try_new(unit, Some(start), Some(end))
                    .unwrap()
                    .unwrap()
                    .to_json()
                    .unwrap(),
            )])),
        );
    }
    Arc::new(Schema::new(fields))
}

fn values<T: ArrowPrimitiveType>(array: &ArrayRef) -> Vec<Option<T::Native>> {
    array
        .as_any()
        .downcast_ref::<PrimitiveArray<T>>()
        .unwrap()
        .iter()
        .collect()
}

#[tokio::test]
async fn remote_orc_executes_time_and_interval_metadata_with_exact_values() -> Result<()> {
    for (bytes, schema) in [(TIME, time_schema()), (INTERVALS, interval_schema())] {
        let fixture = Fixture::new(bytes).await?;
        let driver_pool = pool(AMPLE);
        let worker = fixture.context(pool(AMPLE))?;
        let node = encode_node(Arc::new(
            fixture
                .builder(schema.clone(), "UTC", "UTC", driver_pool.clone())?
                .build(),
        ))?;
        let decoded = decode_node(&node, &worker)?;
        assert_eq!(decoded.schema(), schema);
        assert_eq!(
            config(&decoded).file_groups[0].files()[0].object_meta,
            fixture.file.object_meta
        );
        let batches = read(&decoded, worker.clone()).await?;
        let batch = concat_batches(&schema, &batches)?;
        assert_eq!(batch.schema(), schema);
        assert_eq!(
            values::<Int32Type>(batch.column(0)),
            [Some(0), Some(1), Some(2), Some(3)]
        );
        if bytes == TIME {
            for column in 1..4 {
                assert_eq!(
                    values::<Time64NanosecondType>(batch.column(column)),
                    [
                        Some(0),
                        Some(45_296_123_456_000),
                        Some(86_399_999_999_000),
                        None
                    ]
                );
            }
        } else {
            assert_eq!(
                values::<IntervalYearMonthType>(batch.column(1)),
                [Some(14), Some(-14), Some(0), None]
            );
            assert_eq!(
                values::<DurationMicrosecondType>(batch.column(2)),
                [Some(183_845_123_456), Some(-183_845_123_456), Some(0), None]
            );
        }
        assert!(fixture.store.gets() > 0);
        assert_eq!(driver_pool.reserved(), 0);
        assert_eq!(worker.memory_pool().reserved(), 0);
    }
    Ok(())
}

#[tokio::test]
async fn remote_orc_keeps_captured_reader_timezone_independent_of_display() -> Result<()> {
    let reference: serde_json::Value = serde_json::from_str(READER_REFERENCE).unwrap();
    let mut observed = Vec::new();
    for run in reference["runs"].as_array().unwrap() {
        let reader = run["java"]["default_timezone_id"].as_str().unwrap();
        for case in run["cases"]
            .as_array()
            .unwrap()
            .iter()
            .filter(|case| case["name"] == "foreign_submicro")
        {
            let display = case["session_timezone"].as_str().unwrap();
            let expected = case["rows"]
                .as_array()
                .unwrap()
                .iter()
                .map(|row| row["ts_micros"].as_i64())
                .collect::<Vec<_>>();
            let schema = Arc::new(Schema::new(vec![
                Field::new("id", DataType::Int32, true),
                Field::new(
                    "ts",
                    DataType::Timestamp(TimeUnit::Microsecond, Some(display.into())),
                    true,
                ),
            ]));
            let fixture = Fixture::new(SUBMICRO).await?;
            let worker = fixture.context(pool(AMPLE))?;
            let node = encode_node(Arc::new(
                fixture
                    .builder(schema.clone(), reader, display, pool(AMPLE))?
                    .build(),
            ))?;
            let decoded = decode_node(&node, &worker)?;
            let batches = read(&decoded, worker.clone()).await?;
            let batch = concat_batches(&schema, &batches)?;
            assert_eq!(batch.schema(), schema);
            let actual = values::<TimestampMicrosecondType>(batch.column(1));
            assert_eq!(actual, expected);
            observed.push(actual);
            assert_eq!(worker.memory_pool().reserved(), 0);
        }
    }
    assert_eq!(observed.len(), 4);
    assert_eq!(observed[0], observed[1]);
    assert_eq!(observed[2], observed[3]);
    assert_ne!(observed[0], observed[2]);
    assert_eq!(
        observed[2],
        [Some(1_577_934_245_123_456), Some(-1_876_544), Some(0), None]
    );
    Ok(())
}

#[tokio::test]
async fn remote_orc_rejects_replaced_and_identityless_objects() -> Result<()> {
    for replace in [false, true] {
        let fixture = Fixture::new(TIME).await?;
        let worker = fixture.context(pool(AMPLE))?;
        let mut file = fixture.file.clone();
        if !replace {
            file.object_meta.e_tag = None;
            file.object_meta.version = None;
        }
        let node = encode_node(Arc::new(
            fixture
                .builder(time_schema(), "UTC", "UTC", pool(AMPLE))?
                .with_file_groups(vec![FileGroup::from(vec![file.clone()])])
                .build(),
        ))?;
        if replace {
            fixture
                .store
                .put(&file.object_meta.location, Bytes::from_static(TIME).into())
                .await?;
        }
        let decoded = decode_node(&node, &worker)?;
        assert_eq!(
            config(&decoded).file_groups[0].files()[0].object_meta,
            file.object_meta
        );
        let error = read(&decoded, worker.clone()).await.unwrap_err();
        let expected = if replace {
            "precondition"
        } else {
            "version or usable ETag"
        };
        assert!(error.to_string().contains(expected), "{error}");
        assert_eq!(fixture.store.gets(), usize::from(replace));
        assert_eq!(worker.memory_pool().reserved(), 0);
    }
    Ok(())
}

#[tokio::test]
async fn remote_orc_invalid_ranges_fail_before_any_get() -> Result<()> {
    let fixture = Fixture::new(TIME).await?;
    let worker = fixture.context(pool(AMPLE))?;
    for (start, end) in [
        (-1, 3),
        (0, -1),
        (2, 1),
        (0, i64::try_from(TIME.len()).unwrap() + 1),
    ] {
        let file = fixture.file.clone().with_range(start, end);
        let node = encode_node(Arc::new(
            fixture
                .builder(time_schema(), "UTC", "UTC", pool(AMPLE))?
                .with_file_groups(vec![FileGroup::from(vec![file])])
                .build(),
        ))?;
        let decoded = decode_node(&node, &worker)?;
        let error = read(&decoded, worker.clone()).await.unwrap_err();
        assert!(error.to_string().contains("ORC file range"), "{error}");
        assert_eq!(fixture.store.gets(), 0);
        assert_eq!(worker.memory_pool().reserved(), 0);
    }
    Ok(())
}

#[tokio::test]
async fn remote_orc_rebinds_to_the_worker_pool_in_both_directions() -> Result<()> {
    for (driver_limit, worker_limit) in [(AMPLE, 0), (0, AMPLE)] {
        let fixture = Fixture::new(TIME).await?;
        let driver_pool = pool(driver_limit);
        let worker = fixture.context(pool(worker_limit))?;
        let node = encode_node(Arc::new(
            fixture
                .builder(time_schema(), "UTC", "UTC", driver_pool.clone())?
                .build(),
        ))?;
        let decoded = decode_node(&node, &worker)?;
        let result = read(&decoded, worker.clone()).await;
        if worker_limit == 0 {
            let error = result.unwrap_err();
            assert!(error.to_string().contains("memory"), "{error}");
            assert_eq!(fixture.store.gets(), 0);
        } else {
            assert_eq!(result?.iter().map(RecordBatch::num_rows).sum::<usize>(), 4);
            assert!(fixture.store.gets() > 0);
        }
        assert_eq!(driver_pool.reserved(), 0);
        assert_eq!(worker.memory_pool().reserved(), 0);
    }
    Ok(())
}

#[tokio::test]
async fn dropping_remote_orc_stream_releases_worker_reservations() -> Result<()> {
    let fixture = Fixture::new(TIME).await?;
    let worker = fixture.context(pool(AMPLE))?;
    let node = encode_node(Arc::new(
        fixture
            .builder(time_schema(), "UTC", "UTC", pool(AMPLE))?
            .build(),
    ))?;
    let decoded = decode_node(&node, &worker)?;
    let mut stream = decoded.execute(0, worker.clone())?;
    let batch = stream.next().await.unwrap()?;
    assert_eq!(batch.num_rows(), 1);
    assert!(worker.memory_pool().reserved() > 0);
    let gets = fixture.store.gets();
    drop(stream);
    assert_eq!(worker.memory_pool().reserved(), 0);
    assert_eq!(values::<Int32Type>(batch.column(0)), [Some(0)]);
    tokio::task::yield_now().await;
    assert_eq!(fixture.store.gets(), gets);
    Ok(())
}
