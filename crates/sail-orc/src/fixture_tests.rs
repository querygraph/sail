//! Spark 4.2.1 ORC files, including their generation SQL and reference results,
//! live in tests/fixtures/orc. These tests exercise the real native decoder and
//! logical bridge together; they do not exercise a registered Sail file source.

#![expect(clippy::unwrap_used)]

use std::collections::HashMap;
use std::sync::Arc;

use arrow::array::{Array, ArrayRef, ListArray, MapArray, PrimitiveArray, StructArray};
use arrow::compute::concat_batches;
use arrow::datatypes::{
    ArrowPrimitiveType, DataType, DurationMicrosecondType, Field, Fields, Int32Type, IntervalUnit,
    IntervalYearMonthType, Time64NanosecondType, TimeUnit,
};
use arrow::record_batch::RecordBatch;
use bytes::Bytes;
use orc_rust::ArrowReaderBuilder;
use orc_rust::reader::metadata::{MetadataReadOptions, read_metadata_with_options};
use orc_rust::schema::TimestampPrecision;
use sail_common::spec;
use sail_common::spec::IntervalFieldType::{Day, Hour, Minute, Month, Second, Year};

use super::OrcSchemaBridge;

const TIME: &[u8] = include_bytes!("../tests/fixtures/orc/time_precisions.orc");
const CAST_TIME: &[u8] = include_bytes!("../tests/fixtures/orc/time_cast_precisions.orc");
const INTERVALS: &[u8] = include_bytes!("../tests/fixtures/orc/intervals.orc");
const NESTED: &[u8] = include_bytes!("../tests/fixtures/orc/daytime_time_nested.orc");

fn read_fixture(file: &'static [u8], batch_size: usize) -> RecordBatch {
    let mut bytes = Bytes::from_static(file);
    let metadata = Arc::new(
        read_metadata_with_options(
            &mut bytes,
            MetadataReadOptions {
                max_encoded_metadata_bytes: Some(64 * 1024),
                max_decoded_metadata_bytes: Some(64 * 1024),
                max_schema_depth: Some(16),
            },
        )
        .unwrap(),
    );
    let bridge = OrcSchemaBridge::try_new(&metadata, "UTC").unwrap();
    let reader = ArrowReaderBuilder::new_with_metadata(bytes, metadata)
        .with_timestamp_precision(TimestampPrecision::Microsecond)
        .with_batch_size(batch_size)
        .build();
    let batches = reader
        .map(|batch| {
            let batch = batch.unwrap();
            assert_eq!(batch.schema().fields(), bridge.physical_schema().fields());
            bridge.adapt_batch(batch).unwrap()
        })
        .collect::<Vec<_>>();
    assert!(batches.iter().all(|batch| batch.num_rows() <= batch_size));
    concat_batches(bridge.schema(), &batches).unwrap()
}

fn values<T: ArrowPrimitiveType>(array: &ArrayRef) -> Vec<Option<T::Native>> {
    array
        .as_any()
        .downcast_ref::<PrimitiveArray<T>>()
        .unwrap()
        .iter()
        .collect()
}

fn time_field(name: &str, precision: i32) -> Field {
    Field::new(name, DataType::Time64(TimeUnit::Nanosecond), true).with_metadata(HashMap::from([(
        spec::SAIL_SPARK_TIME_METADATA_KEY.to_string(),
        spec::SparkTimeMetadata::try_new(precision)
            .unwrap()
            .to_json()
            .unwrap(),
    )]))
}

fn interval_field(
    name: &str,
    start: spec::IntervalFieldType,
    end: spec::IntervalFieldType,
) -> Field {
    let (unit, data_type) = if matches!(start, Year | Month) {
        (
            spec::IntervalUnit::YearMonth,
            DataType::Interval(IntervalUnit::YearMonth),
        )
    } else {
        (
            spec::IntervalUnit::DayTime,
            DataType::Duration(TimeUnit::Microsecond),
        )
    };
    Field::new(name, data_type, true).with_metadata(HashMap::from([(
        spec::SAIL_SPARK_INTERVAL_METADATA_KEY.to_string(),
        spec::SparkIntervalMetadata::try_new(unit, Some(start), Some(end))
            .unwrap()
            .unwrap()
            .to_json()
            .unwrap(),
    )]))
}

fn assert_schema(batch: &RecordBatch, fields: Vec<Field>) {
    assert_eq!(batch.schema().fields(), &Fields::from(fields));
    assert_eq!(
        batch.schema().metadata()["org.apache.spark.version"],
        "4.2.1"
    );
}

#[test]
fn spark_time_files_preserve_values_independently_of_declared_precision() {
    for batch_size in [1, 2, 16] {
        for (file, truncated) in [(TIME, false), (CAST_TIME, true)] {
            let batch = read_fixture(file, batch_size);
            assert_eq!(batch.num_rows(), 4);
            assert_schema(
                &batch,
                vec![
                    Field::new("id", DataType::Int32, true),
                    time_field("t0", 0),
                    time_field("t3", 3),
                    time_field("t6", 6),
                ],
            );
            assert_eq!(
                values::<Int32Type>(batch.column(0)),
                [Some(0), Some(1), Some(2), Some(3)]
            );
            for (column, precision) in [(1, 0), (2, 3), (3, 6)] {
                let factor = if truncated {
                    10_i64.pow(9 - precision as u32)
                } else {
                    1
                };
                assert_eq!(
                    values::<Time64NanosecondType>(batch.column(column)),
                    [
                        Some(0),
                        Some(45_296_123_456_000 / factor * factor),
                        Some(86_399_999_999_000 / factor * factor),
                        None
                    ]
                );
            }
        }
    }
}

#[test]
fn spark_interval_file_matches_reference_numeric_projections() {
    // Spark's Python Arrow path cannot represent year-month intervals. The
    // reference therefore uses SQL numeric projections before collection.
    for batch_size in [1, 2, 16] {
        let batch = read_fixture(INTERVALS, batch_size);
        assert_eq!(batch.num_rows(), 4);
        assert_schema(
            &batch,
            vec![
                Field::new("id", DataType::Int32, true),
                interval_field("ym", Year, Month),
                interval_field("y", Year, Year),
                interval_field("m", Month, Month),
                interval_field("dt", Day, Second),
                interval_field("d", Day, Day),
                interval_field("h", Hour, Hour),
                interval_field("mi", Minute, Minute),
                interval_field("sec", Second, Second),
            ],
        );
        for (column, months) in [(1, 14), (2, 24), (3, 14)] {
            assert_eq!(
                values::<IntervalYearMonthType>(batch.column(column)),
                [Some(months), Some(-months), Some(0), None]
            );
        }
        for (column, micros, third) in [
            (4, 183_845_123_456, 0),
            (5, 172_800_000_000, 0),
            (6, 97_200_000_000, 0),
            (7, 3_900_000_000, 0),
            (8, 1_000_001, 1),
        ] {
            assert_eq!(
                values::<DurationMicrosecondType>(batch.column(column)),
                [Some(micros), Some(-micros), Some(third), None]
            );
        }
    }
}

#[test]
fn spark_nested_file_preserves_null_empty_and_nonempty_containers() {
    for batch_size in [1, 2, 16] {
        let batch = read_fixture(NESTED, batch_size);
        assert_eq!(batch.num_rows(), 3);
        assert_schema(
            &batch,
            vec![
                Field::new("id", DataType::Int32, true),
                interval_field("dt", Day, Second),
                Field::new(
                    "ds",
                    DataType::List(Arc::new(interval_field(
                        spec::SAIL_LIST_FIELD_NAME,
                        Second,
                        Second,
                    ))),
                    true,
                ),
                Field::new(
                    "tm",
                    DataType::Map(
                        Arc::new(Field::new(
                            spec::SAIL_MAP_FIELD_NAME,
                            DataType::Struct(
                                vec![
                                    time_field(spec::SAIL_MAP_KEY_FIELD_NAME, 3)
                                        .with_nullable(false),
                                    interval_field(spec::SAIL_MAP_VALUE_FIELD_NAME, Second, Second),
                                ]
                                .into(),
                            ),
                            false,
                        )),
                        false,
                    ),
                    true,
                ),
                Field::new(
                    "st",
                    DataType::Struct(
                        vec![time_field("t", 6), interval_field("d", Second, Second)].into(),
                    ),
                    true,
                ),
            ],
        );
        assert_eq!(
            values::<DurationMicrosecondType>(batch.column(1)),
            [Some(-183_845_123_456), None, Some(0)]
        );
        let list = batch
            .column(2)
            .as_any()
            .downcast_ref::<ListArray>()
            .unwrap();
        assert_eq!(list.value_offsets(), [0, 3, 3, 3]);
        assert_eq!(
            (0..3).map(|i| list.is_null(i)).collect::<Vec<_>>(),
            [false, true, false]
        );
        assert_eq!(
            values::<DurationMicrosecondType>(list.values()),
            [Some(-1_000_001), None, Some(1)]
        );

        let map = batch.column(3).as_any().downcast_ref::<MapArray>().unwrap();
        assert_eq!(map.value_offsets(), [0, 1, 1, 1]);
        assert_eq!(
            (0..3).map(|i| map.is_null(i)).collect::<Vec<_>>(),
            [false, true, false]
        );
        assert_eq!(
            values::<Time64NanosecondType>(map.keys()),
            [Some(3_723_123_000_000)]
        );
        assert_eq!(
            values::<DurationMicrosecondType>(map.values()),
            [Some(-1_000_001)]
        );

        let structure = batch
            .column(4)
            .as_any()
            .downcast_ref::<StructArray>()
            .unwrap();
        assert_eq!(
            (0..3).map(|i| structure.is_null(i)).collect::<Vec<_>>(),
            [false, true, false]
        );
        // Child slots underneath a null parent are not logical values.
        let times = values::<Time64NanosecondType>(structure.column(0));
        let durations = values::<DurationMicrosecondType>(structure.column(1));
        assert_eq!((times[0], times[2]), (Some(45_296_123_456_000), None));
        assert_eq!((durations[0], durations[2]), (Some(-1), None));
    }
}
