//! Retained Spark/Java ORC values, requalified with directly observed LA and UTC
//! reader JVMs. The original reader ID was unlogged. Spark Connect's UTC Arrow
//! labels and Sail's internal display labels are separate representation choices.

#![expect(clippy::unwrap_used)]

use std::collections::BTreeMap;
use std::fmt::Write;
use std::io::Cursor;
use std::sync::Arc;

use arrow::array::{
    Array, ArrayRef, ListArray, MapArray, PrimitiveArray, StringArray, StructArray,
};
use arrow::compute::concat_batches;
use arrow::datatypes::{
    ArrowPrimitiveType, DataType, Int32Type, TimeUnit, TimestampMicrosecondType,
};
use arrow::record_batch::RecordBatch;
use bytes::Bytes;
use futures::TryStreamExt;
use orc_rust::ArrowReaderBuilder;
use orc_rust::reader::metadata::{FileMetadata, MetadataReadOptions, read_metadata_with_options};
use orc_rust::schema::TimestampPrecision;
use serde_json::Value;
use sha2::{Digest, Sha256};

use super::{OrcJavaTimeZoneConfig, OrcSchemaBridge};

const READER_ZONE: &str = "America/Los_Angeles";
const UTC: &[u8] = include_bytes!("../tests/fixtures/orc/timestamps/timestamps_utc.orc");
const LOS_ANGELES: &[u8] =
    include_bytes!("../tests/fixtures/orc/timestamps/timestamps_los_angeles.orc");
const NESTED: &[u8] = include_bytes!("../tests/fixtures/orc/timestamps/nested_timestamp_nulls.orc");
const SUBMICRO: &[u8] = include_bytes!("../tests/fixtures/orc/timestamps/foreign_submicro.orc");
const REFERENCE: &str = include_str!("../tests/fixtures/orc/timestamps/spark-reference.json");
const REREAD: &str =
    include_str!("../tests/fixtures/orc/timestamps/explicit-reader-reference.json");
const FIXTURES: [(&str, &[u8]); 4] = [
    ("timestamps_utc", UTC),
    ("timestamps_los_angeles", LOS_ANGELES),
    ("nested_timestamp_nulls", NESTED),
    ("foreign_submicro", SUBMICRO),
];

fn metadata(file: &'static [u8]) -> Arc<FileMetadata> {
    Arc::new(
        read_metadata_with_options(
            &mut Bytes::from_static(file),
            MetadataReadOptions {
                max_encoded_metadata_bytes: Some(64 * 1024),
                max_decoded_metadata_bytes: Some(64 * 1024),
                max_schema_depth: Some(16),
            },
        )
        .unwrap(),
    )
}

fn reader_config() -> OrcJavaTimeZoneConfig {
    OrcJavaTimeZoneConfig::for_reader_timezone(READER_ZONE.to_string()).unwrap()
}

fn adapt(bridge: &OrcSchemaBridge, batch: RecordBatch) -> RecordBatch {
    assert_eq!(batch.schema().fields(), bridge.physical_schema().fields());
    let tail = (batch.num_rows() > 1).then(|| batch.slice(1, batch.num_rows() - 1));
    let output = bridge.adapt_batch(batch).unwrap();
    if let Some(tail) = tail {
        // Native lists/maps retain child buffers when the parent is sliced.
        // Conversion must preserve the nonzero offsets and parent validity.
        assert_eq!(
            bridge.adapt_batch(tail).unwrap(),
            output.slice(1, output.num_rows() - 1)
        );
    }
    output
}

fn read(file: &'static [u8], display_zone: &str, batch_size: usize) -> RecordBatch {
    read_with_reader(file, display_zone, batch_size, READER_ZONE)
}

fn read_with_reader(
    file: &'static [u8],
    display_zone: &str,
    batch_size: usize,
    reader_zone: &str,
) -> RecordBatch {
    let config = OrcJavaTimeZoneConfig::for_reader_timezone(reader_zone.to_string()).unwrap();
    let (builder, bridge) = OrcSchemaBridge::try_new_reader(
        Bytes::from_static(file),
        metadata(file),
        display_zone,
        &config,
        None,
    )
    .unwrap();
    let batches = builder
        .with_batch_size(batch_size)
        .build()
        .map(|batch| adapt(&bridge, batch.unwrap()))
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

fn micros(array: &ArrayRef) -> Vec<Option<i64>> {
    values::<TimestampMicrosecondType>(array)
}

fn expected_column(rows: &Value, column: &str) -> Vec<Option<i64>> {
    rows.as_array()
        .unwrap()
        .iter()
        .map(|row| serde_json::from_value(row[column].clone()).unwrap())
        .collect()
}

#[test]
fn retained_timestamp_bytes_match_the_original_validation_manifest() {
    let reference: Value = serde_json::from_str(REFERENCE).unwrap();
    assert_eq!(reference["spark_version"], "4.2.1");
    assert_eq!(reference["selected_reader_timezone"], READER_ZONE);
    assert_eq!(
        reference["profile_bundle_sha256"],
        reader_config().profile_bundle_sha256()
    );
    for (name, file) in FIXTURES {
        let expected = &reference["cases"][name];
        assert_eq!(expected["bytes"], u64::try_from(file.len()).unwrap());
        let mut digest = String::with_capacity(64);
        for byte in Sha256::digest(file) {
            write!(digest, "{byte:02x}").unwrap();
        }
        assert_eq!(expected["sha256"], digest);
    }
}

#[test]
fn fresh_reader_contexts_are_observed_and_match_retained_scalar_values() {
    let original: Value = serde_json::from_str(REFERENCE).unwrap();
    let reread: Value = serde_json::from_str(REREAD).unwrap();
    assert_eq!(reread["spark_source_pin"], original["spark_source_pin"]);
    let expected_hash = original["sources"]
        .as_array()
        .unwrap()
        .iter()
        .find(|source| source["file"] == "expected.json")
        .unwrap();
    assert_eq!(
        reread["original_reference_expected_sha256"],
        expected_hash["sha256"]
    );
    let runs = reread["runs"].as_array().unwrap();
    assert_eq!(runs.len(), 2);
    for (run, reader) in runs.iter().zip([READER_ZONE, "UTC"]) {
        assert_eq!(run["status"], "passed");
        assert_eq!(run["owned_gateway_exit_code"], 0);
        assert_eq!(run["requested_reader_timezone"], reader);
        assert_eq!(run["java"]["default_timezone_id"], reader);
        assert_eq!(run["java"]["user_timezone_property"], reader);
        assert_eq!(run["java"]["actual_pid"], run["java"]["gateway_pid"]);
        assert_eq!(run["java"]["format_locale"], "en-US");
        assert_eq!(run["java"]["oldmapping_property"], "false");
        assert_eq!(run["java"]["runtime_version"], "20.0.2+9-78");
        assert_eq!(run["java"]["tzdb_versions"], serde_json::json!(["2023c"]));
        let cases = run["cases"].as_array().unwrap();
        assert_eq!(cases.len(), if reader == READER_ZONE { 8 } else { 2 });
        for case in cases {
            let name = case["name"].as_str().unwrap();
            assert_eq!(case["reader_timezone"], reader);
            assert_eq!(case["fixture_sha256"], original["cases"][name]["sha256"]);
            assert_eq!(case["matches_expected"], true);
            assert!(case["arrow_schema"].as_str().unwrap().contains("tz=UTC"));
            if reader == READER_ZONE && name != "nested_timestamp_nulls" {
                let numeric = &original["cases"][name]["read_numeric"];
                let expected = if name == "foreign_submicro" {
                    numeric
                } else {
                    &numeric["value"]
                };
                assert_eq!(&case["rows"], expected);
            }
        }
    }
}

#[test]
fn observed_reader_identity_changes_values_independently_of_display_labels() {
    let reread: Value = serde_json::from_str(REREAD).unwrap();
    let mut observed = Vec::new();
    for run in reread["runs"].as_array().unwrap() {
        let reader = run["java"]["default_timezone_id"].as_str().unwrap();
        for case in run["cases"].as_array().unwrap() {
            if case["name"] != "foreign_submicro" {
                continue;
            }
            let display = case["session_timezone"].as_str().unwrap();
            let expected = expected_column(&case["rows"], "ts_micros");
            let batch = read_with_reader(SUBMICRO, display, 2, reader);
            assert_eq!(micros(batch.column(1)), expected);
            assert_eq!(
                batch.column(1).data_type(),
                &DataType::Timestamp(TimeUnit::Microsecond, Some(display.into()))
            );
            observed.push(expected);
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
}

#[test]
fn scalar_timestamp_files_match_spark_reads_and_preserve_the_negative_epoch_edge() {
    let reference: Value = serde_json::from_str(REFERENCE).unwrap();
    for (name, file) in [
        ("timestamps_utc", UTC),
        ("timestamps_los_angeles", LOS_ANGELES),
    ] {
        let expected = &reference["cases"][name]["read_numeric"]["value"];
        for display in ["UTC", "America/Los_Angeles", "Asia/Kathmandu"] {
            for batch_size in [1, 2, 16] {
                let batch = read(file, display, batch_size);
                assert_eq!(
                    values::<Int32Type>(batch.column(0)),
                    [Some(0), Some(1), Some(2), Some(3), Some(4), Some(5)]
                );
                assert_eq!(
                    micros(batch.column(1)),
                    expected_column(expected, "ntz_micros")
                );
                assert_eq!(
                    micros(batch.column(2)),
                    expected_column(expected, "ltz_micros")
                );
                assert_eq!(
                    batch.column(1).data_type(),
                    &DataType::Timestamp(TimeUnit::Microsecond, None)
                );
                assert_eq!(
                    batch.column(2).data_type(),
                    &DataType::Timestamp(TimeUnit::Microsecond, Some(display.into()))
                );
                assert!(
                    batch
                        .schema()
                        .fields()
                        .iter()
                        .all(|field| field.is_nullable())
                );
            }
        }
    }
    let original = &reference["cases"]["timestamps_utc"];
    assert_eq!(original["source_numeric"]["value"][2]["ltz_micros"], -1);
    let observed = read(UTC, "UTC", 16);
    assert_eq!(micros(observed.column(1))[2], Some(-1));
    // This is Spark's retained file-read result, not the source SQL value.
    assert_eq!(micros(observed.column(2))[2], Some(999_999));
}

#[test]
fn foreign_submicroseconds_convert_reader_timezone_before_truncation() {
    let reference: Value = serde_json::from_str(REFERENCE).unwrap();
    let expected = expected_column(
        &reference["cases"]["foreign_submicro"]["read_numeric"],
        "ts_micros",
    );
    assert_eq!(
        expected,
        [
            Some(1_577_963_045_123_456),
            Some(28_798_123_456),
            Some(28_800_000_000),
            None
        ]
    );
    for display in ["UTC", "Asia/Kathmandu"] {
        for batch_size in [1, 2, 16] {
            let batch = read(SUBMICRO, display, batch_size);
            assert_eq!(micros(batch.column(1)), expected);
            assert_eq!(
                batch.column(1).data_type(),
                &DataType::Timestamp(TimeUnit::Microsecond, Some(display.into()))
            );
        }
    }
    // Configuring timezone conversion does not alter native strict precision.
    // Only the controlled Sail factory opts into microsecond truncation.
    let mut strict =
        ArrowReaderBuilder::new_with_metadata(Bytes::from_static(SUBMICRO), metadata(SUBMICRO))
            .with_timestamp_precision(TimestampPrecision::Microsecond)
            .with_timestamp_time_zone_options(reader_config().native_options().unwrap())
            .build();
    let error = strict.next().unwrap().unwrap_err();
    assert!(error.to_string().contains("loses precision"));
}

fn nulls(array: &dyn Array) -> Vec<bool> {
    (0..array.len()).map(|index| array.is_null(index)).collect()
}

fn timestamp_labels(data_type: &DataType, display: &str, counts: &mut (usize, usize)) {
    match data_type {
        DataType::Timestamp(unit, zone) => {
            assert_eq!(*unit, TimeUnit::Microsecond);
            if let Some(zone) = zone {
                assert_eq!(zone.as_ref(), display);
                counts.1 += 1;
            } else {
                counts.0 += 1;
            }
        }
        DataType::Struct(fields) => {
            for field in fields {
                timestamp_labels(field.data_type(), display, counts);
            }
        }
        DataType::List(field) | DataType::Map(field, _) => {
            timestamp_labels(field.data_type(), display, counts);
        }
        _ => {}
    }
}

#[test]
fn nested_spark_timestamps_preserve_null_empty_and_nonempty_values() {
    for display in ["UTC", "Asia/Kathmandu"] {
        for batch_size in [1, 2, 16] {
            let batch = read(NESTED, display, batch_size);
            assert_eq!(
                values::<Int32Type>(batch.column(0)),
                [Some(0), Some(1), Some(2)]
            );
            let mut counts = (0, 0);
            for field in batch.schema().fields() {
                timestamp_labels(field.data_type(), display, &mut counts);
            }
            assert_eq!(counts, (4, 3));
            let structure = batch
                .column(1)
                .as_any()
                .downcast_ref::<StructArray>()
                .unwrap();
            assert_eq!(nulls(structure), [false, true, false]);
            assert_eq!(micros(structure.column(0)), [Some(-1_876_544), None, None]);
            assert_eq!(
                micros(structure.column(1)),
                [Some(1_577_934_245_123_456), None, None]
            );

            for (column, offsets, expected) in [
                (
                    2,
                    [0, 3, 3, 3],
                    vec![Some(-1_876_544), None, Some(1_577_934_245_123_456)],
                ),
                (3, [0, 2, 2, 2], vec![Some(1_577_934_245_123_456), None]),
            ] {
                let list = batch
                    .column(column)
                    .as_any()
                    .downcast_ref::<ListArray>()
                    .unwrap();
                assert_eq!(list.value_offsets(), offsets);
                assert_eq!(nulls(list), [false, true, false]);
                assert_eq!(micros(list.values()), expected);
            }

            let map = batch.column(4).as_any().downcast_ref::<MapArray>().unwrap();
            assert_eq!(map.value_offsets(), [0, 2, 2, 2]);
            assert_eq!(nulls(map), [false, true, false]);
            let keys = map.keys().as_any().downcast_ref::<StringArray>().unwrap();
            let entries = keys
                .iter()
                .map(Option::unwrap)
                .zip(micros(map.values()))
                .collect::<BTreeMap<_, _>>();
            assert_eq!(
                entries,
                BTreeMap::from([("null", None), ("value", Some(-1_876_544))])
            );
            assert!(!map.entries().fields()[0].is_nullable());

            let structures = batch
                .column(5)
                .as_any()
                .downcast_ref::<ListArray>()
                .unwrap();
            assert_eq!(structures.value_offsets(), [0, 2, 2, 2]);
            assert_eq!(nulls(structures), [false, true, false]);
            let entries = structures
                .values()
                .as_any()
                .downcast_ref::<StructArray>()
                .unwrap();
            assert_eq!(nulls(entries), [false, true]);
            assert_eq!(
                micros(entries.column(0)),
                [Some(1_577_934_245_123_456), None]
            );
            assert_eq!(micros(entries.column(1)), [None, None]);
        }
    }
}

#[test]
fn old_bridge_still_rejects_unconverted_ltz_from_a_real_spark_file() {
    let metadata = metadata(UTC);
    let bridge = OrcSchemaBridge::try_new(&metadata, "UTC").unwrap();
    let batch = ArrowReaderBuilder::new_with_metadata(Bytes::from_static(UTC), metadata)
        .with_timestamp_precision(TimestampPrecision::Microsecond)
        .build()
        .next()
        .unwrap()
        .unwrap();
    assert_eq!(
        batch.column(2).data_type(),
        &DataType::Timestamp(TimeUnit::Microsecond, None)
    );
    let error = bridge.adapt_batch(batch).unwrap_err();
    assert!(
        error
            .to_string()
            .contains("explicit native reader timezone decoding")
    );
}

#[tokio::test]
async fn actual_timestamp_fixtures_match_through_the_async_factory() {
    for (_, file) in FIXTURES {
        let (builder, bridge) = OrcSchemaBridge::try_new_reader(
            Cursor::new(file.to_vec()),
            metadata(file),
            "Asia/Kathmandu",
            &reader_config(),
            None,
        )
        .unwrap();
        let batches = builder
            .with_batch_size(2)
            .build_async()
            .try_collect::<Vec<_>>()
            .await
            .unwrap()
            .into_iter()
            .map(|batch| adapt(&bridge, batch))
            .collect::<Vec<_>>();
        let output = concat_batches(bridge.schema(), &batches).unwrap();
        assert_eq!(output, read(file, "Asia/Kathmandu", 2));
    }
}
