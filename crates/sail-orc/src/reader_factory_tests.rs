#![expect(clippy::unwrap_used)]

use std::io::Cursor;

use arrow::array::{Int64Array, TimestampMicrosecondArray, TimestampNanosecondArray};
use arrow::datatypes::Schema;
use bytes::Bytes;
use futures::TryStreamExt;
use orc_rust::ArrowWriterBuilder;
use orc_rust::proto::r#type::Kind;
use orc_rust::proto::{self};
use orc_rust::reader::metadata::{MetadataReadOptions, read_metadata_with_options};
use prost013::Message;

use super::*;

fn metadata(file: &Bytes) -> Arc<FileMetadata> {
    Arc::new(
        read_metadata_with_options(
            &mut file.clone(),
            MetadataReadOptions {
                max_encoded_metadata_bytes: Some(64 * 1024),
                max_decoded_metadata_bytes: Some(64 * 1024),
                max_schema_depth: Some(16),
            },
        )
        .unwrap(),
    )
}

fn fixture() -> (Bytes, Arc<FileMetadata>) {
    // The native writer stores ordinary TIMESTAMP with writer timezone UTC.
    // Both signs carry submicroseconds, so a strict reader would reject them.
    let values = vec![Some(1_577_934_245_123_456_789), Some(-1_001_001_001), None];
    let local = TimestampNanosecondArray::from(values.clone());
    let instant = TimestampNanosecondArray::from(values).with_timezone("UTC");
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, true),
        Field::new("local", local.data_type().clone(), true),
        Field::new("instant", instant.data_type().clone(), true),
    ]));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int64Array::from(vec![0, 1, 2])),
            Arc::new(local),
            Arc::new(instant),
        ],
    )
    .unwrap();
    let mut file = Vec::new();
    let mut writer = ArrowWriterBuilder::new(&mut file, schema)
        .try_build()
        .unwrap();
    writer.write(&batch).unwrap();
    writer.close().unwrap();
    let file = Bytes::from(file);
    let metadata = metadata(&file);
    (file, metadata)
}

fn adapted_values(batches: Vec<RecordBatch>, bridge: &OrcSchemaBridge) -> Vec<Vec<Option<i64>>> {
    let mut columns = vec![Vec::new(); 2];
    for batch in batches {
        let output = bridge.adapt_batch(batch).unwrap();
        assert_eq!(output.schema(), bridge.schema().clone());
        for (column, values) in columns.iter_mut().enumerate() {
            values.extend(
                output
                    .column(column + 1)
                    .as_any()
                    .downcast_ref::<TimestampMicrosecondArray>()
                    .unwrap()
                    .iter(),
            );
        }
    }
    columns
}

#[test]
fn configured_reader_converts_ltz_values_and_truncates_before_display_relabeling() {
    let (file, metadata) = fixture();
    let expected = vec![Some(1_577_934_245_123_456), Some(-1_001_002), None];
    for (reader_name, adjustment) in [("UTC", 0), ("GMT+02:00", -7_200_000_000)] {
        let config = OrcJavaTimeZoneConfig::for_reader_timezone(reader_name.to_string()).unwrap();
        for display in ["UTC", "Asia/Kathmandu"] {
            let (builder, bridge) = OrcSchemaBridge::try_new_reader(
                file.clone(),
                metadata.clone(),
                display,
                &config,
                None,
            )
            .unwrap();
            assert_eq!(builder.schema(), bridge.physical_schema().clone());
            for field in &bridge.physical_schema().fields()[1..] {
                assert_eq!(
                    field.data_type(),
                    &DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into()))
                );
            }
            for field in &bridge.schema().fields()[1..] {
                assert_eq!(
                    field.data_type(),
                    &DataType::Timestamp(TimeUnit::Microsecond, Some(display.into()))
                );
            }
            let batches = builder
                .with_batch_size(1)
                .build()
                .collect::<std::result::Result<Vec<_>, _>>()
                .unwrap();
            let columns = adapted_values(batches, &bridge);
            let local = expected
                .iter()
                .map(|value| value.map(|value| value + adjustment))
                .collect::<Vec<_>>();
            assert_eq!(columns[0], local);
            assert_eq!(columns[1], expected); // TIMESTAMP_INSTANT never shifts.
        }
    }
}

#[tokio::test]
async fn configured_async_reader_uses_the_same_numeric_policy() {
    let (file, metadata) = fixture();
    let config = OrcJavaTimeZoneConfig::for_reader_timezone("GMT+02:00".to_string()).unwrap();
    let (builder, bridge) = OrcSchemaBridge::try_new_reader(
        Cursor::new(file),
        metadata,
        "Asia/Kathmandu",
        &config,
        None,
    )
    .unwrap();
    let batches = builder
        .with_batch_size(2)
        .build_async()
        .try_collect::<Vec<_>>()
        .await
        .unwrap();
    assert_eq!(
        adapted_values(batches, &bridge),
        vec![
            vec![Some(1_577_927_045_123_456), Some(-7_201_001_002), None],
            vec![Some(1_577_934_245_123_456), Some(-1_001_002), None],
        ]
    );
}

#[test]
fn reader_factory_reuses_metadata_and_keeps_native_projection_matched() {
    let (file, metadata) = fixture();
    let config = OrcJavaTimeZoneConfig::for_reader_timezone("GMT+02:00".to_string()).unwrap();
    // No ChunkReader implementation: constructing the pair cannot reread metadata.
    let (builder, _) =
        OrcSchemaBridge::try_new_reader((), metadata.clone(), "UTC", &config, None).unwrap();
    assert!(std::ptr::eq(builder.file_metadata(), metadata.as_ref()));
    for indices in [&[1, 2][..], &[2][..], &[][..]] {
        let (builder, bridge) = OrcSchemaBridge::try_new_reader(
            file.clone(),
            metadata.clone(),
            "UTC",
            &config,
            Some(indices),
        )
        .unwrap();
        assert_eq!(builder.schema(), bridge.physical_schema().clone());
        let batches = builder
            .with_batch_size(2)
            .build()
            .collect::<std::result::Result<Vec<_>, _>>()
            .unwrap();
        let mut rows = 0;
        let mut numeric = vec![Vec::new(); indices.len()];
        for batch in batches {
            let output = bridge.adapt_batch(batch).unwrap();
            rows += output.num_rows();
            assert_eq!(output.num_columns(), indices.len());
            assert_eq!(output.schema(), bridge.schema().clone());
            for (column, values) in numeric.iter_mut().enumerate() {
                values.extend(
                    output
                        .column(column)
                        .as_any()
                        .downcast_ref::<TimestampMicrosecondArray>()
                        .unwrap()
                        .iter(),
                );
            }
        }
        assert_eq!(rows, 3);
        for (values, index) in numeric.iter().zip(indices) {
            let expected = if *index == 1 {
                vec![Some(1_577_927_045_123_456), Some(-7_201_001_002), None]
            } else {
                vec![Some(1_577_934_245_123_456), Some(-1_001_002), None]
            };
            assert_eq!(values, &expected);
        }
    }
    for (indices, message) in [
        (&[3][..], "out of bounds"),
        (&[1, 1][..], "strictly increasing"),
        (&[2, 1][..], "strictly increasing"),
    ] {
        let error =
            OrcSchemaBridge::try_new_reader((), metadata.clone(), "UTC", &config, Some(indices))
                .err()
                .unwrap();
        assert!(error.to_string().contains(message), "{error}");
    }
}

#[test]
fn unconfigured_constructor_still_rejects_naive_ltz_batches() {
    let (file, metadata) = fixture();
    let bridge = OrcSchemaBridge::try_new(&metadata, "UTC").unwrap();
    let batch = ArrowReaderBuilder::new_with_metadata(file, metadata)
        .with_timestamp_precision(TimestampPrecision::Microsecond)
        .with_timestamp_read_options(
            TimestampReadOptions::new().with_precision_loss(TimestampPrecisionLoss::Truncate),
        )
        .build()
        .next()
        .unwrap()
        .unwrap();
    let error = bridge.adapt_batch(batch).unwrap_err().to_string();
    assert!(
        error.contains("explicit native reader timezone decoding"),
        "{error}"
    );
}

fn footer_metadata(mut last: proto::Type) -> Arc<FileMetadata> {
    last.kind.get_or_insert(Kind::Timestamp as i32);
    let footer = proto::Footer {
        types: vec![
            proto::Type {
                kind: Some(Kind::Struct as i32),
                subtypes: vec![1, 4],
                field_names: vec!["nested".to_string(), "ts".to_string()],
                ..Default::default()
            },
            proto::Type {
                kind: Some(Kind::Struct as i32),
                subtypes: vec![2, 3],
                field_names: vec!["a".to_string(), "b".to_string()],
                ..Default::default()
            },
            proto::Type {
                kind: Some(Kind::Long as i32),
                ..Default::default()
            },
            proto::Type {
                kind: Some(Kind::String as i32),
                ..Default::default()
            },
            last,
        ],
        ..Default::default()
    }
    .encode_to_vec();
    let postscript = proto::PostScript {
        footer_length: Some(footer.len() as u64),
        metadata_length: Some(0),
        compression: Some(proto::CompressionKind::None as i32),
        magic: Some("ORC".to_string()),
        ..Default::default()
    }
    .encode_to_vec();
    let mut bytes = b"ORC".to_vec();
    bytes.extend(footer);
    bytes.extend(&postscript);
    bytes.push(postscript.len().try_into().unwrap());
    metadata(&Bytes::from(bytes))
}

#[test]
fn projection_uses_native_type_ids_and_validates_unprojected_logical_types() {
    let config = OrcJavaTimeZoneConfig::for_reader_timezone("UTC".to_string()).unwrap();
    let (builder, bridge) = OrcSchemaBridge::try_new_reader(
        (),
        footer_metadata(proto::Type::default()),
        "UTC",
        &config,
        Some(&[1]),
    )
    .unwrap();
    assert_eq!(bridge.schema().fields().len(), 1);
    assert_eq!(bridge.schema().field(0).name(), "ts");
    assert_eq!(builder.schema(), bridge.physical_schema().clone());
    assert_eq!(builder.schema().field(0).name(), "ts");
    // Decimal precision is checked before native Arrow conversion, even when
    // the malformed logical column would not be physically projected.
    let malformed = footer_metadata(proto::Type {
        kind: Some(Kind::Decimal as i32),
        precision: Some(256),
        scale: Some(0),
        ..Default::default()
    });
    let error = OrcSchemaBridge::try_new_reader((), malformed, "UTC", &config, Some(&[0]))
        .err()
        .unwrap();
    assert!(
        error.to_string().contains("invalid decimal precision 256"),
        "{error}"
    );
}
