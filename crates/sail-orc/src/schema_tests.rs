use bytes::Bytes;
use orc_rust::proto;
use orc_rust::reader::metadata::{MetadataReadOptions, read_metadata_with_options};
use prost013::Message;

use super::*;

fn primitive(kind: Kind) -> proto::Type {
    proto::Type {
        kind: Some(kind as i32),
        maximum_length: matches!(kind, Kind::Char | Kind::Varchar).then_some(5),
        ..Default::default()
    }
}

fn annotated(kind: Kind, value: &str) -> proto::Type {
    proto::Type {
        attributes: vec![proto::StringPair {
            key: Some(CATALYST_TYPE.to_string()),
            value: Some(value.to_string()),
        }],
        ..primitive(kind)
    }
}

fn container(kind: Kind, children: &[u32]) -> proto::Type {
    proto::Type {
        subtypes: children.to_vec(),
        field_names: if kind == Kind::Struct {
            children.iter().map(|id| format!("column_{id}")).collect()
        } else {
            vec![]
        },
        ..primitive(kind)
    }
}

fn metadata(types: Vec<proto::Type>) -> FileMetadata {
    let footer = proto::Footer {
        types,
        metadata: vec![
            proto::UserMetadataItem {
                name: Some(SPARK_VERSION.to_string()),
                value: Some(b"4.2.1".to_vec()),
            },
            proto::UserMetadataItem {
                name: Some("foreign.binary.metadata".to_string()),
                value: Some(vec![255]),
            },
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
    read_metadata_with_options(
        &mut Bytes::from(bytes),
        MetadataReadOptions {
            max_encoded_metadata_bytes: Some(64 * 1024),
            max_decoded_metadata_bytes: Some(64 * 1024),
            max_schema_depth: Some(16),
        },
    )
    .unwrap()
}

fn flat_metadata(children: Vec<proto::Type>) -> FileMetadata {
    let mut types = vec![container(
        Kind::Struct,
        &(1..=children.len() as u32).collect::<Vec<_>>(),
    )];
    types.extend(children);
    metadata(types)
}

#[test]
fn primitive_types_and_characters_follow_spark_schema() {
    let cases = [
        (Kind::Boolean, DataType::Boolean),
        (Kind::Byte, DataType::Int8),
        (Kind::Short, DataType::Int16),
        (Kind::Int, DataType::Int32),
        (Kind::Long, DataType::Int64),
        (Kind::Float, DataType::Float32),
        (Kind::Double, DataType::Float64),
        (Kind::String, DataType::Utf8),
        (Kind::Char, DataType::Utf8),
        (Kind::Varchar, DataType::Utf8),
        (Kind::Binary, DataType::Binary),
        (Kind::Date, DataType::Date32),
    ];
    let metadata = flat_metadata(
        cases
            .iter()
            .map(|(kind, _)| {
                if *kind == Kind::String {
                    annotated(*kind, "string")
                } else {
                    primitive(*kind)
                }
            })
            .collect(),
    );
    let schema = infer_schema(&metadata, "UTC").unwrap();
    for (field, (_, expected)) in schema.fields().iter().zip(cases) {
        assert_eq!(field.data_type(), &expected);
        assert!(field.is_nullable());
    }
    assert_eq!(schema.metadata().len(), 1);
    assert_eq!(schema.metadata()[SPARK_VERSION], "4.2.1");
}

#[test]
fn timestamps_are_microseconds_with_explicit_ltz_and_ntz_semantics() {
    let metadata = flat_metadata(vec![
        primitive(Kind::Timestamp),
        annotated(Kind::Timestamp, "timestamp"),
        primitive(Kind::TimestampInstant),
        annotated(Kind::Long, "timestamp_ntz"),
    ]);
    let schema = infer_schema(&metadata, "America/Los_Angeles").unwrap();
    for field in &schema.fields()[..3] {
        assert_eq!(
            field.data_type(),
            &DataType::Timestamp(TimeUnit::Microsecond, Some("America/Los_Angeles".into()))
        );
    }
    assert_eq!(
        schema.field(3).data_type(),
        &DataType::Timestamp(TimeUnit::Microsecond, None)
    );
}

#[test]
fn time_preserves_nanoseconds_and_declared_precision() {
    let metadata = flat_metadata(
        (0..=6)
            .map(|precision| annotated(Kind::Long, &format!("time({precision})")))
            .collect(),
    );
    let schema = infer_schema(&metadata, "UTC").unwrap();
    for (precision, field) in schema.fields().iter().enumerate() {
        assert_eq!(field.data_type(), &DataType::Time64(TimeUnit::Nanosecond));
        assert_eq!(
            field.metadata()[spec::SAIL_SPARK_TIME_METADATA_KEY],
            spec::SparkTimeMetadata::try_new(precision as i32)
                .unwrap()
                .to_json()
                .unwrap()
        );
    }
    let default_field = schema.field(6).clone().with_name("column_1");
    for annotation in ["time", "TIME ( 6 )", "time /* precision */ (6)"] {
        let schema = infer_schema(
            &flat_metadata(vec![annotated(Kind::Long, annotation)]),
            "UTC",
        )
        .unwrap();
        assert_eq!(schema.field(0), &default_field);
    }
    for precision in [-1i64, 7, 9, i64::from(i32::MAX) + 1] {
        let metadata = flat_metadata(vec![annotated(Kind::Long, &format!("time({precision})"))]);
        assert!(infer_schema(&metadata, "UTC").is_err(), "TIME({precision})");
    }
}

#[test]
fn time_annotations_require_long_storage_and_valid_sql_syntax() {
    for kind in [Kind::Int, Kind::String, Kind::Timestamp] {
        let metadata = flat_metadata(vec![annotated(kind, "time(0)")]);
        let error = infer_schema(&metadata, "UTC").unwrap_err().to_string();
        assert!(
            error.contains("TIME annotation requires LONG storage"),
            "{error}"
        );
    }
    for annotation in ["time(1, 2)", "time(1) garbage", "time(1.5)", "time()"] {
        let metadata = flat_metadata(vec![annotated(Kind::Long, annotation)]);
        assert!(infer_schema(&metadata, "UTC").is_err(), "{annotation}");
    }
}

#[test]
fn intervals_retain_sail_field_qualifiers() {
    for (kind, value, expected, start, end) in [
        (
            Kind::Int,
            "interval year to month",
            DataType::Interval(IntervalUnit::YearMonth),
            spec::IntervalFieldType::Year,
            spec::IntervalFieldType::Month,
        ),
        (
            Kind::Int,
            "interval month",
            DataType::Interval(IntervalUnit::YearMonth),
            spec::IntervalFieldType::Month,
            spec::IntervalFieldType::Month,
        ),
        (
            Kind::Long,
            "interval hour to second",
            DataType::Duration(TimeUnit::Microsecond),
            spec::IntervalFieldType::Hour,
            spec::IntervalFieldType::Second,
        ),
        (
            Kind::Long,
            "interval minute",
            DataType::Duration(TimeUnit::Microsecond),
            spec::IntervalFieldType::Minute,
            spec::IntervalFieldType::Minute,
        ),
    ] {
        let metadata = flat_metadata(vec![annotated(kind, value)]);
        let schema = infer_schema(&metadata, "UTC").unwrap();
        let field = schema.field(0);
        assert_eq!(field.data_type(), &expected);
        let interval = spec::SparkIntervalMetadata::from_json(
            &field.metadata()[spec::SAIL_SPARK_INTERVAL_METADATA_KEY],
        )
        .unwrap();
        assert_eq!(interval.start_field(), start);
        assert_eq!(interval.end_field(), end);
    }
}

#[test]
fn nested_fields_ignore_container_annotations_and_preserve_logical_metadata() {
    let mut types = vec![
        container(Kind::Struct, &[1, 3]),
        container(Kind::List, &[2]),
        annotated(Kind::Long, "interval day to second"),
        container(Kind::Map, &[4, 5]),
        primitive(Kind::String),
        container(Kind::Struct, &[6, 7]),
        annotated(Kind::Long, "timestamp_ntz"),
        annotated(Kind::Long, "time(1)"),
    ];
    // Spark ignores all container attributes, including root duplicates and
    // missing values, rather than parsing or validating them as leaf annotations.
    for id in [0, 1, 3, 5] {
        types[id].attributes = vec![
            proto::StringPair {
                key: Some(CATALYST_TYPE.to_string()),
                value: None,
            },
            proto::StringPair {
                key: Some(CATALYST_TYPE.to_string()),
                value: Some("not_a_type".to_string()),
            },
        ];
    }
    let schema = infer_schema(&metadata(types), "UTC").unwrap();
    let DataType::List(item) = schema.field(0).data_type() else {
        panic!("expected LIST");
    };
    assert_eq!(item.name(), "item");
    assert!(item.is_nullable());
    assert!(
        item.metadata()
            .contains_key(spec::SAIL_SPARK_INTERVAL_METADATA_KEY)
    );
    let DataType::Map(entries, sorted) = schema.field(1).data_type() else {
        panic!("expected MAP");
    };
    assert!(!sorted);
    assert_eq!(entries.name(), "entries");
    assert!(!entries.is_nullable());
    let DataType::Struct(pair) = entries.data_type() else {
        panic!("expected map entries STRUCT");
    };
    assert_eq!(pair[0].name(), spec::SAIL_MAP_KEY_FIELD_NAME);
    assert!(!pair[0].is_nullable());
    assert_eq!(pair[1].name(), spec::SAIL_MAP_VALUE_FIELD_NAME);
    assert!(pair[1].is_nullable());
    let DataType::Struct(nested) = pair[1].data_type() else {
        panic!("expected nested STRUCT");
    };
    assert_eq!(nested[0].name(), "column_6");
    assert!(nested[0].is_nullable());
    assert_eq!(
        nested[0].data_type(),
        &DataType::Timestamp(TimeUnit::Microsecond, None)
    );
    assert_eq!(
        nested[1].data_type(),
        &DataType::Time64(TimeUnit::Nanosecond)
    );
    assert_eq!(
        nested[1].metadata()[spec::SAIL_SPARK_TIME_METADATA_KEY],
        spec::SparkTimeMetadata::try_new(1)
            .unwrap()
            .to_json()
            .unwrap()
    );
}

#[test]
fn invalid_annotations_fail_and_unrelated_attributes_are_ignored() {
    for (kind, annotation) in [
        (Kind::Long, "not_a_type"),
        (Kind::Timestamp, "timestamp_ntz"),
        (Kind::Long, "timestamp"),
        (Kind::Int, "interval day to second"),
        (Kind::Long, "interval year to month"),
        (Kind::String, "int"),
        (Kind::Decimal, "decimal(5,2)"),
        (Kind::Long, "array<int>"),
    ] {
        let mut ty = annotated(kind, annotation);
        if kind == Kind::Decimal {
            ty.precision = Some(10);
            ty.scale = Some(2);
        }
        let metadata = flat_metadata(vec![ty]);
        assert!(
            infer_schema(&metadata, "UTC").is_err(),
            "{kind:?}/{annotation}"
        );
    }
    let mut duplicate = annotated(Kind::Long, "timestamp_ntz");
    duplicate.attributes.push(duplicate.attributes[0].clone());
    let mut missing = annotated(Kind::String, "string");
    missing.attributes[0].value = None;
    for ty in [duplicate, missing] {
        assert!(infer_schema(&flat_metadata(vec![ty]), "UTC").is_err());
    }
    let mut foreign = primitive(Kind::Long);
    foreign.attributes.push(proto::StringPair {
        key: Some("foreign.logical.type".to_string()),
        value: Some("opaque".to_string()),
    });
    let schema = infer_schema(&flat_metadata(vec![foreign]), "UTC").unwrap();
    assert_eq!(schema.field(0).data_type(), &DataType::Int64);
    assert!(schema.field(0).metadata().is_empty());
}

#[test]
fn decimal_limits_are_checked_before_native_arrow_conversion() {
    for (precision, scale) in [(1, 0), (18, 4), (38, 38)] {
        let ty = proto::Type {
            precision: Some(precision),
            scale: Some(scale),
            ..primitive(Kind::Decimal)
        };
        let schema = infer_schema(&flat_metadata(vec![ty]), "UTC").unwrap();
        assert_eq!(
            schema.field(0).data_type(),
            &DataType::Decimal128(precision as u8, scale as i8)
        );
    }
    for (precision, scale) in [
        (Some(0), Some(0)),
        (Some(39), Some(0)),
        (Some(256), Some(0)),
        (Some(10), Some(11)),
    ] {
        let ty = proto::Type {
            precision,
            scale,
            ..primitive(Kind::Decimal)
        };
        assert!(matches!(
            infer_schema(&flat_metadata(vec![ty]), "UTC"),
            Err(DataFusionError::Plan(_))
        ));
    }
    // Missing legacy protobuf fields are a compatibility boundary, not a
    // malformed decimal: the native reader has no Java legacy-default policy.
    for (precision, scale) in [(None, Some(0)), (Some(10), None), (None, None)] {
        let ty = proto::Type {
            precision,
            scale,
            ..primitive(Kind::Decimal)
        };
        assert!(matches!(
            infer_schema(&flat_metadata(vec![ty]), "UTC"),
            Err(DataFusionError::NotImplemented(_))
        ));
    }
}

#[test]
fn character_metadata_preserves_full_top_level_catalog_types() {
    let odd_name = "a`b\"c\\d";
    let metadata = metadata(vec![
        container(Kind::Struct, &[1, 2, 3]),
        primitive(Kind::Char),
        proto::Type {
            maximum_length: Some(10),
            ..primitive(Kind::Varchar)
        },
        container(Kind::List, &[4]),
        container(Kind::Map, &[5, 6]),
        primitive(Kind::String),
        proto::Type {
            field_names: [odd_name, "t", "ym", "dt", "ntz", "dec"]
                .map(str::to_string)
                .to_vec(),
            ..container(Kind::Struct, &[7, 8, 9, 10, 11, 12])
        },
        proto::Type {
            maximum_length: Some(4),
            ..primitive(Kind::Char)
        },
        annotated(Kind::Long, "time(1)"),
        annotated(Kind::Int, "interval month"),
        annotated(Kind::Long, "interval hour to second"),
        annotated(Kind::Long, "timestamp_ntz"),
        proto::Type {
            precision: Some(18),
            scale: Some(4),
            ..primitive(Kind::Decimal)
        },
    ]);
    let schema = infer_schema(&metadata, "UTC").unwrap();
    let expected = [
        "char(5)".to_string(),
        "varchar(10)".to_string(),
        format!(
            "array<map<string,struct<{odd_name}:char(4),t:time(1),ym:interval month,dt:interval hour to second,ntz:timestamp_ntz,dec:decimal(18,4)>>>"
        ),
    ];
    for (field, expected) in schema.fields().iter().zip(expected) {
        let metadata: serde_json::Value =
            serde_json::from_str(&field.metadata()[spec::SPARK_METADATA_JSON_KEY]).unwrap();
        assert_eq!(
            metadata,
            serde_json::json!({ (CHAR_VARCHAR_TYPE): expected })
        );
    }
    assert_eq!(schema.field(0).data_type(), &DataType::Utf8);
    assert_eq!(schema.field(1).data_type(), &DataType::Utf8);
    let DataType::List(item) = schema.field(2).data_type() else {
        panic!("expected LIST")
    };
    assert!(!item.metadata().contains_key(spec::SPARK_METADATA_JSON_KEY));
    let DataType::Map(entries, _) = item.data_type() else {
        panic!("expected MAP")
    };
    let DataType::Struct(pair) = entries.data_type() else {
        panic!("expected map STRUCT")
    };
    let DataType::Struct(fields) = pair[1].data_type() else {
        panic!("expected nested STRUCT")
    };
    assert_eq!(fields[0].name(), odd_name);
    assert_eq!(fields[0].data_type(), &DataType::Utf8);
    assert!(
        !fields[0]
            .metadata()
            .contains_key(spec::SPARK_METADATA_JSON_KEY)
    );
}

#[test]
fn character_annotations_and_length_validation_are_explicit() {
    let schema = infer_schema(
        &flat_metadata(vec![annotated(Kind::String, "CHAR (3)")]),
        "UTC",
    )
    .unwrap();
    let metadata: serde_json::Value =
        serde_json::from_str(&schema.field(0).metadata()[spec::SPARK_METADATA_JSON_KEY]).unwrap();
    assert_eq!(metadata[CHAR_VARCHAR_TYPE], "char(3)");
    for kind in [Kind::Char, Kind::Varchar] {
        let mut ty = primitive(kind);
        ty.maximum_length = Some(0);
        assert!(infer_schema(&flat_metadata(vec![ty.clone()]), "UTC").is_ok());
        ty.maximum_length = None;
        assert!(matches!(
            infer_schema(&flat_metadata(vec![ty.clone()]), "UTC"),
            Err(DataFusionError::NotImplemented(_))
        ));
        ty.maximum_length = Some(u32::MAX);
        assert!(matches!(
            infer_schema(&flat_metadata(vec![ty]), "UTC"),
            Err(DataFusionError::Plan(_))
        ));
    }
}

#[test]
fn rejects_union_instead_of_exposing_a_non_spark_type() {
    let metadata = metadata(vec![
        container(Kind::Struct, &[1]),
        container(Kind::Union, &[2]),
        primitive(Kind::Int),
    ]);
    assert!(matches!(
        infer_schema(&metadata, "UTC"),
        Err(DataFusionError::NotImplemented(message)) if message.contains("UNION")
    ));
}
