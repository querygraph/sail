use datafusion::arrow::datatypes::{Field, Schema};

use super::*;
use crate::error::SparkError;
use crate::pipelines::graph::{Output, PipelineGraph};
use crate::pipelines::schema::VirtualTableSchema;
use crate::pipelines::sql;
use crate::spark::connect::pipeline_command as pc;

type TestResult<T = ()> = Result<T, Box<dyn std::error::Error>>;

const LEGACY: EncoderSchemaSettings = EncoderSchemaSettings {
    types_framework_enabled: false,
    preserve_char_varchar_type_info: false,
};
const FRAMEWORK: EncoderSchemaSettings = EncoderSchemaSettings {
    types_framework_enabled: true,
    preserve_char_varchar_type_info: false,
};
const PRESERVED: EncoderSchemaSettings = EncoderSchemaSettings {
    types_framework_enabled: false,
    preserve_char_varchar_type_info: true,
};

async fn output(schema: &str) -> TestResult<Output> {
    let mut graph = PipelineGraph::new("test_catalog".into(), "test_db".into(), HashMap::new());
    sql::prepare(
        pc::DefineSqlGraphElements {
            sql_text: Some(format!("CREATE STREAMING TABLE target {schema}")),
            ..Default::default()
        },
        |_| async { Err(SparkError::internal("fixture has no catalog expression")) },
    )
    .await?
    .commit(&mut graph)?;
    graph
        .definition
        .outputs
        .pop()
        .ok_or_else(|| "fixture output missing".into())
}

fn time(precision: i32) -> spec::DataType {
    spec::DataType::Time { precision }
}

fn year() -> spec::DataType {
    spec::DataType::Interval {
        interval_unit: spec::IntervalUnit::YearMonth,
        start_field: Some(spec::IntervalFieldType::Year),
        end_field: Some(spec::IntervalFieldType::Year),
    }
}

fn year_to_month() -> spec::DataType {
    spec::DataType::Interval {
        interval_unit: spec::IntervalUnit::YearMonth,
        start_field: Some(spec::IntervalFieldType::Year),
        end_field: Some(spec::IntervalFieldType::Month),
    }
}

fn hour() -> spec::DataType {
    spec::DataType::Interval {
        interval_unit: spec::IntervalUnit::DayTime,
        start_field: Some(spec::IntervalFieldType::Hour),
        end_field: Some(spec::IntervalFieldType::Hour),
    }
}

fn day_to_second() -> spec::DataType {
    spec::DataType::Interval {
        interval_unit: spec::IntervalUnit::DayTime,
        start_field: Some(spec::IntervalFieldType::Day),
        end_field: Some(spec::IntervalFieldType::Second),
    }
}

fn field(name: &str, data_type: spec::DataType, nullable: bool) -> spec::Field {
    spec::Field {
        name: name.into(),
        data_type,
        nullable,
        metadata: vec![(
            spec::SPARK_METADATA_JSON_KEY.into(),
            r#"{"comment":"c"}"#.into(),
        )],
    }
}

fn list(data_type: spec::DataType) -> spec::DataType {
    spec::DataType::List {
        data_type: Box::new(data_type),
        nullable: false,
    }
}

fn map(key: spec::DataType, value: spec::DataType) -> spec::DataType {
    spec::DataType::Map {
        key_type: Box::new(key),
        value_type: Box::new(value),
        value_type_nullable: false,
        keys_sorted: false,
    }
}

fn structure(fields: Vec<spec::Field>) -> spec::DataType {
    spec::DataType::Struct {
        fields: fields.into_iter().map(Arc::new).collect(),
    }
}

fn project(
    data_type: spec::DataType,
    settings: EncoderSchemaSettings,
) -> SparkResult<spec::DataType> {
    streaming_read_type(&data_type, settings)
}

#[test]
fn time_widens_only_without_the_types_framework() -> TestResult {
    assert_eq!(project(time(3), LEGACY)?, time(6));
    assert_eq!(project(time(3), FRAMEWORK)?, time(3));
    assert_eq!(project(time(6), LEGACY)?, time(6));
    Ok(())
}

#[test]
fn intervals_widen_in_both_framework_modes() -> TestResult {
    for settings in [LEGACY, FRAMEWORK] {
        assert_eq!(project(year(), settings)?, year_to_month());
        assert_eq!(project(hour(), settings)?, day_to_second());
    }
    let calendar = spec::DataType::Interval {
        interval_unit: spec::IntervalUnit::MonthDayNano,
        start_field: None,
        end_field: None,
    };
    assert_eq!(project(calendar.clone(), LEGACY)?, calendar);
    Ok(())
}

#[test]
fn projection_recurses_through_arrays_structs_and_both_map_sides() -> TestResult {
    let nested = list(map(
        time(3),
        structure(vec![field("t", time(3), false), field("i", year(), true)]),
    ));
    let expected = list(map(
        time(6),
        structure(vec![
            spec::Field {
                metadata: vec![],
                ..field("t", time(6), false)
            },
            spec::Field {
                metadata: vec![],
                ..field("i", year_to_month(), true)
            },
        ]),
    ));
    assert_eq!(project(nested, LEGACY)?, expected);
    Ok(())
}

#[test]
fn metadata_is_dropped_while_names_and_nullability_remain() -> TestResult {
    let fields: spec::Fields = vec![
        Arc::new(field("Required", spec::DataType::Int32, false)),
        Arc::new(field(
            "s",
            structure(vec![field("Inner", spec::DataType::Int32, false)]),
            true,
        )),
    ]
    .into_iter()
    .collect();
    let projected = streaming_read_fields(&fields, FRAMEWORK)?;
    let [required, nested] = projected.iter().collect::<Vec<_>>()[..] else {
        return Err("expected two fields".into());
    };
    assert_eq!(
        (required.name.as_str(), required.nullable),
        ("Required", false)
    );
    assert!(required.metadata.is_empty());
    assert!(nested.metadata.is_empty());
    let spec::DataType::Struct { fields } = &nested.data_type else {
        return Err("expected a struct".into());
    };
    assert_eq!(
        (fields[0].name.as_str(), fields[0].nullable),
        ("Inner", false)
    );
    assert!(fields[0].metadata.is_empty());
    Ok(())
}

#[test]
fn metadata_other_than_spark_column_metadata_is_retained() -> TestResult {
    let internal = ("sail::internal".to_string(), "kept".to_string());
    let fields: spec::Fields = vec![Arc::new(spec::Field {
        name: "id".into(),
        data_type: spec::DataType::Int32,
        nullable: true,
        metadata: vec![
            internal.clone(),
            (spec::SPARK_METADATA_JSON_KEY.into(), "{}".into()),
        ],
    })]
    .into_iter()
    .collect();
    let projected = streaming_read_fields(&fields, LEGACY)?;
    assert_eq!(projected[0].metadata, vec![internal]);
    Ok(())
}

#[test]
fn nullable_list_and_map_values_are_unchanged() -> TestResult {
    let data_type = spec::DataType::Map {
        key_type: Box::new(spec::DataType::Int32),
        value_type: Box::new(spec::DataType::List {
            data_type: Box::new(spec::DataType::Int32),
            nullable: true,
        }),
        value_type_nullable: true,
        keys_sorted: false,
    };
    assert_eq!(project(data_type.clone(), LEGACY)?, data_type);
    Ok(())
}

#[test]
fn declared_char_and_varchar_have_no_encoder_unless_preserved() -> TestResult {
    for (utf8_type, name) in [
        (spec::Utf8Type::Char { length: 3 }, "CHAR(3)"),
        (spec::Utf8Type::VarChar { length: 3 }, "VARCHAR(3)"),
    ] {
        let data_type = spec::DataType::ConfiguredUtf8 { utf8_type };
        assert_eq!(project(data_type.clone(), PRESERVED)?, data_type);
        // The nested type is reported, as Spark's encoder recursion does.
        let Err(SparkError::Structured(error)) = project(list(data_type), LEGACY) else {
            return Err("expected a structured encoder error".into());
        };
        assert_eq!(error.class, "org.apache.spark.sql.AnalysisException");
        assert_eq!(error.condition, "UNSUPPORTED_DATA_TYPE_FOR_ENCODER");
        assert_eq!(error.sql_state, "0A000");
        let quoted = format!("\"{name}\"");
        assert_eq!(error.parameters["dataType"], quoted);
        assert_eq!(
            error.message,
            format!(
                "Cannot create encoder for {quoted}. Please use a different output data type for your UDF or DataFrame."
            )
        );
    }
    Ok(())
}

#[tokio::test]
async fn declared_streaming_load_projects_the_declaration() -> TestResult {
    let ctx = sql::tests::resolution_context()?;
    let output = output("(id INT NOT NULL COMMENT 'declared', t INTERVAL YEAR)").await?;
    let descriptor = VirtualTableSchema::new(&output, vec![]);
    let batch = descriptor.load(&ctx)?;
    let stream = descriptor.load_streaming(&ctx, LEGACY)?;
    assert!(
        batch
            .field(0)
            .metadata()
            .contains_key(spec::SPARK_METADATA_JSON_KEY)
    );
    assert!(
        !stream
            .field(0)
            .metadata()
            .contains_key(spec::SPARK_METADATA_JSON_KEY)
    );
    assert!(!stream.field(0).is_nullable());
    assert_ne!(batch.field(1).metadata(), stream.field(1).metadata());
    Ok(())
}

#[tokio::test]
async fn inferred_streaming_load_projects_the_merged_input() -> TestResult {
    let ctx = sql::tests::resolution_context()?;
    let output = output("").await?;
    let incoming = Field::new(
        "t",
        datafusion::arrow::datatypes::DataType::Time64(
            datafusion::arrow::datatypes::TimeUnit::Nanosecond,
        ),
        true,
    )
    .with_metadata(HashMap::from([(
        spec::SAIL_SPARK_TIME_METADATA_KEY.to_string(),
        spec::SparkTimeMetadata::try_new(3)?.to_json()?,
    )]));
    let descriptor = VirtualTableSchema::new(&output, vec![Arc::new(Schema::new(vec![incoming]))]);
    let batch = descriptor.load(&ctx)?;
    assert_eq!(descriptor.load_streaming(&ctx, FRAMEWORK)?, batch);
    let legacy = descriptor.load_streaming(&ctx, LEGACY)?;
    assert_ne!(legacy, batch);
    Ok(())
}
