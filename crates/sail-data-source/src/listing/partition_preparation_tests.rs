#![expect(clippy::unwrap_used)]

use std::collections::HashMap;

use bytes::Bytes;
use datafusion::execution::context::{SessionConfig, SessionContext};
use datafusion_common::Constraints;
use object_store::ObjectStoreExt;
use object_store::memory::InMemory;
use object_store::path::Path;
use sail_common_datafusion::datasource::{DataSource, SourceInfo};
use url::Url;

use super::*;
use crate::formats::json::JsonDataSource;
use crate::listing::table::ListingTableSource;

fn names(names: &[&str]) -> Vec<String> {
    names.iter().map(|name| (*name).into()).collect()
}

fn supplied_schema() -> Schema {
    Schema::new_with_metadata(
        vec![
            Field::new("PART", DataType::Int32, false).with_metadata(HashMap::from([(
                "partition-note".into(),
                "retained".into(),
            )])),
            Field::new("second", DataType::Utf8, false)
                .with_metadata(HashMap::from([("field-note".into(), "retained".into())])),
            Field::new("first", DataType::Boolean, true),
        ],
        HashMap::from([("schema-note".into(), "retained".into())]),
    )
}

#[test]
fn automatic_partitions_keep_omitted_fields_and_obey_the_case_policy() -> Result<()> {
    let supplied = supplied_schema();
    let discovered = names(&["part", "missing"]);
    for case_sensitive in [false, true] {
        let (file, fields) =
            prepare_explicit_partitions(&supplied, &discovered, &[], case_sensitive)?;
        assert_eq!(file.metadata(), supplied.metadata());
        let expected_file = if case_sensitive {
            supplied.fields().to_vec()
        } else {
            supplied.fields()[1..].to_vec()
        };
        assert_eq!(file.fields(), &expected_file.into());
        assert_eq!(
            fields[1].as_ref(),
            &Field::new("missing", DataType::Utf8, true)
        );
        if case_sensitive {
            assert_eq!(
                fields[0].as_ref(),
                &Field::new("part", DataType::Utf8, true)
            );
        } else {
            assert_eq!(
                fields[0].as_ref(),
                &Field::new("PART", DataType::Int32, true)
            );
        }
        assert_eq!(partition_directory_names(&fields, &discovered)?, discovered);
    }
    Ok(())
}

#[test]
fn automatic_partition_metadata_keeps_logical_types_and_nested_fields_only() -> Result<()> {
    use datafusion::arrow::datatypes::{IntervalUnit, TimeUnit};
    use sail_common::spec::IntervalFieldType::{Month, Year};

    let time = Field::new("clock", DataType::Time64(TimeUnit::Nanosecond), false).with_metadata(
        HashMap::from([(
            spec::SAIL_SPARK_TIME_METADATA_KEY.into(),
            spec::SparkTimeMetadata::try_new(3)
                .unwrap()
                .to_json()
                .unwrap(),
        )]),
    );
    let interval = Field::new("span", DataType::Interval(IntervalUnit::YearMonth), false)
        .with_metadata(HashMap::from([(
            spec::SAIL_SPARK_INTERVAL_METADATA_KEY.into(),
            spec::SparkIntervalMetadata::try_new(
                spec::IntervalUnit::YearMonth,
                Some(Year),
                Some(Month),
            )
            .unwrap()
            .unwrap()
            .to_json()
            .unwrap(),
        )]));
    let udt = Field::new("custom", DataType::Binary, false).with_metadata(HashMap::from([(
        spec::SAIL_SPARK_UDT_METADATA_KEY.into(),
        serde_json::to_string(&spec::SparkUdtMetadata {
            jvm_class: Some("example.CustomType".into()),
            python_class: None,
            serialized_python_class: None,
        })
        .unwrap(),
    )]));
    let geo = Field::new("geo", DataType::Binary, false).with_metadata(HashMap::from([
        (EXTENSION_TYPE_NAME_KEY.into(), "geoarrow.wkb".into()),
        (
            EXTENSION_TYPE_METADATA_KEY.into(),
            "{\"crs\":\"OGC:CRS84\",\"edges\":\"spherical\"}".into(),
        ),
    ]));
    let nested = Field::new(
        "nested",
        DataType::Struct(
            vec![
                Field::new("child", DataType::Utf8, false)
                    .with_metadata(HashMap::from([("child-note".into(), "retained".into())])),
            ]
            .into(),
        ),
        false,
    );
    let type_fields = vec![time, interval, udt, geo, nested];
    let supplied = Schema::new(
        type_fields
            .iter()
            .cloned()
            .map(|field| {
                let mut metadata = field.metadata().clone();
                metadata.extend([
                    ("arbitrary".into(), "discarded".into()),
                    ("comment".into(), "discarded".into()),
                    (
                        spec::SPARK_METADATA_JSON_KEY.into(),
                        "{\"comment\":\"discarded\"}".into(),
                    ),
                    ("sail.schema_evolution.default".into(), "discarded".into()),
                ]);
                field.with_metadata(metadata)
            })
            .collect::<Vec<_>>(),
    );
    let discovered = type_fields
        .iter()
        .map(|field| field.name().clone())
        .collect::<Vec<_>>();
    let (file, automatic) = prepare_explicit_partitions(&supplied, &discovered, &[], true)?;
    assert!(file.fields().is_empty());
    for (actual, expected) in automatic.iter().zip(&type_fields) {
        assert_eq!(actual.as_ref(), &expected.clone().with_nullable(true));
    }
    let (_, explicit) = prepare_explicit_partitions(&supplied, &discovered, &discovered, true)?;
    assert_eq!(explicit, supplied.fields().to_vec());
    Ok(())
}

#[test]
fn explicit_selectors_preserve_supplied_fields_and_fall_back_to_discovery() -> Result<()> {
    let supplied = supplied_schema();
    let discovered = names(&["part", "missing"]);
    let (file, fields) =
        prepare_explicit_partitions(&supplied, &discovered, &names(&["part", "missing"]), false)?;
    assert_eq!(fields[0], supplied.fields()[0]);
    assert!(!fields[0].is_nullable());
    assert_eq!(
        fields[1].as_ref(),
        &Field::new("missing", DataType::Utf8, true)
    );
    assert_eq!(file.fields(), &supplied.fields()[1..].to_vec().into());
    assert_eq!(file.metadata(), supplied.metadata());

    let (_, fields) = prepare_explicit_partitions(&supplied, &discovered, &names(&["part"]), true)?;
    // Under exact matching, lower-case part is discovered; supplied PART stays
    // in the file schema instead of providing its type or nullability.
    assert_eq!(
        fields[0].as_ref(),
        &Field::new("part", DataType::Utf8, true)
    );
    let (file, _) = prepare_explicit_partitions(&supplied, &discovered, &names(&["part"]), true)?;
    assert_eq!(file, supplied);
    let (_, fields) = prepare_explicit_partitions(&supplied, &discovered, &names(&["PART"]), true)?;
    assert_eq!(fields[0], supplied.fields()[0]);
    let error = prepare_explicit_partitions(&supplied, &discovered, &names(&["absent"]), false)
        .unwrap_err();
    assert!(
        error
            .to_string()
            .contains("absent from the supplied schema and discovered directories")
    );
    Ok(())
}

#[test]
fn explicit_selection_keeps_requested_subset_and_order() -> Result<()> {
    let supplied = supplied_schema();
    let (file, fields) = prepare_explicit_partitions(
        &supplied,
        &names(&["part", "missing"]),
        &names(&["missing", "PART"]),
        false,
    )?;
    assert_eq!(
        fields
            .iter()
            .map(|field| field.name().as_str())
            .collect::<Vec<_>>(),
        ["missing", "PART"]
    );
    assert_eq!(file.fields(), &supplied.fields()[1..].to_vec().into());
    let (file, fields) = prepare_explicit_partitions(
        &supplied,
        &names(&["part", "missing"]),
        &names(&["PART"]),
        false,
    )?;
    assert_eq!(fields.len(), 1);
    assert_eq!(file.fields(), &supplied.fields()[1..].to_vec().into());
    Ok(())
}

async fn fixture() -> Result<SessionContext> {
    let mut config = SessionConfig::new().with_collect_statistics(false);
    config
        .options_mut()
        .execution
        .listing_table_ignore_subdirectory = false;
    let context = SessionContext::new_with_config(config);
    let store = Arc::new(InMemory::new());
    store
        .put(
            &Path::from("data/outer=west/inner=one/part.json"),
            Bytes::from_static(b"{\"id\":1}\n").into(),
        )
        .await?;
    context.register_object_store(
        &Url::parse("memory://partition-preparation").unwrap(),
        store,
    );
    Ok(context)
}

fn request(schema: Option<Schema>, partition_by: &[&str]) -> SourceInfo {
    SourceInfo {
        paths: vec!["memory://partition-preparation/data/".into()],
        lakehouse_table: None,
        schema,
        constraints: Constraints::default(),
        partition_by: names(partition_by),
        bucket_by: None,
        sort_order: vec![],
        options: vec![],
        read_case_sensitive: true,
    }
}

#[tokio::test]
async fn source_retains_omitted_partitions_and_ignores_selectors_without_schema() -> Result<()> {
    let context = fixture().await?;
    let supplied = Schema::new(vec![Field::new("id", DataType::Int64, false)]);
    for info in [
        request(Some(supplied.clone()), &[]),
        request(None, &["inner"]),
    ] {
        let source = JsonDataSource::default()
            .create_source(&context.state(), info)
            .await?;
        let listing = source.downcast_ref::<ListingTableSource>().unwrap();
        assert_eq!(
            listing.config().partition_directory_names,
            ["outer", "inner"]
        );
        assert_eq!(
            listing.config().schema.table_partition_cols(),
            &inferred_partition_fields(&names(&["outer", "inner"])).into()
        );
        assert_eq!(listing.config().schema.file_schema().fields().len(), 1);
        assert_eq!(
            source
                .schema()
                .fields()
                .iter()
                .map(|field| field.name().as_str())
                .collect::<Vec<_>>(),
            ["id", "outer", "inner"]
        );
    }
    Ok(())
}

#[tokio::test]
async fn source_case_policy_keeps_file_aliases_distinct_from_discovered_partitions() -> Result<()> {
    let context = fixture().await?;
    for case_sensitive in [false, true] {
        let supplied = Schema::new(vec![
            Field::new("OUTER", DataType::Utf8, false),
            Field::new("id", DataType::Int64, true),
        ]);
        let mut info = request(Some(supplied.clone()), &[]);
        info.read_case_sensitive = case_sensitive;
        let source = JsonDataSource::default()
            .create_source(&context.state(), info)
            .await?;
        let listing = source.downcast_ref::<ListingTableSource>().unwrap();
        assert_eq!(
            listing.config().partition_directory_names,
            ["outer", "inner"]
        );
        let expected_file = if case_sensitive {
            supplied.fields().to_vec()
        } else {
            vec![supplied.fields()[1].clone()]
        };
        assert_eq!(
            listing.config().schema.file_schema().fields(),
            &expected_file.into()
        );
        let fields = listing.config().schema.table_partition_cols();
        assert_eq!(
            fields[0].name(),
            if case_sensitive { "outer" } else { "OUTER" }
        );
        assert!(fields.iter().all(|field| field.is_nullable()));
    }
    Ok(())
}

#[tokio::test]
async fn explicit_inner_only_selection_retains_the_layout_prefix_guard() -> Result<()> {
    let context = fixture().await?;
    let supplied = Schema::new(vec![Field::new("id", DataType::Int64, false)]);
    let source = JsonDataSource::default()
        .create_source(
            &context.state(),
            request(Some(supplied.clone()), &["outer"]),
        )
        .await?;
    assert_eq!(
        source
            .schema()
            .fields()
            .iter()
            .map(|field| field.name().as_str())
            .collect::<Vec<_>>(),
        ["id", "outer"]
    );
    let error = JsonDataSource::default()
        .create_source(&context.state(), request(Some(supplied), &["inner"]))
        .await
        .err()
        .unwrap();
    assert!(error.to_string().contains("Inferred partitions"));
    assert!(error.to_string().contains("outer"));
    Ok(())
}

#[test]
fn an_explicit_empty_schema_retains_all_discovered_partition_fields() -> Result<()> {
    let (file, fields) =
        prepare_explicit_partitions(&Schema::empty(), &names(&["outer", "inner"]), &[], false)?;
    assert!(file.fields().is_empty());
    assert_eq!(
        fields,
        inferred_partition_fields(&names(&["outer", "inner"]))
    );
    Ok(())
}
