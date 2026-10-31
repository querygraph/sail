use std::sync::Arc;

use bytes::Bytes;
use datafusion::arrow::array::RecordBatch;
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::logical_expr::TableSource;
use datafusion::parquet::arrow::ArrowWriter;
use datafusion::prelude::{SessionConfig, SessionContext};
use datafusion_common::{Constraints, DataFusionError};
use object_store::ObjectStoreExt;
use object_store::memory::InMemory;
use object_store::path::Path;
use sail_common_datafusion::datasource::{DataSource, SourceInfo};
use sail_common_datafusion::error::StructuredSparkError;
use url::Url;

use crate::formats::parquet::ParquetDataSource;

type TestResult<T = ()> = Result<T, Box<dyn std::error::Error>>;
fn mixed() -> Schema {
    Schema::new(vec![
        Field::new("Foo", DataType::Int64, true),
        Field::new("foo", DataType::Int64, true),
    ])
}
async fn source(
    physical: Schema,
    requested: Option<Schema>,
    sensitive: bool,
    normalize: bool,
    partitioned: bool,
) -> TestResult<Arc<dyn TableSource>> {
    let mut config = SessionConfig::new();
    config.options_mut().sql_parser.enable_ident_normalization = normalize;
    config
        .options_mut()
        .execution
        .listing_table_ignore_subdirectory = false;
    let ctx = SessionContext::new_with_config(config);
    let schema = Arc::new(physical);
    let mut bytes = Vec::new();
    let mut writer = ArrowWriter::try_new(&mut bytes, schema.clone(), None)?;
    writer.write(&RecordBatch::new_empty(schema))?;
    writer.close()?;
    let store = Arc::new(InMemory::new());
    let path = if partitioned {
        "data/p=one/part.parquet"
    } else {
        "data/part.parquet"
    };
    store
        .put(&Path::from(path), Bytes::from(bytes).into())
        .await?;
    ctx.register_object_store(&Url::parse("memory://schema-validation")?, store);
    Ok(ParquetDataSource::default()
        .create_source(
            &ctx.state(),
            SourceInfo {
                paths: vec!["memory://schema-validation/data/".into()],
                lakehouse_table: None,
                schema: requested,
                constraints: Constraints::default(),
                partition_by: vec![],
                bucket_by: None,
                sort_order: vec![],
                options: vec![],
                read_case_sensitive: sensitive,
            },
        )
        .await?)
}
fn duplicate(error: Box<dyn std::error::Error>, expected: &str) -> TestResult {
    let datafusion = error
        .downcast_ref::<DataFusionError>()
        .ok_or("not a DataFusion error")?;
    let DataFusionError::External(cause) = datafusion else {
        return Err(format!("unstructured: {datafusion}").into());
    };
    let cause = cause
        .downcast_ref::<StructuredSparkError>()
        .ok_or("lost structured details")?;
    assert_eq!(cause.condition, "COLUMN_ALREADY_EXISTS");
    assert_eq!(cause.sql_state, "42711");
    assert_eq!(cause.parameters["columnName"], expected);
    Ok(())
}

#[tokio::test]
async fn actual_parquet_metadata_uses_captured_case_policy_at_root_and_nested_levels() -> TestResult
{
    for normalize in [false, true] {
        for schema in [
            mixed(),
            Schema::new(vec![Field::new(
                "outer",
                DataType::Struct(mixed().fields().clone()),
                true,
            )]),
        ] {
            let expected = schema.fields().clone();
            let allowed = source(schema.clone(), None, true, normalize, false).await?;
            assert_eq!(allowed.schema().fields(), &expected);
            duplicate(
                source(schema, None, false, normalize, false)
                    .await
                    .err()
                    .ok_or("duplicate file columns accepted")?,
                "`foo`",
            )?;
        }
    }
    Ok(())
}

#[tokio::test]
async fn supplied_schema_names_are_validated_with_the_same_source_policy() -> TestResult {
    let physical = Schema::new(vec![Field::new("foo", DataType::Int64, true)]);
    source(physical.clone(), Some(mixed()), true, false, false).await?;
    duplicate(
        source(physical, Some(mixed()), false, true, false)
            .await
            .err()
            .ok_or("duplicate supplied names accepted")?,
        "`foo`",
    )
}

#[tokio::test]
async fn separate_data_and_partition_checks_allow_partition_shadowing() -> TestResult {
    // Exact schemas from the six public Spark Parquet shadowing observations.
    for names in [vec!["p"], vec!["P"], vec!["P", "p"]] {
        let physical = Schema::new(
            names
                .iter()
                .map(|name| Field::new(*name, DataType::Utf8, true))
                .collect::<Vec<_>>(),
        );
        for sensitive in [false, true] {
            let result = source(physical.clone(), None, sensitive, false, true).await;
            if names.len() == 2 && !sensitive {
                duplicate(
                    result
                        .err()
                        .ok_or("partition shadow hid duplicate file fields")?,
                    "`p`",
                )?;
                continue;
            }
            let source = result?;
            let expected = if sensitive && names.contains(&"P") {
                vec!["P", "p"]
            } else {
                vec!["p"]
            };
            assert_eq!(
                source
                    .schema()
                    .fields()
                    .iter()
                    .map(|field| field.name().as_str())
                    .collect::<Vec<_>>(),
                expected
            );
        }
    }
    Ok(())
}
