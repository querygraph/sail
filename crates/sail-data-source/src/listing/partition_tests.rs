#![expect(clippy::unwrap_used)]

use std::sync::Arc;

use bytes::Bytes;
use datafusion::arrow::array::{Int64Array, StringArray};
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::execution::context::{SessionConfig, SessionContext};
use datafusion::logical_expr::physical_planning_context::PhysicalPlanningContext;
use datafusion::logical_expr::{LogicalPlan, LogicalPlanBuilder, TableProviderFilterPushDown};
use datafusion::physical_plan::collect;
use datafusion::physical_plan::empty::EmptyExec;
use datafusion::physical_planner::{DefaultPhysicalPlanner, ExtensionPlanner};
use datafusion::prelude::{col, lit};
use datafusion_common::{Column, Constraints, Result, ScalarValue};
use datafusion_datasource::file_scan_config::FileScanConfig;
use datafusion_datasource::source::DataSourceExec;
use object_store::ObjectStoreExt;
use object_store::memory::InMemory;
use object_store::path::Path;
use sail_common_datafusion::datasource::{DataSource, SourceInfo};
use url::Url;

use super::*;
use crate::formats::json::JsonDataSource;
use crate::listing::planner::ListingPhysicalPlanner;
use crate::listing::table::ListingTableSource;

async fn fixture() -> Result<(SessionContext, Arc<InMemory>)> {
    let context = SessionContext::new_with_config(
        SessionConfig::new()
            .with_collect_statistics(false)
            .with_target_partitions(1),
    );
    let store = Arc::new(InMemory::new());
    for (name, contents) in [
        ("west", "{\"id\":11}\n{\"id\":12}\n"),
        ("east", "{\"id\":21}\n"),
    ] {
        store
            .put(
                &Path::from(format!("data/part={name}/data.json")),
                Bytes::from(contents).into(),
            )
            .await?;
    }
    context.register_object_store(
        &Url::parse("memory://partition-bindings").unwrap(),
        store.clone(),
    );
    Ok((context, store))
}

fn request(partition_name: &str) -> SourceInfo {
    SourceInfo {
        paths: vec!["memory://partition-bindings/data/".into()],
        lakehouse_table: None,
        schema: Some(Schema::new(vec![
            Field::new("id", DataType::Int64, true),
            Field::new(partition_name, DataType::Utf8, true),
        ])),
        constraints: Constraints::default(),
        partition_by: vec![],
        bucket_by: None,
        sort_order: vec![],
        options: vec![],
        read_case_sensitive: false,
    }
}

#[tokio::test]
async fn logical_partition_aliases_preserve_rows_pruning_and_object_identity() -> Result<()> {
    let (context, store) = fixture().await?;
    let state = context.state();
    let planner = DefaultPhysicalPlanner::with_extension_planners(vec![]);
    for (name, selectors) in [
        ("part", vec![]),
        ("PART", vec![]),
        ("part", vec!["PART".into()]),
        ("PART", vec!["part".into()]),
    ] {
        let mut info = request(name);
        info.partition_by = selectors;
        let source = JsonDataSource::default()
            .create_source(&state, info)
            .await?;
        let listing = source.downcast_ref::<ListingTableSource>().unwrap();
        assert_eq!(listing.config().partition_directory_names, ["part"]);
        assert_eq!(
            listing.config().schema.table_partition_cols()[0].name(),
            name
        );
        assert_eq!(listing.config().schema.file_schema().fields().len(), 1);
        let schema = source.schema();
        // col(&str) lowercases an unquoted identifier; construct the actual
        // logical column that Sail's resolver supplies to this planner.
        let partition = col(Column::from_name(name));
        for (filters, expected) in [
            (vec![], vec![(11, "west"), (12, "west"), (21, "east")]),
            (
                vec![partition.clone().eq(lit("west"))],
                vec![(11, "west"), (12, "west")],
            ),
            // A non-equality predicate must also bind against the physical
            // partition schema used by DataFusion's listing filter evaluator.
            (
                vec![partition.clone().not_eq(lit("west"))],
                vec![(21, "east")],
            ),
            (vec![partition.eq(lit("missing"))], vec![]),
        ] {
            assert_eq!(
                source.supports_filters_pushdown(&filters.iter().collect::<Vec<_>>())?,
                vec![TableProviderFilterPushDown::Exact; filters.len()]
            );
            let logical =
                LogicalPlanBuilder::scan_with_filters("files", source.clone(), None, filters)?
                    .build()?;
            let LogicalPlan::TableScan(scan) = logical else {
                unreachable!("scan builder produces TableScan")
            };
            let physical = ListingPhysicalPlanner
                .plan_table_scan(&planner, &scan, &state, &PhysicalPlanningContext::default())
                .await?
                .unwrap();
            assert_eq!(physical.schema(), schema);
            if expected.is_empty() {
                assert!(physical.is::<EmptyExec>());
            } else {
                let exec = physical.downcast_ref::<DataSourceExec>().unwrap();
                let config = exec.data_source().downcast_ref::<FileScanConfig>().unwrap();
                let mut selected = vec![];
                for file in config.file_groups.iter().flat_map(|group| group.iter()) {
                    let original = store.head(&file.object_meta.location).await?;
                    assert_eq!(file.object_meta, original);
                    let partition = if file.object_meta.location.as_ref().contains("part=west/") {
                        "west"
                    } else {
                        "east"
                    };
                    assert_eq!(
                        file.partition_values,
                        [ScalarValue::Utf8(Some(partition.into()))]
                    );
                    selected.push(partition);
                }
                selected.sort_unstable();
                let mut expected_partitions = expected.iter().map(|(_, p)| *p).collect::<Vec<_>>();
                expected_partitions.sort_unstable();
                expected_partitions.dedup();
                assert_eq!(selected, expected_partitions);
            }
            let batches = collect(physical, context.task_ctx()).await?;
            let mut actual = vec![];
            for batch in batches {
                let ids = batch
                    .column(0)
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .unwrap();
                let partitions = batch
                    .column(1)
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .unwrap();
                for i in 0..batch.num_rows() {
                    actual.push((ids.value(i), partitions.value(i).to_owned()));
                }
            }
            actual.sort_unstable();
            assert_eq!(
                actual,
                expected
                    .into_iter()
                    .map(|(id, p)| (id, p.to_owned()))
                    .collect::<Vec<_>>()
            );
        }
    }
    Ok(())
}

#[tokio::test]
async fn invalid_partition_binding_count_is_rejected_before_scan() -> Result<()> {
    let (context, _) = fixture().await?;
    let source = JsonDataSource::default()
        .create_source(&context.state(), request("PART"))
        .await?;
    let mut config = source
        .downcast_ref::<ListingTableSource>()
        .unwrap()
        .config()
        .clone();
    for names in [vec![], vec!["part".into(), "extra".into()]] {
        config.partition_directory_names = names;
        assert!(
            ListingTableSource::try_new(config.clone())
                .unwrap_err()
                .to_string()
                .contains("directory keys must match the number of partition fields")
        );
    }
    Ok(())
}

#[test]
fn filter_rebinding_uses_original_logical_names_once() -> Result<()> {
    // Swapped names must not recursively rename the first replacement again.
    let fields = vec![
        Arc::new(Field::new("a", DataType::Utf8, true)),
        Arc::new(Field::new("b", DataType::Utf8, true)),
    ];
    let filters = vec![col("a").eq(lit("x")).and(col("b").not_eq(lit("y")))];
    assert_eq!(
        rebind_partition_filters(&filters, &fields, &["b".into(), "a".into()])?,
        vec![col("b").eq(lit("x")).and(col("a").not_eq(lit("y")))]
    );
    assert_eq!(
        rebind_partition_filters(&filters, &fields, &["a".into(), "b".into()])?,
        filters
    );
    Ok(())
}
