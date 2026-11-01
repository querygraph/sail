use std::sync::Arc;

use datafusion::arrow::datatypes::DataType;
use futures::TryStreamExt;
use sail_common_datafusion::catalog::TableColumnStatus;
use sail_common_datafusion::rename::logical_plan::rename_logical_plan;
use sail_common_datafusion::session::job::JobService;
use sail_data_source::formats::parquet::ParquetDataSource;
use sail_plan::resolver::PlanResolver;

use super::*;
use crate::config::ConfigKeyValue;
use crate::pipelines::graph::PipelineGraph;
use crate::pipelines::{analysis, catalog_preparation_tests, execution, sql};
use crate::spark::connect::OutputType;

type TestResult = Result<(), Box<dyn std::error::Error>>;

async fn graph(text: &str) -> SparkResult<GraphDefinition> {
    let mut graph = PipelineGraph::new("test_catalog".into(), "test_db".into(), Default::default());
    sql::prepare(
        pc::DefineSqlGraphElements {
            sql_text: Some(text.into()),
            ..Default::default()
        },
        |_| async { Err(SparkError::internal("no catalog expression in fixture")) },
    )
    .await?
    .commit(&mut graph)?;
    Ok(graph.definition)
}

fn context() -> Result<SessionContext, Box<dyn std::error::Error>> {
    let ctx = catalog_preparation_tests::context()?;
    ctx.extension::<SparkSession>()?
        .set_config(vec![ConfigKeyValue {
            key: "spark.sql.warehouse.dir".into(),
            value: Some(location()),
        }])?;
    let registry = DataSourceRegistry::new();
    registry.register_data_source(Arc::new(ParquetDataSource::default()))?;
    ctx.state_ref()
        .write()
        .config_mut()
        .set_extension(Arc::new(registry));
    Ok(ctx)
}

fn location() -> String {
    std::env::temp_dir()
        .join(format!("sail-sdp-metadata-{}", uuid::Uuid::new_v4()))
        .to_string_lossy()
        .into_owned()
}

async fn materialize(ctx: &SessionContext, graph: &GraphDefinition) -> SparkResult<()> {
    let schemas = metadata_schemas(ctx, graph).await?;
    let selection = Selection::new(graph, &pc::StartRun::default())?;
    tables(ctx, graph, &schemas, &selection).await
}

async fn catalog_columns(
    ctx: &SessionContext,
    table: &str,
) -> Result<Vec<TableColumnStatus>, Box<dyn std::error::Error>> {
    let status = ctx
        .extension::<CatalogManager>()?
        .get_table_or_view(&[table])
        .await?;
    let TableKind::Table { columns, .. } = status.kind else {
        return Err("expected catalog table".into());
    };
    Ok(columns)
}

async fn row_count(ctx: &SessionContext, table: &str) -> Result<usize, Box<dyn std::error::Error>> {
    let query = graph(&format!(
        "CREATE MATERIALIZED VIEW probe AS SELECT * FROM {table}"
    ))
    .await?
    .flows
    .remove(0)
    .query;
    let (plan, _) = sail_plan::resolve_and_execute_plan(
        ctx,
        ctx.extension::<SparkSession>()?.plan_config()?,
        spec::Plan::Query(query),
    )
    .await?;
    let batches = ctx
        .extension::<JobService>()?
        .runner()
        .execute(ctx, plan)
        .await?
        .try_collect::<Vec<_>>()
        .await?;
    Ok(batches.iter().map(|batch| batch.num_rows()).sum())
}

// Spark MaterializeTablesSuite:125–161 registers streaming tables with batch
// DataFrame flows. Metadata materialization must not execute either flow.
#[tokio::test]
async fn multiple_streaming_targets_materialize_nullable_columns_without_rows() -> TestResult {
    let ctx = context()?;
    let graph = graph("CREATE STREAMING TABLE t1 AS SELECT 1 AS x UNION ALL SELECT 2 AS x UNION ALL SELECT 3 AS x; \
         CREATE STREAMING TABLE t2 AS SELECT 'a' AS y UNION ALL SELECT 'b' AS y").await?;
    assert!(
        graph
            .outputs
            .iter()
            .all(|output| output.kind == OutputType::Table)
    );
    materialize(&ctx, &graph).await?;
    for (table, name, data_type) in [("t1", "x", DataType::Int32), ("t2", "y", DataType::Utf8)] {
        let columns = catalog_columns(&ctx, table).await?;
        assert_eq!(columns.len(), 1);
        assert_eq!(columns[0].name, name);
        assert_eq!(columns[0].data_type, data_type);
        assert!(columns[0].nullable);
        assert_eq!(row_count(&ctx, table).await?, 0);
    }
    finish(&ctx).await?;
    Ok(())
}

// Spark MaterializeTablesSuite:374–399: declared schema and identity transform.
#[tokio::test]
async fn streaming_target_preserves_declared_schema_and_identity_partition() -> TestResult {
    let ctx = context()?;
    let graph = graph(
        "CREATE STREAMING TABLE a (x1 INT, x2 INT) PARTITIONED BY (x2) \
         AS SELECT 1 AS x1, 1 AS x2 UNION ALL SELECT 2 AS x1, 3 AS x2",
    )
    .await?;
    materialize(&ctx, &graph).await?;
    let status = ctx
        .extension::<CatalogManager>()?
        .get_table_or_view(&["a"])
        .await?;
    let TableKind::Table {
        columns,
        partition_by,
        ..
    } = status.kind
    else {
        return Err("expected table".into());
    };
    assert_eq!(
        columns
            .iter()
            .map(|c| (&*c.name, &c.data_type, c.nullable))
            .collect::<Vec<_>>(),
        vec![
            ("x1", &DataType::Int32, true),
            ("x2", &DataType::Int32, true)
        ]
    );
    assert_eq!(partition_by.len(), 1);
    assert_eq!(partition_by[0].column, "x2");
    assert!(matches!(
        partition_by[0].transform,
        None | Some(PartitionTransform::Identity)
    ));
    assert_eq!(row_count(&ctx, "a").await?, 0);
    finish(&ctx).await?;
    Ok(())
}

#[tokio::test]
async fn ordinary_streaming_refresh_merges_schema_without_truncating_rows() -> TestResult {
    let ctx = context()?;
    let seed = graph("CREATE MATERIALIZED VIEW target (id INT NOT NULL COMMENT 'old', retained INT) AS SELECT 7 AS id, 8 AS retained").await?;
    let analyzed = analysis::analyze(&ctx, &seed).await?;
    execution::execute(
        &ctx,
        &seed,
        analyzed,
        Selection::new(&seed, &pc::StartRun::default())?,
        String::new(),
        &tokio::sync::mpsc::unbounded_channel().0,
    )
    .await?;
    assert_eq!(row_count(&ctx, "target").await?, 1);
    let next = graph("CREATE STREAMING TABLE target (id INT COMMENT 'new', added STRING) TBLPROPERTIES ('user.key'='value') AS SELECT 9 AS id, 'x' AS added").await?;
    materialize(&ctx, &next).await?;
    let columns = catalog_columns(&ctx, "target").await?;
    assert_eq!(
        columns.iter().map(|c| &*c.name).collect::<Vec<_>>(),
        vec!["id", "retained", "added"]
    );
    assert!(columns[0].nullable);
    assert_eq!(columns[0].comment.as_deref(), Some("old"));
    assert_eq!(row_count(&ctx, "target").await?, 1);
    let status = ctx
        .extension::<CatalogManager>()?
        .get_table_or_view(&["target"])
        .await?;
    let TableKind::Table { properties, .. } = status.kind else {
        return Err("expected table".into());
    };
    assert!(properties.contains(&("user.key".into(), "value".into())));
    let reset_disabled = graph("CREATE STREAMING TABLE target TBLPROPERTIES ('pipelines.reset.allowed'='false') AS SELECT 9 AS id").await?;
    let schemas = metadata_schemas(&ctx, &reset_disabled).await?;
    let selection = Selection::new(
        &reset_disabled,
        &pc::StartRun {
            full_refresh_all: Some(true),
            ..Default::default()
        },
    )?;
    tables(&ctx, &reset_disabled, &schemas, &selection).await?;
    assert_eq!(row_count(&ctx, "target").await?, 1);
    assert_eq!(catalog_columns(&ctx, "target").await?.len(), 3);
    let partition_change =
        graph("CREATE STREAMING TABLE target PARTITIONED BY (id) AS SELECT 9 AS id").await?;
    let error = materialize(&ctx, &partition_change)
        .await
        .err()
        .ok_or("partition change unexpectedly accepted")?;
    assert!(
        error
            .to_string()
            .contains("CANNOT_UPDATE_PARTITION_COLUMNS")
    );
    assert_eq!(row_count(&ctx, "target").await?, 1);
    let incompatible = graph("CREATE STREAMING TABLE target AS SELECT 'bad' AS id").await?;
    assert!(materialize(&ctx, &incompatible).await.is_err());
    assert_eq!(row_count(&ctx, "target").await?, 1);
    assert_eq!(catalog_columns(&ctx, "target").await?.len(), 3);
    finish(&ctx).await?;
    Ok(())
}

#[tokio::test]
async fn full_refresh_metadata_materialization_creates_absent_tables() -> TestResult {
    let ctx = context()?;
    let graph = graph("CREATE MATERIALIZED VIEW batch AS SELECT 1 AS x; CREATE STREAMING TABLE streaming AS SELECT 2 AS x").await?;
    let schemas = metadata_schemas(&ctx, &graph).await?;
    // Metadata materialization under full refresh is DatasetManager's own
    // behaviour for absent targets: both tables are created empty, and the
    // auxiliary drop is a no-op.
    let selection = Selection::new(
        &graph,
        &pc::StartRun {
            full_refresh_all: Some(true),
            ..Default::default()
        },
    )?;
    tables(&ctx, &graph, &schemas, &selection).await?;
    for table in ["batch", "streaming"] {
        assert_eq!(row_count(&ctx, table).await?, 0);
    }
    finish(&ctx).await?;
    Ok(())
}

// DatasetManager.scala:301–328 and AutoCdcScd1FullRefreshSuite: a full
// refresh truncates the target, takes the output schema without merging, and
// drops only that target's AutoCDC auxiliary table; `pipelines.reset.allowed`
// = false downgrades the request to an ordinary refresh.
#[tokio::test]
async fn full_refresh_truncates_resets_schema_and_drops_only_its_auxiliary_state() -> TestResult {
    let ctx = context()?;
    let seed = graph(
        "CREATE MATERIALIZED VIEW target (id INT, retained INT) AS SELECT 7 AS id, 8 AS retained; \
         CREATE MATERIALIZED VIEW __spark_autocdc_aux_state_target AS SELECT 1 AS k; \
         CREATE MATERIALIZED VIEW __spark_autocdc_aux_state_other AS SELECT 2 AS k",
    )
    .await?;
    let analyzed = analysis::analyze(&ctx, &seed).await?;
    execution::execute(
        &ctx,
        &seed,
        analyzed,
        Selection::new(&seed, &pc::StartRun::default())?,
        String::new(),
        &tokio::sync::mpsc::unbounded_channel().0,
    )
    .await?;
    assert_eq!(row_count(&ctx, "target").await?, 1);
    assert_eq!(
        row_count(&ctx, "__spark_autocdc_aux_state_target").await?,
        1
    );
    assert_eq!(row_count(&ctx, "__spark_autocdc_aux_state_other").await?, 1);

    let full_refresh_all = pc::StartRun {
        full_refresh_all: Some(true),
        ..Default::default()
    };

    // reset.allowed = false: refreshed, not reset. Rows and auxiliary state
    // survive and the schema is merged as for any incremental refresh.
    let reset_disabled = graph("CREATE STREAMING TABLE target (id INT, added STRING) TBLPROPERTIES ('pipelines.reset.allowed'='false') AS SELECT 9 AS id, 'x' AS added").await?;
    let schemas = metadata_schemas(&ctx, &reset_disabled).await?;
    let selection = Selection::new(&reset_disabled, &full_refresh_all)?;
    assert!(selection.reset.contains(&reset_disabled.outputs[0].name));
    tables(&ctx, &reset_disabled, &schemas, &selection).await?;
    assert_eq!(row_count(&ctx, "target").await?, 1);
    assert_eq!(
        catalog_columns(&ctx, "target")
            .await?
            .iter()
            .map(|c| &*c.name)
            .collect::<Vec<_>>(),
        vec!["id", "retained", "added"]
    );
    assert_eq!(
        row_count(&ctx, "__spark_autocdc_aux_state_target").await?,
        1
    );

    // Full refresh: truncated, schema taken as declared (retained dropped),
    // the target's auxiliary table dropped, the other target's kept.
    let next = graph(
        "CREATE STREAMING TABLE target (id INT, added STRING) AS SELECT 9 AS id, 'x' AS added",
    )
    .await?;
    let schemas = metadata_schemas(&ctx, &next).await?;
    let selection = Selection::new(&next, &full_refresh_all)?;
    tables(&ctx, &next, &schemas, &selection).await?;
    assert_eq!(row_count(&ctx, "target").await?, 0);
    assert_eq!(
        catalog_columns(&ctx, "target")
            .await?
            .iter()
            .map(|c| &*c.name)
            .collect::<Vec<_>>(),
        vec!["id", "added"]
    );
    assert!(
        ctx.extension::<CatalogManager>()?
            .get_table_or_view(&["__spark_autocdc_aux_state_target"])
            .await
            .is_err()
    );
    assert_eq!(row_count(&ctx, "__spark_autocdc_aux_state_other").await?, 1);

    // A second full refresh with no auxiliary table present is a no-op drop.
    tables(&ctx, &next, &schemas, &selection).await?;
    assert_eq!(row_count(&ctx, "target").await?, 0);
    finish(&ctx).await?;
    Ok(())
}

async fn finish(ctx: &SessionContext) -> TestResult {
    ctx.extension::<JobService>()?.runner().stop().await?;
    let config = ctx.extension::<SparkSession>()?.plan_config()?;
    let warehouse = std::path::Path::new(&config.default_warehouse_directory);
    if warehouse.exists() {
        std::fs::remove_dir_all(warehouse)?;
    }
    Ok(())
}

// Mirror DatasetManager's schema-only input for independent batch DataFrame
// functions. Full execution analysis deliberately rejects these Table flows.
// Resolve the real flow queries without changing their registered output kinds.
async fn metadata_schemas(
    ctx: &SessionContext,
    graph: &GraphDefinition,
) -> SparkResult<BTreeMap<Name, SchemaRef>> {
    let resolver = PlanResolver::new(ctx, ctx.extension::<SparkSession>()?.plan_config()?);
    let mut schemas = BTreeMap::new();
    for output in &graph.outputs {
        let mut plans = vec![];
        for flow in graph.flows.iter().filter(|flow| flow.target == output.name) {
            let named = resolver
                .resolve_named_plan(spec::Plan::Query(flow.query.clone()))
                .await?;
            plans.push(match named.fields {
                Some(fields) => rename_logical_plan(named.plan, &fields)?,
                None => named.plan,
            });
        }
        schemas.insert(
            output.name.clone(),
            Arc::new(crate::pipelines::schema::infer(ctx, output, &plans)?),
        );
    }
    Ok(schemas)
}

mod partitions;

mod basic;
