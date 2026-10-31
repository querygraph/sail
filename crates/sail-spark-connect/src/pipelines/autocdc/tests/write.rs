//! The SCD1 write component against a Delta destination, in-process:
//! `AutoCdcScd1SinglePipelineSuite.scala` and the cross-run cases of
//! `AutoCdcScd1AuxiliaryTableDurabilitySuite.scala` and
//! `AutoCdcScd1FullRefreshSuite.scala`, each run as handler invocations over
//! MemTable microbatches. The MemoryStream, checkpoint and runner halves of
//! those suites belong to activation.

use std::sync::Arc;

use datafusion::arrow::array::{ArrayRef, BooleanArray, Int32Array, Int64Array, StringArray};
use datafusion::arrow::datatypes::{Field, Schema};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::arrow::util::display::{ArrayFormatter, FormatOptions};
use datafusion::datasource::{MemTable, provider_as_source};
use datafusion::execution::SessionStateBuilder;
use datafusion::logical_expr::{LogicalPlan, LogicalPlanBuilder};
use datafusion::prelude::SessionContext;
use futures::TryStreamExt;
use sail_catalog::manager::CatalogManager;
use sail_common::spec;
use sail_common_datafusion::datasource::DataSourceRegistry;
use sail_common_datafusion::extension::SessionExtensionAccessor;
use sail_common_datafusion::session::job::JobService;
use sail_common_datafusion::session::repartition::RepartitionBufferConfig;
use sail_data_source::formats::parquet::ParquetDataSource;
use sail_delta_lake::DeltaLakeSource;
use sail_delta_lake::session_extension::DeltaTableCache;
use sail_physical_optimizer::{PhysicalOptimizerOptions, get_physical_optimizers};
use sail_plan::config::PlanConfig;

use super::super::schema::{Case, METADATA_COLUMN};
use super::super::write::{Scd1Write, ensure_auxiliary_table, handle_microbatch};
use super::super::{
    ChangeArgs, ColumnSelection, ScdType, SparkResult, UnqualifiedColumnName, auxiliary,
};
use crate::config::ConfigKeyValue;
use crate::pipelines::catalog_preparation_tests;
use crate::pipelines::execution::run_command;
use crate::pipelines::graph::Name;
use crate::session::SparkSession;

type TestResult = Result<(), Box<dyn std::error::Error>>;

/// The pipelines test context with the production session's analyzer,
/// optimizer and physical optimizer rules, which the Delta merge path needs
/// (it plans an explicit repartition that only Sail's physical rules remove).
pub(super) fn context() -> Result<(SessionContext, Arc<PlanConfig>), Box<dyn std::error::Error>> {
    let ctx = catalog_preparation_tests::context()?;
    let state = SessionStateBuilder::new_from_existing(ctx.state())
        .with_analyzer_rules(sail_session::optimizer::default_analyzer_rules())
        .with_optimizer_rules(sail_session::optimizer::default_optimizer_rules())
        .with_physical_optimizer_rules(get_physical_optimizers(PhysicalOptimizerOptions {
            enable_join_reorder: false,
            join_reorder: Default::default(),
        }))
        .with_query_planner(sail_session::planner::new_query_planner())
        .build();
    *ctx.state_ref().write() = state;
    ctx.state_ref()
        .write()
        .config_mut()
        .set_extension(Arc::new(RepartitionBufferConfig::default()));
    ctx.state_ref()
        .write()
        .config_mut()
        .set_extension(Arc::new(DeltaTableCache::default()));
    let warehouse = std::env::temp_dir()
        .join(format!("sail-sdp-autocdc-{}", uuid::Uuid::new_v4()))
        .to_string_lossy()
        .into_owned();
    ctx.extension::<SparkSession>()?
        .set_config(vec![ConfigKeyValue {
            key: "spark.sql.warehouse.dir".into(),
            value: Some(warehouse),
        }])?;
    let registry = DataSourceRegistry::new();
    registry.register_data_source(Arc::new(ParquetDataSource::default()))?;
    registry.register_data_source(Arc::new(DeltaLakeSource))?;
    ctx.state_ref()
        .write()
        .config_mut()
        .set_extension(Arc::new(registry));
    let config = ctx.extension::<SparkSession>()?.plan_config()?;
    Ok((ctx, config))
}

pub(super) async fn finish(ctx: &SessionContext) -> TestResult {
    ctx.extension::<JobService>()?.runner().stop().await?;
    let config = ctx.extension::<SparkSession>()?.plan_config()?;
    let warehouse = std::path::Path::new(&config.default_warehouse_directory);
    if warehouse.exists() {
        std::fs::remove_dir_all(warehouse)?;
    }
    Ok(())
}

/// `$cdcMetadataDdl`: the metadata column of a BIGINT sequencing type.
pub(super) fn metadata_column() -> spec::TableColumnDefinition {
    spec::TableColumnDefinition {
        name: METADATA_COLUMN.into(),
        data_type: spec::DataType::Struct {
            fields: spec::Fields::from(vec![
                spec::Field {
                    name: "deleteSequence".into(),
                    data_type: spec::DataType::Int64,
                    nullable: true,
                    metadata: vec![],
                },
                spec::Field {
                    name: "upsertSequence".into(),
                    data_type: spec::DataType::Int64,
                    nullable: true,
                    metadata: vec![],
                },
            ]),
        },
        nullable: false,
        comment: None,
        default: None,
        generated_always_as: None,
        identity: None,
    }
}

pub(super) fn column(
    name: &str,
    data_type: spec::DataType,
    nullable: bool,
) -> spec::TableColumnDefinition {
    spec::TableColumnDefinition {
        name: name.into(),
        data_type,
        nullable,
        comment: None,
        default: None,
        generated_always_as: None,
        identity: None,
    }
}

/// `CREATE TABLE target (...) USING <format>` in the warehouse.
pub(super) async fn create_target(
    ctx: &SessionContext,
    config: Arc<PlanConfig>,
    name: &Name,
    format: &str,
    columns: Vec<spec::TableColumnDefinition>,
) -> SparkResult<()> {
    run_command(
        ctx,
        config,
        spec::CommandNode::CreateTable {
            table: name.clone().into(),
            definition: spec::TableDefinition {
                external: false,
                columns,
                comment: None,
                constraints: vec![],
                location: None,
                file_format: Some(spec::TableFileFormat::General {
                    format: format.into(),
                }),
                row_format: None,
                partition_by: vec![],
                sort_by: vec![],
                bucket_by: None,
                cluster_by: vec![],
                mode: spec::CreateTableMode::Create,
                options: vec![],
                properties: vec![],
            },
        },
        None,
    )
    .await
}

pub(super) fn scan(columns: Vec<(&str, ArrayRef)>) -> SparkResult<LogicalPlan> {
    let fields: Vec<Field> = columns
        .iter()
        .map(|(name, array)| Field::new(*name, array.data_type().clone(), true))
        .collect();
    let schema = Arc::new(Schema::new(fields));
    let arrays = columns.into_iter().map(|(_, array)| array).collect();
    let batch = RecordBatch::try_new(schema.clone(), arrays)?;
    let table = MemTable::try_new(schema, vec![vec![batch]])?;
    Ok(LogicalPlanBuilder::scan("batch", provider_as_source(Arc::new(table)), None)?.build()?)
}

/// The flow's schema as the analysis gives it: the batch fields plus the
/// metadata column.
pub(super) fn flow_schema(batch: &LogicalPlan) -> SparkResult<spec::Fields> {
    let mut fields = Vec::new();
    for field in batch.schema().fields() {
        let data_type: spec::DataType =
            crate::spark::connect::DataType::try_from(field.data_type().clone())?.try_into()?;
        fields.push(spec::Field {
            name: field.name().clone(),
            data_type,
            nullable: field.is_nullable(),
            metadata: vec![],
        });
    }
    let metadata = metadata_column();
    fields.push(spec::Field {
        name: metadata.name,
        data_type: metadata.data_type,
        nullable: true,
        metadata: vec![],
    });
    Ok(spec::Fields::from(fields))
}

pub(super) fn attribute(name: &str) -> spec::Expr {
    spec::Expr::UnresolvedAttribute {
        name: spec::ObjectName::bare(name),
        plan_id: None,
        is_metadata_column: false,
    }
}

pub(super) fn eq_true(name: &str) -> spec::Expr {
    spec::Expr::UnresolvedFunction(spec::UnresolvedFunction {
        function_name: spec::ObjectName::bare("="),
        arguments: vec![
            attribute(name),
            spec::Expr::Literal(spec::Literal::Boolean { value: Some(true) }),
        ],
        named_arguments: vec![],
        is_distinct: false,
        is_user_defined_function: false,
        is_internal: None,
        ignore_nulls: None,
        filter: None,
        order_by: None,
    })
}

/// Every row of a table rendered (nulls as `NULL`), sorted.
pub(super) async fn rows(
    ctx: &SessionContext,
    config: Arc<PlanConfig>,
    name: &Name,
) -> SparkResult<Vec<Vec<String>>> {
    let query = spec::QueryPlan::new(spec::QueryNode::Read {
        read_type: spec::ReadType::NamedTable(Box::new(spec::ReadNamedTable {
            name: name.clone().into(),
            temporal: None,
            sample: None,
            options: vec![],
        })),
        is_streaming: false,
    });
    let (plan, _) =
        sail_plan::resolve_and_execute_plan(ctx, config, spec::Plan::Query(query)).await?;
    let batches = ctx
        .extension::<JobService>()?
        .runner()
        .execute(ctx, plan)
        .await?
        .try_collect::<Vec<_>>()
        .await?;
    let options = FormatOptions::default().with_null("NULL");
    let mut out = Vec::new();
    for batch in &batches {
        let formatters = batch
            .columns()
            .iter()
            .map(|column| ArrayFormatter::try_new(column.as_ref(), &options))
            .collect::<Result<Vec<_>, _>>()?;
        for row in 0..batch.num_rows() {
            out.push(
                formatters
                    .iter()
                    .map(|formatter| formatter.value(row).to_string())
                    .collect::<Vec<_>>(),
            );
        }
    }
    out.sort();
    Ok(out)
}

pub(super) fn row(values: &[&str]) -> Vec<String> {
    values.iter().map(|v| (*v).to_string()).collect()
}

pub(super) fn meta(delete: &str, upsert: &str) -> String {
    format!("{{deleteSequence: {delete}, upsertSequence: {upsert}}}")
}

pub(super) fn key(name: &str) -> SparkResult<UnqualifiedColumnName> {
    UnqualifiedColumnName::parse(name)
}

pub(super) fn target() -> Name {
    vec!["test_catalog".into(), "test_db".into(), "target".into()]
}

// SinglePipelineSuite:43–72: an upsert event lands a new row in an empty target.
#[tokio::test]
pub(super) async fn an_upsert_event_lands_a_new_row_in_an_empty_target() -> TestResult {
    let (ctx, config) = context()?;
    let target = target();
    create_target(
        &ctx,
        config.clone(),
        &target,
        "delta",
        vec![
            column("id", spec::DataType::Int32, false),
            column("name", spec::DataType::Utf8, true),
            column("version", spec::DataType::Int64, false),
            metadata_column(),
        ],
    )
    .await?;
    let args = ChangeArgs::new(
        vec![key("id")?],
        attribute("version"),
        ScdType::Type1,
        None,
        None,
    )?;
    let write = Scd1Write {
        flow_name: "test_catalog.test_db.auto_cdc_flow",
        destination: &target,
        destination_format: Some("delta"),
        args: &args,
        sequencing_type: &spec::DataType::Int64,
        case: Case::Insensitive,
    };
    let batch = scan(vec![
        ("id", Arc::new(Int32Array::from(vec![1])) as ArrayRef),
        ("name", Arc::new(StringArray::from(vec!["alice"]))),
        ("version", Arc::new(Int64Array::from(vec![1_i64]))),
    ])?;
    let aux = ensure_auxiliary_table(&ctx, config.clone(), &write, &flow_schema(&batch)?).await?;
    assert_eq!(aux, auxiliary::table_name(&target));
    handle_microbatch(&ctx, config.clone(), &write, &aux, batch, 0).await?;
    assert_eq!(
        rows(&ctx, config.clone(), &target).await?,
        vec![row(&["1", "alice", "1", &meta("NULL", "1")])]
    );
    // The auxiliary table exists, carries the properties, and holds no tombstone.
    let status = ctx
        .extension::<CatalogManager>()?
        .get_table_or_view(&aux)
        .await?;
    let sail_common_datafusion::catalog::TableKind::Table {
        properties,
        columns,
        ..
    } = status.kind
    else {
        return Err("auxiliary table expected".into());
    };
    assert!(properties.contains(&("pipelines.autocdc.scdType".into(), "SCD1".into())));
    assert!(properties.contains(&("pipelines.autocdc.keyColumnNames".into(), "[\"id\"]".into())));
    assert_eq!(
        columns.iter().map(|c| c.name.as_str()).collect::<Vec<_>>(),
        vec!["id", METADATA_COLUMN]
    );
    assert!(rows(&ctx, config, &aux).await?.is_empty());
    finish(&ctx).await?;
    Ok(())
}

// SinglePipelineSuite:74–117: upsert, delete and re-upsert of one key in one
// run, with the delete flag excluded from the target.
#[tokio::test]
pub(super) async fn consecutive_upsert_delete_and_reupsert_in_one_run() -> TestResult {
    let (ctx, config) = context()?;
    let target = target();
    create_target(
        &ctx,
        config.clone(),
        &target,
        "delta",
        vec![
            column("id", spec::DataType::Int32, false),
            column("name", spec::DataType::Utf8, true),
            column("version", spec::DataType::Int64, false),
            metadata_column(),
        ],
    )
    .await?;
    let args = ChangeArgs::new(
        vec![key("id")?],
        attribute("version"),
        ScdType::Type1,
        Some(eq_true("is_delete")),
        Some(ColumnSelection::Exclude(vec![key("is_delete")?])),
    )?;
    let write = Scd1Write {
        flow_name: "test_catalog.test_db.auto_cdc_flow",
        destination: &target,
        destination_format: Some("delta"),
        args: &args,
        sequencing_type: &spec::DataType::Int64,
        case: Case::Insensitive,
    };
    let batch = scan(vec![
        ("id", Arc::new(Int32Array::from(vec![1, 1, 1])) as ArrayRef),
        (
            "name",
            Arc::new(StringArray::from(vec!["alice", "alice", "alice2"])),
        ),
        ("version", Arc::new(Int64Array::from(vec![1_i64, 2, 3]))),
        (
            "is_delete",
            Arc::new(BooleanArray::from(vec![false, true, false])),
        ),
    ])?;
    let aux = ensure_auxiliary_table(&ctx, config.clone(), &write, &flow_schema(&batch)?).await?;
    handle_microbatch(&ctx, config.clone(), &write, &aux, batch, 0).await?;
    assert_eq!(
        rows(&ctx, config.clone(), &target).await?,
        vec![row(&["1", "alice2", "3", &meta("NULL", "3")])]
    );
    finish(&ctx).await?;
    Ok(())
}

pub(super) fn target_columns() -> Vec<spec::TableColumnDefinition> {
    vec![
        column("id", spec::DataType::Int32, false),
        column("name", spec::DataType::Utf8, true),
        column("version", spec::DataType::Int64, false),
        metadata_column(),
    ]
}

pub(super) fn batch(rows: &[(i32, &str, i64)]) -> SparkResult<LogicalPlan> {
    scan(vec![
        (
            "id",
            Arc::new(Int32Array::from(
                rows.iter().map(|r| r.0).collect::<Vec<_>>(),
            )) as ArrayRef,
        ),
        (
            "name",
            Arc::new(StringArray::from(
                rows.iter().map(|r| r.1).collect::<Vec<_>>(),
            )),
        ),
        (
            "version",
            Arc::new(Int64Array::from(
                rows.iter().map(|r| r.2).collect::<Vec<_>>(),
            )),
        ),
    ])
}

pub(super) fn flagged(rows: &[(i32, &str, i64, bool)]) -> SparkResult<LogicalPlan> {
    scan(vec![
        (
            "id",
            Arc::new(Int32Array::from(
                rows.iter().map(|r| r.0).collect::<Vec<_>>(),
            )) as ArrayRef,
        ),
        (
            "name",
            Arc::new(StringArray::from(
                rows.iter().map(|r| r.1).collect::<Vec<_>>(),
            )),
        ),
        (
            "version",
            Arc::new(Int64Array::from(
                rows.iter().map(|r| r.2).collect::<Vec<_>>(),
            )),
        ),
        (
            "is_delete",
            Arc::new(BooleanArray::from(
                rows.iter().map(|r| r.3).collect::<Vec<_>>(),
            )),
        ),
    ])
}

/// One pipeline run: ensure the auxiliary table, then one microbatch.
pub(super) async fn run(
    ctx: &SessionContext,
    config: Arc<PlanConfig>,
    write: &Scd1Write<'_>,
    batch: LogicalPlan,
    batch_id: i64,
) -> SparkResult<Name> {
    let aux = ensure_auxiliary_table(ctx, config.clone(), write, &flow_schema(&batch)?).await?;
    handle_microbatch(ctx, config, write, &aux, batch, batch_id).await?;
    Ok(aux)
}

// DurabilitySuite:42–81: a higher-sequence event in a later run upserts the row.
#[tokio::test]
pub(super) async fn a_higher_sequence_event_in_a_later_run_upserts_the_row() -> TestResult {
    let (ctx, config) = context()?;
    let target = target();
    create_target(&ctx, config.clone(), &target, "delta", target_columns()).await?;
    let args = ChangeArgs::new(
        vec![key("id")?],
        attribute("version"),
        ScdType::Type1,
        None,
        None,
    )?;
    let write = Scd1Write {
        flow_name: "test_catalog.test_db.auto_cdc_flow",
        destination: &target,
        destination_format: Some("delta"),
        args: &args,
        sequencing_type: &spec::DataType::Int64,
        case: Case::Insensitive,
    };
    run(&ctx, config.clone(), &write, batch(&[(1, "alice", 1)])?, 0).await?;
    assert_eq!(
        rows(&ctx, config.clone(), &target).await?,
        vec![row(&["1", "alice", "1", &meta("NULL", "1")])]
    );
    run(
        &ctx,
        config.clone(),
        &write,
        batch(&[(1, "alice2", 2), (2, "bob", 1)])?,
        1,
    )
    .await?;
    assert_eq!(
        rows(&ctx, config.clone(), &target).await?,
        vec![
            row(&["1", "alice2", "2", &meta("NULL", "2")]),
            row(&["2", "bob", "1", &meta("NULL", "1")]),
        ]
    );
    finish(&ctx).await?;
    Ok(())
}

// DurabilitySuite:83–120: an event below a prior run's applied delete is suppressed.
#[tokio::test]
pub(super) async fn an_event_below_a_prior_runs_delete_is_suppressed() -> TestResult {
    let (ctx, config) = context()?;
    let target = target();
    create_target(&ctx, config.clone(), &target, "delta", target_columns()).await?;
    let args = ChangeArgs::new(
        vec![key("id")?],
        attribute("version"),
        ScdType::Type1,
        Some(eq_true("is_delete")),
        Some(ColumnSelection::Exclude(vec![key("is_delete")?])),
    )?;
    let write = Scd1Write {
        flow_name: "test_catalog.test_db.auto_cdc_flow",
        destination: &target,
        destination_format: Some("delta"),
        args: &args,
        sequencing_type: &spec::DataType::Int64,
        case: Case::Insensitive,
    };
    let aux = run(
        &ctx,
        config.clone(),
        &write,
        flagged(&[(1, "alice", 10, true)])?,
        0,
    )
    .await?;
    assert!(rows(&ctx, config.clone(), &target).await?.is_empty());
    // The tombstone is recorded in the auxiliary table.
    assert_eq!(
        rows(&ctx, config.clone(), &aux).await?,
        vec![row(&["1", &meta("10", "NULL")])]
    );
    run(
        &ctx,
        config.clone(),
        &write,
        flagged(&[(1, "stale", 5, false)])?,
        1,
    )
    .await?;
    assert!(rows(&ctx, config.clone(), &target).await?.is_empty());
    finish(&ctx).await?;
    Ok(())
}

// DurabilitySuite:184–214: a dropped auxiliary table is recreated transparently.
#[tokio::test]
pub(super) async fn a_dropped_auxiliary_table_is_recreated() -> TestResult {
    let (ctx, config) = context()?;
    let target = target();
    create_target(
        &ctx,
        config.clone(),
        &target,
        "delta",
        vec![
            column("id", spec::DataType::Int32, false),
            column("version", spec::DataType::Int64, false),
            metadata_column(),
        ],
    )
    .await?;
    let args = ChangeArgs::new(
        vec![key("id")?],
        attribute("version"),
        ScdType::Type1,
        None,
        None,
    )?;
    let write = Scd1Write {
        flow_name: "test_catalog.test_db.auto_cdc_flow",
        destination: &target,
        destination_format: Some("delta"),
        args: &args,
        sequencing_type: &spec::DataType::Int64,
        case: Case::Insensitive,
    };
    let versions = |rows: &[(i32, i64)]| -> SparkResult<LogicalPlan> {
        scan(vec![
            (
                "id",
                Arc::new(Int32Array::from(
                    rows.iter().map(|r| r.0).collect::<Vec<_>>(),
                )) as ArrayRef,
            ),
            (
                "version",
                Arc::new(Int64Array::from(
                    rows.iter().map(|r| r.1).collect::<Vec<_>>(),
                )),
            ),
        ])
    };
    let aux = run(&ctx, config.clone(), &write, versions(&[(1, 1)])?, 0).await?;
    assert!(
        ctx.extension::<CatalogManager>()?
            .get_table_or_view(&aux)
            .await
            .is_ok()
    );
    run_command(
        &ctx,
        config.clone(),
        spec::CommandNode::DropTable {
            table: aux.clone().into(),
            if_exists: false,
            purge: false,
        },
        None,
    )
    .await?;
    assert!(
        ctx.extension::<CatalogManager>()?
            .get_table_or_view(&aux)
            .await
            .is_err()
    );
    run(&ctx, config.clone(), &write, versions(&[(1, 2)])?, 1).await?;
    assert!(
        ctx.extension::<CatalogManager>()?
            .get_table_or_view(&aux)
            .await
            .is_ok()
    );
    assert_eq!(
        rows(&ctx, config.clone(), &target).await?,
        vec![row(&["1", "2", &meta("NULL", "2")])]
    );
    finish(&ctx).await?;
    Ok(())
}

// SinglePipelineSuite:168–216 through the write path: a parquet destination
// is rejected before any auxiliary state is created.
#[tokio::test]
pub(super) async fn a_parquet_destination_is_rejected_before_auxiliary_creation() -> TestResult {
    let (ctx, config) = context()?;
    let target = vec![
        "test_catalog".into(),
        "test_db".into(),
        "target_no_merge".into(),
    ];
    create_target(&ctx, config.clone(), &target, "parquet", target_columns()).await?;
    let args = ChangeArgs::new(
        vec![key("id")?],
        attribute("version"),
        ScdType::Type1,
        None,
        None,
    )?;
    let write = Scd1Write {
        flow_name: "test_catalog.test_db.auto_cdc_flow",
        destination: &target,
        destination_format: None,
        args: &args,
        sequencing_type: &spec::DataType::Int64,
        case: Case::Insensitive,
    };
    let batch = batch(&[(1, "alice", 1)])?;
    let error = ensure_auxiliary_table(&ctx, config.clone(), &write, &flow_schema(&batch)?)
        .await
        .err()
        .ok_or("parquet destination unexpectedly accepted")?;
    let crate::error::SparkError::Structured(error) = error else {
        return Err(format!("expected a structured error, got {error:?}").into());
    };
    assert_eq!(error.condition, "AUTOCDC_TARGET_DOES_NOT_SUPPORT_MERGE");
    assert_eq!(
        error.parameters.get("format").map(String::as_str),
        Some("parquet")
    );
    assert_eq!(
        error.parameters.get("tableName").map(String::as_str),
        Some("`test_catalog`.`test_db`.`target_no_merge`")
    );
    assert!(
        ctx.extension::<CatalogManager>()?
            .get_table_or_view(&auxiliary::table_name(&target))
            .await
            .is_err()
    );
    finish(&ctx).await?;
    Ok(())
}
