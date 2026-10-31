//! The SCD1 write component: `AutoCdcMergeWriteBase.createAuxiliaryTableIfNotExists`
//! (`graph/FlowExecution.scala:398–440`) and `Scd1ForeachBatchHandler.execute`
//! (`autocdc/Scd1ForeachBatchHandler.scala:39–72`), Spark 4.2.1 at `617618396`,
//! over the pure components: the auxiliary state contract, microbatch
//! validation, reconciliation and the two merges. Nothing calls this from
//! the pipeline runner: tests invoke it per microbatch, and activation
//! (the streaming runner, checkpoints, `execution::execute`) is a later,
//! user-notified step.
#![cfg_attr(
    not(test),
    expect(dead_code, reason = "activated by the streaming runner unit")
)]

use std::sync::Arc;

use datafusion::arrow::array::RecordBatch;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::datasource::{MemTable, provider_as_source};
use datafusion::logical_expr::{Expr, LogicalPlan, LogicalPlanBuilder};
use datafusion::prelude::{SessionContext, col};
use sail_catalog::error::{CatalogError, CatalogObject};
use sail_catalog::manager::CatalogManager;
use sail_catalog::provider::CreateTemporaryViewOptions;
use sail_common::spec;
use sail_common_datafusion::catalog::TableKind;
use sail_common_datafusion::extension::SessionExtensionAccessor;
use sail_common_datafusion::rename::logical_plan::rename_logical_plan;
use sail_plan::config::PlanConfig;
use sail_plan::error::PlanError;
use sail_plan::resolver::PlanResolver;

use super::auxiliary::{self, KEY_COLUMN_NAMES_PROPERTY, SCD_TYPE_PROPERTY};
use super::schema::{Case, METADATA_COLUMN, RESERVED_PREFIX};
use super::{ChangeArgs, merge, microbatch, reconcile};
use crate::error::{SparkError, SparkResult};
use crate::pipelines::execution::run_command;
use crate::pipelines::graph::{Name, quoted};

/// Helper columns the handler evaluates once over the batch, as Spark
/// evaluates `changeArgs.sequencing` and the delete condition as `Column`s.
/// Reserved-prefix names cannot collide with user columns.
const SEQUENCING_COLUMN: &str = "__spark_autocdc_sequence";
const DELETE_COLUMN: &str = "__spark_autocdc_delete";
/// The temporary views the handler registers (replaced per microbatch).
const BATCH_VIEW: &str = "__spark_autocdc_batch";
const RECONCILED_VIEW: &str = "__spark_autocdc_reconciled";

/// A flow's write-time configuration.
pub(super) struct Scd1Write<'a> {
    pub flow_name: &'a str,
    pub destination: &'a Name,
    /// The destination's declared format, if any (`Table.format`).
    pub destination_format: Option<&'a str>,
    pub args: &'a ChangeArgs,
    /// The resolved sequencing type (`AutoCdcMergeFlow.sequencingType`).
    pub sequencing_type: &'a spec::DataType,
    pub case: Case,
}

/// `createAuxiliaryTableIfNotExists`: creates the auxiliary table with the
/// A3 schema and the two reserved properties when it does not exist, after
/// the merge-support and key-drift checks; an existing table is left as it
/// is. Returns the auxiliary identifier.
pub(super) async fn ensure_auxiliary_table(
    ctx: &SessionContext,
    config: Arc<PlanConfig>,
    write: &Scd1Write<'_>,
    flow_schema: &spec::Fields,
) -> SparkResult<Name> {
    let manager = ctx.extension::<CatalogManager>()?;
    let destination = manager
        .get_table_or_view(write.destination)
        .await
        .map_err(PlanError::from)?;
    let TableKind::Table {
        format, properties, ..
    } = &destination.kind
    else {
        return Err(SparkError::internal(format!(
            "AutoCDC destination {} is not a table",
            quoted(write.destination)
        )));
    };
    // The auxiliary table is committed the same way as its destination.
    let enrolled_properties: Vec<(String, String)> = properties
        .iter()
        .filter(|(key, _)| key == IN_COMMIT_TIMESTAMPS_PROPERTY)
        .cloned()
        .collect();
    let provider = properties
        .iter()
        .find(|(key, _)| key == "provider")
        .map(|(_, value)| value.as_str())
        .or(Some(format.as_str()));
    let supports_row_level = ctx
        .extension::<sail_common_datafusion::datasource::DataSourceRegistry>()?
        .get_lake_source(&format.to_ascii_lowercase())
        .is_ok();
    auxiliary::require_merge_support(
        &quoted(write.destination),
        write.destination_format,
        provider,
        supports_row_level,
    )?;
    let expected = auxiliary::expected_key_fields(
        &write.args.keys,
        flow_schema,
        write.case,
        write.flow_name,
        &quoted(write.destination),
    )?;
    let aux_name = auxiliary::table_name(write.destination);
    match manager.get_table_or_view(&aux_name).await {
        Ok(existing) => {
            let TableKind::Table {
                columns,
                properties,
                ..
            } = existing.kind
            else {
                return Err(SparkError::internal(format!(
                    "AutoCDC auxiliary {} is not a table",
                    quoted(&aux_name)
                )));
            };
            let mut existing_columns = Vec::with_capacity(columns.len());
            for column in &columns {
                let data_type: spec::DataType =
                    crate::spark::connect::DataType::try_from(column.data_type.clone())?
                        .try_into()?;
                existing_columns.push(spec::Field {
                    name: column.name.clone(),
                    data_type,
                    nullable: column.nullable,
                    metadata: vec![],
                });
            }
            auxiliary::validate_no_key_drift(
                write.flow_name,
                &aux_name.join("."),
                &expected,
                &existing_columns,
                &properties,
                write.case,
            )?;
        }
        Err(CatalogError::NotFound(CatalogObject::Table | CatalogObject::View, _)) => {
            let columns = auxiliary::auxiliary_schema(&expected, write.sequencing_type)
                .into_iter()
                .map(|field| spec::TableColumnDefinition {
                    name: field.name,
                    data_type: field.data_type,
                    nullable: field.nullable,
                    comment: None,
                    default: None,
                    generated_always_as: None,
                    identity: None,
                })
                .collect();
            let key_names: Vec<String> = expected.iter().map(|key| key.name.clone()).collect();
            let definition = spec::TableDefinition {
                external: false,
                columns,
                comment: None,
                constraints: vec![],
                location: None,
                file_format: write.destination_format.map(|format| {
                    spec::TableFileFormat::General {
                        format: format.to_string(),
                    }
                }),
                row_format: None,
                partition_by: vec![],
                sort_by: vec![],
                bucket_by: None,
                cluster_by: vec![],
                mode: spec::CreateTableMode::Create,
                options: vec![],
                properties: [
                    (
                        SCD_TYPE_PROPERTY.to_string(),
                        write.args.scd_type.label().to_string(),
                    ),
                    (
                        KEY_COLUMN_NAMES_PROPERTY.to_string(),
                        auxiliary::serialize_key_column_names(&key_names),
                    ),
                ]
                .into_iter()
                .chain(enrolled_properties)
                .collect(),
            };
            run_command(
                ctx,
                config,
                spec::CommandNode::CreateTable {
                    table: aux_name.clone().into(),
                    definition,
                },
                None,
            )
            .await?;
        }
        Err(error) => return Err(PlanError::from(error).into()),
    }
    Ok(aux_name)
}

/// Resolve a `spec` expression over the batch by evaluating it as a column
/// of a projection over the batch view, with the batch columns kept first.
async fn with_evaluated_columns(
    ctx: &SessionContext,
    config: Arc<PlanConfig>,
    batch: LogicalPlan,
    evaluated: Vec<(String, spec::Expr)>,
) -> SparkResult<LogicalPlan> {
    let manager = ctx.extension::<CatalogManager>()?;
    manager
        .create_temporary_view(
            BATCH_VIEW,
            CreateTemporaryViewOptions {
                input: Arc::new(batch),
                columns: vec![],
                if_not_exists: false,
                replace: true,
                comment: None,
                properties: vec![],
                source: None,
            },
        )
        .await
        .map_err(PlanError::from)?;
    let mut expressions = vec![spec::Expr::UnresolvedStar {
        target: None,
        plan_id: None,
        wildcard_options: Default::default(),
    }];
    for (name, expr) in evaluated {
        expressions.push(spec::Expr::Alias {
            expr: Box::new(expr),
            name: vec![name.into()],
            metadata: None,
        });
    }
    let query = spec::QueryPlan::new(spec::QueryNode::Project {
        input: Some(Box::new(spec::QueryPlan::new(spec::QueryNode::Read {
            read_type: spec::ReadType::NamedTable(Box::new(spec::ReadNamedTable {
                name: spec::ObjectName::bare(BATCH_VIEW),
                temporal: None,
                sample: None,
                options: vec![],
            })),
            is_streaming: false,
        }))),
        expressions,
    });
    let named = PlanResolver::new(ctx, config)
        .resolve_named_plan(spec::Plan::Query(query))
        .await?;
    Ok(match named.fields {
        Some(fields) => rename_logical_plan(named.plan, &fields)?,
        None => named.plan,
    })
}

/// Read a catalog table as a logical plan with user-facing column names.
pub(super) async fn read_table(
    ctx: &SessionContext,
    config: Arc<PlanConfig>,
    name: &Name,
) -> SparkResult<LogicalPlan> {
    let query = spec::QueryPlan::new(spec::QueryNode::Read {
        read_type: spec::ReadType::NamedTable(Box::new(spec::ReadNamedTable {
            name: name.clone().into(),
            temporal: None,
            sample: None,
            options: vec![],
        })),
        is_streaming: false,
    });
    let named = PlanResolver::new(ctx, config)
        .resolve_named_plan(spec::Plan::Query(query))
        .await?;
    Ok(match named.fields {
        Some(fields) => rename_logical_plan(named.plan, &fields)?,
        None => named.plan,
    })
}

/// Execute `plan` once and return a scan over the collected rows, so every
/// later reader sees exactly these rows. The sealed batches are the
/// prepared artifact the control-store design binds receipts to; archiving
/// them durably is the sealed-input unit.
/// Ratified staging needs a timestamp inside each commit.
pub(super) const IN_COMMIT_TIMESTAMPS_PROPERTY: &str = "delta.enableInCommitTimestamps";

/// The reconciled rows of one microbatch, sealed once.
pub(super) struct PreparedBatch {
    pub schema: SchemaRef,
    pub batches: Vec<RecordBatch>,
    pub columns: Vec<String>,
}

/// `Scd1ForeachBatchHandler.execute`: validate the original rows, reconcile
/// against the auxiliary table, publish the reconciled rows as a temporary
/// view, merge onto the auxiliary table, then merge onto the destination.
pub(super) async fn prepare_microbatch(
    ctx: &SessionContext,
    config: Arc<PlanConfig>,
    write: &Scd1Write<'_>,
    auxiliary_table: &Name,
    batch: LogicalPlan,
    batch_id: i64,
) -> SparkResult<PreparedBatch> {
    let mut evaluated = vec![(SEQUENCING_COLUMN.to_string(), write.args.sequencing.clone())];
    if let Some(delete) = &write.args.delete_condition {
        evaluated.push((DELETE_COLUMN.to_string(), delete.clone()));
    }
    let batch = with_evaluated_columns(ctx, config.clone(), batch, evaluated).await?;
    let resolver = write.case.resolver();
    let names: Vec<String> = batch
        .schema()
        .fields()
        .iter()
        .map(|field| field.name().clone())
        .collect();
    let key_indices = write
        .args
        .keys
        .iter()
        .map(|key| {
            names
                .iter()
                .position(|name| resolver(name, key.name()))
                .map(|index| (key.clone(), index))
                .ok_or_else(|| {
                    SparkError::internal(format!(
                        "AutoCDC key column '{}' is missing from the microbatch",
                        key.name()
                    ))
                })
        })
        .collect::<SparkResult<Vec<_>>>()?;
    let sequencing: Expr = col(SEQUENCING_COLUMN);
    let delete_condition: Option<Expr> = write
        .args
        .delete_condition
        .as_ref()
        .map(|_| col(DELETE_COLUMN));
    microbatch::validate(
        ctx,
        microbatch::Microbatch {
            batch: batch.clone(),
            sequencing: sequencing.clone(),
            keys: &key_indices,
            target: write.destination,
            batch_id,
        },
    )
    .await?;
    let auxiliary = read_table(ctx, config.clone(), auxiliary_table).await?;
    // `resolvedSequencingType`: the type the sequencing expression takes over
    // the batch, which the evaluated helper column carries.
    let sequencing_type = batch
        .schema()
        .fields()
        .iter()
        .find(|field| field.name() == SEQUENCING_COLUMN)
        .map(|field| field.data_type().clone())
        .ok_or_else(|| SparkError::internal("AutoCDC sequencing column was not evaluated"))?;
    let reconciled = reconcile::reconcile(
        batch,
        auxiliary,
        &write.args.keys,
        write.args.column_selection.as_ref(),
        sequencing,
        delete_condition,
        &sequencing_type,
        write.case,
    )?;
    // The helper columns are not target columns.
    let kept: Vec<Expr> = reconciled
        .schema()
        .fields()
        .iter()
        .enumerate()
        .filter(|(_, field)| {
            !(field.name().starts_with(RESERVED_PREFIX) && field.name() != METADATA_COLUMN)
        })
        .map(|(index, _)| {
            let (qualifier, field) = reconciled.schema().qualified_field(index);
            Expr::Column(datafusion::common::Column::from((qualifier, field)))
        })
        .collect();
    let reconciled = LogicalPlanBuilder::from(reconciled)
        .project(kept)?
        .build()?;
    let columns: Vec<String> = reconciled
        .schema()
        .fields()
        .iter()
        .map(|field| field.name().clone())
        .collect();
    // Seal the reconciled rows once: both merges and every retry consume the
    // same winners (CDC-CONTROL-STORE-DESIGN.md, two-table protocol). A lazy
    // plan re-executed per merge could keep a different tied `max_by` row.
    let schema = Arc::new(reconciled.schema().as_arrow().clone());
    let batches = ctx
        .execute_logical_plan(reconciled)
        .await?
        .collect()
        .await?;
    Ok(PreparedBatch {
        schema,
        batches,
        columns,
    })
}

/// Publish the sealed rows as the view both merges read.
pub(super) async fn publish_prepared(
    ctx: &SessionContext,
    prepared: &PreparedBatch,
) -> SparkResult<()> {
    let table = MemTable::try_new(prepared.schema.clone(), vec![prepared.batches.clone()])?;
    let sealed =
        LogicalPlanBuilder::scan(RECONCILED_VIEW, provider_as_source(Arc::new(table)), None)?
            .build()?;
    ctx.extension::<CatalogManager>()?
        .create_temporary_view(
            RECONCILED_VIEW,
            CreateTemporaryViewOptions {
                input: Arc::new(sealed),
                columns: vec![],
                if_not_exists: false,
                replace: true,
                comment: None,
                properties: vec![],
                source: None,
            },
        )
        .await
        .map_err(PlanError::from)?;
    Ok(())
}

pub(super) async fn merge_auxiliary(
    ctx: &SessionContext,
    config: Arc<PlanConfig>,
    write: &Scd1Write<'_>,
    auxiliary_table: &Name,
    fenced_write: Option<spec::FencedWrite>,
) -> SparkResult<()> {
    let mut merge = merge::auxiliary_merge(auxiliary_table, RECONCILED_VIEW, &write.args.keys);
    merge.fenced_write = fenced_write;
    run_command(ctx, config, spec::CommandNode::MergeInto(merge), None).await
}

pub(super) async fn merge_destination(
    ctx: &SessionContext,
    config: Arc<PlanConfig>,
    write: &Scd1Write<'_>,
    prepared: &PreparedBatch,
    fenced_write: Option<spec::FencedWrite>,
) -> SparkResult<()> {
    let mut merge = merge::target_merge(
        write.destination,
        RECONCILED_VIEW,
        &write.args.keys,
        &prepared.columns,
        write.case,
    );
    merge.fenced_write = fenced_write;
    run_command(ctx, config, spec::CommandNode::MergeInto(merge), None).await
}

pub(super) async fn handle_microbatch(
    ctx: &SessionContext,
    config: Arc<PlanConfig>,
    write: &Scd1Write<'_>,
    auxiliary_table: &Name,
    batch: LogicalPlan,
    batch_id: i64,
) -> SparkResult<()> {
    let prepared =
        prepare_microbatch(ctx, config.clone(), write, auxiliary_table, batch, batch_id).await?;
    publish_prepared(ctx, &prepared).await?;
    merge_auxiliary(ctx, config.clone(), write, auxiliary_table, None).await?;
    merge_destination(ctx, config, write, &prepared, None).await
}
