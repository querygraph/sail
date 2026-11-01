use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

use datafusion::arrow::datatypes::SchemaRef;
use datafusion::prelude::SessionContext;
use sail_catalog::command::CatalogCommand;
use sail_catalog::error::{CatalogError, CatalogObject};
use sail_catalog::manager::CatalogManager;
use sail_common::spec;
use sail_common_datafusion::catalog::{CLUSTERING_COLUMNS_PROPERTY, PartitionTransform, TableKind};
use sail_common_datafusion::datasource::{DataSourceRegistry, is_lakehouse_format};
use sail_common_datafusion::extension::SessionExtensionAccessor;
use sail_plan::config::PlanConfig;
use sail_plan::error::PlanError;

use super::analysis::{AnalyzedGraph, DUPLICATE_COLUMN_MARK};
use super::execution::{RunEvents, reads_failed_output, run_catalog_command, run_command};
use super::graph::{GraphDefinition, Name, Output, analysis, quoted};
use super::selection::Selection;
use crate::error::{SparkError, SparkResult, StructuredSparkError};
use crate::session::SparkSession;
use crate::spark::connect::{OutputType, pipeline_command as pc};

mod errors;

/// Materialize the selected table metadata before executing any batch flow,
/// matching Spark's DatasetManager. A failed write can leave an empty target.
/// `DatasetManager.constructFullRefreshSet` (:397–430): a table selected for
/// full refresh whose `pipelines.reset.allowed` is false is refreshed instead.
pub(super) fn resets(output: &Output, selection: &Selection) -> bool {
    if !selection.reset.contains(&output.name) {
        return false;
    }
    let Some(pc::define_output::Details::TableDetails(details)) = &output.definition.details else {
        return true;
    };
    !details.table_properties.iter().any(|(key, value)| {
        key.eq_ignore_ascii_case("pipelines.reset.allowed") && value.eq_ignore_ascii_case("false")
    })
}

/// What a flow reports on each attempt, instead of writing.
pub(in crate::pipelines) type DeferredFailures = BTreeMap<Name, StructuredSparkError>;
pub(super) use errors::ambiguous_reference;

pub(super) async fn tables(
    ctx: &SessionContext,
    graph: &GraphDefinition,
    schemas: &BTreeMap<Name, SchemaRef>,
    selection: &Selection,
) -> SparkResult<DeferredFailures> {
    let mut deferred = DeferredFailures::new();
    let manager = ctx.extension::<CatalogManager>()?;
    let config = ctx
        .extension::<SparkSession>()?
        .plan_config_with_overrides(graph.sql_conf.clone())?;
    for output in &graph.outputs {
        if !matches!(
            output.kind,
            OutputType::MaterializedView | OutputType::Table
        ) || output.view.is_some()
            || !selection.runs(&output.name)
        {
            continue;
        }
        let details = match &output.definition.details {
            Some(pc::define_output::Details::TableDetails(details)) => details.clone(),
            _ => pc::define_output::TableDetails::default(),
        };
        let mut properties = super::properties::canonicalize(
            details.table_properties.clone().into_iter().collect(),
        )?;
        // DatasetManager.constructFullRefreshSet (:397–430): a table selected
        // for full refresh whose `pipelines.reset.allowed` is false is
        // refreshed instead.
        let full_refresh = selection.reset.contains(&output.name)
            && if properties
                .get("pipelines.reset.allowed")
                .is_some_and(|value| value.eq_ignore_ascii_case("false"))
            {
                log::info!(
                    "Skipping full refresh on {} because pipelines.reset.allowed was set to false.",
                    quoted(&output.name)
                );
                false
            } else {
                true
            };
        for (key, value) in [
            ("comment", output.definition.comment.as_ref()),
            ("provider", details.format.as_ref()),
        ] {
            if let Some(value) = value {
                if properties
                    .get(key)
                    .is_some_and(|previous| previous != value)
                {
                    return Err(SparkError::invalid(format!(
                        "For dataset {}, {key} '{value}' does not match its reserved table property",
                        quoted(&output.name)
                    )));
                }
                properties.insert(key.to_string(), value.clone());
            }
        }
        if !details.clustering_columns.is_empty() && !details.partition_cols.is_empty() {
            return Err(errors::partition_and_cluster());
        }
        let schema = schemas
            .get(&output.name)
            .ok_or_else(|| SparkError::internal("analyzed output missing"))?
            .as_ref();
        let mut columns = super::schema::table_columns(output, schema)?;
        columns.retain(|column| !column.name.starts_with(DUPLICATE_COLUMN_MARK));
        for column in &details.clustering_columns {
            if !columns.iter().any(|x| x.name.eq_ignore_ascii_case(column)) {
                return Err(errors::missing_cluster_column(column, &output.name));
            }
        }
        if !details.clustering_columns.is_empty() {
            properties.insert(
                CLUSTERING_COLUMNS_PROPERTY.to_string(),
                details.clustering_columns.join(","),
            );
        }
        // Spark creates the table anyway, and the flow reports the column on
        // every attempt.
        let mut details = details;
        if let Some(missing) = details
            .partition_cols
            .iter()
            .find(|name| !columns.iter().any(|x| x.name.eq_ignore_ascii_case(name)))
        {
            deferred.insert(
                output.name.clone(),
                errors::missing_partition_column(
                    missing,
                    &output.name,
                    &columns.iter().map(|x| x.name.clone()).collect::<Vec<_>>(),
                ),
            );
            details.partition_cols.clear();
        }
        let table: spec::ObjectName = output.name.clone().into();
        let existing = match manager.get_table_or_view(&output.name).await {
            Ok(status) => Some(status),
            Err(CatalogError::NotFound(CatalogObject::Table | CatalogObject::View, _)) => None,
            Err(error) => return Err(PlanError::from(error).into()),
        };
        // PreprocessTableCreation: a new file table needs a column outside its partition.
        let format = details.format.as_deref().unwrap_or_default();
        if existing.is_none()
            && !is_lakehouse_format(format)
            && !columns.is_empty()
            && details.partition_cols.len() == columns.len()
        {
            return Err(errors::all_partition_columns());
        }
        if let Some(existing) = existing {
            let TableKind::Table {
                columns: old_columns,
                partition_by,
                properties: old_properties,
                ..
            } = existing.kind
            else {
                return Err(analysis(
                    "EXPECT_TABLE_NOT_VIEW.NO_ALTERNATIVE",
                    quoted(&output.name),
                ));
            };
            let clusters = old_properties
                .iter()
                .find(|(key, _)| key == CLUSTERING_COLUMNS_PROPERTY)
                .map(|(_, value)| value.split(',').map(str::to_string).collect::<Vec<_>>())
                .unwrap_or_default();
            if clusters != details.clustering_columns {
                return Err(errors::cluster_change(
                    &clusters,
                    &details.clustering_columns,
                ));
            }
            if partition_by.len() != details.partition_cols.len()
                || partition_by
                    .iter()
                    .zip(&details.partition_cols)
                    .any(|(old, new)| {
                        old.column != *new
                            || !matches!(old.transform, None | Some(PartitionTransform::Identity))
                    })
            {
                return Err(errors::partition_change(
                    &partition_by,
                    &details.partition_cols,
                ));
            }
            // DatasetManager.materializeTable (:301–318): wipe the rows on a
            // full refresh or for a batch target, then drop the AutoCDC
            // auxiliary state of a fully refreshed target.
            if full_refresh || output.kind != OutputType::Table {
                run_command(
                    ctx,
                    config.clone(),
                    spec::CommandNode::TruncateTable {
                        table: table.clone(),
                    },
                    None,
                )
                .await?;
            }
            if full_refresh {
                drop_auxiliary_state(ctx, config.clone(), &output.name).await?;
            }
            // :324–328: only an incremental streaming refresh merges the
            // existing schema; a full refresh takes the output schema as is.
            if output.kind == OutputType::Table && !full_refresh {
                columns = super::schema::streaming_columns(output, schema, &old_columns)?;
            }
            let mut changes = vec![];
            for column in &columns {
                if let Some(old) = old_columns.iter().find(|old| old.name == column.name) {
                    let old_type: spec::DataType =
                        crate::spark::connect::DataType::try_from(old.data_type.clone())?
                            .try_into()?;
                    if old_type != column.data_type {
                        run_command(
                            ctx,
                            config.clone(),
                            spec::CommandNode::AlterTable {
                                table: table.clone(),
                                if_exists: false,
                                operation: spec::AlterTableOperation::AlterColumnType {
                                    name: spec::ObjectName::bare(column.name.clone()),
                                    data_type: column.data_type.clone(),
                                },
                            },
                            None,
                        )
                        .await?;
                    }
                    // A column is never tightened: its rows may already hold nulls.
                    if column.nullable && !old.nullable {
                        changes.push(spec::ColumnChange::Nullability {
                            path: vec![column.name.clone()],
                            nullable: column.nullable,
                        });
                    }
                    if old.comment != column.comment {
                        changes.push(spec::ColumnChange::Comment {
                            path: vec![column.name.clone()],
                            comment: column.comment.clone(),
                        });
                    }
                } else {
                    changes.push(spec::ColumnChange::Add {
                        path: vec![column.name.clone()],
                        data_type: column.data_type.clone(),
                        nullable: column.nullable,
                        comment: column.comment.clone(),
                        default: None,
                        position: None,
                    });
                }
            }
            for column in &old_columns {
                if !columns.iter().any(|new| new.name == column.name) {
                    changes.push(spec::ColumnChange::Drop {
                        path: vec![column.name.clone()],
                        if_exists: false,
                    });
                }
            }
            if !changes.is_empty() {
                run_command(
                    ctx,
                    config.clone(),
                    spec::CommandNode::AlterTable {
                        table: table.clone(),
                        if_exists: false,
                        operation: spec::AlterTableOperation::AlterColumns { changes },
                    },
                    None,
                )
                .await?;
            }
            if !properties.is_empty() {
                run_command(
                    ctx,
                    config.clone(),
                    spec::CommandNode::AlterTable {
                        table,
                        if_exists: false,
                        operation: spec::AlterTableOperation::SetTableProperties {
                            properties: properties.into_iter().collect(),
                        },
                    },
                    None,
                )
                .await?;
            }
        } else {
            // Spark issues the auxiliary drop for every fully refreshed
            // target, whether or not the target itself exists yet.
            if full_refresh {
                drop_auxiliary_state(ctx, config.clone(), &output.name).await?;
            }
            let format = properties
                .get("provider")
                .cloned()
                .unwrap_or_else(|| config.default_table_file_format.clone());
            ctx.extension::<DataSourceRegistry>()?
                .get_data_source(&format.to_ascii_lowercase())?;
            let definition = spec::TableDefinition {
                external: false,
                columns,
                comment: properties.get("comment").cloned(),
                constraints: vec![],
                location: None,
                file_format: Some(spec::TableFileFormat::General { format }),
                row_format: None,
                partition_by: details
                    .partition_cols
                    .into_iter()
                    .map(|name| {
                        spec::PartitionColumn::Expression(spec::Expr::UnresolvedAttribute {
                            name: spec::ObjectName::bare(name),
                            plan_id: None,
                            is_metadata_column: false,
                        })
                    })
                    .collect(),
                sort_by: vec![],
                bucket_by: None,
                cluster_by: vec![],
                mode: spec::CreateTableMode::Create,
                options: vec![],
                properties: properties.into_iter().collect(),
            };
            run_command(
                ctx,
                config.clone(),
                spec::CommandNode::CreateTable { table, definition },
                None,
            )
            .await?;
        }
    }
    Ok(deferred)
}

/// `verifyAutoGeneratedAliasesNotExists`: a persisted view stores its column
/// names, so an expression has to name its column.
fn check_view_aliases(name: &Name, query: &spec::QueryPlan) -> SparkResult<()> {
    let expressions = match &query.node {
        spec::QueryNode::Project { expressions, .. } => expressions,
        spec::QueryNode::Sort { input, .. } | spec::QueryNode::Filter { input, .. } => {
            return check_view_aliases(name, input);
        }
        _ => return Ok(()),
    };
    let named = |expression: &spec::Expr| {
        matches!(
            expression,
            spec::Expr::Alias { .. }
                | spec::Expr::UnresolvedAttribute { .. }
                | spec::Expr::UnresolvedStar { .. }
        )
    };
    if expressions.iter().all(named) {
        return Ok(());
    }
    let view = quoted(name);
    Err(Box::new(StructuredSparkError {
        class: "org.apache.spark.sql.AnalysisException".into(),
        condition: "CREATE_PERMANENT_VIEW_WITHOUT_ALIAS".into(),
        sql_state: "0A000".into(),
        message: format!(
            "Not allowed to create the permanent view {view} without explicitly assigning an \
             alias for an expression."
        ),
        parameters: std::collections::HashMap::from([("name".into(), view)]),
    })
    .into())
}

/// Publish every persisted view from its analyzed definition, regardless of
/// table refresh selection. An excluded upstream table need not exist yet.
pub(super) async fn views(
    ctx: &SessionContext,
    graph: &GraphDefinition,
    analyzed: &AnalyzedGraph,
    events: &RunEvents,
) -> SparkResult<()> {
    // A view that cannot be published is a flow failure, not a run failure.
    let mut failed = BTreeSet::new();
    for name in &analyzed.order {
        // An Iggy view became a source and left the graph.
        let Some(output) = graph.outputs.iter().find(|output| &output.name == name) else {
            continue;
        };
        if output.view.is_none() {
            continue;
        }
        let display = name.join(".");
        if reads_failed_output(analyzed, name, &failed) {
            let _ = events.send(format!(
                "Flow '{display}' has been processed by a previous iteration and will not be \
                 rerun."
            ));
            failed.insert(name.clone());
            continue;
        }
        let flow = graph.flows.iter().find(|flow| &flow.target == name);
        if let Some(Err(error)) = flow.map(|flow| check_view_aliases(name, &flow.query)) {
            let _ = events.send(format!("Flow '{display}' has FAILED.\nError: {error}"));
            failed.insert(name.clone());
            continue;
        }
        let mut options = analyzed
            .publications
            .get(name)
            .cloned()
            .ok_or_else(|| SparkError::internal("analyzed view publication missing"))?;
        options.replace = true;
        options.if_not_exists = false;
        run_catalog_command(
            ctx,
            CatalogCommand::CreateView {
                view: name.clone(),
                options,
            },
        )
        .await?;
    }
    Ok(())
}

/// `DROP TABLE IF EXISTS` on the AutoCDC auxiliary table of a fully refreshed
/// target (`DatasetManager.scala:306–318`). Spark drops rather than truncates
/// because the auxiliary table is an internal state store outside the
/// dataflow graph: a full refresh recreates it against the new target schema
/// instead of carrying the previous generation's layout forward. The drop is
/// unconditional and a no-op where no auxiliary table exists.
async fn drop_auxiliary_state(
    ctx: &SessionContext,
    config: Arc<PlanConfig>,
    target: &Name,
) -> SparkResult<()> {
    run_command(
        ctx,
        config,
        spec::CommandNode::DropTable {
            table: super::autocdc::auxiliary::table_name(target).into(),
            if_exists: true,
            purge: false,
        },
        None,
    )
    .await
}

#[cfg(test)]
mod tests;
