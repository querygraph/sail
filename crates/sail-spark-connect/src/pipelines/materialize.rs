use std::collections::BTreeMap;

use datafusion::arrow::datatypes::SchemaRef;
use datafusion::prelude::SessionContext;
use sail_catalog::command::CatalogCommand;
use sail_catalog::error::{CatalogError, CatalogObject};
use sail_catalog::manager::CatalogManager;
use sail_common::spec;
use sail_common_datafusion::catalog::{PartitionTransform, TableKind};
use sail_common_datafusion::datasource::DataSourceRegistry;
use sail_common_datafusion::extension::SessionExtensionAccessor;
use sail_plan::error::PlanError;

use super::analysis::AnalyzedGraph;
use super::execution::{run_catalog_command, run_command};
use super::graph::{GraphDefinition, Name, analysis, quoted};
use super::selection::Selection;
use crate::error::{SparkError, SparkResult};
use crate::session::SparkSession;
use crate::spark::connect::{OutputType, pipeline_command as pc};

mod errors;

/// Materialize the selected table metadata before executing any batch flow,
/// matching Spark's DatasetManager. A failed write can leave an empty target.
pub(super) async fn tables(
    ctx: &SessionContext,
    graph: &GraphDefinition,
    schemas: &BTreeMap<Name, SchemaRef>,
    selection: &Selection,
) -> SparkResult<()> {
    // Metadata-only callers may materialize streaming targets, but full reset
    // also deletes AutoCDC state in Spark. Keep that effect unavailable until
    // its ownership path is qualified, including when the target is absent.
    for output in &graph.outputs {
        if output.kind == OutputType::Table && selection.reset.contains(&output.name) {
            let properties = match &output.definition.details {
                Some(pc::define_output::Details::TableDetails(details)) => {
                    super::properties::canonicalize(
                        details.table_properties.clone().into_iter().collect(),
                    )?
                }
                _ => Default::default(),
            };
            if !properties
                .get("pipelines.reset.allowed")
                .is_some_and(|value| value.eq_ignore_ascii_case("false"))
            {
                return Err(SparkError::unsupported(
                    "streaming table metadata full refresh",
                ));
            }
        }
    }
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
        let mut properties =
            super::properties::canonicalize(details.table_properties.into_iter().collect())?;
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
        if !details.clustering_columns.is_empty() {
            if !details.partition_cols.is_empty() {
                return Err(errors::partition_and_cluster());
            }
            return Err(SparkError::unsupported("pipeline table clustering"));
        }
        let schema = schemas
            .get(&output.name)
            .ok_or_else(|| SparkError::internal("analyzed output missing"))?
            .as_ref();
        let mut columns = super::schema::table_columns(output, schema)?;
        let table: spec::ObjectName = output.name.clone().into();
        let existing = match manager.get_table_or_view(&output.name).await {
            Ok(status) => Some(status),
            Err(CatalogError::NotFound(CatalogObject::Table | CatalogObject::View, _)) => None,
            Err(error) => return Err(PlanError::from(error).into()),
        };
        if let Some(existing) = existing {
            let TableKind::Table {
                columns: old_columns,
                partition_by,
                ..
            } = existing.kind
            else {
                return Err(analysis(
                    "EXPECT_TABLE_NOT_VIEW.NO_ALTERNATIVE",
                    quoted(&output.name),
                ));
            };
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
            if output.kind == OutputType::Table {
                columns = super::schema::streaming_columns(output, schema, &old_columns)?;
            } else {
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
                    if old.nullable != column.nullable {
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
    Ok(())
}

/// Publish every persisted view from its analyzed definition, regardless of
/// table refresh selection. An excluded upstream table need not exist yet.
pub(super) async fn views(
    ctx: &SessionContext,
    graph: &GraphDefinition,
    analyzed: &AnalyzedGraph,
) -> SparkResult<()> {
    for name in &analyzed.order {
        let output = graph
            .outputs
            .iter()
            .find(|output| &output.name == name)
            .ok_or_else(|| SparkError::internal("analyzed output missing"))?;
        if output.view.is_none() {
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

#[cfg(test)]
mod tests;
