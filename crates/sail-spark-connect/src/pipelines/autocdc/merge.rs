//! The two SCD1 merges of `Scd1BatchProcessor` (`autocdc/Scd1BatchProcessor.scala`
//! `:250–380`, Spark 4.2.1 at `617618396`) as `spec::MergeInto` commands over
//! a reconciled microbatch that the caller has registered as a temporary
//! view. Spark issues them with `Dataset.mergeInto`; Sail runs them through
//! its MERGE resolver, which requires a target with row-level operations
//! (the A3 merge-support check). Nothing here executes.

use sail_common::spec;

use super::UnqualifiedColumnName;
use super::schema::{Case, DELETE_SEQUENCE_FIELD, METADATA_COLUMN, UPSERT_SEQUENCE_FIELD};

/// Alias of the reduced microbatch in the auxiliary merge (`reducedMicrobatch`).
pub(super) const REDUCED_MICROBATCH: &str = "reducedMicrobatch";
/// Alias of the microbatch in the target merge (`microbatch`).
pub(super) const MICROBATCH: &str = "microbatch";
/// Alias Sail gives the auxiliary table in its merge; Spark names it by its
/// quoted identifier, which Sail's resolver takes as an alias here.
pub(super) const AUXILIARY_TABLE: &str = "auxiliaryTable";
/// Alias Sail gives the destination table in its merge.
pub(super) const DESTINATION: &str = "destination";

fn attr(parts: &[&str]) -> spec::Expr {
    spec::Expr::UnresolvedAttribute {
        name: spec::ObjectName::from(parts.iter().map(|p| p.to_string()).collect::<Vec<_>>()),
        plan_id: None,
        is_metadata_column: false,
    }
}

fn func(name: &str, arguments: Vec<spec::Expr>) -> spec::Expr {
    spec::Expr::UnresolvedFunction(spec::UnresolvedFunction {
        function_name: spec::ObjectName::bare(name),
        arguments,
        named_arguments: vec![],
        is_distinct: false,
        is_user_defined_function: false,
        is_internal: None,
        ignore_nulls: None,
        filter: None,
        order_by: None,
    })
}

fn and(left: spec::Expr, right: spec::Expr) -> spec::Expr {
    func("and", vec![left, right])
}

fn or(left: spec::Expr, right: spec::Expr) -> spec::Expr {
    func("or", vec![left, right])
}

fn not(expr: spec::Expr) -> spec::Expr {
    func("not", vec![expr])
}

fn gt(left: spec::Expr, right: spec::Expr) -> spec::Expr {
    func(">", vec![left, right])
}

fn ge(left: spec::Expr, right: spec::Expr) -> spec::Expr {
    func(">=", vec![left, right])
}

fn eq(left: spec::Expr, right: spec::Expr) -> spec::Expr {
    func("=", vec![left, right])
}

fn is_null(expr: spec::Expr) -> spec::Expr {
    spec::Expr::IsNull(Box::new(expr))
}

fn is_not_null(expr: spec::Expr) -> spec::Expr {
    spec::Expr::IsNotNull(Box::new(expr))
}

fn condition(expr: spec::Expr) -> Option<spec::ExprWithSource> {
    Some(spec::ExprWithSource { expr, source: None })
}

/// `keysMatch`: every key equal between the two aliases.
fn keys_match(left: &str, right: &str, keys: &[UnqualifiedColumnName]) -> spec::Expr {
    keys.iter()
        .map(|key| eq(attr(&[left, key.name()]), attr(&[right, key.name()])))
        .reduce(and)
        .unwrap_or(spec::Expr::Literal(spec::Literal::Boolean {
            value: Some(false),
        }))
}

/// `mergeMicrobatchOntoAuxiliaryTable`: the view reduced to keys and the
/// metadata column, merged into the auxiliary table on the keys; a matched
/// tombstone advances on a strictly newer delete, is removed by an upsert at
/// or above its sequence, and a new delete event inserts a tombstone.
pub(super) fn auxiliary_merge(
    auxiliary_table: &[String],
    reconciled_view: &str,
    keys: &[UnqualifiedColumnName],
) -> spec::MergeInto {
    let reduced = spec::QueryPlan::new(spec::QueryNode::Project {
        input: Some(Box::new(spec::QueryPlan::new(spec::QueryNode::Read {
            read_type: spec::ReadType::NamedTable(Box::new(spec::ReadNamedTable {
                name: spec::ObjectName::bare(reconciled_view),
                temporal: None,
                sample: None,
                options: vec![],
            })),
            is_streaming: false,
        }))),
        expressions: keys
            .iter()
            .map(|key| attr(&[key.name()]))
            .chain(std::iter::once(attr(&[METADATA_COLUMN])))
            .collect(),
    });
    let incoming_metadata = attr(&[REDUCED_MICROBATCH, METADATA_COLUMN]);
    let incoming_delete = attr(&[REDUCED_MICROBATCH, METADATA_COLUMN, DELETE_SEQUENCE_FIELD]);
    let incoming_upsert = attr(&[REDUCED_MICROBATCH, METADATA_COLUMN, UPSERT_SEQUENCE_FIELD]);
    let aux_delete = attr(&[AUXILIARY_TABLE, METADATA_COLUMN, DELETE_SEQUENCE_FIELD]);
    let incoming_is_delete_event = and(
        is_not_null(incoming_delete.clone()),
        or(
            is_null(incoming_upsert.clone()),
            gt(incoming_delete.clone(), incoming_upsert.clone()),
        ),
    );
    spec::MergeInto {
        target: spec::ObjectName::from(auxiliary_table.to_vec()),
        target_alias: Some(AUXILIARY_TABLE.into()),
        source: spec::MergeSource::Query {
            input: Box::new(reduced),
            alias: Some(REDUCED_MICROBATCH.into()),
        },
        on_condition: spec::ExprWithSource {
            expr: keys_match(REDUCED_MICROBATCH, AUXILIARY_TABLE, keys),
            source: None,
        },
        clauses: vec![
            spec::MergeClause::Matched(spec::MergeMatchedClause {
                condition: condition(and(
                    incoming_is_delete_event.clone(),
                    gt(incoming_delete, aux_delete.clone()),
                )),
                action: spec::MergeMatchedAction::UpdateSet(vec![(
                    spec::ObjectName::from(vec![
                        AUXILIARY_TABLE.to_string(),
                        METADATA_COLUMN.to_string(),
                    ]),
                    incoming_metadata,
                )]),
            }),
            spec::MergeClause::Matched(spec::MergeMatchedClause {
                condition: condition(and(
                    not(incoming_is_delete_event.clone()),
                    ge(incoming_upsert, aux_delete),
                )),
                action: spec::MergeMatchedAction::Delete,
            }),
            spec::MergeClause::NotMatchedByTarget(spec::MergeNotMatchedByTargetClause {
                condition: condition(incoming_is_delete_event),
                action: spec::MergeNotMatchedByTargetAction::InsertAll,
            }),
        ],
        with_schema_evolution: false,
        fenced_write: None,
    }
}

/// A fenced truncate: an empty source matches nothing, so every row of the
/// table is deleted. A plain truncate cannot carry a fence.
pub(super) fn reset_merge(table: &[String], empty_view: &str) -> spec::MergeInto {
    empty_source_merge(table, empty_view, vec![delete_unmatched(None)], false)
}

/// A fenced schema change: nothing is inserted, and the commit adds the
/// columns that the empty source has and the table lacks.
pub(super) fn evolve_merge(table: &[String], empty_view: &str) -> spec::MergeInto {
    let insert = spec::MergeClause::NotMatchedByTarget(spec::MergeNotMatchedByTargetClause {
        condition: None,
        action: spec::MergeNotMatchedByTargetAction::InsertAll,
    });
    empty_source_merge(table, empty_view, vec![insert], true)
}

/// A fenced effect that changes nothing, for a step that must still have a
/// receipt.
pub(super) fn noop_merge(table: &[String], empty_view: &str) -> spec::MergeInto {
    let never = spec::Expr::Literal(spec::Literal::Boolean { value: Some(false) });
    empty_source_merge(
        table,
        empty_view,
        vec![delete_unmatched(condition(never))],
        false,
    )
}

fn delete_unmatched(condition: Option<spec::ExprWithSource>) -> spec::MergeClause {
    spec::MergeClause::NotMatchedBySource(spec::MergeNotMatchedBySourceClause {
        condition,
        action: spec::MergeNotMatchedBySourceAction::Delete,
    })
}

fn empty_source_merge(
    table: &[String],
    empty_view: &str,
    clauses: Vec<spec::MergeClause>,
    with_schema_evolution: bool,
) -> spec::MergeInto {
    spec::MergeInto {
        target: spec::ObjectName::from(table.to_vec()),
        target_alias: Some(DESTINATION.into()),
        source: spec::MergeSource::Query {
            input: Box::new(spec::QueryPlan::new(spec::QueryNode::Read {
                read_type: spec::ReadType::NamedTable(Box::new(spec::ReadNamedTable {
                    name: spec::ObjectName::bare(empty_view),
                    temporal: None,
                    sample: None,
                    options: vec![],
                })),
                is_streaming: false,
            })),
            alias: Some(MICROBATCH.into()),
        },
        on_condition: spec::ExprWithSource {
            expr: spec::Expr::Literal(spec::Literal::Boolean { value: Some(false) }),
            source: None,
        },
        clauses,
        with_schema_evolution,
        fenced_write: None,
    }
}

/// `mergeMicrobatchOntoTarget`: the reconciled view merged into the
/// destination on the keys; a delete newer than the target's upsert deletes,
/// an upsert at or above it updates every non-key column, and an unmatched
/// non-delete row inserts every column. `columns` are the reconciled view's
/// columns in order, including the metadata column.
pub(super) fn target_merge(
    destination: &[String],
    reconciled_view: &str,
    keys: &[UnqualifiedColumnName],
    columns: &[String],
    case: Case,
) -> spec::MergeInto {
    let resolver = case.resolver();
    let incoming_delete = attr(&[MICROBATCH, METADATA_COLUMN, DELETE_SEQUENCE_FIELD]);
    let incoming_upsert = attr(&[MICROBATCH, METADATA_COLUMN, UPSERT_SEQUENCE_FIELD]);
    let destination_upsert = attr(&[DESTINATION, METADATA_COLUMN, UPSERT_SEQUENCE_FIELD]);
    let incoming_wins_upsert = and(
        is_not_null(incoming_upsert.clone()),
        ge(incoming_upsert, destination_upsert.clone()),
    );
    let incoming_wins_delete = and(
        is_not_null(incoming_delete.clone()),
        gt(incoming_delete.clone(), destination_upsert),
    );
    let assignment = |column: &String| {
        (
            spec::ObjectName::from(vec![DESTINATION.to_string(), column.clone()]),
            attr(&[MICROBATCH, column]),
        )
    };
    let updates: Vec<(spec::ObjectName, spec::Expr)> = columns
        .iter()
        .filter(|column| !keys.iter().any(|key| resolver(key.name(), column)))
        .map(assignment)
        .collect();
    let (insert_columns, insert_values): (Vec<_>, Vec<_>) = columns.iter().map(assignment).unzip();
    spec::MergeInto {
        target: spec::ObjectName::from(destination.to_vec()),
        target_alias: Some(DESTINATION.into()),
        source: spec::MergeSource::Table {
            name: spec::ObjectName::bare(reconciled_view),
            alias: Some(MICROBATCH.into()),
        },
        on_condition: spec::ExprWithSource {
            expr: keys_match(MICROBATCH, DESTINATION, keys),
            source: None,
        },
        clauses: vec![
            spec::MergeClause::Matched(spec::MergeMatchedClause {
                condition: condition(incoming_wins_delete),
                action: spec::MergeMatchedAction::Delete,
            }),
            spec::MergeClause::Matched(spec::MergeMatchedClause {
                condition: condition(incoming_wins_upsert),
                action: spec::MergeMatchedAction::UpdateSet(updates),
            }),
            spec::MergeClause::NotMatchedByTarget(spec::MergeNotMatchedByTargetClause {
                condition: condition(is_null(incoming_delete)),
                action: spec::MergeNotMatchedByTargetAction::InsertColumns {
                    columns: insert_columns,
                    values: insert_values,
                },
            }),
        ],
        with_schema_evolution: false,
        fenced_write: None,
    }
}
