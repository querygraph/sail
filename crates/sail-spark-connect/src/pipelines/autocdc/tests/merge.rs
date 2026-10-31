//! Structure of the two SCD1 merges against `Scd1BatchProcessor.scala:250–380`:
//! sources, aliases, join keys, clause order, conditions and assignments.
//! Execution against a row-level target belongs to the activation unit.

use sail_common::spec;

use super::super::UnqualifiedColumnName;
use super::super::merge::*;
use super::super::schema::Case;

type TestResult = Result<(), Box<dyn std::error::Error>>;

fn keys(names: &[&str]) -> Result<Vec<UnqualifiedColumnName>, Box<dyn std::error::Error>> {
    Ok(names
        .iter()
        .map(|n| UnqualifiedColumnName::parse(n))
        .collect::<Result<Vec<_>, _>>()?)
}

fn json(value: &impl serde::Serialize) -> Result<String, Box<dyn std::error::Error>> {
    Ok(serde_json::to_string(value)?)
}

fn attr_json(parts: &[&str]) -> String {
    let name = parts
        .iter()
        .map(|p| format!("\"{p}\""))
        .collect::<Vec<_>>()
        .join(",");
    format!(
        "{{\"unresolvedAttribute\":{{\"name\":[{name}],\"planId\":null,\"isMetadataColumn\":false}}}}"
    )
}

#[test]
fn auxiliary_merge_matches_spark_clause_order_and_conditions() -> TestResult {
    let merge = auxiliary_merge(
        &[
            "cat".into(),
            "ns1".into(),
            "__spark_autocdc_aux_state_target".into(),
        ],
        "reconciled",
        &keys(&["id"])?,
    );
    assert_eq!(
        Vec::<String>::from(merge.target.clone()),
        vec!["cat", "ns1", "__spark_autocdc_aux_state_target"]
    );
    assert_eq!(
        merge.target_alias.as_ref().map(|a| a.as_ref()),
        Some(AUXILIARY_TABLE)
    );
    let spec::MergeSource::Query { input, alias } = &merge.source else {
        return Err("the auxiliary merge reads a reduced projection of the view".into());
    };
    assert_eq!(alias.as_ref().map(|a| a.as_ref()), Some(REDUCED_MICROBATCH));
    let spec::QueryNode::Project { expressions, .. } = &input.node else {
        return Err("the reduced source is a projection".into());
    };
    assert_eq!(
        expressions
            .iter()
            .map(json)
            .collect::<Result<Vec<_>, _>>()?,
        vec![attr_json(&["id"]), attr_json(&["__spark_autocdc_metadata"])]
    );
    assert_eq!(merge.clauses.len(), 3);
    let on = json(&merge.on_condition.expr)?;
    assert!(on.contains(&attr_json(&["reducedMicrobatch", "id"])));
    assert!(on.contains(&attr_json(&["auxiliaryTable", "id"])));
    assert!(on.contains("\"functionName\":[\"=\"]"));
    // 1: matched, delete event newer than the tombstone → update the metadata.
    let spec::MergeClause::Matched(update) = &merge.clauses[0] else {
        return Err("first clause is matched".into());
    };
    let spec::MergeMatchedAction::UpdateSet(assignments) = &update.action else {
        return Err("first clause updates".into());
    };
    assert_eq!(assignments.len(), 1);
    assert_eq!(
        Vec::<String>::from(assignments[0].0.clone()),
        vec!["auxiliaryTable", "__spark_autocdc_metadata"]
    );
    let condition = json(&update.condition)?;
    assert!(condition.contains("\"functionName\":[\">\"]"));
    assert!(condition.contains(&attr_json(&[
        "auxiliaryTable",
        "__spark_autocdc_metadata",
        "deleteSequence"
    ])));
    assert!(condition.contains("\"isNotNull\""));
    assert!(condition.contains("\"functionName\":[\"or\"]"));
    // 2: matched, not a delete event, upsert at or above the tombstone → delete.
    let spec::MergeClause::Matched(delete) = &merge.clauses[1] else {
        return Err("second clause is matched".into());
    };
    assert_eq!(delete.action, spec::MergeMatchedAction::Delete);
    let condition = json(&delete.condition)?;
    assert!(condition.contains("\"functionName\":[\"not\"]"));
    assert!(condition.contains("\"functionName\":[\">=\"]"));
    // 3: not matched, delete event → insert the tombstone.
    let spec::MergeClause::NotMatchedByTarget(insert) = &merge.clauses[2] else {
        return Err("third clause is not matched by target".into());
    };
    assert_eq!(
        insert.action,
        spec::MergeNotMatchedByTargetAction::InsertAll
    );
    assert!(json(&insert.condition)?.contains("\"isNotNull\""));
    assert!(!merge.with_schema_evolution);
    Ok(())
}

#[test]
fn target_merge_updates_non_key_columns_and_inserts_all_columns() -> TestResult {
    let columns: Vec<String> = ["id", "name", "age", "__spark_autocdc_metadata"]
        .iter()
        .map(|c| c.to_string())
        .collect();
    let merge = target_merge(
        &["cat".into(), "ns1".into(), "target".into()],
        "reconciled",
        &keys(&["ID"])?,
        &columns,
        Case::Insensitive,
    );
    assert_eq!(
        merge.target_alias.as_ref().map(|a| a.as_ref()),
        Some(DESTINATION)
    );
    let spec::MergeSource::Table { name, alias } = &merge.source else {
        return Err("the target merge reads the view directly".into());
    };
    assert_eq!(Vec::<String>::from(name.clone()), vec!["reconciled"]);
    assert_eq!(alias.as_ref().map(|a| a.as_ref()), Some(MICROBATCH));
    assert_eq!(merge.clauses.len(), 3);
    let spec::MergeClause::Matched(delete) = &merge.clauses[0] else {
        return Err("first clause is matched".into());
    };
    assert_eq!(delete.action, spec::MergeMatchedAction::Delete);
    let condition = json(&delete.condition)?;
    assert!(condition.contains(&attr_json(&[
        "microbatch",
        "__spark_autocdc_metadata",
        "deleteSequence"
    ])));
    assert!(condition.contains(&attr_json(&[
        "destination",
        "__spark_autocdc_metadata",
        "upsertSequence"
    ])));
    assert!(condition.contains("\"functionName\":[\">\"]"));
    let spec::MergeClause::Matched(update) = &merge.clauses[1] else {
        return Err("second clause is matched".into());
    };
    let spec::MergeMatchedAction::UpdateSet(assignments) = &update.action else {
        return Err("second clause updates".into());
    };
    // The key is excluded by the case-insensitive resolver; the metadata column is updated.
    assert_eq!(
        assignments
            .iter()
            .map(|(column, _)| Vec::<String>::from(column.clone()).join("."))
            .collect::<Vec<_>>(),
        vec![
            "destination.name",
            "destination.age",
            "destination.__spark_autocdc_metadata"
        ]
    );
    assert!(json(&update.condition)?.contains("\"functionName\":[\">=\"]"));
    let spec::MergeClause::NotMatchedByTarget(insert) = &merge.clauses[2] else {
        return Err("third clause is not matched by target".into());
    };
    let spec::MergeNotMatchedByTargetAction::InsertColumns {
        columns: names,
        values,
    } = &insert.action
    else {
        return Err("third clause inserts named columns".into());
    };
    assert_eq!(
        names
            .iter()
            .map(|column| Vec::<String>::from(column.clone()).join("."))
            .collect::<Vec<_>>(),
        vec![
            "destination.id",
            "destination.name",
            "destination.age",
            "destination.__spark_autocdc_metadata"
        ]
    );
    assert_eq!(
        values.iter().map(json).collect::<Result<Vec<_>, _>>()?[1],
        attr_json(&["microbatch", "name"])
    );
    assert!(json(&insert.condition)?.contains("\"isNull\""));
    Ok(())
}

#[test]
fn composite_keys_join_on_every_key_and_case_sensitivity_keeps_the_key_spelling() -> TestResult {
    let columns: Vec<String> = ["region", "Id", "v", "__spark_autocdc_metadata"]
        .iter()
        .map(|c| c.to_string())
        .collect();
    let merge = target_merge(
        &["t".into()],
        "reconciled",
        &keys(&["region", "id"])?,
        &columns,
        Case::Sensitive,
    );
    let on = json(&merge.on_condition.expr)?;
    assert!(on.contains("\"functionName\":[\"and\"]"));
    assert!(on.contains(&attr_json(&["microbatch", "region"])));
    assert!(on.contains(&attr_json(&["destination", "id"])));
    let spec::MergeClause::Matched(update) = &merge.clauses[1] else {
        return Err("second clause is matched".into());
    };
    let spec::MergeMatchedAction::UpdateSet(assignments) = &update.action else {
        return Err("second clause updates".into());
    };
    // Under case-sensitive analysis `Id` is not the key `id`, so it is updated.
    assert_eq!(
        assignments
            .iter()
            .map(|(column, _)| Vec::<String>::from(column.clone()).join("."))
            .collect::<Vec<_>>(),
        vec![
            "destination.Id",
            "destination.v",
            "destination.__spark_autocdc_metadata"
        ]
    );
    Ok(())
}
