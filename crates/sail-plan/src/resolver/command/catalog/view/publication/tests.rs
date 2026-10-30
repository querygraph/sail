use std::cell::Cell;
use std::collections::HashMap;

use datafusion::arrow::datatypes::{DataType, Field, Schema, TimeUnit};

use super::*;

type TestResult = std::result::Result<(), Box<dyn std::error::Error>>;

fn schema() -> PlanResult<DFSchema> {
    Ok(DFSchema::try_from(Schema::new(vec![
        Field::new("opaque_1", DataType::Int64, false)
            .with_metadata(HashMap::from([("comment".into(), "input comment".into())])),
        Field::new("opaque_2", DataType::Decimal128(12, 3), true),
        Field::new(
            "opaque_3",
            DataType::Timestamp(TimeUnit::Microsecond, Some("Europe/Paris".into())),
            false,
        ),
    ]))?)
}
fn expected(names: &[&str], comments: Vec<Option<&str>>) -> Vec<CreateViewColumnOptions> {
    names
        .iter()
        .zip(comments)
        .zip([
            (DataType::Int64, false),
            (DataType::Decimal128(12, 3), true),
            (
                DataType::Timestamp(TimeUnit::Microsecond, Some("Europe/Paris".into())),
                false,
            ),
        ])
        .map(
            |((name, comment), (data_type, nullable))| CreateViewColumnOptions {
                name: (*name).into(),
                data_type,
                nullable,
                comment: comment.map(str::to_owned),
            },
        )
        .collect()
}
#[test]
fn explicit_view_columns_preserve_types_nullability_and_comments_without_resolving_names()
-> TestResult {
    let called = Cell::new(false);
    let explicit = vec![
        spec::ViewColumnDefinition {
            name: "雪".into(),
            comment: Some("first".into()),
        },
        spec::ViewColumnDefinition {
            name: "MixedCase".into(),
            comment: None,
        },
        spec::ViewColumnDefinition {
            name: "time".into(),
            comment: Some("clock".into()),
        },
    ];
    let actual = columns(&schema()?, Some(explicit), || {
        called.set(true);
        Err(PlanError::internal(
            "explicit names must not query resolver state",
        ))
    })?;
    assert_eq!(
        actual,
        expected(
            &["雪", "MixedCase", "time"],
            vec![Some("first"), None, Some("clock")]
        )
    );
    assert!(!called.get());
    Ok(())
}
#[test]
fn inferred_view_columns_use_user_names_without_copying_input_comments() -> TestResult {
    let calls = Cell::new(0);
    let actual = columns(&schema()?, None, || {
        calls.set(calls.get() + 1);
        Ok(vec!["id".into(), "amount".into(), "clock".into()])
    })?;
    assert_eq!(
        actual,
        expected(&["id", "amount", "clock"], vec![None, None, None])
    );
    assert_eq!(calls.get(), 1);
    Ok(())
}
#[test]
fn view_column_count_error_precedes_name_resolution() -> TestResult {
    let called = Cell::new(false);
    let error = columns(&schema()?, Some(vec![]), || {
        called.set(true);
        Err(PlanError::internal("name resolution must not win"))
    })
    .err()
    .ok_or("mismatched explicit column list accepted")?;
    let PlanError::AnalysisError(message) = error else {
        return Err("column mismatch lost its native analysis error".into());
    };
    assert_eq!(
        message,
        "CREATE VIEW column list has 0 columns, but the query produces 3 columns"
    );
    assert!(!called.get());
    Ok(())
}
#[test]
fn inferred_name_failure_retains_original_variant() -> TestResult {
    let error = columns(&schema()?, None, || {
        Err(PlanError::UnresolvedRelation(vec!["missing".into()]))
    })
    .err()
    .ok_or("field-name failure disappeared")?;
    assert!(matches!(error, PlanError::UnresolvedRelation(name) if name == ["missing"]));
    Ok(())
}
#[test]
fn publication_namespace_replaces_reserved_entries_and_preserves_other_properties_in_order() {
    let mut properties = vec![
        ("user".into(), "first".into()),
        (VIEW_NAMESPACE_SIZE.into(), "8".into()),
        (format!("{VIEW_NAMESPACE_PART}0"), "stale".into()),
        ("user".into(), "second".into()),
        (format!("{VIEW_NAMESPACE_PART}invalid"), "stale-too".into()),
        ("comment".into(), "keep".into()),
    ];
    set_namespace(&mut properties, vec!["cat.雪".into(), "db`x".into()]);
    assert_eq!(
        properties,
        vec![
            ("user".into(), "first".into()),
            ("user".into(), "second".into()),
            ("comment".into(), "keep".into()),
            (VIEW_NAMESPACE_SIZE.into(), "2".into()),
            (format!("{VIEW_NAMESPACE_PART}0"), "cat.雪".into()),
            (format!("{VIEW_NAMESPACE_PART}1"), "db`x".into()),
        ]
    );
}
