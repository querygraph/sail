//! Original CreateFlowCommandSuite inputs and all parser-level observations.
//! Spark write privileges are assigned during resolution in Sail; this test
//! checks syntax/SQL normalization and does not qualify privilege enforcement.

use super::*;
use crate::value::from_ast_string;

#[test]
fn flow_parser_preserves_optional_comment() -> TestResult {
    for (sql, expected) in [
        ("CREATE FLOW f AS INSERT INTO a SELECT * FROM b", None),
        (
            "CREATE FLOW F COMMENT 'comment' AS INSERT INTO a SELECT * FROM b",
            Some("comment"),
        ),
    ] {
        let Statement::CreatePipelineFlow { comment, .. } = parse_one_statement(sql)? else {
            return Err("expected Flow syntax".into());
        };
        assert_eq!(
            comment
                .map(|(_, text)| from_ast_string(text))
                .transpose()?
                .as_deref(),
            expected
        );
    }
    Ok(())
}

#[test]
fn flow_parser_preserves_all_multipart_name_components() -> TestResult {
    for (sql, expected) in [
        ("CREATE FLOW f AS INSERT INTO a SELECT * FROM b", vec!["f"]),
        (
            "CREATE FLOW c.f AS INSERT INTO a SELECT * FROM b",
            vec!["c", "f"],
        ),
        (
            "CREATE FLOW c.d.f AS INSERT INTO a SELECT * FROM b",
            vec!["c", "d", "f"],
        ),
    ] {
        let Statement::CreatePipelineFlow { name, .. } = parse_one_statement(sql)? else {
            return Err("expected Flow syntax".into());
        };
        assert_eq!(Vec::<String>::from(from_ast_object_name(name)?), expected);
    }
    Ok(())
}

#[test]
fn flow_parser_preserves_insert_overwrite_dynamic_partition_and_by_name_query() -> TestResult {
    let sql =
        "CREATE FLOW f AS INSERT OVERWRITE a PARTITION (col1) BY NAME SELECT col1, col2 FROM b";
    let Statement::CreatePipelineFlow { statement, .. } = parse_one_statement(sql)? else {
        return Err("expected Flow syntax".into());
    };
    let Statement::InsertInto {
        with,
        into_or_overwrite,
        name,
        partition,
        if_not_exists,
        columns,
        query,
        ..
    } = *statement
    else {
        return Err("expected the INSERT child".into());
    };
    assert!(with.is_none());
    assert_eq!(Vec::<String>::from(from_ast_object_name(name)?), ["a"]);
    assert!(matches!(into_or_overwrite, Either::Right(_)));
    assert!(if_not_exists.is_none());
    assert!(matches!(columns, Some(Either::Left(_))));
    let partition = partition.ok_or("missing PARTITION clause")?;
    let values = partition.values.values.items().collect::<Vec<_>>();
    assert_eq!(values.len(), 1);
    assert_eq!(values[0].column.value, "col1");
    assert!(values[0].value.is_none());
    let Statement::Query(expected) = parse_one_statement("SELECT col1, col2 FROM b")? else {
        return Err("expected SELECT query".into());
    };
    assert_eq!(from_ast_query(query)?, from_ast_query(expected)?);
    Ok(())
}
