use std::collections::HashMap;

use super::*;
use crate::error::SqlError;
use crate::source::SqlSource;

type TestResult = Result<(), Box<dyn std::error::Error>>;

#[test]
fn insert_and_flow_syntax_rejects_at_the_reference_token() -> TestResult {
    for (sql, token) in [
        ("INSERT OVERWRITE output IF NOT EXISTS SELECT 1", "IF"),
        (
            "INSERT OVERWRITE output IF NOT EXISTS BY NAME SELECT 1",
            "IF",
        ),
        (
            "CREATE FLOW f AS INSERT OVERWRITE output IF NOT EXISTS BY NAME SELECT 1",
            "IF",
        ),
        ("CREATE FLOW f AS SELECT 1", "SELECT"),
        (
            "CREATE FLOW f AS WITH s AS (SELECT 1) INSERT INTO output BY NAME SELECT * FROM s",
            "WITH",
        ),
        ("CREATE FLOW f AS CREATE TABLE output AS SELECT 1", "CREATE"),
        (
            "CREATE FLOW f AS EXPLAIN INSERT INTO output BY NAME SELECT 1",
            "EXPLAIN",
        ),
        (
            "CREATE FLOW f AS CREATE FLOW g AS INSERT INTO output BY NAME SELECT 1",
            "CREATE",
        ),
        (
            "CREATE FLOW a.f AS INSERT OVERWRITE output IF NOT EXISTS SELECT 1",
            "IF",
        ),
        (
            "/* 雪🦀 */ CREATE FLOW f AS /* SELECT */ select 1",
            "select",
        ),
    ] {
        let source = SqlSource::new(sql)?;
        for error in [
            source.parse_statements().err(),
            source.parse_statements_with_spans().err(),
        ] {
            let Some(SqlError::SqlParserError(diagnostic)) = error else {
                return Err(format!("expected grammar error: {sql}").into());
            };
            let error = diagnostic
                .spark_syntax_error()
                .ok_or("missing source-backed syntax error")?;
            assert_eq!(
                error.class,
                "org.apache.spark.sql.catalyst.parser.ParseException"
            );
            assert_eq!(error.condition, "PARSE_SYNTAX_ERROR");
            assert_eq!(error.sql_state, "42601");
            assert_eq!(
                error.parameters,
                HashMap::from([
                    ("error".into(), format!("'{token}'")),
                    ("hint".into(), String::new()),
                ])
            );
            assert_eq!(error.message, format!("Syntax error at or near '{token}'."));
        }
    }
    Ok(())
}

#[test]
fn valid_insert_forms_keep_query_ctes_and_directory_syntax() -> TestResult {
    let Statement::InsertInto { with, .. } =
        parse_one_statement("WITH s AS (SELECT 1) INSERT INTO output BY NAME SELECT * FROM s")?
    else {
        return Err("ordinary WITH-led INSERT must remain valid".into());
    };
    assert!(with.is_some());
    let Statement::CreatePipelineFlow { statement, .. } = parse_one_statement(
        "CREATE FLOW f AS INSERT INTO output BY NAME WITH s AS (SELECT 1) SELECT * FROM s",
    )?
    else {
        return Err("expected Flow".into());
    };
    let Statement::InsertInto { with, query, .. } = *statement else {
        return Err("expected INSERT".into());
    };
    assert!(with.is_none());
    assert!(query.with.is_some());
    for sql in [
        "CREATE FLOW f AS INSERT OVERWRITE DIRECTORY '/tmp/unwritten' USING parquet SELECT 1",
        "CREATE FLOW f AS INSERT INTO output REPLACE WHERE true SELECT 1",
    ] {
        assert!(matches!(
            parse_one_statement(sql)?,
            Statement::CreatePipelineFlow { .. }
        ));
    }
    Ok(())
}
