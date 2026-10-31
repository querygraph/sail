use super::*;

#[test]
fn replace_using_retains_keys_name_matching_and_ctes_without_executing() -> TestResult {
    for by_name in [false, true] {
        for cte in [false, true] {
            let prefix = if cte {
                "WITH s AS (SELECT 1 AS a, 2 AS b) "
            } else {
                ""
            };
            let clause = if by_name { "BY NAME " } else { "" };
            let query = if cte {
                "SELECT * FROM s"
            } else {
                "SELECT 1 AS a, 2 AS b"
            };
            let sql = format!("{prefix}INSERT INTO output {clause}REPLACE USING (b, a) {query}");
            let statement = parse_one_statement(&sql)?;
            let Statement::InsertIntoReplaceUsing {
                with,
                by_name: parsed,
                keys,
                ..
            } = &statement
            else {
                return Err("expected USING AST".into());
            };
            assert_eq!(with.is_some(), cte);
            assert_eq!(parsed.is_some(), by_name);
            assert_eq!(
                keys.names
                    .items()
                    .map(|key| key.value.as_str())
                    .collect::<Vec<_>>(),
                ["b", "a"]
            );
            let Err(SqlError::NotSupported(message)) = from_ast_statement(statement) else {
                return Err("USING must not silently lower to an ordinary append".into());
            };
            assert_eq!(
                message,
                "ordinary INSERT INTO ... REPLACE USING execution is not supported"
            );
        }
    }
    Ok(())
}

#[test]
fn replace_using_pipeline_policy_preserves_name_before_overwrite_before_target() -> TestResult {
    for sql in [
        "CREATE FLOW f AS INSERT INTO output REPLACE USING (id) SELECT 1 AS id",
        "CREATE FLOW f AS INSERT INTO output BY NAME REPLACE USING (id) SELECT 1 AS id",
        "CREATE FLOW f AS INSERT INTO A.b.c.d REPLACE USING (id) SELECT 1 AS id",
    ] {
        let source = SqlSource::new(sql)?;
        for result in [
            from_ast_pipeline_statement(parse_one_statement(sql)?),
            from_ast_pipeline_statement_with_source(parse_one_statement(sql)?, &source),
        ] {
            let Err(SqlError::AnalysisError(message)) = result else {
                return Err("expected overwrite policy".into());
            };
            assert_eq!(message, "INSERT OVERWRITE flows not supported.");
        }
    }
    let Err(SqlError::Structured(error)) = from_ast_pipeline_statement(parse_one_statement(
        "CREATE FLOW d.f AS INSERT INTO output REPLACE USING (id) SELECT 1 AS id",
    )?) else {
        return Err("expected raw Flow-name error".into());
    };
    assert_eq!(error.condition, "MULTIPART_FLOW_NAME_NOT_SUPPORTED");
    assert_eq!(
        error.parameters.get("flowName").map(String::as_str),
        Some("d.f")
    );
    Ok(())
}
