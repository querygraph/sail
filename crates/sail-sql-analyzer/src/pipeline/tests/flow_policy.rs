use super::*;

#[test]
fn flow_insert_policy_preserves_messages_and_precedence() -> TestResult {
    for (operation, expected) in [
        (
            "INSERT INTO output SELECT 1",
            "Only INSERT INTO by name flows supported.",
        ),
        (
            "INSERT OVERWRITE output BY NAME SELECT 1",
            "INSERT OVERWRITE flows not supported.",
        ),
        (
            "INSERT INTO output(a) SELECT 1",
            "Column schema may not be specified for flow target.",
        ),
        (
            "INSERT INTO output PARTITION(p) BY NAME SELECT 1",
            "Partition spec may not be specified for flow target.",
        ),
        (
            "INSERT INTO output PARTITION(p) (a) SELECT 1",
            "Partition spec may not be specified for flow target.",
        ),
        (
            "INSERT OVERWRITE output(a) SELECT 1",
            "Column schema may not be specified for flow target.",
        ),
        (
            "INSERT OVERWRITE output PARTITION(p) BY NAME SELECT 1",
            "Partition spec may not be specified for flow target.",
        ),
        (
            "INSERT OVERWRITE output SELECT 1",
            "INSERT OVERWRITE flows not supported.",
        ),
        (
            "INSERT OVERWRITE output PARTITION(p=1) IF NOT EXISTS SELECT 1",
            "Partition spec may not be specified for flow target.",
        ),
        (
            "INSERT OVERWRITE A.b.c.d BY NAME SELECT 1",
            "INSERT OVERWRITE flows not supported.",
        ),
        (
            "INSERT INTO A.b.c.d(a) SELECT 1",
            "Column schema may not be specified for flow target.",
        ),
    ] {
        let sql = format!("CREATE FLOW f AS {operation}");
        let source = SqlSource::new(&sql)?;
        for result in [
            from_ast_pipeline_statement(parse_one_statement(&sql)?),
            from_ast_pipeline_statement_with_source(parse_one_statement(&sql)?, &source),
        ] {
            let Err(SqlError::AnalysisError(message)) = result else {
                return Err(format!("expected registration message for {sql}").into());
            };
            assert_eq!(message, expected, "{sql}");
        }
    }
    Ok(())
}

#[test]
fn valid_flow_insert_keeps_target_and_query() -> TestResult {
    let PipelineStatement::Flow { target, query, .. } = from_ast_pipeline_statement(
        parse_one_statement("CREATE FLOW f AS INSERT INTO c.d.output BY NAME SELECT 1")?,
    )?
    else {
        return Err("expected valid Flow".into());
    };
    assert_eq!(Vec::<String>::from(target), ["c", "d", "output"]);
    let Statement::Query(expected) = parse_one_statement("SELECT 1")? else {
        return Err("expected query".into());
    };
    assert_eq!(query, from_ast_query(expected)?);
    Ok(())
}
