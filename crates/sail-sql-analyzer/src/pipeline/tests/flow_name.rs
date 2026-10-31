use super::*;

#[test]
fn multipart_flow_registration_preserves_spelling_and_precedes_insert_policy() -> TestResult {
    for (name, expected) in [
        ("some_database.f", "some_database.f"),
        ("Cat.Db.F", "Cat.Db.F"),
        ("`D.b`.`F``Q`", "D.b.F`Q"),
    ] {
        for operation in [
            "INSERT INTO st BY NAME SELECT * FROM STREAM external_t1",
            "INSERT OVERWRITE st BY NAME SELECT 1",
            "INSERT INTO st PARTITION (p) BY NAME SELECT 1",
        ] {
            let sql = format!("CREATE FLOW {name} AS {operation}");
            let Err(SqlError::Structured(error)) =
                from_ast_pipeline_statement(parse_one_statement(&sql)?)
            else {
                return Err("expected raw multipart error".into());
            };
            assert_eq!(error.class, "org.apache.spark.sql.AnalysisException");
            assert_eq!(error.condition, "MULTIPART_FLOW_NAME_NOT_SUPPORTED");
            assert_eq!(error.sql_state, "0A000");
            assert_eq!(
                error.parameters,
                std::collections::HashMap::from([("flowName".into(), expected.into())])
            );
            assert_eq!(
                error.message,
                format!("Flow with multipart name '{expected}' is not supported.")
            );
        }
    }
    Ok(())
}

#[test]
fn flow_identifier_arity_preserves_unclassified_error_before_policy() -> TestResult {
    for (name, target, expected) in [("A.b.c.d", "st", "A.b.c.d"), ("f", "A.b.c.d", "A.b.c.d")] {
        let sql = format!("CREATE FLOW {name} AS INSERT INTO {target} BY NAME SELECT 1");
        let Err(SqlError::Structured(error)) =
            from_ast_pipeline_statement(parse_one_statement(&sql)?)
        else {
            return Err("expected raw arity error".into());
        };
        assert_eq!(error.class, "java.lang.UnsupportedOperationException");
        assert!(
            error.condition.is_empty() && error.sql_state.is_empty() && error.parameters.is_empty()
        );
        assert_eq!(
            error.to_string(),
            format!("4+ part table identifier {expected} is not supported.")
        );
    }
    Ok(())
}

#[test]
fn quoted_single_component_flow_name_remains_valid_for_registration() -> TestResult {
    let PipelineStatement::Flow { name, .. } = from_ast_pipeline_statement(parse_one_statement(
        "CREATE FLOW `literal.dot` AS INSERT INTO st BY NAME SELECT 1",
    )?)?
    else {
        return Err("expected Flow".into());
    };
    assert_eq!(Vec::<String>::from(name), ["literal.dot"]);
    // Multipart syntax still parses; only registration rejects it.
    assert!(matches!(
        parse_one_statement("CREATE FLOW c.d.f AS INSERT INTO st BY NAME SELECT 1")?,
        Statement::CreatePipelineFlow { .. }
    ));
    Ok(())
}
