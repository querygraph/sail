use super::*;

#[tokio::test]
async fn original_unsupported_ctas_uses_registration_message_and_source()
-> Result<(), Box<dyn std::error::Error>> {
    let mut graph = graph();
    let SparkError::Structured(error) =
        register_sql(&mut graph, "CREATE TABLE t AS SELECT 1", "a.sql")
            .await
            .err()
            .ok_or("expected error")?
    else {
        return Err("expected registration wrapper".into());
    };
    assert_eq!(error.class, "org.apache.spark.sql.AnalysisException");
    assert_eq!(
        error.condition,
        "PIPELINE_SQL_GRAPH_ELEMENT_REGISTRATION_ERROR"
    );
    assert_eq!(error.sql_state, "42000");
    assert_eq!(
        error.parameters,
        HashMap::from([
            (
                "message".into(),
                "Unsupported plan CreateTableAsSelect parsed from SQL query".into()
            ),
            (
                "offendingQuery".into(),
                "\nOffending query:\nCREATE TABLE t AS SELECT 1\n".into()
            ),
            ("codeLocation".into(), "Query defined at a.sql:1".into()),
        ])
    );
    assert!(graph.definition.outputs.is_empty() && graph.definition.flows.is_empty());
    Ok(())
}
