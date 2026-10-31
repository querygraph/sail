use super::*;

#[test]
fn ordinary_ctas_is_rejected_only_by_pipeline_normalization() -> TestResult {
    for (sql, node) in [
        ("CREATE TABLE t AS SELECT 1", "CreateTableAsSelect"),
        (
            "CREATE TABLE IF NOT EXISTS t AS SELECT 1",
            "CreateTableAsSelect",
        ),
        (
            "CREATE OR REPLACE TABLE t AS SELECT 1",
            "ReplaceTableAsSelect",
        ),
        ("REPLACE TABLE t AS SELECT 1", "ReplaceTableAsSelect"),
    ] {
        let Err(SqlError::AnalysisError(message)) =
            from_ast_pipeline_statement(parse_one_statement(sql)?)
        else {
            return Err("expected pipeline registration rejection".into());
        };
        assert_eq!(
            message,
            format!("Unsupported plan {node} parsed from SQL query")
        );
        assert!(matches!(
            from_ast_statement(parse_one_statement(sql)?)?,
            spec::Plan::Command(spec::CommandPlan {
                node: spec::CommandNode::CreateTableAsSelect { .. },
                ..
            })
        ));
    }

    Ok(())
}
