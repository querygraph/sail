use super::*;

#[test]
fn ordinary_insert_nodes_are_rejected_before_pipeline_query_lowering() -> TestResult {
    for (sql, node) in [
        ("INSERT INTO output BY NAME SELECT 1", "InsertIntoStatement"),
        (
            "INSERT OVERWRITE output PARTITION(p=1) IF NOT EXISTS SELECT 1",
            "InsertIntoStatement",
        ),
        (
            "WITH s AS (SELECT 1) INSERT INTO output BY NAME SELECT * FROM s",
            "UnresolvedWith",
        ),
        (
            "INSERT INTO output REPLACE WHERE true SELECT 1",
            "OverwriteByExpression",
        ),
        (
            "WITH s AS (SELECT 1) INSERT INTO output REPLACE WHERE true SELECT * FROM s",
            "UnresolvedWith",
        ),
        (
            "INSERT OVERWRITE DIRECTORY '/tmp/unwritten' USING parquet SELECT 1",
            "InsertIntoDir",
        ),
        (
            "WITH s AS (SELECT 1) INSERT OVERWRITE DIRECTORY '/tmp/unwritten' USING parquet SELECT * FROM s",
            "UnresolvedWith",
        ),
    ] {
        let source = SqlSource::new(sql)?;
        for result in [
            from_ast_pipeline_statement(parse_one_statement(sql)?),
            from_ast_pipeline_statement_with_source(parse_one_statement(sql)?, &source),
        ] {
            let Err(SqlError::AnalysisError(message)) = result else {
                return Err("expected unsupported ordinary INSERT".into());
            };
            assert_eq!(
                message,
                format!("Unsupported plan {node} parsed from SQL query")
            );
        }
        assert!(matches!(
            from_ast_statement(parse_one_statement(sql)?)?,
            spec::Plan::Command(_)
        ));
    }
    // The parser/shared mode validator still wins before this producer.
    assert!(matches!(
        parse_one_statement("INSERT INTO output IF NOT EXISTS SELECT 1"),
        Err(SqlError::Structured(_))
    ));
    Ok(())
}

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

#[test]
fn only_a_keyword_makes_use_a_pipeline_directive() -> Result<(), Box<dyn std::error::Error>> {
    let Err(SqlError::AnalysisError(message)) =
        from_ast_pipeline_statement(parse_one_statement("USE other")?)
    else {
        return Err("expected an unsupported USE".into());
    };
    assert_eq!(
        message,
        "Unsupported plan SetCatalogAndNamespace parsed from SQL query"
    );
    assert!(from_ast_pipeline_statement(parse_one_statement("USE SCHEMA other")?).is_ok());
    Ok(())
}
