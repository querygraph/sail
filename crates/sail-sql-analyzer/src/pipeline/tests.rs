use super::*;
use crate::parser::parse_one_statement;

type TestResult = Result<(), Box<dyn std::error::Error>>;

#[test]
fn direct_dataset_execution_preserves_spark_error_without_affecting_registration() -> TestResult {
    for (kind, streaming) in [("MATERIALIZED VIEW", false), ("STREAMING TABLE", true)] {
        let sql = format!("CREATE {kind} table1 AS SELECT 1");
        let Err(SqlError::Structured(error)) = from_ast_statement(parse_one_statement(&sql)?)
        else {
            return Err("expected a structured direct-execution rejection".into());
        };
        assert_eq!(
            error.class,
            "org.apache.spark.SparkUnsupportedOperationException"
        );
        assert_eq!(
            error.condition,
            "UNSUPPORTED_FEATURE.CREATE_PIPELINE_DATASET_QUERY_EXECUTION"
        );
        assert_eq!(error.sql_state, "0A000");
        assert_eq!(
            error.parameters,
            std::collections::HashMap::from([("pipelineDatasetType".into(), kind.into())])
        );
        let PipelineStatement::Dataset {
            streaming: actual,
            query,
            ..
        } = from_ast_pipeline_statement(parse_one_statement(&sql)?)?
        else {
            return Err("pipeline registration no longer lowers a dataset".into());
        };
        assert_eq!(actual, streaming);
        assert!(query.is_some());
    }
    assert!(matches!(
        from_ast_statement(parse_one_statement("CREATE TABLE ordinary AS SELECT 1")?)?,
        spec::Plan::Command(spec::CommandPlan {
            node: spec::CommandNode::CreateTableAsSelect { .. },
            ..
        })
    ));
    Ok(())
}
