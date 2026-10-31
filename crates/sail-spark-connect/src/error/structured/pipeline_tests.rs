use tonic::Status;
use tonic_types::StatusExt;

use crate::error::SparkError;

#[test]
fn direct_pipeline_ddl_keeps_unsupported_operation_hierarchy_and_metadata()
-> Result<(), Box<dyn std::error::Error>> {
    for kind in ["MATERIALIZED VIEW", "STREAMING TABLE"] {
        let statement = sail_sql_analyzer::parser::parse_one_statement(&format!(
            "CREATE {kind} table1 AS SELECT 1"
        ))?;
        let error = sail_sql_analyzer::statement::from_ast_statement(statement)
            .err()
            .ok_or("expected direct-execution error")?;
        let status: Status = SparkError::from(error).into();
        let details = status.get_error_details();
        let info = details.error_info().ok_or("missing ErrorInfo")?;
        assert_eq!(
            info.reason,
            "org.apache.spark.SparkUnsupportedOperationException"
        );
        assert_eq!(
            info.metadata["errorClass"],
            "UNSUPPORTED_FEATURE.CREATE_PIPELINE_DATASET_QUERY_EXECUTION"
        );
        assert_eq!(info.metadata["sqlState"], "0A000");
        assert_eq!(
            serde_json::from_str::<Vec<String>>(&info.metadata["classes"])?,
            vec![
                "org.apache.spark.SparkUnsupportedOperationException",
                "java.lang.UnsupportedOperationException",
                "java.lang.RuntimeException",
                "java.lang.Exception",
                "java.lang.Throwable",
                "java.lang.Object",
            ]
        );
        assert_eq!(
            serde_json::from_str::<serde_json::Value>(&info.metadata["messageParameters"])?,
            serde_json::json!({"pipelineDatasetType":kind})
        );
    }
    Ok(())
}
