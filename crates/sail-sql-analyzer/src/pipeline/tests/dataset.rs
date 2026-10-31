//! Native counterparts of CreatePipelineDatasetAsSelectParserSuiteBase's
//! forbidden-clause cases and the materialized-view missing-query case.
//! Spark's logical parser corresponds to Sail's AST parsing plus SQL lowering.

use super::*;

fn expect_parse_error(error: SqlError, message: &str) -> TestResult {
    let SqlError::Structured(error) = error else {
        return Err(format!("expected structured parser error, got {error}").into());
    };
    assert_eq!(
        error.class,
        "org.apache.spark.sql.catalyst.parser.ParseException"
    );
    assert_eq!(error.condition, "_LEGACY_ERROR_TEMP_0035");
    assert!(error.sql_state.is_empty());
    assert_eq!(
        error.parameters,
        std::collections::HashMap::from([("message".into(), message.into())])
    );
    assert_eq!(error.message, format!("Operation not allowed: {message}."));
    Ok(())
}

#[test]
fn forbidden_dataset_clauses_preserve_both_syntaxes_and_lowering_routes() -> TestResult {
    for kind in ["MATERIALIZED VIEW", "STREAMING TABLE"] {
        for (clause, message) in [
            (
                "LOCATION 'dbfs:/path/to/table'",
                format!(
                    "Specifying location is not supported for CREATE {kind} statements. The storage location for a pipeline dataset is managed by the pipeline itself."
                ),
            ),
            (
                "STORED AS TEXTFILE",
                format!(
                    "The STORED AS syntax is not supported for CREATE {kind} statements. Consider using the Data Source based USING clause instead."
                ),
            ),
            (
                "ROW FORMAT DELIMITED FIELDS TERMINATED BY ','",
                format!(
                    "Hive SerDe format options are not supported for CREATE {kind} statements."
                ),
            ),
            (
                "CLUSTERED BY (a) INTO 5 BUCKETS",
                format!(
                    "Bucketing is not supported for CREATE {kind} statements. Please remove any bucket spec specified in the statement."
                ),
            ),
            (
                "OPTIONS ('key' = 'value')",
                format!(
                    "Options are not supported for CREATE {kind} statements. Please remove any OPTIONS lists specified in the statement."
                ),
            ),
        ] {
            let sql = format!("CREATE {kind} table1 {clause} AS SELECT * FROM input");
            expect_parse_error(
                from_ast_statement(parse_one_statement(&sql)?)
                    .err()
                    .ok_or("expected direct error")?,
                &message,
            )?;
            expect_parse_error(
                from_ast_pipeline_statement(parse_one_statement(&sql)?)
                    .err()
                    .ok_or("expected registration error")?,
                &message,
            )?;
            // The pipeline-specific restriction must not affect ordinary table DDL.
            assert!(matches!(
                from_ast_statement(parse_one_statement(&format!(
                    "CREATE TABLE ordinary {clause} AS SELECT * FROM input"
                ))?)?,
                spec::Plan::Command(_)
            ));
        }
    }
    Ok(())
}

#[test]
fn missing_materialized_view_query_is_a_parse_error_but_streaming_table_can_be_empty() -> TestResult
{
    let sql = "CREATE MATERIALIZED VIEW table";
    let message = "Unable to find query for CREATE MATERIALIZED VIEW statement.";
    expect_parse_error(
        from_ast_statement(parse_one_statement(sql)?)
            .err()
            .ok_or("expected direct error")?,
        message,
    )?;
    expect_parse_error(
        from_ast_pipeline_statement(parse_one_statement(sql)?)
            .err()
            .ok_or("expected registration error")?,
        message,
    )?;
    let PipelineStatement::Dataset {
        streaming,
        table,
        definition,
        query,
    } = from_ast_pipeline_statement(parse_one_statement(
        "CREATE STREAMING TABLE st COMMENT 'populate with flow later'",
    )?)?
    else {
        return Err("expected a streaming dataset declaration".into());
    };
    assert!(streaming);
    assert_eq!(Vec::<String>::from(table), ["st"]);
    assert!(query.is_none());
    assert!(definition.columns.is_empty());
    assert!(definition.partition_by.is_empty());
    assert_eq!(
        definition.comment.as_deref(),
        Some("populate with flow later")
    );
    assert!(definition.properties.is_empty());
    assert!(definition.options.is_empty());
    assert!(definition.location.is_none());
    assert!(definition.file_format.is_none());
    assert!(definition.row_format.is_none());
    assert!(!definition.external);
    assert!(definition.constraints.is_empty());
    assert!(matches!(definition.mode, spec::CreateTableMode::Create));
    Ok(())
}

#[test]
fn dataset_restriction_precedence_is_independent_of_clause_order() -> TestResult {
    for kind in ["MATERIALIZED VIEW", "STREAMING TABLE"] {
        for clauses in [
            "LOCATION '/tmp/location' OPTIONS ('x'='y') CLUSTERED BY (a) INTO 5 BUCKETS",
            "CLUSTERED BY (a) INTO 5 BUCKETS OPTIONS ('x'='y') LOCATION '/tmp/location'",
        ] {
            let sql = format!("CREATE {kind} table1 {clauses} AS SELECT 1");
            expect_parse_error(
                from_ast_pipeline_statement(parse_one_statement(&sql)?)
                    .err()
                    .ok_or("expected error")?,
                &format!(
                    "Bucketing is not supported for CREATE {kind} statements. Please remove any bucket spec specified in the statement."
                ),
            )?;
        }
    }
    Ok(())
}
