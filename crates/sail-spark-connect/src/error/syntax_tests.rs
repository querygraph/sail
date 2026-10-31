use sail_plan::error::PlanError;
use sail_sql_analyzer::error::SqlError;
use sail_sql_analyzer::source::SqlSource;

use super::SparkError;

type TestResult = Result<(), Box<dyn std::error::Error>>;

#[test]
fn syntax_metadata_survives_direct_and_plan_error_conversions() -> TestResult {
    for via_plan in [false, true] {
        let error = SqlSource::new("CREATE FLOW f AS SELECT 1")?
            .parse_statements()
            .err()
            .ok_or("expected syntax error")?;
        let error = if via_plan {
            SparkError::from(PlanError::from(error))
        } else {
            SparkError::from(error)
        };
        let SparkError::Structured(error) = error else {
            return Err("syntax metadata was discarded during conversion".into());
        };
        assert_eq!(
            error.class,
            "org.apache.spark.sql.catalyst.parser.ParseException"
        );
        assert_eq!(error.condition, "PARSE_SYNTAX_ERROR");
        assert_eq!(error.sql_state, "42601");
        assert_eq!(
            error.parameters.get("error").map(String::as_str),
            Some("'SELECT'")
        );
        assert_eq!(error.parameters.get("hint").map(String::as_str), Some(""));
    }
    Ok(())
}

#[test]
fn unsourced_expression_parser_errors_keep_legacy_conversion() -> TestResult {
    for via_plan in [false, true] {
        let error = sail_sql_analyzer::parser::parse_expression(")")
            .err()
            .ok_or("expected expression error")?;
        let SqlError::SqlParserError(diagnostic) = &error else {
            return Err("expected parser error".into());
        };
        let expected = diagnostic.to_string();
        let error = if via_plan {
            SparkError::from(PlanError::from(error))
        } else {
            SparkError::from(error)
        };
        let SparkError::InvalidArgument(message) = error else {
            return Err("unexpected classification for an unsourced custom failure".into());
        };
        assert_eq!(message, expected);
        assert!(message.contains("found )"));
    }
    Ok(())
}
