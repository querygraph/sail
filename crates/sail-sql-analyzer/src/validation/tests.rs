use chumsky::Parser;
use chumsky::input::Input;
use sail_sql_parser::options::ParserOptions;
use sail_sql_parser::parser::create_parser;

use super::*;
use crate::error::SqlError;
use crate::parser::{map_parser_input, parse_one_statement, tokenize};
use crate::pipeline::from_ast_pipeline_statement;
use crate::source::SqlSource;
use crate::statement::from_ast_statement;
type TestResult = Result<(), Box<dyn std::error::Error>>;

fn check(error: SqlError) -> TestResult {
    let SqlError::Structured(error) = error else {
        return Err("expected typed parse error".into());
    };
    assert_eq!(
        error.class,
        "org.apache.spark.sql.catalyst.parser.ParseException"
    );
    assert_eq!(error.condition, "INVALID_STATEMENT_OR_CLAUSE");
    assert_eq!(error.sql_state, "42601");
    assert_eq!(
        error.parameters,
        std::collections::HashMap::from([(
            "operation".into(),
            "INSERT INTO ... IF NOT EXISTS".into()
        )])
    );
    assert_eq!(
        error.message,
        "The statement or clause: INSERT INTO ... IF NOT EXISTS is not valid."
    );
    Ok(())
}

fn syntactic_statement(sql: &str) -> Result<Statement, SqlError> {
    let options = ParserOptions::default();
    let tokens = tokenize(sql, &options)?;
    let input = tokens
        .as_slice()
        .map((sql.len()..sql.len()).into(), map_parser_input);
    let mut statements =
        create_parser::<_, chumsky::extra::Err<chumsky::error::Rich<_, _>>>(&options)
            .parse(input)
            .into_result()
            .map_err(SqlError::parser)?;
    statements
        .pop()
        .ok_or_else(|| SqlError::invalid("missing statement"))
}

#[test]
fn invalid_into_mode_is_shared_by_parse_and_ast_lowering_routes() -> TestResult {
    for sql in [
        "INSERT INTO missing IF NOT EXISTS SELECT 1",
        "INSERT INTO missing IF NOT EXISTS BY NAME SELECT 1",
        "INSERT INTO missing PARTITION(p=1) IF NOT EXISTS SELECT 1",
        "CREATE FLOW f AS INSERT INTO missing IF NOT EXISTS BY NAME SELECT 1",
        "CREATE FLOW a.f AS INSERT INTO missing IF NOT EXISTS SELECT 1",
        "EXPLAIN INSERT INTO missing IF NOT EXISTS SELECT 1",
    ] {
        check(parse_one_statement(sql).err().ok_or("expected error")?)?;
        let source = SqlSource::new(sql)?;
        check(source.parse_statements().err().ok_or("expected error")?)?;
        check(
            source
                .parse_statements_with_spans()
                .err()
                .ok_or("expected error")?,
        )?;
        check(
            from_ast_statement(syntactic_statement(sql)?)
                .err()
                .ok_or("expected error")?,
        )?;
        check(
            from_ast_pipeline_statement(syntactic_statement(sql)?)
                .err()
                .ok_or("expected error")?,
        )?;
    }
    Ok(())
}

#[test]
fn whole_file_insert_parse_error_precedes_registration_errors() -> TestResult {
    for first in [
        "CREATE FLOW a.f AS INSERT INTO st BY NAME SELECT 1",
        "CREATE TABLE t AS SELECT 1",
        "SET CATALOG CAST(CAST('bad' AS INT) AS STRING)",
    ] {
        let sql = format!("{first}; INSERT INTO missing IF NOT EXISTS SELECT 1");
        let source = SqlSource::new(&sql)?;
        check(source.parse_statements().err().ok_or("expected error")?)?;
        check(
            source
                .parse_statements_with_spans()
                .err()
                .ok_or("expected error")?,
        )?;
    }
    Ok(())
}

#[test]
fn overwrite_if_not_exists_keeps_its_existing_command_flags() -> TestResult {
    for sql in [
        "INSERT OVERWRITE t PARTITION(p=1) IF NOT EXISTS SELECT 1",
        "INSERT OVERWRITE t PARTITION(p=1) IF NOT EXISTS BY NAME SELECT 1",
    ] {
        let statement = parse_one_statement(sql)?;
        let sail_common::spec::Plan::Command(plan) = from_ast_statement(statement)? else {
            return Err("expected command".into());
        };
        let sail_common::spec::CommandNode::InsertInto {
            mode,
            if_not_exists,
            partition,
            ..
        } = plan.node
        else {
            return Err("expected insert".into());
        };
        assert!(if_not_exists && !partition.is_empty());
        assert!(matches!(
            mode,
            sail_common::spec::InsertMode::InsertByPosition { overwrite: true }
                | sail_common::spec::InsertMode::InsertByName { overwrite: true }
        ));
    }
    Ok(())
}
