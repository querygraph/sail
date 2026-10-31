use pbjson_types::Any;
use prost::bytes::Bytes;

use super::*;
use crate::spark::connect::{Command, Sql};

type TestResult<T = ()> = Result<T, Box<dyn std::error::Error>>;

fn extension(flow_name: Option<&str>) -> Any {
    #[expect(deprecated)]
    let context = PipelineAnalysisContext {
        dataflow_graph_id: Some("graph".into()),
        flow_name: flow_name.map(str::to_string),
        ..Default::default()
    };
    Any {
        type_url: format!("type.googleapis.com/{CONTEXT_TYPE}"),
        value: Bytes::from(context.encode_to_vec()),
    }
}

fn user(extensions: Vec<Any>) -> UserContext {
    UserContext {
        extensions,
        ..Default::default()
    }
}

fn sql(query: &str) -> Relation {
    #[expect(deprecated)]
    let sql = Sql {
        query: query.into(),
        args: Default::default(),
        pos_args: vec![],
        named_arguments: Default::default(),
        pos_arguments: vec![],
    };
    Relation {
        common: None,
        rel_type: Some(relation::RelType::Sql(sql)),
    }
}

fn condition(error: SparkError) -> TestResult<(String, HashMap<String, String>)> {
    match error {
        SparkError::Structured(error) => Ok((error.condition, error.parameters)),
        other => Err(format!("expected a structured error: {other}").into()),
    }
}

fn blocked_command(request: PipelineRequest, query: &str) -> TestResult<String> {
    let Err(error) = request.parse_sql_command(sql(query)) else {
        return Err(format!("{query} was not blocked").into());
    };
    let (condition, parameters) = condition(error)?;
    assert_eq!(condition, "UNSUPPORTED_PIPELINE_SPARK_SQL_COMMAND");
    Ok(parameters["command"].clone())
}

#[test]
fn a_flow_name_alone_marks_a_query_function() -> TestResult {
    assert_eq!(PipelineRequest::of(None)?, PipelineRequest::Ordinary);
    assert_eq!(
        PipelineRequest::of(Some(&user(vec![])))?,
        PipelineRequest::Ordinary
    );
    assert_eq!(
        PipelineRequest::of(Some(&user(vec![extension(None)])))?,
        PipelineRequest::Definition
    );
    assert_eq!(
        PipelineRequest::of(Some(&user(vec![extension(Some("flow"))])))?,
        PipelineRequest::QueryFunction
    );
    Ok(())
}

#[test]
fn any_extension_with_a_flow_name_wins_regardless_of_order() -> TestResult {
    for extensions in [
        vec![extension(None), extension(Some("flow"))],
        vec![extension(Some("flow")), extension(None)],
    ] {
        assert_eq!(
            PipelineRequest::of(Some(&user(extensions)))?,
            PipelineRequest::QueryFunction
        );
    }
    Ok(())
}

#[test]
fn unrelated_extensions_are_ignored_and_a_corrupt_context_fails() -> TestResult {
    let unrelated = Any {
        type_url: "type.googleapis.com/other.Message".into(),
        value: Bytes::from_static(b"\xff"),
    };
    assert_eq!(
        PipelineRequest::of(Some(&user(vec![unrelated])))?,
        PipelineRequest::Ordinary
    );
    let corrupt = Any {
        type_url: format!("type.googleapis.com/{CONTEXT_TYPE}"),
        value: Bytes::from_static(b"\xff"),
    };
    assert!(PipelineRequest::of(Some(&user(vec![corrupt]))).is_err());
    Ok(())
}

#[test]
fn only_sql_commands_reach_the_server_from_a_query_function() -> TestResult {
    let root = plan::OpType::Root(sql("SELECT 1"));
    let command = |command_type| plan::OpType::Command(Command { command_type });
    let sql_command = command(Some(command::CommandType::SqlCommand(Default::default())));
    let write = command(Some(command::CommandType::WriteOperation(
        Default::default(),
    )));
    let function = PipelineRequest::QueryFunction;
    function.check_execution(&sql_command)?;
    for blocked in [&root, &write] {
        let Err(error) = function.check_execution(blocked) else {
            return Err("eager work was not blocked".into());
        };
        let (condition, parameters) = condition(error)?;
        assert_eq!(condition, "ATTEMPT_ANALYSIS_IN_PIPELINE_QUERY_FUNCTION");
        assert!(parameters.is_empty());
    }
    for request in [PipelineRequest::Ordinary, PipelineRequest::Definition] {
        request.check_execution(&root)?;
        request.check_execution(&write)?;
        request.check_analysis()?;
    }
    assert!(function.check_analysis().is_err());
    Ok(())
}

#[test]
fn statement_policy_applies_to_definitions_and_query_functions() -> TestResult {
    for request in [PipelineRequest::Definition, PipelineRequest::QueryFunction] {
        for (query, command) in [
            ("DROP VIEW v", "DropView"),
            ("CREATE TABLE t (id INT)", "CreateTable"),
            ("USE SCHEMA s", "SetNamespaceCommand"),
            ("INSERT INTO t VALUES (1)", "InsertIntoStatement"),
            (
                "CREATE MATERIALIZED VIEW m AS SELECT 1",
                "CreateMaterializedViewAsSelect",
            ),
            (
                "CREATE STREAMING TABLE t AS SELECT 1",
                "CreateStreamingTableAsSelect",
            ),
        ] {
            assert_eq!(blocked_command(request, query)?, command);
        }
        for query in [
            "SELECT 1",
            "SHOW TABLES",
            "SHOW CATALOGS",
            "DESCRIBE TABLE t",
            "DESCRIBE TABLE EXTENDED t",
        ] {
            request.parse_sql_command(sql(query))?;
        }
    }
    Ok(())
}

#[test]
fn commands_outside_the_allowlist_are_denied_by_default() -> TestResult {
    let request = PipelineRequest::Definition;
    for query in ["EXPLAIN SELECT 1", "DROP TABLE t", "DESCRIBE TABLE t c"] {
        assert!(request.parse_sql_command(sql(query)).is_err(), "{query}");
    }
    Ok(())
}

#[test]
fn ordinary_requests_keep_their_statements() -> TestResult {
    let plan = PipelineRequest::Ordinary.parse_sql_command(sql("DROP VIEW v"))?;
    assert!(matches!(plan, spec::Plan::Command(_)));
    Ok(())
}

#[test]
fn only_a_query_function_defers_sql() {
    assert!(PipelineRequest::QueryFunction.defers_sql());
    assert!(!PipelineRequest::Definition.defers_sql());
    assert!(!PipelineRequest::Ordinary.defers_sql());
}
