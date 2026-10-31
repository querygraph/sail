//! Ports of the Spark 4.2.1 (617618396) Auto CDC definition assertions:
//! `autocdc/ChangeArgsSuite.scala`, the registration cases of
//! `connect/pipelines/PythonPipelineSuite.scala` ("AutoCDC API: ..."), the
//! Auto CDC cases of `graph/ConnectInvalidPipelineSuite.scala` and the
//! schema-level validators of `autocdc/AutoCdcFlowSuite.scala`. Each test
//! names its source test. Assertions that need resolved data types or a
//! DataFrame are recorded as partial in the Grust evidence.

use std::collections::HashMap;

use super::schema::*;
use super::*;
use crate::error::StructuredSparkError;
use crate::pipelines::PipelineGraph;
use crate::spark::connect::expression::{ExprType, ExpressionString, UnresolvedAttribute};
use crate::spark::connect::pipeline_command::define_flow::Details;
use crate::spark::connect::{OutputType, pipeline_command as pc};

// `ChangeArgsSuite.sourceSchema`: id INT NOT NULL, Name STRING, age INT.
fn source_schema() -> Vec<String> {
    vec!["id".into(), "Name".into(), "age".into()]
}

fn column(name: &str) -> UnqualifiedColumnName {
    UnqualifiedColumnName::parse(name)
        .unwrap_or_else(|_| UnqualifiedColumnName { name: name.into() })
}

fn columns(names: &[&str]) -> Vec<UnqualifiedColumnName> {
    names.iter().map(|name| column(name)).collect()
}

fn attribute(name: &str) -> sc::Expression {
    sc::Expression {
        expr_type: Some(ExprType::UnresolvedAttribute(UnresolvedAttribute {
            unparsed_identifier: name.into(),
            plan_id: None,
            is_metadata_column: None,
        })),
        ..Default::default()
    }
}

fn expression(sql: &str) -> sc::Expression {
    sc::Expression {
        expr_type: Some(ExprType::ExpressionString(ExpressionString {
            expression: sql.into(),
        })),
        ..Default::default()
    }
}

/// The proto the Python client builds for string arguments: `col(key)` for
/// keys and column lists, `expr(sequence_by)` for the sequence.
fn details(source: &str, keys: &[&str], sequence_by: &str) -> AutoCdcFlowDetails {
    AutoCdcFlowDetails {
        source: Some(source.into()),
        keys: keys.iter().map(|key| attribute(key)).collect(),
        sequence_by: Some(expression(sequence_by)),
        ..Default::default()
    }
}

fn structured<T>(result: SparkResult<T>) -> Option<StructuredSparkError> {
    match result {
        Err(SparkError::Structured(error)) => Some(*error),
        _ => None,
    }
}

/// Mirrors `checkError(condition, sqlState, parameters)`.
fn check_error<T: std::fmt::Debug>(
    result: SparkResult<T>,
    condition: &str,
    sql_state: &str,
    parameters: &[(&str, &str)],
) {
    let message = format!("{result:?}");
    let error = structured(result);
    assert!(
        error.is_some(),
        "expected a structured {condition} error, got {message}"
    );
    let Some(error) = error else { return };
    assert_eq!(error.condition, condition, "{message}");
    assert_eq!(error.sql_state, sql_state, "{message}");
    let expected: HashMap<String, String> = parameters
        .iter()
        .map(|(key, value)| ((*key).into(), (*value).into()))
        .collect();
    assert_eq!(error.parameters, expected, "{message}");
}

fn selected(selection: Option<&ColumnSelection>, case: Case) -> SparkResult<Vec<String>> {
    let fields = source_schema();
    ColumnSelection::apply(selection, "test", &fields, case)
        .map(|indices| indices.into_iter().map(|i| fields[i].clone()).collect())
}

fn names(values: &[&str]) -> Vec<String> {
    values.iter().map(|value| (*value).to_string()).collect()
}

mod attribute;
mod auxiliary;
mod change_args;
mod dedup;
mod definition;
mod graph;
mod lookup;
mod merge;
mod microbatch;
mod prepared;
mod reconcile;
mod schema;
