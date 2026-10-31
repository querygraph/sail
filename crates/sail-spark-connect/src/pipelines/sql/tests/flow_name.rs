use sail_sql_analyzer::pipeline::PipelineStatement;
use sail_sql_analyzer::source::SqlSource;

use super::super::{PreparedSql, PreparedStatement};
use super::*;
use crate::spark::connect as sc;

fn assert_name_error(error: SparkError, expected: &str) -> Result<(), Box<dyn std::error::Error>> {
    let SparkError::Structured(error) = error else {
        return Err("expected typed raw name error".into());
    };
    assert_eq!(error.class, "org.apache.spark.sql.AnalysisException");
    assert_eq!(error.condition, "MULTIPART_FLOW_NAME_NOT_SUPPORTED");
    assert_eq!(error.sql_state, "0A000");
    assert_eq!(
        error.parameters,
        HashMap::from([("flowName".into(), expected.into())])
    );
    assert_eq!(
        error.message,
        format!("Flow with multipart name '{expected}' is not supported.")
    );
    Ok(())
}

#[tokio::test]
async fn original_multipart_flow_input_keeps_raw_error_and_discards_sail_draft()
-> Result<(), Box<dyn std::error::Error>> {
    let mut graph = graph();
    let error=register_sql(&mut graph,"\nCREATE STREAMING TABLE st;\nCREATE FLOW some_database.f AS INSERT INTO st BY NAME\nSELECT * FROM STREAM external_t1;\n","a.sql").await.err().ok_or("expected failure")?;
    assert_name_error(error, "some_database.f")?;
    assert!(graph.definition.outputs.is_empty() && graph.definition.flows.is_empty());
    Ok(())
}

#[test]
fn typed_commit_error_is_not_rewrapped_with_statement_origin()
-> Result<(), Box<dyn std::error::Error>> {
    let text = "SELECT 1";
    let source = SqlSource::new(text)?;
    let (statement, span) = source
        .parse_statements_with_spans()?
        .pop()
        .ok_or("missing statement")?;
    let spec::Plan::Query(query) = sail_sql_analyzer::statement::from_ast_statement(statement)?
    else {
        return Err("expected query".into());
    };
    let prepared = PreparedSql {
        text: text.into(),
        file: Some("should-not-appear.sql".into()),
        statements: vec![PreparedStatement {
            span,
            result: Ok(PipelineStatement::Flow {
                name: vec!["Some_DB".to_string(), "F".to_string()].into(),
                target: vec!["st".to_string()].into(),
                query,
            }),
        }],
    };
    assert_name_error(
        prepared
            .commit(&mut graph())
            .err()
            .ok_or("expected failure")?,
        "Some_DB.F",
    )
}

fn flow(name: &str, target: &str) -> pc::DefineFlow {
    pc::DefineFlow {
        flow_name: Some(name.into()),
        target_dataset_name: Some(target.into()),
        details: Some(pc::define_flow::Details::RelationFlowDetails(
            pc::define_flow::WriteRelationFlowDetails {
                relation: Some(sc::Relation {
                    common: None,
                    rel_type: Some(sc::relation::RelType::Range(sc::Range {
                        start: None,
                        end: 1,
                        step: 1,
                        num_partitions: None,
                    })),
                }),
            },
        )),
        ..Default::default()
    }
}

#[test]
fn protocol_name_error_retains_raw_spelling_and_implicit_flow_exception()
-> Result<(), Box<dyn std::error::Error>> {
    let raw = "`D.b`.`F``Q`";
    assert_name_error(
        graph()
            .define_flow(flow(raw, "st"))
            .err()
            .ok_or("expected failure")?,
        raw,
    )?;
    let mut graph = graph();
    assert_eq!(
        graph.define_flow(flow("Cat.Db.F", "Cat.Db.F"))?,
        vec!["cat", "db", "f"]
    );
    assert_eq!(graph.definition.flows.len(), 1);
    Ok(())
}

#[test]
fn protocol_identifier_arity_precedes_named_and_implicit_flow_rules()
-> Result<(), Box<dyn std::error::Error>> {
    for (name, target) in [("A.b.c.d", "st"), ("A.b.c.d", "A.b.c.d"), ("f", "A.b.c.d")] {
        let SparkError::Structured(error) = graph()
            .define_flow(flow(name, target))
            .err()
            .ok_or("expected failure")?
        else {
            return Err("expected typed failure".into());
        };
        assert_eq!(error.class, "java.lang.UnsupportedOperationException");
        assert!(
            error.condition.is_empty() && error.sql_state.is_empty() && error.parameters.is_empty()
        );
        assert_eq!(
            error.to_string(),
            "4+ part table identifier A.b.c.d is not supported."
        );
    }
    Ok(())
}
