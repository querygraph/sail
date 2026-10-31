use std::collections::HashMap;

use super::*;
use crate::pipelines::graph::Output;
use crate::spark::connect::OutputType;

type TestResult = Result<(), Box<dyn std::error::Error>>;
fn graph() -> GraphDefinition {
    GraphDefinition {
        catalog: "c".into(),
        database: "d".into(),
        sql_conf: HashMap::new(),
        flows: vec![],
        outputs: [
            vec!["z_sensitive"],
            vec!["a_insensitive"],
            vec!["c", "d", "table"],
            vec!["c", "d.a", "b"],
            vec!["c", "d", "a.b"],
            vec!["probe", "input"],
        ]
        .into_iter()
        .map(|name| Output {
            name: name.into_iter().map(str::to_string).collect(),
            kind: OutputType::TemporaryView,
            definition: Default::default(),
            schema: None,
            view: None,
        })
        .collect(),
    }
}
fn parse(sql: &str) -> Result<spec::QueryPlan, Box<dyn std::error::Error>> {
    let ast = sail_sql_analyzer::parser::parse_one_statement(sql)?;
    match sail_sql_analyzer::statement::from_ast_statement(ast)? {
        spec::Plan::Query(query) => Ok(query),
        _ => Err("test statement is not a query".into()),
    }
}
fn trace(sql: &str, sensitive: bool) -> Result<Vec<Name>, Box<dyn std::error::Error>> {
    let query = parse(sql)?;
    let flow = Flow {
        name: vec!["result".into()],
        target: vec!["result".into()],
        query: query.clone(),
        sql_conf: HashMap::new(),
        catalog: "c".into(),
        database: "d".into(),
    };
    Ok(ordered_inputs(
        &graph(),
        &flow,
        &query,
        sensitive,
        |name| name == "probe",
    )?)
}
fn expected(names: &[&[&str]]) -> Vec<Name> {
    names
        .iter()
        .map(|parts| parts.iter().map(|s| s.to_string()).collect())
        .collect()
}

#[test]
fn real_sql_traces_views_before_semantic_resolution_in_reference_order() -> TestResult {
    for (sql, names) in [
        (
            "SELECT Foo FROM (SELECT Foo FROM z_sensitive) nested CROSS JOIN a_insensitive",
            vec!["z_sensitive", "a_insensitive"],
        ),
        (
            "SELECT Bar, (SELECT foo FROM z_sensitive) AS nested FROM a_insensitive",
            vec!["z_sensitive", "a_insensitive"],
        ),
        (
            "SELECT Foo FROM a_insensitive CROSS JOIN z_sensitive",
            vec!["a_insensitive", "z_sensitive"],
        ),
        (
            "SELECT z1.Foo FROM z_sensitive z1 CROSS JOIN a_insensitive CROSS JOIN z_sensitive z2",
            vec!["z_sensitive", "a_insensitive", "z_sensitive"],
        ),
        (
            "SELECT Bar FROM a_insensitive WHERE EXISTS (SELECT foo FROM z_sensitive)",
            vec!["z_sensitive", "a_insensitive"],
        ),
    ] {
        assert_eq!(
            trace(sql, false)?,
            names
                .into_iter()
                .map(|name| vec![name.to_string()])
                .collect::<Vec<_>>(),
            "{sql}"
        );
    }
    Ok(())
}

#[test]
fn cte_definitions_shadow_graph_names_and_unused_definitions_are_visited() -> TestResult {
    assert_eq!(
        trace(
            "WITH staged AS (SELECT Foo FROM z_sensitive) SELECT Bar FROM a_insensitive",
            false
        )?,
        expected(&[&["z_sensitive"], &["a_insensitive"]])
    );
    assert!(
        trace(
            "WITH z_sensitive AS (SELECT 1 AS foo) SELECT Foo FROM Z_SENSITIVE",
            false
        )?
        .is_empty()
    );
    assert_eq!(
        trace(
            "WITH z_sensitive AS (SELECT 1 AS foo) SELECT Foo FROM Z_SENSITIVE",
            true
        )?,
        expected(&[&["z_sensitive"]])
    );
    assert_eq!(
        trace(
            "WITH c AS (SELECT * FROM z_sensitive) SELECT * FROM (WITH c AS (SELECT * FROM a_insensitive) SELECT * FROM c)",
            false
        )?,
        expected(&[&["z_sensitive"], &["a_insensitive"]])
    );
    Ok(())
}

#[test]
fn qualification_keeps_canonical_identity_and_registered_format_paths_are_external() -> TestResult {
    assert_eq!(
        trace(
            "SELECT * FROM table CROSS JOIN c.d.table CROSS JOIN unrelated",
            false
        )?,
        expected(&[&["c", "d", "table"], &["c", "d", "table"]])
    );
    let reads = trace("SELECT * FROM c.`d.a`.b CROSS JOIN c.d.`a.b`", false)?;
    assert_eq!(reads, expected(&[&["c", "d.a", "b"], &["c", "d", "a.b"]]));
    assert_eq!(reads[0].join("."), reads[1].join("."));
    assert_ne!(reads[0], reads[1]);
    assert!(trace("SELECT * FROM probe.input", false)?.is_empty());
    Ok(())
}

#[test]
fn deferred_binding_forms_are_explicit_errors_instead_of_silently_missing_reads() -> TestResult {
    let graph = graph();
    let query = spec::QueryPlan::new(spec::QueryNode::Read {
        read_type: spec::ReadType::DynamicTable(Box::new(spec::ReadDynamicTable {
            name: spec::Expr::Literal(spec::Literal::Utf8 {
                value: Some("z_sensitive".into()),
            }),
            sample: None,
            options: vec![],
        })),
        is_streaming: false,
    });
    let flow = Flow {
        name: vec!["result".into()],
        target: vec!["result".into()],
        query: query.clone(),
        sql_conf: HashMap::new(),
        catalog: "c".into(),
        database: "d".into(),
    };
    assert!(ordered_inputs(&graph, &flow, &query, false, |_| false).is_err());
    let references = spec::QueryPlan::new(spec::QueryNode::WithRelations {
        root: Box::new(query),
        references: vec![],
    });
    assert!(ordered_inputs(&graph, &flow, &references, false, |_| false).is_err());
    Ok(())
}
