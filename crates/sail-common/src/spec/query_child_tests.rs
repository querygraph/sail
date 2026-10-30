use std::convert::Infallible;
use std::sync::Arc;

use super::*;
use crate::spec::{Join, JoinCriteria, JoinType, ObjectName, QueryNode, ReadNamedTable};

fn read(name: &str) -> QueryPlan {
    QueryPlan::new(QueryNode::Read {
        read_type: ReadType::NamedTable(Box::new(ReadNamedTable {
            name: ObjectName::bare(name),
            temporal: None,
            sample: None,
            options: vec![],
        })),
        is_streaming: false,
    })
}
fn scalar(name: &str) -> Expr {
    Expr::ScalarSubquery {
        subquery: Box::new(read(name)),
    }
}
fn names(plan: &QueryPlan, seen: &mut Vec<String>) -> Result<(), Infallible> {
    if let QueryNode::Read {
        read_type: ReadType::NamedTable(table),
        ..
    } = &plan.node
    {
        seen.push(table.name.parts()[0].as_ref().to_string());
    }
    plan.visit_children(&mut |child| match child {
        QueryChild::Expression(expr) => expr.visit_subqueries(&mut |query| names(query, seen)),
        QueryChild::Window(window) => window.visit_subqueries(&mut |query| names(query, seen)),
        QueryChild::Query(query) => names(query, seen),
    })
}

#[test]
fn expression_subqueries_precede_inputs_and_join_condition_precedes_both_sides()
-> Result<(), Infallible> {
    let plan = QueryPlan::new(QueryNode::Project {
        expressions: vec![scalar("projection"), scalar("projection")],
        input: Some(Box::new(QueryPlan::new(QueryNode::Join(Join {
            left: Box::new(read("z_left")),
            right: Box::new(read("a_right")),
            join_type: JoinType::Inner,
            join_data_type: None,
            join_criteria: Some(JoinCriteria::On(scalar("condition"))),
        })))),
    });
    let mut seen = vec![];
    names(&plan, &mut seen)?;
    assert_eq!(
        seen,
        ["projection", "projection", "condition", "z_left", "a_right"]
    );
    Ok(())
}

#[test]
fn cte_definitions_and_reference_lists_are_unfiltered_structural_children() -> Result<(), Infallible>
{
    let ctes = QueryPlan::new(QueryNode::WithCtes {
        recursive: false,
        ctes: vec![
            ("same".into(), read("unused")),
            ("same".into(), read("last")),
        ],
        input: Box::new(read("body")),
    });
    let plan = QueryPlan::new(QueryNode::WithRelations {
        root: Box::new(ctes),
        references: vec![read("ref"), read("ref")],
    });
    let mut seen = vec![];
    names(&plan, &mut seen)?;
    // No CTE shadowing/name lookup or reference deduplication is imposed here.
    assert_eq!(seen, ["ref", "ref", "unused", "last", "body"]);
    Ok(())
}

#[test]
fn callback_borrows_original_children_and_errors_before_later_inputs() {
    let plan = QueryPlan::new(QueryNode::Filter {
        condition: scalar("condition"),
        input: Box::new(read("input")),
    });
    let error = Arc::new("original error");
    let mut seen = vec![];
    let mut later_inputs = 0;
    let result = plan.visit_children(&mut |child| match child {
        QueryChild::Expression(expr) => {
            seen.push(expr);
            Err(Arc::clone(&error))
        }
        QueryChild::Query(_) | QueryChild::Window(_) => {
            later_inputs += 1;
            Ok(())
        }
    });
    assert!(result.is_err_and(|e| Arc::ptr_eq(&e, &error)));
    assert_eq!(later_inputs, 0);
    let QueryNode::Filter { condition, .. } = &plan.node else {
        unreachable!()
    };
    assert_eq!(seen.len(), 1);
    assert!(std::ptr::eq(seen[0], condition));
}

#[test]
fn sampling_temporal_dynamic_and_function_fields_expose_their_expressions() -> Result<(), Infallible>
{
    let sample = || TableSample {
        method: TableSampleMethod::Percent {
            value: scalar("sample"),
        },
        seed: None,
    };
    let plans = [
        QueryPlan::new(QueryNode::Read {
            read_type: ReadType::NamedTable(Box::new(ReadNamedTable {
                name: ObjectName::bare("outer"),
                temporal: Some(TableTemporal::Version {
                    value: scalar("version"),
                }),
                sample: Some(sample()),
                options: vec![],
            })),
            is_streaming: false,
        }),
        QueryPlan::new(QueryNode::Read {
            read_type: ReadType::DynamicTable(Box::new(crate::spec::ReadDynamicTable {
                name: scalar("dynamic"),
                sample: Some(sample()),
                options: vec![],
            })),
            is_streaming: false,
        }),
        QueryPlan::new(QueryNode::Read {
            read_type: ReadType::Udtf(Box::new(crate::spec::ReadUdtf {
                name: ObjectName::bare("f"),
                arguments: vec![scalar("positional")],
                named_arguments: vec![("key".into(), scalar("named"))],
                options: vec![],
            })),
            is_streaming: false,
        }),
        QueryPlan::new(QueryNode::Read {
            read_type: ReadType::DataSource(Box::new(crate::spec::ReadDataSource {
                format: None,
                schema: None,
                options: vec![],
                paths: vec![],
                predicates: vec![scalar("predicate")],
            })),
            is_streaming: false,
        }),
    ];
    let mut seen = vec![];
    for plan in plans {
        names(&plan, &mut seen)?;
    }
    assert_eq!(
        seen,
        [
            "outer",
            "version",
            "sample",
            "dynamic",
            "sample",
            "positional",
            "named",
            "predicate"
        ]
    );
    Ok(())
}

#[test]
fn named_window_fields_visit_before_the_input_without_resolving_the_window()
-> Result<(), Infallible> {
    let plan = QueryPlan::new(QueryNode::NamedWindows {
        input: Box::new(read("input")),
        windows: vec![(
            "w".into(),
            Window::Unnamed {
                cluster_by: vec![],
                partition_by: vec![scalar("partition")],
                order_by: vec![],
                frame: None,
            },
        )],
    });
    let mut seen = vec![];
    names(&plan, &mut seen)?;
    assert_eq!(seen, ["partition", "input"]);
    Ok(())
}
