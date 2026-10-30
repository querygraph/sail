use std::convert::Infallible;
use std::sync::Arc;

use super::super::{
    ObjectName, QueryNode, UnresolvedFunction, WildcardOptions, WildcardReplaceColumn,
};
use super::*;

fn subquery(id: i64) -> Expr {
    Expr::ScalarSubquery {
        subquery: Box::new(QueryPlan {
            node: QueryNode::Empty {
                produce_one_row: false,
            },
            plan_id: Some(id),
        }),
    }
}

fn sort(child: Expr) -> SortOrder {
    SortOrder {
        child: Box::new(child),
        direction: super::super::SortDirection::Ascending,
        null_ordering: super::super::NullOrdering::NullsFirst,
    }
}

#[test]
fn expression_lists_preserve_order_repetition_and_query_identity() -> Result<(), Infallible> {
    let expr = Expr::UnresolvedFunction(UnresolvedFunction {
        function_name: ObjectName::bare("f"),
        arguments: vec![subquery(8), subquery(3), subquery(8)],
        named_arguments: vec![("named".into(), subquery(2))],
        filter: Some(Box::new(subquery(5))),
        order_by: Some(vec![sort(subquery(1))]),
        is_user_defined_function: false,
        is_distinct: false,
        is_internal: None,
        ignore_nulls: None,
    });
    // Borrow the actual queries; the traversal need not clone/resolve/rewrite
    // anything and must not deduplicate equal queries or sort their plan IDs.
    let mut seen = vec![];
    expr.visit_subqueries(&mut |query| {
        seen.push(query);
        Ok::<_, Infallible>(())
    })?;
    assert_eq!(
        seen.iter().map(|query| query.plan_id).collect::<Vec<_>>(),
        vec![Some(8), Some(3), Some(8), Some(2), Some(5), Some(1)]
    );
    let Expr::UnresolvedFunction(function) = &expr else {
        unreachable!()
    };
    let Expr::ScalarSubquery { subquery } = &function.arguments[0] else {
        unreachable!()
    };
    assert!(std::ptr::eq(seen[0], subquery.as_ref()));
    Ok(())
}

#[test]
fn first_callback_error_stops_traversal_and_keeps_the_original_error() {
    let error = Arc::new("stop at the second occurrence");
    let expr = Expr::Rollup(vec![subquery(9), subquery(2), subquery(1)]);
    let mut seen = vec![];
    let result = expr.visit_subqueries(&mut |query| {
        seen.push(query.plan_id);
        if query.plan_id == Some(2) {
            Err(Arc::clone(&error))
        } else {
            Ok(())
        }
    });
    assert_eq!(seen, vec![Some(9), Some(2)]);
    assert!(result.is_err_and(|actual| Arc::ptr_eq(&actual, &error)));
}

#[test]
fn wildcard_replacements_and_window_bounds_are_expression_children() -> Result<(), Infallible> {
    let expr = Expr::UnresolvedStar {
        target: None,
        plan_id: None,
        wildcard_options: WildcardOptions {
            replace_columns: Some(vec![WildcardReplaceColumn {
                expression: Box::new(Expr::Window {
                    window_function: Box::new(subquery(7)),
                    window: Window::Unnamed {
                        cluster_by: vec![subquery(6)],
                        partition_by: vec![subquery(5)],
                        order_by: vec![sort(subquery(4))],
                        frame: Some(super::super::WindowFrame {
                            frame_type: super::super::WindowFrameType::Range,
                            lower: WindowFrameBoundary::Preceding(Box::new(subquery(3))),
                            upper: WindowFrameBoundary::Following(Box::new(subquery(2))),
                        }),
                    },
                }),
                column_name: "result".into(),
                as_keyword: true,
            }]),
            ..Default::default()
        },
    };
    let mut seen = vec![];
    expr.visit_subqueries(&mut |query| {
        seen.push(query.plan_id);
        Ok::<_, Infallible>(())
    })?;
    assert_eq!(seen, (2..=7).rev().map(Some).collect::<Vec<_>>());
    Ok(())
}

#[test]
fn query_contents_and_plan_id_references_remain_the_callers_responsibility()
-> Result<(), Infallible> {
    let expr = Expr::Subquery {
        plan_id: 90,
        subquery_type: super::super::SubqueryType::In,
        in_subquery_values: vec![Expr::ScalarSubquery {
            subquery: Box::new(QueryPlan {
                node: QueryNode::Project {
                    input: None,
                    expressions: vec![subquery(12)],
                },
                plan_id: Some(6),
            }),
        }],
        negated: false,
    };
    let mut seen = vec![];
    expr.visit_subqueries(&mut |query| {
        seen.push(query.plan_id);
        Ok::<_, Infallible>(())
    })?;
    // No invented lookup for reference 90 or automatic descent into query 6.
    assert_eq!(seen, vec![Some(6)]);
    Ok(())
}
