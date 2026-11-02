//! The shapes of `test_a_persisted_view_needs_named_columns_wherever_they_come_from`,
//! measured on Spark 4.2.1: a persisted view is refused when any output column
//! has no name of its own.

use sail_common::spec;
use sail_sql_analyzer::parser::parse_one_statement;
use sail_sql_analyzer::statement::from_ast_statement;

use super::output_has_unnamed_column;

const SHAPES: &[(&str, bool)] = &[
    ("SELECT 1 + 1 FROM t1 LIMIT 1", true),
    ("SELECT id FROM t1 LIMIT 1", false),
    ("WITH s AS (SELECT 1 + 1) SELECT * FROM s", true),
    ("SELECT count(*) FROM t1", true),
    ("SELECT count(*) AS n FROM t1", false),
    ("SELECT id, count(*) FROM t1 GROUP BY id", true),
    ("SELECT max(id) FROM t1", true),
    ("SELECT * FROM t1 UNION ALL SELECT * FROM t1", false),
    ("SELECT 1 + 1 FROM t1 UNION ALL SELECT 2 + 2 FROM t1", true),
    ("SELECT DISTINCT id + 1 FROM t1", true),
    ("SELECT id + 1 FROM (SELECT id FROM t1) sub", true),
    ("SELECT id + 1 AS next FROM (SELECT id FROM t1) sub", false),
    ("SELECT * FROM (SELECT 1 + 1) sub", true),
    ("WITH s AS (SELECT 1 + 1 AS two) SELECT * FROM s", false),
];

fn query(sql: &str) -> spec::QueryPlan {
    let statement = parse_one_statement(sql).unwrap_or_else(|e| unreachable!("{sql}: {e}"));
    match from_ast_statement(statement) {
        Ok(spec::Plan::Query(plan)) => plan,
        other => unreachable!("{sql}: not a query: {other:?}"),
    }
}

#[test]
fn an_unnamed_output_column_is_found_wherever_it_comes_from() {
    for (sql, unnamed) in SHAPES {
        let plan = query(sql);
        assert_eq!(
            output_has_unnamed_column(&plan, &mut Vec::new()),
            *unnamed,
            "{sql}\n{plan:#?}"
        );
    }
}
