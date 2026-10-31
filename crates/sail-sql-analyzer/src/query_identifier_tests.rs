use sail_common::spec;

use crate::parser::parse_one_statement;
use crate::statement::from_ast_statement;

type TestResult = Result<(), Box<dyn std::error::Error>>;
fn query(sql: &str) -> Result<spec::Plan, Box<dyn std::error::Error>> {
    Ok(from_ast_statement(parse_one_statement(sql)?)?)
}

#[test]
fn literal_identifier_is_the_same_named_relation_before_binding() -> TestResult {
    for (literal, ordinary) in [
        ("SELECT * FROM IDENTIFIER('c.t')", "SELECT * FROM c.t"),
        (
            "SELECT * FROM IDENTIFIER('c.`a.b`') AS x",
            "SELECT * FROM c.`a.b` AS x",
        ),
        (
            "SELECT * FROM IDENTIFIER('t') TABLESAMPLE (10 PERCENT)",
            "SELECT * FROM t TABLESAMPLE (10 PERCENT)",
        ),
    ] {
        assert_eq!(query(literal)?, query(ordinary)?, "{literal}");
    }
    assert!(query("SELECT * FROM IDENTIFIER('a..b')").is_err());
    Ok(())
}

#[test]
fn computed_and_null_identifiers_remain_late_bound() -> TestResult {
    fn dynamic(query: &spec::QueryPlan) -> bool {
        if matches!(
            query.node,
            spec::QueryNode::Read {
                read_type: spec::ReadType::DynamicTable(_),
                ..
            }
        ) {
            return true;
        }
        let mut found = false;
        let _: Result<(), std::convert::Infallible> = query.visit_children(&mut |child| {
            if let spec::QueryChild::Query(query) = child {
                found |= dynamic(query);
            }
            Ok(())
        });
        found
    }
    for sql in [
        "SELECT * FROM IDENTIFIER(concat('c.', 't'))",
        "SELECT * FROM IDENTIFIER(CAST('t' AS STRING))",
        "SELECT * FROM IDENTIFIER(NULL)",
    ] {
        let spec::Plan::Query(query) = query(sql)? else {
            return Err("not a query".into());
        };
        assert!(dynamic(&query), "{sql}");
    }
    Ok(())
}
