use super::*;
use crate::parser::parse_one_statement;

#[test]
fn replace_where_preserves_ast_and_plan_column_matching() -> Result<(), Box<dyn std::error::Error>>
{
    for by_name in [false, true] {
        let clause = if by_name { "BY NAME " } else { "" };
        let sql = format!("INSERT INTO output {clause}REPLACE WHERE id=2 SELECT 2 AS id");
        let statement = parse_one_statement(&sql)?;
        let Statement::InsertIntoAndReplace {
            by_name: parsed, ..
        } = &statement
        else {
            return Err("expected predicate replacement AST".into());
        };
        assert_eq!(parsed.is_some(), by_name);
        let spec::Plan::Command(spec::CommandPlan {
            node:
                spec::CommandNode::InsertInto {
                    mode:
                        spec::InsertMode::Replace {
                            condition,
                            by_name: actual,
                        },
                    ..
                },
            ..
        }) = from_ast_statement(statement)?
        else {
            return Err("expected predicate replacement command".into());
        };
        assert_eq!(actual, by_name);
        assert!(condition.source.is_some());
    }
    Ok(())
}
