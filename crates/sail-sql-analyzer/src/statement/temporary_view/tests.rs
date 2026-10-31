use super::*;
use crate::parser::parse_one_statement;
use crate::statement::from_ast_statement;

fn command(sql: &str) -> SqlResult<spec::CommandNode> {
    let plan = from_ast_statement(parse_one_statement(sql)?)?;
    match plan {
        spec::Plan::Command(plan) => Ok(plan.node),
        _ => Err(SqlError::invalid("expected command")),
    }
}

#[test]
fn query_view_names_use_spark_session_namespace_normalization() -> SqlResult<()> {
    for global in ["", "GLOBAL "] {
        for (name, expected) in [
            ("v", "v"),
            ("session.v", "v"),
            ("SeSsIoN.v", "v"),
            ("system.session.v", "v"),
            ("`seſſıon`.v", "v"),
            ("`ſyſtem`.`SESSİON`.v", "v"),
            ("session.`a.b`", "a.b"),
            ("`session.v`", "session.v"),
        ] {
            let node = command(&format!("CREATE {global}TEMPORARY VIEW {name} AS SELECT 1"))?;
            let spec::CommandNode::CreateTemporaryView {
                view, is_global, ..
            } = node
            else {
                return Err(SqlError::invalid("expected temporary view"));
            };
            assert_eq!(String::from(view), expected);
            assert_eq!(is_global, !global.is_empty());
        }
    }
    Ok(())
}

#[test]
fn invalid_query_view_qualifiers_keep_structured_parse_details() -> SqlResult<()> {
    for (name, object_name, qualifier) in [
        ("some_database.tv", "`tv`", "`some_database`"),
        ("db.ns.v", "`v`", "`db`.`ns`"),
        ("system.other.v", "`v`", "`system`.`other`"),
        (
            "system.session.extra.v",
            "`v`",
            "`system`.`session`.`extra`",
        ),
        ("`a.b`.`c.d`", "`c`.`d`", "`a`.`b`"),
    ] {
        let error = command(&format!("CREATE TEMPORARY VIEW {name} AS SELECT 1"))
            .err()
            .ok_or_else(|| SqlError::invalid("expected parse error"))?;
        let SqlError::Structured(error) = error else {
            return Err(SqlError::invalid("lost structured parse error"));
        };
        assert_eq!(
            error.class,
            "org.apache.spark.sql.catalyst.parser.ParseException"
        );
        assert_eq!(error.condition, "INVALID_TEMP_OBJ_QUALIFIER");
        assert_eq!(error.sql_state, "42602");
        assert_eq!(
            error.parameters,
            HashMap::from([
                ("objectType".into(), "VIEW".into()),
                ("objectName".into(), object_name.into()),
                ("qualifier".into(), qualifier.into()),
            ])
        );
        assert_eq!(
            error.message,
            format!(
                "Temporary VIEW {object_name} cannot be qualified with {qualifier}. Temporary objects can only be qualified with SESSION or SYSTEM.SESSION."
            )
        );
    }
    Ok(())
}

#[test]
fn malformed_diagnostic_identifier_is_still_a_parse_exception() -> SqlResult<()> {
    let error = command("CREATE TEMPORARY VIEW db.`a``b` AS SELECT 1")
        .err()
        .ok_or_else(|| SqlError::invalid("expected parse error"))?;
    let SqlError::Structured(error) = error else {
        return Err(SqlError::invalid("lost structured error"));
    };
    assert_eq!(
        error.class,
        "org.apache.spark.sql.catalyst.parser.ParseException"
    );
    assert_eq!(error.condition, "INVALID_ATTRIBUTE_NAME_SYNTAX");
    assert_eq!(error.sql_state, "42601");
    assert_eq!(
        error.parameters,
        HashMap::from([("name".into(), "a`b".into())])
    );
    Ok(())
}

#[test]
fn persisted_create_and_drop_qualifiers_are_unchanged() -> SqlResult<()> {
    for sql in ["CREATE VIEW db.v AS SELECT 1", "DROP VIEW db.v"] {
        let node = command(sql)?;
        let name = match node {
            spec::CommandNode::CreateView { view, .. }
            | spec::CommandNode::DropView { view, .. } => view,
            _ => return Err(SqlError::invalid("expected persisted view command")),
        };
        assert_eq!(Vec::<String>::from(name), vec!["db", "v"]);
    }
    Ok(())
}
