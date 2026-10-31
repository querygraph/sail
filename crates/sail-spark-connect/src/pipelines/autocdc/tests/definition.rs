//! PythonPipelineSuite "AutoCDC API" registration cases.

use super::*;

#[test]
fn minimal_flow_registers_default_scd1_without_deletes_or_selection() -> SparkResult<()> {
    let flow = definition(details("src", &["value"], "timestamp"))?;
    assert_eq!(flow.source, names(&["src"]));
    assert_eq!(flow.change_args.keys, columns(&["value"]));
    assert_eq!(expression_sql(&flow.change_args.sequencing), "timestamp");
    assert_eq!(flow.change_args.delete_condition, None);
    assert_eq!(flow.change_args.column_selection, None);
    assert_eq!(flow.change_args.scd_type, ScdType::Type1);
    assert_eq!(flow.change_args.scd_type.label(), "SCD1");
    Ok(())
}

#[test]
fn composite_keys_are_forwarded_in_order() -> SparkResult<()> {
    let flow = definition(details("src", &["value", "timestamp"], "timestamp"))?;
    assert_eq!(flow.change_args.keys, columns(&["value", "timestamp"]));
    Ok(())
}

#[test]
fn apply_as_deletes_is_forwarded_as_a_delete_condition() -> SparkResult<()> {
    let flow = definition(AutoCdcFlowDetails {
        apply_as_deletes: Some(expression("value % 2 = 0")),
        ..details("src", &["value"], "timestamp")
    })?;
    let condition = flow.change_args.delete_condition;
    assert!(
        condition.is_some(),
        "expected apply_as_deletes to populate the delete condition"
    );
    let sql = condition.as_ref().map(expression_sql).unwrap_or_default();
    assert!(sql.contains("value"), "{sql}");
    assert!(sql.contains('0'), "{sql}");
    Ok(())
}

#[test]
fn column_list_is_forwarded_as_include_columns() -> SparkResult<()> {
    let flow = definition(AutoCdcFlowDetails {
        column_list: vec![attribute("value"), attribute("timestamp")],
        ..details("src", &["value"], "timestamp")
    })?;
    assert_eq!(
        flow.change_args.column_selection,
        Some(ColumnSelection::Include(columns(&["value", "timestamp"])))
    );
    Ok(())
}

#[test]
fn except_column_list_is_forwarded_as_exclude_columns() -> SparkResult<()> {
    let flow = definition(AutoCdcFlowDetails {
        except_column_list: vec![attribute("timestamp")],
        ..details("src", &["value"], "timestamp")
    })?;
    assert_eq!(
        flow.change_args.column_selection,
        Some(ColumnSelection::Exclude(columns(&["timestamp"])))
    );
    Ok(())
}

#[test]
fn multipart_keys_column_is_rejected_at_flow_registration() {
    check_error(
        definition(details("src", &["a.b"], "timestamp")),
        "AUTOCDC_MULTIPART_COLUMN_IDENTIFIER",
        "22023",
        &[("columnName", "a.b"), ("nameParts", "a, b")],
    );
}

#[test]
fn multipart_column_list_entry_is_rejected_at_flow_registration() {
    check_error(
        definition(AutoCdcFlowDetails {
            column_list: vec![attribute("nested.field")],
            ..details("src", &["value"], "timestamp")
        }),
        "AUTOCDC_MULTIPART_COLUMN_IDENTIFIER",
        "22023",
        &[
            ("columnName", "nested.field"),
            ("nameParts", "nested, field"),
        ],
    );
}

#[test]
fn column_object_form_of_arguments_is_honored() -> SparkResult<()> {
    // `col("value")`, `col("timestamp")` and `expr("value % 2 = 0")`.
    let flow = definition(AutoCdcFlowDetails {
        source: Some("src".into()),
        keys: vec![attribute("value")],
        sequence_by: Some(attribute("timestamp")),
        apply_as_deletes: Some(expression("value % 2 = 0")),
        ..Default::default()
    })?;
    assert_eq!(flow.change_args.keys, columns(&["value"]));
    assert_eq!(expression_sql(&flow.change_args.sequencing), "timestamp");
    let sql = flow
        .change_args
        .delete_condition
        .as_ref()
        .map(expression_sql)
        .unwrap_or_default();
    assert!(sql.contains("value") && sql.contains('0'), "{sql}");
    Ok(())
}

#[test]
fn non_attribute_expression_in_keys_is_rejected() {
    let result = definition(AutoCdcFlowDetails {
        keys: vec![expression("value + 1")],
        ..details("src", &["value"], "timestamp")
    });
    check_error(
        result,
        "AUTOCDC_NON_COLUMN_IDENTIFIER",
        "22023",
        &[("expression", "(value + 1)")],
    );
}

#[test]
fn specifying_both_column_list_and_except_column_list_is_rejected() {
    check_error(
        definition(AutoCdcFlowDetails {
            column_list: vec![attribute("value")],
            except_column_list: vec![attribute("timestamp")],
            ..details("src", &["value"], "timestamp")
        }),
        "AUTOCDC_BOTH_COLUMN_LIST_AND_EXCEPT_COLUMN_LIST",
        "42613",
        &[],
    );
}

#[test]
fn missing_source_and_sequence_by_are_rejected_in_handler_order() {
    check_error(
        definition(AutoCdcFlowDetails {
            source: None,
            sequence_by: None,
            keys: vec![attribute("value")],
            ..Default::default()
        }),
        "AUTOCDC_MISSING_SOURCE",
        "22023",
        &[],
    );
    check_error(
        definition(AutoCdcFlowDetails {
            source: Some("src".into()),
            sequence_by: None,
            keys: vec![attribute("value")],
            ..Default::default()
        }),
        "AUTOCDC_MISSING_SEQUENCE_BY",
        "22023",
        &[],
    );
    // The source is parsed before the keys, so an empty name fails first.
    check_error(
        definition(details("", &["a.b"], "timestamp")),
        "PARSE_EMPTY_STATEMENT",
        "42617",
        &[],
    );
}

#[test]
fn empty_keys_are_rejected_after_the_other_arguments() {
    check_error(
        definition(details("src", &[], "timestamp")),
        "AUTOCDC_EMPTY_KEYS",
        "22023",
        &[],
    );
}

#[test]
fn unrecognized_scd_type_is_unsupported_and_scd1_values_are_accepted() -> SparkResult<()> {
    for value in [0, 1] {
        let flow = definition(AutoCdcFlowDetails {
            stored_as_scd_type: value,
            ..details("src", &["value"], "timestamp")
        })?;
        assert_eq!(flow.change_args.scd_type, ScdType::Type1);
    }
    let result = definition(AutoCdcFlowDetails {
        stored_as_scd_type: 2,
        ..details("src", &["value"], "timestamp")
    });
    let message = format!("{result:?}");
    assert!(
        message.contains("Unsupported AutoCDC SCD type: UNRECOGNIZED"),
        "{message}"
    );
    Ok(())
}

#[test]
fn multi_part_source_is_kept_for_resolution_against_the_graph() -> SparkResult<()> {
    let flow = definition(details(
        "some_catalog.some_schema.src",
        &["value"],
        "timestamp",
    ))?;
    assert_eq!(flow.source, names(&["some_catalog", "some_schema", "src"]));
    assert!(matches!(
        flow.query().node,
        spec::QueryNode::Read {
            is_streaming: true,
            ..
        }
    ));
    Ok(())
}

fn unsupported_message<T>(result: SparkResult<T>) -> Option<String> {
    match result {
        Err(SparkError::NotSupported(message)) => Some(message),
        _ => None,
    }
}

#[test]
fn four_part_source_is_rejected_before_key_checks() -> SparkResult<()> {
    // `GraphIdentifierManager.parseTableIdentifier` -> `toTableIdentifier`,
    // reached after the missing-argument checks and before any key parsing.
    for details in [
        details("a.b.c.d", &["value"], "timestamp"),
        details("a.b.c.d", &[], "timestamp"),
        AutoCdcFlowDetails {
            keys: vec![expression("value + 1"), attribute("a..b")],
            column_list: vec![attribute("x")],
            except_column_list: vec![attribute("y")],
            stored_as_scd_type: 7,
            ..details("a.b.c.d", &[], "timestamp")
        },
    ] {
        assert_eq!(
            unsupported_message(definition(details)).as_deref(),
            Some("4+ part table identifier a.b.c.d is not supported.")
        );
    }
    assert_eq!(
        unsupported_message(definition(details(
            "Cat.Sch.Tbl.extra",
            &["value"],
            "timestamp"
        )))
        .as_deref(),
        Some("4+ part table identifier Cat.Sch.Tbl.extra is not supported.")
    );
    check_error(
        definition(AutoCdcFlowDetails {
            sequence_by: None,
            ..details("a.b.c.d", &["value"], "timestamp")
        }),
        "AUTOCDC_MISSING_SEQUENCE_BY",
        "22023",
        &[],
    );
    let flow = definition(details("`a.b.c.d`", &["value"], "timestamp"))?;
    assert_eq!(flow.source, names(&["a.b.c.d"]));
    Ok(())
}

#[test]
fn malformed_key_and_list_names_are_rejected_by_the_attribute_parser() {
    // The planner's `UnresolvedAttribute.quotedString` runs before
    // `asUnqualifiedColumnName`, for keys and both column lists.
    for name in ["a..b", "`a", "`a`b"] {
        let expected = [("name", name)];
        check_error(
            definition(details("src", &[name], "timestamp")),
            "INVALID_ATTRIBUTE_NAME_SYNTAX",
            "42601",
            &expected,
        );
        check_error(
            definition(AutoCdcFlowDetails {
                column_list: vec![attribute(name)],
                ..details("src", &["value"], "timestamp")
            }),
            "INVALID_ATTRIBUTE_NAME_SYNTAX",
            "42601",
            &expected,
        );
        check_error(
            definition(AutoCdcFlowDetails {
                except_column_list: vec![attribute(name)],
                ..details("src", &["value"], "timestamp")
            }),
            "INVALID_ATTRIBUTE_NAME_SYNTAX",
            "42601",
            &expected,
        );
    }
}

#[test]
fn unquoted_spaces_keep_their_part_and_dots_still_split() {
    check_error(
        definition(details("src", &["a b.c"], "timestamp")),
        "AUTOCDC_MULTIPART_COLUMN_IDENTIFIER",
        "22023",
        &[("columnName", "a b.c"), ("nameParts", "a b, c")],
    );
}

#[test]
fn quoted_dots_make_a_single_column_name() -> SparkResult<()> {
    let flow = definition(AutoCdcFlowDetails {
        column_list: vec![attribute("`x.y`"), attribute("`q``r`")],
        ..details("src", &["`a..b`"], "timestamp")
    })?;
    assert_eq!(flow.change_args.keys, columns(&["a..b"]));
    assert_eq!(
        flow.change_args.column_selection,
        Some(ColumnSelection::Include(columns(&["x.y", "q`r"])))
    );
    Ok(())
}

#[test]
fn competing_invalid_arguments_follow_handler_order() {
    // Keys are parsed before the column lists are examined.
    check_error(
        definition(AutoCdcFlowDetails {
            column_list: vec![attribute("a")],
            except_column_list: vec![attribute("b")],
            stored_as_scd_type: 7,
            ..details("src", &["a..b"], "timestamp")
        }),
        "INVALID_ATTRIBUTE_NAME_SYNTAX",
        "42601",
        &[("name", "a..b")],
    );
    // Both lists present is reported before any list member is parsed.
    check_error(
        definition(AutoCdcFlowDetails {
            column_list: vec![attribute("a..b")],
            except_column_list: vec![attribute("b")],
            ..details("src", &["value"], "timestamp")
        }),
        "AUTOCDC_BOTH_COLUMN_LIST_AND_EXCEPT_COLUMN_LIST",
        "42613",
        &[],
    );
    // A malformed list member is reported before the SCD type and empty keys.
    check_error(
        definition(AutoCdcFlowDetails {
            except_column_list: vec![attribute("a..b")],
            stored_as_scd_type: 7,
            ..details("src", &[], "timestamp")
        }),
        "INVALID_ATTRIBUTE_NAME_SYNTAX",
        "42601",
        &[("name", "a..b")],
    );
    // A non-attribute key is reported before a malformed later key.
    check_error(
        definition(AutoCdcFlowDetails {
            keys: vec![expression("value + 1"), attribute("a..b")],
            ..details("src", &[], "timestamp")
        }),
        "AUTOCDC_NON_COLUMN_IDENTIFIER",
        "22023",
        &[("expression", "(value + 1)")],
    );
}
