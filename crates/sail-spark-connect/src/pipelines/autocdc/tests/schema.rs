//! AutoCdcFlowSuite: schema-level validators (field names only).

use super::*;

fn change_args(
    keys: &[&str],
    scd_type: ScdType,
    column_selection: Option<ColumnSelection>,
) -> SparkResult<ChangeArgs> {
    ChangeArgs::new(
        columns(keys),
        spec::Expr::try_from(attribute("seq"))?,
        scd_type,
        None,
        column_selection,
    )
}

// `threeColumnSourceDf`: id INT, name STRING, seq LONG.
fn three_column_source() -> Vec<String> {
    names(&["id", "name", "seq"])
}

#[test]
fn schema_appends_metadata_to_the_source_schema_without_selection() -> SparkResult<()> {
    let args = change_args(&["id"], ScdType::Type1, None)?;
    assert_eq!(
        output_field_names(&args, &three_column_source(), Case::Insensitive)?,
        names(&["id", "name", "seq", "__spark_autocdc_metadata"])
    );
    assert_eq!(METADATA_COLUMN, format!("{RESERVED_PREFIX}metadata"));
    assert_eq!(DELETE_SEQUENCE_FIELD, "deleteSequence");
    assert_eq!(UPSERT_SEQUENCE_FIELD, "upsertSequence");
    Ok(())
}

#[test]
fn schema_applies_an_include_columns_selection() -> SparkResult<()> {
    let selection = ColumnSelection::Include(columns(&["id", "seq"]));
    let args = change_args(&["id"], ScdType::Type1, Some(selection))?;
    assert_eq!(
        output_field_names(&args, &three_column_source(), Case::Insensitive)?,
        names(&["id", "seq", METADATA_COLUMN])
    );
    Ok(())
}

#[test]
fn schema_applies_an_exclude_columns_selection() -> SparkResult<()> {
    let selection = ColumnSelection::Exclude(columns(&["name"]));
    let args = change_args(&["id"], ScdType::Type1, Some(selection))?;
    assert_eq!(
        output_field_names(&args, &three_column_source(), Case::Insensitive)?,
        names(&["id", "seq", METADATA_COLUMN])
    );
    Ok(())
}

#[test]
fn scd2_is_rejected_at_construction() -> SparkResult<()> {
    let args = change_args(&["id"], ScdType::Type2, None)?;
    assert_eq!(args.scd_type.label(), "SCD2");
    check_error(
        output_field_names(&args, &three_column_source(), Case::Insensitive),
        "AUTOCDC_SCD2_NOT_SUPPORTED",
        "0A000",
        &[],
    );
    Ok(())
}

#[test]
fn contract_a_source_column_with_the_reserved_prefix_is_rejected() -> SparkResult<()> {
    let conflicting = format!("{RESERVED_PREFIX}foo");
    let fields = names(&["id", "seq", &conflicting]);
    let args = change_args(&["id"], ScdType::Type1, None)?;
    check_error(
        output_field_names(&args, &fields, Case::Insensitive),
        "AUTOCDC_RESERVED_COLUMN_NAME_PREFIX_CONFLICT",
        "42710",
        &[
            ("caseSensitivity", "case-insensitive"),
            ("columnName", &conflicting),
            ("schemaName", "changeDataFeed"),
            ("reservedColumnNamePrefix", RESERVED_PREFIX),
        ],
    );
    Ok(())
}

#[test]
fn contract_change_args_referencing_a_reserved_prefix_column_are_rejected() -> SparkResult<()> {
    let reserved = format!("{RESERVED_PREFIX}foo");
    let fields = three_column_source();
    let keys = change_args(&[&reserved], ScdType::Type1, None)?;
    assert_eq!(
        structured(output_field_names(&keys, &fields, Case::Insensitive))
            .map(|error| error.condition),
        Some("AUTOCDC_KEY_NOT_IN_SELECTED_SCHEMA".into())
    );
    let include = ColumnSelection::Include(columns(&["id", &reserved]));
    let args = change_args(&["id"], ScdType::Type1, Some(include))?;
    assert_eq!(
        structured(output_field_names(&args, &fields, Case::Insensitive))
            .map(|error| error.condition),
        Some("AUTOCDC_COLUMNS_NOT_FOUND_IN_SCHEMA".into())
    );
    let exclude = ColumnSelection::Exclude(columns(&[&reserved]));
    let args = change_args(&["id"], ScdType::Type1, Some(exclude))?;
    assert_eq!(
        structured(output_field_names(&args, &fields, Case::Insensitive))
            .map(|error| error.condition),
        Some("AUTOCDC_COLUMNS_NOT_FOUND_IN_SCHEMA".into())
    );
    Ok(())
}

#[test]
fn a_source_column_equal_to_the_metadata_column_is_rejected() -> SparkResult<()> {
    let fields = names(&["id", "seq", METADATA_COLUMN]);
    let args = change_args(&["id"], ScdType::Type1, None)?;
    check_error(
        output_field_names(&args, &fields, Case::Insensitive),
        "AUTOCDC_RESERVED_COLUMN_NAME_PREFIX_CONFLICT",
        "42710",
        &[
            ("caseSensitivity", "case-insensitive"),
            ("columnName", METADATA_COLUMN),
            ("schemaName", "changeDataFeed"),
            ("reservedColumnNamePrefix", RESERVED_PREFIX),
        ],
    );
    Ok(())
}

#[test]
fn an_uppercase_reserved_prefix_column_is_rejected_when_case_insensitive() -> SparkResult<()> {
    let conflicting = format!("{RESERVED_PREFIX}foo").to_uppercase();
    let fields = names(&["id", "seq", &conflicting]);
    let args = change_args(&["id"], ScdType::Type1, None)?;
    check_error(
        output_field_names(&args, &fields, Case::Insensitive),
        "AUTOCDC_RESERVED_COLUMN_NAME_PREFIX_CONFLICT",
        "42710",
        &[
            ("caseSensitivity", "case-insensitive"),
            ("columnName", &conflicting),
            ("schemaName", "changeDataFeed"),
            ("reservedColumnNamePrefix", RESERVED_PREFIX),
        ],
    );
    Ok(())
}

#[test]
fn an_uppercase_reserved_prefix_column_is_allowed_when_case_sensitive() -> SparkResult<()> {
    let name = format!("{RESERVED_PREFIX}foo").to_uppercase();
    let fields = names(&["id", "seq", &name]);
    let args = change_args(&["id"], ScdType::Type1, None)?;
    assert_eq!(
        output_field_names(&args, &fields, Case::Sensitive)?,
        names(&["id", "seq", &name, METADATA_COLUMN])
    );
    Ok(())
}

#[test]
fn a_key_absent_from_the_change_data_feed_is_rejected() -> SparkResult<()> {
    let fields = names(&["name", "seq"]);
    let args = change_args(&["id"], ScdType::Type1, None)?;
    check_error(
        output_field_names(&args, &fields, Case::Insensitive),
        "AUTOCDC_KEY_NOT_IN_SELECTED_SCHEMA",
        "22023",
        &[
            ("caseSensitivity", "case-insensitive"),
            ("keyColumnName", "id"),
        ],
    );
    Ok(())
}

#[test]
fn a_key_dropped_by_an_include_columns_selection_is_rejected() -> SparkResult<()> {
    let selection = ColumnSelection::Include(columns(&["name", "seq"]));
    let args = change_args(&["id"], ScdType::Type1, Some(selection))?;
    check_error(
        output_field_names(&args, &three_column_source(), Case::Insensitive),
        "AUTOCDC_KEY_NOT_IN_SELECTED_SCHEMA",
        "22023",
        &[
            ("caseSensitivity", "case-insensitive"),
            ("keyColumnName", "id"),
        ],
    );
    Ok(())
}

#[test]
fn a_key_dropped_by_an_exclude_columns_selection_is_rejected() -> SparkResult<()> {
    let selection = ColumnSelection::Exclude(columns(&["id"]));
    let args = change_args(&["id"], ScdType::Type1, Some(selection))?;
    check_error(
        output_field_names(&args, &three_column_source(), Case::Insensitive),
        "AUTOCDC_KEY_NOT_IN_SELECTED_SCHEMA",
        "22023",
        &[
            ("caseSensitivity", "case-insensitive"),
            ("keyColumnName", "id"),
        ],
    );
    Ok(())
}

#[test]
fn auto_cdc_details_constructed_in_rust_select_the_proto_variant() {
    // A construction smoke test only; wire compatibility is established by
    // the PySpark fixture below, not by this.
    let flow = pc::DefineFlow {
        details: Some(Details::AutoCdcFlowDetails(details("src", &["id"], "seq"))),
        ..Default::default()
    };
    assert!(matches!(flow.details, Some(Details::AutoCdcFlowDetails(_))));
}

#[derive(serde::Deserialize)]
struct WireFixture {
    pyspark_version: String,
    hex: String,
    length: usize,
    define_flow_details_oneof: HashMap<String, i32>,
    auto_cdc_flow_details_fields: HashMap<String, i32>,
    scd_type_values: HashMap<String, i32>,
}

fn wire_fixture() -> WireFixture {
    #[expect(clippy::expect_used)]
    serde_json::from_str(include_str!("define_flow_wire.json"))
        .expect("the wire fixture should parse")
}

fn decode_hex(hex: &str) -> Vec<u8> {
    hex.as_bytes()
        .chunks(2)
        .filter_map(|pair| std::str::from_utf8(pair).ok())
        .filter_map(|pair| u8::from_str_radix(pair, 16).ok())
        .collect()
}

#[test]
fn pyspark_define_flow_bytes_decode_into_auto_cdc_details() -> SparkResult<()> {
    // Bytes produced by PySpark 4.2.1's client proto for a DefineFlow with
    // AutoCdcFlowDetails (see define_flow_wire.json for the generator and
    // field numbers). Decoding them with Sail's generated proto and reading
    // every meaningful field checks tags and types against the real client.
    use prost::Message;
    let fixture = wire_fixture();
    assert_eq!(fixture.pyspark_version, "4.2.1");
    assert_eq!(
        fixture
            .define_flow_details_oneof
            .get("auto_cdc_flow_details"),
        Some(&10)
    );
    assert_eq!(
        fixture
            .auto_cdc_flow_details_fields
            .get("stored_as_scd_type"),
        Some(&10)
    );
    assert_eq!(fixture.scd_type_values.get("SCD_TYPE_1"), Some(&1));
    let bytes = decode_hex(&fixture.hex);
    assert_eq!(bytes.len(), fixture.length);
    let command =
        sc::Command::decode(bytes.as_slice()).map_err(|e| SparkError::invalid(e.to_string()))?;
    let Some(sc::command::CommandType::PipelineCommand(pipeline)) = command.command_type else {
        return Err(SparkError::invalid("expected a pipeline command"));
    };
    let Some(pc::CommandType::DefineFlow(flow)) = pipeline.command_type else {
        return Err(SparkError::invalid("expected a DefineFlow"));
    };
    assert_eq!(flow.dataflow_graph_id.as_deref(), Some("graph-1"));
    assert_eq!(flow.flow_name.as_deref(), Some("cdc_flow"));
    assert_eq!(flow.target_dataset_name.as_deref(), Some("target"));
    let Some(Details::AutoCdcFlowDetails(cdc)) = flow.details else {
        return Err(SparkError::invalid("expected AutoCdcFlowDetails"));
    };
    assert_eq!(cdc.source.as_deref(), Some("src"));
    assert_eq!(cdc.stored_as_scd_type, 1);
    assert!(cdc.except_column_list.is_empty());
    let attribute_names = |expressions: &[sc::Expression]| -> Vec<String> {
        expressions
            .iter()
            .filter_map(|e| match &e.expr_type {
                Some(ExprType::UnresolvedAttribute(a)) => Some(a.unparsed_identifier.clone()),
                _ => None,
            })
            .collect()
    };
    assert_eq!(attribute_names(&cdc.keys), names(&["id", "Region"]));
    assert_eq!(attribute_names(&cdc.column_list), names(&["id", "`a.b`"]));
    let expression_text = |expression: &Option<sc::Expression>| -> Option<String> {
        match expression.as_ref().and_then(|e| e.expr_type.as_ref()) {
            Some(ExprType::ExpressionString(s)) => Some(s.expression.clone()),
            _ => None,
        }
    };
    assert_eq!(expression_text(&cdc.sequence_by).as_deref(), Some("seq"));
    assert_eq!(
        expression_text(&cdc.apply_as_deletes).as_deref(),
        Some("op = 'D'")
    );
    // And the definition built from the real bytes carries the same arguments.
    let parsed = definition(cdc)?;
    assert_eq!(parsed.source, names(&["src"]));
    assert_eq!(parsed.change_args.keys, columns(&["id", "Region"]));
    assert_eq!(
        parsed.change_args.column_selection,
        Some(ColumnSelection::Include(columns(&["id", "a.b"])))
    );
    assert!(parsed.change_args.delete_condition.is_some());
    Ok(())
}

#[test]
fn case_insensitive_lookup_uses_jdk_17_root_lowercase_not_rust_lowercase() {
    // Vithkuqi U+10570/U+10597 and Cyrillic TJE U+1C89/U+1C8A gained case
    // mappings after Unicode 13; Java 17's `Locale.ROOT` lowercase leaves them
    // distinct, so Spark's case-insensitive lookup does not match them.
    // Dotted capital I lowercases to two code points, so it never matches `i`.
    let fields = names(&["\u{10570}", "\u{1C89}", "İ", "Σ"]);
    for (requested, expected) in [
        ("\u{10597}", None),
        ("\u{1C8A}", None),
        ("i", None),
        ("i\u{307}", Some(2)),
        ("σ", Some(3)),
        ("ς", None),
        ("\u{10570}", Some(0)),
    ] {
        let selection = ColumnSelection::Include(columns(&[requested]));
        let result = ColumnSelection::apply(Some(&selection), "test", &fields, Case::Insensitive);
        match expected {
            Some(index) => assert_eq!(result.ok(), Some(vec![index]), "{requested:?}"),
            None => check_error(
                result,
                "AUTOCDC_COLUMNS_NOT_FOUND_IN_SCHEMA",
                "42703",
                &[
                    ("caseSensitivity", "case-insensitive"),
                    ("schemaName", "test"),
                    ("missingColumns", requested),
                    ("availableColumns", "\u{10570}, \u{1C89}, İ, Σ"),
                ],
            ),
        }
    }
}
