//! ChangeArgsSuite.

use super::*;

#[test]
fn column_selection_none_leaves_schema_unchanged() -> SparkResult<()> {
    assert_eq!(selected(None, Case::Sensitive)?, source_schema());
    Ok(())
}

#[test]
fn column_selection_include_empty_returns_an_empty_schema() -> SparkResult<()> {
    let selection = ColumnSelection::Include(vec![]);
    assert_eq!(
        selected(Some(&selection), Case::Sensitive)?,
        Vec::<String>::new()
    );
    Ok(())
}

#[test]
fn column_selection_exclude_empty_leaves_schema_unchanged() -> SparkResult<()> {
    let selection = ColumnSelection::Exclude(vec![]);
    assert_eq!(
        selected(Some(&selection), Case::Sensitive)?,
        source_schema()
    );
    Ok(())
}

#[test]
fn column_selection_include_filters_by_exact_name_in_schema_order() -> SparkResult<()> {
    let selection = ColumnSelection::Include(columns(&["age", "Name"]));
    assert_eq!(
        selected(Some(&selection), Case::Sensitive)?,
        names(&["Name", "age"])
    );
    Ok(())
}

#[test]
fn column_selection_exclude_filters_by_exact_name() -> SparkResult<()> {
    let selection = ColumnSelection::Exclude(columns(&["id"]));
    assert_eq!(
        selected(Some(&selection), Case::Sensitive)?,
        names(&["Name", "age"])
    );
    Ok(())
}

#[test]
fn column_selection_include_fails_for_columns_not_present_in_schema() {
    // Under case sensitivity, "name" does not match the schema field "Name".
    let selection = ColumnSelection::Include(columns(&["name", "missing"]));
    check_error(
        selected(Some(&selection), Case::Sensitive),
        "AUTOCDC_COLUMNS_NOT_FOUND_IN_SCHEMA",
        "42703",
        &[
            ("caseSensitivity", "case-sensitive"),
            ("schemaName", "test"),
            ("missingColumns", "name, missing"),
            ("availableColumns", "id, Name, age"),
        ],
    );
}

#[test]
fn column_selection_exclude_fails_for_columns_not_present_in_schema() {
    let selection = ColumnSelection::Exclude(columns(&["NAME", "missing"]));
    check_error(
        selected(Some(&selection), Case::Sensitive),
        "AUTOCDC_COLUMNS_NOT_FOUND_IN_SCHEMA",
        "42703",
        &[
            ("caseSensitivity", "case-sensitive"),
            ("schemaName", "test"),
            ("missingColumns", "NAME, missing"),
            ("availableColumns", "id, Name, age"),
        ],
    );
}

#[test]
fn column_selection_include_matches_case_insensitively() -> SparkResult<()> {
    let selection = ColumnSelection::Include(columns(&["AGE", "NAME"]));
    // The retained fields keep the schema's casing, not the user's input.
    assert_eq!(
        selected(Some(&selection), Case::Insensitive)?,
        names(&["Name", "age"])
    );
    Ok(())
}

#[test]
fn column_selection_deduplicates_columns_that_normalize_to_the_same_name() -> SparkResult<()> {
    let selection = ColumnSelection::Include(columns(&["name", "NAME"]));
    assert_eq!(
        selected(Some(&selection), Case::Insensitive)?,
        names(&["Name"])
    );
    Ok(())
}

#[test]
fn column_selection_exclude_matches_case_insensitively() -> SparkResult<()> {
    let selection = ColumnSelection::Exclude(columns(&["name"]));
    assert_eq!(
        selected(Some(&selection), Case::Insensitive)?,
        names(&["id", "age"])
    );
    Ok(())
}

#[test]
fn column_selection_missing_column_error_preserves_user_casing_when_insensitive() {
    let selection = ColumnSelection::Include(columns(&["NAME", "Missing"]));
    check_error(
        selected(Some(&selection), Case::Insensitive),
        "AUTOCDC_COLUMNS_NOT_FOUND_IN_SCHEMA",
        "42703",
        &[
            ("caseSensitivity", "case-insensitive"),
            ("schemaName", "test"),
            ("missingColumns", "Missing"),
            ("availableColumns", "id, Name, age"),
        ],
    );
}

#[test]
fn unqualified_column_name_accepts_a_simple_single_part_identifier() -> SparkResult<()> {
    let name = UnqualifiedColumnName::parse("col")?;
    assert_eq!(name.name(), "col");
    assert_eq!(name.quoted(), "`col`");
    Ok(())
}

#[test]
fn unqualified_column_name_accepts_a_backtick_quoted_name_with_a_literal_dot() -> SparkResult<()> {
    let name = UnqualifiedColumnName::parse("`a.b`")?;
    assert_eq!(name.name(), "a.b");
    assert_eq!(name.quoted(), "`a.b`");
    Ok(())
}

#[test]
fn unqualified_column_name_accepts_redundant_backticks() -> SparkResult<()> {
    let name = UnqualifiedColumnName::parse("`col`")?;
    assert_eq!(name.name(), "col");
    assert_eq!(name.quoted(), "`col`");
    Ok(())
}

#[test]
fn unqualified_column_name_quoted_is_safe_to_reparse_for_literal_dot_names() -> SparkResult<()> {
    // Spark checks this through `functions.col`: the bare name re-parses as a
    // nested path and fails to resolve, while the quoted form resolves the
    // top-level "a.b" column. The identifier parser is the shared mechanism.
    let key = UnqualifiedColumnName::parse("`a.b`")?;
    check_error(
        UnqualifiedColumnName::parse(key.name()),
        "AUTOCDC_MULTIPART_COLUMN_IDENTIFIER",
        "22023",
        &[("columnName", "a.b"), ("nameParts", "a, b")],
    );
    assert_eq!(UnqualifiedColumnName::parse(&key.quoted())?, key);
    Ok(())
}

#[test]
fn include_columns_matches_a_backtick_quoted_literal_dot_column() -> SparkResult<()> {
    let fields = names(&["a.b", "c"]);
    let selection = ColumnSelection::Include(vec![UnqualifiedColumnName::parse("`a.b`")?]);
    assert_eq!(
        ColumnSelection::apply(Some(&selection), "test", &fields, Case::Sensitive)?,
        vec![0]
    );
    Ok(())
}

#[test]
fn include_columns_matches_a_backtick_quoted_mixed_case_column() -> SparkResult<()> {
    let selection = ColumnSelection::Include(vec![UnqualifiedColumnName::parse("`Name`")?]);
    assert_eq!(
        selected(Some(&selection), Case::Sensitive)?,
        names(&["Name"])
    );
    Ok(())
}

#[test]
fn unqualified_column_name_rejects_a_dotted_identifier() {
    check_error(
        UnqualifiedColumnName::parse("a.b"),
        "AUTOCDC_MULTIPART_COLUMN_IDENTIFIER",
        "22023",
        &[("columnName", "a.b"), ("nameParts", "a, b")],
    );
}

#[test]
fn unqualified_column_name_rejects_a_qualified_column_reference() {
    check_error(
        UnqualifiedColumnName::parse("src.x"),
        "AUTOCDC_MULTIPART_COLUMN_IDENTIFIER",
        "22023",
        &[("columnName", "src.x"), ("nameParts", "src, x")],
    );
}

#[test]
fn unqualified_column_name_rejects_three_or_more_parts() {
    check_error(
        UnqualifiedColumnName::parse("a.b.c"),
        "AUTOCDC_MULTIPART_COLUMN_IDENTIFIER",
        "22023",
        &[("columnName", "a.b.c"), ("nameParts", "a, b, c")],
    );
}

#[test]
fn change_args_rejects_an_empty_key_list() -> SparkResult<()> {
    let sequencing = spec::Expr::try_from(attribute("seq"))?;
    check_error(
        ChangeArgs::new(vec![], sequencing, ScdType::Type1, None, None),
        "AUTOCDC_EMPTY_KEYS",
        "22023",
        &[],
    );
    Ok(())
}

#[test]
fn unqualified_column_name_lets_the_parser_reject_an_empty_input() {
    check_error(
        UnqualifiedColumnName::parse(""),
        "PARSE_EMPTY_STATEMENT",
        "42617",
        &[],
    );
}
