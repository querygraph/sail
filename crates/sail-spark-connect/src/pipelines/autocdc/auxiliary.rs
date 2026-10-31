//! The AutoCDC auxiliary state table (`object AutoCdcAuxiliaryTable` in
//! `graph/FlowExecution.scala`): its identifier, its reserved properties and
//! the JSON form of the key column names property.

use super::schema::RESERVED_PREFIX;
use crate::pipelines::graph::Name;

/// Reserved table property recording which SCD strategy the auxiliary table
/// serves (`AutoCdcAuxiliaryTable.scdTypePropertyKey`).
#[cfg_attr(
    not(test),
    expect(dead_code, reason = "written by the SCD1 write unit")
)]
pub(crate) const SCD_TYPE_PROPERTY: &str = "pipelines.autocdc.scdType";

/// Reserved table property holding the auxiliary table's unquoted AutoCDC key
/// column names as a JSON string array, e.g. `["id","region"]`
/// (`AutoCdcAuxiliaryTable.keyColumnNamesProperty`). Written once when the
/// table is created and immutable afterwards; a full refresh is the only way
/// to change it.
#[cfg_attr(
    not(test),
    expect(dead_code, reason = "written by the SCD1 write unit")
)]
pub(crate) const KEY_COLUMN_NAMES_PROPERTY: &str = "pipelines.autocdc.keyColumnNames";

/// The auxiliary table identifier derived from a target table identifier
/// (`AutoCdcAuxiliaryTable.identifier`): the same catalog and database, and
/// the table part prefixed with `__spark_autocdc_aux_state_`. A table with
/// this name is assumed to be an AutoCDC auxiliary table managed by the
/// pipeline.
pub(crate) fn table_name(target: &[String]) -> Name {
    let mut name = target.to_vec();
    if let Some(table) = name.last_mut() {
        *table = format!("{RESERVED_PREFIX}aux_state_{table}");
    }
    name
}

/// Serialize key column names to the JSON array stored at
/// [`KEY_COLUMN_NAMES_PROPERTY`] (`serializeKeyColumnNames`). The empty list
/// round-trips as `[]`; callers enforce a non-empty key set upstream.
#[cfg_attr(not(test), expect(dead_code, reason = "used by the SCD1 write unit"))]
pub(crate) fn serialize_key_column_names(names: &[String]) -> String {
    // The JSON layer is the only encoding: identifier text is stored verbatim
    // apart from the escapes JSON itself requires, and SQL quoting never
    // enters the stored bytes.
    serde_json::to_string(names).unwrap_or_else(|_| "[]".to_string())
}

/// Parse a [`KEY_COLUMN_NAMES_PROPERTY`] value (`parseKeyColumnNames`).
/// `None` unless the text is a JSON array whose elements are all strings.
#[cfg_attr(
    not(test),
    expect(dead_code, reason = "used by the SCD1 drift validator unit")
)]
pub(crate) fn parse_key_column_names(raw: &str) -> Option<Vec<String>> {
    let serde_json::Value::Array(elements) = serde_json::from_str(raw).ok()? else {
        return None;
    };
    elements
        .into_iter()
        .map(|element| match element {
            serde_json::Value::String(name) => Some(name),
            _ => None,
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn round_trip(names: &[&str]) {
        let names: Vec<String> = names.iter().map(|name| name.to_string()).collect();
        let json = serialize_key_column_names(&names);
        assert_eq!(
            parse_key_column_names(&json).as_ref(),
            Some(&names),
            "round-trip failed: input={names:?}, serialized={json}"
        );
    }

    // AutoCdcAuxiliaryTableSuite: plain ASCII names.
    #[test]
    fn round_trip_preserves_plain_ascii_names() {
        round_trip(&["id"]);
        round_trip(&["id", "region"]);
        round_trip(&["id", "region", "country"]);
    }

    // AutoCdcAuxiliaryTableSuite: the empty list is not user-reachable but must
    // round-trip as `[]`.
    #[test]
    fn round_trip_preserves_the_empty_list() {
        round_trip(&[]);
        assert_eq!(serialize_key_column_names(&[]), "[]");
    }

    // AutoCdcAuxiliaryTableSuite: JSON-escaped characters.
    #[test]
    fn round_trip_preserves_json_escaped_characters() {
        round_trip(&["a\"b"]);
        round_trip(&["a\\b"]);
        round_trip(&["a\nb"]);
        round_trip(&["a\tb"]);
        round_trip(&["a\"b\\c\nd"]);
    }

    // AutoCdcAuxiliaryTableSuite: characters JSON does not escape.
    #[test]
    fn round_trip_preserves_characters_json_does_not_escape() {
        round_trip(&["it's"]);
        round_trip(&["a.b"]);
        round_trip(&["name with spaces"]);
        round_trip(&["a`b"]);
        round_trip(&["it's", "name with spaces", "a.b.c", "back`tick"]);
    }

    // AutoCdcAuxiliaryTableSuite: every non-array-of-strings shape is `None`.
    #[test]
    fn parse_rejects_inputs_that_are_not_a_json_array_of_strings() {
        for raw in [
            "not-json",
            "",
            "\"id\"",
            "null",
            "{\"id\": 1}",
            "[1, 2, 3]",
            "[\"id\", 1]",
            "[\"id\", null]",
            "[[\"id\"]]",
        ] {
            assert_eq!(parse_key_column_names(raw), None, "{raw}");
        }
    }

    // Spark stores the JSON compactly (`compact(JArray(...))`).
    #[test]
    fn serialized_form_is_compact_json() {
        assert_eq!(
            serialize_key_column_names(&["id".into(), "region".into()]),
            "[\"id\",\"region\"]"
        );
    }

    // `AutoCdcAuxiliaryTable.scdTypePropertyKey` / `keyColumnNamesProperty`.
    #[test]
    fn reserved_property_keys_match_spark() {
        assert_eq!(SCD_TYPE_PROPERTY, "pipelines.autocdc.scdType");
        assert_eq!(
            KEY_COLUMN_NAMES_PROPERTY,
            "pipelines.autocdc.keyColumnNames"
        );
    }

    #[test]
    fn auxiliary_name_keeps_catalog_and_database_and_prefixes_the_table() {
        assert_eq!(
            table_name(&["cat".into(), "ns1".into(), "target".into()]),
            vec!["cat", "ns1", "__spark_autocdc_aux_state_target"]
        );
        assert_eq!(
            table_name(&["t_a".into()]),
            vec!["__spark_autocdc_aux_state_t_a"]
        );
    }
}
