//! The AutoCDC auxiliary state table (`object AutoCdcAuxiliaryTable` in
//! `graph/FlowExecution.scala`): its identifier, its reserved properties and
//! the JSON form of the key column names property.

use super::schema::RESERVED_PREFIX;
use crate::pipelines::graph::Name;

/// Reserved table property recording which SCD strategy the auxiliary table
/// serves (`AutoCdcAuxiliaryTable.scdTypePropertyKey`).
pub(crate) const SCD_TYPE_PROPERTY: &str = "pipelines.autocdc.scdType";

/// Reserved table property holding the auxiliary table's unquoted AutoCDC key
/// column names as a JSON string array, e.g. `["id","region"]`
/// (`AutoCdcAuxiliaryTable.keyColumnNamesProperty`). Written once when the
/// table is created and immutable afterwards; a full refresh is the only way
/// to change it.
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
pub(crate) fn serialize_key_column_names(names: &[String]) -> String {
    // The JSON layer is the only encoding: identifier text is stored verbatim
    // apart from the escapes JSON itself requires, and SQL quoting never
    // enters the stored bytes.
    serde_json::to_string(names).unwrap_or_else(|_| "[]".to_string())
}

/// Parse a [`KEY_COLUMN_NAMES_PROPERTY`] value (`parseKeyColumnNames`).
/// `None` unless the text is a JSON array whose elements are all strings.
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

// ---------------------------------------------------------------------------
// Auxiliary state contract: schema derivation, key drift, merge support
// (`AutoCdcMergeWriteBase` and `Scd1MergeStreamingWrite` in
// `graph/FlowExecution.scala:385–690`). Pure functions over what the catalog
// reports; the SCD1 write unit calls them before creating the table.
// ---------------------------------------------------------------------------

use sail_common::spec;

use super::schema::{Case, DELETE_SEQUENCE_FIELD, METADATA_COLUMN, UPSERT_SEQUENCE_FIELD};
use super::{UnqualifiedColumnName, ddl, errors};
use crate::error::{SparkError, SparkResult};

/// A key field of the auxiliary table: the name, type and nullability the
/// flow's selected schema gives the key column.
#[derive(Clone, Debug, PartialEq)]
pub(super) struct KeyField {
    pub name: String,
    pub data_type: spec::DataType,
    pub nullable: bool,
}

impl KeyField {
    pub fn from_field(field: &spec::Field) -> Self {
        Self {
            name: field.name.clone(),
            data_type: field.data_type.clone(),
            nullable: field.nullable,
        }
    }
}

/// `expectedAuxiliaryKeyFields` (`FlowExecution.scala:450–465`): every
/// declared key resolved against the flow's selected schema, in declaration
/// order. The definition-time validators have already checked the keys
/// exist, so a miss is an internal error, not a user condition.
pub(super) fn expected_key_fields(
    keys: &[UnqualifiedColumnName],
    schema: &spec::Fields,
    case: Case,
    flow_name: &str,
    target_quoted: &str,
) -> SparkResult<Vec<KeyField>> {
    let resolver = case.resolver();
    keys.iter()
        .map(|key| {
            schema
                .iter()
                .find(|field| resolver(&field.name, key.name()))
                .map(|field| KeyField::from_field(field))
                .ok_or_else(|| {
                    SparkError::internal(format!(
                        "AutoCDC key column '{}' is missing from the auxiliary table schema for flow {flow_name} writing to target {target_quoted}.",
                        key.name()
                    ))
                })
        })
        .collect()
}

/// The CDC metadata field (`Scd1BatchProcessor.cdcMetadataColSchema`):
/// `struct<deleteSequence: T, upsertSequence: T>` for the resolved sequencing
/// type `T`, both members nullable, the field itself not nullable
/// (`AutoCdcFlowSuite.scala:270–278`; the suites' `cdcMetadataDdl` declares
/// it `NOT NULL`).
pub(super) fn metadata_field(sequencing_type: &spec::DataType) -> spec::Field {
    let member = |name: &str| spec::Field {
        name: name.to_string(),
        data_type: sequencing_type.clone(),
        nullable: true,
        metadata: vec![],
    };
    spec::Field {
        name: METADATA_COLUMN.to_string(),
        data_type: spec::DataType::Struct {
            fields: spec::Fields::from(vec![
                member(DELETE_SEQUENCE_FIELD),
                member(UPSERT_SEQUENCE_FIELD),
            ]),
        },
        nullable: false,
        metadata: vec![],
    }
}

/// `Scd1MergeStreamingWrite.auxiliaryTableSchema` (`:661–662`): the key
/// fields in declared order, then the CDC metadata field.
pub(super) fn auxiliary_schema(
    keys: &[KeyField],
    sequencing_type: &spec::DataType,
) -> Vec<spec::Field> {
    keys.iter()
        .map(|key| spec::Field {
            name: key.name.clone(),
            data_type: key.data_type.clone(),
            nullable: key.nullable,
            metadata: vec![],
        })
        .chain(std::iter::once(metadata_field(sequencing_type)))
        .collect()
}

/// `validateNoAutoCdcKeyDrift` (`FlowExecution.scala:478–560`), for an
/// auxiliary table the caller found to exist: the recorded key names come
/// from the `pipelines.autocdc.keyColumnNames` property (missing, then
/// malformed, are their own conditions), each must resolve in the existing
/// schema (the first miss in recorded order is reported), and then the
/// recorded and expected keys must agree in count, name under the resolver
/// and `sameType`.
pub(super) fn validate_no_key_drift(
    flow_name: &str,
    aux_table_name: &str,
    expected: &[KeyField],
    existing_columns: &[spec::Field],
    existing_properties: &[(String, String)],
    case: Case,
) -> SparkResult<()> {
    let resolver = case.resolver();
    let raw = existing_properties
        .iter()
        .find(|(key, _)| key == KEY_COLUMN_NAMES_PROPERTY)
        .map(|(_, value)| value.as_str())
        .ok_or_else(|| {
            errors::auxiliary_table_property_missing(
                flow_name,
                aux_table_name,
                KEY_COLUMN_NAMES_PROPERTY,
            )
        })?;
    let recorded_names = parse_key_column_names(raw).ok_or_else(|| {
        errors::auxiliary_table_property_malformed(
            flow_name,
            aux_table_name,
            KEY_COLUMN_NAMES_PROPERTY,
            raw,
        )
    })?;
    let recorded = recorded_names
        .iter()
        .map(|name| {
            existing_columns
                .iter()
                .find(|field| resolver(&field.name, name))
                .map(KeyField::from_field)
                .ok_or_else(|| {
                    errors::auxiliary_table_key_column_missing(
                        flow_name,
                        aux_table_name,
                        name,
                        KEY_COLUMN_NAMES_PROPERTY,
                    )
                })
        })
        .collect::<SparkResult<Vec<_>>>()?;
    let drifted = recorded.len() != expected.len()
        || expected.iter().any(|expected| {
            match recorded
                .iter()
                .find(|recorded| resolver(&recorded.name, &expected.name))
            {
                None => true,
                Some(recorded) => !ddl::same_type(&recorded.data_type, &expected.data_type, case),
            }
        });
    if drifted {
        return Err(errors::key_schema_drift(
            flow_name,
            aux_table_name,
            &ddl::key_schema_ddl(expected)?,
            &ddl::key_schema_ddl(&recorded)?,
        ));
    }
    Ok(())
}

/// `requireDestinationSupportsRowLevelOps` (`FlowExecution.scala:435–455`):
/// the `format` parameter is the declared format, else the table's provider
/// property, else `<unknown>`.
pub(super) fn require_merge_support(
    table_name_quoted: &str,
    declared_format: Option<&str>,
    provider_property: Option<&str>,
    supports_row_level_operations: bool,
) -> SparkResult<()> {
    if supports_row_level_operations {
        return Ok(());
    }
    Err(errors::target_does_not_support_merge(
        table_name_quoted,
        declared_format.or(provider_property).unwrap_or("<unknown>"),
    ))
}
