//! Resolution-time Auto CDC validators from `graph/Flow.scala`
//! (`AutoCdcMergeFlow`) and `autocdc/ChangeArgs.scala`
//! (`ColumnSelection.applyToSchema`), over source field names. They are
//! exercised by the ports in `tests.rs`; the prepared CDC input unit wires them
//! into flow resolution, where the source schema and the session's
//! `spark.sql.caseSensitive` become available.
#![cfg_attr(
    not(test),
    expect(dead_code, reason = "wired by the prepared CDC input unit")
)]

use grust_columns::Selection;
use sail_common::utils::string::spark_case_insensitive_name_eq;
use sail_sql_analyzer::expression::from_ast_object_name;
use sail_sql_analyzer::parser::parse_object_name;

use super::lookup::FieldLookup;
use super::{ChangeArgs, ColumnSelection, ScdType, UnqualifiedColumnName, errors};
use crate::error::SparkResult;

/// Prefix reserved for Auto CDC internal columns and tables
/// (`AutoCdcReservedNames.prefix`).
pub(super) const RESERVED_PREFIX: &str = "__spark_autocdc_";
/// The metadata column Auto CDC projects onto the target
/// (`Scd1BatchProcessor.cdcMetadataColName`).
pub(super) const METADATA_COLUMN: &str = "__spark_autocdc_metadata";
pub(super) const DELETE_SEQUENCE_FIELD: &str = "deleteSequence";
pub(super) const UPSERT_SEQUENCE_FIELD: &str = "upsertSequence";
/// The schema name Spark reports for the source change-data feed.
pub(super) const CHANGE_DATA_FEED: &str = "changeDataFeed";

/// The session's `spark.sql.caseSensitive` setting
/// (`SQLConf.caseSensitiveAnalysis`).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum Case {
    Sensitive,
    Insensitive,
}

impl Case {
    /// The label Spark prints for the comparison mode (`CaseSensitivityLabels`).
    pub fn label(self) -> &'static str {
        match self {
            Self::Sensitive => "case-sensitive",
            Self::Insensitive => "case-insensitive",
        }
    }

    /// Spark's analysis `Resolver`: exact equality, or Java
    /// `String.equalsIgnoreCase` through Sail's JDK 17 parity comparator.
    pub fn resolver(self) -> fn(&str, &str) -> bool {
        match self {
            Self::Sensitive => grust_columns::exact,
            Self::Insensitive => spark_case_insensitive_name_eq,
        }
    }
}

impl ScdType {
    /// Stable label persisted as an auxiliary table property.
    pub fn label(self) -> &'static str {
        match self {
            Self::Type1 => "SCD1",
            Self::Type2 => "SCD2",
        }
    }
}

impl UnqualifiedColumnName {
    /// Parses `input` with the SQL identifier parser, as
    /// `CatalystSqlParser.parseMultipartIdentifier` does.
    pub fn parse(input: &str) -> SparkResult<Self> {
        if input.trim().is_empty() {
            return Err(errors::parse_empty_statement());
        }
        let parts: Vec<String> = from_ast_object_name(parse_object_name(input)?)?.into();
        Self::from_parts(parts)
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    /// Backtick-quoted form, safe for APIs that re-parse identifiers.
    pub fn quoted(&self) -> String {
        format!("`{}`", self.name.replace('`', "``"))
    }
}

impl ColumnSelection {
    /// `ColumnSelection.applyToSchema`: the indices of `fields` a selection
    /// retains, in field order, resolving names through Spark's `StructType`
    /// lookup for the session's case setting. `None` retains every field.
    pub(super) fn apply(
        selection: Option<&Self>,
        schema_name: &str,
        fields: &[String],
        case: Case,
    ) -> SparkResult<Vec<usize>> {
        let names = |columns: &[UnqualifiedColumnName]| -> Vec<String> {
            columns.iter().map(|x| x.name.clone()).collect()
        };
        let (names, include) = match selection {
            None => return Ok((0..fields.len()).collect()),
            Some(Self::Include(columns)) => (names(columns), true),
            Some(Self::Exclude(columns)) => (names(columns), false),
        };
        let selection = if include {
            Selection::Include(names.as_slice())
        } else {
            Selection::Exclude(names.as_slice())
        };
        let lookup = FieldLookup::new(fields);
        grust_columns::select(fields.len(), selection, lookup.resolver(case)).map_err(
            |unresolved| {
                errors::columns_not_found_in_schema(
                    case.label(),
                    schema_name,
                    &unresolved.names,
                    fields,
                )
            },
        )
    }
}

/// `AutoCdcMergeFlow.requireReservedPrefixAbsentInSourceColumns`.
pub(super) fn require_reserved_prefix_absent(fields: &[String], case: Case) -> SparkResult<()> {
    match grust_columns::reserved(fields, RESERVED_PREFIX, case.resolver()) {
        Some(index) => Err(errors::reserved_column_name_prefix_conflict(
            case.label(),
            &fields[index],
            CHANGE_DATA_FEED,
            RESERVED_PREFIX,
        )),
        None => Ok(()),
    }
}

/// `AutoCdcMergeFlow.userSelectedSchema`: the retained field indices after the
/// column selection, with every key present among them.
pub(super) fn selected_fields(
    args: &ChangeArgs,
    fields: &[String],
    case: Case,
) -> SparkResult<Vec<usize>> {
    let selected = ColumnSelection::apply(
        args.column_selection.as_ref(),
        CHANGE_DATA_FEED,
        fields,
        case,
    )?;
    let names: Vec<&String> = selected.iter().map(|index| &fields[*index]).collect();
    let keys: Vec<&str> = args.keys.iter().map(|key| key.name()).collect();
    if let Some(index) = grust_columns::unresolved(&names, &keys, case.resolver()) {
        return Err(errors::key_not_in_selected_schema(
            case.label(),
            keys[index],
        ));
    }
    Ok(selected)
}

/// `AutoCdcMergeFlow.schema`, by field name: the validated source fields in
/// selection order, then the metadata column. SCD2 is rejected here, as the
/// Scala constructor does, after the reserved-prefix and selection checks.
pub(super) fn output_field_names(
    args: &ChangeArgs,
    fields: &[String],
    case: Case,
) -> SparkResult<Vec<String>> {
    require_reserved_prefix_absent(fields, case)?;
    let selected = selected_fields(args, fields, case)?;
    if args.scd_type == ScdType::Type2 {
        return Err(errors::scd2_not_supported());
    }
    Ok(selected
        .into_iter()
        .map(|index| fields[index].clone())
        .chain(std::iter::once(METADATA_COLUMN.to_string()))
        .collect())
}
