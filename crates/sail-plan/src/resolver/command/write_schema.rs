use datafusion::arrow::datatypes::DataType;

use crate::error::{PlanError, PlanResult};

/// Reject missing or extra nested fields before DataFusion coerces a write by
/// name. Its general struct coercion may fill missing nullable fields with null,
/// which is not Spark's table-write contract.
pub(super) fn validate_nested_fields(
    source: &DataType,
    target: &DataType,
    path: &str,
    case_sensitive: bool,
) -> PlanResult<()> {
    let matches = |left: &str, right: &str| {
        left == right || (!case_sensitive && left.eq_ignore_ascii_case(right))
    };
    match (source, target) {
        (DataType::Struct(source), DataType::Struct(target)) => {
            for field in target {
                let name = format!("{path}.{}", field.name());
                let input = source.iter().find(|input| matches(input.name(), field.name()))
                    .ok_or_else(|| PlanError::analysis(format!(
                        "[INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_FIND_DATA] Cannot find data for output column {name}"
                    )))?;
                validate_nested_fields(
                    input.data_type(),
                    field.data_type(),
                    &name,
                    case_sensitive,
                )?;
            }
            if source.iter().any(|input| {
                !target
                    .iter()
                    .any(|field| matches(input.name(), field.name()))
            }) {
                return Err(PlanError::analysis(format!(
                    "[INCOMPATIBLE_DATA_FOR_TABLE.EXTRA_STRUCT_FIELDS] Extra fields in input struct {path}"
                )));
            }
        }
        (
            DataType::List(source)
            | DataType::LargeList(source)
            | DataType::FixedSizeList(source, _),
            DataType::List(target)
            | DataType::LargeList(target)
            | DataType::FixedSizeList(target, _),
        ) => validate_nested_fields(
            source.data_type(),
            target.data_type(),
            &format!("{path}.element"),
            case_sensitive,
        )?,
        // A map's entries have no names of their own in Spark. Catalogs and
        // functions spell them differently, so they compare by position.
        (DataType::Map(source, _), DataType::Map(target, _)) => {
            match (source.data_type(), target.data_type()) {
                (DataType::Struct(source), DataType::Struct(target))
                    if source.len() == 2 && target.len() == 2 =>
                {
                    for (index, part) in ["key", "value"].into_iter().enumerate() {
                        validate_nested_fields(
                            source[index].data_type(),
                            target[index].data_type(),
                            &format!("{path}.{part}"),
                            case_sensitive,
                        )?;
                    }
                }
                (source, target) => validate_nested_fields(source, target, path, case_sensitive)?,
            }
        }
        _ => {}
    }
    Ok(())
}
