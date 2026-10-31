//! Spark's `DataType.sql` and `StructType.toDDL` renderings, and the
//! `sameType` comparison, over the retained spec types. Used for the
//! `expectedKeySchema`/`recordedKeySchema` parameters of
//! `AUTOCDC_INVALID_STATE.KEY_SCHEMA_DRIFT` and for drift detection.

use sail_catalog::utils::quote_name_if_needed;
use sail_common::spec;

use super::auxiliary::KeyField;
use super::schema::Case;
use crate::error::{SparkError, SparkResult};

/// `StructType.toDDL` over key fields: each `StructField.toDDL`, which is
/// `quoteIfNeeded(name) + " " + dataType.sql`, plus " NOT NULL" when the field
/// is not nullable, joined by "," (`StructType.scala:458`,
/// `StructField.scala:294–312`).
#[cfg_attr(not(test), expect(dead_code, reason = "used by the SCD1 write unit"))]
pub(super) fn key_schema_ddl(fields: &[KeyField]) -> SparkResult<String> {
    let mut parts = Vec::with_capacity(fields.len());
    for field in fields {
        let mut part = quote_name_if_needed(&field.name);
        part.push(' ');
        sql_type(&field.data_type, &mut part)?;
        if !field.nullable {
            part.push_str(" NOT NULL");
        }
        parts.push(part);
    }
    Ok(parts.join(","))
}

/// `DataType.sql` for the types Spark has an SQL rendering for. Types outside
/// that surface cannot be AutoCDC key types, so they are an internal error
/// rather than an approximation.
#[cfg_attr(not(test), expect(dead_code, reason = "used by the SCD1 write unit"))]
pub(super) fn sql_type(data_type: &spec::DataType, buf: &mut String) -> SparkResult<()> {
    use spec::DataType;
    match data_type {
        DataType::Null => buf.push_str("VOID"),
        DataType::Boolean => buf.push_str("BOOLEAN"),
        DataType::Int8 => buf.push_str("TINYINT"),
        DataType::Int16 => buf.push_str("SMALLINT"),
        DataType::Int32 => buf.push_str("INT"),
        DataType::Int64 => buf.push_str("BIGINT"),
        DataType::Float32 => buf.push_str("FLOAT"),
        DataType::Float64 => buf.push_str("DOUBLE"),
        DataType::Decimal128 { precision, scale } | DataType::Decimal256 { precision, scale } => {
            buf.push_str(&format!("DECIMAL({precision},{scale})"));
        }
        DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View => buf.push_str("STRING"),
        DataType::ConfiguredUtf8 { utf8_type } => match utf8_type {
            spec::Utf8Type::Configured => buf.push_str("STRING"),
            spec::Utf8Type::VarChar { length } => buf.push_str(&format!("VARCHAR({length})")),
            spec::Utf8Type::Char { length } => buf.push_str(&format!("CHAR({length})")),
        },
        DataType::Binary
        | DataType::LargeBinary
        | DataType::BinaryView
        | DataType::ConfiguredBinary
        | DataType::FixedSizeBinary { .. } => buf.push_str("BINARY"),
        DataType::Date32 | DataType::Date64 => buf.push_str("DATE"),
        DataType::Timestamp { timestamp_type, .. } => match timestamp_type {
            spec::TimestampType::WithoutTimeZone => buf.push_str("TIMESTAMP_NTZ"),
            spec::TimestampType::Configured | spec::TimestampType::WithLocalTimeZone => {
                buf.push_str("TIMESTAMP");
            }
        },
        DataType::Time { precision } => buf.push_str(&format!("TIME({precision})")),
        DataType::Interval {
            interval_unit,
            start_field,
            end_field,
        } => {
            use spec::IntervalFieldType as F;
            let (default_start, default_end) = match interval_unit {
                spec::IntervalUnit::YearMonth => (F::Year, F::Month),
                spec::IntervalUnit::DayTime => (F::Day, F::Second),
                spec::IntervalUnit::MonthDayNano => {
                    buf.push_str("INTERVAL");
                    return Ok(());
                }
            };
            let field = |field: F| match field {
                F::Year => "YEAR",
                F::Month => "MONTH",
                F::Day => "DAY",
                F::Hour => "HOUR",
                F::Minute => "MINUTE",
                F::Second => "SECOND",
            };
            let start = start_field.unwrap_or(default_start);
            let end = end_field.unwrap_or(match start_field {
                Some(start) => *start,
                None => default_end,
            });
            buf.push_str("INTERVAL ");
            buf.push_str(field(start));
            if start != end {
                buf.push_str(" TO ");
                buf.push_str(field(end));
            }
        }
        DataType::List { data_type, .. } | DataType::LargeList { data_type, .. } => {
            buf.push_str("ARRAY<");
            sql_type(data_type, buf)?;
            buf.push('>');
        }
        DataType::Map {
            key_type,
            value_type,
            ..
        } => {
            buf.push_str("MAP<");
            sql_type(key_type, buf)?;
            buf.push_str(", ");
            sql_type(value_type, buf)?;
            buf.push('>');
        }
        DataType::Struct { fields } => {
            buf.push_str("STRUCT<");
            for (i, field) in fields.iter().enumerate() {
                if i > 0 {
                    buf.push_str(", ");
                }
                buf.push_str(&quote_name_if_needed(&field.name));
                buf.push_str(": ");
                sql_type(&field.data_type, buf)?;
                if !field.nullable {
                    buf.push_str(" NOT NULL");
                }
            }
            buf.push('>');
        }
        DataType::Variant => buf.push_str("VARIANT"),
        DataType::UserDefined {
            sql_type: inner, ..
        } => sql_type(inner, buf)?,
        other => {
            return Err(SparkError::internal(format!(
                "no Spark SQL rendering for AutoCDC key type {other:?}"
            )));
        }
    }
    Ok(())
}

/// `DataType.sameType`: `equalsIgnoreNullability` under case-sensitive
/// analysis, else `equalsIgnoreCaseAndNullability` (`DataType.scala:90–95`,
/// `:599–617`). Nullability is ignored at every depth; struct field names
/// are compared with the resolver; Sail's string, binary, date and list
/// representations that Spark maps to one type compare equal.
#[cfg_attr(not(test), expect(dead_code, reason = "used by the SCD1 write unit"))]
pub(super) fn same_type(left: &spec::DataType, right: &spec::DataType, case: Case) -> bool {
    use spec::DataType;
    let resolver = case.resolver();
    match (left, right) {
        (
            DataType::List { data_type: l, .. }
            | DataType::LargeList { data_type: l, .. }
            | DataType::FixedSizeList { data_type: l, .. },
            DataType::List { data_type: r, .. }
            | DataType::LargeList { data_type: r, .. }
            | DataType::FixedSizeList { data_type: r, .. },
        ) => same_type(l, r, case),
        (
            DataType::Map {
                key_type: lk,
                value_type: lv,
                ..
            },
            DataType::Map {
                key_type: rk,
                value_type: rv,
                ..
            },
        ) => same_type(lk, rk, case) && same_type(lv, rv, case),
        (DataType::Struct { fields: l }, DataType::Struct { fields: r }) => {
            l.len() == r.len()
                && l.iter().zip(r.iter()).all(|(l, r)| {
                    resolver(&l.name, &r.name) && same_type(&l.data_type, &r.data_type, case)
                })
        }
        (DataType::UserDefined { sql_type: l, .. }, r) => same_type(l, r, case),
        (l, DataType::UserDefined { sql_type: r, .. }) => same_type(l, r, case),
        (l, r) => scalar_family(l) == scalar_family(r),
    }
}

/// Spark's scalar type for each Sail scalar representation, as a comparable
/// value; representations Spark does not distinguish share one family.
fn scalar_family(data_type: &spec::DataType) -> String {
    use spec::DataType;
    match data_type {
        DataType::Utf8
        | DataType::LargeUtf8
        | DataType::Utf8View
        | DataType::ConfiguredUtf8 {
            utf8_type: spec::Utf8Type::Configured,
        } => "STRING".into(),
        DataType::Binary
        | DataType::LargeBinary
        | DataType::BinaryView
        | DataType::ConfiguredBinary
        | DataType::FixedSizeBinary { .. } => "BINARY".into(),
        DataType::Date32 | DataType::Date64 => "DATE".into(),
        DataType::Decimal128 { precision, scale } | DataType::Decimal256 { precision, scale } => {
            format!("DECIMAL({precision},{scale})")
        }
        DataType::Timestamp { timestamp_type, .. } => format!("TIMESTAMP:{timestamp_type:?}"),
        other => format!("{other:?}"),
    }
}
