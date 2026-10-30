use std::collections::HashMap;
use std::sync::Arc;

use arrow::datatypes::{DataType, Field, Fields, IntervalUnit, Schema, SchemaRef, TimeUnit};
use datafusion_common::{DataFusionError, Result};
use orc_rust::proto::r#type::Kind;
use orc_rust::proto::{self};
use orc_rust::reader::metadata::FileMetadata;
use sail_common::spec;
use sail_sql_analyzer::data_type::from_ast_data_type;
use sail_sql_analyzer::parser::parse_data_type;
use sail_sql_parser::ast::data_type::DataType as AstDataType;

const CATALYST_TYPE: &str = "spark.sql.catalyst.type";
const SPARK_VERSION: &str = "org.apache.spark.version";
pub(super) const CHAR_VARCHAR_TYPE: &str = "__CHAR_VARCHAR_TYPE_STRING";

struct LogicalType {
    data_type: DataType,
    metadata: HashMap<String, String>,
    catalog_type: String,
    has_char_varchar: bool,
}

impl LogicalType {
    fn new(data_type: DataType, catalog_type: impl Into<String>) -> Self {
        Self {
            data_type,
            metadata: HashMap::new(),
            catalog_type: catalog_type.into(),
            has_char_varchar: false,
        }
    }

    fn with_char_varchar(mut self, present: bool) -> Self {
        self.has_char_varchar = present;
        self
    }

    fn into_field(mut self, name: &str, nullable: bool, preserve_char_type: bool) -> Field {
        if preserve_char_type && self.has_char_varchar {
            self.metadata.insert(
                spec::SPARK_METADATA_JSON_KEY.to_string(),
                serde_json::json!({ (CHAR_VARCHAR_TYPE): self.catalog_type }).to_string(),
            );
        }
        Field::new(name, self.data_type, nullable).with_metadata(self.metadata)
    }
}

/// Infer Spark logical types from a validated ORC footer.
///
/// Spark 4.2.1 OrcUtils.toCatalystSchema/orcTypeDescription establish the physical
/// encodings and Catalyst annotations. Field nullability is recursive; map keys
/// remain non-null. Container names follow Sail's logical schema; the reader
/// adapts native ORC container fields by position. Callers reading untrusted files
/// must bound metadata bytes and schema depth through `MetadataReadOptions`.
pub fn infer_schema(metadata: &FileMetadata, session_timezone: &str) -> Result<SchemaRef> {
    let root = convert(metadata.column_types(), 0, session_timezone)?;
    let DataType::Struct(fields) = root.data_type else {
        return Err(invalid(0, "expected a STRUCT root"));
    };
    let mut schema_metadata = HashMap::new();
    if let Some(version) = metadata.user_custom_metadata().get(SPARK_VERSION) {
        let version = std::str::from_utf8(version)
            .map_err(|_| invalid(0, "Spark version metadata is not UTF-8"))?;
        schema_metadata.insert(SPARK_VERSION.to_string(), version.to_string());
    }
    Ok(Arc::new(Schema::new_with_metadata(fields, schema_metadata)))
}

fn invalid(column: usize, message: impl std::fmt::Display) -> DataFusionError {
    DataFusionError::Plan(format!("Invalid ORC schema at column {column}: {message}"))
}

fn convert(types: &[proto::Type], column: usize, timezone: &str) -> Result<LogicalType> {
    let ty = types
        .get(column)
        .ok_or_else(|| invalid(column, "type ID is out of bounds"))?;
    let kind = ty
        .kind
        .and_then(|kind| Kind::try_from(kind).ok())
        .ok_or_else(|| invalid(column, "missing or unknown type kind"))?;
    // Spark OrcUtils.toCatalystSchema recurses into containers before looking
    // up Catalyst attributes, so even malformed container annotations are ignored.
    let annotation = if matches!(kind, Kind::Struct | Kind::List | Kind::Map) {
        None
    } else {
        catalyst_annotation(ty, column)?
    };
    let native = match kind {
        Kind::Boolean => LogicalType::new(DataType::Boolean, "boolean"),
        Kind::Byte => LogicalType::new(DataType::Int8, "tinyint"),
        Kind::Short => LogicalType::new(DataType::Int16, "smallint"),
        Kind::Int => LogicalType::new(DataType::Int32, "int"),
        Kind::Long => LogicalType::new(DataType::Int64, "bigint"),
        Kind::Float => LogicalType::new(DataType::Float32, "float"),
        Kind::Double => LogicalType::new(DataType::Float64, "double"),
        Kind::String => LogicalType::new(DataType::Utf8, "string"),
        Kind::Char | Kind::Varchar => {
            let length = ty.maximum_length.ok_or_else(|| {
                DataFusionError::NotImplemented(format!(
                    "ORC character column {column} requires an explicit maximum length"
                ))
            })?;
            let name = if kind == Kind::Char {
                "char"
            } else {
                "varchar"
            };
            LogicalType::new(DataType::Utf8, character_type(name, length, column)?)
                .with_char_varchar(true)
        }
        Kind::Binary => LogicalType::new(DataType::Binary, "binary"),
        // An unannotated ORC TIMESTAMP is LTZ regardless of the configured
        // default SQL timestamp type. All Spark timestamps use microseconds.
        Kind::Timestamp | Kind::TimestampInstant => LogicalType::new(
            DataType::Timestamp(TimeUnit::Microsecond, Some(timezone.into())),
            "timestamp",
        ),
        Kind::Date => LogicalType::new(DataType::Date32, "date"),
        Kind::Decimal => decimal_type(ty, column)?,
        Kind::Struct => {
            let children = ty
                .subtypes
                .iter()
                .map(|&child| convert(types, child as usize, timezone))
                .collect::<Result<Vec<_>>>()?;
            let has_char_varchar = children.iter().any(|child| child.has_char_varchar);
            // Spark StructType.catalogString uses raw names, unlike .sql, which
            // quotes identifiers. JSON serialization below handles JSON escaping.
            let catalog_type = children
                .iter()
                .zip(&ty.field_names)
                .map(|(child, name)| format!("{name}:{}", child.catalog_type))
                .collect::<Vec<_>>()
                .join(",");
            let fields = children
                .into_iter()
                .zip(&ty.field_names)
                .map(|(child, name)| child.into_field(name, true, column == 0))
                .collect::<Fields>();
            LogicalType::new(DataType::Struct(fields), format!("struct<{catalog_type}>"))
                .with_char_varchar(has_char_varchar)
        }
        Kind::List => {
            let child = convert(types, ty.subtypes[0] as usize, timezone)?;
            let catalog_type = format!("array<{}>", child.catalog_type);
            let has_char_varchar = child.has_char_varchar;
            LogicalType::new(
                DataType::List(Arc::new(child.into_field(
                    spec::SAIL_LIST_FIELD_NAME,
                    true,
                    false,
                ))),
                catalog_type,
            )
            .with_char_varchar(has_char_varchar)
        }
        Kind::Map => {
            let key = convert(types, ty.subtypes[0] as usize, timezone)?;
            let value = convert(types, ty.subtypes[1] as usize, timezone)?;
            let catalog_type = format!("map<{},{}>", key.catalog_type, value.catalog_type);
            let has_char_varchar = key.has_char_varchar || value.has_char_varchar;
            let fields = vec![
                key.into_field(spec::SAIL_MAP_KEY_FIELD_NAME, false, false),
                value.into_field(spec::SAIL_MAP_VALUE_FIELD_NAME, true, false),
            ];
            LogicalType::new(
                DataType::Map(
                    Arc::new(Field::new(
                        spec::SAIL_MAP_FIELD_NAME,
                        DataType::Struct(fields.into()),
                        false,
                    )),
                    false,
                ),
                catalog_type,
            )
            .with_char_varchar(has_char_varchar)
        }
        Kind::Union => {
            return Err(DataFusionError::NotImplemented(
                "ORC UNION is not a Spark SQL data type".to_string(),
            ));
        }
    };
    match annotation {
        Some(value) => annotated_type(kind, native, value, column, timezone),
        None => Ok(native),
    }
}

fn catalyst_annotation(ty: &proto::Type, column: usize) -> Result<Option<&str>> {
    let mut attributes = ty
        .attributes
        .iter()
        .filter(|attribute| attribute.key.as_deref() == Some(CATALYST_TYPE));
    let Some(attribute) = attributes.next() else {
        return Ok(None);
    };
    if attributes.next().is_some() {
        return Err(invalid(column, "duplicate Catalyst type annotation"));
    }
    attribute
        .value
        .as_deref()
        .map(Some)
        .ok_or_else(|| invalid(column, "Catalyst type annotation has no value"))
}

fn character_type(name: &str, length: u32, column: usize) -> Result<String> {
    if length > i32::MAX as u32 {
        return Err(invalid(
            column,
            "character length exceeds Spark's integer range",
        ));
    }
    Ok(format!("{name}({length})"))
}

fn decimal_type(ty: &proto::Type, column: usize) -> Result<LogicalType> {
    // The native reader defaults missing protobuf fields to zero. Do not pretend
    // that this implements the Java reader's legacy decimal defaults.
    let (Some(precision), Some(scale)) = (ty.precision, ty.scale) else {
        return Err(DataFusionError::NotImplemented(format!(
            "ORC decimal column {column} requires explicit precision and scale"
        )));
    };
    if !(1..=38).contains(&precision) || scale > precision {
        return Err(invalid(
            column,
            format!("invalid decimal precision {precision} and scale {scale}"),
        ));
    }
    Ok(LogicalType::new(
        DataType::Decimal128(precision as u8, scale as i8),
        format!("decimal({precision},{scale})"),
    ))
}

fn annotated_type(
    kind: Kind,
    native: LogicalType,
    value: &str,
    column: usize,
    timezone: &str,
) -> Result<LogicalType> {
    let parsed = parse_data_type(value)
        .map_err(|error| invalid(column, format!("Catalyst annotation {value:?}: {error}")))?;
    // The shared SQL converter still maps TIME to legacy storage-unit types.
    // Keep SQL syntax validation, but preserve the declared semantic precision
    // here until that independent converter adopts spec::DataType::Time.
    let parsed = match parsed {
        AstDataType::Time(_, precision) => {
            let precision = precision.as_ref().map_or(6, |(_, value, _)| value.value);
            let precision = i32::try_from(precision).map_err(|_| {
                invalid(
                    column,
                    format!("TIME precision {precision} is out of range"),
                )
            })?;
            spec::DataType::Time { precision }
        }
        other => from_ast_data_type(other)
            .map_err(|error| invalid(column, format!("Catalyst annotation {value:?}: {error}")))?,
    };
    let mut metadata = HashMap::new();
    let mut catalog_type = native.catalog_type;
    let mut has_char_varchar = false;
    let data_type = match parsed {
        spec::DataType::Time { precision } => {
            if kind != Kind::Long {
                return Err(invalid(
                    column,
                    format!("TIME annotation requires LONG storage, found {kind:?}"),
                ));
            }
            let time = spec::SparkTimeMetadata::try_new(precision)
                .map_err(|error| invalid(column, error))?;
            metadata.insert(
                spec::SAIL_SPARK_TIME_METADATA_KEY.to_string(),
                time.to_json().map_err(|error| invalid(column, error))?,
            );
            catalog_type = format!("time({precision})");
            // Spark stores nanoseconds at every declared precision. A string
            // cast to TIME(0) can retain fractions; preserve those stored bits.
            DataType::Time64(TimeUnit::Nanosecond)
        }
        spec::DataType::Boolean if kind == Kind::Boolean => DataType::Boolean,
        spec::DataType::Int8 if kind == Kind::Byte => DataType::Int8,
        spec::DataType::Int16 if kind == Kind::Short => DataType::Int16,
        spec::DataType::Int32 if kind == Kind::Int => DataType::Int32,
        spec::DataType::Int64 if kind == Kind::Long => DataType::Int64,
        spec::DataType::Float32 if kind == Kind::Float => DataType::Float32,
        spec::DataType::Float64 if kind == Kind::Double => DataType::Float64,
        spec::DataType::ConfiguredUtf8 { utf8_type }
            if matches!(kind, Kind::String | Kind::Char | Kind::Varchar) =>
        {
            catalog_type = match utf8_type {
                spec::Utf8Type::Char { length } => {
                    has_char_varchar = true;
                    character_type("char", length, column)?
                }
                spec::Utf8Type::VarChar { length } => {
                    has_char_varchar = true;
                    character_type("varchar", length, column)?
                }
                spec::Utf8Type::Configured => "string".to_string(),
            };
            DataType::Utf8
        }
        spec::DataType::ConfiguredBinary if kind == Kind::Binary => DataType::Binary,
        spec::DataType::Date32 if kind == Kind::Date => DataType::Date32,
        spec::DataType::Decimal128 { precision, scale }
            if native.data_type == DataType::Decimal128(precision, scale) =>
        {
            native.data_type
        }
        spec::DataType::Timestamp {
            time_unit: spec::TimeUnit::Microsecond,
            timestamp_type: spec::TimestampType::WithoutTimeZone,
        } if kind == Kind::Long => {
            catalog_type = "timestamp_ntz".to_string();
            DataType::Timestamp(TimeUnit::Microsecond, None)
        }
        spec::DataType::Timestamp {
            time_unit: spec::TimeUnit::Microsecond,
            timestamp_type: spec::TimestampType::Configured | spec::TimestampType::WithLocalTimeZone,
        } if matches!(kind, Kind::Timestamp | Kind::TimestampInstant) => {
            DataType::Timestamp(TimeUnit::Microsecond, Some(timezone.into()))
        }
        spec::DataType::Interval {
            interval_unit,
            start_field,
            end_field,
        } if (interval_unit == spec::IntervalUnit::YearMonth && kind == Kind::Int)
            || (interval_unit == spec::IntervalUnit::DayTime && kind == Kind::Long) =>
        {
            let interval =
                spec::SparkIntervalMetadata::try_new(interval_unit, start_field, end_field)
                    .map_err(|error| invalid(column, error))?
                    .ok_or_else(|| invalid(column, "unsupported interval annotation"))?;
            metadata.insert(
                spec::SAIL_SPARK_INTERVAL_METADATA_KEY.to_string(),
                interval.to_json().map_err(|error| invalid(column, error))?,
            );
            let start = interval_field_name(interval.start_field());
            let end = interval_field_name(interval.end_field());
            catalog_type = if start == end {
                format!("interval {start}")
            } else {
                format!("interval {start} to {end}")
            };
            match interval_unit {
                spec::IntervalUnit::YearMonth => DataType::Interval(IntervalUnit::YearMonth),
                _ => DataType::Duration(TimeUnit::Microsecond),
            }
        }
        _ => {
            return Err(invalid(
                column,
                format!("unsupported or inconsistent Catalyst annotation {value:?} for {kind:?}"),
            ));
        }
    };
    Ok(LogicalType {
        data_type,
        metadata,
        catalog_type,
        has_char_varchar,
    })
}

fn interval_field_name(field: spec::IntervalFieldType) -> &'static str {
    match field {
        spec::IntervalFieldType::Year => "year",
        spec::IntervalFieldType::Month => "month",
        spec::IntervalFieldType::Day => "day",
        spec::IntervalFieldType::Hour => "hour",
        spec::IntervalFieldType::Minute => "minute",
        spec::IntervalFieldType::Second => "second",
    }
}

#[cfg(test)]
#[path = "schema_tests.rs"]
#[expect(clippy::unwrap_used, clippy::panic)]
mod tests;
