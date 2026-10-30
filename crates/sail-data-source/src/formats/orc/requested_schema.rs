//! Per-file requested-schema mapping, after native ORC logical conversion.

use std::collections::BTreeSet;
use std::sync::Arc;

use arrow::datatypes::{
    DataType, Field, FieldRef, Fields, IntervalUnit, Schema, SchemaRef, TimeUnit,
};
use arrow::record_batch::{RecordBatch, RecordBatchOptions};
use datafusion_common::{Result, exec_err, not_impl_err, plan_err};
use sail_common::spec;

mod array;

/// Maps a projected, logically converted ORC batch to the requested file columns.
///
/// The full requested file schema remains distinct from the required projection.
/// Timestamp compatibility follows Spark's positional, STRUCT-only check before
/// required fields are resolved. The reader must use `file_projection()`, apply
/// [`super::OrcSchemaBridge`], then apply this adapter. A subsequent projection
/// handles expressions and partition columns.
///
/// Case-insensitive mapping currently qualifies ASCII names in the retained
/// Java baseline. Unicode/locale matching, cross-kind timestamp conversion and
/// other unqualified leaf conversions fail explicitly; these are remaining
/// compatibility work, not a complete Spark evolution implementation.
#[derive(Debug, Clone)]
pub struct OrcRequestedSchemaAdapter {
    file_projection: Vec<usize>,
    input_schema: SchemaRef,
    schema: SchemaRef,
    fields: Vec<FieldPlan>,
}

#[derive(Debug, Clone)]
struct FieldPlan {
    source_index: Option<usize>,
    target: FieldRef,
    conversion: Conversion,
}

#[derive(Debug, Clone)]
enum Conversion {
    Identity,
    SafeInteger,
    TimestampLabel,
    Struct(Vec<FieldPlan>),
    List(Box<FieldPlan>),
    Map(Box<FieldPlan>, Box<FieldPlan>),
}

impl OrcRequestedSchemaAdapter {
    /// `required_indices` indexes the full requested file schema. Its order and
    /// repeated indices are preserved; the native projection is sorted/unique.
    /// An empty slice requests zero file columns while retaining row counts.
    pub fn try_new(
        file_schema: SchemaRef,
        full_data_schema: SchemaRef,
        required_indices: &[usize],
        read_case_sensitive: bool,
    ) -> Result<Self> {
        validate_timestamps(file_schema.fields(), full_data_schema.fields(), "root")?;
        let positional = !file_schema.fields().is_empty()
            && file_schema
                .fields()
                .iter()
                .all(|f| f.name().starts_with("_col"));
        if positional && file_schema.fields().len() > full_data_schema.fields().len() {
            return exec_err!(
                "ORC positional evolution requires the full data schema: {} file fields, {} requested fields",
                file_schema.fields().len(),
                full_data_schema.fields().len()
            );
        }
        let requested = Self::nullable_schema(&full_data_schema);
        let mut fields = Vec::with_capacity(required_indices.len());
        let mut projection = BTreeSet::new();
        for &index in required_indices {
            let target = requested.fields().get(index).ok_or_else(|| {
                datafusion_common::plan_datafusion_err!(
                    "ORC requested projection index {index} is out of bounds"
                )
            })?;
            validate_target(target)?;
            let source_index = if positional {
                (index < file_schema.fields().len()).then_some(index)
            } else {
                find_field(
                    file_schema.fields(),
                    target.name(),
                    read_case_sensitive,
                    true,
                )?
            };
            let conversion = if let Some(source_index) = source_index {
                projection.insert(source_index);
                compile_field(
                    &file_schema.fields()[source_index],
                    target,
                    read_case_sensitive,
                    false,
                )?
            } else {
                Conversion::Identity
            };
            fields.push(FieldPlan {
                source_index,
                target: Arc::clone(target),
                conversion,
            });
        }
        let file_projection: Vec<_> = projection.into_iter().collect();
        for field in &mut fields {
            field.source_index = field
                .source_index
                .map(|index| file_projection.partition_point(|&source_index| source_index < index));
        }
        Ok(Self {
            input_schema: Arc::new(file_schema.project(&file_projection)?),
            schema: Arc::new(requested.project(required_indices)?),
            file_projection,
            fields,
        })
    }

    /// Spark's ordinary file-read schema is nullable recursively. Preserve field
    /// names and metadata, including declared TIME/interval types. Arrow's map
    /// entries and key wrappers remain non-nullable.
    pub fn nullable_schema(schema: &Schema) -> SchemaRef {
        Arc::new(Schema::new_with_metadata(
            schema
                .fields()
                .iter()
                .map(|f| nullable_field(f, true))
                .collect::<Vec<_>>(),
            schema.metadata().clone(),
        ))
    }

    pub fn file_projection(&self) -> &[usize] {
        &self.file_projection
    }
    pub fn input_schema(&self) -> &SchemaRef {
        &self.input_schema
    }
    pub fn schema(&self) -> &SchemaRef {
        &self.schema
    }

    pub fn adapt_batch(&self, batch: RecordBatch) -> Result<RecordBatch> {
        if batch.schema().fields() != self.input_schema.fields() {
            return exec_err!(
                "ORC requested-schema adapter received a different logical input schema"
            );
        }
        let columns = self
            .fields
            .iter()
            .map(|field| array::adapt_field(field, batch.columns(), batch.num_rows()))
            .collect::<Result<Vec<_>>>()?;
        Ok(RecordBatch::try_new_with_options(
            Arc::clone(&self.schema),
            columns,
            &RecordBatchOptions::new().with_row_count(Some(batch.num_rows())),
        )?)
    }
}

fn nullable_field(field: &FieldRef, nullable: bool) -> FieldRef {
    let data_type = match field.data_type() {
        DataType::Struct(fields) => {
            DataType::Struct(fields.iter().map(|f| nullable_field(f, true)).collect())
        }
        DataType::List(element) => DataType::List(nullable_field(element, true)),
        DataType::LargeList(element) => DataType::LargeList(nullable_field(element, true)),
        DataType::FixedSizeList(element, size) => {
            DataType::FixedSizeList(nullable_field(element, true), *size)
        }
        DataType::Map(entries, sorted) => {
            let entry_type = match entries.data_type() {
                DataType::Struct(fields) => DataType::Struct(
                    fields
                        .iter()
                        .enumerate()
                        .map(|(i, f)| nullable_field(f, i != 0))
                        .collect(),
                ),
                other => other.clone(),
            };
            DataType::Map(
                Arc::new(
                    entries
                        .as_ref()
                        .clone()
                        .with_data_type(entry_type)
                        .with_nullable(false),
                ),
                *sorted,
            )
        }
        other => other.clone(),
    };
    Arc::new(
        field
            .as_ref()
            .clone()
            .with_data_type(data_type)
            .with_nullable(nullable),
    )
}

fn validate_timestamps(file: &Fields, requested: &Fields, path: &str) -> Result<()> {
    for (source, target) in file.iter().zip(requested) {
        let path = format!("{path}.{}", target.name());
        match (source.data_type(), target.data_type()) {
            (DataType::Timestamp(_, Some(_)), DataType::Timestamp(_, None)) => {
                return exec_err!(
                    "ORC cannot convert TIMESTAMP to TIMESTAMP_NTZ in full data schema at {path}"
                );
            }
            (DataType::Timestamp(_, None), DataType::Timestamp(_, Some(_))) => {
                return exec_err!(
                    "ORC cannot convert TIMESTAMP_NTZ to TIMESTAMP in full data schema at {path}"
                );
            }
            (DataType::Struct(left), DataType::Struct(right)) => {
                validate_timestamps(left, right, &path)?
            }
            _ => {}
        }
    }
    Ok(())
}

fn find_field(
    fields: &Fields,
    name: &str,
    case_sensitive: bool,
    top_level: bool,
) -> Result<Option<usize>> {
    if !case_sensitive && (!name.is_ascii() || fields.iter().any(|f| !f.name().is_ascii())) {
        return not_impl_err!(
            "ORC case-insensitive Unicode/locale field matching is not yet qualified for {name}"
        );
    }
    let mut matches = fields
        .iter()
        .enumerate()
        .filter(|(_, f)| {
            if case_sensitive {
                f.name() == name
            } else {
                f.name().eq_ignore_ascii_case(name)
            }
        })
        .map(|(index, _)| index);
    let first = matches.next();
    if top_level && !case_sensitive {
        if matches.next().is_some() {
            return exec_err!("ORC found duplicate field {name} in case-insensitive mode");
        }
        Ok(first)
    } else {
        // ORC Java inserts physical names in order into its lookup map. Exact
        // duplicates and nested normalized names retain the last physical field.
        Ok(matches.next_back().or(first))
    }
}

fn validate_target(field: &Field) -> Result<()> {
    if let Some(value) = field.metadata().get(spec::SAIL_SPARK_TIME_METADATA_KEY) {
        spec::SparkTimeMetadata::from_json(value)
            .map_err(|e| datafusion_common::plan_datafusion_err!("{e}"))?;
        if field.data_type() != &DataType::Time64(TimeUnit::Nanosecond) {
            return plan_err!("ORC requested TIME metadata requires nanosecond storage");
        }
    }
    if let Some(value) = field.metadata().get(spec::SAIL_SPARK_INTERVAL_METADATA_KEY) {
        let metadata = spec::SparkIntervalMetadata::from_json(value)
            .map_err(|e| datafusion_common::plan_datafusion_err!("{e}"))?;
        let expected = match metadata.interval_unit() {
            spec::IntervalUnit::YearMonth => DataType::Interval(IntervalUnit::YearMonth),
            spec::IntervalUnit::DayTime => DataType::Duration(TimeUnit::Microsecond),
            spec::IntervalUnit::MonthDayNano => {
                return plan_err!("ORC Spark interval metadata has unsupported unit");
            }
        };
        if field.data_type() != &expected {
            return plan_err!("ORC requested interval metadata has inconsistent storage");
        }
    }
    match field.data_type() {
        DataType::Struct(fields) => {
            for child in fields {
                validate_target(child)?;
            }
        }
        DataType::List(child) | DataType::LargeList(child) | DataType::FixedSizeList(child, _) => {
            validate_target(child)?
        }
        DataType::Map(entries, _) => {
            let DataType::Struct(fields) = entries.data_type() else {
                return plan_err!("ORC requested map entries must be a struct");
            };
            if fields.len() != 2 {
                return plan_err!("ORC requested map requires key and value fields");
            }
            for child in fields {
                validate_target(child)?;
            }
        }
        _ => {}
    }
    Ok(())
}

fn compile_field(
    source: &Field,
    target: &FieldRef,
    case_sensitive: bool,
    in_map_key: bool,
) -> Result<Conversion> {
    validate_target(source)?;
    let conversion = match (source.data_type(), target.data_type()) {
        (DataType::Struct(source_fields), DataType::Struct(target_fields)) => {
            let fields = target_fields
                .iter()
                .map(|target| {
                    let source_index =
                        find_field(source_fields, target.name(), case_sensitive, false)?;
                    let conversion = match source_index {
                        Some(index) => compile_field(
                            &source_fields[index],
                            target,
                            case_sensitive,
                            in_map_key,
                        )?,
                        None => Conversion::Identity,
                    };
                    Ok(FieldPlan {
                        source_index,
                        target: Arc::clone(target),
                        conversion,
                    })
                })
                .collect::<Result<Vec<_>>>()?;
            Conversion::Struct(fields)
        }
        (DataType::List(source), DataType::List(target)) => Conversion::List(Box::new(FieldPlan {
            source_index: Some(0),
            target: Arc::clone(target),
            conversion: compile_field(source, target, case_sensitive, in_map_key)?,
        })),
        (DataType::Map(source, source_sorted), DataType::Map(target, target_sorted))
            if source_sorted == target_sorted =>
        {
            let (DataType::Struct(source_fields), DataType::Struct(target_fields)) =
                (source.data_type(), target.data_type())
            else {
                return plan_err!("ORC map entries must be structs");
            };
            if source_fields.len() != 2 || target_fields.len() != 2 {
                return plan_err!("ORC map requires key and value fields");
            }
            let child = |index: usize| -> Result<Box<FieldPlan>> {
                Ok(Box::new(FieldPlan {
                    source_index: Some(index),
                    target: Arc::clone(&target_fields[index]),
                    conversion: compile_field(
                        &source_fields[index],
                        &target_fields[index],
                        case_sensitive,
                        in_map_key || index == 0,
                    )?,
                }))
            };
            Conversion::Map(child(0)?, child(1)?)
        }
        (left, right) if left == right => Conversion::Identity,
        (left, right) if signed_integer(left) && signed_integer(right) => {
            // Spark's observed map-key narrowing differs between vectorized and
            // row readers. Safe-null casts are not a qualified map-key policy.
            if in_map_key && left.primitive_width() > right.primitive_width() {
                return not_impl_err!(
                    "ORC integer narrowing from {left} to {right} inside MAP KEYS for {} is not yet qualified",
                    target.name()
                );
            }
            Conversion::SafeInteger
        }
        (
            DataType::Timestamp(TimeUnit::Microsecond, Some(_)),
            DataType::Timestamp(TimeUnit::Microsecond, Some(_)),
        ) => Conversion::TimestampLabel,
        _ => {
            return not_impl_err!(
                "ORC requested-schema conversion from {} to {} for {} is not yet qualified",
                source.data_type(),
                target.data_type(),
                target.name()
            );
        }
    };
    Ok(conversion)
}

fn signed_integer(data_type: &DataType) -> bool {
    matches!(
        data_type,
        DataType::Int8 | DataType::Int16 | DataType::Int32 | DataType::Int64
    )
}

#[cfg(test)]
mod tests;
