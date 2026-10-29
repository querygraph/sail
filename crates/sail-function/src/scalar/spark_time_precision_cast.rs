use std::sync::Arc;

use datafusion::arrow::array::{
    Array, ArrayRef, AsArray, FixedSizeListArray, LargeListArray, ListArray, MapArray, StructArray,
};
use datafusion::arrow::compute::cast;
use datafusion::arrow::datatypes::{DataType, Field, FieldRef, Time64NanosecondType, TimeUnit};
use datafusion_common::{Result, ScalarValue, exec_err, plan_datafusion_err};
use datafusion_expr::{
    ColumnarValue, ReturnFieldArgs, ScalarFunctionArgs, ScalarUDFImpl, Signature, Volatility,
};
use sail_common::spec::{SAIL_SPARK_TIME_METADATA_KEY, SparkTimeMetadata};

/// Applies only TIME precision changes before the ordinary Spark cast.
///
/// The intermediate field retains the source's container shape, names and
/// non-TIME leaves. The planner still owns positional renaming and other casts.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct SparkTimePrecisionCast {
    signature: Signature,
    source_field: FieldRef,
    target_field: FieldRef,
    output_field: FieldRef,
}

impl SparkTimePrecisionCast {
    pub fn try_new(source_field: FieldRef, target_field: FieldRef) -> Result<Self> {
        let output_field = prepare_field(&source_field, &target_field)?;
        Ok(Self {
            signature: Signature::any(1, Volatility::Immutable),
            source_field,
            target_field,
            output_field,
        })
    }

    pub fn source_field(&self) -> &FieldRef {
        &self.source_field
    }

    pub fn target_field(&self) -> &FieldRef {
        &self.target_field
    }

    pub fn output_field(&self) -> &FieldRef {
        &self.output_field
    }

    pub fn is_needed(&self) -> bool {
        self.source_field != self.output_field
    }
}

impl ScalarUDFImpl for SparkTimePrecisionCast {
    fn name(&self) -> &str {
        "spark_time_precision_cast"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        if arg_types != [self.source_field.data_type().clone()] {
            return exec_err!("TIME precision cast input does not match its declared source field");
        }
        Ok(self.output_field.data_type().clone())
    }

    fn return_field_from_args(&self, args: ReturnFieldArgs) -> Result<FieldRef> {
        let types = args
            .arg_fields
            .iter()
            .map(|f| f.data_type().clone())
            .collect::<Vec<_>>();
        self.return_type(&types)?;
        Ok(Arc::clone(&self.output_field))
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let [input] = args.args.as_slice() else {
            return exec_err!("TIME precision cast expects one input");
        };
        self.return_type(&[input.data_type()])?;
        let scalar = matches!(input, ColumnarValue::Scalar(_));
        let input = input.to_array(if scalar { 1 } else { args.number_rows })?;
        let output = transform(&input, &self.source_field, &self.output_field)?;
        if scalar {
            Ok(ColumnarValue::Scalar(ScalarValue::try_from_array(
                &output, 0,
            )?))
        } else {
            Ok(ColumnarValue::Array(output))
        }
    }
}

fn declared_precision(field: &Field) -> Result<Option<i32>> {
    let Some(metadata) = field.metadata().get(SAIL_SPARK_TIME_METADATA_KEY) else {
        return Ok(None);
    };
    if field.data_type() != &DataType::Time64(TimeUnit::Nanosecond) {
        return Err(plan_datafusion_err!(
            "Spark TIME metadata requires Time64(Nanosecond)"
        ));
    }
    SparkTimeMetadata::from_json(metadata)
        .map(|value| Some(value.precision()))
        .map_err(|error| plan_datafusion_err!("{error}"))
}

fn source_precision(field: &Field) -> Result<i32> {
    if let Some(precision) = declared_precision(field)? {
        return Ok(precision);
    }
    match field.data_type() {
        DataType::Time32(TimeUnit::Second) => Ok(0),
        DataType::Time32(TimeUnit::Millisecond) => Ok(3),
        DataType::Time64(TimeUnit::Microsecond) => Ok(6),
        DataType::Time64(TimeUnit::Nanosecond) => Ok(9),
        other => Err(plan_datafusion_err!("invalid TIME source type: {other}")),
    }
}

fn list_element(data_type: &DataType) -> Option<&FieldRef> {
    match data_type {
        DataType::List(field) | DataType::LargeList(field) | DataType::FixedSizeList(field, _) => {
            Some(field)
        }
        _ => None,
    }
}

fn prepare_field(source: &FieldRef, target: &FieldRef) -> Result<FieldRef> {
    let target_precision = declared_precision(target)?;
    if matches!(
        source.data_type(),
        DataType::Time32(_) | DataType::Time64(_)
    ) && let Some(precision) = target_precision
    {
        source_precision(source)?;
        let mut field = source
            .as_ref()
            .clone()
            .with_data_type(DataType::Time64(TimeUnit::Nanosecond));
        field.metadata_mut().insert(
            SAIL_SPARK_TIME_METADATA_KEY.to_string(),
            SparkTimeMetadata::try_new(precision)
                .and_then(SparkTimeMetadata::to_json)
                .map_err(|error| plan_datafusion_err!("{error}"))?,
        );
        return Ok(Arc::new(field));
    }
    let data_type = match (source.data_type(), target.data_type()) {
        (DataType::Struct(from), DataType::Struct(to)) if from.len() == to.len() => {
            DataType::Struct(
                from.iter()
                    .zip(to)
                    .map(|(a, b)| prepare_field(a, b))
                    .collect::<Result<_>>()?,
            )
        }
        (DataType::List(from), to) if list_element(to).is_some() => DataType::List(prepare_field(
            from,
            list_element(to).ok_or_else(|| plan_datafusion_err!("missing list element"))?,
        )?),
        (DataType::LargeList(from), to) if list_element(to).is_some() => {
            DataType::LargeList(prepare_field(
                from,
                list_element(to).ok_or_else(|| plan_datafusion_err!("missing list element"))?,
            )?)
        }
        (DataType::FixedSizeList(from, length), to) if list_element(to).is_some() => {
            DataType::FixedSizeList(
                prepare_field(
                    from,
                    list_element(to).ok_or_else(|| plan_datafusion_err!("missing list element"))?,
                )?,
                *length,
            )
        }
        (DataType::Map(from, sorted), DataType::Map(to, _)) => {
            DataType::Map(prepare_field(from, to)?, *sorted)
        }
        _ => return Ok(Arc::clone(source)),
    };
    Ok(Arc::new(source.as_ref().clone().with_data_type(data_type)))
}

fn transform(input: &ArrayRef, source: &FieldRef, output: &FieldRef) -> Result<ArrayRef> {
    if source == output {
        return Ok(Arc::clone(input));
    }
    if matches!(
        source.data_type(),
        DataType::Time32(_) | DataType::Time64(_)
    ) {
        let precision = declared_precision(output)?
            .ok_or_else(|| plan_datafusion_err!("missing target TIME precision"))?;
        let normalized = cast(input, &DataType::Time64(TimeUnit::Nanosecond))?;
        if source_precision(source)? == precision {
            return Ok(normalized);
        }
        let factor = 10_i64.pow((9 - precision) as u32);
        // Spark's truncateTimeToPrecision uses integer division, not floating point.
        return Ok(Arc::new(
            normalized
                .as_primitive::<Time64NanosecondType>()
                .unary::<_, Time64NanosecondType>(|value| value / factor * factor),
        ));
    }
    match (source.data_type(), output.data_type()) {
        (DataType::Struct(from), DataType::Struct(to)) => {
            let array = input.as_struct();
            let columns = array
                .columns()
                .iter()
                .zip(from)
                .zip(to)
                .map(|((column, a), b)| transform(column, a, b))
                .collect::<Result<Vec<_>>>()?;
            Ok(Arc::new(StructArray::try_new_with_length(
                to.clone(),
                columns,
                array.nulls().cloned(),
                array.len(),
            )?))
        }
        (DataType::List(from), DataType::List(to)) => {
            let array = input.as_list::<i32>();
            Ok(Arc::new(ListArray::try_new(
                Arc::clone(to),
                array.offsets().clone(),
                transform(array.values(), from, to)?,
                array.nulls().cloned(),
            )?))
        }
        (DataType::LargeList(from), DataType::LargeList(to)) => {
            let array = input.as_list::<i64>();
            Ok(Arc::new(LargeListArray::try_new(
                Arc::clone(to),
                array.offsets().clone(),
                transform(array.values(), from, to)?,
                array.nulls().cloned(),
            )?))
        }
        (DataType::FixedSizeList(from, _), DataType::FixedSizeList(to, length)) => {
            let array = input.as_fixed_size_list();
            Ok(Arc::new(FixedSizeListArray::try_new_with_length(
                Arc::clone(to),
                *length,
                transform(array.values(), from, to)?,
                array.nulls().cloned(),
                array.len(),
            )?))
        }
        (DataType::Map(from, _), DataType::Map(to, sorted)) => {
            let array = input.as_map();
            let entries: ArrayRef = Arc::new(array.entries().clone());
            let entries = transform(&entries, from, to)?;
            Ok(Arc::new(MapArray::try_new(
                Arc::clone(to),
                array.offsets().clone(),
                entries.as_struct().clone(),
                array.nulls().cloned(),
                *sorted,
            )?))
        }
        _ => exec_err!("TIME precision transform changed a non-TIME leaf"),
    }
}

#[cfg(test)]
#[path = "spark_time_precision_cast_tests.rs"]
mod tests;
