use std::sync::Arc;

use arrow::array::{Array, ArrayRef, ListArray, MapArray, StructArray, new_null_array};
use arrow::compute::{CastOptions, cast_with_options};
use arrow::datatypes::DataType;
use datafusion_common::{Result, exec_datafusion_err, exec_err};
use sail_common_datafusion::array::record_batch::cast_array_recursively;

use super::{Conversion, FieldPlan};

pub(super) fn adapt_field(plan: &FieldPlan, columns: &[ArrayRef], rows: usize) -> Result<ArrayRef> {
    match plan.source_index {
        None => Ok(new_null_array(plan.target.data_type(), rows)),
        Some(index) => {
            let source = columns
                .get(index)
                .ok_or_else(|| exec_datafusion_err!("ORC mapped column {index} is absent"))?;
            adapt_array(plan, source)
        }
    }
}

fn adapt_array(plan: &FieldPlan, source: &ArrayRef) -> Result<ArrayRef> {
    match &plan.conversion {
        Conversion::Identity => Ok(Arc::clone(source)),
        Conversion::SafeInteger => Ok(cast_with_options(
            source,
            plan.target.data_type(),
            &CastOptions {
                safe: true,
                ..Default::default()
            },
        )?),
        Conversion::TimestampLabel => cast_array_recursively(source, plan.target.data_type()),
        Conversion::Struct(fields) => {
            let source = source
                .as_any()
                .downcast_ref::<StructArray>()
                .ok_or_else(|| {
                    exec_datafusion_err!(
                        "ORC requested struct has a different input representation"
                    )
                })?;
            let columns = fields
                .iter()
                .map(|field| adapt_field(field, source.columns(), source.len()))
                .collect::<Result<Vec<_>>>()?;
            let DataType::Struct(target_fields) = plan.target.data_type() else {
                return exec_err!("ORC requested struct plan has a different target type");
            };
            Ok(Arc::new(StructArray::try_new_with_length(
                target_fields.clone(),
                columns,
                source.nulls().cloned(),
                source.len(),
            )?))
        }
        Conversion::List(element) => {
            let source = source.as_any().downcast_ref::<ListArray>().ok_or_else(|| {
                exec_datafusion_err!("ORC requested list has a different input representation")
            })?;
            Ok(Arc::new(ListArray::try_new(
                Arc::clone(&element.target),
                source.offsets().clone(),
                adapt_array(element, source.values())?,
                source.nulls().cloned(),
            )?))
        }
        Conversion::Map(key, value) => {
            let source = source.as_any().downcast_ref::<MapArray>().ok_or_else(|| {
                exec_datafusion_err!("ORC requested map has a different input representation")
            })?;
            let DataType::Map(entries, sorted) = plan.target.data_type() else {
                return exec_err!("ORC requested map plan has a different target type");
            };
            let DataType::Struct(fields) = entries.data_type() else {
                return exec_err!("ORC requested map entries are not a struct");
            };
            let arrays = vec![
                adapt_array(key, source.keys())?,
                adapt_array(value, source.values())?,
            ];
            let entries_array = StructArray::try_new_with_length(
                fields.clone(),
                arrays,
                None,
                source.entries().len(),
            )?;
            Ok(Arc::new(MapArray::try_new(
                Arc::clone(entries),
                source.offsets().clone(),
                entries_array,
                source.nulls().cloned(),
                *sorted,
            )?))
        }
    }
}
