//! `nutmeg_bucket(key, n)`: the bucket DataFusion's own repartitioning would
//! give a row for `Partitioning::Hash([key], n)`, as a scalar function.
//!
//! With it, a checkpoint can be written through Sail's ordinary, distributed
//! Parquet writer, `partitionBy` on the bucket, and still be declared as
//! hash partitioned by `key` truthfully: the bucket is computed by the same
//! hash the engine uses, so a declared scan co-partitions with any other
//! declared scan on the same key and count, and with anything the engine
//! itself hash partitions on them. Pass the key column itself, not a cast of
//! it: the hash is of the value in its type.
//!
//! Exported by the `nutmeg-bucket` entry point, placed `any`, because the
//! host refuses scalar functions from a driver-only extension.
use std::sync::Arc;

use arrow::array::{Array, ArrayRef, Int64Array};
use arrow::datatypes::{DataType, Field, FieldRef};
use datafusion_common::hash_utils::create_hashes;
use datafusion_common::{Result, ScalarValue, exec_err};
use datafusion::logical_expr::{
    ColumnarValue, ReturnFieldArgs, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature,
    Volatility,
};
use datafusion::physical_plan::repartition::REPARTITION_RANDOM_STATE;
use datafusion_ffi::udf::FFI_ScalarUDF;
use pyo3::prelude::*;
use pyo3::types::PyCapsule;

#[derive(Debug, PartialEq, Eq, Hash)]
pub struct NutmegBucket {
    signature: Signature,
}

impl Default for NutmegBucket {
    fn default() -> Self {
        Self {
            signature: Signature::any(2, Volatility::Immutable),
        }
    }
}

/// The engine's bucket of every row of `key` among `partitions`.
pub fn buckets(key: &ArrayRef, partitions: u64) -> Result<Int64Array> {
    let mut hashes = vec![0u64; key.len()];
    create_hashes(&[Arc::clone(key)], REPARTITION_RANDOM_STATE.random_state(), &mut hashes)?;
    Ok(Int64Array::from(
        hashes
            .iter()
            .map(|hash| (*hash % partitions) as i64)
            .collect::<Vec<_>>(),
    ))
}

impl ScalarUDFImpl for NutmegBucket {
    fn name(&self) -> &str {
        "nutmeg_bucket"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Int64)
    }

    fn return_field_from_args(&self, _args: ReturnFieldArgs) -> Result<FieldRef> {
        Ok(Arc::new(Field::new(self.name(), DataType::Int64, false)))
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let [key, partitions] = args.args.as_slice() else {
            return exec_err!("nutmeg_bucket takes a key and a partition count");
        };
        // The count reaches us as whatever the host planned the literal as:
        // an integer scalar of some width, or a constant array of one across
        // the FFI. Read it through a cast to Int64 either way.
        let count = match partitions {
            ColumnarValue::Scalar(scalar) => scalar.to_array_of_size(1)?,
            ColumnarValue::Array(array) => Arc::clone(array),
        };
        let count = arrow::compute::cast(&count, &DataType::Int64)?;
        let count = count
            .as_any()
            .downcast_ref::<Int64Array>()
            .ok_or_else(|| datafusion_common::DataFusionError::Internal("cast to Int64".into()))?;
        if count.is_empty() || count.is_null(0) || count.value(0) < 1 {
            return exec_err!(
                "nutmeg_bucket needs a positive integer partition count, got {partitions:?} over {} rows",
                args.number_rows
            );
        }
        if count.len() > 1 && (0..count.len()).any(|i| count.is_null(i) || count.value(i) != count.value(0)) {
            return exec_err!("nutmeg_bucket needs one constant partition count");
        }
        let partitions = count.value(0) as u64;
        let key = key.to_array(args.number_rows)?;
        Ok(ColumnarValue::Array(Arc::new(buckets(&key, partitions)?)))
    }
}

#[pyclass(skip_from_py_object)]
#[derive(Clone)]
pub struct NativeScalarUdf {
    inner: Arc<ScalarUDF>,
}

#[pymethods]
impl NativeScalarUdf {
    fn name(&self) -> &str {
        self.inner.name()
    }

    fn aliases(&self) -> Vec<String> {
        self.inner.aliases().to_vec()
    }

    fn __datafusion_scalar_udf__<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyCapsule>> {
        PyCapsule::new_with_value(
            py,
            FFI_ScalarUDF::from(Arc::clone(&self.inner)),
            c"datafusion_scalar_udf",
        )
    }
}

/// The functions-only extension binding: no relations, no memory lease.
#[pyclass]
pub struct BoundBucket {
    #[pyo3(get)]
    session_id: String,
    functions: Vec<NativeScalarUdf>,
}

#[pymethods]
impl BoundBucket {
    #[new]
    fn new(session_id: String) -> Self {
        Self {
            session_id,
            functions: vec![NativeScalarUdf {
                inner: Arc::new(ScalarUDF::new_from_impl(NutmegBucket::default())),
            }],
        }
    }

    fn scalar_udfs(&self) -> Vec<NativeScalarUdf> {
        self.functions.clone()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::physical_expr::expressions::Column;
    use datafusion::physical_plan::repartition::RepartitionExec;
    use datafusion::physical_plan::common::collect;
    use datafusion::physical_plan::{ExecutionPlan, Partitioning};
    use datafusion::prelude::SessionContext;
    use arrow::datatypes::Schema;
    use arrow::record_batch::RecordBatch;

    /// The function's bucket is the partition RepartitionExec sends the row to.
    #[tokio::test]
    async fn bucket_equals_the_engine_repartition() {
        let n = 5usize;
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
        let ids: Vec<i64> = (0..5000).map(|i| (i * 7919) % 613 - 100).collect();
        let batch = RecordBatch::try_new(Arc::clone(&schema), vec![Arc::new(Int64Array::from(ids.clone()))]).unwrap();
        let input = datafusion::datasource::memory::MemorySourceConfig::try_new_exec(&[vec![batch]], schema, None).unwrap();
        let repartitioned = RepartitionExec::try_new(
            input,
            Partitioning::Hash(vec![Arc::new(Column::new("id", 0))], n),
        )
        .unwrap();
        let ctx = SessionContext::new();
        let plan: Arc<dyn ExecutionPlan> = Arc::new(repartitioned);
        let key: ArrayRef = Arc::new(Int64Array::from(ids.clone()));
        let computed = buckets(&key, n as u64).unwrap();
        for bucket in 0..n {
            let batches = collect(plan.execute(bucket, ctx.task_ctx()).unwrap()).await.unwrap();
            let mut engine: Vec<i64> = batches
                .iter()
                .flat_map(|b| b.column(0).as_any().downcast_ref::<Int64Array>().unwrap().values().to_vec())
                .collect();
            let mut ours: Vec<i64> = ids
                .iter()
                .zip(computed.values())
                .filter(|(_, b)| **b as usize == bucket)
                .map(|(k, _)| *k)
                .collect();
            engine.sort();
            ours.sort();
            assert_eq!(engine, ours, "bucket {bucket}");
        }
    }
}
