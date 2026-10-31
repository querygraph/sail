use std::sync::Arc;

use datafusion::arrow::array::{Array, ArrayRef, new_empty_array, new_null_array};
use datafusion::arrow::compute::{concat, take_arrays};
use datafusion::arrow::datatypes::{DataType, Field, FieldRef};
use datafusion_common::{Result, exec_err, plan_err};
use datafusion_expr::{
    ColumnarValue, HigherOrderFunctionArgs, HigherOrderReturnFieldArgs, HigherOrderSignature,
    HigherOrderUDFImpl, LambdaParametersProgress, ValueOrLambda, Volatility,
};

use crate::functions_nested_utils::evaluate_lambda_rows;
use crate::scalar::math::divisor::numeric_zero_mask;

/// Internal DivModLike control flow: right thunk, left thunk, operation(left, right).
///
/// The operation lambda retains native arithmetic and its coercion. The thunks
/// run once per reached row, right first. No failing batch is evaluated again.
/// This component is not registered as a public SQL function.
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct SparkDivModLazy {
    signature: HigherOrderSignature,
    ansi_mode: bool,
}

impl SparkDivModLazy {
    pub fn new(ansi_mode: bool) -> Self {
        Self {
            signature: HigherOrderSignature::exact(
                vec![ValueOrLambda::Lambda(()); 3],
                Volatility::Immutable,
            ),
            ansi_mode,
        }
    }

    pub fn ansi_mode(&self) -> bool {
        self.ansi_mode
    }
}

impl HigherOrderUDFImpl for SparkDivModLazy {
    fn name(&self) -> &str {
        "spark_divmod_lazy"
    }

    fn signature(&self) -> &HigherOrderSignature {
        &self.signature
    }

    fn lambda_parameters(
        &self,
        _step: usize,
        fields: &[ValueOrLambda<FieldRef, Option<FieldRef>>],
    ) -> Result<LambdaParametersProgress> {
        let [right, left, _operation] = lambda_args(fields)?;
        let dummy = Arc::new(Field::new("", DataType::Null, true));
        if let (Some(right), Some(left)) = (right, left) {
            validate_divisor_type(right.data_type())?;
            Ok(LambdaParametersProgress::Complete(vec![
                vec![Arc::clone(&dummy)],
                vec![dummy],
                vec![Arc::clone(left), Arc::clone(right)],
            ]))
        } else {
            Ok(LambdaParametersProgress::Partial(vec![
                Some(vec![Arc::clone(&dummy)]),
                Some(vec![dummy]),
                None,
            ]))
        }
    }

    fn return_field_from_args(&self, args: HigherOrderReturnFieldArgs) -> Result<FieldRef> {
        let [right, _left, operation] = lambda_args(args.arg_fields)?;
        validate_divisor_type(right.data_type())?;
        Ok(Arc::new(operation.as_ref().clone().with_nullable(true)))
    }

    fn short_circuits(&self) -> bool {
        true
    }

    fn coerce_value_types(&self, arg_types: &[DataType]) -> Result<Vec<DataType>> {
        if !arg_types.is_empty() {
            return plan_err!("spark_divmod_lazy accepts only lambda arguments");
        }
        Ok(vec![])
    }

    fn invoke_with_args(&self, args: HigherOrderFunctionArgs) -> Result<ColumnarValue> {
        let [right, left, operation] = lambda_args(&args.args)?;
        let [right_field, _left_field, _operation_field] = lambda_args(&args.arg_fields)?;
        validate_divisor_type(right_field.data_type())?;
        let mut chunks = Vec::<ArrayRef>::with_capacity(args.number_rows);
        for row in 0..args.number_rows {
            let row = u64::try_from(row)
                .map_err(|_| datafusion_common::exec_datafusion_err!("row index exceeds u64"))?;
            let right_value = evaluate_lambda_rows(right, &[row])?;
            if right_value.is_null(0)
                || (!self.ansi_mode && numeric_zero_mask(&right_value)?.true_count() != 0)
            {
                chunks.push(new_null_array(args.return_type(), 1));
                continue;
            }
            let left_value = evaluate_lambda_rows(left, &[row])?;
            if left_value.is_null(0) {
                chunks.push(new_null_array(args.return_type(), 1));
                continue;
            }
            let left_arg = || Ok(Arc::clone(&left_value));
            let right_arg = || Ok(Arc::clone(&right_value));
            let indices = datafusion::arrow::array::UInt64Array::from(vec![row]);
            let value = operation
                .evaluate(&[&left_arg, &right_arg], |arrays| {
                    Ok(take_arrays(arrays, &indices, None)?)
                })?
                .into_array(1)?;
            chunks.push(value);
        }
        if chunks.is_empty() {
            return Ok(ColumnarValue::Array(new_empty_array(args.return_type())));
        }
        let arrays = chunks.iter().map(|a| a.as_ref()).collect::<Vec<_>>();
        Ok(ColumnarValue::Array(concat(&arrays)?))
    }
}

fn lambda_args<V, L>(args: &[ValueOrLambda<V, L>]) -> Result<[&L; 3]> {
    match args {
        [
            ValueOrLambda::Lambda(right),
            ValueOrLambda::Lambda(left),
            ValueOrLambda::Lambda(operation),
        ] => Ok([right, left, operation]),
        _ => exec_err!("spark_divmod_lazy expects exactly three lambda arguments"),
    }
}

fn validate_divisor_type(data_type: &DataType) -> Result<()> {
    if data_type.is_numeric() || data_type == &DataType::Null {
        Ok(())
    } else {
        plan_err!("spark_divmod_lazy expects a numeric or NULL divisor, got {data_type}")
    }
}
