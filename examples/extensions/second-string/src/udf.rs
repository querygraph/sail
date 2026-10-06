//! Arrow batch execution and the DataFusion scalar interface.
use std::panic::{catch_unwind, AssertUnwindSafe};
use std::sync::Arc;

use arrow_schema::{DataType, Field, FieldRef};
use datafusion_common::arrow::array::{
    Array, ArrayRef, Float64Array, LargeStringArray, StringArray, StringViewArray,
};
use datafusion_common::{exec_err, plan_err, Result, ScalarValue};
use datafusion_expr::{
    ColumnarValue, ReturnFieldArgs, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature,
    Volatility,
};

use crate::function::Function;
use crate::options::{self, Options};
use crate::phonetic;

#[derive(Debug, PartialEq, Eq, Hash)]
pub struct SimilarityUdf {
    function: Function,
    configured: bool,
    name: &'static str,
    signature: Signature,
}

impl SimilarityUdf {
    pub fn new(function: Function, configured: bool) -> Self {
        let (name, signature) = if configured {
            (
                function.configured_name().unwrap_or(function.name()),
                // Preserve numeric literal types: inserted casts obscure literals
                // before return_field_from_args can validate the configuration.
                Signature::user_defined(Volatility::Immutable),
            )
        } else {
            (
                function.name(),
                Signature::string(
                    if function.phonetic() { 1 } else { 2 },
                    Volatility::Immutable,
                ),
            )
        };
        Self {
            function,
            configured,
            name,
            signature,
        }
    }

    fn arity(&self) -> usize {
        if !self.configured {
            if self.function.phonetic() {
                1
            } else {
                2
            }
        } else {
            match self.function {
                Function::MongeElkan | Function::JaroWinkler => 4,
                Function::NeedlemanWunsch | Function::SmithWaterman | Function::AffineGap => 5,
                _ => 3,
            }
        }
    }

    fn invoke(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        if args.args.len() != self.arity() {
            return exec_err!("{} expects {} arguments", self.name, self.arity());
        }
        let scalar_only = args
            .args
            .iter()
            .all(|arg| matches!(arg, ColumnarValue::Scalar(_)));
        let rows = if scalar_only { 1 } else { args.number_rows };
        // No first-row option extraction is required for an empty batch.
        if rows == 0 {
            let output: ArrayRef = if self.function.phonetic() {
                Arc::new(StringArray::from(Vec::<Option<&str>>::new()))
            } else {
                Arc::new(Float64Array::from(Vec::<Option<f64>>::new()))
            };
            return Ok(ColumnarValue::Array(output));
        }
        let config = if self.configured {
            let literals: Result<Vec<_>> = args.args[2..]
                .iter()
                .map(|value| match value {
                    ColumnarValue::Scalar(value) => Ok(value.clone()),
                    // Arrow FFI transports scalar values as broadcast arrays.
                    ColumnarValue::Array(array) if !array.is_empty() => {
                        ScalarValue::try_from_array(array, 0)
                    }
                    _ => exec_err!("{} has an empty option array", self.name),
                })
                .collect();
            let literals = literals?;
            options::parse(self.function, &literals.iter().collect::<Vec<_>>())?
        } else {
            Options::Default
        };
        let input_count = if self.function.phonetic() { 1 } else { 2 };
        let arrays: Result<Vec<_>> = args
            .args
            .into_iter()
            .take(input_count)
            .map(|arg| arg.into_array(rows))
            .collect();
        let arrays = arrays?;
        if arrays.iter().any(|array| array.len() != rows) {
            return exec_err!("{} received inconsistent Arrow cardinality", self.name);
        }
        let left = TextArray::new(&arrays[0])?;
        let result: ArrayRef = if self.function.phonetic() {
            Arc::new(
                (0..rows)
                    .map(|row| {
                        left.value(row).map(|text| match self.function {
                            Function::Soundex => phonetic::soundex(text),
                            Function::RefinedSoundex => phonetic::refined_soundex(text),
                            Function::DoubleMetaphone => phonetic::double_metaphone(text),
                            _ => String::new(),
                        })
                    })
                    .collect::<StringArray>(),
            )
        } else {
            let right = TextArray::new(&arrays[1])?;
            Arc::new(
                (0..rows)
                    .map(|row| match (left.value(row), right.value(row)) {
                        (Some(left), Some(right)) => {
                            Some(options::score(self.function, left, right, &config))
                        }
                        _ => None,
                    })
                    .collect::<Float64Array>(),
            )
        };
        if scalar_only {
            Ok(ColumnarValue::Scalar(ScalarValue::try_from_array(
                &result, 0,
            )?))
        } else {
            Ok(ColumnarValue::Array(result))
        }
    }
}

impl ScalarUDFImpl for SimilarityUdf {
    fn name(&self) -> &str {
        self.name
    }
    fn signature(&self) -> &Signature {
        &self.signature
    }
    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        if arg_types.len() != self.arity() {
            return plan_err!("{} expects {} arguments", self.name, self.arity());
        }
        for data_type in arg_types
            .iter()
            .take(if self.function.phonetic() { 1 } else { 2 })
        {
            if !matches!(
                data_type,
                DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View | DataType::Null
            ) {
                return plan_err!("{} requires string inputs", self.name);
            }
        }
        Ok(if self.function.phonetic() {
            DataType::Utf8
        } else {
            DataType::Float64
        })
    }
    fn coerce_types(&self, arg_types: &[DataType]) -> Result<Vec<DataType>> {
        self.return_type(arg_types)?;
        if self.configured {
            for (index, data_type) in arg_types.iter().enumerate().skip(2) {
                let valid = data_type == &DataType::Null
                    || match (self.function, index) {
                        (Function::MongeElkan, 2) => matches!(
                            data_type,
                            DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View
                        ),
                        (Function::JaroWinkler, 2) => data_type.is_numeric(),
                        _ => matches!(data_type, DataType::Int32 | DataType::Int64),
                    };
                if !valid {
                    return plan_err!("{} has an invalid option type {data_type}", self.name);
                }
            }
        }
        Ok(arg_types.to_vec())
    }
    fn return_field_from_args(&self, args: ReturnFieldArgs) -> Result<FieldRef> {
        let types: Vec<_> = args
            .arg_fields
            .iter()
            .map(|field| field.data_type().clone())
            .collect();
        let data_type = self.return_type(&types)?;
        if self.configured {
            let values: Result<Vec<_>> = args
                .scalar_arguments
                .iter()
                .skip(2)
                .map(|value| {
                    value.ok_or_else(|| {
                        datafusion_common::DataFusionError::Plan(format!(
                            "{} configuration must be literal",
                            self.name
                        ))
                    })
                })
                .collect();
            options::parse(self.function, &values?)?;
        }
        Ok(Arc::new(Field::new(
            self.name,
            data_type,
            args.arg_fields.iter().any(|field| field.is_nullable()),
        )))
    }
    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        catch_unwind(AssertUnwindSafe(|| self.invoke(args)))
            .unwrap_or_else(|_| exec_err!("{} native kernel panicked", self.name))
    }
}

enum TextArray<'a> {
    Utf8(&'a StringArray),
    Large(&'a LargeStringArray),
    View(&'a StringViewArray),
    Null,
}

impl<'a> TextArray<'a> {
    fn new(array: &'a ArrayRef) -> Result<Self> {
        match array.data_type() {
            DataType::Utf8 => array.as_any().downcast_ref::<StringArray>().map(Self::Utf8),
            DataType::LargeUtf8 => array
                .as_any()
                .downcast_ref::<LargeStringArray>()
                .map(Self::Large),
            DataType::Utf8View => array
                .as_any()
                .downcast_ref::<StringViewArray>()
                .map(Self::View),
            DataType::Null => Some(Self::Null),
            _ => None,
        }
        .ok_or_else(|| {
            datafusion_common::DataFusionError::Execution("expected an Arrow string array".into())
        })
    }
    fn value(&self, row: usize) -> Option<&str> {
        match self {
            Self::Utf8(array) => (!array.is_null(row)).then(|| array.value(row)),
            Self::Large(array) => (!array.is_null(row)).then(|| array.value(row)),
            Self::View(array) => (!array.is_null(row)).then(|| array.value(row)),
            Self::Null => None,
        }
    }
}

pub fn functions() -> Vec<Arc<ScalarUDF>> {
    Function::ALL
        .iter()
        .flat_map(|&function| {
            let mut definitions = vec![Arc::new(ScalarUDF::new_from_impl(SimilarityUdf::new(
                function, false,
            )))];
            if function.configured_name().is_some() {
                definitions.push(Arc::new(ScalarUDF::new_from_impl(SimilarityUdf::new(
                    function, true,
                ))));
            }
            definitions
        })
        .collect()
}
