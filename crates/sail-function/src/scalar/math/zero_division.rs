use std::collections::HashMap;
use std::sync::Arc;

use datafusion::arrow::datatypes::{DataType, Field, FieldRef};
use datafusion_common::{DataFusionError, Result};
use datafusion_expr::{
    ColumnarValue, ReturnFieldArgs, ScalarFunctionArgs, ScalarUDFImpl, Signature, Volatility,
};
use sail_common_datafusion::error::StructuredSparkError;

use crate::error::invalid_arg_count_exec_err;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ZeroDivisionKind {
    Divide,
    Remainder,
}

/// An internal error branch evaluated only when a numeric divisor is zero.
/// A NULL result type lets the surrounding CASE retain the divisor's type.
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct SparkZeroDivisionError {
    kind: ZeroDivisionKind,
    signature: Signature,
}

impl SparkZeroDivisionError {
    pub fn new(kind: ZeroDivisionKind) -> Self {
        Self {
            kind,
            signature: Signature::nullary(Volatility::Immutable),
        }
    }
}

impl ScalarUDFImpl for SparkZeroDivisionError {
    fn name(&self) -> &str {
        match self.kind {
            ZeroDivisionKind::Divide => "spark_divide_by_zero",
            ZeroDivisionKind::Remainder => "spark_remainder_by_zero",
        }
    }
    fn signature(&self) -> &Signature {
        &self.signature
    }
    fn return_type(&self, _: &[DataType]) -> Result<DataType> {
        Ok(DataType::Null)
    }
    fn return_field_from_args(&self, _: ReturnFieldArgs) -> Result<FieldRef> {
        Ok(Arc::new(Field::new(self.name(), DataType::Null, true)))
    }
    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        if !args.args.is_empty() {
            return Err(invalid_arg_count_exec_err(
                self.name(),
                (0, 0),
                args.args.len(),
            ));
        }
        Err(self.kind.error())
    }
}

impl ZeroDivisionKind {
    pub(crate) fn error(self) -> DataFusionError {
        let (condition, operation, fallback) = match self {
            ZeroDivisionKind::Divide => ("DIVIDE_BY_ZERO", "Division", "try_divide"),
            ZeroDivisionKind::Remainder => ("REMAINDER_BY_ZERO", "Remainder", "try_mod"),
        };
        let config = "\"spark.sql.ansi.enabled\"";
        DataFusionError::External(Box::new(StructuredSparkError {
            class: "org.apache.spark.SparkArithmeticException".into(),
            condition: condition.into(),
            sql_state: "22012".into(),
            message: format!(
                "{operation} by zero. Use `{fallback}` to tolerate divisor being 0 and return NULL instead. If necessary set {config} to \"false\" to bypass this error."
            ),
            parameters: HashMap::from([("config".into(), config.into())]),
        }))
    }
}
