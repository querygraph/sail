use datafusion::arrow::datatypes::{DataType, FieldRef};
use datafusion_common::tree_node::TreeNode;
use datafusion_common::{Result, plan_err};
use datafusion_expr::{
    ColumnarValue, Expr, ReturnFieldArgs, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature,
    Volatility,
};

/// Retain function foldability when lowering erases its original expression.
/// Constant-name validation rejects this marker. Type-only expressions may
/// read its unchanged type and discard it, without evaluating its value.
pub fn mark_non_foldable(expression: Expr) -> Expr {
    ScalarUDF::from(NonFoldable {
        signature: Signature::any(1, Volatility::Volatile),
    })
    .call(vec![expression])
}

pub(super) fn contains_marker(expression: &Expr) -> Result<bool> {
    expression.exists(|node| Ok(matches!(node, Expr::ScalarFunction(function) if function.func.inner().is::<NonFoldable>())))
}

#[derive(Debug, PartialEq, Eq, Hash)]
struct NonFoldable {
    signature: Signature,
}
impl ScalarUDFImpl for NonFoldable {
    fn name(&self) -> &str {
        "sail_non_foldable"
    }
    fn signature(&self) -> &Signature {
        &self.signature
    }
    fn return_type(&self, arguments: &[DataType]) -> Result<DataType> {
        match arguments {
            [value] => Ok(value.clone()),
            _ => plan_err!("non-foldable marker requires one argument"),
        }
    }
    fn return_field_from_args(&self, arguments: ReturnFieldArgs) -> Result<FieldRef> {
        match arguments.arg_fields {
            [field] => Ok(field.clone()),
            _ => plan_err!("non-foldable marker requires one argument"),
        }
    }
    fn invoke_with_args(&self, _: ScalarFunctionArgs) -> Result<ColumnarValue> {
        plan_err!("[NOT_A_CONSTANT_STRING.NOT_CONSTANT] non-foldable marker cannot be evaluated")
    }
}

#[cfg(test)]
mod tests {
    use datafusion_common::DFSchema;
    use datafusion_expr::{ExprSchemable, lit, when};

    use super::*;
    use crate::literal::LiteralEvaluator;

    #[test]
    fn materialization_keeps_type_and_rejects_nonfoldable_dead_branches() -> Result<()> {
        let marked = mark_non_foldable(lit("erased"));
        assert_eq!(marked.get_type(&DFSchema::empty())?, DataType::Utf8);
        let expression = when(lit(false), marked).otherwise(lit("kept"))?;
        let error = LiteralEvaluator::new()
            .evaluate(&expression)
            .err()
            .ok_or_else(|| {
                datafusion_common::plan_datafusion_err!("materialization erased non-foldability")
            })?;
        assert!(
            error
                .to_string()
                .contains("NOT_A_CONSTANT_STRING.NOT_CONSTANT")
        );
        Ok(())
    }
}
