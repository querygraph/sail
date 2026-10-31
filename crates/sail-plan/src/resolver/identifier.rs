use std::sync::Arc;

use arrow::datatypes::DataType;
use datafusion::optimizer::simplify_expressions::ExprSimplifier;
use datafusion_common::tree_node::{Transformed, TreeNode};
use datafusion_common::{DFSchema, ScalarValue};
use datafusion_expr::simplify::SimplifyContextBuilder;
use datafusion_expr::{Expr, ExprSchemable};
use sail_common::spec;
use sail_common_datafusion::literal::LiteralEvaluator;

use crate::error::{PlanError, PlanResult};
use crate::function::constant_context::ConstantContext;
use crate::resolver::PlanResolver;
use crate::resolver::state::PlanResolverState;

impl PlanResolver<'_> {
    /// A table identifier's subqueries are not analyzed before foldability is
    /// checked. Scalar IDENTIFIER keeps its ordinary expression resolution first.
    pub(super) async fn resolve_table_identifier_expr(
        &self,
        expression: spec::Expr,
        state: &mut PlanResolverState,
    ) -> PlanResult<String> {
        let mut scope = state.enter_config_scope();
        scope.state().config_mut().require_foldable = true;
        scope.state().config_mut().constant_context = ConstantContext::Identifier;
        scope.state().config_mut().analyzer_identifier = true;
        let resolved = self
            .resolve_expression(expression, &Arc::new(DFSchema::empty()), scope.state())
            .await
            .map_err(|error| match error {
                // Spark leaves unresolved identifier arguments on the table node;
                // CheckAnalysis reports NOT_CONSTANT instead of a column failure.
                PlanError::Structured(details)
                    if details.condition.starts_with("UNRESOLVED_COLUMN.") =>
                {
                    PlanError::analysis(
                        "[NOT_A_CONSTANT_STRING.NOT_CONSTANT] IDENTIFIER expression is unresolved",
                    )
                }
                error => error,
            })?;
        self.evaluate_identifier_expr(resolved, scope.state())
    }

    /// Scalar arguments finish semantic resolution before constant validation.
    pub(super) async fn resolve_scalar_identifier_expr(
        &self,
        expression: spec::Expr,
        schema: &datafusion_common::DFSchemaRef,
        state: &mut PlanResolverState,
    ) -> PlanResult<String> {
        let mut scope = state.enter_config_scope();
        scope.state().config_mut().constant_context = ConstantContext::Identifier;
        let resolved = self
            .resolve_expression(expression, schema, scope.state())
            .await?;
        self.evaluate_identifier_expr(resolved, scope.state())
    }

    /// Substitute bound SQL parameters, then validate before constant folding
    /// can remove a non-foldable child or evaluate a wrongly typed expression.
    pub(super) fn evaluate_identifier_expr(
        &self,
        expression: Expr,
        state: &PlanResolverState,
    ) -> PlanResult<String> {
        let expression = expression
            .transform(|expr| {
                if let Expr::Placeholder(placeholder) = &expr {
                    let value = state
                        .get_placeholder_value(&placeholder.id)
                        .ok_or_else(|| {
                            datafusion_common::plan_datafusion_err!(
                                "IDENTIFIER expression contains an unbound SQL parameter"
                            )
                        })?;
                    return Ok(Transformed::yes(Expr::Literal(value.clone(), None)));
                }
                Ok(Transformed::no(expr))
            })?
            .data;
        Self::require_constant_expression(&expression)?;
        let schema = Arc::new(DFSchema::empty());
        let simplifier = ExprSimplifier::new(
            SimplifyContextBuilder::default()
                .with_schema(schema.clone())
                .with_current_time()
                .build(),
        );
        let expression = simplifier.coerce(expression, &schema)?;
        if !matches!(
            expression.get_type(schema.as_ref())?,
            DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View
        ) {
            return Err(PlanError::analysis(
                "[NOT_A_CONSTANT_STRING.WRONG_TYPE] IDENTIFIER expression must have STRING type",
            ));
        }
        let expression = simplifier.simplify(expression)?;
        let scalar = LiteralEvaluator::new().evaluate(&expression)?;
        match scalar {
            ScalarValue::Utf8(Some(value))
            | ScalarValue::LargeUtf8(Some(value))
            | ScalarValue::Utf8View(Some(value)) => Ok(value),
            _ if scalar.is_null() => Err(PlanError::analysis(
                "[NOT_A_CONSTANT_STRING.NULL] IDENTIFIER expression must not be NULL",
            )),
            _ => Err(PlanError::internal(
                "STRING identifier evaluation produced a non-string scalar",
            )),
        }
    }
}

#[cfg(test)]
mod tests;

#[cfg(test)]
#[path = "identifier/provenance_tests.rs"]
mod provenance_tests;

#[cfg(test)]
#[path = "identifier/arithmetic_tests.rs"]
mod arithmetic_tests;
