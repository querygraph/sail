use std::sync::Arc;

use arrow::datatypes::DataType;
use datafusion::optimizer::simplify_expressions::ExprSimplifier;
use datafusion_common::DFSchema;
use datafusion_expr::simplify::SimplifyContextBuilder;
use datafusion_expr::{Expr, ExprSchemable};
use sail_catalog::utils::quote_names_if_needed;
use sail_common::spec;
use sail_common_datafusion::literal::{LiteralEvaluator, LiteralValue};
use sail_sql_analyzer::expression::from_ast_object_name;
use sail_sql_analyzer::parser::parse_object_name;

use crate::error::{PlanError, PlanResult};
use crate::function::constant_context::ConstantContext;
use crate::resolver::PlanResolver;
use crate::resolver::state::PlanResolverState;

impl PlanResolver<'_> {
    /// Resolve SQL's catalog-name expression without selecting a catalog or
    /// executing a query. Direct Catalog API names do not need this SQL parsing.
    pub async fn resolve_catalog_name(&self, expression: spec::Expr) -> PlanResult<String> {
        let text = match expression {
            spec::Expr::IdentifierClause { expr } => {
                let mut state = PlanResolverState::new();
                state.config_mut().analyzer_identifier = true;
                // Catalog IDENTIFIER resolves scalar arguments before foldability;
                // table identifiers alone remap unresolved columns to NOT_CONSTANT.
                self.resolve_scalar_identifier_expr(*expr, &Arc::new(DFSchema::empty()), &mut state)
                    .await?
            }
            expression => {
                self.resolve_unwrapped_catalog_expression(expression)
                    .await?
            }
        };
        let name = from_ast_object_name(
            parse_object_name(&text).map_err(|error| PlanError::analysis(error.to_string()))?,
        )?;
        match Vec::<String>::from(name).as_slice() {
            [name] => Ok(name.clone()),
            _ => Err(PlanError::analysis(
                "[INVALID_SQL_SYNTAX.MULTI_PART_NAME] SET CATALOG requires a single-part name",
            )),
        }
    }

    /// Unwrapped directives validate the analyzed expression without optimization.
    async fn resolve_unwrapped_catalog_expression(
        &self,
        expression: spec::Expr,
    ) -> PlanResult<String> {
        let expression = match expression {
            spec::Expr::UnresolvedAttribute { name, .. } => {
                spec::Expr::Literal(spec::Literal::Utf8 {
                    value: Some(quote_names_if_needed(name.parts())),
                })
            }
            expression => expression,
        };
        let schema = Arc::new(DFSchema::empty());
        let mut state = PlanResolverState::new();
        state.config_mut().require_foldable = true;
        state.config_mut().constant_context = ConstantContext::Identifier;
        let resolved = self
            .resolve_expression(expression, &schema, &mut state)
            .await
            .map_err(|error| PlanError::analysis(error.to_string()))?;
        let context = SimplifyContextBuilder::default()
            .with_schema(schema.clone())
            .with_current_time()
            .build();
        let simplifier = ExprSimplifier::new(context);
        let resolved = simplifier
            .coerce(resolved, &schema)
            .map_err(|error| PlanError::analysis(error.to_string()))?;
        // Type validation precedes evaluation: an integer division by zero is
        // a wrong-type error here, while its CAST AS STRING may fail execution.
        if !matches!(
            resolved.get_type(schema.as_ref())?,
            DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View
        ) {
            return Err(PlanError::analysis(
                "[NOT_A_CONSTANT_STRING.WRONG_TYPE] catalog expression must have STRING type",
            ));
        }
        let resolved = simplifier.simplify(resolved)?;
        let scalar = LiteralEvaluator::new().evaluate(&resolved)?;
        if scalar.is_null() {
            return Err(PlanError::analysis(
                "[NOT_A_CONSTANT_STRING.NULL] catalog expression must not be NULL",
            ));
        }
        Ok(LiteralValue(&scalar).try_to_string()?.to_string())
    }

    /// Check before simplification or physical evaluation can erase a volatile
    /// function or row-dependent child. Resolution calls this for each child too.
    pub(super) fn require_constant_expression(expression: &Expr) -> PlanResult<()> {
        if !validation::is_constant_expression(expression)? {
            Err(PlanError::analysis(
                "[NOT_A_CONSTANT_STRING.NOT_CONSTANT] expression is not foldable",
            ))
        } else {
            Ok(())
        }
    }
}

#[cfg(test)]
mod tests;

mod validation;

#[cfg(test)]
mod validation_tests;
