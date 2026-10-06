//! Merges a projection into the projection below it when that costs nothing:
//! when the outer one only picks and renames columns, or when the inner one
//! only picks, renames or casts columns (or yields literals), so substituting
//! its expressions duplicates no real work. Sail renames columns at every
//! scope, so plans have many such pairs; SailDoom's tic had 147.

use std::sync::Arc;

use datafusion::common::Result;
use datafusion::common::config::ConfigOptions;
use datafusion::common::tree_node::{Transformed, TreeNode};
use datafusion::physical_expr::PhysicalExpr;
use datafusion::physical_expr::expressions::{CastExpr, Column, Literal};
use datafusion::physical_expr::projection::ProjectionExpr;
use datafusion::physical_optimizer::PhysicalOptimizerRule;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::physical_plan::projection::ProjectionExec;

#[derive(Debug, Default)]
pub struct MergeProjections;

fn is_trivial(expr: &Arc<dyn PhysicalExpr>) -> bool {
    if expr.downcast_ref::<Column>().is_some() || expr.downcast_ref::<Literal>().is_some() {
        return true;
    }
    match expr.downcast_ref::<CastExpr>() {
        Some(cast) => cast.expr().downcast_ref::<Column>().is_some(),
        None => false,
    }
}

/// `expr` with every column replaced by the inner projection's expression for it.
fn substitute(
    expr: &Arc<dyn PhysicalExpr>,
    inner: &[ProjectionExpr],
) -> Result<Arc<dyn PhysicalExpr>> {
    Ok(Arc::clone(expr)
        .transform_up(|e| match e.downcast_ref::<Column>() {
            Some(column) => Ok(Transformed::yes(Arc::clone(&inner[column.index()].expr))),
            None => Ok(Transformed::no(e)),
        })?
        .data)
}

impl PhysicalOptimizerRule for MergeProjections {
    fn optimize(
        &self,
        plan: Arc<dyn ExecutionPlan>,
        _config: &ConfigOptions,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        plan.transform_up(|node| {
            let Some(outer) = node.downcast_ref::<ProjectionExec>() else {
                return Ok(Transformed::no(node));
            };
            let Some(inner) = outer.input().downcast_ref::<ProjectionExec>() else {
                return Ok(Transformed::no(node));
            };
            // A lambda's variables are resolved against the batch its
            // projection sees; substituting into or under one breaks them.
            if crate::projection_pushdown::projection_contains_lambda_variable(outer)?
                || crate::projection_pushdown::projection_contains_lambda_variable(inner)?
            {
                return Ok(Transformed::no(node));
            }
            let outer_columns_only = outer
                .expr()
                .iter()
                .all(|e| e.expr.downcast_ref::<Column>().is_some());
            let inner_trivial = inner.expr().iter().all(|e| is_trivial(&e.expr));
            if !outer_columns_only && !inner_trivial {
                return Ok(Transformed::no(node));
            }
            let merged = outer
                .expr()
                .iter()
                .map(|e| {
                    Ok(ProjectionExpr {
                        expr: substitute(&e.expr, inner.expr())?,
                        alias: e.alias.clone(),
                    })
                })
                .collect::<Result<Vec<_>>>()?;
            let merged = ProjectionExec::try_new(merged, Arc::clone(inner.input()))?;
            if merged.schema() != outer.schema() {
                // Field metadata or nullability would change: leave it.
                return Ok(Transformed::no(node));
            }
            Ok(Transformed::yes(Arc::new(merged)))
        })
        .map(|t| t.data)
    }

    fn name(&self) -> &str {
        "merge_projections"
    }

    fn schema_check(&self) -> bool {
        true
    }
}
