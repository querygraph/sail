//! Folds a projection that only picks and renames columns into the in-memory
//! leaf below it: a shared CTE reference or a slot scan.
//!
//! Sail reads a CTE through a projection at every reference (its columns get
//! the reference's own ids), so a query that reads its CTEs in many places
//! has as many projections over references; SailDoom's tic had 820 of 5,227
//! operators. Each costs a stream, its metrics and a batch rebuild on every
//! run; the leaf can output the columns itself.

use std::sync::Arc;

use datafusion::common::Result;
use datafusion::common::config::ConfigOptions;
use datafusion::common::tree_node::{Transformed, TreeNode};
use datafusion::physical_expr::expressions::Column;
use datafusion::physical_optimizer::PhysicalOptimizerRule;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::physical_plan::projection::ProjectionExec;
use sail_common_datafusion::plan_reuse::SlotExec;
use sail_physical_plan::shared_cte::SharedCteRefExec;

#[derive(Debug, Default)]
pub struct FoldLeafProjections;

impl PhysicalOptimizerRule for FoldLeafProjections {
    fn optimize(
        &self,
        plan: Arc<dyn ExecutionPlan>,
        _config: &ConfigOptions,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        plan.transform_up(|node| {
            let Some(projection) = node.downcast_ref::<ProjectionExec>() else {
                return Ok(Transformed::no(node));
            };
            let columns = projection
                .expr()
                .iter()
                .map(|e| e.expr.downcast_ref::<Column>().map(|c| c.index()))
                .collect::<Option<Vec<_>>>();
            let Some(columns) = columns else {
                return Ok(Transformed::no(node));
            };
            let input = projection.input();
            let schema = projection.schema();
            if let Some(reference) = input.downcast_ref::<SharedCteRefExec>() {
                return Ok(Transformed::yes(Arc::new(
                    reference.with_output(&columns, schema),
                )));
            }
            if let Some(scan) = input.downcast_ref::<SlotExec>() {
                return Ok(Transformed::yes(Arc::new(
                    scan.with_output(&columns, schema),
                )));
            }
            Ok(Transformed::no(node))
        })
        .map(|t| t.data)
    }

    fn name(&self) -> &str {
        "fold_leaf_projections"
    }

    fn schema_check(&self) -> bool {
        true
    }
}
