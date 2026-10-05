use std::sync::Arc;

use datafusion::arrow::datatypes::{Schema, SchemaRef};
use datafusion::datasource::provider_as_source;
use datafusion_common::TableReference;
use datafusion_common::tree_node::{TreeNode, TreeNodeRecursion};
use datafusion_expr::{LogicalPlan, LogicalPlanBuilder, SubqueryAlias, TableSource};
use sail_common::spec;
use sail_common_datafusion::cte_work_table::SharedCteWorkTable;

use crate::error::PlanResult;
use crate::resolver::PlanResolver;
use crate::resolver::query::cte::CteKind;
use crate::resolver::state::PlanResolverState;

impl PlanResolver<'_> {
    /// Resolves one CTE of a `WITH RECURSIVE` clause.
    ///
    /// A recursive CTE is `static_term UNION [ALL] recursive_term`, where only the
    /// recursive term may refer to the CTE itself. The static term is resolved first.
    /// The self-reference is then bound to a work table with the static term's
    /// schema, and the recursive term is resolved against it. The result is a
    /// [`LogicalPlan::RecursiveQuery`], which DataFusion executes by feeding each
    /// iteration's output back through the work table until an iteration is empty.
    ///
    /// A CTE whose body is not a `UNION`, or whose second term does not refer to
    /// the CTE, is resolved as an ordinary CTE (or an ordinary union).
    pub(super) async fn resolve_recursive_query_plan(
        &self,
        reference: &TableReference,
        plan: spec::QueryPlan,
        state: &mut PlanResolverState,
    ) -> PlanResult<LogicalPlan> {
        // The SQL analyzer wraps every CTE body in a table alias carrying the
        // CTE's name and optional column list.
        let (body, alias) = match plan.node {
            spec::QueryNode::TableAlias {
                input,
                name,
                columns,
            } => (*input, Some((name, columns, plan.plan_id))),
            node => (
                spec::QueryPlan {
                    node,
                    plan_id: plan.plan_id,
                },
                None,
            ),
        };
        let (left, right, is_all) = match body.node {
            spec::QueryNode::SetOperation(spec::SetOperation {
                left,
                right,
                set_op_type: spec::SetOpType::Union,
                is_all,
                by_name: false,
                allow_missing_columns: false,
            }) => (left, right, is_all),
            node => {
                let body = spec::QueryPlan {
                    node,
                    plan_id: body.plan_id,
                };
                let plan = match alias {
                    Some((name, columns, plan_id)) => spec::QueryPlan {
                        node: spec::QueryNode::TableAlias {
                            input: Box::new(body),
                            name,
                            columns,
                        },
                        plan_id,
                    },
                    None => body,
                };
                return self.resolve_query_plan(plan, state).await;
            }
        };

        // The static term, with the CTE's column list applied to its output.
        let static_term = match alias {
            Some((name, columns, _)) => spec::QueryPlan::new(spec::QueryNode::TableAlias {
                input: left,
                name,
                columns,
            }),
            None => *left,
        };
        let static_plan = self.resolve_query_plan(static_term, state).await?;

        // The work table that stands for the CTE inside the recursive term. Values
        // from earlier iterations may be null even where the static term's are not.
        // The recursive term may refer to the CTE more than once: every reference
        // replays the same iteration's rows.
        let name = reference.table().to_string();
        let work_table: Arc<dyn TableSource> = provider_as_source(Arc::new(
            SharedCteWorkTable::new(&name, nullable_schema(static_plan.schema().inner())),
        ));
        let work_table_plan =
            LogicalPlanBuilder::scan(name.clone(), Arc::clone(&work_table), None)?.build()?;
        let work_table_plan = LogicalPlan::SubqueryAlias(SubqueryAlias::try_new(
            Arc::new(work_table_plan),
            reference.clone(),
        )?);
        state.insert_cte(reference.clone(), work_table_plan, CteKind::Definition)?;

        let recursive_plan = self.resolve_query_plan(*right, state).await?;
        let builder = LogicalPlanBuilder::from(static_plan);
        let plan = if refers_to(&recursive_plan, &work_table) {
            builder.to_recursive_query(name, recursive_plan, !is_all)?
        } else if is_all {
            builder.union(recursive_plan)?
        } else {
            builder.union_distinct(recursive_plan)?
        }
        .build()?;
        Ok(plan)
    }
}

/// `schema` with every field nullable, keeping field and schema metadata.
fn nullable_schema(schema: &Schema) -> SchemaRef {
    Arc::new(Schema::new_with_metadata(
        schema
            .fields()
            .iter()
            .map(|field| field.as_ref().clone().with_nullable(true))
            .collect::<Vec<_>>(),
        schema.metadata().clone(),
    ))
}

/// Whether `plan` scans the given work table.
fn refers_to(plan: &LogicalPlan, work_table: &Arc<dyn TableSource>) -> bool {
    let mut found = false;
    // The closure never fails.
    let _ = plan.apply(|node| {
        if let LogicalPlan::TableScan(scan) = node
            && Arc::ptr_eq(&scan.source, work_table)
        {
            found = true;
            return Ok(TreeNodeRecursion::Stop);
        }
        Ok(TreeNodeRecursion::Continue)
    });
    found
}
