use std::collections::{BTreeMap, BTreeSet};
use std::sync::{Arc, Mutex};

use datafusion::arrow::datatypes::Schema;
use datafusion::common::DFSchema;
use datafusion::logical_expr::{EmptyRelation, LogicalPlan};
use datafusion::prelude::SessionContext;
use sail_common::spec;
use sail_common_datafusion::extension::SessionExtensionAccessor;
use sail_common_datafusion::rename::logical_plan::rename_logical_plan;
use sail_plan::error::{PlanError, PlanResult};
use sail_plan::resolver::PlanResolver;
use sail_plan::resolver::scope::RelationScope;

use super::graph::{GraphDefinition, Name, analysis, quoted};
use crate::error::SparkResult;
use crate::session::SparkSession;
use crate::spark::connect::OutputType;

/// All declared outputs are known before analysis starts. An unresolved local
/// name must never fall through to a stale table from a previous pipeline run.
pub(super) struct GraphScope<'a> {
    pub graph: &'a GraphDefinition,
    pub resolved: &'a BTreeMap<Name, LogicalPlan>,
    pub reads: Mutex<BTreeSet<Name>>,
}

impl RelationScope for GraphScope<'_> {
    fn resolve(&self, name: &[String]) -> PlanResult<Option<LogicalPlan>> {
        let name = self
            .graph
            .resolve_name(name)
            .map_err(|e| PlanError::analysis(e.to_string()))?;
        if self.graph.outputs.iter().any(|x| x.name == name) {
            self.reads
                .lock()
                .map_err(|e| PlanError::internal(e.to_string()))?
                .insert(name.clone());
            self.resolved
                .get(&name)
                .cloned()
                .map(Some)
                .ok_or(PlanError::UnresolvedRelation(name))
        } else {
            Ok(None)
        }
    }
}

pub(super) struct AnalyzedGraph {
    pub order: Vec<Name>,
    pub dependencies: BTreeMap<Name, BTreeSet<Name>>,
}

pub(super) async fn analyze(
    ctx: &SessionContext,
    graph: &GraphDefinition,
) -> SparkResult<AnalyzedGraph> {
    graph.validate()?;
    let spark = ctx.extension::<SparkSession>()?;
    let mut resolved = BTreeMap::new();
    let mut order = vec![];
    let mut dependencies = BTreeMap::new();
    while order.len() < graph.outputs.len() {
        let before = order.len();
        for output in &graph.outputs {
            if resolved.contains_key(&output.name) {
                continue;
            }
            let scope = GraphScope {
                graph,
                resolved: &resolved,
                reads: Mutex::default(),
            };
            let mut plans = vec![];
            let mut pending = false;
            for flow in graph.flows.iter().filter(|x| x.target == output.name) {
                let config = spark.plan_config_with_overrides(flow.sql_conf.clone())?;
                let resolver = PlanResolver::new(ctx, config).with_relation_scope(&scope);
                match resolver
                    .resolve_named_plan(spec::Plan::Query(flow.query.clone()))
                    .await
                {
                    Ok(plan) => {
                        let plan = if let Some(fields) = plan.fields {
                            rename_logical_plan(plan.plan, &fields)?
                        } else {
                            plan.plan
                        };
                        plans.push(plan);
                    }
                    Err(PlanError::UnresolvedRelation(_)) => {
                        pending = true;
                        break;
                    }
                    Err(error) => return Err(error.into()),
                }
            }
            let inputs = scope.reads.into_inner()?;
            dependencies.insert(output.name.clone(), inputs);
            if pending {
                continue;
            }
            let plan = if output.kind == OutputType::TemporaryView {
                if plans.len() != 1 {
                    return Err(analysis("MULTIPLE_QUERIES_FOR_VIEW", quoted(&output.name)));
                }
                plans.remove(0)
            } else {
                let schema =
                    Schema::try_merge(plans.iter().map(|x| x.schema().as_arrow().clone()))?;
                LogicalPlan::EmptyRelation(EmptyRelation {
                    produce_one_row: false,
                    schema: Arc::new(DFSchema::try_from(schema)?),
                })
            };
            resolved.insert(output.name.clone(), plan);
            order.push(output.name.clone());
        }
        if before == order.len() {
            // Even partial resolution records the blocked dependency for every
            // pending output. Grust returns the closed cycle, without naming
            // unrelated consumers as members of it.
            super::topology::order(&dependencies)?;
            let pending = graph
                .outputs
                .iter()
                .filter(|x| !resolved.contains_key(&x.name))
                .map(|x| quoted(&x.name))
                .collect::<Vec<_>>()
                .join(", ");
            return Err(analysis("PIPELINE_CIRCULAR_DEPENDENCY", pending));
        }
    }
    Ok(AnalyzedGraph {
        order: super::topology::order(&dependencies)?,
        dependencies,
    })
}
