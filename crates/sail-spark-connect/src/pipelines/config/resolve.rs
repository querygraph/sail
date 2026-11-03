//! The two analysis calls of a flow, with success-only configuration validation.
use std::collections::{BTreeMap, BTreeSet, HashMap};

use datafusion::common::tree_node::{TreeNode, TreeNodeRecursion};
use datafusion::logical_expr::LogicalPlan;
use datafusion::logical_expr::type_coercion::binary::comparison_coercion;
use datafusion::prelude::SessionContext;
use sail_catalog::provider::CreateViewOptions;
use sail_common::spec;
use sail_common_datafusion::datasource::DataSourceRegistry;
use sail_common_datafusion::extension::SessionExtensionAccessor;
use sail_plan::error::{PlanError, PlanResult};
use sail_plan::resolver::PlanResolver;
use sail_plan::resolver::plan::NamedPlan;

use super::merge::{ConfMap, ViewContribution, first_pass_map, merge};
use crate::config::INCOMPATIBLE_VIEW_CHECK;
use crate::error::{SparkError, SparkResult, StructuredSparkError};
use crate::pipelines::analysis::GraphScope;
use crate::pipelines::graph::{Flow, GraphDefinition, Name};
use crate::service::{contains_relation_extension, reads_streaming_source};
use crate::session::SparkSession;
use crate::spark::connect::OutputType;

pub(in crate::pipelines) struct ResolvedFlow {
    pub plan: NamedPlan,
    pub publication: Option<CreateViewOptions>,
    pub accepted: ConfMap,
    pub reads: BTreeSet<Name>,
    pub streaming: bool,
}
pub(in crate::pipelines) struct PendingFlow {
    pub input: Name,
    pub reads: BTreeSet<Name>,
}
pub(in crate::pipelines) struct FailedFlow {
    pub error: SparkError,
    pub reads: BTreeSet<Name>,
}
pub(in crate::pipelines) enum FlowResolution {
    Resolved(Box<ResolvedFlow>),
    Pending(PendingFlow),
    Failed(FailedFlow),
}
enum CallResult {
    Resolved(Box<(NamedPlan, Option<CreateViewOptions>)>),
    Pending(Name),
    Failed(SparkError),
}
struct Call {
    result: CallResult,
    reads: BTreeSet<Name>,
    streaming: bool,
}
struct ResolvedCall {
    plan: NamedPlan,
    publication: Option<CreateViewOptions>,
    reads: BTreeSet<Name>,
    streaming: bool,
}
impl Call {
    fn into_plan(self) -> Result<ResolvedCall, FlowResolution> {
        match self.result {
            CallResult::Resolved(plan) => {
                let (plan, publication) = *plan;
                Ok(ResolvedCall {
                    plan,
                    publication,
                    reads: self.reads,
                    streaming: self.streaming,
                })
            }
            CallResult::Pending(input) => Err(FlowResolution::Pending(PendingFlow {
                input,
                reads: self.reads,
            })),
            CallResult::Failed(error) => Err(FlowResolution::Failed(FailedFlow {
                error,
                reads: self.reads,
            })),
        }
    }
}

fn contributions<'a>(
    graph: &GraphDefinition,
    accepted: &'a BTreeMap<Name, ConfMap>,
    names: impl IntoIterator<Item = &'a Name>,
) -> PlanResult<Option<Vec<(String, &'a ConfMap)>>> {
    let mut views = vec![];
    for name in names {
        let output = graph
            .outputs
            .iter()
            .find(|output| &output.name == name)
            .ok_or_else(|| PlanError::internal("graph input output missing"))?;
        if output.kind != OutputType::TemporaryView && output.view.is_none() {
            continue;
        }
        let flow = graph
            .flows
            .iter()
            .find(|flow| &flow.target == name)
            .ok_or_else(|| PlanError::internal("graph view flow missing"))?;
        let Some(conf) = accepted.get(&flow.name) else {
            return Ok(None);
        };
        views.push((flow.name.join("."), conf));
    }
    Ok(Some(views))
}
fn overlay(base: &ConfMap, overrides: &ConfMap) -> ConfMap {
    let mut values = base.clone();
    values.extend(overrides.clone());
    values
}

pub(in crate::pipelines) struct FlowContext<'a> {
    pub ctx: &'a SessionContext,
    pub graph: &'a GraphDefinition,
    pub resolved: &'a BTreeMap<Name, LogicalPlan>,
    pub virtual_tables: Option<&'a BTreeMap<Name, crate::pipelines::schema::VirtualTableSchema>>,
    pub view_streaming: &'a BTreeMap<Name, bool>,
    pub accepted: &'a BTreeMap<Name, ConfMap>,
    pub base: &'a ConfMap,
    pub source_case_sensitive: bool,
}
impl FlowContext<'_> {
    fn unavailable_input<'a>(
        &self,
        reads: impl IntoIterator<Item = &'a Name>,
    ) -> SparkResult<Name> {
        reads
            .into_iter()
            .find(|name| {
                !self.resolved.contains_key(*name)
                    && !self
                        .virtual_tables
                        .is_some_and(|tables| tables.contains_key(*name))
            })
            .cloned()
            .ok_or_else(|| SparkError::internal("pending flow has no unavailable input"))
    }
    async fn call(
        &self,
        flow: &Flow,
        view: Option<&spec::ViewDefinition>,
        conf: &ConfMap,
    ) -> SparkResult<Call> {
        let spark = self.ctx.extension::<SparkSession>()?;
        let initial = spark.plan_config_with_overrides(overlay(self.base, conf))?;
        let registry = self.ctx.extension::<DataSourceRegistry>().ok();
        // With no declared view settings anywhere in the graph, every accepted
        // view map is empty as well. Native resolution alone then gives the exact
        // inputs; there is no inherited setting to collect before resolving SQL.
        // Keep native query forms available even when the configuration trace
        // does not yet support them. Table settings do not cross this boundary.
        let has_view_settings = self.graph.flows.iter().any(|flow| {
            !flow.sql_conf.is_empty()
                && self.graph.outputs.iter().any(|output| {
                    output.name == flow.target
                        && (output.kind == OutputType::TemporaryView || output.view.is_some())
                })
        });
        let ordered = if has_view_settings {
            super::trace::ordered_inputs(
                self.graph,
                flow,
                &flow.query,
                initial.case_sensitive,
                |format| {
                    registry
                        .as_ref()
                        .is_some_and(|registry| registry.get_data_source(format).is_ok())
                },
            )?
        } else {
            vec![]
        };
        let mut reads = ordered.iter().cloned().collect::<BTreeSet<_>>();
        let Some(views) = contributions(self.graph, self.accepted, ordered.iter())? else {
            return Ok(Call {
                result: CallResult::Pending(self.unavailable_input(ordered.iter())?),
                reads,
                streaming: false,
            });
        };
        let semantic = first_pass_map(
            conf,
            views.iter().map(|(name, conf)| ViewContribution {
                flow_name: name,
                accepted: conf,
            }),
        );
        // Both come from one snapshot of this call's effective settings, so a
        // second call under the merged map sees the view contributions as well.
        let (config, settings) = spark.flow_call_config(overlay(self.base, &semantic))?;
        let scope = GraphScope::new(self.graph, self.resolved, flow);
        let scope = match self.virtual_tables {
            Some(tables) => {
                // Spark reads this from the conf in force at the read, before the
                // target view's own settings fuse. The merged map reaches it only
                // on a second call, which a failed check here never starts.
                let view_check = match conf.get(INCOMPATIBLE_VIEW_CHECK) {
                    Some(value) => Some(value.clone()),
                    None => match self.base.get(INCOMPATIBLE_VIEW_CHECK) {
                        Some(value) => Some(value.clone()),
                        None => spark.incompatible_view_check()?,
                    },
                };
                scope.with_virtual_tables(
                    self.ctx,
                    tables,
                    settings,
                    self.view_streaming,
                    view_check,
                )
            }
            None => scope,
        };
        let resolver = PlanResolver::new(self.ctx, config)
            .with_relation_scope(&scope)
            .with_source_read_case_sensitive(self.source_case_sensitive);
        let result = match view {
            Some(view) => resolver
                .resolve_view_publication(view.clone())
                .await
                .map(|(plan, options)| (plan, Some(options))),
            None => resolver
                .resolve_named_plan(spec::Plan::Query(flow.query.clone()))
                .await
                .map(|plan| (plan, None)),
        };
        let result = result.and_then(|(plan, options)| {
            check_unions(&plan.plan)?;
            Ok((plan, options))
        });
        let streaming = scope.reads_streaming_input() || reads_streaming_source(&flow.query);
        let actual = scope.reads.into_inner()?;
        // A native region is a batch source. A streaming flow would plan the
        // extension again for every batch, which is not a stream of it; the
        // flow is refused by name here, at analysis, before any batch runs.
        let result = if streaming && contains_relation_extension(&flow.query) {
            Err(PlanError::unsupported(
                "relation extension cannot be read by a streaming flow; a native region is a batch source",
            ))
        } else {
            result
        };
        match result {
            Ok(plan) => Ok(Call {
                result: CallResult::Resolved(Box::new(plan)),
                reads: actual,
                streaming,
            }),
            Err(PlanError::UnresolvedRelation(input)) => {
                reads.extend(actual);
                Ok(Call {
                    result: CallResult::Pending(input),
                    reads,
                    streaming,
                })
            }
            Err(error) => {
                reads.extend(actual);
                Ok(Call {
                    result: CallResult::Failed(error.into()),
                    reads,
                    streaming,
                })
            }
        }
    }

    /// `base` is the run-session setting overlay (empty at initial graph analysis,
    /// accepted command settings at command reanalysis). It is not a declared-flow
    /// contribution to conflict checking. Provider metadata has its own case flag.
    pub(in crate::pipelines) async fn resolve(
        &self,
        flow: &Flow,
        view: Option<&spec::ViewDefinition>,
    ) -> SparkResult<FlowResolution> {
        let first = match self.call(flow, view, &flow.sql_conf).await?.into_plan() {
            Ok(plan) => plan,
            Err(status) => return Ok(status),
        };
        let Some(views) = contributions(self.graph, self.accepted, first.reads.iter())? else {
            return Ok(FlowResolution::Pending(PendingFlow {
                input: self.unavailable_input(&first.reads)?,
                reads: first.reads,
            }));
        };
        let display = flow.name.join(".");
        let merged = merge(
            &display,
            &display,
            &flow.sql_conf,
            views.iter().map(|(name, conf)| ViewContribution {
                flow_name: name,
                accepted: conf,
            }),
        )
        .map_err(|error| {
            Box::new(StructuredSparkError {
                class: "org.apache.spark.sql.AnalysisException".into(),
                condition: "DUPLICATE_FLOW_SQL_CONF".into(),
                sql_state: "42710".into(),
                message: error.to_string(),
                parameters: HashMap::from([
                    ("key".into(), error.key),
                    ("datasetName".into(), error.dataset_name),
                    ("flowName1".into(), error.flow_name1),
                    ("flowName2".into(), error.flow_name2),
                ]),
            })
        })?;
        let (plan, publication, reads, streaming) = if merged.changed {
            // The second call's raw configuration is the accepted union. Spark does
            // not run the merge a third time if that call binds a different input.
            let second = match self.call(flow, view, &merged.map).await?.into_plan() {
                Ok(plan) => plan,
                Err(status) => return Ok(status),
            };
            (
                second.plan,
                second.publication,
                second.reads,
                second.streaming,
            )
        } else {
            (first.plan, first.publication, first.reads, first.streaming)
        };
        Ok(FlowResolution::Resolved(Box::new(ResolvedFlow {
            plan,
            publication,
            reads,
            streaming,
            accepted: merged.map,
        })))
    }
}

const ORDINALS: [&str; 9] = [
    "first", "second", "third", "fourth", "fifth", "sixth", "seventh", "eighth", "ninth",
];

fn ordinal(index: usize) -> String {
    ORDINALS
        .get(index)
        .map_or_else(|| format!("{}th", index + 1), |name| (*name).to_string())
}

/// Report a union's incompatible columns during analysis, as Spark does. The
/// planner refuses them too, but only a real run reaches it.
fn check_unions(plan: &LogicalPlan) -> Result<(), PlanError> {
    let mut failure = None;
    plan.apply(|node| {
        let LogicalPlan::Union(union) = node else {
            return Ok(TreeNodeRecursion::Continue);
        };
        let Some(first) = union.inputs.first() else {
            return Ok(TreeNodeRecursion::Continue);
        };
        for (table, input) in union.inputs.iter().enumerate().skip(1) {
            let fields = first.schema().fields().iter().zip(input.schema().fields());
            for (column, (left, right)) in fields.enumerate() {
                if comparison_coercion(left.data_type(), right.data_type()).is_none() {
                    failure = Some(PlanError::invalid(format!(
                        "[INCOMPATIBLE_COLUMN_TYPE] UNION can only be performed on tables with \
                         compatible column types. The {} column of the {} table is {} type which \
                         is not compatible with {} at the same column of the first table.",
                        ordinal(column),
                        ordinal(table),
                        right.data_type(),
                        left.data_type(),
                    )));
                    return Ok(TreeNodeRecursion::Stop);
                }
            }
        }
        Ok(TreeNodeRecursion::Continue)
    })?;
    failure.map_or(Ok(()), Err)
}
