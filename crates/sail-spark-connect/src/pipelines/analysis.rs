use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};

use datafusion::arrow::datatypes::SchemaRef;
use datafusion::common::DFSchema;
use datafusion::logical_expr::{EmptyRelation, LogicalPlan};
use datafusion::prelude::SessionContext;
use sail_catalog::provider::CreateViewOptions;
use sail_common_datafusion::extension::SessionExtensionAccessor;
use sail_common_datafusion::rename::logical_plan::rename_logical_plan;
use sail_plan::error::{PlanError, PlanResult};
use sail_plan::resolver::scope::RelationScope;

use super::graph::{Flow, GraphDefinition, Name, analysis, quoted};
use super::schema::VirtualTableSchema;
use crate::config::{EncoderSchemaSettings, INCOMPATIBLE_VIEW_CHECK};
use crate::error::{SparkError, SparkResult, StructuredSparkError};
use crate::session::SparkSession;
use crate::spark::connect::OutputType;
use crate::spark::connect::pipeline_command::define_output;

/// All declared outputs are known before analysis starts. An unresolved local
/// name must never fall through to a stale table from a previous pipeline run.
pub(super) struct GraphScope<'a> {
    pub graph: &'a GraphDefinition,
    pub resolved: &'a BTreeMap<Name, LogicalPlan>,
    pub reads: Mutex<BTreeSet<Name>>,
    read_with_options: Mutex<BTreeSet<Name>>,
    pub flow: &'a Flow,
    virtual_tables: Option<VirtualInputs<'a>>,
    // Whether any input of this call is streaming as analyzed, which a STREAM
    // flag alone does not decide for a graph view.
    streaming: AtomicBool,
}

/// Provider-free materialized inputs of one flow call. `settings` is that call's
/// effective configuration snapshot; the shared session is never mutated for it.
struct VirtualInputs<'a> {
    ctx: &'a SessionContext,
    tables: &'a BTreeMap<Name, VirtualTableSchema>,
    settings: EncoderSchemaSettings,
    views: &'a BTreeMap<Name, bool>,
    // Raw so an invalid value fails the flow reading a view, not the whole graph.
    view_check: Option<String>,
}

impl VirtualInputs<'_> {
    /// Spark rejects a read whose mode differs from the view's analyzed mode.
    fn check_view_read(&self, name: &Name, is_streaming: bool, streaming: bool) -> PlanResult<()> {
        if is_streaming == streaming || !self.view_check_enabled()? {
            return Ok(());
        }
        let identifier = quoted(name);
        let (condition, kind, reader) = if is_streaming {
            ("INCOMPATIBLE_BATCH_VIEW_READ", "batch", "read")
        } else {
            (
                "INCOMPATIBLE_STREAMING_VIEW_READ",
                "streaming",
                "readStream",
            )
        };
        Err(PlanError::Structured(Box::new(StructuredSparkError {
            class: "org.apache.spark.sql.AnalysisException".into(),
            condition: condition.into(),
            sql_state: "42000".into(),
            message: format!(
                "View {identifier} is a {kind} view and must be referenced using SparkSession#{reader}. This check can be disabled by setting Spark conf {INCOMPATIBLE_VIEW_CHECK} = false."
            ),
            parameters: HashMap::from([("datasetIdentifier".into(), identifier)]),
        })))
    }

    // Scala's String.toBoolean: case-insensitive, untrimmed, with its own error text.
    fn view_check_enabled(&self) -> PlanResult<bool> {
        match self.view_check.as_deref() {
            None => Ok(true),
            Some(value) if value.eq_ignore_ascii_case("true") => Ok(true),
            Some(value) if value.eq_ignore_ascii_case("false") => Ok(false),
            Some(value) => Err(PlanError::InvalidArgument(format!(
                "For input string: \"{value}\""
            ))),
        }
    }
}

impl<'a> GraphScope<'a> {
    /// A scope over `resolved` relations for `flow`, recording nothing yet.
    pub fn new(
        graph: &'a GraphDefinition,
        resolved: &'a BTreeMap<Name, LogicalPlan>,
        flow: &'a Flow,
    ) -> Self {
        Self {
            graph,
            resolved,
            reads: Mutex::default(),
            read_with_options: Mutex::default(),
            flow,
            virtual_tables: None,
            streaming: AtomicBool::new(false),
        }
    }
    pub fn with_virtual_tables(
        mut self,
        ctx: &'a SessionContext,
        tables: &'a BTreeMap<Name, VirtualTableSchema>,
        settings: EncoderSchemaSettings,
        views: &'a BTreeMap<Name, bool>,
        view_check: Option<String>,
    ) -> Self {
        self.virtual_tables = Some(VirtualInputs {
            ctx,
            tables,
            settings,
            views,
            view_check,
        });
        self
    }

    /// Whether this call read any graph or external input as a stream.
    pub fn reads_streaming_input(&self) -> bool {
        self.streaming.load(Ordering::Relaxed)
    }

    fn record_streaming(&self, streaming: bool) {
        self.streaming.fetch_or(streaming, Ordering::Relaxed);
    }
    pub fn input_name(&self, name: &[String]) -> PlanResult<Name> {
        let raw: Name = name.iter().map(|x| x.to_ascii_lowercase()).collect();
        if self.graph.outputs.iter().any(|x| x.name == raw) {
            Ok(raw)
        } else {
            self.qualify(&raw)
        }
    }

    fn has_read_options(&self, name: &Name) -> PlanResult<bool> {
        Ok(self
            .read_with_options
            .lock()
            .map_err(|e| PlanError::internal(e.to_string()))?
            .contains(name))
    }

    fn read(&self, name: &[String], is_streaming: bool) -> PlanResult<Option<LogicalPlan>> {
        let name = self.input_name(name)?;
        if self.graph.outputs.iter().any(|x| x.name == name) {
            self.reads
                .lock()
                .map_err(|e| PlanError::internal(e.to_string()))?
                .insert(name.clone());
            if let Some(inputs) = &self.virtual_tables
                && let Some(table) = inputs.tables.get(&name)
            {
                self.record_streaming(is_streaming);
                let schema = if is_streaming {
                    table.load_streaming(inputs.ctx, inputs.settings)
                } else {
                    table.load(inputs.ctx)
                };
                let schema = schema.map_err(schema_plan_error)?;
                return Ok(Some(LogicalPlan::EmptyRelation(EmptyRelation {
                    produce_one_row: false,
                    schema: Arc::new(DFSchema::try_from(schema)?),
                })));
            }
            if let Some(inputs) = &self.virtual_tables
                && let Some(&streaming) = inputs.views.get(&name)
            {
                inputs.check_view_read(&name, is_streaming, streaming)?;
                self.record_streaming(streaming);
            }
            // Options apply to the table's files, so a read with options goes to
            // the table itself. Before a run there is only its schema.
            if !is_streaming
                && self.virtual_tables.is_none()
                && self.has_read_options(&name)?
                && self.graph.outputs.iter().any(|output| {
                    output.name == name
                        && output.view.is_none()
                        && output.kind != OutputType::TemporaryView
                })
            {
                return Ok(None);
            }
            self.resolved
                .get(&name)
                .cloned()
                .map(Some)
                .ok_or(PlanError::UnresolvedRelation(name))
        } else {
            self.record_streaming(is_streaming);
            Ok(None)
        }
    }
}

impl RelationScope for GraphScope<'_> {
    fn resolve(&self, name: &[String]) -> PlanResult<Option<LogicalPlan>> {
        self.read(name, false)
    }

    fn resolve_read(&self, name: &[String], is_streaming: bool) -> PlanResult<Option<LogicalPlan>> {
        self.read(name, is_streaming)
    }

    fn observe_source_read(&self, is_streaming: bool) {
        self.record_streaming(is_streaming);
    }

    fn observe_read_options(&self, name: &[String], options: &[(String, String)]) {
        if let (false, Ok(name), Ok(mut recorded)) = (
            options.is_empty(),
            self.input_name(name),
            self.read_with_options.lock(),
        ) {
            recorded.insert(name);
        }
    }

    // Computed IDENTIFIER targets materialize after Spark's graph substitution.
    // Their expression subqueries still use this scope; the eventual target does not.
    fn resolve_late_bound(&self, _: &[String]) -> PlanResult<Option<LogicalPlan>> {
        Ok(None)
    }

    fn resolve_late_bound_read(
        &self,
        name: &[String],
        is_streaming: bool,
    ) -> PlanResult<Option<LogicalPlan>> {
        self.record_streaming(is_streaming);
        self.resolve_late_bound(name)
    }

    fn qualify_late_bound(&self, name: &[String]) -> PlanResult<Name> {
        // Late analysis uses the ordinary session catalog namespace, which may
        // differ from the graph's captured namespace. Do not mutate either one.
        Ok(name.to_vec())
    }

    fn qualify(&self, name: &[String]) -> PlanResult<Name> {
        match name {
            [table] if !self.flow.database.is_empty() => Ok(vec![
                self.flow.catalog.clone(),
                self.flow.database.clone(),
                table.clone(),
            ]),
            [database, table] => Ok(vec![
                self.flow.catalog.clone(),
                database.clone(),
                table.clone(),
            ]),
            [_, _, _] => Ok(name.to_vec()),
            _ => Err(PlanError::analysis(format!(
                "cannot qualify pipeline relation {}",
                quoted(name)
            ))),
        }
    }

    fn default_namespace(&self) -> Option<Vec<String>> {
        Some(vec![self.flow.catalog.clone(), self.flow.database.clone()])
    }
}

// Keep native diagnostic types/cause chains across the scope's PlanResult boundary.
fn schema_plan_error(error: SparkError) -> PlanError {
    match error {
        SparkError::Structured(error) => PlanError::Structured(error),
        SparkError::DataFusionError(error) => PlanError::DataFusionError(error),
        SparkError::ArrowError(error) => PlanError::ArrowError(error),
        SparkError::AnalysisError(message) => PlanError::AnalysisError(message),
        SparkError::InvalidArgument(message) => PlanError::InvalidArgument(message),
        SparkError::MissingArgument(message) => PlanError::MissingArgument(message),
        SparkError::NotImplemented(message) => PlanError::NotImplemented(message),
        SparkError::NotSupported(message) => PlanError::NotSupported(message),
        SparkError::InternalError(message) => PlanError::InternalError(message),
        other => PlanError::DataFusionError(datafusion::common::DataFusionError::External(
            Box::new(other),
        )),
    }
}

pub(super) struct AnalyzedGraph {
    pub order: Vec<Name>,
    pub dependencies: BTreeMap<Name, BTreeSet<Name>>,
    pub schemas: BTreeMap<Name, SchemaRef>,
    pub publications: BTreeMap<Name, CreateViewOptions>,
    pub accepted_confs: BTreeMap<Name, HashMap<String, String>>,
    /// A column a target's query produces twice. Spark analyzes such a query,
    /// and its flow fails when it runs.
    pub duplicate_columns: BTreeMap<Name, String>,
}

/// Marks a repeated column, which only analysis sees.
pub(super) const DUPLICATE_COLUMN_MARK: &str = "__sail_duplicate_column_";

/// Names a query's repeated columns apart, so its plan has a schema.
fn disambiguated(fields: Vec<String>, duplicate: &mut Option<String>) -> Vec<String> {
    let mut seen = BTreeSet::new();
    fields
        .into_iter()
        .enumerate()
        .map(|(index, name)| {
            if seen.insert(name.to_ascii_lowercase()) {
                return name;
            }
            let renamed = format!("{DUPLICATE_COLUMN_MARK}{index}");
            duplicate.get_or_insert(name);
            renamed
        })
        .collect()
}

mod failure;
pub(super) use failure::{AnalysisError, ResolutionFailures};

pub(super) async fn analyze(
    ctx: &SessionContext,
    graph: &GraphDefinition,
) -> Result<AnalyzedGraph, AnalysisError> {
    analyze_queries(ctx, graph)
        .await?
        .map_err(AnalysisError::from)
}

async fn analyze_queries(
    ctx: &SessionContext,
    graph: &GraphDefinition,
) -> SparkResult<Result<AnalyzedGraph, ResolutionFailures>> {
    graph.validate_bindings()?;
    let spark = ctx.extension::<SparkSession>()?;
    let mut resolved = BTreeMap::new();
    let mut virtual_tables = BTreeMap::new();
    let mut order = vec![];
    let mut flow_plans: BTreeMap<Name, LogicalPlan> = BTreeMap::new();
    let mut flow_streaming: BTreeMap<Name, bool> = BTreeMap::new();
    let mut view_streaming: BTreeMap<Name, bool> = BTreeMap::new();
    let mut flow_reads: BTreeMap<Name, BTreeSet<Name>> = BTreeMap::new();
    let mut failures: BTreeMap<Name, super::config::resolve::FailedFlow> = BTreeMap::new();
    let mut pending_flows = BTreeMap::new();
    let mut dependencies = BTreeMap::new();
    let mut publications = BTreeMap::new();
    let mut accepted_confs = BTreeMap::new();
    let mut duplicate_columns = BTreeMap::new();
    let base = HashMap::new();
    let source_case_sensitive = spark.plan_config()?.case_sensitive;
    while order.len() < graph.outputs.len() {
        let before = order.len();
        for output in &graph.outputs {
            if resolved.contains_key(&output.name) || virtual_tables.contains_key(&output.name) {
                continue;
            }
            let mut inputs = BTreeSet::new();
            let mut plans = vec![];
            let mut pending = false;
            for flow in graph.flows.iter().filter(|x| x.target == output.name) {
                if let Some(plan) = flow_plans.get(&flow.name) {
                    inputs.extend(flow_reads[&flow.name].iter().cloned());
                    plans.push(plan.clone());
                    continue;
                }
                if let Some(failure) = failures.get(&flow.name) {
                    inputs.extend(failure.reads.iter().cloned());
                    pending = true;
                    continue;
                }
                let context = super::config::resolve::FlowContext {
                    ctx,
                    graph,
                    resolved: &resolved,
                    virtual_tables: Some(&virtual_tables),
                    view_streaming: &view_streaming,
                    accepted: &accepted_confs,
                    base: &base,
                    source_case_sensitive,
                };
                match context.resolve(flow, output.view.as_ref()).await? {
                    super::config::resolve::FlowResolution::Resolved(result) => {
                        inputs.extend(result.reads.iter().cloned());
                        flow_reads.insert(flow.name.clone(), result.reads);
                        flow_streaming.insert(flow.name.clone(), result.streaming);
                        pending_flows.remove(&flow.name);
                        accepted_confs.insert(flow.name.clone(), result.accepted);
                        if let Some(options) = result.publication {
                            publications.insert(output.name.clone(), options);
                        }
                        let plan = result.plan;
                        let plan = if let Some(fields) = plan.fields {
                            let is_view =
                                output.kind == OutputType::TemporaryView || output.view.is_some();
                            let mut duplicate = None;
                            let fields = if is_view {
                                fields
                            } else {
                                disambiguated(fields, &mut duplicate)
                            };
                            if let Some(duplicate) = duplicate {
                                duplicate_columns.insert(output.name.clone(), duplicate);
                            }
                            rename_logical_plan(plan.plan, &fields)?
                        } else {
                            plan.plan
                        };
                        flow_plans.insert(flow.name.clone(), plan.clone());
                        plans.push(plan);
                    }
                    super::config::resolve::FlowResolution::Pending(waiting) => {
                        inputs.extend(waiting.reads.iter().cloned());
                        pending_flows.insert(flow.name.clone(), waiting);
                        pending = true;
                    }
                    super::config::resolve::FlowResolution::Failed(failure) => {
                        inputs.extend(failure.reads.iter().cloned());
                        pending_flows.remove(&flow.name);
                        failures.insert(flow.name.clone(), failure);
                        pending = true;
                    }
                }
            }
            dependencies.insert(output.name.clone(), inputs);
            if pending {
                continue;
            }
            if (output.kind == OutputType::TemporaryView || output.view.is_some())
                && !plans.is_empty()
            {
                if plans.len() != 1 {
                    return Err(analysis("MULTIPLE_QUERIES_FOR_VIEW", quoted(&output.name)));
                }
                resolved.insert(output.name.clone(), plans.remove(0));
                let streaming = graph
                    .flows
                    .iter()
                    .filter(|flow| flow.target == output.name)
                    .any(|flow| flow_streaming.get(&flow.name).copied().unwrap_or(false));
                view_streaming.insert(output.name.clone(), streaming);
            } else {
                // Construct the virtual input, not its schema. An unread output
                // must not fail inference ahead of another unresolved query.
                let schemas =
                    super::autocdc::flow_schemas(ctx, graph, &output.name, &plans).await?;
                virtual_tables.insert(
                    output.name.clone(),
                    VirtualTableSchema::new(output, schemas).with_streaming_flow(
                        graph.flows.iter().any(|flow| {
                            flow.target == output.name
                                && flow_streaming.get(&flow.name).copied().unwrap_or(false)
                        }),
                    ),
                );
            }
            order.push(output.name.clone());
            // Only unfinished multi-flow outputs need cached working plans.
            // Materialized inputs keep schemas rather than source providers.
            for flow in graph.flows.iter().filter(|flow| flow.target == output.name) {
                flow_plans.remove(&flow.name);
                flow_reads.remove(&flow.name);
            }
        }
        if before == order.len() {
            for (name, waiting) in pending_flows {
                failures.insert(name, super::config::resolve::FailedFlow {
                    error: crate::error::SparkError::AnalysisError(format!(
                        "Failed to read dataset '{}'. Dataset is defined in the pipeline but could not be resolved.",
                        waiting.input.join(".")
                    )),
                    reads: waiting.reads,
                });
            }
            return Ok(Err(ResolutionFailures::new(graph, failures)?));
        }
    }
    // Spark validates declarations for table identifiers represented by a
    // same-named flow, after every query has resolved successfully.
    for flow in &graph.flows {
        if let Some(table) = virtual_tables.get(&flow.name) {
            table.validate(ctx)?;
        }
    }
    // Persisted-view source restrictions are graph validation, after successful
    // query resolution (GraphValidations.validateSuccessfulFlowAnalysis).
    for output in &graph.outputs {
        if output.view.is_some()
            && let Some(temporary) = graph.outputs.iter().find(|candidate| {
                candidate.kind == OutputType::TemporaryView
                    && dependencies[&output.name].contains(&candidate.name)
            })
        {
            return Err(Box::new(StructuredSparkError {
                class: "org.apache.spark.sql.AnalysisException".into(),
                condition: "INVALID_TEMP_OBJ_REFERENCE".into(),
                sql_state: "42K0F".into(),
                message: format!(
                    "view {} references temporary view {}",
                    quoted(&output.name),
                    quoted(&temporary.name)
                ),
                parameters: HashMap::from([
                    ("obj".into(), "view".into()),
                    ("objName".into(), quoted(&output.name)),
                    ("tempObj".into(), "temporary view".into()),
                    ("tempObjName".into(), quoted(&temporary.name)),
                ]),
            })
            .into());
        }
    }
    // Spark resolves the graph before validating its semantics. In particular,
    // an unresolved source must not be hidden by flow count or streamingness.
    graph.validate(&flow_streaming)?;
    for output in &graph.outputs {
        if let Some(define_output::Details::TableDetails(details)) = &output.definition.details {
            super::properties::validate_reset(
                details.table_properties.get("pipelines.reset.allowed"),
            )?;
        }
    }
    let mut schemas: BTreeMap<_, _> = resolved
        .into_iter()
        .map(|(name, plan)| (name, Arc::new(plan.schema().as_arrow().clone())))
        .collect();
    for (name, table) in virtual_tables {
        schemas.insert(name, Arc::new(table.final_schema(ctx)?));
    }
    Ok(Ok(AnalyzedGraph {
        order: super::topology::order(
            &dependencies,
            &graph
                .flows
                .iter()
                .map(|flow| (flow.name.clone(), flow.target.clone()))
                .collect(),
        )?,
        dependencies,
        schemas,
        publications,
        accepted_confs,
        duplicate_columns,
    }))
}

#[cfg(test)]
#[path = "analysis_artifact_tests.rs"]
mod artifact_tests;
