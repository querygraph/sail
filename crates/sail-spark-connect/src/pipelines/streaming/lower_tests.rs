//! Lowering over the deterministic source: the bound plan executes exactly
//! the intent's ranges, evidence and snapshot are recorded, refusals and
//! rejections surface with their causes, and nothing outside the bindings
//! is reached.

use std::collections::{BTreeSet, HashMap};

use datafusion::arrow::array::AsArray;
use datafusion::arrow::datatypes::{DataType, Field, Int64Type, Schema, SchemaRef};
use datafusion::common::{DFSchema, ScalarValue, TableReference};
use datafusion::datasource::{MemTable, TableProvider, provider_as_source};
use datafusion::logical_expr::{EmptyRelation, LogicalPlanBuilder};
use tokio::runtime::Handle;

use super::super::binding::LocalBinding;
use super::super::intent::{Progress, RunFrontier, next_intent};
use super::super::source::BoundedSource;
use super::super::test_source::DeterministicSource;
use super::*;
use crate::pipelines::graph::Output;
use crate::spark::connect::OutputType;
use crate::spark::connect::pipeline_command::DefineOutput;

type TestResult = Result<(), Box<dyn std::error::Error>>;

fn name(parts: &[&str]) -> Name {
    parts.iter().map(|part| part.to_string()).collect()
}

fn schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("v", DataType::Int64, true),
    ]))
}

fn read(table: &str, streaming: bool) -> spec::QueryPlan {
    spec::QueryPlan::new(spec::QueryNode::Read {
        read_type: spec::ReadType::NamedTable(Box::new(spec::ReadNamedTable {
            name: spec::ObjectName::bare(table),
            temporal: None,
            sample: None,
            options: vec![],
        })),
        is_streaming: streaming,
    })
}

fn join(left: spec::QueryPlan, right: spec::QueryPlan) -> spec::QueryPlan {
    spec::QueryPlan::new(spec::QueryNode::Join(spec::Join {
        left: Box::new(left),
        right: Box::new(right),
        join_type: spec::JoinType::Inner,
        join_criteria: Some(spec::JoinCriteria::Using(vec!["id".into()])),
        join_data_type: None,
    }))
}

fn graph() -> GraphDefinition {
    let output = |parts: &[&str]| Output {
        name: name(parts),
        kind: OutputType::Table,
        definition: DefineOutput::default(),
        schema: None,
        view: None,
    };
    GraphDefinition {
        catalog: "c".into(),
        database: "d".into(),
        sql_conf: HashMap::new(),
        outputs: vec![output(&["c", "d", "events"]), output(&["c", "d", "target"])],
        flows: vec![],
    }
}

fn flow(query: spec::QueryPlan, sql_conf: &[(&str, &str)]) -> Flow {
    Flow {
        name: name(&["c", "d", "target"]),
        target: name(&["c", "d", "target"]),
        query,
        sql_conf: sql_conf
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect(),
        catalog: "c".into(),
        database: "d".into(),
        auto_cdc: None,
    }
}

/// The analysis placeholder of the local output `c.d.events`: an empty
/// relation, as `analysis::analyze` installs it. Lowering must not accept
/// it in place of rows.
fn placeholder_relations() -> Result<BTreeMap<Name, LogicalPlan>, Box<dyn std::error::Error>> {
    let relation = LogicalPlan::EmptyRelation(EmptyRelation {
        produce_one_row: false,
        schema: Arc::new(DFSchema::try_from(schema().as_ref().clone())?),
    });
    Ok(BTreeMap::from([(name(&["c", "d", "events"]), relation)]))
}

/// The materialised local output `c.d.events` as execution installs it: a
/// scan of its provider, with the matching local binding.
fn materialised_events()
-> Result<(BTreeMap<Name, LogicalPlan>, LocalBinding), Box<dyn std::error::Error>> {
    let provider: Arc<dyn TableProvider> = Arc::new(MemTable::try_new(schema(), vec![vec![]])?);
    let plan = LogicalPlanBuilder::scan(
        TableReference::full("c", "d", "events"),
        provider_as_source(provider.clone()),
        None,
    )?
    .build()?;
    Ok((
        BTreeMap::from([(name(&["c", "d", "events"]), plan)]),
        LocalBinding {
            name: name(&["c", "d", "events"]),
            identity: "run:1".into(),
            provider,
        },
    ))
}

fn relations() -> Result<BTreeMap<Name, LogicalPlan>, Box<dyn std::error::Error>> {
    Ok(materialised_events()?.0)
}

/// A source with two partitions, the first holding rows 0..3 and the
/// second rows 10..12, bound over the first intent of a fresh run.
fn bind_clicks(
    rows_limit: usize,
) -> Result<(DeterministicSource, SourceBinding), Box<dyn std::error::Error>> {
    let source = DeterministicSource::new("clicks", schema());
    source.create_partition("p0");
    source.create_partition("p1");
    let rows = |ids: std::ops::Range<i64>| {
        ids.map(|id| {
            vec![
                ScalarValue::Int64(Some(id)),
                ScalarValue::Int64(Some(id * 10)),
            ]
        })
        .collect::<Vec<_>>()
    };
    source.append("p0", rows(0..3));
    source.append("p1", rows(10..12));
    let frontier = RunFrontier::capture(&source)?;
    let intent = next_intent(&source, &frontier, &Progress::default(), rows_limit)
        .map_err(|error| format!("{error:?}"))?
        .unwrap_or(super::super::intent::BatchIntent { ranges: vec![] });
    let scans = intent
        .ranges
        .iter()
        .map(|range| source.scan(range))
        .collect::<Result<Vec<_>, _>>()?;
    let binding = SourceBinding {
        name: name(&["c", "d", "clicks"]),
        schema: schema(),
        descriptor: source.describe(),
        intent,
        scans,
    };
    Ok((source, binding))
}

async fn ids(
    ctx: &SessionContext,
    plan: LogicalPlan,
) -> Result<Vec<i64>, Box<dyn std::error::Error>> {
    let batches = ctx.execute_logical_plan(plan).await?.collect().await?;
    let mut ids: Vec<i64> = batches
        .iter()
        .flat_map(|batch| {
            batch
                .column(0)
                .as_primitive::<Int64Type>()
                .values()
                .iter()
                .copied()
        })
        .collect();
    ids.sort_unstable();
    Ok(ids)
}

#[tokio::test]
async fn a_streaming_read_lowers_to_exactly_the_intent_ranges() -> TestResult {
    let ctx = SessionContext::new();
    let scope = TaskResourceScope::new(Handle::current());
    let (_source, binding) = bind_clicks(100)?;
    let graph = graph();
    let flow = flow(read("clicks", true), &[("spark.sql.caseSensitive", "true")]);
    let lowered = lower(
        &ctx,
        Arc::new(PlanConfig::default()),
        &scope,
        &Inputs {
            graph: &graph,
            flow: &flow,
            relations: &relations()?,
            sources: std::slice::from_ref(&binding),
            statics: &[],
            locals: &[],
        },
    )
    .await?;
    assert!(lowered.evidence.admitted, "{:?}", lowered.evidence);
    assert!(lowered.evidence.replayable);
    assert_eq!(
        lowered.evidence.sources_read,
        BTreeSet::from([name(&["c", "d", "clicks"])])
    );
    assert_eq!(ids(&ctx, lowered.plan).await?, vec![0, 1, 2, 10, 11]);
    assert_eq!(lowered.snapshot.bindings.len(), 1);
    assert_eq!(lowered.snapshot.bindings[0].ranges, binding.intent.ranges);
    assert_eq!(lowered.snapshot.bindings[0].descriptor_digest.len(), 64);
    assert_eq!(
        lowered
            .snapshot
            .sql_conf
            .get("spark.sql.caseSensitive")
            .map(String::as_str),
        Some("true")
    );
    assert_eq!(
        lowered.snapshot.case_sensitive,
        PlanConfig::default().case_sensitive
    );
    Ok(())
}

#[tokio::test]
async fn an_empty_recorded_intent_lowers_to_an_empty_relation_with_the_source_schema() -> TestResult
{
    let ctx = SessionContext::new();
    let scope = TaskResourceScope::new(Handle::current());
    let (_source, mut binding) = bind_clicks(100)?;
    binding.intent.ranges.clear();
    binding.scans.clear();
    let graph = graph();
    let flow = flow(read("clicks", true), &[]);
    let lowered = lower(
        &ctx,
        Arc::new(PlanConfig::default()),
        &scope,
        &Inputs {
            graph: &graph,
            flow: &flow,
            relations: &relations()?,
            sources: std::slice::from_ref(&binding),
            statics: &[],
            locals: &[],
        },
    )
    .await?;
    assert!(lowered.evidence.admitted);
    assert_eq!(lowered.plan.schema().fields().len(), 2);
    assert_eq!(ids(&ctx, lowered.plan).await?, Vec::<i64>::new());
    assert!(lowered.snapshot.bindings[0].ranges.is_empty());
    Ok(())
}

#[tokio::test]
async fn a_bounded_range_joined_with_a_materialised_local_output_is_admitted() -> TestResult {
    let ctx = SessionContext::new();
    let scope = TaskResourceScope::new(Handle::current());
    let (_source, binding) = bind_clicks(100)?;
    let (relations, events) = materialised_events()?;
    let graph = graph();
    let flow = flow(join(read("clicks", true), read("events", false)), &[]);
    let lowered = lower(
        &ctx,
        Arc::new(PlanConfig::default()),
        &scope,
        &Inputs {
            graph: &graph,
            flow: &flow,
            relations: &relations,
            sources: std::slice::from_ref(&binding),
            statics: &[],
            locals: std::slice::from_ref(&events),
        },
    )
    .await?;
    assert!(lowered.evidence.admitted, "{:?}", lowered.evidence);
    assert!(lowered.evidence.provenance_verified);
    assert_eq!(
        lowered.evidence.static_read,
        BTreeSet::from([name(&["c", "d", "events"])])
    );
    Ok(())
}

#[tokio::test]
async fn an_analysis_placeholder_for_a_local_output_is_not_admitted() -> TestResult {
    let ctx = SessionContext::new();
    let scope = TaskResourceScope::new(Handle::current());
    let (_source, binding) = bind_clicks(100)?;
    let graph = graph();
    let flow = flow(join(read("clicks", true), read("events", false)), &[]);
    let error = lower(
        &ctx,
        Arc::new(PlanConfig::default()),
        &scope,
        &Inputs {
            graph: &graph,
            flow: &flow,
            relations: &placeholder_relations()?,
            sources: std::slice::from_ref(&binding),
            statics: &[],
            locals: &[],
        },
    )
    .await
    .err();
    match error {
        Some(LowerError::NotAdmitted(evidence)) => {
            assert!(!evidence.provenance_verified);
            assert!(
                evidence
                    .operators
                    .iter()
                    .any(|op| op.operator == "EmptyRelation")
            );
        }
        other => return Err(format!("expected NotAdmitted, got {other:?}").into()),
    }
    // A substituted provider under the local output's name is not the bound one.
    let (relations, mut events) = materialised_events()?;
    events.provider = Arc::new(MemTable::try_new(schema(), vec![vec![]])?);
    let error = lower(
        &ctx,
        Arc::new(PlanConfig::default()),
        &scope,
        &Inputs {
            graph: &graph,
            flow: &flow,
            relations: &relations,
            sources: std::slice::from_ref(&binding),
            statics: &[],
            locals: std::slice::from_ref(&events),
        },
    )
    .await
    .err();
    assert!(
        matches!(error, Some(LowerError::NotAdmitted(_))),
        "{error:?}"
    );
    Ok(())
}

#[tokio::test]
async fn refusals_and_rejections_carry_their_causes() -> TestResult {
    let ctx = SessionContext::new();
    let scope = TaskResourceScope::new(Handle::current());
    let (_source, clicks) = bind_clicks(100)?;
    let graph = graph();
    // Unbound streaming read: refused before resolution.
    let flow_unbound = flow(read("views", true), &[]);
    let error = lower(
        &ctx,
        Arc::new(PlanConfig::default()),
        &scope,
        &Inputs {
            graph: &graph,
            flow: &flow_unbound,
            relations: &relations()?,
            sources: std::slice::from_ref(&clicks),
            statics: &[],
            locals: &[],
        },
    )
    .await
    .err();
    assert!(
        matches!(
            error,
            Some(LowerError::Binding(BindingError::UnboundStreamingRead(_)))
        ),
        "{error:?}"
    );
    // Two range-bearing sides: resolved, then rejected with evidence.
    let (_other, mut views) = bind_clicks(100)?;
    views.name = name(&["c", "d", "views"]);
    let flow_join = flow(join(read("clicks", true), read("views", true)), &[]);
    let error = lower(
        &ctx,
        Arc::new(PlanConfig::default()),
        &scope,
        &Inputs {
            graph: &graph,
            flow: &flow_join,
            relations: &relations()?,
            sources: &[clicks, views],
            statics: &[],
            locals: &[],
        },
    )
    .await
    .err();
    match error {
        Some(LowerError::NotAdmitted(evidence)) => {
            assert!(!evidence.admitted);
            assert!(
                evidence
                    .operators
                    .iter()
                    .any(|op| op.operator == "Join" && op.reason.contains("cross-batch"))
            );
        }
        other => return Err(format!("expected NotAdmitted, got {other:?}").into()),
    }
    Ok(())
}

#[tokio::test]
async fn a_static_binding_is_scanned_through_its_pinned_provider() -> TestResult {
    let ctx = SessionContext::new();
    let scope = TaskResourceScope::new(Handle::current());
    let (_source, clicks) = bind_clicks(100)?;
    let dim = StaticBinding {
        name: name(&["c", "d", "dim"]),
        identity: "version:3".into(),
        provider: Arc::new(MemTable::try_new(schema(), vec![vec![]])?),
    };
    let graph = graph();
    let flow = flow(join(read("clicks", true), read("dim", false)), &[]);
    let lowered = lower(
        &ctx,
        Arc::new(PlanConfig::default()),
        &scope,
        &Inputs {
            graph: &graph,
            flow: &flow,
            relations: &relations()?,
            sources: std::slice::from_ref(&clicks),
            statics: std::slice::from_ref(&dim),
            locals: &[],
        },
    )
    .await?;
    assert!(lowered.evidence.admitted, "{:?}", lowered.evidence);
    assert_eq!(
        lowered.evidence.static_read,
        BTreeSet::from([name(&["c", "d", "dim"])])
    );
    assert_eq!(ids(&ctx, lowered.plan).await?, Vec::<i64>::new());
    Ok(())
}

/// A graph whose output `c.d.clicks` is also the bound streaming source.
fn graph_with_clicks_output() -> GraphDefinition {
    let mut graph = graph();
    graph.outputs.push(Output {
        name: name(&["c", "d", "clicks"]),
        kind: OutputType::Table,
        definition: DefineOutput::default(),
        schema: None,
        view: None,
    });
    graph
}

#[tokio::test]
async fn a_streaming_read_of_a_local_streaming_output_uses_its_recorded_ranges() -> TestResult {
    let ctx = SessionContext::new();
    let scope = TaskResourceScope::new(Handle::current());
    let (_source, binding) = bind_clicks(100)?;
    let graph = graph_with_clicks_output();
    let flow = flow(read("clicks", true), &[]);
    // With the analysis placeholder installed for the output.
    let mut relations = placeholder_relations()?;
    relations.insert(
        name(&["c", "d", "clicks"]),
        LogicalPlan::EmptyRelation(EmptyRelation {
            produce_one_row: false,
            schema: Arc::new(DFSchema::try_from(schema().as_ref().clone())?),
        }),
    );
    let lowered = lower(
        &ctx,
        Arc::new(PlanConfig::default()),
        &scope,
        &Inputs {
            graph: &graph,
            flow: &flow,
            relations: &relations,
            sources: std::slice::from_ref(&binding),
            statics: &[],
            locals: &[],
        },
    )
    .await?;
    assert!(lowered.evidence.admitted, "{:?}", lowered.evidence);
    assert_eq!(ids(&ctx, lowered.plan).await?, vec![0, 1, 2, 10, 11]);
    // With a stale materialised relation installed for the output, holding
    // other rows under a different provider: still the recorded ranges.
    let stale_rows = datafusion::arrow::record_batch::RecordBatch::try_new(
        schema(),
        vec![
            Arc::new(datafusion::arrow::array::Int64Array::from(vec![99_i64])),
            Arc::new(datafusion::arrow::array::Int64Array::from(vec![990_i64])),
        ],
    )?;
    let stale: Arc<dyn TableProvider> =
        Arc::new(MemTable::try_new(schema(), vec![vec![stale_rows]])?);
    let stale_plan = LogicalPlanBuilder::scan(
        TableReference::full("c", "d", "clicks"),
        provider_as_source(stale.clone()),
        None,
    )?
    .build()?;
    let relations = BTreeMap::from([(name(&["c", "d", "clicks"]), stale_plan)]);
    let lowered = lower(
        &ctx,
        Arc::new(PlanConfig::default()),
        &scope,
        &Inputs {
            graph: &graph,
            flow: &flow,
            relations: &relations,
            sources: std::slice::from_ref(&binding),
            statics: &[],
            locals: &[],
        },
    )
    .await?;
    assert!(lowered.evidence.admitted, "{:?}", lowered.evidence);
    assert_eq!(ids(&ctx, lowered.plan).await?, vec![0, 1, 2, 10, 11]);
    assert_eq!(
        lowered.evidence.sources_read,
        BTreeSet::from([name(&["c", "d", "clicks"])])
    );
    Ok(())
}

#[tokio::test]
async fn the_snapshot_carries_the_declared_static_and_local_identities() -> TestResult {
    let ctx = SessionContext::new();
    let scope = TaskResourceScope::new(Handle::current());
    let (_source, clicks) = bind_clicks(100)?;
    let (relations, mut events) = materialised_events()?;
    let dim_provider: Arc<dyn TableProvider> = Arc::new(MemTable::try_new(schema(), vec![vec![]])?);
    let mut dim = StaticBinding {
        name: name(&["c", "d", "dim"]),
        identity: "version:3".into(),
        provider: dim_provider,
    };
    let graph = graph();
    let flow = flow(
        join(
            join(read("clicks", true), read("dim", false)),
            read("events", false),
        ),
        &[],
    );
    let first = lower(
        &ctx,
        Arc::new(PlanConfig::default()),
        &scope,
        &Inputs {
            graph: &graph,
            flow: &flow,
            relations: &relations,
            sources: std::slice::from_ref(&clicks),
            statics: std::slice::from_ref(&dim),
            locals: std::slice::from_ref(&events),
        },
    )
    .await?
    .snapshot;
    assert_eq!(
        first.statics,
        vec![IdentitySnapshot {
            name: name(&["c", "d", "dim"]),
            identity: "version:3".into(),
        }]
    );
    assert_eq!(
        first.locals,
        vec![IdentitySnapshot {
            name: name(&["c", "d", "events"]),
            identity: "run:1".into(),
        }]
    );
    // The run-record form: JSON round trip of the whole snapshot.
    let first_json = serde_json::to_vec(&first)?;
    let decoded: LoweringSnapshot = serde_json::from_slice(&first_json)?;
    assert_eq!(decoded, first);
    dim.identity = "version:4".into();
    events.identity = "run:2".into();
    let second = lower(
        &ctx,
        Arc::new(PlanConfig::default()),
        &scope,
        &Inputs {
            graph: &graph,
            flow: &flow,
            relations: &relations,
            sources: std::slice::from_ref(&clicks),
            statics: std::slice::from_ref(&dim),
            locals: std::slice::from_ref(&events),
        },
    )
    .await?
    .snapshot;
    assert_ne!(first, second);
    assert_eq!(second.statics[0].identity, "version:4");
    assert_eq!(second.locals[0].identity, "run:2");
    assert_eq!(first.bindings, second.bindings);
    // A changed identity changes the serialised bytes, not only the Rust value.
    assert_ne!(serde_json::to_vec(&second)?, first_json);
    Ok(())
}

#[test]
fn identity_snapshots_with_dotted_and_quoted_name_parts_round_trip_through_json() -> TestResult {
    let snapshot = LoweringSnapshot {
        case_sensitive: false,
        ansi_mode: true,
        session_timezone: "UTC".into(),
        sql_conf: BTreeMap::from([("spark.sql.caseSensitive".to_string(), "false".to_string())]),
        bindings: vec![],
        statics: vec![
            IdentitySnapshot {
                name: name(&["c", "my.db", "dim`one"]),
                identity: "version:7".into(),
            },
            IdentitySnapshot {
                name: name(&["c", "my.db", "zeta"]),
                identity: "snapshot:abc".into(),
            },
        ],
        locals: vec![IdentitySnapshot {
            name: name(&["c", "d", "events with space"]),
            identity: "run:1".into(),
        }],
    };
    let json = serde_json::to_vec(&snapshot)?;
    let decoded: LoweringSnapshot = serde_json::from_slice(&json)?;
    assert_eq!(decoded, snapshot);
    // Name parts stay separate values, never joined into one string.
    let value: serde_json::Value = serde_json::from_slice(&json)?;
    assert_eq!(
        value["statics"][0]["name"],
        serde_json::json!(["c", "my.db", "dim`one"])
    );
    assert_eq!(value["locals"][0]["identity"], serde_json::json!("run:1"));
    Ok(())
}

#[tokio::test]
async fn a_local_view_expanding_to_other_leaf_scans_is_not_admitted() -> TestResult {
    // A local temporary view's installed relation is a plan over leaf
    // providers with other scan names; LocalBinding models one provider per
    // name, so the leaves are unbound and admission rejects them. This pins
    // the tracked boundary; it does not claim view support.
    let ctx = SessionContext::new();
    let scope = TaskResourceScope::new(Handle::current());
    let (_source, clicks) = bind_clicks(100)?;
    let leaf: Arc<dyn TableProvider> = Arc::new(MemTable::try_new(schema(), vec![vec![]])?);
    let view_plan = LogicalPlanBuilder::scan(
        TableReference::full("c", "d", "leaf_table"),
        provider_as_source(leaf.clone()),
        None,
    )?
    .project(vec![
        datafusion::logical_expr::col("id"),
        datafusion::logical_expr::col("v"),
    ])?
    .build()?;
    let relations = BTreeMap::from([(name(&["c", "d", "events"]), view_plan)]);
    let events = LocalBinding {
        name: name(&["c", "d", "events"]),
        identity: "run:1".into(),
        provider: leaf,
    };
    let graph = graph();
    let flow = flow(join(read("clicks", true), read("events", false)), &[]);
    let error = lower(
        &ctx,
        Arc::new(PlanConfig::default()),
        &scope,
        &Inputs {
            graph: &graph,
            flow: &flow,
            relations: &relations,
            sources: std::slice::from_ref(&clicks),
            statics: &[],
            locals: std::slice::from_ref(&events),
        },
    )
    .await
    .err();
    match error {
        Some(LowerError::NotAdmitted(evidence)) => {
            assert_eq!(evidence.unbound_scans, vec!["c.d.leaf_table".to_string()]);
        }
        other => return Err(format!("expected NotAdmitted, got {other:?}").into()),
    }
    Ok(())
}

// CF-0163 review fixtures, replayed verbatim from Codex's detached candidate
// 15e39b125 on 9dc1b1157 (where all three failed).
async fn codex_local_streaming_case(placeholder: bool) -> TestResult {
    let ctx = SessionContext::new();
    let scope = TaskResourceScope::new(Handle::current());
    let (_source, mut binding) = bind_clicks(100)?;
    binding.name = name(&["c", "d", "events"]);
    let graph = graph();
    let selected_flow = flow(read("events", true), &[]);
    let installed = if placeholder {
        placeholder_relations()?
    } else {
        relations()?
    };
    let lowered = lower(
        &ctx,
        Arc::new(PlanConfig::default()),
        &scope,
        &Inputs {
            graph: &graph,
            flow: &selected_flow,
            relations: &installed,
            sources: std::slice::from_ref(&binding),
            statics: &[],
            locals: &[],
        },
    )
    .await?;
    assert_eq!(ids(&ctx, lowered.plan).await?, vec![0, 1, 2, 10, 11]);
    Ok(())
}

#[tokio::test]
async fn codex_lower_local_stream_ignores_analysis_placeholder() -> TestResult {
    codex_local_streaming_case(true).await
}

#[tokio::test]
async fn codex_lower_local_stream_ignores_materialised_graph_relation() -> TestResult {
    codex_local_streaming_case(false).await
}

#[tokio::test]
async fn codex_lower_snapshot_distinguishes_declared_local_versions() -> TestResult {
    let ctx = SessionContext::new();
    let (_source, binding) = bind_clicks(100)?;
    let (installed, mut local) = materialised_events()?;
    let graph = graph();
    let selected_flow = flow(join(read("clicks", true), read("events", false)), &[]);
    let mut snapshots = vec![];
    for version in ["version:1", "version:2"] {
        let scope = TaskResourceScope::new(Handle::current());
        local.identity = version.into();
        let lowered = lower(
            &ctx,
            Arc::new(PlanConfig::default()),
            &scope,
            &Inputs {
                graph: &graph,
                flow: &selected_flow,
                relations: &installed,
                sources: std::slice::from_ref(&binding),
                statics: &[],
                locals: std::slice::from_ref(&local),
            },
        )
        .await?;
        snapshots.push(serde_json::to_value(lowered.snapshot)?);
    }
    assert_ne!(
        snapshots[0], snapshots[1],
        "different declared local versions have the same lowering snapshot"
    );
    Ok(())
}

/// A projection over `input` in the Spark spec, as the Connect decoder would
/// hand it to the resolver.
fn project(input: spec::QueryPlan, expressions: Vec<spec::Expr>) -> spec::QueryPlan {
    spec::QueryPlan::new(spec::QueryNode::Project {
        input: Some(Box::new(input)),
        expressions,
    })
}

fn attribute(name: &str) -> spec::Expr {
    spec::Expr::UnresolvedAttribute {
        name: spec::ObjectName::bare(name),
        plan_id: None,
        is_metadata_column: false,
    }
}

/// An unresolved call of a Sail built-in by name, resolved by `PlanResolver`
/// through Sail's own function registry.
fn call(function: &str, arguments: Vec<spec::Expr>) -> spec::Expr {
    spec::Expr::UnresolvedFunction(spec::UnresolvedFunction {
        function_name: spec::ObjectName::bare(function),
        arguments,
        named_arguments: vec![],
        is_distinct: false,
        is_user_defined_function: false,
        is_internal: None,
        ignore_nulls: None,
        filter: None,
        order_by: None,
    })
}

fn alias(expr: spec::Expr, name: &str) -> spec::Expr {
    spec::Expr::Alias {
        expr: Box::new(expr),
        name: vec![name.into()],
        metadata: None,
    }
}

/// Lower a streaming read of `clicks` projected to `id` and `expression`,
/// through the real resolver, and return the lowering.
async fn lower_projection(expression: spec::Expr) -> Result<Lowered, Box<dyn std::error::Error>> {
    // Function resolution consults the catalog manager for temporary
    // functions, so the session needs Sail's extensions.
    let ctx = crate::pipelines::sql::tests::resolution_context()?;
    let scope = TaskResourceScope::new(Handle::current());
    let (_source, binding) = bind_clicks(100)?;
    let graph = graph();
    let flow = flow(
        project(
            read("clicks", true),
            vec![attribute("id"), alias(expression, "x")],
        ),
        &[],
    );
    let lowered = lower(
        &ctx,
        Arc::new(PlanConfig::default()),
        &scope,
        &Inputs {
            graph: &graph,
            flow: &flow,
            relations: &relations()?,
            sources: std::slice::from_ref(&binding),
            statics: &[],
            locals: &[],
        },
    )
    .await?;
    Ok(lowered)
}

/// Shared body of the resolved-clock cases: the function is resolved by Sail
/// (not built from DataFusion's `expr_fn`), the plan is admitted, and the
/// evidence marks it non-replayable naming a stable function. This pins the
/// seam between resolution and admission: any folding of the clock into a
/// literal before admission would erase the provenance and fail here.
async fn assert_resolved_clock_not_replayable(function: &str) -> TestResult {
    let lowered = lower_projection(call(function, vec![])).await?;
    let evidence = &lowered.evidence;
    assert!(evidence.admitted, "{function}: {evidence:?}");
    assert!(
        !evidence.replayable,
        "{function}: a resolved clock must not be content-replayable: {evidence:?}"
    );
    assert!(
        evidence
            .operators
            .iter()
            .any(|op| op.reason.contains("is stable, not immutable")),
        "{function}: {evidence:?}"
    );
    Ok(())
}

#[tokio::test]
async fn a_resolved_sail_current_timestamp_is_not_content_replayable() -> TestResult {
    assert_resolved_clock_not_replayable("current_timestamp").await
}

#[tokio::test]
async fn a_resolved_sail_now_is_not_content_replayable() -> TestResult {
    assert_resolved_clock_not_replayable("now").await
}

#[tokio::test]
async fn a_resolved_sail_current_date_is_not_content_replayable() -> TestResult {
    assert_resolved_clock_not_replayable("current_date").await
}

#[tokio::test]
async fn a_resolved_immutable_projection_remains_content_replayable() -> TestResult {
    // Control: a resolved Immutable built-in in the same position.
    let lowered = lower_projection(call("abs", vec![attribute("id")])).await?;
    let evidence = &lowered.evidence;
    assert!(evidence.admitted, "{evidence:?}");
    assert!(evidence.replayable, "{evidence:?}");
    Ok(())
}

/// A provider error type the lowerer must never name.
#[derive(Debug)]
struct ProviderFailure(u32);

impl std::fmt::Display for ProviderFailure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "provider failure {}", self.0)
    }
}

impl std::error::Error for ProviderFailure {}

#[test]
fn lower_errors_expose_their_typed_causes_through_the_standard_chain() {
    // A typed provider error wrapped through DataFusionError → SparkError →
    // LowerError is recovered by walking source() and downcasting.
    let provider: Box<dyn std::error::Error + Send + Sync> = Box::new(ProviderFailure(87));
    let datafusion = datafusion::error::DataFusionError::External(provider);
    let spark = SparkError::from(datafusion);
    let error = LowerError::Resolution(spark);
    assert!(error.to_string().contains("provider failure 87"));
    let mut found = false;
    let mut current: Option<&(dyn std::error::Error + 'static)> = std::error::Error::source(&error);
    while let Some(cause) = current {
        if let Some(failure) = cause.downcast_ref::<ProviderFailure>() {
            assert_eq!(failure.0, 87);
            found = true;
            break;
        }
        current = cause.source();
    }
    assert!(
        found,
        "the provider failure was not reachable through source()"
    );
    // A binding refusal is the source of a Binding error.
    let binding = LowerError::Binding(BindingError::Tail);
    let source = std::error::Error::source(&binding);
    assert_eq!(
        source.and_then(|cause| cause.downcast_ref::<BindingError>()),
        Some(&BindingError::Tail)
    );
    // A rejection carries evidence, not a cause.
    let rejected = LowerError::NotAdmitted(Box::default());
    assert!(std::error::Error::source(&rejected).is_none());
}
