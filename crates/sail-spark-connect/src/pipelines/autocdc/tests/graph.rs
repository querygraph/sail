//! Graph registration and validation.

use super::*;

fn graph() -> PipelineGraph {
    PipelineGraph::new("test_catalog".into(), "test_db".into(), HashMap::new())
}

fn define_output(graph: &mut PipelineGraph, name: &str, kind: OutputType) -> SparkResult<()> {
    graph.define_output(pc::DefineOutput {
        output_name: Some(name.into()),
        output_type: Some(kind as i32),
        ..Default::default()
    })?;
    Ok(())
}

fn define_cdc_flow(
    graph: &mut PipelineGraph,
    name: &str,
    target: &str,
    source: &str,
) -> SparkResult<Vec<String>> {
    graph.define_flow(pc::DefineFlow {
        flow_name: Some(name.into()),
        target_dataset_name: Some(target.into()),
        details: Some(Details::AutoCdcFlowDetails(details(source, &["id"], "seq"))),
        ..Default::default()
    })
}

fn define_relation_flow(
    graph: &mut PipelineGraph,
    name: &str,
    target: &str,
    source: &str,
) -> SparkResult<Vec<String>> {
    // A streaming read of `source`, as `spark.readStream.table(source)` sends it.
    let relation = sc::Relation {
        rel_type: Some(sc::relation::RelType::Read(sc::Read {
            read_type: Some(sc::read::ReadType::NamedTable(sc::read::NamedTable {
                unparsed_identifier: source.into(),
                options: HashMap::new(),
            })),
            is_streaming: true,
        })),
        ..Default::default()
    };
    graph.define_flow(pc::DefineFlow {
        flow_name: Some(name.into()),
        target_dataset_name: Some(target.into()),
        details: Some(Details::RelationFlowDetails(
            pc::define_flow::WriteRelationFlowDetails {
                relation: Some(relation),
            },
        )),
        ..Default::default()
    })
}

/// The checks `analysis::analyze` runs, in its order, without resolving any
/// flow: registration validation, then after resolution the Auto CDC
/// multiflow clause over the graph, the materialized-view multiflow clause,
/// streamingness, and the temporary-view clause. Public phase ordering
/// against resolution failures is covered by the Connect-level Python tests.
fn post_resolution_checks(graph: &PipelineGraph) -> SparkResult<()> {
    let definition = &graph.definition;
    definition.validate()?;
    validate_multiple_flows_globally(definition)?;
    definition.validate()?;
    validate_temporary_views(definition)
}

#[test]
fn default_flow_name_is_the_target_and_explicit_name_is_honored() -> SparkResult<()> {
    let mut graph = graph();
    define_output(&mut graph, "target", OutputType::Table)?;
    assert_eq!(
        define_cdc_flow(&mut graph, "target", "target", "src")?,
        names(&["test_catalog", "test_db", "target"])
    );
    assert_eq!(
        define_cdc_flow(&mut graph, "my_flow", "target", "src")?,
        names(&["test_catalog", "test_db", "my_flow"])
    );
    let flows = &graph.definition.flows;
    assert_eq!(flows.len(), 2);
    assert!(flows.iter().all(|flow| flow.auto_cdc.is_some()));
    assert!(
        flows
            .iter()
            .all(|flow| flow.target == names(&["test_catalog", "test_db", "target"]))
    );
    Ok(())
}

#[test]
fn auto_cdc_flow_targeting_a_temporary_view_is_rejected() -> SparkResult<()> {
    let mut graph = graph();
    define_output(&mut graph, "dummy_table", OutputType::Table)?;
    define_relation_flow(&mut graph, "dummy_table", "dummy_table", "events")?;
    define_output(&mut graph, "target_view", OutputType::TemporaryView)?;
    define_cdc_flow(&mut graph, "target_view", "target_view", "events")?;
    // Sail keeps temporary view identities single-part; Spark's test expects
    // the fully qualified identifier for both parameters.
    check_error(
        post_resolution_checks(&graph),
        "INVALID_FLOW_QUERY_TYPE.AUTOCDC_RELATION_FOR_TEMPORARY_VIEW",
        "42000",
        &[
            ("flowIdentifier", "`target_view`"),
            ("viewIdentifier", "`target_view`"),
        ],
    );
    Ok(())
}

#[test]
fn a_multiquery_table_cannot_have_an_auto_cdc_input() -> SparkResult<()> {
    let mut graph = graph();
    define_output(&mut graph, "target", OutputType::Table)?;
    define_cdc_flow(&mut graph, "auto_cdc_flow", "target", "events")?;
    define_relation_flow(&mut graph, "extra_flow", "target", "events")?;
    check_error(
        post_resolution_checks(&graph),
        "AUTOCDC_MULTIPLE_FLOWS_TO_TARGET",
        "42000",
        &[
            ("tableName", "test_catalog.test_db.target"),
            (
                "flows",
                "test_catalog.test_db.auto_cdc_flow, test_catalog.test_db.extra_flow",
            ),
        ],
    );
    Ok(())
}

#[test]
fn auto_cdc_flow_targeting_a_materialized_view_is_rejected() -> SparkResult<()> {
    // Spark checks `flowIdentifier` and `tableIdentifier` parameters; Sail's
    // existing streamingness arm reports the condition without parameters.
    let mut graph = graph();
    define_output(&mut graph, "target", OutputType::MaterializedView)?;
    define_cdc_flow(&mut graph, "auto_cdc_flow", "target", "events")?;
    let message = format!("{:?}", post_resolution_checks(&graph));
    assert!(
        message.contains("INVALID_FLOW_QUERY_TYPE.STREAMING_RELATION_FOR_MATERIALIZED_VIEW"),
        "{message}"
    );
    Ok(())
}

#[test]
fn auto_cdc_flow_targeting_a_persisted_view_is_rejected() -> SparkResult<()> {
    // Spark registers a persisted view without a query; Sail's SQL path is
    // the only producer of persisted views, so the output is adjusted here.
    // Parameters are not checked: Sail's existing arm reports the condition only.
    let mut graph = graph();
    define_output(&mut graph, "target_view", OutputType::MaterializedView)?;
    let query = definition(details("events", &["id"], "seq"))?.query();
    if let Some(output) = graph.definition.outputs.last_mut() {
        output.view = Some(spec::ViewDefinition {
            definition: "SELECT 1 AS id, 1 AS seq".into(),
            input: Box::new(query),
            columns: None,
            if_not_exists: false,
            replace: false,
            comment: None,
            properties: vec![],
        });
    }
    define_cdc_flow(&mut graph, "target_view", "target_view", "events")?;
    let message = format!("{:?}", post_resolution_checks(&graph));
    assert!(
        message.contains("INVALID_FLOW_QUERY_TYPE.STREAMING_RELATION_FOR_PERSISTED_VIEW"),
        "{message}"
    );
    Ok(())
}

#[test]
fn auto_cdc_flow_targeting_a_sink_passes_graph_validation() -> SparkResult<()> {
    // `PipelineExecution.dryRunPipeline` resolves and validates only; the
    // sink rejection lives in `FlowPlanner` and is reached when a flow is
    // planned for execution. Graph validation therefore accepts the target.
    let mut graph = graph();
    define_output(&mut graph, "dummy_table", OutputType::Table)?;
    define_relation_flow(&mut graph, "dummy_table", "dummy_table", "events")?;
    define_output(&mut graph, "out", OutputType::Sink)?;
    define_cdc_flow(&mut graph, "out", "out", "events")?;
    post_resolution_checks(&graph)
}

fn define_batch_flow(
    graph: &mut PipelineGraph,
    name: &str,
    target: &str,
    source: &str,
) -> SparkResult<Vec<String>> {
    let relation = sc::Relation {
        rel_type: Some(sc::relation::RelType::Read(sc::Read {
            read_type: Some(sc::read::ReadType::NamedTable(sc::read::NamedTable {
                unparsed_identifier: source.into(),
                options: HashMap::new(),
            })),
            is_streaming: false,
        })),
        ..Default::default()
    };
    graph.define_flow(pc::DefineFlow {
        flow_name: Some(name.into()),
        target_dataset_name: Some(target.into()),
        details: Some(Details::RelationFlowDetails(
            pc::define_flow::WriteRelationFlowDetails {
                relation: Some(relation),
            },
        )),
        ..Default::default()
    })
}

#[test]
fn auto_cdc_multiflow_is_reported_before_materialized_view_multiflow_in_either_order()
-> SparkResult<()> {
    // `validateMultiQueryTables` searches every multiflow destination for an
    // Auto CDC flow before searching any for a non-streaming multiflow.
    for cdc_first in [false, true] {
        let mut graph = graph();
        let define_mv = |graph: &mut PipelineGraph| -> SparkResult<()> {
            define_output(graph, "mv", OutputType::MaterializedView)?;
            define_batch_flow(graph, "mv_a", "mv", "events")?;
            define_batch_flow(graph, "mv_b", "mv", "events")?;
            Ok(())
        };
        let define_cdc = |graph: &mut PipelineGraph| -> SparkResult<()> {
            define_output(graph, "target", OutputType::Table)?;
            define_cdc_flow(graph, "auto_cdc_flow", "target", "events")?;
            define_relation_flow(graph, "extra_flow", "target", "events")?;
            Ok(())
        };
        if cdc_first {
            define_cdc(&mut graph)?;
            define_mv(&mut graph)?;
        } else {
            define_mv(&mut graph)?;
            define_cdc(&mut graph)?;
        }
        check_error(
            post_resolution_checks(&graph),
            "AUTOCDC_MULTIPLE_FLOWS_TO_TARGET",
            "42000",
            &[
                ("tableName", "test_catalog.test_db.target"),
                (
                    "flows",
                    "test_catalog.test_db.auto_cdc_flow, test_catalog.test_db.extra_flow",
                ),
            ],
        );
    }
    Ok(())
}

#[test]
fn materialized_view_multiflow_is_still_reported_without_an_auto_cdc_flow() -> SparkResult<()> {
    let mut graph = graph();
    define_output(&mut graph, "mv", OutputType::MaterializedView)?;
    define_batch_flow(&mut graph, "mv_a", "mv", "events")?;
    define_batch_flow(&mut graph, "mv_b", "mv", "events")?;
    // `validate` now carries the materialized-view clause itself.
    let message = format!("{:?}", post_resolution_checks(&graph));
    assert!(
        message.contains("MATERIALIZED_VIEW_WITH_MULTIPLE_QUERIES"),
        "{message}"
    );
    Ok(())
}
