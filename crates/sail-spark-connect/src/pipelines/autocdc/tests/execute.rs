//! Admission into `execution::execute`: only AutoCDC SCD1 tables over
//! registered bounded sources run, end to end through materialization, and
//! everything else stays refused before its first write.

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::sync::Arc;

use datafusion::arrow::datatypes::{DataType, Field, Fields, Schema};
use datafusion::prelude::SessionContext;
use sail_common::spec;
use sail_common_datafusion::extension::SessionExtensionAccessor;

use super::super::execute::{BoundedSources, admitted, execute_graph};
use super::super::schema::{Case, METADATA_COLUMN};
use super::super::write::flow_output_schema;
use super::super::{ChangeArgs, ColumnSelection, ScdType, UnqualifiedColumnName};
use super::run::{event, flow, source_name, source_schema, warehouse};
use super::write::{attribute, context, finish, key, meta, row, rows, target};
use crate::pipelines::analysis::AnalyzedGraph;
use crate::pipelines::graph::{Flow, GraphDefinition, Output};
use crate::pipelines::selection::Selection;
use crate::pipelines::streaming::test_source::DeterministicSource;
use crate::spark::connect::OutputType;
use crate::spark::connect::pipeline_command::{DefineOutput, define_output};

type TestResult = Result<(), Box<dyn std::error::Error>>;

fn output() -> Output {
    Output {
        name: target(),
        kind: OutputType::Table,
        definition: DefineOutput {
            details: Some(define_output::Details::TableDetails(
                define_output::TableDetails {
                    format: Some("delta".into()),
                    ..Default::default()
                },
            )),
            ..Default::default()
        },
        schema: None,
        view: None,
    }
}

fn cdc_flow() -> Result<Flow, Box<dyn std::error::Error>> {
    Ok(Flow {
        auto_cdc: Some(ChangeArgs::new(
            vec![key("id")?],
            attribute("version"),
            ScdType::Type1,
            None,
            None,
        )?),
        ..flow()
    })
}

fn graph_of(flows: Vec<Flow>, outputs: Vec<Output>) -> GraphDefinition {
    GraphDefinition {
        catalog: "test_catalog".into(),
        database: "test_db".into(),
        sql_conf: HashMap::new(),
        outputs,
        flows,
    }
}

fn analyzed(flow: &Flow) -> AnalyzedGraph {
    let metadata = Fields::from(vec![
        Field::new("deleteSequence", DataType::Int64, true),
        Field::new("upsertSequence", DataType::Int64, true),
    ]);
    let schema = Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("name", DataType::Utf8, true),
        Field::new("version", DataType::Int64, false),
        Field::new(METADATA_COLUMN, DataType::Struct(metadata), false),
    ]);
    AnalyzedGraph {
        order: vec![target()],
        dependencies: BTreeMap::new(),
        schemas: BTreeMap::from([(target(), Arc::new(schema))]),
        publications: BTreeMap::new(),
        accepted_confs: BTreeMap::from([(flow.name.clone(), HashMap::new())]),
    }
}

fn selection(reset: bool) -> Selection {
    let names = BTreeSet::from([target()]);
    if reset {
        Selection {
            refresh: BTreeSet::new(),
            reset: names,
        }
    } else {
        Selection {
            refresh: names,
            reset: BTreeSet::new(),
        }
    }
}

fn register(ctx: &SessionContext, source: Arc<DeterministicSource>) {
    ctx.state_ref()
        .write()
        .config_mut()
        .set_extension(Arc::new(BoundedSources(BTreeMap::from([(
            source_name(),
            source as _,
        )]))));
}

fn events() -> Arc<DeterministicSource> {
    let source = DeterministicSource::new("events", source_schema());
    source.create_partition("p0");
    source.append("p0", vec![event(1, "alice", 1), event(2, "bob", 1)]);
    Arc::new(source)
}

#[tokio::test]
async fn an_admitted_graph_runs_through_materialization() -> TestResult {
    let (ctx, _) = context()?;
    let config = ctx
        .extension::<crate::session::SparkSession>()?
        .plan_config()?;
    let source = events();
    register(&ctx, source.clone());
    let flow = cdc_flow()?;
    let analyzed = analyzed(&flow);
    let graph = graph_of(vec![flow], vec![output()]);
    assert!(admitted(&ctx, &graph)?);
    let first = execute_graph(&ctx, &graph, &analyzed, &selection(false)).await?;
    assert_eq!(first.len(), 1);
    assert_eq!(first[0].batches, 1);
    assert_eq!(
        rows(&ctx, config.clone(), &target()).await?,
        vec![
            row(&["1", "alice", "1", &meta("NULL", "1")]),
            row(&["2", "bob", "1", &meta("NULL", "1")]),
        ]
    );
    assert!(warehouse(&ctx)?.join("_control/authority.db").exists());
    // The authority survived the run: a second run finds nothing to do, and
    // an appended event continues from its progress.
    source.append("p0", vec![event(1, "alice2", 2)]);
    let second = execute_graph(&ctx, &graph, &analyzed, &selection(false)).await?;
    assert_eq!(second[0].batches, 1);
    assert_eq!(
        rows(&ctx, config.clone(), &target()).await?,
        vec![
            row(&["1", "alice2", "2", &meta("NULL", "2")]),
            row(&["2", "bob", "1", &meta("NULL", "1")]),
        ]
    );
    let third = execute_graph(&ctx, &graph, &analyzed, &selection(false)).await?;
    assert_eq!(third[0].batches, 0);
    finish(&ctx).await?;
    Ok(())
}

/// A full refresh runs through the authority, not the unfenced truncate.
#[tokio::test]
async fn a_full_refresh_is_a_reset_of_the_enrolled_target() -> TestResult {
    let (ctx, _) = context()?;
    let config = ctx
        .extension::<crate::session::SparkSession>()?
        .plan_config()?;
    register(&ctx, events());
    let flow = cdc_flow()?;
    let analyzed = analyzed(&flow);
    let graph = graph_of(vec![flow], vec![output()]);
    let first = execute_graph(&ctx, &graph, &analyzed, &selection(false)).await?;
    assert_eq!(first[0].reset, None);
    let before = rows(&ctx, config.clone(), &target()).await?;
    let refreshed = execute_graph(&ctx, &graph, &analyzed, &selection(true)).await?;
    assert_eq!(refreshed[0].reset, Some(1));
    assert_eq!(refreshed[0].batches, first[0].batches);
    assert_eq!(rows(&ctx, config, &target()).await?, before);
    finish(&ctx).await?;
    Ok(())
}

#[tokio::test]
async fn nothing_else_is_admitted() -> TestResult {
    let (ctx, _) = context()?;
    let graph = graph_of(vec![cdc_flow()?], vec![output()]);
    assert!(
        !admitted(&ctx, &graph)?,
        "no bounded sources in the session"
    );

    register(&ctx, events());
    assert!(admitted(&ctx, &graph)?);
    let batch_flow = graph_of(vec![flow()], vec![output()]);
    assert!(!admitted(&ctx, &batch_flow)?, "a flow that is not AutoCDC");

    let sink = Output {
        kind: OutputType::Sink,
        ..output()
    };
    let with_sink = graph_of(vec![cdc_flow()?], vec![sink]);
    assert!(!admitted(&ctx, &with_sink)?, "a sink output");

    let mut unregistered = cdc_flow()?;
    unregistered.query = spec::QueryPlan::new(spec::QueryNode::Read {
        read_type: spec::ReadType::NamedTable(Box::new(spec::ReadNamedTable {
            name: spec::ObjectName::bare("elsewhere"),
            temporal: None,
            sample: None,
            options: vec![],
        })),
        is_streaming: true,
    });
    let elsewhere = graph_of(vec![unregistered], vec![output()]);
    assert!(!admitted(&ctx, &elsewhere)?, "an unregistered source");

    let mut type2 = cdc_flow()?;
    if let Some(args) = &mut type2.auto_cdc {
        args.scd_type = ScdType::Type2;
    }
    let scd2 = graph_of(vec![type2], vec![output()]);
    assert!(!admitted(&ctx, &scd2)?, "SCD type 2");
    finish(&ctx).await?;
    Ok(())
}

fn iggy_view(format: &str, is_streaming: bool) -> (Output, Flow) {
    let name = vec!["changes".to_string()];
    let output = Output {
        name: name.clone(),
        kind: OutputType::TemporaryView,
        ..output()
    };
    let read = spec::ReadDataSource {
        format: Some(format.to_string()),
        schema: None,
        options: vec![],
        paths: vec![],
        predicates: vec![],
    };
    let flow = Flow {
        name: name.clone(),
        target: name,
        auto_cdc: None,
        query: spec::QueryPlan::new(spec::QueryNode::Read {
            read_type: spec::ReadType::DataSource(Box::new(read)),
            is_streaming,
        }),
        ..flow()
    };
    (output, flow)
}

fn cdc_flow_over_changes() -> Result<Flow, Box<dyn std::error::Error>> {
    let mut flow = cdc_flow()?;
    flow.query = spec::QueryPlan::new(spec::QueryNode::Read {
        read_type: spec::ReadType::NamedTable(Box::new(spec::ReadNamedTable {
            name: spec::ObjectName::bare("Changes"),
            temporal: None,
            sample: None,
            options: vec![],
        })),
        is_streaming: true,
    });
    Ok(flow)
}

#[tokio::test]
async fn an_iggy_view_is_a_source_and_only_a_bare_streaming_read_is_one() -> TestResult {
    let (ctx, _) = context()?;
    let with = |view: (Output, Flow)| -> Result<_, Box<dyn std::error::Error>> {
        let (view_output, view_flow) = view;
        Ok(graph_of(
            vec![view_flow, cdc_flow_over_changes()?],
            vec![view_output, output()],
        ))
    };
    assert!(admitted(&ctx, &with(iggy_view("iggy", true))?)?);
    assert!(admitted(&ctx, &with(iggy_view("IGGY", true))?)?);
    assert!(
        !admitted(&ctx, &with(iggy_view("iggy", false))?)?,
        "a batch read has no bound"
    );
    assert!(
        !admitted(&ctx, &with(iggy_view("rate", true))?)?,
        "another streaming format"
    );

    let (view_output, mut view_flow) = iggy_view("iggy", true);
    view_flow.query = spec::QueryPlan::new(spec::QueryNode::Limit {
        input: Box::new(view_flow.query.clone()),
        skip: None,
        limit: Some(spec::Expr::Literal(spec::Literal::Int64 { value: Some(1) })),
    });
    assert!(
        !admitted(&ctx, &with((view_output, view_flow))?)?,
        "a transformed read would be run past its plan"
    );

    let (view_output, view_flow) = iggy_view("iggy", true);
    let mut second = view_flow.clone();
    second.name = vec!["again".to_string()];
    let two_flows = graph_of(
        vec![view_flow, second, cdc_flow_over_changes()?],
        vec![view_output, output()],
    );
    assert!(!admitted(&ctx, &two_flows)?, "a view fed by two flows");

    let (view_output, view_flow) = iggy_view("iggy", true);
    let ordinary = Flow {
        auto_cdc: None,
        ..cdc_flow_over_changes()?
    };
    let plain = graph_of(vec![view_flow, ordinary], vec![view_output, output()]);
    assert!(!admitted(&ctx, &plain)?, "an ordinary flow over the view");
    finish(&ctx).await?;
    Ok(())
}

#[tokio::test]
async fn the_output_schema_is_the_selection_then_the_typed_metadata_column() -> TestResult {
    let (ctx, config) = context()?;
    let source = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("op", DataType::Utf8, true),
        Field::new("seq", DataType::Int32, false),
    ]));
    let args = |selection| {
        ChangeArgs::new(
            vec![key("id")?],
            attribute("seq"),
            ScdType::Type1,
            None,
            selection,
        )
    };
    let everything = flow_output_schema(
        &ctx,
        config.clone(),
        &args(None)?,
        Case::Insensitive,
        &source,
    )
    .await?;
    let names: Vec<&str> = everything
        .fields()
        .iter()
        .map(|x| x.name().as_str())
        .collect();
    assert_eq!(names, vec!["id", "op", "seq", METADATA_COLUMN]);
    assert!(
        !everything.field(0).is_nullable(),
        "source fields keep their nullability"
    );
    let metadata = everything.field(3);
    assert!(!metadata.is_nullable());
    let DataType::Struct(members) = metadata.data_type() else {
        return Err("the metadata column is not a struct".into());
    };
    let members: Vec<(&str, &DataType, bool)> = members
        .iter()
        .map(|x| (x.name().as_str(), x.data_type(), x.is_nullable()))
        .collect();
    assert_eq!(
        members,
        vec![
            ("deleteSequence", &DataType::Int32, true),
            ("upsertSequence", &DataType::Int32, true),
        ]
    );

    let without_op = Some(ColumnSelection::Exclude(vec![
        UnqualifiedColumnName::parse("OP")?,
    ]));
    let selected = flow_output_schema(
        &ctx,
        config.clone(),
        &args(without_op)?,
        Case::Insensitive,
        &source,
    )
    .await?;
    let names: Vec<&str> = selected
        .fields()
        .iter()
        .map(|x| x.name().as_str())
        .collect();
    assert_eq!(names, vec!["id", "seq", METADATA_COLUMN]);

    let unknown = ChangeArgs::new(
        vec![key("id")?],
        attribute("missing"),
        ScdType::Type1,
        None,
        None,
    )?;
    assert!(
        flow_output_schema(&ctx, config, &unknown, Case::Insensitive, &source)
            .await
            .is_err(),
        "a sequencing expression over no column"
    );
    finish(&ctx).await?;
    Ok(())
}
