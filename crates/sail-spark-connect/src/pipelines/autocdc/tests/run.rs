//! Triggered SCD1 flow runs over the deterministic bounded source and a
//! Delta destination: `AutoCdcScd1AuxiliaryTableDurabilitySuite.scala:42–81`
//! as two real runs with an append between, the checkpoint layout and reset
//! numbering of `SystemMetadata`/`State.reset`, and replay of a completed
//! batch whose record advance was lost (the retry argument of
//! `Scd1ForeachBatchHandler.scala:55–60`).

use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;

use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use datafusion::common::ScalarValue;
use sail_common::spec;
use sail_common_datafusion::extension::SessionExtensionAccessor;
use sail_common_datafusion::task_resources::TaskResourceScope;
use tokio::runtime::Handle;

use super::super::run::{FlowRun, RunRecord, checkpoint_dir, run_flow};
use super::super::schema::Case;
use super::super::write::Scd1Write;
use super::super::{ChangeArgs, ScdType};
use super::write::{
    attribute, context, create_target, finish, key, meta, row, rows, target, target_columns,
};
use crate::pipelines::graph::{Flow, GraphDefinition, Name, Output};
use crate::pipelines::streaming::test_source::DeterministicSource;
use crate::session::SparkSession;
use crate::spark::connect::OutputType;
use crate::spark::connect::pipeline_command::DefineOutput;

type TestResult = Result<(), Box<dyn std::error::Error>>;

fn source_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, true),
        Field::new("name", DataType::Utf8, true),
        Field::new("version", DataType::Int64, true),
    ]))
}

fn event(id: i32, name: &str, version: i64) -> Vec<ScalarValue> {
    vec![
        ScalarValue::Int32(Some(id)),
        ScalarValue::Utf8(Some(name.into())),
        ScalarValue::Int64(Some(version)),
    ]
}

fn source_name() -> Name {
    vec!["test_catalog".into(), "test_db".into(), "events".into()]
}

fn flow_name() -> Name {
    vec![
        "test_catalog".into(),
        "test_db".into(),
        "auto_cdc_flow".into(),
    ]
}

fn graph() -> GraphDefinition {
    let output = |name: Name| Output {
        name,
        kind: OutputType::Table,
        definition: DefineOutput::default(),
        schema: None,
        view: None,
    };
    GraphDefinition {
        catalog: "test_catalog".into(),
        database: "test_db".into(),
        sql_conf: HashMap::new(),
        outputs: vec![output(source_name()), output(target())],
        flows: vec![],
    }
}

fn flow() -> Flow {
    Flow {
        name: flow_name(),
        target: target(),
        query: spec::QueryPlan::new(spec::QueryNode::Read {
            read_type: spec::ReadType::NamedTable(Box::new(spec::ReadNamedTable {
                name: source_name().into(),
                temporal: None,
                sample: None,
                options: vec![],
            })),
            is_streaming: true,
        }),
        sql_conf: HashMap::new(),
        catalog: "test_catalog".into(),
        database: "test_db".into(),
        auto_cdc: None,
    }
}

fn warehouse(
    ctx: &datafusion::prelude::SessionContext,
) -> Result<std::path::PathBuf, Box<dyn std::error::Error>> {
    Ok(std::path::PathBuf::from(
        &ctx.extension::<SparkSession>()?
            .plan_config()?
            .default_warehouse_directory,
    ))
}

// DurabilitySuite:42–81 as two runs: the second run's higher-sequence event
// upserts the row and a new key lands, continuing from the record.
#[tokio::test]
async fn two_runs_continue_from_the_record() -> TestResult {
    let (ctx, config) = context()?;
    let scope = TaskResourceScope::new(Handle::current());
    let target = target();
    create_target(&ctx, config.clone(), &target, "delta", target_columns()).await?;
    let source = DeterministicSource::new("events", source_schema());
    source.create_partition("p0");
    source.append("p0", vec![event(1, "alice", 1)]);
    let args = ChangeArgs::new(
        vec![key("id")?],
        attribute("version"),
        ScdType::Type1,
        None,
        None,
    )?;
    let write = Scd1Write {
        flow_name: "test_catalog.test_db.auto_cdc_flow",
        destination: &target,
        destination_format: Some("delta"),
        args: &args,
        sequencing_type: &spec::DataType::Int64,
        case: Case::Insensitive,
    };
    let checkpoint = checkpoint_dir(&warehouse(&ctx)?, &target, &flow_name(), false)?;
    assert!(checkpoint.ends_with(
        "_checkpoints/test_catalog.test_db.target/test_catalog.test_db.auto_cdc_flow/0"
    ));
    let graph = graph();
    let flow = flow();
    let relations = BTreeMap::new();
    let run = FlowRun {
        graph: &graph,
        flow: &flow,
        relations: &relations,
        source_name: &source_name(),
        source: &source,
        write: &write,
        checkpoint: &checkpoint,
        max_rows_per_partition: 100,
    };
    let first = run_flow(&ctx, config.clone(), &scope, &run).await?;
    assert_eq!(first.batches, 1);
    assert_eq!(first.record.completed_batches, 1);
    assert_eq!(
        rows(&ctx, config.clone(), &target).await?,
        vec![row(&["1", "alice", "1", &meta("NULL", "1")])]
    );
    // Nothing new: a run to the same frontier does no batch.
    let idle = run_flow(&ctx, config.clone(), &scope, &run).await?;
    assert_eq!(idle.batches, 0);
    source.append("p0", vec![event(1, "alice2", 2), event(2, "bob", 1)]);
    let second = run_flow(&ctx, config.clone(), &scope, &run).await?;
    assert_eq!(second.batches, 1);
    assert_eq!(second.record.completed_batches, 2);
    assert_eq!(
        rows(&ctx, config.clone(), &target).await?,
        vec![
            row(&["1", "alice2", "2", &meta("NULL", "2")]),
            row(&["2", "bob", "1", &meta("NULL", "1")]),
        ]
    );
    let stored = RunRecord::load(&checkpoint)?.ok_or("record missing")?;
    assert_eq!(stored, second.record);
    assert!(stored.authority.contains("no ownership fencing"));
    finish(&ctx).await?;
    Ok(())
}

// Replay: the batch completed but its record advance was lost; the next run
// replays the same intent and the merges leave the target unchanged.
#[tokio::test]
async fn a_replayed_batch_leaves_the_target_unchanged() -> TestResult {
    let (ctx, config) = context()?;
    let scope = TaskResourceScope::new(Handle::current());
    let target = target();
    create_target(&ctx, config.clone(), &target, "delta", target_columns()).await?;
    let source = DeterministicSource::new("events", source_schema());
    source.create_partition("p0");
    source.append("p0", vec![event(1, "alice", 1), event(2, "bob", 5)]);
    let args = ChangeArgs::new(
        vec![key("id")?],
        attribute("version"),
        ScdType::Type1,
        None,
        None,
    )?;
    let write = Scd1Write {
        flow_name: "test_catalog.test_db.auto_cdc_flow",
        destination: &target,
        destination_format: Some("delta"),
        args: &args,
        sequencing_type: &spec::DataType::Int64,
        case: Case::Insensitive,
    };
    let checkpoint = checkpoint_dir(&warehouse(&ctx)?, &target, &flow_name(), false)?;
    let graph = graph();
    let flow = flow();
    let relations = BTreeMap::new();
    let run = FlowRun {
        graph: &graph,
        flow: &flow,
        relations: &relations,
        source_name: &source_name(),
        source: &source,
        write: &write,
        checkpoint: &checkpoint,
        max_rows_per_partition: 100,
    };
    let first = run_flow(&ctx, config.clone(), &scope, &run).await?;
    assert_eq!(first.batches, 1);
    let expected = vec![
        row(&["1", "alice", "1", &meta("NULL", "1")]),
        row(&["2", "bob", "5", &meta("NULL", "5")]),
    ];
    assert_eq!(rows(&ctx, config.clone(), &target).await?, expected);
    // Lose the advance: roll the record back to before the batch.
    let mut rolled_back = first.record.clone();
    rolled_back.progress = Default::default();
    rolled_back.completed_batches = 0;
    rolled_back.store(&checkpoint)?;
    let replay = run_flow(&ctx, config.clone(), &scope, &run).await?;
    assert_eq!(replay.batches, 1);
    assert_eq!(rows(&ctx, config.clone(), &target).await?, expected);
    finish(&ctx).await?;
    Ok(())
}

// SystemMetadata/State.reset: the checkpoint directory numbers advance only
// on a reset, and a reset run starts again from the earliest position.
#[tokio::test]
async fn a_reset_creates_the_next_checkpoint_version_and_restarts() -> TestResult {
    let (ctx, config) = context()?;
    let scope = TaskResourceScope::new(Handle::current());
    let target = target();
    create_target(&ctx, config.clone(), &target, "delta", target_columns()).await?;
    let source = DeterministicSource::new("events", source_schema());
    source.create_partition("p0");
    source.append("p0", vec![event(1, "alice", 1)]);
    let args = ChangeArgs::new(
        vec![key("id")?],
        attribute("version"),
        ScdType::Type1,
        None,
        None,
    )?;
    let write = Scd1Write {
        flow_name: "test_catalog.test_db.auto_cdc_flow",
        destination: &target,
        destination_format: Some("delta"),
        args: &args,
        sequencing_type: &spec::DataType::Int64,
        case: Case::Insensitive,
    };
    let root = warehouse(&ctx)?;
    let first_dir = checkpoint_dir(&root, &target, &flow_name(), false)?;
    assert!(first_dir.ends_with("0"));
    assert_eq!(
        checkpoint_dir(&root, &target, &flow_name(), false)?,
        first_dir
    );
    let graph = graph();
    let flow = flow();
    let relations = BTreeMap::new();
    let run = FlowRun {
        graph: &graph,
        flow: &flow,
        relations: &relations,
        source_name: &source_name(),
        source: &source,
        write: &write,
        checkpoint: &first_dir,
        max_rows_per_partition: 100,
    };
    assert_eq!(
        run_flow(&ctx, config.clone(), &scope, &run).await?.batches,
        1
    );
    let reset_dir = checkpoint_dir(&root, &target, &flow_name(), true)?;
    assert!(reset_dir.ends_with("1"));
    assert!(RunRecord::load(&reset_dir)?.is_none());
    let reset_run = FlowRun {
        checkpoint: &reset_dir,
        ..run
    };
    let again = run_flow(&ctx, config.clone(), &scope, &reset_run).await?;
    assert_eq!(again.batches, 1);
    assert_eq!(again.record.completed_batches, 1);
    assert_eq!(
        rows(&ctx, config.clone(), &target).await?,
        vec![row(&["1", "alice", "1", &meta("NULL", "1")])]
    );
    finish(&ctx).await?;
    Ok(())
}
