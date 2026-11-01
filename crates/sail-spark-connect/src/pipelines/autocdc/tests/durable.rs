//! The durable SCD1 run: a crash at every stage of a batch, recovered by a
//! new owner, with both tables inspected afterwards. Rows are archived before
//! the first effect, so a source that has since expired them cannot matter.

use std::sync::Arc;
use std::sync::atomic::{AtomicI64, Ordering};

use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use datafusion::common::ScalarValue;
use datafusion::prelude::SessionContext;
use sail_common_datafusion::task_resources::TaskResourceScope;
use sail_plan::config::PlanConfig;
use sail_sdp_control::control::Control;
use sail_sdp_control::identity::{EffectStep, FlowKey, IntentId, SinkIdentity, TargetKey};
use sail_sdp_control::memory::UnfencedMemoryStore;
use sail_sdp_control::records::ReceiptKind;
use sail_sdp_control::store::{Clock, Store};
use sail_sdp_control::turso_store::TursoStore;
use tokio::runtime::Handle;

use super::super::durable::{DurableOutcome, DurableRun, Stage, run_durable_flow};
use super::super::run::FlowRun;
use super::super::schema::Case;
use super::super::write::{IN_COMMIT_TIMESTAMPS_PROPERTY, Scd1Write};
use super::super::{ChangeArgs, ColumnSelection, ScdType, auxiliary};
use super::fenced::{delta_version, table_dir, table_url};
use super::run::{flow, flow_name, graph, source_name, warehouse};
use super::write::{
    attribute, context, create_target_with, eq_true, finish, key, meta, row, rows, target,
    target_columns,
};
use crate::error::{SparkError, SparkResult};
use crate::pipelines::graph::Name;
use crate::pipelines::streaming::test_source::DeterministicSource;

type TestResult = Result<(), Box<dyn std::error::Error>>;

const LEASE: i64 = 10_000;

#[derive(Default)]
struct ManualClock(AtomicI64);

impl Clock for ManualClock {
    fn now(&self) -> i64 {
        self.0.load(Ordering::SeqCst)
    }
}

fn schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, true),
        Field::new("name", DataType::Utf8, true),
        Field::new("version", DataType::Int64, true),
        Field::new("is_delete", DataType::Boolean, true),
    ]))
}

fn event(id: i32, name: &str, version: i64, delete: bool) -> Vec<ScalarValue> {
    vec![
        ScalarValue::Int32(Some(id)),
        ScalarValue::Utf8(Some(name.into())),
        ScalarValue::Int64(Some(version)),
        ScalarValue::Boolean(Some(delete)),
    ]
}

/// Batch 0 upserts, batch 1 deletes (the auxiliary effect carries data), and
/// batch 2 upserts another key.
fn source() -> DeterministicSource {
    let source = DeterministicSource::new("events", schema());
    source.create_partition("p0");
    source.append(
        "p0",
        vec![
            event(1, "alice", 1, false),
            event(1, "", 2, true),
            event(2, "bob", 1, false),
        ],
    );
    source
}

struct Fixture {
    ctx: SessionContext,
    config: Arc<PlanConfig>,
    args: ChangeArgs,
    checkpoint: std::path::PathBuf,
    scope: TaskResourceScope,
}

async fn setup() -> Result<Fixture, Box<dyn std::error::Error>> {
    let (ctx, config) = context()?;
    create_target_with(
        &ctx,
        config.clone(),
        &target(),
        "delta",
        target_columns(),
        vec![(IN_COMMIT_TIMESTAMPS_PROPERTY.into(), "true".into())],
    )
    .await?;
    let args = ChangeArgs::new(
        vec![key("id")?],
        attribute("version"),
        ScdType::Type1,
        Some(eq_true("is_delete")),
        Some(ColumnSelection::Exclude(vec![key("is_delete")?])),
    )?;
    let checkpoint = warehouse(&ctx)?.join("_checkpoints_test");
    std::fs::create_dir_all(&checkpoint)?;
    Ok(Fixture {
        ctx,
        config,
        args,
        checkpoint,
        scope: TaskResourceScope::new(Handle::current()),
    })
}

impl Fixture {
    async fn run<S: Store + 'static>(
        &self,
        source: &DeterministicSource,
        control: &Arc<Control<S>>,
        owner: &str,
        hook: Option<&(dyn Fn(Stage, u64) -> SparkResult<()> + Send + Sync)>,
    ) -> SparkResult<DurableOutcome> {
        let (graph, flow, relations, source_name, target) = (
            graph(),
            flow(),
            std::collections::BTreeMap::new(),
            source_name(),
            target(),
        );
        let write = Scd1Write {
            flow_name: "test_catalog.test_db.auto_cdc_flow",
            destination: &target,
            destination_format: Some("delta"),
            args: &self.args,
            sequencing_type: &sail_common::spec::DataType::Int64,
            case: Case::Insensitive,
        };
        let run = FlowRun {
            graph: &graph,
            flow: &flow,
            relations: &relations,
            source_name: &source_name,
            source,
            write: &write,
            checkpoint: &self.checkpoint,
            max_rows_per_partition: 1,
        };
        run_durable_flow(
            &self.ctx,
            self.config.clone(),
            &self.scope,
            &DurableRun {
                run: &run,
                control: control.clone(),
                owner,
                lease_ms: LEASE,
                stage_hook: hook,
            },
        )
        .await
    }

    async fn assert_exact(&self) -> TestResult {
        assert_eq!(
            rows(&self.ctx, self.config.clone(), &target()).await?,
            vec![row(&["2", "bob", "1", &meta("NULL", "1")])]
        );
        assert_eq!(
            rows(&self.ctx, self.config.clone(), &aux()).await?,
            vec![row(&["1", &meta("2", "NULL")])]
        );
        Ok(())
    }
}

fn aux() -> Name {
    auxiliary::table_name(&target())
}

fn crash_at(stage: Stage, sequence: u64) -> impl Fn(Stage, u64) -> SparkResult<()> + Send + Sync {
    move |reached, at| {
        if reached == stage && at == sequence {
            Err(SparkError::internal("simulated crash"))
        } else {
            Ok(())
        }
    }
}

async fn assert_heads_match_the_logs<S: Store>(case: &Fixture, control: &Control<S>) -> TestResult {
    let key = TargetKey(target().join("."));
    let guard = control.guard(&key).await?.ok_or("no guard")?;
    assert_eq!(guard.active_intent, None);
    for (name, sink) in [(target(), guard.sink), (aux(), guard.auxiliary)] {
        let dir = table_dir(&case.ctx, &name)?;
        let head = control.head(&sink).await?.ok_or("no head")?;
        assert_eq!(head.version, delta_version(&dir)?, "{name:?}");
    }
    let flow = FlowKey(flow_name().join("."));
    let checkpoint = control.checkpoint(&key, 0, &flow).await?;
    assert_eq!(checkpoint.map(|c| c.next_batch_sequence), Some(3));
    Ok(())
}

async fn crash_and_recover(stage: Stage) -> TestResult {
    let case = setup().await?;
    let source = source();
    let clock = Arc::new(ManualClock::default());
    let control = Arc::new(Control::new(UnfencedMemoryStore::new(), clock.clone()));
    let hook = crash_at(stage, 1);
    let crashed = case.run(&source, &control, "w1", Some(&hook)).await;
    assert!(crashed.is_err(), "{stage:?} must stop the run");
    // Once archived, batch 1's rows no longer depend on the source.
    if stage != Stage::Sealed {
        source.expire_before("p0", 2);
    }
    clock.0.store(LEASE + 1, Ordering::SeqCst);
    let recovered = case.run(&source, &control, "w2", None).await?;
    assert_eq!(
        recovered,
        if stage == Stage::Completed {
            DurableOutcome {
                batches: 1,
                recovered: None,
            }
        } else {
            DurableOutcome {
                batches: 2,
                recovered: Some(1),
            }
        },
        "{stage:?}"
    );
    case.assert_exact().await?;
    assert_heads_match_the_logs(&case, &control).await?;
    finish(&case.ctx).await?;
    Ok(())
}

#[tokio::test]
async fn a_clean_run_completes_every_batch() -> TestResult {
    let case = setup().await?;
    let control = Arc::new(Control::new(
        UnfencedMemoryStore::new(),
        Arc::new(ManualClock::default()),
    ));
    let outcome = case.run(&source(), &control, "w1", None).await?;
    assert_eq!(outcome.batches, 3);
    case.assert_exact().await?;
    assert_heads_match_the_logs(&case, &control).await?;
    assert_eq!(case.run(&source(), &control, "w1", None).await?.batches, 0);
    finish(&case.ctx).await?;
    Ok(())
}

#[tokio::test]
async fn a_crash_after_sealing_recovers() -> TestResult {
    crash_and_recover(Stage::Sealed).await
}

#[tokio::test]
async fn a_crash_after_archiving_recovers_without_the_source() -> TestResult {
    crash_and_recover(Stage::Prepared).await
}

#[tokio::test]
async fn a_crash_after_the_auxiliary_effect_does_not_repeat_it() -> TestResult {
    crash_and_recover(Stage::AuxiliaryDone).await
}

#[tokio::test]
async fn a_crash_after_both_effects_completes_the_batch() -> TestResult {
    crash_and_recover(Stage::TargetDone).await
}

#[tokio::test]
async fn a_crash_after_completion_moves_on() -> TestResult {
    crash_and_recover(Stage::Completed).await
}

/// A crash between ratification and publication: the log misses an entry the
/// authority ratified. Recovery publishes it before planning anything else.
#[tokio::test]
async fn a_lost_publication_is_recovered_from_the_authority() -> TestResult {
    let case = setup().await?;
    let source = source();
    let clock = Arc::new(ManualClock::default());
    let control = Arc::new(Control::new(UnfencedMemoryStore::new(), clock.clone()));
    let hook = crash_at(Stage::AuxiliaryDone, 1);
    case.run(&source, &control, "w1", Some(&hook))
        .await
        .err()
        .ok_or("the run must stop")?;
    let dir = table_dir(&case.ctx, &aux())?;
    let latest = delta_version(&dir)?;
    std::fs::remove_file(dir.join("_delta_log").join(format!("{latest:020}.json")))?;
    clock.0.store(LEASE + 1, Ordering::SeqCst);
    case.run(&source, &control, "w2", None).await?;
    case.assert_exact().await?;
    assert_heads_match_the_logs(&case, &control).await?;
    finish(&case.ctx).await?;
    Ok(())
}

/// The log lost an entry the authority ratified, and what is left of it in
/// staging is no longer what was ratified. Publishing it would put unratified
/// actions in the log, so recovery stops until the original bytes are back.
#[tokio::test]
async fn a_changed_staged_entry_is_not_published() -> TestResult {
    let case = setup().await?;
    let source = source();
    let clock = Arc::new(ManualClock::default());
    let control = Arc::new(Control::new(UnfencedMemoryStore::new(), clock.clone()));
    let hook = crash_at(Stage::AuxiliaryDone, 1);
    case.run(&source, &control, "w1", Some(&hook))
        .await
        .err()
        .ok_or("the run must stop")?;
    let dir = table_dir(&case.ctx, &aux())?;
    let latest = delta_version(&dir)?;
    let log = dir.join("_delta_log");
    let published = log.join(format!("{latest:020}.json"));
    let ratified = std::fs::read(&published)?;
    let mut staged = None;
    for entry in std::fs::read_dir(log.join("_staged_commits"))? {
        let path = entry?.path();
        if std::fs::read(&path)? == ratified {
            staged = Some(path);
        }
    }
    let staged = staged.ok_or("no staged entry holds the ratified bytes")?;
    let mut changed = ratified.clone();
    changed.push(b'\n');
    std::fs::write(&staged, &changed)?;
    std::fs::remove_file(&published)?;
    clock.0.store(LEASE + 1, Ordering::SeqCst);

    let error = case
        .run(&source, &control, "w2", None)
        .await
        .err()
        .ok_or("recovery must refuse the changed entry")?;
    assert!(
        error
            .to_string()
            .contains("not the one the authority ratified"),
        "{error}"
    );
    assert!(!published.exists(), "nothing was published");

    std::fs::write(&staged, &ratified)?;
    clock.0.store(2 * (LEASE + 1), Ordering::SeqCst);
    case.run(&source, &control, "w3", None).await?;
    case.assert_exact().await?;
    assert_heads_match_the_logs(&case, &control).await?;
    finish(&case.ctx).await?;
    Ok(())
}

#[tokio::test]
async fn a_changed_archive_stops_the_run() -> TestResult {
    let case = setup().await?;
    let source = source();
    let clock = Arc::new(ManualClock::default());
    let control = Arc::new(Control::new(UnfencedMemoryStore::new(), clock.clone()));
    let hook = crash_at(Stage::Prepared, 1);
    case.run(&source, &control, "w1", Some(&hook))
        .await
        .err()
        .ok_or("the run must stop")?;
    let archive = case.checkpoint.join("prepared").join("1.arrow");
    let mut bytes = std::fs::read(&archive)?;
    bytes.push(0);
    std::fs::write(&archive, bytes)?;
    clock.0.store(LEASE + 1, Ordering::SeqCst);
    let refused = case.run(&source, &control, "w2", None).await;
    let message = format!("{:?}", refused.err().ok_or("the run must stop")?);
    assert!(message.contains("PreparedMismatch"), "{message}");
    finish(&case.ctx).await?;
    Ok(())
}

#[tokio::test]
async fn a_recreated_table_is_not_adopted() -> TestResult {
    let case = setup().await?;
    let control = Arc::new(Control::new(
        UnfencedMemoryStore::new(),
        Arc::new(ManualClock::default()),
    ));
    let dir = table_dir(&case.ctx, &target())?;
    control
        .enroll(
            &TargetKey(target().join(".")),
            &table_url(&dir)?,
            &SinkIdentity("an-earlier-incarnation".into()),
            &SinkIdentity("an-earlier-auxiliary".into()),
            (0, 0),
        )
        .await?;
    let refused = case.run(&source(), &control, "w1", None).await;
    let message = format!("{:?}", refused.err().ok_or("the run must stop")?);
    assert!(message.contains("IdentityMismatch"), "{message}");
    finish(&case.ctx).await?;
    Ok(())
}

/// The worked recovery trace end to end: real Delta tables, the Turso
/// authority reopened from disk, and the auxiliary effect not repeated.
#[tokio::test]
async fn the_recovery_trace_survives_an_authority_restart() -> TestResult {
    let case = setup().await?;
    let dir = tempfile::TempDir::new()?;
    let path = dir.path().join("authority.db").display().to_string();
    let source = source();
    let clock = Arc::new(ManualClock::default());
    let control = Arc::new(Control::new(
        TursoStore::open(&path, "auth").await?,
        clock.clone(),
    ));
    let hook = crash_at(Stage::AuxiliaryDone, 1);
    case.run(&source, &control, "w1", Some(&hook))
        .await
        .err()
        .ok_or("the run must stop")?;
    let intent = IntentId::new(
        &TargetKey(target().join(".")),
        0,
        &FlowKey(flow_name().join(".")),
        1,
    );
    let before = control.receipt(&intent, EffectStep::Auxiliary).await?;
    assert!(matches!(
        before.as_ref().map(|r| &r.kind),
        Some(ReceiptKind::Data { .. })
    ));
    drop(control);

    source.expire_before("p0", 2);
    clock.0.store(LEASE + 1, Ordering::SeqCst);
    let control = Arc::new(Control::new(
        TursoStore::open(&path, "ignored").await?,
        clock,
    ));
    let recovered = case.run(&source, &control, "w2", None).await?;
    assert_eq!(recovered.recovered, Some(1));
    assert_eq!(
        control.receipt(&intent, EffectStep::Auxiliary).await?,
        before,
        "the auxiliary effect is not repeated"
    );
    case.assert_exact().await?;
    assert_heads_match_the_logs(&case, &control).await?;
    finish(&case.ctx).await?;
    Ok(())
}
