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
use super::super::write::{IN_COMMIT_TIMESTAMPS_PROPERTY, Scd1Write, drop_table};
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
    let checkpoint = warehouse(&ctx)?.join("_checkpoints_test").join("0");
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
        self.run_with(source, control, owner, hook, false).await
    }

    async fn run_with<S: Store + 'static>(
        &self,
        source: &DeterministicSource,
        control: &Arc<Control<S>>,
        owner: &str,
        hook: Option<&(dyn Fn(Stage, u64) -> SparkResult<()> + Send + Sync)>,
        reset: bool,
    ) -> SparkResult<DurableOutcome> {
        self.run_leased(source, control, owner, hook, reset, LEASE)
            .await
    }

    async fn run_leased<S: Store + 'static>(
        &self,
        source: &DeterministicSource,
        control: &Arc<Control<S>>,
        owner: &str,
        hook: Option<&(dyn Fn(Stage, u64) -> SparkResult<()> + Send + Sync)>,
        reset: bool,
        lease_ms: i64,
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
            flow_source: None,
        };
        run_durable_flow(
            &self.ctx,
            self.config.clone(),
            &self.scope,
            &DurableRun {
                run: &run,
                control: control.clone(),
                owner,
                lease_ms,
                lease_wait_ms: 0,
                reset,
                stage_hook: hook,
                lease_lost: Default::default(),
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
    let generation = control.guard(&key).await?.ok_or("no guard")?.generation;
    let checkpoint = control.checkpoint(&key, generation, &flow).await?;
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
                reset: None,
            }
        } else {
            DurableOutcome {
                batches: 2,
                recovered: Some(1),
                reset: None,
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

/// A batch that outlives the lease: the hook holds the run at Prepared for
/// longer than the lease while the authority's clock passes the deadline.
/// With renewal the deadline moves and the merge is ratified; without it the
/// run fences itself.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_long_batch_renews_its_lease() -> TestResult {
    let case = setup().await?;
    let source = source();
    let clock = Arc::new(ManualClock::default());
    let control = Arc::new(Control::new(UnfencedMemoryStore::new(), clock.clone()));
    let lease_ms = 300;
    let slow = {
        let clock = clock.clone();
        let control = control.clone();
        move |stage: Stage, sequence: u64| {
            if stage == Stage::Prepared && sequence == 1 {
                // The clock passes the original deadline in steps shorter
                // than the lease, as wall time would; each step waits for a
                // renewal to move the deadline past it, as it would in time.
                for _ in 0..4 {
                    let now = clock.0.fetch_add(lease_ms / 3, Ordering::SeqCst) + lease_ms / 3;
                    let waited = std::time::Instant::now();
                    loop {
                        let deadline = tokio::task::block_in_place(|| {
                            Handle::current().block_on(control.guard(&target_key()))
                        })
                        .ok()
                        .flatten()
                        .and_then(|guard| guard.lease_deadline)
                        .unwrap_or_default();
                        if deadline >= now + lease_ms {
                            break;
                        }
                        if waited.elapsed() > std::time::Duration::from_secs(10) {
                            return Err(SparkError::internal("the lease was not renewed"));
                        }
                        std::thread::sleep(std::time::Duration::from_millis(10));
                    }
                }
            }
            Ok(())
        }
    };
    let outcome = case
        .run_leased(&source, &control, "w1", Some(&slow), false, lease_ms)
        .await?;
    assert_eq!(outcome.batches, 3);
    case.assert_exact().await?;
    assert_heads_match_the_logs(&case, &control).await?;
    finish(&case.ctx).await?;
    Ok(())
}

/// A target dropped and created again outside the pipeline: an ordinary run
/// refuses it and names the repair; a full refresh adopts the new table under
/// the reset, and the fenced truncate runs against its log.
#[tokio::test]
async fn a_recreated_target_needs_a_full_refresh_which_adopts_it() -> TestResult {
    let case = setup().await?;
    let control = Arc::new(Control::new(
        UnfencedMemoryStore::new(),
        Arc::new(ManualClock::default()),
    ));
    case.run(&source(), &control, "w1", None).await?;
    let before = control.guard(&target_key()).await?.ok_or("no guard")?;
    drop_table(&case.ctx, case.config.clone(), &target()).await?;
    create_target_with(
        &case.ctx,
        case.config.clone(),
        &target(),
        "delta",
        target_columns(),
        vec![(IN_COMMIT_TIMESTAMPS_PROPERTY.into(), "true".into())],
    )
    .await?;
    let refused = case.run(&source(), &control, "w1", None).await;
    let message = refused
        .err()
        .ok_or("a replaced target must be refused")?
        .to_string();
    assert!(message.contains("run a full refresh"), "{message}");
    let outcome = case.run_with(&source(), &control, "w1", None, true).await?;
    assert_eq!((outcome.batches, outcome.reset), (3, Some(1)));
    case.assert_exact().await?;
    let guard = control.guard(&target_key()).await?.ok_or("no guard")?;
    assert_ne!(guard.sink, before.sink, "the guard names the new table");
    // The dropped table's directory may remain beside the new one, so the
    // guard's own location is the one whose log the head must match.
    let dir = url::Url::parse(&guard.physical_key)?
        .to_file_path()
        .map_err(|()| "the target is not a local path")?;
    let head = control.head(&guard.sink).await?.ok_or("no head")?;
    assert_eq!(head.version, delta_version(&dir)?);
    assert_eq!(guard.active_intent, None);
    finish(&case.ctx).await?;
    Ok(())
}

#[tokio::test]
async fn a_failed_batch_releases_the_lease_for_the_next_owner() -> TestResult {
    let case = setup().await?;
    let source = source();
    let clock = Arc::new(ManualClock::default());
    let control = Arc::new(Control::new(UnfencedMemoryStore::new(), clock.clone()));
    let hook = crash_at(Stage::Prepared, 1);
    let failed = case.run(&source, &control, "w1", Some(&hook)).await;
    assert!(failed.is_err());
    let guard = control
        .guard(&TargetKey(target().join(".")))
        .await?
        .ok_or("no guard")?;
    assert_eq!(guard.owner, None, "a failed batch hands the target back");
    // The clock has not moved: a new owner recovers without waiting the
    // lease out, and the harness waits 0 ms for one.
    source.expire_before("p0", 2);
    let recovered = case.run(&source, &control, "w2", None).await?;
    assert_eq!(recovered.recovered, Some(1));
    case.assert_exact().await?;
    assert_heads_match_the_logs(&case, &control).await?;
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
    assert!(message.contains("run a full refresh"), "{message}");
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

fn target_key() -> sail_sdp_control::identity::TargetKey {
    sail_sdp_control::identity::TargetKey(target().join("."))
}

#[tokio::test]
async fn a_reset_rebuilds_the_tables_from_the_origin_under_a_new_generation() -> TestResult {
    let case = setup().await?;
    let control = Arc::new(Control::new(
        UnfencedMemoryStore::new(),
        Arc::new(ManualClock::default()),
    ));
    case.run(&source(), &control, "w1", None).await?;
    let before = control.guard(&target_key()).await?.ok_or("no guard")?;
    let outcome = case.run_with(&source(), &control, "w1", None, true).await?;
    assert_eq!((outcome.batches, outcome.reset), (3, Some(1)));
    case.assert_exact().await?;
    assert_heads_match_the_logs(&case, &control).await?;
    let guard = control.guard(&target_key()).await?.ok_or("no guard")?;
    assert_eq!((guard.generation, guard.checkpoint_index), (1, 1));
    // The target is replaced in place; the auxiliary table is dropped, as
    // `DatasetManager` does, which is how a flow's keys change.
    assert_eq!(guard.sink, before.sink);
    assert_ne!(guard.auxiliary, before.auxiliary);
    // Batch numbers restart, so the new generation archives beside the old one.
    let root = case.checkpoint.parent().ok_or("no parent")?;
    assert!(root.join("0").join("prepared").join("2.arrow").exists());
    assert!(root.join("1").join("prepared").join("2.arrow").exists());
    assert_eq!(case.run(&source(), &control, "w1", None).await?.batches, 0);
    finish(&case.ctx).await?;
    Ok(())
}

async fn crash_during_reset(stage: Stage) -> TestResult {
    let case = setup().await?;
    let clock = Arc::new(ManualClock::default());
    let control = Arc::new(Control::new(UnfencedMemoryStore::new(), clock.clone()));
    case.run(&source(), &control, "w1", None).await?;
    let hook = crash_at(stage, 0);
    let crashed = case
        .run_with(&source(), &control, "w1", Some(&hook), true)
        .await;
    assert!(crashed.is_err(), "{stage:?} must stop the run");
    assert_eq!(
        control.guard(&target_key()).await?.ok_or("no guard")?.state,
        sail_sdp_control::records::TargetState::Resetting
    );
    clock.0.store(LEASE + 1, Ordering::SeqCst);
    // The successor was not asked for a reset and finishes it anyway.
    let outcome = case.run(&source(), &control, "w2", None).await?;
    assert_eq!((outcome.batches, outcome.reset), (3, Some(1)), "{stage:?}");
    case.assert_exact().await?;
    assert_heads_match_the_logs(&case, &control).await?;
    finish(&case.ctx).await?;
    Ok(())
}

#[tokio::test]
async fn a_crash_after_beginning_a_reset_resumes_it() -> TestResult {
    crash_during_reset(Stage::ResetBegun).await
}

#[tokio::test]
async fn a_crash_between_the_reset_effects_resumes_it() -> TestResult {
    crash_during_reset(Stage::ResetTargetDone).await
}

#[tokio::test]
async fn a_reset_is_refused_before_any_effect_when_the_origin_is_gone() -> TestResult {
    let case = setup().await?;
    let control = Arc::new(Control::new(
        UnfencedMemoryStore::new(),
        Arc::new(ManualClock::default()),
    ));
    let source = source();
    case.run(&source, &control, "w1", None).await?;
    source.expire_before("p0", 1);
    let refused = case.run_with(&source, &control, "w1", None, true).await;
    let message = refused.err().ok_or("reset must be refused")?.to_string();
    assert!(
        message.contains("no longer retains its first position"),
        "{message}"
    );
    assert_eq!(
        control
            .guard(&target_key())
            .await?
            .ok_or("no guard")?
            .generation,
        0
    );
    case.assert_exact().await?;
    finish(&case.ctx).await?;
    Ok(())
}

#[tokio::test]
async fn a_reset_replaces_an_unfinished_batch_and_keeps_its_ratified_commit() -> TestResult {
    let case = setup().await?;
    let clock = Arc::new(ManualClock::default());
    let control = Arc::new(Control::new(UnfencedMemoryStore::new(), clock.clone()));
    let hook = crash_at(Stage::AuxiliaryDone, 1);
    assert!(
        case.run(&source(), &control, "w1", Some(&hook))
            .await
            .is_err()
    );
    // The batch's ratified commit never reached the log, so the head the
    // reset must build on exists only once that commit is published.
    let dir = table_dir(&case.ctx, &aux())?;
    let latest = delta_version(&dir)?;
    std::fs::remove_file(dir.join("_delta_log").join(format!("{latest:020}.json")))?;
    clock.0.store(LEASE + 1, Ordering::SeqCst);
    let outcome = case.run_with(&source(), &control, "w2", None, true).await?;
    assert_eq!(
        (outcome.batches, outcome.recovered, outcome.reset),
        (3, None, Some(1))
    );
    case.assert_exact().await?;
    assert_heads_match_the_logs(&case, &control).await?;
    finish(&case.ctx).await?;
    Ok(())
}

#[tokio::test]
async fn a_run_stopped_by_its_source_leaves_the_target_free_for_a_reset() -> TestResult {
    let case = setup().await?;
    let control = Arc::new(Control::new(
        UnfencedMemoryStore::new(),
        Arc::new(ManualClock::default()),
    ));
    let source = source();
    case.run(&source, &control, "w1", None).await?;
    source.create_partition("p0");
    assert!(case.run(&source, &control, "w2", None).await.is_err());
    // The clock has not moved, so w2's lease would still block this.
    let outcome = case.run_with(&source, &control, "w3", None, true).await?;
    assert_eq!((outcome.batches, outcome.reset), (0, Some(1)));
    assert!(
        rows(&case.ctx, case.config.clone(), &target())
            .await?
            .is_empty()
    );
    assert!(
        rows(&case.ctx, case.config.clone(), &aux())
            .await?
            .is_empty()
    );
    finish(&case.ctx).await?;
    Ok(())
}

#[tokio::test]
async fn a_crash_during_a_schema_change_resumes_it_and_advances_no_batch() -> TestResult {
    let mut case = setup().await?;
    let clock = Arc::new(ManualClock::default());
    let control = Arc::new(Control::new(UnfencedMemoryStore::new(), clock.clone()));
    case.run(&source(), &control, "w1", None).await?;
    // The flow stops excluding a column, so the target must gain it.
    case.args = ChangeArgs::new(
        vec![key("id")?],
        attribute("version"),
        ScdType::Type1,
        Some(eq_true("is_delete")),
        None,
    )?;
    let hook = crash_at(Stage::EvolvedTarget, 0);
    let crashed = case.run(&source(), &control, "w1", Some(&hook)).await;
    assert!(crashed.is_err(), "the run must stop");
    clock.0.store(LEASE + 1, Ordering::SeqCst);
    let outcome = case.run(&source(), &control, "w2", None).await?;
    assert_eq!(outcome.batches, 0);
    let guard = control.guard(&target_key()).await?.ok_or("no guard")?;
    assert_eq!((guard.active_intent, guard.generation), (None, 0));
    let plan = super::super::write::read_table(&case.ctx, case.config.clone(), &target()).await?;
    assert!(plan.schema().has_column_with_unqualified_name("is_delete"));
    assert_heads_match_the_logs(&case, &control).await?;
    finish(&case.ctx).await?;
    Ok(())
}
