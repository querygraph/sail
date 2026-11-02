//! Fenced SCD1 merges against real Delta tables: the commit stages its log
//! entry, the authority ratifies it, and only a new ratification publishes.

use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicI64, Ordering};

use datafusion::logical_expr::LogicalPlan;
use datafusion::prelude::SessionContext;
use sail_common::spec;
use sail_common_datafusion::extension::SessionExtensionAccessor;
use sail_common_datafusion::fenced_commit::FencedCommits;
use sail_plan::config::PlanConfig;
use sail_sdp_control::control::{Control, RequestId};
use sail_sdp_control::identity::{
    EffectStep, FenceToken, FlowKey, IntentId, SinkIdentity, TargetKey,
};
use sail_sdp_control::memory::UnfencedMemoryStore;
use sail_sdp_control::records::{FlowCheckpoint, Intent, IntentKind, ReceiptKind};
use sail_sdp_control::store::Clock;
use serde_json::json;

use super::super::authority::{ControlCommits, effect_step_name};
use super::super::merge::{auxiliary_merge, target_merge};
use super::super::schema::Case;
use super::merge_execution::{
    VIEW, aux_name, create_seeded, create_seeded_with, id_key, minimal, minimal_columns, publish,
    target_columns, target_name, valued,
};
use super::write::{context, finish, meta, row, rows};
use crate::pipelines::execution::run_command;
use crate::pipelines::graph::Name;

type TestResult = Result<(), Box<dyn std::error::Error>>;

const LEASE: i64 = 10_000;
const PREPARED: &str = "prepared";

#[derive(Default)]
struct ManualClock(AtomicI64);

impl Clock for ManualClock {
    fn now(&self) -> i64 {
        self.0.load(Ordering::SeqCst)
    }
}

struct Enrolled {
    ctx: SessionContext,
    config: Arc<PlanConfig>,
    control: Arc<Control<UnfencedMemoryStore>>,
    clock: Arc<ManualClock>,
    token: FenceToken,
    intent: IntentId,
    target: TargetKey,
    sink: SinkIdentity,
    auxiliary: SinkIdentity,
    sink_dir: PathBuf,
    auxiliary_dir: PathBuf,
}

pub(super) fn table_dir(
    ctx: &SessionContext,
    name: &Name,
) -> Result<PathBuf, Box<dyn std::error::Error>> {
    fn find(dir: &Path, leaf: &str) -> Option<PathBuf> {
        for entry in std::fs::read_dir(dir).ok()?.flatten() {
            let path = entry.path();
            if !path.is_dir() {
                continue;
            }
            let named = path
                .file_name()
                .is_some_and(|f| f.to_string_lossy().starts_with(&format!("{leaf}-")));
            if named && path.join("_delta_log").exists() {
                return Some(path);
            }
            // An enrolled auxiliary table lives beside its target, one
            // directory for each generation.
            if leaf.starts_with("__spark_autocdc_aux_state_")
                && path.to_string_lossy().ends_with(".autocdc-aux")
            {
                let latest = std::fs::read_dir(&path)
                    .ok()?
                    .flatten()
                    .filter_map(|entry| {
                        let generation: u64 = entry.file_name().to_str()?.parse().ok()?;
                        Some((generation, entry.path()))
                    })
                    .max_by_key(|(generation, _)| *generation);
                if let Some((_, table)) = latest {
                    return Some(table);
                }
            }
            if let Some(found) = find(&path, leaf) {
                return Some(found);
            }
        }
        None
    }
    let config = ctx
        .extension::<crate::session::SparkSession>()?
        .plan_config()?;
    let leaf = name.last().ok_or("empty name")?;
    let warehouse = config
        .default_warehouse_directory
        .trim_start_matches("file://")
        .to_string();
    find(Path::new(&warehouse), leaf).ok_or_else(|| format!("no table dir for {leaf}").into())
}

pub(super) fn table_url(dir: &Path) -> Result<String, Box<dyn std::error::Error>> {
    Ok(url::Url::from_directory_path(dir)
        .map_err(|()| "not absolute")?
        .to_string())
}

/// The newest published version.
pub(super) fn delta_version(dir: &Path) -> Result<u64, Box<dyn std::error::Error>> {
    let mut latest = 0;
    for entry in std::fs::read_dir(dir.join("_delta_log"))? {
        let name = entry?.file_name().to_string_lossy().into_owned();
        if let Some(version) = name.strip_suffix(".json") {
            latest = latest.max(version.parse()?);
        }
    }
    Ok(latest)
}

/// Ratified staging needs a timestamp inside the commit.
fn enrolled_properties() -> Vec<(String, String)> {
    vec![("delta.enableInCommitTimestamps".into(), "true".into())]
}

async fn enrolled() -> Result<Enrolled, Box<dyn std::error::Error>> {
    let (ctx, config) = context()?;
    create_seeded_with(
        &ctx,
        config.clone(),
        &aux_name(),
        minimal_columns(),
        Some(minimal(&[(1, (Some(10), None))])?),
        enrolled_properties(),
    )
    .await?;
    create_seeded_with(
        &ctx,
        config.clone(),
        &target_name(),
        target_columns(),
        Some(valued(&[(1, "seed", (None, Some(1)))])?),
        enrolled_properties(),
    )
    .await?;
    let (sink_dir, auxiliary_dir) = (
        table_dir(&ctx, &target_name())?,
        table_dir(&ctx, &aux_name())?,
    );
    let clock = Arc::new(ManualClock::default());
    let control = Arc::new(Control::new(UnfencedMemoryStore::new(), clock.clone()));
    let (target, sink, auxiliary) = (
        TargetKey("t".into()),
        SinkIdentity("delta:target".into()),
        SinkIdentity("delta:auxiliary".into()),
    );
    let flow = FlowKey("flow".into());
    control
        .enroll(
            &target,
            &table_url(&sink_dir)?,
            &sink,
            &auxiliary,
            (delta_version(&sink_dir)?, delta_version(&auxiliary_dir)?),
        )
        .await?;
    // The auxiliary table is a physical table of the same target group.
    control
        .bind_physical(&target, &table_url(&auxiliary_dir)?)
        .await?;
    control
        .put_checkpoint(&FlowCheckpoint {
            target: target.clone(),
            generation: 0,
            flow: flow.clone(),
            definition_digest: "def".into(),
            next_batch_sequence: 0,
            progress: json!({"offset": 0}),
            progress_digest: "p0".into(),
        })
        .await?;
    ctx.state_ref()
        .write()
        .config_mut()
        .set_extension(Arc::new(FencedCommits(Arc::new(ControlCommits::new(
            control.clone(),
            None,
        )))));
    let token = control.acquire(&target, "w1", LEASE).await?.token;
    let intent = Intent {
        id: IntentId::new(&target, 0, &flow, 0),
        kind: IntentKind::Batch,
        target: target.clone(),
        generation: 0,
        flow,
        batch_sequence: 0,
        request: json!({"range": [0, 1]}),
        request_digest: "request".into(),
        start_progress_digest: "p0".into(),
        end_progress: json!({"offset": 1}),
        end_progress_digest: "p1".into(),
        prepared_digest: None,
    };
    let sealed = control.seal_intent(&token, &intent).await?;
    control
        .record_prepared(&token, &sealed.id, PREPARED)
        .await?;
    Ok(Enrolled {
        ctx,
        config,
        control,
        clock,
        token,
        intent: sealed.id,
        target,
        sink,
        auxiliary,
        sink_dir,
        auxiliary_dir,
    })
}

impl Enrolled {
    fn fence(
        &self,
        token: &FenceToken,
        step: EffectStep,
        request: &str,
    ) -> Result<spec::FencedWrite, Box<dyn std::error::Error>> {
        Ok(spec::FencedWrite {
            authority: token.authority.0.clone(),
            target: token.target.0.clone(),
            generation: token.generation,
            epoch: token.epoch,
            owner: token.owner.clone(),
            intent: self.intent.0.clone(),
            step: effect_step_name(step)?,
            request_id: request.to_string(),
            request_digest: format!("d:{request}"),
            prepared_digest: PREPARED.to_string(),
        })
    }

    async fn merge_aux(
        &self,
        batch: LogicalPlan,
        fence: Option<spec::FencedWrite>,
    ) -> Result<(), crate::error::SparkError> {
        publish(&self.ctx, batch).await?;
        let mut merge = auxiliary_merge(&aux_name(), VIEW, &id_key()?);
        merge.fenced_write = fence;
        run_command(
            &self.ctx,
            self.config.clone(),
            spec::CommandNode::MergeInto(merge),
            None,
        )
        .await
    }

    async fn merge_target(
        &self,
        batch: LogicalPlan,
        fence: Option<spec::FencedWrite>,
    ) -> Result<(), crate::error::SparkError> {
        let columns: Vec<String> = batch
            .schema()
            .fields()
            .iter()
            .map(|f| f.name().clone())
            .collect();
        publish(&self.ctx, batch).await?;
        let mut merge = target_merge(
            &target_name(),
            VIEW,
            &id_key()?,
            &columns,
            Case::Insensitive,
        );
        merge.fenced_write = fence;
        run_command(
            &self.ctx,
            self.config.clone(),
            spec::CommandNode::MergeInto(merge),
            None,
        )
        .await
    }
}

#[tokio::test]
async fn fenced_merges_ratify_publish_and_complete_the_batch() -> TestResult {
    let f = enrolled().await?;
    let start = delta_version(&f.auxiliary_dir)?;
    let fence = f.fence(&f.token, EffectStep::Auxiliary, "aux-0")?;
    f.merge_aux(valued(&[(1, "x", (Some(20), None))])?, Some(fence))
        .await?;
    assert_eq!(delta_version(&f.auxiliary_dir)?, start + 1);
    assert_eq!(
        f.control.head(&f.auxiliary).await?.map(|h| h.version),
        Some(start + 1)
    );
    let receipt = f.control.receipt(&f.intent, EffectStep::Auxiliary).await?;
    assert_eq!(
        receipt.map(|r| r.kind),
        Some(ReceiptKind::Data {
            ratified_version: start + 1
        })
    );
    let commit = f.control.ratified_commit(&f.auxiliary, start + 1).await?;
    let staged = commit.ok_or("no ratified commit")?.staged_path;
    assert!(staged.contains("_staged_commits"), "{staged}");
    assert_eq!(
        rows(&f.ctx, f.config.clone(), &aux_name()).await?,
        vec![row(&["1", &meta("20", "NULL")])]
    );

    let fence = f.fence(&f.token, EffectStep::Target, "target-0")?;
    f.merge_target(valued(&[(2, "new", (None, Some(20)))])?, Some(fence))
        .await?;
    assert_eq!(
        rows(&f.ctx, f.config.clone(), &target_name()).await?.len(),
        2
    );
    assert_eq!(
        f.control.head(&f.sink).await?.map(|h| h.version),
        Some(delta_version(&f.sink_dir)?)
    );
    let done = f
        .control
        .complete_batch(
            &RequestId {
                id: "complete-0".into(),
                digest: "d".into(),
            },
            &f.token,
            &f.intent,
        )
        .await?;
    assert_eq!(done.checkpoint.next_batch_sequence, 1);
    finish(&f.ctx).await?;
    Ok(())
}

#[tokio::test]
async fn a_stale_owner_cannot_publish() -> TestResult {
    let f = enrolled().await?;
    let before = rows(&f.ctx, f.config.clone(), &aux_name()).await?;
    let version = delta_version(&f.auxiliary_dir)?;
    f.clock.0.store(LEASE + 1, Ordering::SeqCst);
    f.control.acquire(&f.target, "w2", LEASE).await?;
    let fence = f.fence(&f.token, EffectStep::Auxiliary, "aux-0")?;
    let refused = f
        .merge_aux(valued(&[(1, "x", (Some(20), None))])?, Some(fence))
        .await;
    let message = format!("{:?}", refused.err().ok_or("the merge must be refused")?);
    assert!(message.contains("Fenced"), "{message}");
    assert_eq!(delta_version(&f.auxiliary_dir)?, version);
    assert_eq!(rows(&f.ctx, f.config.clone(), &aux_name()).await?, before);
    assert_eq!(
        f.control.head(&f.auxiliary).await?.map(|h| h.version),
        Some(version)
    );
    finish(&f.ctx).await?;
    Ok(())
}

#[tokio::test]
async fn an_enrolled_table_refuses_an_unfenced_merge() -> TestResult {
    let f = enrolled().await?;
    let version = delta_version(&f.auxiliary_dir)?;
    let refused = f
        .merge_aux(valued(&[(1, "x", (Some(20), None))])?, None)
        .await;
    let message = format!("{:?}", refused.err().ok_or("the merge must be refused")?);
    assert!(message.contains("enrolled"), "{message}");
    assert_eq!(delta_version(&f.auxiliary_dir)?, version);
    finish(&f.ctx).await?;
    Ok(())
}

#[tokio::test]
async fn a_fenced_merge_needs_an_authority() -> TestResult {
    let (ctx, config) = context()?;
    create_seeded(
        &ctx,
        config.clone(),
        &aux_name(),
        minimal_columns(),
        Some(minimal(&[(1, (Some(10), None))])?),
    )
    .await?;
    publish(&ctx, valued(&[(1, "x", (Some(20), None))])?).await?;
    let mut merge = auxiliary_merge(&aux_name(), VIEW, &id_key()?);
    merge.fenced_write = Some(spec::FencedWrite {
        authority: "a".into(),
        target: "t".into(),
        generation: 0,
        epoch: 1,
        owner: "w".into(),
        intent: "i".into(),
        step: "Auxiliary".into(),
        request_id: "r".into(),
        request_digest: "d".into(),
        prepared_digest: "p".into(),
    });
    let refused = run_command(&ctx, config, spec::CommandNode::MergeInto(merge), None).await;
    let message = format!("{:?}", refused.err().ok_or("the merge must be refused")?);
    assert!(message.contains("commit authority"), "{message}");
    finish(&ctx).await?;
    Ok(())
}

#[tokio::test]
async fn an_empty_merge_records_a_noop_receipt() -> TestResult {
    let f = enrolled().await?;
    let version = delta_version(&f.auxiliary_dir)?;
    let fence = f.fence(&f.token, EffectStep::Auxiliary, "aux-0")?;
    f.merge_aux(valued(&[])?, Some(fence)).await?;
    assert_eq!(delta_version(&f.auxiliary_dir)?, version);
    let receipt = f.control.receipt(&f.intent, EffectStep::Auxiliary).await?;
    assert_eq!(
        receipt.map(|r| r.kind),
        Some(ReceiptKind::Noop {
            checked_head: version
        })
    );
    finish(&f.ctx).await?;
    Ok(())
}

/// A retry of a request whose response was lost replays the receipt, and its
/// own staged entry is not published: the log gains one version, not two.
#[tokio::test]
async fn a_replayed_merge_does_not_publish_again() -> TestResult {
    let f = enrolled().await?;
    let start = delta_version(&f.auxiliary_dir)?;
    let fence = f.fence(&f.token, EffectStep::Auxiliary, "aux-0")?;
    f.merge_aux(valued(&[(1, "x", (Some(20), None))])?, Some(fence.clone()))
        .await?;
    f.merge_aux(valued(&[(1, "x", (Some(30), None))])?, Some(fence))
        .await?;
    assert_eq!(delta_version(&f.auxiliary_dir)?, start + 1);
    assert_eq!(
        rows(&f.ctx, f.config.clone(), &aux_name()).await?,
        vec![row(&["1", &meta("20", "NULL")])]
    );
    finish(&f.ctx).await?;
    Ok(())
}
