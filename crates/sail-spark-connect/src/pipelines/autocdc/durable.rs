//! A durable SCD1 flow run over the control model: the two-table protocol of
//! `CDC-CONTROL-STORE-DESIGN.md`. Each batch is sealed before its first
//! effect, its reconciled rows are archived before any merge, and each merge
//! is a fenced effect with a stable request identity. A successor resumes an
//! unfinished batch from what the authority recorded and repeats only what
//! has no receipt.

use std::io::Cursor;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use datafusion::arrow::array::RecordBatch;
use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use datafusion::arrow::ipc::reader::FileReader;
use datafusion::arrow::ipc::writer::FileWriter;
use datafusion::prelude::SessionContext;
use sail_catalog::manager::CatalogManager;
use sail_common::spec;
use sail_common_datafusion::catalog::TableKind;
use sail_common_datafusion::extension::SessionExtensionAccessor;
use sail_common_datafusion::fenced_commit::{FencedCommitRegistry, FencedCommits};
use sail_common_datafusion::task_resources::TaskResourceScope;
use sail_delta_lake::fenced::{publish_ratified, table_state};
use sail_plan::config::PlanConfig;
use sail_plan::error::PlanError;
use sail_sdp_control::control::{Control, RequestId};
use sail_sdp_control::digest::digest;
use sail_sdp_control::identity::{
    EffectStep, FenceToken, FlowKey, IntentId, SinkIdentity, TargetKey,
};
use sail_sdp_control::records::{
    FlowCheckpoint, Intent, IntentKind, ReceiptKind, TargetGuard, TargetState,
};
use sail_sdp_control::store::Store;
use sail_sdp_control::transitions::{Outcome, ResetIdentities};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use url::Url;

use super::authority::{ControlCommits, effect_step_name};
use super::run::{FlowRun, flow_schema};
use super::state::StateKeys;
use super::write::{
    AuxiliaryPlacement, IN_COMMIT_TIMESTAMPS_PROPERTY, PreparedBatch, drop_table,
    ensure_auxiliary_table, evolve_fenced, flow_output_schema, mark_enrolled, merge_auxiliary,
    merge_destination, noop_fenced, prepare_microbatch, publish_prepared, read_table,
    replace_delta_with_view,
};
use crate::error::{SparkError, SparkResult};
use crate::pipelines::graph::Name;
use crate::pipelines::streaming::intent::{BatchIntent, Progress, RunFrontier, next_intent};
use crate::pipelines::streaming::source::BoundedSource;

/// Where a run stands, for the crash tests. Production runs pass no hook.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum Stage {
    Sealed,
    Prepared,
    AuxiliaryDone,
    TargetDone,
    Completed,
    /// `BeginReset` is durable and neither reset effect has run.
    ResetBegun,
    /// The target is truncated and the auxiliary table is not.
    ResetTargetDone,
    /// The target has its new columns and the initialization is unfinished.
    EvolvedTarget,
}

type StageHook<'a> = &'a (dyn Fn(Stage, u64) -> SparkResult<()> + Send + Sync);

pub(super) struct DurableRun<'a, S> {
    pub run: &'a FlowRun<'a>,
    pub control: Arc<Control<S>>,
    pub owner: &'a str,
    pub lease_ms: i64,
    /// How long to wait for an owner that died to lose its lease. A run that
    /// starts after a crash waits rather than fails.
    pub lease_wait_ms: i64,
    /// Rebuild the target from the source's origin before reading it.
    pub reset: bool,
    pub stage_hook: Option<StageHook<'a>>,
    /// Set by the lease renewal task when the lease could not be renewed, so
    /// the run stops at its next stage instead of walking into the fence.
    pub lease_lost: Arc<std::sync::Mutex<Option<String>>>,
}

/// Renews the run's lease every third of the lease while it lives; dropping
/// it stops the renewal.
struct LeaseRenewal(tokio::task::JoinHandle<()>);

impl Drop for LeaseRenewal {
    fn drop(&mut self) {
        self.0.abort();
    }
}

#[derive(Debug, Default, PartialEq, Eq)]
pub(in crate::pipelines) struct DurableOutcome {
    /// Batches this run completed.
    pub batches: u64,
    /// The unfinished batch it found and finished, if any.
    pub recovered: Option<u64>,
    /// The generation a reset finished by this run enabled, if any.
    pub reset: Option<u64>,
}

fn control_error(outcome: Outcome) -> SparkError {
    SparkError::internal(format!("control authority: {outcome}"))
}

fn encoding_error(error: impl std::fmt::Display) -> SparkError {
    SparkError::internal(format!("control record encoding: {error}"))
}

/// `checkpoint` is a `<flow>/<n>` directory. The authority owns `n`, so a
/// reset moves to the number it reserved, not the highest found on disk.
fn prepared_path(checkpoint: &Path, index: u64, sequence: u64) -> PathBuf {
    checkpoint
        .with_file_name(index.to_string())
        .join("prepared")
        .join(format!("{sequence}.arrow"))
}

/// Write the reconciled rows and return their digest. Written to a sibling
/// file and renamed, so a reader sees a whole archive or none.
fn archive(path: &Path, prepared: &PreparedBatch) -> SparkResult<String> {
    let mut bytes = Vec::new();
    {
        let mut writer = FileWriter::try_new(&mut bytes, &prepared.schema)?;
        for batch in &prepared.batches {
            writer.write(batch)?;
        }
        writer.finish()?;
    }
    let parent = path
        .parent()
        .ok_or_else(|| SparkError::internal("archive path has no parent"))?;
    std::fs::create_dir_all(parent)
        .map_err(|e| SparkError::internal(format!("create {}: {e}", parent.display())))?;
    let staged = path.with_extension("next");
    std::fs::write(&staged, &bytes)
        .map_err(|e| SparkError::internal(format!("write {}: {e}", staged.display())))?;
    std::fs::rename(&staged, path)
        .map_err(|e| SparkError::internal(format!("rename {}: {e}", staged.display())))?;
    Ok(content_digest(&bytes))
}

fn content_digest(bytes: &[u8]) -> String {
    Sha256::digest(bytes)
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect()
}

/// Read an archive back; another digest stops the run, never reruns it.
fn restore(path: &Path, expected: &str) -> SparkResult<PreparedBatch> {
    let bytes = std::fs::read(path).map_err(|e| {
        control_error(Outcome::EvidenceUnavailable {
            reason: format!("prepared archive {}: {e}", path.display()),
        })
    })?;
    if content_digest(&bytes) != expected {
        return Err(control_error(Outcome::PreparedMismatch {
            reason: format!("prepared archive {} changed", path.display()),
        }));
    }
    let reader = FileReader::try_new(Cursor::new(bytes), None)?;
    let schema: SchemaRef = reader.schema();
    let batches = reader.collect::<Result<Vec<RecordBatch>, _>>()?;
    let columns = schema
        .fields()
        .iter()
        .map(|field| field.name().clone())
        .collect();
    Ok(PreparedBatch {
        schema,
        batches,
        columns,
    })
}

/// The table's location as the URL its commits use.
async fn table_url(ctx: &SessionContext, name: &Name) -> SparkResult<Url> {
    let table = ctx
        .extension::<CatalogManager>()?
        .get_table_or_view(name)
        .await
        .map_err(PlanError::from)?;
    let TableKind::Table {
        location: Some(location),
        ..
    } = table.kind
    else {
        return Err(SparkError::internal(format!(
            "{} is not a table with a location",
            name.join(".")
        )));
    };
    // Resolve as Delta's table source does, so a relative location (the default
    // `spark-warehouse`) names the same directory against the working directory.
    sail_data_source::resolve_table_url(location.clone())
        .map_err(|e| SparkError::internal(format!("table location {location}: {e}")))
}

/// An empty batch with the destination's columns. Merging it writes a table's
/// first commit, which gives the table the identity enrollment binds.
async fn empty_batch(
    ctx: &SessionContext,
    config: Arc<PlanConfig>,
    destination: &Name,
) -> SparkResult<PreparedBatch> {
    let plan = read_table(ctx, config, destination).await?;
    let schema: SchemaRef = Arc::new(plan.schema().as_arrow().clone());
    let columns = schema
        .fields()
        .iter()
        .map(|field| field.name().clone())
        .collect();
    Ok(PreparedBatch {
        schema,
        batches: vec![],
        columns,
    })
}

/// One effect's table.
struct EffectTable {
    sink: SinkIdentity,
    url: Url,
}

struct Enrolled {
    target: TargetKey,
    flow: FlowKey,
    auxiliary_name: Name,
    auxiliary: EffectTable,
    destination: EffectTable,
}

fn version(head: i64) -> SparkResult<u64> {
    u64::try_from(head).map_err(|_| SparkError::internal("negative Delta version"))
}

/// Give the auxiliary table its first commit, if it has none. That table may
/// already be bound, from before it was dropped, and a first commit has no
/// head to fence.
async fn birth_auxiliary<S: Store + 'static>(
    ctx: &SessionContext,
    config: Arc<PlanConfig>,
    durable: &DurableRun<'_, S>,
    name: &Name,
    url: &Url,
) -> SparkResult<()> {
    if table_state(&ctx.task_ctx(), url).await?.is_some() {
        return Ok(());
    }
    let write = durable.run.write;
    let empty = empty_batch(ctx, config.clone(), write.destination).await?;
    publish_prepared(ctx, &empty).await?;
    let commits = ControlCommits::for_birth(durable.control.clone(), url.as_str());
    ctx.state_ref()
        .write()
        .config_mut()
        .set_extension(Arc::new(FencedCommits(Arc::new(commits))));
    let born = merge_auxiliary(ctx, config, write, name, None).await;
    durable.install(ctx);
    born
}

/// Bind the destination and auxiliary tables once. A recreated table has a
/// new identity, which enrollment refuses rather than adopts.
async fn enroll<S: Store + 'static>(
    ctx: &SessionContext,
    config: Arc<PlanConfig>,
    durable: &DurableRun<'_, S>,
    auxiliary_name: Name,
) -> SparkResult<Enrolled> {
    let destination_name = durable.run.write.destination;
    let target = TargetKey(destination_name.join("."));
    let flow = FlowKey(durable.run.flow.name.join("."));
    let (sink_url, auxiliary_url) = (
        table_url(ctx, destination_name).await?,
        table_url(ctx, &auxiliary_name).await?,
    );
    let task = ctx.task_ctx();
    let write = durable.run.write;
    let control = &durable.control;
    let guard = control.guard(&target).await.map_err(control_error)?;
    if table_state(&task, &sink_url).await?.is_none() {
        let empty = empty_batch(ctx, config.clone(), destination_name).await?;
        publish_prepared(ctx, &empty).await?;
        merge_destination(ctx, config.clone(), write, &empty, None).await?;
    }
    birth_auxiliary(
        ctx,
        config.clone(),
        durable,
        &auxiliary_name,
        &auxiliary_url,
    )
    .await?;
    if guard.is_none() {
        mark_enrolled(ctx, config, &auxiliary_name, &durable.control.authority().0).await?;
    }
    let born = async |url: &Url| {
        table_state(&task, url)
            .await?
            .ok_or_else(|| SparkError::internal(format!("table {url} has no commit")))
    };
    let (sink_id, sink_head) = born(&sink_url).await?;
    let (auxiliary_id, auxiliary_head) = born(&auxiliary_url).await?;
    let (sink, auxiliary) = (SinkIdentity(sink_id), SinkIdentity(auxiliary_id));
    let (sink, auxiliary) = match &guard {
        // A begun reset rebinds what it replaces when it finishes.
        Some(guard) if guard.state == TargetState::Resetting => {
            (guard.sink.clone(), guard.auxiliary.clone())
        }
        Some(guard)
            if guard.physical_key == sink_url.as_str()
                && guard.sink == sink
                && guard.auxiliary != auxiliary =>
        {
            let acquired = control
                .acquire(&target, durable.owner, durable.lease_ms)
                .await
                .map_err(control_error)?;
            control
                .rebind_auxiliary(&acquired.token, &auxiliary, version(auxiliary_head)?)
                .await
                .map_err(control_error)?;
            (sink, auxiliary)
        }
        // A target recreated outside the authority is adopted by an explicit
        // full refresh, which rebinds it under the reset; see `finish_reset`.
        Some(guard) if durable.reset => (guard.sink.clone(), guard.auxiliary.clone()),
        Some(guard) if guard.sink != sink || guard.physical_key != sink_url.as_str() => {
            return Err(SparkError::invalid(format!(
                "the AutoCDC target {} was replaced outside the pipeline; run a full refresh to \
                 enroll the new table",
                destination_name.join(".")
            )));
        }
        _ => {
            control
                .enroll(
                    &target,
                    sink_url.as_str(),
                    &sink,
                    &auxiliary,
                    (version(sink_head)?, version(auxiliary_head)?),
                )
                .await
                .map_err(control_error)?;
            (sink, auxiliary)
        }
    };
    control
        .bind_physical(&target, auxiliary_url.as_str())
        .await
        .map_err(control_error)?;
    let guard = control
        .guard(&target)
        .await
        .map_err(control_error)?
        .ok_or_else(|| SparkError::internal("enrolled target has no guard"))?;
    ensure_checkpoint(control, &target, guard.generation, &flow).await?;
    Ok(Enrolled {
        target,
        flow,
        auxiliary_name,
        auxiliary: EffectTable {
            sink: auxiliary,
            url: auxiliary_url,
        },
        destination: EffectTable {
            sink,
            url: sink_url,
        },
    })
}

/// Give the flow a checkpoint at the start of the source in `generation`,
/// unless it has one.
async fn ensure_checkpoint<S: Store>(
    control: &Control<S>,
    target: &TargetKey,
    generation: u64,
    flow: &FlowKey,
) -> SparkResult<()> {
    let existing = control
        .checkpoint(target, generation, flow)
        .await
        .map_err(control_error)?;
    if existing.is_some() {
        return Ok(());
    }
    let progress = Progress::default();
    control
        .put_checkpoint(&FlowCheckpoint {
            target: target.clone(),
            generation,
            flow: flow.clone(),
            definition_digest: digest("flow", &flow.0).map_err(encoding_error)?,
            next_batch_sequence: 0,
            progress_digest: digest("progress", &progress).map_err(encoding_error)?,
            progress: serde_json::to_value(&progress).map_err(encoding_error)?,
        })
        .await
        .map_err(control_error)
}

/// Refuse a reset the source cannot serve, before anything is fenced or
/// truncated: a new checkpoint is not permission to start at whatever the
/// source still retains.
fn check_reset_origin(source: &dyn BoundedSource) -> SparkResult<()> {
    for partition in source.describe()?.partitions {
        if !source.retains_origin(&partition.id)? {
            return Err(SparkError::invalid(format!(
                "full refresh needs the whole history of the source, and partition {} no longer \
                 retains its first position",
                partition.id
            )));
        }
    }
    Ok(())
}

fn schema_intent(
    enrolled: &Enrolled,
    generation: u64,
    checkpoint: &FlowCheckpoint,
    added: &[String],
) -> SparkResult<Intent> {
    let request = json!({"add_columns": added});
    let request_digest = digest("schema-intent", &request).map_err(encoding_error)?;
    Ok(Intent {
        id: IntentId(format!(
            "{}/schema/{generation}/{}",
            enrolled.target.0, request_digest
        )),
        kind: IntentKind::Initialization,
        target: enrolled.target.clone(),
        generation,
        flow: enrolled.flow.clone(),
        batch_sequence: checkpoint.next_batch_sequence,
        request_digest,
        request,
        start_progress_digest: checkpoint.progress_digest.clone(),
        end_progress: Value::Null,
        end_progress_digest: String::new(),
        prepared_digest: None,
    })
}

fn reset_intent(enrolled: &Enrolled, generation: u64) -> SparkResult<Intent> {
    let id = IntentId::reset(&enrolled.target, generation);
    let request = json!({"reset": id.0});
    Ok(Intent {
        kind: IntentKind::Reset,
        target: enrolled.target.clone(),
        generation,
        flow: enrolled.flow.clone(),
        batch_sequence: 0,
        request_digest: digest("reset-intent", &request).map_err(encoding_error)?,
        request,
        start_progress_digest: String::new(),
        end_progress: Value::Null,
        end_progress_digest: String::new(),
        prepared_digest: None,
        id,
    })
}

fn batch_tables(enrolled: &Enrolled) -> [(&EffectTable, EffectStep); 2] {
    [
        (&enrolled.auxiliary, EffectStep::Auxiliary),
        (&enrolled.destination, EffectStep::Target),
    ]
}

fn sealed_intent(
    enrolled: &Enrolled,
    generation: u64,
    checkpoint: &FlowCheckpoint,
    progress: &Progress,
    batch: &BatchIntent,
) -> SparkResult<Intent> {
    let sequence = checkpoint.next_batch_sequence;
    let request = serde_json::to_value(batch).map_err(encoding_error)?;
    let end = progress.clone().advance(batch);
    Ok(Intent {
        id: IntentId::new(&enrolled.target, generation, &enrolled.flow, sequence),
        kind: IntentKind::Batch,
        target: enrolled.target.clone(),
        generation,
        flow: enrolled.flow.clone(),
        batch_sequence: sequence,
        request_digest: digest("batch-intent", &request).map_err(encoding_error)?,
        request,
        start_progress_digest: checkpoint.progress_digest.clone(),
        end_progress_digest: digest("progress", &end).map_err(encoding_error)?,
        end_progress: serde_json::to_value(&end).map_err(encoding_error)?,
        prepared_digest: None,
    })
}

/// The stable identity of an effect's request: every attempt, by any owner,
/// is the same request.
fn effect_request(intent: &Intent, step: EffectStep) -> SparkResult<RequestId> {
    let id = format!("{}#{step:?}", intent.id.0);
    let body: Value = json!({"intent": intent.id.0, "step": format!("{step:?}")});
    Ok(RequestId {
        digest: digest("effect-request", &body).map_err(encoding_error)?,
        id,
    })
}

fn fence(
    token: &FenceToken,
    intent: &Intent,
    step: EffectStep,
    prepared_digest: &str,
) -> SparkResult<spec::FencedWrite> {
    let request = effect_request(intent, step)?;
    Ok(spec::FencedWrite {
        authority: token.authority.0.clone(),
        target: token.target.0.clone(),
        generation: token.generation,
        epoch: token.epoch,
        owner: token.owner.clone(),
        intent: intent.id.0.clone(),
        step: effect_step_name(step)?,
        request_id: request.id,
        request_digest: request.digest,
        prepared_digest: prepared_digest.to_string(),
    })
}

/// Publish what the authority ratified, in case a crash came first. It runs
/// before completion, so a completed batch is always fully published, and an
/// unfinished batch plans nothing from these tables: its rows are archived
/// before its first receipt.
async fn publish_receipts<S: Store>(
    ctx: &SessionContext,
    control: &Control<S>,
    intent: &IntentId,
    tables: &[(&EffectTable, EffectStep)],
) -> SparkResult<()> {
    for (table, step) in tables.iter().copied() {
        let receipt = control.receipt(intent, step).await.map_err(control_error)?;
        let Some(ReceiptKind::Data { ratified_version }) = receipt.map(|receipt| receipt.kind)
        else {
            continue;
        };
        let commit = control
            .ratified_commit(&table.sink, ratified_version)
            .await
            .map_err(control_error)?
            .ok_or_else(|| {
                control_error(Outcome::EvidenceUnavailable {
                    reason: format!("no ratified commit for version {ratified_version}"),
                })
            })?;
        let published = i64::try_from(ratified_version)
            .map_err(|_| SparkError::internal("Delta version overflow"))?;
        publish_ratified(
            &ctx.task_ctx(),
            &table.url,
            published,
            &commit.staged_path,
            &commit.action_digest,
        )
        .await?;
    }
    Ok(())
}

impl<S: Store + 'static> DurableRun<'_, S> {
    /// Make this authority the session's commit authority.
    fn install(&self, ctx: &SessionContext)
    where
        S: 'static,
    {
        self.install_with(ctx, None);
    }

    /// `ambient` fences the commits that follow, whose plans carry no fence.
    fn install_with(&self, ctx: &SessionContext, ambient: Option<spec::FencedWrite>)
    where
        S: 'static,
    {
        let commits = ControlCommits::new(self.control.clone(), ambient);
        ctx.state_ref()
            .write()
            .config_mut()
            .set_extension(Arc::new(FencedCommits(Arc::new(commits))));
        // Every session of this server then refuses an unfenced write to a
        // table this authority enrolled.
        if let Ok(registry) = ctx.extension::<FencedCommitRegistry>() {
            registry.register(
                &self.control.authority().0,
                Arc::new(ControlCommits::new(self.control.clone(), None)),
            );
        }
    }

    fn reached(&self, stage: Stage, sequence: u64) -> SparkResult<()> {
        let lost = self
            .lease_lost
            .lock()
            .map_err(|e| SparkError::internal(format!("lease state: {e}")))?
            .clone();
        if let Some(reason) = lost {
            return Err(SparkError::invalid(format!(
                "the lease on the AutoCDC target was lost during the run: {reason}"
            )));
        }
        self.stage_hook.map_or(Ok(()), |hook| hook(stage, sequence))
    }

    /// Keep the lease alive for as long as the returned guard lives. A
    /// renewal by the live owner extends the deadline without a new epoch, so
    /// the run's fence tokens stay valid; a renewal after expiry fails rather
    /// than re-acquiring. A failed renewal is recorded and stops the run at
    /// its next stage.
    fn renew_lease(&self, target: TargetKey) -> LeaseRenewal
    where
        S: 'static,
    {
        let control = self.control.clone();
        let owner = self.owner.to_string();
        let lease_ms = self.lease_ms;
        let lost = self.lease_lost.clone();
        let every =
            std::time::Duration::from_millis(u64::try_from(lease_ms / 3).unwrap_or(250).max(50));
        LeaseRenewal(tokio::spawn(async move {
            loop {
                tokio::time::sleep(every).await;
                if let Err(outcome) = control.renew(&target, &owner, lease_ms).await {
                    if let Ok(mut slot) = lost.lock() {
                        *slot = Some(control_error(outcome).to_string());
                    }
                    return;
                }
            }
        }))
    }

    /// The batch's rows: from its archive if they were sealed, otherwise
    /// read from the source and archived.
    async fn prepared(
        &self,
        ctx: &SessionContext,
        config: Arc<PlanConfig>,
        scope: &TaskResourceScope,
        token: &FenceToken,
        enrolled: &Enrolled,
        intent: &Intent,
    ) -> SparkResult<(PreparedBatch, String)> {
        let guard = self
            .control
            .guard(&enrolled.target)
            .await
            .map_err(control_error)?
            .ok_or_else(|| SparkError::internal("enrolled target has no guard"))?;
        let path = prepared_path(
            self.run.checkpoint,
            guard.checkpoint_index,
            intent.batch_sequence,
        );
        if let Some(digest) = &intent.prepared_digest {
            return Ok((restore(&path, digest)?, digest.clone()));
        }
        let batch: BatchIntent =
            serde_json::from_value(intent.request.clone()).map_err(encoding_error)?;
        let run = self.run;
        let end: Progress =
            serde_json::from_value(intent.end_progress.clone()).map_err(encoding_error)?;
        let state = StateKeys {
            dir: self
                .run
                .checkpoint
                .with_file_name(guard.checkpoint_index.to_string())
                .join("state"),
            start: intent.start_progress_digest.clone(),
            end: intent.end_progress_digest.clone(),
        };
        let plan = run
            .batch_plan(
                ctx,
                &config,
                scope,
                &batch,
                &end,
                &state,
                run.source.describe()?,
            )
            .await?;
        let batch_id = i64::try_from(intent.batch_sequence)
            .map_err(|_| SparkError::internal("batch id overflow"))?;
        let prepared = prepare_microbatch(
            ctx,
            config,
            run.write,
            &enrolled.auxiliary_name,
            plan,
            batch_id,
        )
        .await?;
        let digest = archive(&path, &prepared)?;
        self.control
            .record_prepared(token, &intent.id, &digest)
            .await
            .map_err(control_error)?;
        Ok((prepared, digest))
    }

    /// Finish one sealed batch, repeating only what has no receipt.
    async fn process(
        &self,
        ctx: &SessionContext,
        config: Arc<PlanConfig>,
        scope: &TaskResourceScope,
        token: &FenceToken,
        enrolled: &Enrolled,
        intent: &Intent,
    ) -> SparkResult<()> {
        let control = &self.control;
        let sequence = intent.batch_sequence;
        self.reached(Stage::Sealed, sequence)?;
        let (prepared, digest) = self
            .prepared(ctx, config.clone(), scope, token, enrolled, intent)
            .await?;
        self.reached(Stage::Prepared, sequence)?;
        publish_prepared(ctx, &prepared).await?;
        let auxiliary_done = control
            .receipt(&intent.id, EffectStep::Auxiliary)
            .await
            .map_err(control_error)?
            .is_some();
        if !auxiliary_done {
            merge_auxiliary(
                ctx,
                config.clone(),
                self.run.write,
                &enrolled.auxiliary_name,
                Some(fence(token, intent, EffectStep::Auxiliary, &digest)?),
            )
            .await?;
        }
        self.reached(Stage::AuxiliaryDone, sequence)?;
        let target_done = control
            .receipt(&intent.id, EffectStep::Target)
            .await
            .map_err(control_error)?
            .is_some();
        if !target_done {
            merge_destination(
                ctx,
                config,
                self.run.write,
                &prepared,
                Some(fence(token, intent, EffectStep::Target, &digest)?),
            )
            .await?;
        }
        self.reached(Stage::TargetDone, sequence)?;
        publish_receipts(ctx, control, &intent.id, &batch_tables(enrolled)).await?;
        let request = RequestId {
            id: format!("{}#complete", intent.id.0),
            digest: intent.request_digest.clone(),
        };
        control
            .complete_batch(&request, token, &intent.id)
            .await
            .map_err(control_error)?;
        self.reached(Stage::Completed, sequence)
    }

    /// Finish an initialization, repeating only what has no receipt. It
    /// advances no progress and no batch number.
    async fn finish_initialization(
        &self,
        ctx: &SessionContext,
        config: Arc<PlanConfig>,
        token: &FenceToken,
        enrolled: &Enrolled,
        intent: &Intent,
    ) -> SparkResult<()> {
        let control = &self.control;
        let destination = self.run.write.destination;
        let digest = digest("schema-prepared", &intent.id.0).map_err(encoding_error)?;
        let target_done = control
            .receipt(&intent.id, EffectStep::InitializeTarget)
            .await
            .map_err(control_error)?
            .is_some();
        let evolved = evolved_schema(ctx, config.clone(), self.run).await?;
        let schema = match &evolved {
            Some((schema, _)) => schema.clone(),
            None => Arc::new(
                read_table(ctx, config.clone(), destination)
                    .await?
                    .schema()
                    .as_arrow()
                    .clone(),
            ),
        };
        let columns = schema
            .fields()
            .iter()
            .map(|field| field.name().clone())
            .collect();
        publish_prepared(
            ctx,
            &PreparedBatch {
                schema,
                batches: vec![],
                columns,
            },
        )
        .await?;
        if !target_done {
            let fenced = fence(token, intent, EffectStep::InitializeTarget, &digest)?;
            evolve_fenced(ctx, config.clone(), destination, fenced).await?;
        }
        self.reached(Stage::EvolvedTarget, 0)?;
        let auxiliary_done = control
            .receipt(&intent.id, EffectStep::InitializeAuxiliary)
            .await
            .map_err(control_error)?
            .is_some();
        if !auxiliary_done {
            let fenced = fence(token, intent, EffectStep::InitializeAuxiliary, &digest)?;
            noop_fenced(ctx, config, &enrolled.auxiliary_name, fenced).await?;
        }
        let tables = [
            (&enrolled.auxiliary, EffectStep::InitializeAuxiliary),
            (&enrolled.destination, EffectStep::InitializeTarget),
        ];
        publish_receipts(ctx, control, &intent.id, &tables).await?;
        control
            .complete_initialization(token, &intent.id)
            .await
            .map_err(control_error)?;
        Ok(())
    }

    /// Finish a begun reset, repeating only what has no receipt. Truncating
    /// keeps both tables and their identities, so their schemas carry over.
    async fn finish_reset(
        &self,
        ctx: &SessionContext,
        config: Arc<PlanConfig>,
        token: &FenceToken,
        enrolled: &Enrolled,
        intent: &Intent,
    ) -> SparkResult<u64> {
        let control = &self.control;
        let write = self.run.write;
        let output = flow_output_schema(
            ctx,
            config.clone(),
            write.args,
            write.case,
            &self.run.source.schema(),
        )
        .await?;
        let digest = digest("reset-prepared", &intent.id.0).map_err(encoding_error)?;
        let auxiliary_url = enrolled.auxiliary.url.clone();
        let task = ctx.task_ctx();
        // A target recreated outside the authority is adopted here, under the
        // begun reset, so the truncate below is fenced against the live log.
        let destination = match table_state(&task, &enrolled.destination.url).await? {
            Some((id, _)) if SinkIdentity(id.clone()) != enrolled.destination.sink => {
                mark_enrolled(
                    ctx,
                    config.clone(),
                    write.destination,
                    &control.authority().0,
                )
                .await?;
                let (_, head) = table_state(&task, &enrolled.destination.url)
                    .await?
                    .ok_or_else(|| SparkError::internal("recreated target has no commit"))?;
                let sink = SinkIdentity(id);
                control
                    .rebind_sink(
                        token,
                        enrolled.destination.url.as_str(),
                        &sink,
                        version(head)?,
                    )
                    .await
                    .map_err(control_error)?;
                EffectTable {
                    sink,
                    url: enrolled.destination.url.clone(),
                }
            }
            _ => EffectTable {
                sink: enrolled.destination.sink.clone(),
                url: enrolled.destination.url.clone(),
            },
        };
        let auxiliary_replaced = table_state(&task, &auxiliary_url)
            .await?
            .is_none_or(|(id, _)| SinkIdentity(id) != enrolled.auxiliary.sink);
        let target_done = control
            .receipt(&intent.id, EffectStep::ResetTarget)
            .await
            .map_err(control_error)?
            .is_some();
        if !target_done {
            let columns = output.fields().iter().map(|x| x.name().clone()).collect();
            let empty = PreparedBatch {
                schema: output.clone(),
                batches: vec![],
                columns,
            };
            publish_prepared(ctx, &empty).await?;
            let fenced = fence(token, intent, EffectStep::ResetTarget, &digest)?;
            self.install_with(ctx, Some(fenced));
            let replaced =
                replace_delta_with_view(ctx, config.clone(), &enrolled.destination.url).await;
            self.install(ctx);
            replaced?;
        }
        self.reached(Stage::ResetTargetDone, 0)?;
        let auxiliary_done = control
            .receipt(&intent.id, EffectStep::ResetAuxiliary)
            .await
            .map_err(control_error)?
            .is_some();
        if !auxiliary_done {
            let fenced = fence(token, intent, EffectStep::ResetAuxiliary, &digest)?;
            noop_fenced(ctx, config.clone(), &enrolled.auxiliary_name, fenced).await?;
        }
        publish_receipts(
            ctx,
            control,
            &intent.id,
            &[(&destination, EffectStep::ResetTarget)],
        )
        .await?;
        // `DatasetManager` drops the auxiliary state rather than truncating
        // it: this is how a flow's keys change.
        if !auxiliary_replaced {
            drop_table(ctx, config.clone(), &enrolled.auxiliary_name).await?;
        }
        ensure_auxiliary_table(
            ctx,
            config.clone(),
            write,
            &flow_schema(self.run.source, write)?,
            false,
            Some(fresh_auxiliary(intent.generation)),
        )
        .await?;
        // A table's location is unique to it, so the new one has its own.
        let auxiliary_url = table_url(ctx, &enrolled.auxiliary_name).await?;
        birth_auxiliary(
            ctx,
            config.clone(),
            self,
            &enrolled.auxiliary_name,
            &auxiliary_url,
        )
        .await?;
        mark_enrolled(
            ctx,
            config.clone(),
            &enrolled.auxiliary_name,
            &control.authority().0,
        )
        .await?;
        control
            .bind_physical(&enrolled.target, auxiliary_url.as_str())
            .await
            .map_err(control_error)?;
        let (auxiliary, auxiliary_head) = table_state(&task, &auxiliary_url)
            .await?
            .ok_or_else(|| SparkError::internal("recreated auxiliary table has no commit"))?;
        let identities = ResetIdentities {
            sink: destination.sink,
            auxiliary: SinkIdentity(auxiliary),
            auxiliary_head: version(auxiliary_head)?,
        };
        let guard = control
            .finalize_reset(token, &identities)
            .await
            .map_err(control_error)?;
        ensure_checkpoint(control, &enrolled.target, guard.generation, &enrolled.flow).await?;
        Ok(guard.generation)
    }
}

/// The target's schema with the flow's new columns, if it has any. Columns the
/// flow no longer emits stay, as `StructType.merge` keeps them.
pub(super) async fn evolved_schema(
    ctx: &SessionContext,
    config: Arc<PlanConfig>,
    run: &FlowRun<'_>,
) -> SparkResult<Option<(SchemaRef, Vec<String>)>> {
    let write = run.write;
    let current = read_table(ctx, config.clone(), write.destination).await?;
    let current = current.schema().as_arrow().clone();
    let output =
        flow_output_schema(ctx, config, write.args, write.case, &run.source.schema()).await?;
    let resolver = write.case.resolver();
    let mut changed = vec![];
    let mut fields = Vec::with_capacity(current.fields().len());
    for existing in current.fields() {
        let incoming = output
            .fields()
            .iter()
            .find(|field| resolver(existing.name(), field.name()));
        let merged = incoming.map_or_else(
            || existing.data_type().clone(),
            |field| evolved_type(existing.data_type(), field.data_type(), resolver),
        );
        if &merged != existing.data_type() {
            changed.push(existing.name().clone());
        }
        fields.push(existing.as_ref().clone().with_data_type(merged));
    }
    for field in output.fields() {
        if !current
            .fields()
            .iter()
            .any(|x| resolver(x.name(), field.name()))
        {
            changed.push(field.name().clone());
            fields.push(field.as_ref().clone().with_nullable(true));
        }
    }
    if changed.is_empty() {
        return Ok(None);
    }
    Ok(Some((Arc::new(Schema::new(fields)), changed)))
}

/// `current` with the struct fields of `incoming` it lacks, at any depth.
fn evolved_type(
    current: &DataType,
    incoming: &DataType,
    resolver: fn(&str, &str) -> bool,
) -> DataType {
    match (current, incoming) {
        (DataType::Struct(current), DataType::Struct(incoming)) => {
            let mut fields: Vec<Field> = current
                .iter()
                .map(|field| {
                    let other = incoming.iter().find(|x| resolver(x.name(), field.name()));
                    let data_type = other.map_or_else(
                        || field.data_type().clone(),
                        |other| evolved_type(field.data_type(), other.data_type(), resolver),
                    );
                    field.as_ref().clone().with_data_type(data_type)
                })
                .collect();
            for field in incoming.iter() {
                if !current.iter().any(|x| resolver(x.name(), field.name())) {
                    fields.push(field.as_ref().clone().with_nullable(true));
                }
            }
            DataType::Struct(fields.into())
        }
        (DataType::List(current), DataType::List(incoming)) => {
            let item = evolved_type(current.data_type(), incoming.data_type(), resolver);
            DataType::List(Arc::new(current.as_ref().clone().with_data_type(item)))
        }
        _ => current.clone(),
    }
}

/// Whether a Delta table made outside a pipeline needs in-commit timestamps.
/// Only a table that is not enrolled can lack them, so this writes no enrolled
/// table around the fence.
async fn lacks_commit_timestamps(ctx: &SessionContext, name: &Name) -> SparkResult<bool> {
    let table = ctx
        .extension::<CatalogManager>()?
        .get_table_or_view(name)
        .await
        .map_err(PlanError::from)?;
    let TableKind::Table {
        format, properties, ..
    } = &table.kind
    else {
        return Ok(false);
    };
    Ok(format.eq_ignore_ascii_case("delta")
        && !properties
            .iter()
            .any(|(key, value)| key == IN_COMMIT_TIMESTAMPS_PROPERTY && value == "true"))
}

/// Where a missing auxiliary table belongs. Mid-batch it holds state to recover.
/// Otherwise it starts empty, as Spark starts a dropped table, at a path nothing
/// can be left at: a metastore gives a recreated target its old location.
fn auxiliary_placement(guard: Option<&TargetGuard>) -> AuxiliaryPlacement {
    let open = guard
        .filter(|guard| guard.active_intent.is_some() && guard.state != TargetState::Resetting);
    match open.and_then(|guard| guard.auxiliary_location.as_ref()) {
        Some(location) => AuxiliaryPlacement::At(location.clone()),
        None => fresh_auxiliary(guard.map_or(0, |guard| guard.generation)),
    }
}

fn fresh_auxiliary(generation: u64) -> AuxiliaryPlacement {
    AuxiliaryPlacement::BesideTarget(format!("{generation}-{}", uuid::Uuid::new_v4()))
}

/// Wait for another owner's lease to end. Only a dead or concurrent run holds
/// one, and this is the one place a run may still be waiting for it.
async fn await_lease<S: Store>(durable: &DurableRun<'_, S>, destination: &Name) -> SparkResult<()> {
    let target = TargetKey(destination.join("."));
    let control = &durable.control;
    let deadline = std::time::Instant::now()
        + std::time::Duration::from_millis(u64::try_from(durable.lease_wait_ms).unwrap_or(0));
    loop {
        if control
            .guard(&target)
            .await
            .map_err(control_error)?
            .is_none()
        {
            return Ok(());
        }
        match control
            .acquire(&target, durable.owner, durable.lease_ms)
            .await
        {
            Err(Outcome::Conflict { .. }) if std::time::Instant::now() < deadline => {
                tokio::time::sleep(std::time::Duration::from_millis(250)).await;
            }
            Err(outcome) => return Err(control_error(outcome)),
            Ok(_) => return Ok(()),
        }
    }
}

/// Hand the target back when a run stops on an error, so that the next run,
/// perhaps the full refresh the error asks for, need not wait out the lease.
/// Safe on every error: an unfinished intent stays in the guard and every
/// outcome lives in the authority, so the next owner recovers it. A release
/// failure leaves the lease to expire; the run's error is the one to report.
async fn released_on_error<S: Store, T>(
    durable: &DurableRun<'_, S>,
    target: &TargetKey,
    result: SparkResult<T>,
) -> SparkResult<T> {
    if result.is_err() {
        let control = &durable.control;
        if let Ok(acquired) = control
            .acquire(target, durable.owner, durable.lease_ms)
            .await
        {
            let _ = control.release(&acquired.token).await;
        }
    }
    result
}

/// Run one SCD1 flow to the frontier captured now, finishing any unfinished
/// batch or reset first. A requested reset replaces an unfinished batch: what
/// that batch already ratified is published, and its source progress is not
/// advanced. One failure stops the run; the next run recovers.
pub(super) async fn run_durable_flow<S: Store + 'static>(
    ctx: &SessionContext,
    config: Arc<PlanConfig>,
    scope: &TaskResourceScope,
    durable: &DurableRun<'_, S>,
) -> SparkResult<DurableOutcome> {
    let run = durable.run;
    let control = &durable.control;
    if let Some(flow_source) = run.flow_source {
        flow_source.refresh(ctx).await?;
    }
    let target = TargetKey(run.write.destination.join("."));
    let guard = control.guard(&target).await.map_err(control_error)?;
    let resetting = durable.reset
        || guard
            .as_ref()
            .is_some_and(|guard| guard.state == TargetState::Resetting);
    let placement = auxiliary_placement(guard.as_ref());
    // An enrolled table already has them, and DDL cannot carry a fence.
    if guard.is_none() && lacks_commit_timestamps(ctx, run.write.destination).await? {
        mark_enrolled(
            ctx,
            config.clone(),
            run.write.destination,
            &durable.control.authority().0,
        )
        .await?;
    }
    let auxiliary_name = ensure_auxiliary_table(
        ctx,
        config.clone(),
        run.write,
        &flow_schema(run.source, run.write)?,
        resetting,
        Some(placement),
    )
    .await?;
    await_lease(durable, run.write.destination).await?;
    let mut enrolled = enroll(ctx, config.clone(), durable, auxiliary_name.clone()).await?;
    durable.install(ctx);
    let target = enrolled.target.clone();
    let _renewal = durable.renew_lease(target.clone());
    let result: SparkResult<DurableOutcome> = async {
        let mut outcome = DurableOutcome::default();
        let acquired = control
            .acquire(&enrolled.target, durable.owner, durable.lease_ms)
            .await
            .map_err(control_error)?;
        let mut reset = durable.reset;
        if let Some(id) = &acquired.recovery_only {
            let intent = control
                .intent(id)
                .await
                .map_err(control_error)?
                .ok_or_else(|| {
                    control_error(Outcome::EvidenceUnavailable {
                        reason: format!("unfinished intent {} is not sealed", id.0),
                    })
                })?;
            match intent.kind {
                IntentKind::Initialization => {
                    durable
                        .finish_initialization(
                            ctx,
                            config.clone(),
                            &acquired.token,
                            &enrolled,
                            &intent,
                        )
                        .await?;
                }
                IntentKind::Reset => {
                    let generation = durable
                        .finish_reset(ctx, config.clone(), &acquired.token, &enrolled, &intent)
                        .await?;
                    outcome.reset = Some(generation);
                    reset = false;
                    enrolled = enroll(ctx, config.clone(), durable, auxiliary_name.clone()).await?;
                }
                IntentKind::Batch if reset => {
                    check_reset_origin(run.source)?;
                    publish_receipts(ctx, control, id, &batch_tables(&enrolled)).await?;
                }
                IntentKind::Batch => {
                    durable
                        .process(
                            ctx,
                            config.clone(),
                            scope,
                            &acquired.token,
                            &enrolled,
                            &intent,
                        )
                        .await?;
                    outcome.batches += 1;
                    outcome.recovered = Some(intent.batch_sequence);
                }
            }
        }
        if reset {
            check_reset_origin(run.source)?;
            let intent = reset_intent(&enrolled, acquired.token.generation + 1)?;
            let token = control
                .begin_reset(&acquired.token, &intent)
                .await
                .map_err(control_error)?;
            durable.reached(Stage::ResetBegun, 0)?;
            let generation = durable
                .finish_reset(ctx, config.clone(), &token, &enrolled, &intent)
                .await?;
            outcome.reset = Some(generation);
            enrolled = enroll(ctx, config.clone(), durable, auxiliary_name).await?;
        }
        if let Some((_, added)) = evolved_schema(ctx, config.clone(), durable.run).await? {
            let token = control
                .acquire(&enrolled.target, durable.owner, durable.lease_ms)
                .await
                .map_err(control_error)?
                .token;
            let checkpoint = control
                .checkpoint(&enrolled.target, token.generation, &enrolled.flow)
                .await
                .map_err(control_error)?
                .ok_or_else(|| SparkError::internal("flow has no checkpoint"))?;
            let intent = control
                .seal_intent(
                    &token,
                    &schema_intent(&enrolled, token.generation, &checkpoint, &added)?,
                )
                .await
                .map_err(control_error)?;
            durable
                .finish_initialization(ctx, config.clone(), &token, &enrolled, &intent)
                .await?;
        }
        let frontier = RunFrontier::capture(run.source)?;
        loop {
            let token = control
                .acquire(&enrolled.target, durable.owner, durable.lease_ms)
                .await
                .map_err(control_error)?
                .token;
            let checkpoint = control
                .checkpoint(&enrolled.target, token.generation, &enrolled.flow)
                .await
                .map_err(control_error)?
                .ok_or_else(|| {
                    control_error(Outcome::EvidenceUnavailable {
                        reason: "flow has no checkpoint".into(),
                    })
                })?;
            let progress: Progress =
                serde_json::from_value(checkpoint.progress.clone()).map_err(encoding_error)?;
            let Some(batch) =
                next_intent(run.source, &frontier, &progress, run.max_rows_per_partition)?
            else {
                control.release(&token).await.map_err(control_error)?;
                return Ok(outcome);
            };
            let intent = control
                .seal_intent(
                    &token,
                    &sealed_intent(&enrolled, token.generation, &checkpoint, &progress, &batch)?,
                )
                .await
                .map_err(control_error)?;
            durable
                .process(ctx, config.clone(), scope, &token, &enrolled, &intent)
                .await?;
            outcome.batches += 1;
        }
    }
    .await;
    released_on_error(durable, &target, result).await
}
