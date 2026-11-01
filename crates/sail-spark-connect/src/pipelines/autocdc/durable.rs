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
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::arrow::ipc::reader::FileReader;
use datafusion::arrow::ipc::writer::FileWriter;
use datafusion::prelude::SessionContext;
use sail_catalog::manager::CatalogManager;
use sail_common::spec;
use sail_common_datafusion::catalog::TableKind;
use sail_common_datafusion::extension::SessionExtensionAccessor;
use sail_common_datafusion::fenced_commit::FencedCommits;
use sail_common_datafusion::task_resources::TaskResourceScope;
use sail_delta_lake::fenced::{publish_ratified, table_state};
use sail_plan::config::PlanConfig;
use sail_plan::error::PlanError;
use sail_sdp_control::control::{Control, RequestId};
use sail_sdp_control::digest::digest;
use sail_sdp_control::identity::{
    EffectStep, FenceToken, FlowKey, IntentId, SinkIdentity, TargetKey,
};
use sail_sdp_control::records::{FlowCheckpoint, Intent, IntentKind, ReceiptKind};
use sail_sdp_control::store::Store;
use sail_sdp_control::transitions::{Outcome, ResetIdentities};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use url::Url;

use super::authority::{ControlCommits, effect_step_name};
use super::run::{FlowRun, flow_schema};
use super::write::{
    PreparedBatch, ensure_auxiliary_table, merge_auxiliary, merge_destination, prepare_microbatch,
    publish_prepared, read_table, truncate_fenced,
};
use crate::error::{SparkError, SparkResult};
use crate::pipelines::graph::Name;
use crate::pipelines::streaming::binding::SourceBinding;
use crate::pipelines::streaming::intent::{BatchIntent, Progress, RunFrontier, next_intent};
use crate::pipelines::streaming::lower::{Inputs, lower};
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
}

type StageHook<'a> = &'a (dyn Fn(Stage, u64) -> SparkResult<()> + Send + Sync);

pub(super) struct DurableRun<'a, S> {
    pub run: &'a FlowRun<'a>,
    pub control: Arc<Control<S>>,
    pub owner: &'a str,
    pub lease_ms: i64,
    /// Rebuild the target from the source's origin before reading it.
    pub reset: bool,
    pub stage_hook: Option<StageHook<'a>>,
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
    let directory = format!("{}/", location.trim_end_matches('/'));
    Url::parse(&directory)
        .or_else(|_| {
            Url::from_directory_path(&location)
                .map_err(|()| url::ParseError::RelativeUrlWithoutBase)
        })
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
    step: EffectStep,
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

/// Bind the destination and auxiliary tables once. A recreated table has a
/// new identity, which enrollment refuses rather than adopts.
async fn enroll<S: Store>(
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
    if table_state(&task, &sink_url).await?.is_none() {
        let empty = empty_batch(ctx, config.clone(), destination_name).await?;
        publish_prepared(ctx, &empty).await?;
        merge_destination(ctx, config.clone(), write, &empty, None).await?;
    }
    if table_state(&task, &auxiliary_url).await?.is_none() {
        let empty = empty_batch(ctx, config.clone(), destination_name).await?;
        publish_prepared(ctx, &empty).await?;
        merge_auxiliary(ctx, config, write, &auxiliary_name, None).await?;
    }
    let born = async |url: &Url| {
        table_state(&task, url)
            .await?
            .ok_or_else(|| SparkError::internal(format!("table {url} has no commit")))
    };
    let (sink_id, sink_head) = born(&sink_url).await?;
    let (auxiliary_id, auxiliary_head) = born(&auxiliary_url).await?;
    let (sink, auxiliary) = (SinkIdentity(sink_id), SinkIdentity(auxiliary_id));
    let control = &durable.control;
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
            step: EffectStep::Auxiliary,
            sink: auxiliary,
            url: auxiliary_url,
        },
        destination: EffectTable {
            step: EffectStep::Target,
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
    tables: [(&EffectTable, EffectStep); 2],
) -> SparkResult<()> {
    for (table, step) in tables {
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

impl<S: Store> DurableRun<'_, S> {
    /// Make this authority the session's commit authority.
    fn install(&self, ctx: &SessionContext)
    where
        S: 'static,
    {
        let commits = ControlCommits::new(self.control.clone());
        ctx.state_ref()
            .write()
            .config_mut()
            .set_extension(Arc::new(FencedCommits(Arc::new(commits))));
    }

    fn reached(&self, stage: Stage, sequence: u64) -> SparkResult<()> {
        self.stage_hook.map_or(Ok(()), |hook| hook(stage, sequence))
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
        let scans = batch
            .ranges
            .iter()
            .map(|range| run.source.scan(range))
            .collect::<Result<Vec<_>, _>>()?;
        let binding = SourceBinding {
            name: run.source_name.clone(),
            schema: run.source.schema(),
            descriptor: run.source.describe()?,
            intent: batch.clone(),
            scans,
        };
        let lowered = lower(
            ctx,
            config.clone(),
            scope,
            &Inputs {
                graph: run.graph,
                flow: run.flow,
                relations: run.relations,
                sources: std::slice::from_ref(&binding),
                statics: &[],
                locals: &[],
            },
        )
        .await
        .map_err(|error| SparkError::invalid(error.to_string()))?;
        let batch_id = i64::try_from(intent.batch_sequence)
            .map_err(|_| SparkError::internal("batch id overflow"))?;
        let prepared = prepare_microbatch(
            ctx,
            config,
            run.write,
            &enrolled.auxiliary_name,
            lowered.plan,
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
        publish_receipts(ctx, control, &intent.id, batch_tables(enrolled)).await?;
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
        let destination = self.run.write.destination;
        let empty = empty_batch(ctx, config.clone(), destination).await?;
        publish_prepared(ctx, &empty).await?;
        let digest = digest("reset-prepared", &intent.id.0).map_err(encoding_error)?;
        let effects = [
            (EffectStep::ResetTarget, destination),
            (EffectStep::ResetAuxiliary, &enrolled.auxiliary_name),
        ];
        for (step, table) in effects {
            let done = control
                .receipt(&intent.id, step)
                .await
                .map_err(control_error)?
                .is_some();
            if !done {
                let fenced = fence(token, intent, step, &digest)?;
                truncate_fenced(ctx, config.clone(), table, fenced).await?;
            }
            if step == EffectStep::ResetTarget {
                self.reached(Stage::ResetTargetDone, 0)?;
            }
        }
        let tables = [
            (&enrolled.auxiliary, EffectStep::ResetAuxiliary),
            (&enrolled.destination, EffectStep::ResetTarget),
        ];
        publish_receipts(ctx, control, &intent.id, tables).await?;
        let identities = ResetIdentities {
            sink: enrolled.destination.sink.clone(),
            auxiliary: enrolled.auxiliary.sink.clone(),
        };
        let guard = control
            .finalize_reset(token, &identities)
            .await
            .map_err(control_error)?;
        ensure_checkpoint(control, &enrolled.target, guard.generation, &enrolled.flow).await?;
        Ok(guard.generation)
    }
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
    let auxiliary_name = ensure_auxiliary_table(
        ctx,
        config.clone(),
        run.write,
        &flow_schema(run.source, run.write)?,
    )
    .await?;
    let enrolled = enroll(ctx, config.clone(), durable, auxiliary_name).await?;
    durable.install(ctx);
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
            IntentKind::Reset => {
                let generation = durable
                    .finish_reset(ctx, config.clone(), &acquired.token, &enrolled, &intent)
                    .await?;
                outcome.reset = Some(generation);
                reset = false;
            }
            IntentKind::Batch if reset => {
                check_reset_origin(run.source)?;
                publish_receipts(ctx, control, id, batch_tables(&enrolled)).await?;
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
            kind => {
                return Err(control_error(Outcome::UnsupportedCapability {
                    reason: format!("unfinished {kind:?} intent"),
                }));
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
