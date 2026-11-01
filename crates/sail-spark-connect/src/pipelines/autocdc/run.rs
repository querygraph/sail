//! A triggered SCD1 flow run over the bounded streaming components
//! (`CDC-ACTIVATION-DESIGN.md`, unit A5): `Trigger.AvailableNow` semantics
//! from `TriggeredGraphExecution.scala:77`, the checkpoint layout of
//! `SystemMetadata.scala:40–80` (`<root>/_checkpoints/<destination>/<flow>/<n>`,
//! a reset creating `n + 1` as `State.scala:81–100` does), and
//! `Scd1MergeStreamingWrite.startStream` (`FlowExecution.scala:640–660`):
//! the auxiliary table once at flow start, then one handler call per
//! microbatch, the record advancing after each completed batch.
//!
//! The run record is a local JSON file with a single writer and no fencing;
//! that boundary is decision 1 of the design checkpoint and is stated in
//! every record. `durable.rs` is the fenced run that `execution::execute`
//! admits; this one stays for its tests of the flow semantics.
#![cfg_attr(
    not(test),
    expect(
        dead_code,
        reason = "the unfenced run exists for its flow-semantics tests"
    )
)]

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use datafusion::logical_expr::LogicalPlan;
use datafusion::prelude::SessionContext;
use sail_common::spec;
use sail_common_datafusion::task_resources::TaskResourceScope;
use sail_plan::config::PlanConfig;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use super::auxiliary;
use super::write::{Scd1Write, ensure_auxiliary_table, handle_microbatch};
use crate::error::{SparkError, SparkResult};
use crate::pipelines::graph::{Flow, GraphDefinition, Name};
use crate::pipelines::streaming::binding::SourceBinding;
use crate::pipelines::streaming::intent::{Progress, RunFrontier, next_intent};
use crate::pipelines::streaming::lower::{Inputs, lower};
use crate::pipelines::streaming::source::BoundedSource;

/// The persisted state of one flow's checkpoint directory.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub(super) struct RunRecord {
    /// The boundary this record lives under; stated so a reader of the file
    /// never mistakes it for a fenced authority.
    pub authority: String,
    /// SHA-256 over the source descriptor captured by the last run.
    pub descriptor_digest: String,
    /// The frontier the last run captured (`Trigger.AvailableNow`).
    pub frontier: Option<RunFrontier>,
    /// Positions after the last completed batch; the next run continues here.
    pub progress: Progress,
    /// `batchId` of the next microbatch, continuing across runs as
    /// Structured Streaming's does.
    pub completed_batches: u64,
}

const AUTHORITY: &str = "local single-writer file; no ownership fencing";

impl RunRecord {
    fn path(dir: &Path) -> PathBuf {
        dir.join("record.json")
    }

    pub fn load(dir: &Path) -> SparkResult<Option<Self>> {
        let path = Self::path(dir);
        if !path.exists() {
            return Ok(None);
        }
        let text = std::fs::read_to_string(&path)
            .map_err(|e| SparkError::internal(format!("read {}: {e}", path.display())))?;
        serde_json::from_str(&text)
            .map(Some)
            .map_err(|e| SparkError::internal(format!("parse {}: {e}", path.display())))
    }

    /// Written to a sibling file and renamed, so a reader sees either the
    /// previous record or the new one.
    pub fn store(&self, dir: &Path) -> SparkResult<()> {
        std::fs::create_dir_all(dir)
            .map_err(|e| SparkError::internal(format!("create {}: {e}", dir.display())))?;
        let text = serde_json::to_string_pretty(self)
            .map_err(|e| SparkError::internal(format!("serialize run record: {e}")))?;
        let staged = dir.join("record.json.next");
        std::fs::write(&staged, text)
            .map_err(|e| SparkError::internal(format!("write {}: {e}", staged.display())))?;
        std::fs::rename(&staged, Self::path(dir))
            .map_err(|e| SparkError::internal(format!("rename {}: {e}", staged.display())))
    }
}

/// `SystemMetadata`: the checkpoint directory of a flow is
/// `<root>/_checkpoints/<destination>/<flow>/<n>` with `n` the highest
/// existing number; `reset` creates `n + 1` (`State.reset`), and a flow
/// without a checkpoint starts at `0`.
pub(super) fn checkpoint_dir(
    root: &Path,
    destination: &Name,
    flow: &Name,
    reset: bool,
) -> SparkResult<PathBuf> {
    let flow_dir = root
        .join("_checkpoints")
        .join(destination.join("."))
        .join(flow.join("."));
    std::fs::create_dir_all(&flow_dir)
        .map_err(|e| SparkError::internal(format!("create {}: {e}", flow_dir.display())))?;
    let latest = std::fs::read_dir(&flow_dir)
        .map_err(|e| SparkError::internal(format!("list {}: {e}", flow_dir.display())))?
        .filter_map(|entry| entry.ok())
        .filter_map(|entry| entry.file_name().to_str()?.parse::<u64>().ok())
        .max();
    let version = match (latest, reset) {
        (None, _) => 0,
        (Some(n), true) => n + 1,
        (Some(n), false) => n,
    };
    let dir = flow_dir.join(version.to_string());
    std::fs::create_dir_all(&dir)
        .map_err(|e| SparkError::internal(format!("create {}: {e}", dir.display())))?;
    Ok(dir)
}

/// One flow's run inputs.
pub(super) struct FlowRun<'a> {
    pub graph: &'a GraphDefinition,
    pub flow: &'a Flow,
    pub relations: &'a BTreeMap<Name, LogicalPlan>,
    /// The graph name the flow's streaming read binds to.
    pub source_name: &'a Name,
    pub source: &'a dyn BoundedSource,
    pub write: &'a Scd1Write<'a>,
    pub checkpoint: &'a Path,
    pub max_rows_per_partition: usize,
    /// Set when the flow's query is read through views or from several
    /// streams; a bare read of `source` lowers through `source_name` instead.
    pub flow_source: Option<&'a super::flow_source::FlowSource>,
}

pub(super) struct RunOutcome {
    /// Microbatches completed in this run.
    pub batches: u64,
    pub record: RunRecord,
}

fn descriptor_digest(source: &dyn BoundedSource) -> SparkResult<String> {
    let descriptor = source.describe()?;
    let text = serde_json::to_string(&descriptor)
        .map_err(|e| SparkError::internal(format!("serialize source descriptor: {e}")))?;
    Ok(Sha256::digest(text.as_bytes())
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect())
}

/// The flow's schema for the auxiliary contract: the source fields, then
/// the metadata column (`AutoCdcMergeFlow.schema` without a selection).
pub(super) fn flow_schema(
    source: &dyn BoundedSource,
    write: &Scd1Write<'_>,
) -> SparkResult<spec::Fields> {
    let mut fields = Vec::new();
    for field in source.schema().fields() {
        let data_type: spec::DataType =
            crate::spark::connect::DataType::try_from(field.data_type().clone())?.try_into()?;
        fields.push(spec::Field {
            name: field.name().clone(),
            data_type,
            nullable: field.is_nullable(),
            metadata: vec![],
        });
    }
    fields.push(auxiliary::metadata_field(write.sequencing_type));
    Ok(spec::Fields::from(fields))
}

/// Run one SCD1 flow to the frontier captured now.
pub(super) async fn run_flow(
    ctx: &SessionContext,
    config: Arc<PlanConfig>,
    scope: &TaskResourceScope,
    run: &FlowRun<'_>,
) -> SparkResult<RunOutcome> {
    let aux = ensure_auxiliary_table(
        ctx,
        config.clone(),
        run.write,
        &flow_schema(run.source, run.write)?,
    )
    .await?;
    let mut record = RunRecord::load(run.checkpoint)?.unwrap_or_default();
    record.authority = AUTHORITY.into();
    record.descriptor_digest = descriptor_digest(run.source)?;
    let frontier = RunFrontier::capture(run.source)?;
    record.frontier = Some(frontier.clone());
    record.store(run.checkpoint)?;
    let descriptor = run.source.describe()?;
    let mut batches = 0;
    while let Some(intent) = next_intent(
        run.source,
        &frontier,
        &record.progress,
        run.max_rows_per_partition,
    )? {
        let scans = intent
            .ranges
            .iter()
            .map(|range| run.source.scan(range))
            .collect::<Result<Vec<_>, _>>()?;
        let binding = SourceBinding {
            name: run.source_name.clone(),
            schema: run.source.schema(),
            descriptor: descriptor.clone(),
            intent: intent.clone(),
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
        let batch_id = i64::try_from(record.completed_batches)
            .map_err(|_| SparkError::internal("batch id overflow"))?;
        handle_microbatch(ctx, config.clone(), run.write, &aux, lowered.plan, batch_id).await?;
        record.progress = record.progress.advance(&intent);
        record.completed_batches += 1;
        batches += 1;
        record.store(run.checkpoint)?;
    }
    Ok(RunOutcome { batches, record })
}

/// `TriggeredGraphExecution`: the flows of a run start in the given
/// (topological) order; this runs them one at a time, each to its frontier,
/// and stops at the first failure, returning the outcomes completed before
/// it with the error. Concurrency (`maxConcurrentFlows`) and watchdog
/// retries are not modelled.
pub(super) async fn run_flows(
    ctx: &SessionContext,
    config: Arc<PlanConfig>,
    scope: &TaskResourceScope,
    runs: &[FlowRun<'_>],
) -> Result<Vec<RunOutcome>, (Vec<RunOutcome>, SparkError)> {
    let mut outcomes = Vec::with_capacity(runs.len());
    for run in runs {
        match run_flow(ctx, config.clone(), scope, run).await {
            Ok(outcome) => outcomes.push(outcome),
            Err(error) => return Err((outcomes, error)),
        }
    }
    Ok(outcomes)
}
