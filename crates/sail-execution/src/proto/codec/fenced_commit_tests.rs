//! The fence must reach the worker that commits: a commit that lost it on
//! the wire would be an unfenced write.

use std::sync::Arc;

use datafusion::arrow::datatypes::Schema;
use datafusion::common::{Result, internal_datafusion_err};
use datafusion::execution::TaskContext;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::physical_plan::empty::EmptyExec;
use sail_common::spec::FencedWrite;
use sail_common_datafusion::datasource::PhysicalSinkMode;
use sail_delta_lake::physical_plan::{DeltaCommitContext, DeltaCommitExec};
use url::Url;

use super::RemoteExecutionCodec;

fn round_trip(fenced_write: Option<FencedWrite>) -> Result<Option<FencedWrite>> {
    let schema = Arc::new(Schema::empty());
    let table = Url::parse("file:///tmp/table/")
        .map_err(|e| internal_datafusion_err!("invalid test URL: {e}"))?;
    let commit: Arc<dyn ExecutionPlan> = Arc::new(
        DeltaCommitExec::new(
            Arc::new(EmptyExec::new(schema.clone())),
            table,
            vec![],
            true,
            schema,
            PhysicalSinkMode::Append,
            None,
            DeltaCommitContext::default(),
            None,
        )
        .with_fenced_write(fenced_write),
    );
    let encoded = crate::proto::encode_remote_physical_plan(&RemoteExecutionCodec, commit)?;
    let decoded = crate::proto::decode_remote_physical_plan(
        &TaskContext::default(),
        &RemoteExecutionCodec,
        &encoded,
    )?;
    let decoded = decoded
        .downcast_ref::<DeltaCommitExec>()
        .ok_or_else(|| internal_datafusion_err!("decoded plan is not a Delta commit"))?;
    Ok(decoded.fenced_write().cloned())
}

#[test]
fn the_fence_survives_the_codec() -> Result<()> {
    let fence = FencedWrite {
        authority: "authority".into(),
        target: "target".into(),
        generation: 3,
        epoch: 7,
        owner: "owner".into(),
        intent: "intent".into(),
        step: "Auxiliary".into(),
        request_id: "request".into(),
        request_digest: "digest".into(),
        prepared_digest: "prepared".into(),
    };
    assert_eq!(round_trip(Some(fence.clone()))?, Some(fence));
    assert_eq!(round_trip(None)?, None);
    Ok(())
}
