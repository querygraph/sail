//! The control model as a Delta commit authority: what `DeltaCommitExec`
//! calls to ratify a staged effect.

use std::fmt;
use std::sync::Arc;

use async_trait::async_trait;
use datafusion::common::{DataFusionError, Result};
use sail_common::spec::FencedWrite;
use sail_common_datafusion::fenced_commit::{FencedCommitAuthority, FencedEffect, Ratification};
use sail_sdp_control::control::{Control, RequestId, StagedEffect};
use sail_sdp_control::identity::{
    AuthorityIncarnation, EffectStep, FenceToken, IntentId, TargetKey,
};
use sail_sdp_control::store::Store;
use sail_sdp_control::transitions::{Attestation, Outcome};

/// The action digest of an effect with no actions.
const NOOP_DIGEST: &str = "noop";

pub(super) struct ControlCommits<S> {
    control: Arc<Control<S>>,
    ambient: Option<FencedWrite>,
    /// A table whose log does not exist yet, so its first commit has no head
    /// for a fence to check.
    birth: Option<String>,
}

impl<S> ControlCommits<S> {
    pub(super) fn new(control: Arc<Control<S>>, ambient: Option<FencedWrite>) -> Self {
        Self {
            control,
            ambient,
            birth: None,
        }
    }

    pub(super) fn for_birth(control: Arc<Control<S>>, table_url: &str) -> Self {
        Self {
            control,
            ambient: None,
            birth: Some(table_url.to_string()),
        }
    }
}

impl<S> fmt::Debug for ControlCommits<S> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("ControlCommits")
    }
}

fn external(outcome: Outcome) -> DataFusionError {
    DataFusionError::External(Box::new(outcome))
}

pub(super) fn effect_step_name(step: EffectStep) -> Result<String> {
    match serde_json::to_value(step) {
        Ok(serde_json::Value::String(name)) => Ok(name),
        other => Err(DataFusionError::Internal(format!(
            "effect step did not encode as a name: {other:?}"
        ))),
    }
}

fn effect_step(name: &str) -> Result<EffectStep> {
    serde_json::from_value(serde_json::Value::String(name.to_string()))
        .map_err(|e| DataFusionError::Plan(format!("unknown effect step {name}: {e}")))
}

fn head(version: i64) -> Result<u64> {
    u64::try_from(version)
        .map_err(|_| DataFusionError::Internal(format!("negative Delta version {version}")))
}

#[async_trait]
impl<S> FencedCommitAuthority for ControlCommits<S>
where
    S: Store + 'static,
{
    fn ambient_fence(&self) -> Option<FencedWrite> {
        self.ambient.clone()
    }

    async fn is_enrolled(&self, table_url: &str) -> Result<bool> {
        if self.birth.as_deref() == Some(table_url) {
            return Ok(false);
        }
        let target = self.control.enrolled_target(table_url).await;
        Ok(target.map_err(external)?.is_some())
    }

    async fn ratify(&self, fence: &FencedWrite, effect: &FencedEffect) -> Result<Ratification> {
        let token = FenceToken {
            authority: AuthorityIncarnation(fence.authority.clone()),
            target: TargetKey(fence.target.clone()),
            generation: fence.generation,
            epoch: fence.epoch,
            owner: fence.owner.clone(),
        };
        let request = RequestId {
            id: fence.request_id.clone(),
            digest: fence.request_digest.clone(),
        };
        let (staged, action_digest) = match effect {
            FencedEffect::Data {
                version,
                action_digest,
                staged_path,
            } => (
                StagedEffect {
                    expected_head: head(version - 1)?,
                    staged_path: Some(staged_path.clone()),
                },
                action_digest.as_str(),
            ),
            FencedEffect::Noop { checked_head } => (
                StagedEffect {
                    expected_head: head(*checked_head)?,
                    staged_path: None,
                },
                NOOP_DIGEST,
            ),
        };
        let receipt = self
            .control
            .ratify_staged(
                &request,
                &token,
                (&IntentId(fence.intent.clone()), effect_step(&fence.step)?),
                &staged,
                Attestation {
                    action_digest,
                    prepared_digest: &fence.prepared_digest,
                },
            )
            .await
            .map_err(external)?;
        Ok(if receipt.action_digest == action_digest {
            Ratification::Ratified
        } else {
            Ratification::Replayed
        })
    }
}
