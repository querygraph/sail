//! An in-process store for model tests. It applies the transitions under one
//! mutex, which gives the single-process serialisation the design requires
//! of a real adapter, and it says what it is: unfenced across processes and
//! not an authority.

use std::collections::BTreeMap;
use std::sync::Mutex;

use serde::Serialize;
use serde::de::DeserializeOwned;

use crate::identity::{
    AuthorityIncarnation, EffectStep, FenceToken, IntentId, SinkIdentity, TargetKey,
};
use crate::records::{
    Ack, AuthorityMillis, Completion, EffectPrepare, EffectReceipt, FlowCheckpoint, Intent,
    Operation, SinkHead, TargetGuard,
};
use crate::transitions::{self, Acquired, Attestation, Lookup, Outcome, ResetIdentities, Sealed};

/// The store's own statement of its boundary.
pub const AUTHORITY_STATEMENT: &str =
    "in-process memory store: single process, not fenced across processes, not an authority";

#[derive(Debug, Default)]
struct State {
    guards: BTreeMap<TargetKey, TargetGuard>,
    checkpoints: BTreeMap<(TargetKey, u64, String), FlowCheckpoint>,
    intents: BTreeMap<IntentId, Intent>,
    prepares: BTreeMap<(IntentId, EffectStep), EffectPrepare>,
    receipts: BTreeMap<(IntentId, EffectStep), EffectReceipt>,
    heads: BTreeMap<SinkIdentity, SinkHead>,
    operations: BTreeMap<String, Operation>,
    acks: BTreeMap<String, Ack>,
}

impl State {
    fn guard(&self, target: &TargetKey) -> Result<&TargetGuard, Outcome> {
        self.guards
            .get(target)
            .ok_or_else(|| Outcome::IdentityMismatch {
                reason: "target is not enrolled".into(),
            })
    }

    fn intent(&self, id: &IntentId) -> Result<&Intent, Outcome> {
        self.intents
            .get(id)
            .ok_or_else(|| Outcome::EvidenceUnavailable {
                reason: "intent is not sealed".into(),
            })
    }

    fn ack(&self, id: &str) -> Result<&Ack, Outcome> {
        self.acks
            .get(id)
            .ok_or_else(|| Outcome::EvidenceUnavailable {
                reason: "no such ack".into(),
            })
    }

    fn receipts_of(&self, intent: &IntentId) -> Vec<EffectReceipt> {
        self.receipts
            .values()
            .filter(|receipt| &receipt.intent == intent)
            .cloned()
            .collect()
    }

    /// The original response of a replayed request.
    fn replay<T: DeserializeOwned>(&self, request: &RequestId) -> Result<Option<T>, Outcome> {
        match self.operations.get(&request.id) {
            None => Ok(None),
            Some(op) if op.request_digest == request.digest => {
                serde_json::from_value(op.response.clone())
                    .map(Some)
                    .map_err(|e| Outcome::EvidenceUnavailable {
                        reason: e.to_string(),
                    })
            }
            Some(_) => Err(Outcome::PreparedMismatch {
                reason: format!("request {} was recorded with another digest", request.id),
            }),
        }
    }

    fn record<T: Serialize>(&mut self, request: &RequestId, response: &T) -> Result<(), Outcome> {
        let response =
            serde_json::to_value(response).map_err(|e| Outcome::EvidenceUnavailable {
                reason: e.to_string(),
            })?;
        self.operations.insert(
            request.id.clone(),
            Operation {
                request_id: request.id.clone(),
                request_digest: request.digest.clone(),
                response,
            },
        );
        Ok(())
    }
}

#[derive(Debug, Default)]
pub struct UnfencedMemoryStore {
    state: Mutex<State>,
}

/// A request's ledger identity.
#[derive(Clone, Debug)]
pub struct RequestId {
    pub id: String,
    pub digest: String,
}

/// Which ratification to apply.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Effect {
    Data,
    Noop,
}

impl UnfencedMemoryStore {
    pub fn new() -> Self {
        Self::default()
    }

    fn with<T>(&self, f: impl FnOnce(&mut State) -> T) -> T {
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        f(&mut state)
    }

    pub fn enroll(
        &self,
        target: TargetKey,
        authority: AuthorityIncarnation,
        sink: SinkIdentity,
        auxiliary: SinkIdentity,
    ) -> Result<TargetGuard, Outcome> {
        self.with(|state| {
            let guard = transitions::enroll(
                state.guards.get(&target),
                target.clone(),
                authority,
                sink.clone(),
                auxiliary.clone(),
            )?;
            for identity in [sink, auxiliary] {
                state.heads.entry(identity.clone()).or_insert(SinkHead {
                    sink: identity,
                    version: 0,
                });
            }
            state.guards.insert(target, guard.clone());
            Ok(guard)
        })
    }

    pub fn put_checkpoint(&self, checkpoint: FlowCheckpoint) {
        self.with(|state| {
            state.checkpoints.insert(
                (
                    checkpoint.target.clone(),
                    checkpoint.generation,
                    checkpoint.flow.0.clone(),
                ),
                checkpoint,
            );
        })
    }

    pub fn guard(&self, target: &TargetKey) -> Option<TargetGuard> {
        self.with(|state| state.guards.get(target).cloned())
    }

    pub fn head(&self, sink: &SinkIdentity) -> Option<SinkHead> {
        self.with(|state| state.heads.get(sink).cloned())
    }

    pub fn receipt(&self, intent: &IntentId, step: EffectStep) -> Option<EffectReceipt> {
        self.with(|state| state.receipts.get(&(intent.clone(), step)).cloned())
    }

    pub fn intent(&self, id: &IntentId) -> Option<Intent> {
        self.with(|state| state.intents.get(id).cloned())
    }

    pub fn ack(&self, id: &str) -> Option<Ack> {
        self.with(|state| state.acks.get(id).cloned())
    }

    pub fn checkpoint(
        &self,
        target: &TargetKey,
        generation: u64,
        flow: &str,
    ) -> Option<FlowCheckpoint> {
        self.with(|state| {
            state
                .checkpoints
                .get(&(target.clone(), generation, flow.to_string()))
                .cloned()
        })
    }

    pub fn acquire(
        &self,
        target: &TargetKey,
        owner: &str,
        now: AuthorityMillis,
        lease_ms: i64,
    ) -> Result<Acquired, Outcome> {
        self.with(|state| {
            let (next, acquired) =
                transitions::acquire(state.guard(target)?, owner, now, lease_ms)?;
            state.guards.insert(target.clone(), next);
            Ok(acquired)
        })
    }

    pub fn seal_intent(
        &self,
        token: &FenceToken,
        now: AuthorityMillis,
        intent: Intent,
    ) -> Result<Intent, Outcome> {
        self.with(|state| {
            let existing = state.intents.get(&intent.id);
            let (next, sealed) = transitions::seal_intent(
                state.guard(&token.target)?,
                token,
                now,
                intent,
                existing,
            )?;
            state.guards.insert(token.target.clone(), next);
            state.intents.insert(sealed.id.clone(), sealed.clone());
            Ok(sealed)
        })
    }

    pub fn record_prepared(
        &self,
        token: &FenceToken,
        now: AuthorityMillis,
        intent: &IntentId,
        prepared_digest: &str,
    ) -> Result<Intent, Outcome> {
        self.with(|state| {
            let next = transitions::record_prepared(
                state.guard(&token.target)?,
                token,
                now,
                state.intent(intent)?,
                prepared_digest,
            )?;
            state.intents.insert(intent.clone(), next.clone());
            Ok(next)
        })
    }

    pub fn prepare_effect(
        &self,
        token: &FenceToken,
        now: AuthorityMillis,
        prepare: EffectPrepare,
    ) -> Result<EffectPrepare, Outcome> {
        self.with(|state| {
            let auxiliary = state
                .receipts
                .get(&(prepare.intent.clone(), EffectStep::Auxiliary));
            let prepared = transitions::prepare_effect(
                state.guard(&token.target)?,
                token,
                now,
                prepare,
                auxiliary,
            )?;
            let key = (prepared.intent.clone(), prepared.step);
            if state.prepares.get(&key).is_some_and(|p| p != &prepared) {
                return Err(Outcome::PreparedMismatch {
                    reason: "an effect was already prepared differently for this step".into(),
                });
            }
            state.prepares.insert(key, prepared.clone());
            Ok(prepared)
        })
    }

    /// `RatifyData`/`RatifyNoop` with the operation ledger. A replayed
    /// request returns its original receipt. An effect's identity is
    /// `(intent, step)`, so a retry under a new request id also returns the
    /// original receipt, and never a second effect.
    pub fn ratify(
        &self,
        request: &RequestId,
        token: &FenceToken,
        now: AuthorityMillis,
        (intent, step): (&IntentId, EffectStep),
        attestation: Attestation<'_>,
        effect: Effect,
    ) -> Result<EffectReceipt, Outcome> {
        self.with(|state| {
            if let Some(receipt) = state.replay(request)? {
                return Ok(receipt);
            }
            let key = (intent.clone(), step);
            if let Some(receipt) = state.receipts.get(&key) {
                if receipt.action_digest != attestation.action_digest
                    || receipt.prepared_digest != attestation.prepared_digest
                {
                    return Err(Outcome::PreparedMismatch {
                        reason: "the effect was ratified with other digests".into(),
                    });
                }
                return Ok(receipt.clone());
            }
            let guard = state.guard(&token.target)?;
            let prepare = state
                .prepares
                .get(&key)
                .ok_or_else(|| Outcome::EvidenceUnavailable {
                    reason: "effect was not prepared".into(),
                })?;
            let head = state
                .heads
                .get(&prepare.sink)
                .ok_or_else(|| Outcome::IdentityMismatch {
                    reason: "sink has no head".into(),
                })?;
            let sealed = Sealed {
                intent: state.intent(intent)?,
                prepare,
                head,
            };
            let (next, new_head, receipt) = match effect {
                Effect::Data => {
                    let (next, new_head, receipt) =
                        transitions::ratify_data(guard, token, now, sealed, attestation)?;
                    (next, Some(new_head), receipt)
                }
                Effect::Noop => {
                    let (next, receipt) =
                        transitions::ratify_noop(guard, token, now, sealed, attestation)?;
                    (next, None, receipt)
                }
            };
            state.record(request, &receipt)?;
            state.guards.insert(token.target.clone(), next);
            if let Some(new_head) = new_head {
                state.heads.insert(new_head.sink.clone(), new_head);
            }
            state.receipts.insert(key, receipt.clone());
            Ok(receipt)
        })
    }

    pub fn complete_batch(
        &self,
        request: &RequestId,
        token: &FenceToken,
        now: AuthorityMillis,
        intent: &IntentId,
    ) -> Result<Completion, Outcome> {
        self.with(|state| {
            if let Some(completion) = state.replay(request)? {
                return Ok(completion);
            }
            let sealed = state.intent(intent)?;
            let key = (
                sealed.target.clone(),
                sealed.generation,
                sealed.flow.0.clone(),
            );
            let checkpoint =
                state
                    .checkpoints
                    .get(&key)
                    .ok_or_else(|| Outcome::EvidenceUnavailable {
                        reason: "flow has no checkpoint".into(),
                    })?;
            let (next, completion) = transitions::complete_batch(
                state.guard(&token.target)?,
                token,
                now,
                sealed,
                &state.receipts_of(intent),
                checkpoint,
            )?;
            state.record(request, &completion)?;
            state.guards.insert(token.target.clone(), next);
            state.checkpoints.insert(key, completion.checkpoint.clone());
            state
                .acks
                .insert(completion.ack.id.clone(), completion.ack.clone());
            Ok(completion)
        })
    }

    pub fn begin_reset(
        &self,
        token: &FenceToken,
        now: AuthorityMillis,
        reset: Intent,
    ) -> Result<FenceToken, Outcome> {
        self.with(|state| {
            let (next, token) =
                transitions::begin_reset(state.guard(&token.target)?, token, now, &reset)?;
            state.guards.insert(token.target.clone(), next);
            state.intents.insert(reset.id.clone(), reset);
            Ok(token)
        })
    }

    pub fn finalize_reset(
        &self,
        token: &FenceToken,
        now: AuthorityMillis,
        identities: ResetIdentities,
    ) -> Result<TargetGuard, Outcome> {
        self.with(|state| {
            let guard = state.guard(&token.target)?;
            let receipts = guard
                .active_intent
                .as_ref()
                .map(|reset| state.receipts_of(reset))
                .unwrap_or_default();
            let next = transitions::finalize_reset(guard, token, now, &receipts, identities)?;
            for identity in [next.sink.clone(), next.auxiliary.clone()] {
                state.heads.entry(identity.clone()).or_insert(SinkHead {
                    sink: identity,
                    version: 0,
                });
            }
            state.guards.insert(token.target.clone(), next.clone());
            Ok(next)
        })
    }

    pub fn record_ack(
        &self,
        token: &FenceToken,
        now: AuthorityMillis,
        id: &str,
    ) -> Result<Ack, Outcome> {
        self.with(|state| {
            let next =
                transitions::record_ack(state.guard(&token.target)?, token, now, state.ack(id)?)?;
            state.acks.insert(id.to_string(), next.clone());
            Ok(next)
        })
    }

    pub fn resolve_ack(
        &self,
        token: &FenceToken,
        now: AuthorityMillis,
        id: &str,
        confirmed: bool,
    ) -> Result<Ack, Outcome> {
        self.with(|state| {
            let next = transitions::resolve_ack(
                state.guard(&token.target)?,
                token,
                now,
                state.ack(id)?,
                confirmed,
            )?;
            state.acks.insert(id.to_string(), next.clone());
            Ok(next)
        })
    }

    pub fn lookup(&self, request: &RequestId) -> Lookup {
        self.with(|state| {
            transitions::lookup(state.operations.get(&request.id), &request.digest, true)
        })
    }
}
