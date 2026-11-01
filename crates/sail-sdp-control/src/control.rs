//! The operations: each one transaction over a [`Store`], applying a pure
//! transition. The body contains no network or object-store I/O, and it is
//! rerun on a fresh snapshot after a lost write race. Every operation that
//! must conflict with a takeover or reset writes the guard row.

use std::sync::Arc;
use std::time::Duration;

use serde::Serialize;
use serde::de::DeserializeOwned;

use crate::identity::{EffectStep, FenceToken, FlowKey, IntentId, SinkIdentity, TargetKey, escape};
use crate::records::{
    Ack, AuthorityMillis, Completion, EffectPrepare, EffectReceipt, FlowCheckpoint, Intent,
    Operation, RatifiedCommit, SinkHead, TargetGuard,
};
use crate::store::{Clock, Fail, Store, Table, Tx};
use crate::transitions::{self, Acquired, Attestation, Lookup, Outcome, ResetIdentities, Sealed};

const MAX_ATTEMPTS: u32 = 512;

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

/// What a commit staged: the head it was built on and, for data, its entry.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct StagedEffect {
    pub expected_head: u64,
    pub staged_path: Option<String>,
}

impl StagedEffect {
    fn effect(&self) -> Effect {
        if self.staged_path.is_some() {
            Effect::Data
        } else {
            Effect::Noop
        }
    }
}

fn unknown(reason: impl ToString) -> Fail {
    Fail::Final(Outcome::StateUnknown {
        reason: reason.to_string(),
    })
}

fn encode<T: Serialize>(value: &T) -> Result<String, Fail> {
    serde_json::to_string(value).map_err(unknown)
}

fn decode<T: DeserializeOwned>(body: &str) -> Result<T, Fail> {
    serde_json::from_str(body).map_err(unknown)
}

async fn get<T: DeserializeOwned, X: Tx>(
    tx: &X,
    table: Table,
    key: &str,
) -> Result<Option<T>, Fail> {
    tx.get(table, key).await?.as_deref().map(decode).transpose()
}

fn flow_key(target: &TargetKey, generation: u64, flow: &FlowKey) -> String {
    format!("{}/{generation}/{}", escape(&target.0), escape(&flow.0))
}

fn ratified_key(sink: &SinkIdentity, version: u64) -> String {
    format!("{}/{version}", escape(&sink.0))
}

fn step_key(intent: &IntentId, step: EffectStep) -> String {
    format!("{}#{step:?}", intent.0)
}

async fn guard_of<X: Tx>(tx: &X, target: &TargetKey) -> Result<TargetGuard, Fail> {
    get(tx, Table::Target, &target.0).await?.ok_or_else(|| {
        Fail::Final(Outcome::IdentityMismatch {
            reason: "target is not enrolled".into(),
        })
    })
}

async fn put_guard<X: Tx>(tx: &X, guard: &TargetGuard) -> Result<(), Fail> {
    tx.put(Table::Target, &guard.target.0, None, &encode(guard)?)
        .await
}

async fn intent_of<X: Tx>(tx: &X, id: &IntentId) -> Result<Intent, Fail> {
    get(tx, Table::Intent, &id.0).await?.ok_or_else(|| {
        Fail::Final(Outcome::EvidenceUnavailable {
            reason: "intent is not sealed".into(),
        })
    })
}

async fn put_intent<X: Tx>(tx: &X, intent: &Intent) -> Result<(), Fail> {
    tx.put(Table::Intent, &intent.id.0, None, &encode(intent)?)
        .await
}

async fn receipts_of<X: Tx>(tx: &X, intent: &IntentId) -> Result<Vec<EffectReceipt>, Fail> {
    tx.list(Table::Receipt, &intent.0)
        .await?
        .iter()
        .map(|body| decode(body))
        .collect()
}

async fn ack_of<X: Tx>(tx: &X, id: &str) -> Result<Ack, Fail> {
    get(tx, Table::Ack, id).await?.ok_or_else(|| {
        Fail::Final(Outcome::EvidenceUnavailable {
            reason: "no such ack".into(),
        })
    })
}

async fn put_ack<X: Tx>(tx: &X, ack: &Ack) -> Result<(), Fail> {
    tx.put(Table::Ack, &ack.id, Some(&ack.intent.0), &encode(ack)?)
        .await
}

/// The original response of a replayed request.
async fn replay<T: DeserializeOwned, X: Tx>(
    tx: &X,
    request: &RequestId,
) -> Result<Option<T>, Fail> {
    let stored: Option<Operation> = get(tx, Table::Operation, &request.id).await?;
    match stored {
        None => Ok(None),
        Some(operation) if operation.request_digest == request.digest => {
            serde_json::from_value(operation.response)
                .map(Some)
                .map_err(unknown)
        }
        Some(_) => Err(Fail::Final(Outcome::PreparedMismatch {
            reason: format!("request {} was recorded with another digest", request.id),
        })),
    }
}

async fn record<T: Serialize, X: Tx>(
    tx: &X,
    request: &RequestId,
    response: &T,
) -> Result<(), Fail> {
    let operation = Operation {
        request_id: request.id.clone(),
        request_digest: request.digest.clone(),
        response: serde_json::to_value(response).map_err(unknown)?,
    };
    tx.put(Table::Operation, &request.id, None, &encode(&operation)?)
        .await
}

async fn ensure_head<X: Tx>(tx: &X, sink: &SinkIdentity, version: u64) -> Result<(), Fail> {
    if tx.get(Table::Head, &sink.0).await?.is_none() {
        let head = SinkHead {
            sink: sink.clone(),
            version,
        };
        tx.put(Table::Head, &sink.0, None, &encode(&head)?).await?;
    }
    Ok(())
}

async fn ratify_in<X: Tx>(
    tx: &X,
    now: AuthorityMillis,
    request: &RequestId,
    token: &FenceToken,
    (intent, step): (&IntentId, EffectStep),
    attestation: Attestation<'_>,
    effect: Effect,
) -> Result<EffectReceipt, Fail> {
    if let Some(receipt) = replay(tx, request).await? {
        return Ok(receipt);
    }
    let key = step_key(intent, step);
    let existing: Option<EffectReceipt> = get(tx, Table::Receipt, &key).await?;
    if let Some(receipt) = existing {
        if receipt.action_digest != attestation.action_digest
            || receipt.prepared_digest != attestation.prepared_digest
        {
            return Err(Fail::Final(Outcome::PreparedMismatch {
                reason: "the effect was ratified with other digests".into(),
            }));
        }
        return Ok(receipt);
    }
    let guard = guard_of(tx, &token.target).await?;
    let prepare: EffectPrepare = get(tx, Table::Prepare, &key).await?.ok_or_else(|| {
        Fail::Final(Outcome::EvidenceUnavailable {
            reason: "effect was not prepared".into(),
        })
    })?;
    let head: SinkHead = get(tx, Table::Head, &prepare.sink.0)
        .await?
        .ok_or_else(|| {
            Fail::Final(Outcome::IdentityMismatch {
                reason: "sink has no head".into(),
            })
        })?;
    let sealed_intent = intent_of(tx, intent).await?;
    let auxiliary: Option<EffectReceipt> =
        get(tx, Table::Receipt, &step_key(intent, EffectStep::Auxiliary)).await?;
    let sealed = Sealed {
        intent: &sealed_intent,
        prepare: &prepare,
        head: &head,
        auxiliary_receipt: auxiliary.as_ref(),
    };
    let (next, receipt) = match effect {
        Effect::Data => {
            let (next, new_head, receipt) =
                transitions::ratify_data(&guard, token, now, sealed, attestation)?;
            tx.put(Table::Head, &new_head.sink.0, None, &encode(&new_head)?)
                .await?;
            (next, receipt)
        }
        Effect::Noop => transitions::ratify_noop(&guard, token, now, sealed, attestation)?,
    };
    record(tx, request, &receipt).await?;
    put_guard(tx, &next).await?;
    tx.put(Table::Receipt, &key, Some(&intent.0), &encode(&receipt)?)
        .await?;
    Ok(receipt)
}

/// Run `$body` in a transaction, rerunning it on a lost write race. A failed
/// commit that is not a lost race is reported as unknown, never as absent.
macro_rules! transact {
    ($self:ident, |$tx:ident| $body:block) => {{
        let mut attempt = 0;
        loop {
            attempt += 1;
            if attempt > MAX_ATTEMPTS {
                break Err(Outcome::Conflict {
                    reason: "contention retries exhausted".into(),
                });
            }
            let $tx = match $self.store.begin().await {
                Ok(tx) => tx,
                Err(Fail::Retry) => {
                    backoff(attempt).await;
                    continue;
                }
                Err(Fail::Final(outcome)) => break Err(outcome),
            };
            let result = async { $body }.await;
            let result = match result {
                Ok(value) => $tx.commit().await.map(|()| value),
                Err(fail) => Err(fail),
            };
            match result {
                Ok(value) => break Ok(value),
                Err(Fail::Retry) => {
                    $tx.rollback().await;
                    backoff(attempt).await;
                }
                Err(Fail::Final(outcome)) => {
                    $tx.rollback().await;
                    break Err(outcome);
                }
            }
        }
    }};
}

async fn backoff(attempt: u32) {
    tokio::time::sleep(Duration::from_micros(u64::from(attempt.min(100)) * 100)).await;
}

/// The control authority's operations over one store and one clock.
pub struct Control<S> {
    store: S,
    clock: Arc<dyn Clock>,
}

impl<S: Store> Control<S> {
    pub fn new(store: S, clock: Arc<dyn Clock>) -> Self {
        Self { store, clock }
    }

    pub async fn enroll(
        &self,
        target: &TargetKey,
        physical_key: &str,
        sink: &SinkIdentity,
        auxiliary: &SinkIdentity,
        (sink_head, auxiliary_head): (u64, u64),
    ) -> Result<TargetGuard, Outcome> {
        transact!(self, |tx| {
            let bound: Option<TargetKey> = get(&tx, Table::Physical, physical_key).await?;
            if bound.as_ref().is_some_and(|bound| bound != target) {
                return Err(Fail::Final(Outcome::IdentityMismatch {
                    reason: "another target enrolled this physical table".into(),
                }));
            }
            let existing: Option<TargetGuard> = get(&tx, Table::Target, &target.0).await?;
            let guard = transitions::enroll(
                existing.as_ref(),
                target.clone(),
                physical_key,
                self.store.authority(),
                sink.clone(),
                auxiliary.clone(),
            )?;
            tx.put(Table::Physical, physical_key, None, &encode(target)?)
                .await?;
            ensure_head(&tx, sink, sink_head).await?;
            ensure_head(&tx, auxiliary, auxiliary_head).await?;
            put_guard(&tx, &guard).await?;
            Ok::<_, Fail>(guard)
        })
    }

    /// Bind another physical table to an enrolled target, as the auxiliary
    /// table of the group is.
    pub async fn bind_physical(
        &self,
        target: &TargetKey,
        physical_key: &str,
    ) -> Result<(), Outcome> {
        transact!(self, |tx| {
            let bound: Option<TargetKey> = get(&tx, Table::Physical, physical_key).await?;
            if bound.as_ref().is_some_and(|bound| bound != target) {
                return Err(Fail::Final(Outcome::IdentityMismatch {
                    reason: "another target enrolled this physical table".into(),
                }));
            }
            let guard = guard_of(&tx, target).await?;
            tx.put(Table::Physical, physical_key, None, &encode(target)?)
                .await?;
            put_guard(&tx, &guard).await?;
            Ok::<_, Fail>(())
        })
    }

    /// Register a flow's starting checkpoint at enrollment.
    pub async fn put_checkpoint(&self, checkpoint: &FlowCheckpoint) -> Result<(), Outcome> {
        transact!(self, |tx| {
            let key = flow_key(&checkpoint.target, checkpoint.generation, &checkpoint.flow);
            tx.put(Table::Flow, &key, None, &encode(checkpoint)?)
                .await?;
            Ok::<_, Fail>(())
        })
    }

    pub async fn guard(&self, target: &TargetKey) -> Result<Option<TargetGuard>, Outcome> {
        transact!(self, |tx| { get(&tx, Table::Target, &target.0).await })
    }

    pub async fn head(&self, sink: &SinkIdentity) -> Result<Option<SinkHead>, Outcome> {
        transact!(self, |tx| { get(&tx, Table::Head, &sink.0).await })
    }

    pub async fn receipt(
        &self,
        intent: &IntentId,
        step: EffectStep,
    ) -> Result<Option<EffectReceipt>, Outcome> {
        transact!(self, |tx| {
            get(&tx, Table::Receipt, &step_key(intent, step)).await
        })
    }

    pub async fn intent(&self, id: &IntentId) -> Result<Option<Intent>, Outcome> {
        transact!(self, |tx| { get(&tx, Table::Intent, &id.0).await })
    }

    pub async fn ack(&self, id: &str) -> Result<Option<Ack>, Outcome> {
        transact!(self, |tx| { get(&tx, Table::Ack, id).await })
    }

    pub async fn checkpoint(
        &self,
        target: &TargetKey,
        generation: u64,
        flow: &FlowKey,
    ) -> Result<Option<FlowCheckpoint>, Outcome> {
        transact!(self, |tx| {
            get(&tx, Table::Flow, &flow_key(target, generation, flow)).await
        })
    }

    pub async fn acquire(
        &self,
        target: &TargetKey,
        owner: &str,
        lease_ms: i64,
    ) -> Result<Acquired, Outcome> {
        transact!(self, |tx| {
            let guard = guard_of(&tx, target).await?;
            let (next, acquired) = transitions::acquire(&guard, owner, self.clock.now(), lease_ms)?;
            put_guard(&tx, &next).await?;
            Ok::<_, Fail>(acquired)
        })
    }

    pub async fn release(&self, token: &FenceToken) -> Result<(), Outcome> {
        transact!(self, |tx| {
            let guard = guard_of(&tx, &token.target).await?;
            let next = transitions::release(&guard, token, self.clock.now())?;
            put_guard(&tx, &next).await?;
            Ok::<_, Fail>(())
        })
    }

    pub async fn seal_intent(
        &self,
        token: &FenceToken,
        intent: &Intent,
    ) -> Result<Intent, Outcome> {
        transact!(self, |tx| {
            let guard = guard_of(&tx, &token.target).await?;
            let existing: Option<Intent> = get(&tx, Table::Intent, &intent.id.0).await?;
            let (next, sealed) = transitions::seal_intent(
                &guard,
                token,
                self.clock.now(),
                intent.clone(),
                existing.as_ref(),
            )?;
            put_guard(&tx, &next).await?;
            if existing.is_none() {
                put_intent(&tx, &sealed).await?;
            }
            Ok::<_, Fail>(sealed)
        })
    }

    pub async fn record_prepared(
        &self,
        token: &FenceToken,
        intent: &IntentId,
        prepared_digest: &str,
    ) -> Result<Intent, Outcome> {
        transact!(self, |tx| {
            let guard = guard_of(&tx, &token.target).await?;
            let sealed = intent_of(&tx, intent).await?;
            let next = transitions::record_prepared(
                &guard,
                token,
                self.clock.now(),
                &sealed,
                prepared_digest,
            )?;
            put_guard(&tx, &guard).await?;
            put_intent(&tx, &next).await?;
            Ok::<_, Fail>(next)
        })
    }

    pub async fn prepare_effect(
        &self,
        token: &FenceToken,
        prepare: &EffectPrepare,
    ) -> Result<EffectPrepare, Outcome> {
        transact!(self, |tx| {
            let guard = guard_of(&tx, &token.target).await?;
            let auxiliary: Option<EffectReceipt> = get(
                &tx,
                Table::Receipt,
                &step_key(&prepare.intent, EffectStep::Auxiliary),
            )
            .await?;
            let prepared = transitions::prepare_effect(
                &guard,
                token,
                self.clock.now(),
                prepare.clone(),
                auxiliary.as_ref(),
            )?;
            let key = step_key(&prepared.intent, prepared.step);
            let existing: Option<EffectPrepare> = get(&tx, Table::Prepare, &key).await?;
            match existing {
                Some(existing) if existing != prepared => {
                    return Err(Fail::Final(Outcome::PreparedMismatch {
                        reason: "an effect was already prepared differently for this step".into(),
                    }));
                }
                Some(_) => {}
                None => {
                    tx.put(
                        Table::Prepare,
                        &key,
                        Some(&prepared.intent.0),
                        &encode(&prepared)?,
                    )
                    .await?;
                }
            }
            put_guard(&tx, &guard).await?;
            Ok::<_, Fail>(prepared)
        })
    }

    /// `RatifyData`/`RatifyNoop` with the operation ledger. A replayed
    /// request returns its original receipt. An effect's identity is
    /// `(intent, step)`, so a retry under a new request id also returns the
    /// original receipt, and never a second effect.
    pub async fn ratify(
        &self,
        request: &RequestId,
        token: &FenceToken,
        intent: &IntentId,
        step: EffectStep,
        attestation: Attestation<'_>,
        effect: Effect,
    ) -> Result<EffectReceipt, Outcome> {
        transact!(self, |tx| {
            ratify_in(
                &tx,
                self.clock.now(),
                request,
                token,
                (intent, step),
                attestation,
                effect,
            )
            .await
        })
    }

    /// Ratify a staged effect at commit time, in one transaction with its
    /// preparation. An unratified preparation has no effect, so a new attempt
    /// under the same effect identity replaces it; a ratified one is
    /// answered from its receipt.
    pub async fn ratify_staged(
        &self,
        request: &RequestId,
        token: &FenceToken,
        (intent, step): (&IntentId, EffectStep),
        staged: &StagedEffect,
        attestation: Attestation<'_>,
    ) -> Result<EffectReceipt, Outcome> {
        transact!(self, |tx| {
            let key = step_key(intent, step);
            let done = tx.get(Table::Operation, &request.id).await?.is_some()
                || tx.get(Table::Receipt, &key).await?.is_some();
            let guard = guard_of(&tx, &token.target).await?;
            let sink = transitions::enrolled_sink(&guard, step).clone();
            if !done {
                let auxiliary: Option<EffectReceipt> = get(
                    &tx,
                    Table::Receipt,
                    &step_key(intent, EffectStep::Auxiliary),
                )
                .await?;
                let prepare = transitions::prepare_effect(
                    &guard,
                    token,
                    self.clock.now(),
                    EffectPrepare {
                        intent: intent.clone(),
                        step,
                        sink: sink.clone(),
                        expected_head: staged.expected_head,
                        action_digest: attestation.action_digest.to_string(),
                        request_digest: request.digest.clone(),
                    },
                    auxiliary.as_ref(),
                )?;
                tx.put(Table::Prepare, &key, Some(&intent.0), &encode(&prepare)?)
                    .await?;
            }
            let receipt = ratify_in(
                &tx,
                self.clock.now(),
                request,
                token,
                (intent, step),
                attestation,
                staged.effect(),
            )
            .await?;
            if let (false, Some(staged_path)) = (done, &staged.staged_path) {
                let commit = RatifiedCommit {
                    sink: sink.clone(),
                    version: staged.expected_head + 1,
                    staged_path: staged_path.clone(),
                    action_digest: attestation.action_digest.to_string(),
                    intent: intent.clone(),
                    step,
                };
                tx.put(
                    Table::RatifiedCommit,
                    &ratified_key(&sink, commit.version),
                    Some(&intent.0),
                    &encode(&commit)?,
                )
                .await?;
            }
            Ok::<_, Fail>(receipt)
        })
    }

    pub async fn ratified_commit(
        &self,
        sink: &SinkIdentity,
        version: u64,
    ) -> Result<Option<RatifiedCommit>, Outcome> {
        transact!(self, |tx| {
            get(&tx, Table::RatifiedCommit, &ratified_key(sink, version)).await
        })
    }

    /// The target this physical table is enrolled under.
    pub async fn enrolled_target(&self, physical_key: &str) -> Result<Option<TargetKey>, Outcome> {
        transact!(self, |tx| { get(&tx, Table::Physical, physical_key).await })
    }

    pub async fn complete_batch(
        &self,
        request: &RequestId,
        token: &FenceToken,
        intent: &IntentId,
    ) -> Result<Completion, Outcome> {
        transact!(self, |tx| {
            if let Some(completion) = replay(&tx, request).await? {
                return Ok(completion);
            }
            let guard = guard_of(&tx, &token.target).await?;
            let sealed = intent_of(&tx, intent).await?;
            let key = flow_key(&sealed.target, sealed.generation, &sealed.flow);
            let checkpoint: FlowCheckpoint =
                get(&tx, Table::Flow, &key).await?.ok_or_else(|| {
                    Fail::Final(Outcome::EvidenceUnavailable {
                        reason: "flow has no checkpoint".into(),
                    })
                })?;
            let (next, completion) = transitions::complete_batch(
                &guard,
                token,
                self.clock.now(),
                &sealed,
                &receipts_of(&tx, intent).await?,
                &checkpoint,
            )?;
            record(&tx, request, &completion).await?;
            put_guard(&tx, &next).await?;
            tx.put(Table::Flow, &key, None, &encode(&completion.checkpoint)?)
                .await?;
            put_ack(&tx, &completion.ack).await?;
            Ok::<_, Fail>(completion)
        })
    }

    pub async fn complete_initialization(
        &self,
        token: &FenceToken,
        intent: &IntentId,
    ) -> Result<TargetGuard, Outcome> {
        transact!(self, |tx| {
            let guard = guard_of(&tx, &token.target).await?;
            let sealed = intent_of(&tx, intent).await?;
            let next = transitions::complete_initialization(
                &guard,
                token,
                self.clock.now(),
                &sealed,
                &receipts_of(&tx, intent).await?,
            )?;
            put_guard(&tx, &next).await?;
            Ok::<_, Fail>(next)
        })
    }

    pub async fn begin_reset(
        &self,
        token: &FenceToken,
        reset: &Intent,
    ) -> Result<FenceToken, Outcome> {
        transact!(self, |tx| {
            let guard = guard_of(&tx, &token.target).await?;
            let (next, new_token) =
                transitions::begin_reset(&guard, token, self.clock.now(), reset)?;
            put_guard(&tx, &next).await?;
            put_intent(&tx, reset).await?;
            Ok::<_, Fail>(new_token)
        })
    }

    pub async fn rebind_auxiliary(
        &self,
        token: &FenceToken,
        auxiliary: &SinkIdentity,
        head: u64,
    ) -> Result<TargetGuard, Outcome> {
        transact!(self, |tx| {
            let guard = guard_of(&tx, &token.target).await?;
            let next =
                transitions::rebind_auxiliary(&guard, token, self.clock.now(), auxiliary.clone())?;
            ensure_head(&tx, auxiliary, head).await?;
            put_guard(&tx, &next).await?;
            Ok::<_, Fail>(next)
        })
    }

    pub async fn finalize_reset(
        &self,
        token: &FenceToken,
        identities: &ResetIdentities,
    ) -> Result<TargetGuard, Outcome> {
        transact!(self, |tx| {
            let guard = guard_of(&tx, &token.target).await?;
            let receipts = match &guard.active_intent {
                Some(reset) => receipts_of(&tx, reset).await?,
                None => Vec::new(),
            };
            let next = transitions::finalize_reset(
                &guard,
                token,
                self.clock.now(),
                &receipts,
                identities.clone(),
            )?;
            ensure_head(&tx, &next.sink, 0).await?;
            ensure_head(&tx, &next.auxiliary, identities.auxiliary_head).await?;
            put_guard(&tx, &next).await?;
            Ok::<_, Fail>(next)
        })
    }

    pub async fn record_ack(&self, token: &FenceToken, id: &str) -> Result<Ack, Outcome> {
        transact!(self, |tx| {
            let guard = guard_of(&tx, &token.target).await?;
            let ack = ack_of(&tx, id).await?;
            let next = transitions::record_ack(&guard, token, self.clock.now(), &ack)?;
            put_guard(&tx, &guard).await?;
            put_ack(&tx, &next).await?;
            Ok::<_, Fail>(next)
        })
    }

    pub async fn resolve_ack(
        &self,
        token: &FenceToken,
        id: &str,
        confirmed: bool,
    ) -> Result<Ack, Outcome> {
        transact!(self, |tx| {
            let guard = guard_of(&tx, &token.target).await?;
            let ack = ack_of(&tx, id).await?;
            let next = transitions::resolve_ack(&guard, token, self.clock.now(), &ack, confirmed)?;
            put_guard(&tx, &guard).await?;
            put_ack(&tx, &next).await?;
            Ok::<_, Fail>(next)
        })
    }

    pub async fn lookup(&self, request: &RequestId) -> Result<Lookup, Outcome> {
        transact!(self, |tx| {
            let stored: Option<Operation> = get(&tx, Table::Operation, &request.id).await?;
            Ok::<_, Fail>(transitions::lookup(stored.as_ref(), &request.digest, true))
        })
    }
}
