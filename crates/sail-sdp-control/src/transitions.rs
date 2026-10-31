//! Guarded transitions (design section "Atomic operations and their
//! linearization points"). Each takes the current records and a sealed
//! request and returns the records to persist, or a typed outcome. An
//! adapter runs exactly one transition inside one transaction that also
//! mutates the target guard row, so conflicting operations serialise.

use serde::{Deserialize, Serialize};

use crate::identity::{
    AuthorityIncarnation, EffectStep, FenceToken, IntentId, SinkIdentity, TargetKey,
};
use crate::records::{
    Ack, AckState, AuthorityMillis, Completion, EffectPrepare, EffectReceipt, FlowCheckpoint,
    Intent, IntentKind, Operation, ReceiptKind, SinkHead, TargetGuard, TargetState,
};

/// Why a transition did not apply. None of these authorises a rerun.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum Outcome {
    /// Another owner, an occupied slot, or a moved head; retry after lookup.
    Conflict { reason: String },
    /// The token no longer names the current authority state.
    Fenced { reason: String },
    /// A sink, target or authority identity differs from the enrolled one.
    IdentityMismatch { reason: String },
    /// The sealed request or prepared artifact differs from the recorded one.
    PreparedMismatch { reason: String },
    /// The source cannot serve a required range; raised by source adapters.
    SourceUnavailable { reason: String },
    /// An external outcome cannot be determined; raised by sink adapters.
    StateUnknown { reason: String },
    /// Required evidence (a predecessor receipt, retained history) is absent.
    EvidenceUnavailable { reason: String },
    /// The requested step is not valid in the current state.
    UnsupportedCapability { reason: String },
}

fn fenced(reason: impl Into<String>) -> Outcome {
    Outcome::Fenced {
        reason: reason.into(),
    }
}

fn conflict(reason: impl Into<String>) -> Outcome {
    Outcome::Conflict {
        reason: reason.into(),
    }
}

/// Every seal, ratification and completion starts here: the token must name
/// the guard's authority, target, generation, epoch and owner, and the lease
/// must be alive by authority time.
pub fn check_fence(
    guard: &TargetGuard,
    token: &FenceToken,
    now: AuthorityMillis,
) -> Result<(), Outcome> {
    if guard.authority != token.authority {
        return Err(fenced("authority incarnation differs"));
    }
    if guard.target != token.target {
        return Err(Outcome::IdentityMismatch {
            reason: "token names another target".into(),
        });
    }
    if guard.generation != token.generation {
        return Err(fenced(format!(
            "generation {} is not the current {}",
            token.generation, guard.generation
        )));
    }
    if guard.epoch != token.epoch {
        return Err(fenced(format!(
            "epoch {} is not the current {}",
            token.epoch, guard.epoch
        )));
    }
    if guard.owner.as_deref() != Some(token.owner.as_str()) {
        return Err(fenced("token owner is not the current owner"));
    }
    match guard.lease_deadline {
        Some(deadline) if deadline > now => Ok(()),
        _ => Err(fenced("lease expired by authority time")),
    }
}

/// `Enroll`: bind the physical identities once; a different identity for an
/// enrolled target is a mismatch, never a transparent replacement.
pub fn enroll(
    existing: Option<&TargetGuard>,
    target: TargetKey,
    physical_key: &str,
    authority: AuthorityIncarnation,
    sink: SinkIdentity,
    auxiliary: SinkIdentity,
) -> Result<TargetGuard, Outcome> {
    if let Some(guard) = existing {
        if guard.physical_key != physical_key || guard.sink != sink || guard.auxiliary != auxiliary
        {
            return Err(Outcome::IdentityMismatch {
                reason: "target is enrolled with other sink identities".into(),
            });
        }
        return Ok(guard.clone());
    }
    Ok(TargetGuard {
        target,
        physical_key: physical_key.to_string(),
        authority,
        sink,
        auxiliary,
        revision: 1,
        epoch: 0,
        generation: 0,
        owner: None,
        lease_deadline: None,
        state: TargetState::Enrolled,
        checkpoint_index: 0,
        active_intent: None,
        superseded_intent: None,
    })
}

/// What an acquisition granted.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Acquired {
    pub token: FenceToken,
    pub lease_deadline: AuthorityMillis,
    /// Unfinished work the new owner must recover before anything else.
    pub recovery_only: Option<IntentId>,
}

fn token_for(guard: &TargetGuard, owner: &str) -> FenceToken {
    FenceToken {
        authority: guard.authority.clone(),
        target: guard.target.clone(),
        generation: guard.generation,
        epoch: guard.epoch,
        owner: owner.to_string(),
    }
}

/// `Acquire/Renew`: the current owner renews under its epoch; anyone else
/// takes over only after the lease expired, which bumps the epoch and
/// preserves the active intent or reset. Expiry is permission, not proof
/// that the old owner's requests stopped: the epoch check in every later
/// transition is what fences them. A resetting target stays resetting, so a
/// crash mid-reset resumes the reset and admits nothing else.
pub fn acquire(
    guard: &TargetGuard,
    owner: &str,
    now: AuthorityMillis,
    lease_ms: i64,
) -> Result<(TargetGuard, Acquired), Outcome> {
    let alive = guard.lease_deadline.is_some_and(|deadline| deadline > now);
    let is_owner = guard.owner.as_deref() == Some(owner);
    if alive && !is_owner {
        return Err(conflict(format!(
            "owned by {} until {}",
            guard.owner.clone().unwrap_or_default(),
            guard.lease_deadline.unwrap_or_default()
        )));
    }
    let mut next = guard.clone();
    if !(alive && is_owner) {
        next.epoch += 1;
    }
    next.owner = Some(owner.to_string());
    next.lease_deadline = Some(now + lease_ms);
    if next.state == TargetState::Enrolled {
        next.state = TargetState::Running;
    }
    next.revision += 1;
    let acquired = Acquired {
        token: token_for(&next, owner),
        lease_deadline: now + lease_ms,
        recovery_only: next.active_intent.clone(),
    };
    Ok((next, acquired))
}

/// `Seal/resume intent`: needs ownership and an empty slot, or the exact
/// prior intent while it is still active. A different request for the same
/// identity is a hard mismatch; a finished intent is not sealed again. The
/// resumed intent keeps its original request, whoever owns the target now.
pub fn seal_intent(
    guard: &TargetGuard,
    token: &FenceToken,
    now: AuthorityMillis,
    intent: Intent,
    existing: Option<&Intent>,
) -> Result<(TargetGuard, Intent), Outcome> {
    check_fence(guard, token, now)?;
    if intent.generation != guard.generation {
        return Err(fenced("intent belongs to another generation"));
    }
    if let Some(prior) = existing {
        if prior.request_digest != intent.request_digest
            || prior.start_progress_digest != intent.start_progress_digest
            || prior.end_progress_digest != intent.end_progress_digest
        {
            return Err(Outcome::PreparedMismatch {
                reason: format!("intent {} was sealed with another request", prior.id.0),
            });
        }
        if guard.active_intent.as_ref() != Some(&prior.id) {
            return Err(conflict(format!("intent {} is finished", prior.id.0)));
        }
        return Ok((guard.clone(), prior.clone()));
    }
    if intent.kind == IntentKind::Reset {
        return Err(Outcome::UnsupportedCapability {
            reason: "a reset is sealed by BeginReset".into(),
        });
    }
    // A resetting target's slot holds the reset, so this also admits nothing
    // else during a reset.
    if let Some(active) = &guard.active_intent {
        return Err(conflict(format!(
            "target slot holds unfinished intent {}",
            active.0
        )));
    }
    let mut next = guard.clone();
    next.active_intent = Some(intent.id.clone());
    next.revision += 1;
    Ok((next, intent))
}

/// Record the prepared-input digest once, under the fence; a second
/// different value is a mismatch.
pub fn record_prepared(
    guard: &TargetGuard,
    token: &FenceToken,
    now: AuthorityMillis,
    intent: &Intent,
    prepared_digest: &str,
) -> Result<Intent, Outcome> {
    check_fence(guard, token, now)?;
    if guard.active_intent.as_ref() != Some(&intent.id) {
        return Err(conflict("intent is not the target's active intent"));
    }
    match intent.prepared_digest.as_deref() {
        Some(existing) if existing != prepared_digest => Err(Outcome::PreparedMismatch {
            reason: "prepared input already recorded with another digest".into(),
        }),
        _ => {
            let mut next = intent.clone();
            next.prepared_digest = Some(prepared_digest.to_string());
            Ok(next)
        }
    }
}

/// `Prepare effect`: a target step needs the auxiliary receipt of the same
/// intent; reset steps need a resetting target.
pub fn prepare_effect(
    guard: &TargetGuard,
    token: &FenceToken,
    now: AuthorityMillis,
    prepare: EffectPrepare,
    auxiliary_receipt: Option<&EffectReceipt>,
) -> Result<EffectPrepare, Outcome> {
    check_fence(guard, token, now)?;
    if guard.active_intent.as_ref() != Some(&prepare.intent) {
        return Err(conflict("intent is not the target's active intent"));
    }
    match prepare.step {
        EffectStep::Target => match auxiliary_receipt {
            Some(receipt)
                if receipt.intent == prepare.intent && receipt.step == EffectStep::Auxiliary => {}
            _ => {
                return Err(Outcome::EvidenceUnavailable {
                    reason: "target step needs the auxiliary receipt of the same intent".into(),
                });
            }
        },
        EffectStep::ResetTarget | EffectStep::ResetAuxiliary => {
            if guard.state != TargetState::Resetting {
                return Err(Outcome::UnsupportedCapability {
                    reason: "reset steps need a resetting target".into(),
                });
            }
        }
        EffectStep::Auxiliary | EffectStep::InitializeTarget | EffectStep::InitializeAuxiliary => {}
    }
    Ok(prepare)
}

/// What the committing side attests about the staged effect.
#[derive(Clone, Copy, Debug)]
pub struct Attestation<'a> {
    pub action_digest: &'a str,
    pub prepared_digest: &'a str,
}

/// The sealed records a ratification is checked against.
#[derive(Clone, Copy, Debug)]
pub struct Sealed<'a> {
    pub intent: &'a Intent,
    pub prepare: &'a EffectPrepare,
    pub head: &'a SinkHead,
    /// The auxiliary receipt of the same intent, which a target step needs.
    pub auxiliary_receipt: Option<&'a EffectReceipt>,
}

/// `Ratify data`: the critical transition. Checks the fence, the active
/// intent, the sealed preparation and prepared input, the sink identity and
/// the original expected head; then advances the head, records the receipt
/// and bumps the guard, all to be persisted in one transaction.
pub fn ratify_data(
    guard: &TargetGuard,
    token: &FenceToken,
    now: AuthorityMillis,
    sealed: Sealed<'_>,
    attestation: Attestation<'_>,
) -> Result<(TargetGuard, SinkHead, EffectReceipt), Outcome> {
    let next = ratify_checks(guard, token, now, sealed, attestation)?;
    let new_head = SinkHead {
        sink: sealed.head.sink.clone(),
        version: sealed.head.version + 1,
    };
    let kind = ReceiptKind::Data {
        ratified_version: new_head.version,
    };
    let receipt = receipt(&next, sealed, attestation, kind);
    Ok((next, new_head, receipt))
}

/// `Ratify no-op`: the same guard and exact head check, an explicit receipt,
/// no head advance. Zero rows is not a receipt; this is.
pub fn ratify_noop(
    guard: &TargetGuard,
    token: &FenceToken,
    now: AuthorityMillis,
    sealed: Sealed<'_>,
    attestation: Attestation<'_>,
) -> Result<(TargetGuard, EffectReceipt), Outcome> {
    let next = ratify_checks(guard, token, now, sealed, attestation)?;
    let kind = ReceiptKind::Noop {
        checked_head: sealed.head.version,
    };
    let receipt = receipt(&next, sealed, attestation, kind);
    Ok((next, receipt))
}

fn receipt(
    guard: &TargetGuard,
    sealed: Sealed<'_>,
    attestation: Attestation<'_>,
    kind: ReceiptKind,
) -> EffectReceipt {
    EffectReceipt {
        intent: sealed.intent.id.clone(),
        step: sealed.prepare.step,
        kind,
        prepared_digest: attestation.prepared_digest.to_string(),
        action_digest: attestation.action_digest.to_string(),
        authority_commit: guard.revision,
    }
}

fn ratify_checks(
    guard: &TargetGuard,
    token: &FenceToken,
    now: AuthorityMillis,
    Sealed {
        intent,
        prepare,
        head,
        auxiliary_receipt,
    }: Sealed<'_>,
    attestation: Attestation<'_>,
) -> Result<TargetGuard, Outcome> {
    check_fence(guard, token, now)?;
    let reset_step = matches!(
        prepare.step,
        EffectStep::ResetTarget | EffectStep::ResetAuxiliary
    );
    if (guard.state == TargetState::Resetting) != reset_step {
        return Err(conflict("step does not match the target state"));
    }
    if guard.active_intent.as_ref() != Some(&intent.id) || prepare.intent != intent.id {
        return Err(conflict("intent is not the target's active intent"));
    }
    if prepare.step == EffectStep::Target && auxiliary_receipt.is_none() {
        return Err(Outcome::EvidenceUnavailable {
            reason: "target step needs the auxiliary receipt of the same intent".into(),
        });
    }
    if prepare.action_digest != attestation.action_digest {
        return Err(Outcome::PreparedMismatch {
            reason: "staged action digest differs from the preparation".into(),
        });
    }
    if intent.kind == IntentKind::Batch
        && intent.prepared_digest.as_deref() != Some(attestation.prepared_digest)
    {
        return Err(Outcome::PreparedMismatch {
            reason: "prepared input digest differs from the sealed intent".into(),
        });
    }
    if head.sink != prepare.sink {
        return Err(Outcome::IdentityMismatch {
            reason: "sink identity differs from the preparation".into(),
        });
    }
    let enrolled = match prepare.step {
        EffectStep::Auxiliary | EffectStep::ResetAuxiliary | EffectStep::InitializeAuxiliary => {
            &guard.auxiliary
        }
        EffectStep::Target | EffectStep::ResetTarget | EffectStep::InitializeTarget => &guard.sink,
    };
    if head.sink != *enrolled {
        return Err(Outcome::IdentityMismatch {
            reason: "sink is not the enrolled incarnation for this step".into(),
        });
    }
    if head.version != prepare.expected_head {
        return Err(conflict(format!(
            "head {} moved past the prepared {}",
            head.version, prepare.expected_head
        )));
    }
    let mut next = guard.clone();
    next.revision += 1;
    Ok(next)
}

/// `Complete batch`: both receipts of the intent and the sealed start
/// progress, then progress set to the sealed end vector, the sequence
/// advanced, the slot cleared and ack work queued, in one transaction.
pub fn complete_batch(
    guard: &TargetGuard,
    token: &FenceToken,
    now: AuthorityMillis,
    intent: &Intent,
    receipts: &[EffectReceipt],
    checkpoint: &FlowCheckpoint,
) -> Result<(TargetGuard, Completion), Outcome> {
    check_fence(guard, token, now)?;
    if intent.kind != IntentKind::Batch {
        return Err(Outcome::UnsupportedCapability {
            reason: "not a batch intent".into(),
        });
    }
    if guard.active_intent.as_ref() != Some(&intent.id) {
        return Err(conflict("intent is not the target's active intent"));
    }
    for step in [EffectStep::Auxiliary, EffectStep::Target] {
        if !receipts
            .iter()
            .any(|receipt| receipt.intent == intent.id && receipt.step == step)
        {
            return Err(Outcome::EvidenceUnavailable {
                reason: format!("no {step:?} receipt for intent {}", intent.id.0),
            });
        }
    }
    if checkpoint.progress_digest != intent.start_progress_digest {
        return Err(conflict("flow progress moved since the intent was sealed"));
    }
    if checkpoint.next_batch_sequence != intent.batch_sequence {
        return Err(conflict("intent sequence is not the flow's next batch"));
    }
    let mut next_guard = guard.clone();
    next_guard.active_intent = None;
    next_guard.revision += 1;
    let mut next_checkpoint = checkpoint.clone();
    next_checkpoint.progress = intent.end_progress.clone();
    next_checkpoint.progress_digest = intent.end_progress_digest.clone();
    next_checkpoint.next_batch_sequence = intent.batch_sequence + 1;
    let ack = Ack {
        id: format!("{}/ack", intent.id.0),
        intent: intent.id.clone(),
        generation: intent.generation,
        cursor: intent.end_progress.clone(),
        state: AckState::Pending,
        attempts: 0,
    };
    Ok((
        next_guard,
        Completion {
            checkpoint: next_checkpoint,
            ack,
        },
    ))
}

/// Finish an Initialization intent: both effects have receipts, data or
/// no-op. It advances no source progress and no batch sequence.
pub fn complete_initialization(
    guard: &TargetGuard,
    token: &FenceToken,
    now: AuthorityMillis,
    intent: &Intent,
    receipts: &[EffectReceipt],
) -> Result<TargetGuard, Outcome> {
    check_fence(guard, token, now)?;
    if intent.kind != IntentKind::Initialization {
        return Err(Outcome::UnsupportedCapability {
            reason: "not an initialization intent".into(),
        });
    }
    if guard.active_intent.as_ref() != Some(&intent.id) {
        return Err(conflict("intent is not the target's active intent"));
    }
    for step in [
        EffectStep::InitializeTarget,
        EffectStep::InitializeAuxiliary,
    ] {
        if !receipts
            .iter()
            .any(|receipt| receipt.intent == intent.id && receipt.step == step)
        {
            return Err(Outcome::EvidenceUnavailable {
                reason: format!("no {step:?} receipt for intent {}", intent.id.0),
            });
        }
    }
    let mut next = guard.clone();
    next.active_intent = None;
    next.revision += 1;
    Ok(next)
}

/// `BeginReset`: bumps generation and epoch so old owners lose ratification
/// authority at once, reserves the next checkpoint index, retires any
/// unfinished intent as superseded and puts the reset in the slot. The
/// returned token is the only one that can run the reset's effects.
pub fn begin_reset(
    guard: &TargetGuard,
    token: &FenceToken,
    now: AuthorityMillis,
    reset: &Intent,
) -> Result<(TargetGuard, FenceToken), Outcome> {
    check_fence(guard, token, now)?;
    if guard.state == TargetState::Resetting {
        return Err(conflict("a reset is already in progress"));
    }
    if reset.kind != IntentKind::Reset || reset.generation != guard.generation + 1 {
        return Err(Outcome::UnsupportedCapability {
            reason: "BeginReset needs a reset intent for the next generation".into(),
        });
    }
    let mut next = guard.clone();
    next.generation += 1;
    next.epoch += 1;
    next.checkpoint_index += 1;
    next.state = TargetState::Resetting;
    next.superseded_intent = next.active_intent.take();
    next.active_intent = Some(reset.id.clone());
    next.revision += 1;
    let token = token_for(&next, &token.owner);
    Ok((next, token))
}

/// The physical identities after a reset. Replacing a table changes its
/// identity, and the auxiliary table is always dropped and recreated.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ResetIdentities {
    pub sink: SinkIdentity,
    pub auxiliary: SinkIdentity,
}

/// `FinalizeReset`: only after both reset effects are confirmed under the
/// new generation; rebinds the identities and enables the generation.
pub fn finalize_reset(
    guard: &TargetGuard,
    token: &FenceToken,
    now: AuthorityMillis,
    receipts: &[EffectReceipt],
    identities: ResetIdentities,
) -> Result<TargetGuard, Outcome> {
    check_fence(guard, token, now)?;
    if guard.state != TargetState::Resetting {
        return Err(Outcome::UnsupportedCapability {
            reason: "target is not resetting".into(),
        });
    }
    let Some(reset) = guard.active_intent.clone() else {
        return Err(Outcome::EvidenceUnavailable {
            reason: "resetting target has no reset intent".into(),
        });
    };
    for step in [EffectStep::ResetTarget, EffectStep::ResetAuxiliary] {
        if !receipts
            .iter()
            .any(|receipt| receipt.intent == reset && receipt.step == step)
        {
            return Err(Outcome::EvidenceUnavailable {
                reason: format!("no {step:?} receipt for reset {}", reset.0),
            });
        }
    }
    let mut next = guard.clone();
    next.sink = identities.sink;
    next.auxiliary = identities.auxiliary;
    next.state = TargetState::Running;
    next.active_intent = None;
    next.revision += 1;
    Ok(next)
}

fn check_ack(
    guard: &TargetGuard,
    token: &FenceToken,
    now: AuthorityMillis,
    ack: &Ack,
) -> Result<(), Outcome> {
    check_fence(guard, token, now)?;
    if ack.generation != guard.generation {
        return Err(fenced("ack belongs to an older generation"));
    }
    Ok(())
}

/// `Record ack`: note an attempt. A confirmed ack needs no further attempt.
pub fn record_ack(
    guard: &TargetGuard,
    token: &FenceToken,
    now: AuthorityMillis,
    ack: &Ack,
) -> Result<Ack, Outcome> {
    check_ack(guard, token, now, ack)?;
    let mut next = ack.clone();
    if next.state != AckState::Confirmed {
        next.attempts += 1;
    }
    Ok(next)
}

/// `Resolve ack`: Confirmed is terminal. Ack uncertainty changes the ack
/// record only, never a confirmed data outcome or committed progress.
pub fn resolve_ack(
    guard: &TargetGuard,
    token: &FenceToken,
    now: AuthorityMillis,
    ack: &Ack,
    confirmed: bool,
) -> Result<Ack, Outcome> {
    check_ack(guard, token, now, ack)?;
    let mut next = ack.clone();
    if next.state != AckState::Confirmed {
        next.state = if confirmed {
            AckState::Confirmed
        } else {
            AckState::Unknown
        };
    }
    Ok(next)
}

/// `Lookup` of a request in the operation ledger.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum Lookup {
    /// The request committed; here is its original response.
    Confirmed(serde_json::Value),
    /// No such request within retained history.
    AbsentWithinCoverage,
    /// The id exists with another digest.
    Mismatch,
    /// Retained history does not cover the request.
    Unknown,
}

pub fn lookup(stored: Option<&Operation>, request_digest: &str, within_coverage: bool) -> Lookup {
    match stored {
        Some(operation) if operation.request_digest == request_digest => {
            Lookup::Confirmed(operation.response.clone())
        }
        Some(_) => Lookup::Mismatch,
        None if within_coverage => Lookup::AbsentWithinCoverage,
        None => Lookup::Unknown,
    }
}
