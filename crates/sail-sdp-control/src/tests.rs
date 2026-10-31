//! Model tests: the fault matrix's interleavings and the worked recovery
//! trace of `CDC-CONTROL-STORE-DESIGN.md`, executed against the in-process
//! store. The same trace against a real authority is the adapter unit's
//! target and is ignored here by design.

use serde_json::json;

use crate::digest::{canonical, digest};
use crate::identity::{
    AuthorityIncarnation, EffectStep, FenceToken, FlowKey, IntentId, SinkIdentity, TargetKey,
};
use crate::memory::{AUTHORITY_STATEMENT, Effect, RequestId, UnfencedMemoryStore};
use crate::records::{
    AckState, Completion, EffectPrepare, EffectReceipt, FlowCheckpoint, Intent, IntentKind,
    ReceiptKind, TargetGuard, TargetState,
};
use crate::transitions::{
    self, Attestation, Lookup, Outcome, ResetIdentities, Sealed, check_fence,
};

type Result<T = ()> = std::result::Result<T, Outcome>;

const LEASE: i64 = 10_000;
const PREPARED: &str = "prepared";

struct Fixture {
    store: UnfencedMemoryStore,
    target: TargetKey,
    flow: FlowKey,
    sink: SinkIdentity,
    auxiliary: SinkIdentity,
}

fn fixture() -> Result<Fixture> {
    let f = Fixture {
        store: UnfencedMemoryStore::new(),
        target: TargetKey("t-1".into()),
        flow: FlowKey("pipe.flow".into()),
        sink: SinkIdentity("delta:aaaa".into()),
        auxiliary: SinkIdentity("delta:bbbb".into()),
    };
    f.store.enroll(
        f.target.clone(),
        AuthorityIncarnation("auth-1".into()),
        f.sink.clone(),
        f.auxiliary.clone(),
    )?;
    f.checkpoint(0);
    Ok(f)
}

fn request(name: &str) -> RequestId {
    RequestId {
        id: name.to_string(),
        digest: format!("d:{name}"),
    }
}

fn attest(step: EffectStep) -> (String, String) {
    (format!("action:{step:?}"), PREPARED.to_string())
}

impl Fixture {
    fn checkpoint(&self, generation: u64) {
        self.store.put_checkpoint(FlowCheckpoint {
            target: self.target.clone(),
            generation,
            flow: self.flow.clone(),
            definition_digest: "def".into(),
            next_batch_sequence: 0,
            progress: json!({"offset": 0}),
            progress_digest: "p0".into(),
        });
    }

    fn intent(&self, generation: u64, sequence: u64) -> Intent {
        let request = json!({"range": [sequence, sequence + 1]});
        Intent {
            id: IntentId::new(&self.target, generation, &self.flow, sequence),
            kind: IntentKind::Batch,
            target: self.target.clone(),
            generation,
            flow: self.flow.clone(),
            batch_sequence: sequence,
            request_digest: digest("intent", &request).unwrap_or_default(),
            request,
            start_progress_digest: format!("p{sequence}"),
            end_progress: json!({"offset": sequence + 1}),
            end_progress_digest: format!("p{}", sequence + 1),
            prepared_digest: None,
        }
    }

    fn reset(&self, generation: u64) -> Intent {
        Intent {
            id: IntentId::reset(&self.target, generation),
            kind: IntentKind::Reset,
            start_progress_digest: String::new(),
            end_progress: json!(null),
            end_progress_digest: String::new(),
            ..self.intent(generation, 0)
        }
    }

    fn prepare(&self, intent: &IntentId, step: EffectStep, expected_head: u64) -> EffectPrepare {
        let sink = match step {
            EffectStep::Auxiliary | EffectStep::ResetAuxiliary => self.auxiliary.clone(),
            _ => self.sink.clone(),
        };
        EffectPrepare {
            intent: intent.clone(),
            step,
            sink,
            expected_head,
            action_digest: attest(step).0,
            request_digest: "req".into(),
        }
    }

    fn head(&self, sink: &SinkIdentity) -> Option<u64> {
        self.store.head(sink).map(|head| head.version)
    }

    fn guard(&self) -> Result<TargetGuard> {
        self.store
            .guard(&self.target)
            .ok_or_else(|| Outcome::EvidenceUnavailable {
                reason: "guard".into(),
            })
    }

    fn seal(&self, token: &FenceToken, now: i64, sequence: u64) -> Result<IntentId> {
        let sealed = self
            .store
            .seal_intent(token, now, self.intent(token.generation, sequence))?;
        self.store
            .record_prepared(token, now, &sealed.id, PREPARED)?;
        Ok(sealed.id)
    }

    fn step(
        &self,
        name: &str,
        token: &FenceToken,
        now: i64,
        intent: &IntentId,
        step: EffectStep,
        effect: Effect,
    ) -> Result<EffectReceipt> {
        let (action_digest, prepared_digest) = attest(step);
        self.store.ratify(
            &request(name),
            token,
            now,
            (intent, step),
            Attestation {
                action_digest: &action_digest,
                prepared_digest: &prepared_digest,
            },
            effect,
        )
    }

    /// Prepare at the current head, then ratify data.
    fn effect(
        &self,
        name: &str,
        token: &FenceToken,
        now: i64,
        intent: &IntentId,
        step: EffectStep,
    ) -> Result<EffectReceipt> {
        let sink = self.prepare(intent, step, 0).sink;
        let head = self.head(&sink).unwrap_or_default();
        self.store
            .prepare_effect(token, now, self.prepare(intent, step, head))?;
        self.step(name, token, now, intent, step, Effect::Data)
    }

    fn run_batch(&self, token: &FenceToken, now: i64, sequence: u64) -> Result<Completion> {
        let id = self.seal(token, now, sequence)?;
        self.effect(
            &format!("aux-{}-{sequence}", token.generation),
            token,
            now,
            &id,
            EffectStep::Auxiliary,
        )?;
        self.effect(
            &format!("target-{}-{sequence}", token.generation),
            token,
            now,
            &id,
            EffectStep::Target,
        )?;
        self.store.complete_batch(
            &request(&format!("complete-{}-{sequence}", token.generation)),
            token,
            now,
            &id,
        )
    }
}

#[test]
fn memory_store_states_its_boundary() {
    assert!(AUTHORITY_STATEMENT.contains("not an authority"));
}

#[test]
fn canonical_encoding_sorts_keys_and_refuses_floats() {
    let a = json!({"b": 1, "a": [1, 2]});
    let b = json!({"a": [1, 2], "b": 1});
    assert_eq!(canonical(&a), canonical(&b));
    assert_eq!(canonical(&json!(1.5)), "\"float:1.5\"");
    assert_ne!(
        digest("x", &a).unwrap_or_default(),
        digest("y", &a).unwrap_or_default(),
        "domain separation"
    );
}

#[test]
fn batches_run_to_completion_and_take_the_sealed_end() -> Result {
    let f = fixture()?;
    let w1 = f.store.acquire(&f.target, "w1", 0, LEASE)?;
    assert_eq!(w1.recovery_only, None);
    let done = f.run_batch(&w1.token, 1, 0)?;
    assert_eq!(done.checkpoint.next_batch_sequence, 1);
    assert_eq!(done.checkpoint.progress, f.intent(0, 0).end_progress);
    assert_eq!(done.ack.cursor, f.intent(0, 0).end_progress);
    assert_eq!(done.ack.state, AckState::Pending);
    assert_eq!(f.head(&f.sink), Some(1));
    assert_eq!(f.head(&f.auxiliary), Some(1));
    assert_eq!(f.guard()?.active_intent, None);
    assert_eq!(f.guard()?.state, TargetState::Running);
    f.run_batch(&w1.token, 2, 1)?;
    assert_eq!(f.head(&f.sink), Some(2));
    Ok(())
}

#[test]
fn enrollment_binds_identities_once() -> Result {
    let f = fixture()?;
    let again = f.store.enroll(
        f.target.clone(),
        AuthorityIncarnation("auth-1".into()),
        f.sink.clone(),
        f.auxiliary.clone(),
    )?;
    assert_eq!(again.revision, 1);
    let recreated = f.store.enroll(
        f.target.clone(),
        AuthorityIncarnation("auth-1".into()),
        SinkIdentity("delta:cccc".into()),
        f.auxiliary.clone(),
    );
    assert!(matches!(recreated, Err(Outcome::IdentityMismatch { .. })));
    Ok(())
}

/// Fault matrix: takeover after the auxiliary effect, and a stale commit
/// racing the takeover.
#[test]
fn expired_owner_is_fenced_and_takeover_preserves_the_intent() -> Result {
    let f = fixture()?;
    let w1 = f.store.acquire(&f.target, "w1", 0, LEASE)?;
    let id = f.seal(&w1.token, 1, 0)?;
    f.effect("aux-0", &w1.token, 2, &id, EffectStep::Auxiliary)?;
    f.store
        .prepare_effect(&w1.token, 2, f.prepare(&id, EffectStep::Target, 0))?;

    assert!(matches!(
        f.store.acquire(&f.target, "w2", 3, LEASE),
        Err(Outcome::Conflict { .. })
    ));
    let now = LEASE + 1;
    let w2 = f.store.acquire(&f.target, "w2", now, LEASE)?;
    assert_eq!(w2.recovery_only, Some(id.clone()));
    assert_eq!(w2.token.epoch, 2);

    let late = f.step(
        "late",
        &w1.token,
        now,
        &id,
        EffectStep::Target,
        Effect::Data,
    );
    assert!(matches!(late, Err(Outcome::Fenced { .. })));
    assert_eq!(f.head(&f.sink), Some(0));

    let resumed = f.store.seal_intent(&w2.token, now, f.intent(0, 0))?;
    assert_eq!(resumed.prepared_digest.as_deref(), Some(PREPARED));
    f.step(
        "target-0",
        &w2.token,
        now,
        &id,
        EffectStep::Target,
        Effect::Data,
    )?;
    assert_eq!(f.head(&f.sink), Some(1));
    f.store
        .complete_batch(&request("complete-0"), &w2.token, now, &id)?;
    Ok(())
}

/// Fault matrix: a lost response and a duplicate request with a changed
/// digest.
#[test]
fn ratification_retry_replays_the_receipt() -> Result {
    let f = fixture()?;
    let w1 = f.store.acquire(&f.target, "w1", 0, LEASE)?;
    let id = f.seal(&w1.token, 1, 0)?;
    let first = f.effect("aux-0", &w1.token, 2, &id, EffectStep::Auxiliary)?;
    let second = f.step(
        "aux-0",
        &w1.token,
        3,
        &id,
        EffectStep::Auxiliary,
        Effect::Data,
    )?;
    assert_eq!(first, second);
    assert_eq!(f.head(&f.auxiliary), Some(1));
    assert_eq!(
        f.store.lookup(&request("aux-0")),
        Lookup::Confirmed(serde_json::to_value(&first).unwrap_or_default())
    );
    let altered = RequestId {
        id: "aux-0".into(),
        digest: "other".into(),
    };
    assert!(matches!(
        f.store.ratify(
            &altered,
            &w1.token,
            4,
            (&id, EffectStep::Auxiliary),
            Attestation {
                action_digest: "action:Auxiliary",
                prepared_digest: PREPARED,
            },
            Effect::Data,
        ),
        Err(Outcome::PreparedMismatch { .. })
    ));
    assert_eq!(f.store.lookup(&altered), Lookup::Mismatch);
    assert_eq!(
        f.store.lookup(&request("never")),
        Lookup::AbsentWithinCoverage
    );
    Ok(())
}

/// An effect's identity survives a new request id: a successor retrying
/// under its own request gets the original receipt, not a second effect.
#[test]
fn a_new_request_id_cannot_repeat_an_effect() -> Result {
    let f = fixture()?;
    let w1 = f.store.acquire(&f.target, "w1", 0, LEASE)?;
    let id = f.seal(&w1.token, 1, 0)?;
    let first = f.effect("aux-w1", &w1.token, 2, &id, EffectStep::Auxiliary)?;
    let w2 = f.store.acquire(&f.target, "w2", LEASE + 1, LEASE)?;
    let again = f.step(
        "aux-w2",
        &w2.token,
        LEASE + 2,
        &id,
        EffectStep::Auxiliary,
        Effect::Data,
    )?;
    assert_eq!(first, again);
    assert_eq!(f.head(&f.auxiliary), Some(1));
    Ok(())
}

#[test]
fn moved_head_is_a_conflict() -> Result {
    let f = fixture()?;
    let w1 = f.store.acquire(&f.target, "w1", 0, LEASE)?;
    let id = f.seal(&w1.token, 1, 0)?;
    f.effect("aux-0", &w1.token, 2, &id, EffectStep::Auxiliary)?;
    f.store
        .prepare_effect(&w1.token, 3, f.prepare(&id, EffectStep::Target, 7))?;
    assert!(matches!(
        f.step(
            "target-0",
            &w1.token,
            4,
            &id,
            EffectStep::Target,
            Effect::Data
        ),
        Err(Outcome::Conflict { .. })
    ));
    assert_eq!(f.head(&f.sink), Some(0));
    Ok(())
}

#[test]
fn ordering_is_enforced_by_evidence() -> Result {
    let f = fixture()?;
    let w1 = f.store.acquire(&f.target, "w1", 0, LEASE)?;
    let id = f.seal(&w1.token, 1, 0)?;
    assert!(matches!(
        f.store
            .prepare_effect(&w1.token, 1, f.prepare(&id, EffectStep::Target, 0)),
        Err(Outcome::EvidenceUnavailable { .. })
    ));
    f.effect("aux-0", &w1.token, 2, &id, EffectStep::Auxiliary)?;
    assert!(matches!(
        f.store
            .complete_batch(&request("complete-0"), &w1.token, 3, &id),
        Err(Outcome::EvidenceUnavailable { .. })
    ));
    let checkpoint = f.store.checkpoint(&f.target, 0, &f.flow.0);
    assert_eq!(checkpoint.map(|c| c.next_batch_sequence), Some(0));
    Ok(())
}

#[test]
fn a_sealed_intent_is_immutable_exclusive_and_finishes_once() -> Result {
    let f = fixture()?;
    let w1 = f.store.acquire(&f.target, "w1", 0, LEASE)?;
    let id = f.seal(&w1.token, 1, 0)?;
    let mut altered = f.intent(0, 0);
    altered.end_progress_digest = "elsewhere".into();
    assert!(matches!(
        f.store.seal_intent(&w1.token, 2, altered),
        Err(Outcome::PreparedMismatch { .. })
    ));
    assert!(matches!(
        f.store.seal_intent(&w1.token, 2, f.intent(0, 1)),
        Err(Outcome::Conflict { .. })
    ));
    assert!(matches!(
        f.store.record_prepared(&w1.token, 2, &id, "another"),
        Err(Outcome::PreparedMismatch { .. })
    ));
    f.effect("aux-0", &w1.token, 3, &id, EffectStep::Auxiliary)?;
    f.effect("target-0", &w1.token, 3, &id, EffectStep::Target)?;
    f.store
        .complete_batch(&request("complete-0"), &w1.token, 3, &id)?;
    assert!(matches!(
        f.store.seal_intent(&w1.token, 4, f.intent(0, 0)),
        Err(Outcome::Conflict { .. })
    ));
    assert_eq!(f.guard()?.active_intent, None);
    Ok(())
}

#[test]
fn prepared_input_mismatch_is_refused() -> Result {
    let f = fixture()?;
    let w1 = f.store.acquire(&f.target, "w1", 0, LEASE)?;
    let id = f.seal(&w1.token, 1, 0)?;
    f.store
        .prepare_effect(&w1.token, 1, f.prepare(&id, EffectStep::Auxiliary, 0))?;
    for (action_digest, prepared_digest) in
        [("action:Auxiliary", "rerun"), ("action:Other", PREPARED)]
    {
        assert!(matches!(
            f.store.ratify(
                &request("aux-0"),
                &w1.token,
                2,
                (&id, EffectStep::Auxiliary),
                Attestation {
                    action_digest,
                    prepared_digest,
                },
                Effect::Data,
            ),
            Err(Outcome::PreparedMismatch { .. })
        ));
    }
    Ok(())
}

/// Fault matrix: no-op followed by later writes. Zero rows is not a receipt:
/// an empty effect records an explicit one, and the next batch's effects
/// still prepare against the unchanged head.
#[test]
fn an_empty_effect_records_a_noop_receipt() -> Result {
    let f = fixture()?;
    let w1 = f.store.acquire(&f.target, "w1", 0, LEASE)?;
    let id = f.seal(&w1.token, 1, 0)?;
    f.store
        .prepare_effect(&w1.token, 1, f.prepare(&id, EffectStep::Auxiliary, 0))?;
    let aux = f.step(
        "aux-0",
        &w1.token,
        2,
        &id,
        EffectStep::Auxiliary,
        Effect::Noop,
    )?;
    assert_eq!(aux.kind, ReceiptKind::Noop { checked_head: 0 });
    f.store
        .prepare_effect(&w1.token, 3, f.prepare(&id, EffectStep::Target, 0))?;
    f.step(
        "target-0",
        &w1.token,
        4,
        &id,
        EffectStep::Target,
        Effect::Noop,
    )?;
    assert_eq!(f.head(&f.sink), Some(0));
    let done = f
        .store
        .complete_batch(&request("complete-0"), &w1.token, 5, &id)?;
    assert_eq!(done.checkpoint.next_batch_sequence, 1);
    f.run_batch(&w1.token, 6, 1)?;
    assert_eq!(f.head(&f.sink), Some(1));
    Ok(())
}

/// Fault matrix: a stale commit racing a reset, target and auxiliary
/// recreation, failure between reset effects. The crash resumes the reset
/// and admits nothing else; the old intent stays sealed and is never
/// advanced or acknowledged.
#[test]
fn reset_fences_the_old_generation_and_resumes_after_a_crash() -> Result {
    let f = fixture()?;
    let w1 = f.store.acquire(&f.target, "w1", 0, LEASE)?;
    let old = f.seal(&w1.token, 1, 0)?;
    f.effect("aux-0", &w1.token, 2, &old, EffectStep::Auxiliary)?;
    f.store
        .prepare_effect(&w1.token, 2, f.prepare(&old, EffectStep::Target, 0))?;

    let reset = f.reset(1);
    let token = f.store.begin_reset(&w1.token, 3, reset.clone())?;
    assert_eq!((token.generation, token.epoch), (1, 2));
    let guard = f.guard()?;
    assert_eq!(guard.state, TargetState::Resetting);
    assert_eq!(guard.checkpoint_index, 1);
    assert_eq!(guard.superseded_intent, Some(old.clone()));
    assert_eq!(guard.active_intent, Some(reset.id.clone()));

    // The old token and the old intent lost authority at once.
    assert!(matches!(
        f.step("late", &w1.token, 4, &old, EffectStep::Target, Effect::Data),
        Err(Outcome::Fenced { .. })
    ));
    assert!(matches!(
        f.store.seal_intent(&w1.token, 4, f.intent(0, 1)),
        Err(Outcome::Fenced { .. })
    ));
    assert!(matches!(
        f.step("late-2", &token, 4, &old, EffectStep::Target, Effect::Data),
        Err(Outcome::Conflict { .. })
    ));
    assert!(matches!(
        f.store.complete_batch(&request("late-3"), &token, 4, &old),
        Err(Outcome::Conflict { .. })
    ));
    // No other work is admitted, and a reset needs both effects.
    assert!(matches!(
        f.store.seal_intent(&token, 4, f.intent(1, 0)),
        Err(Outcome::Conflict { .. })
    ));
    f.effect(
        "reset-target",
        &token,
        5,
        &reset.id,
        EffectStep::ResetTarget,
    )?;
    let new = ResetIdentities {
        sink: f.sink.clone(),
        auxiliary: SinkIdentity("delta:dddd".into()),
    };
    assert!(matches!(
        f.store.finalize_reset(&token, 6, new.clone()),
        Err(Outcome::EvidenceUnavailable { .. })
    ));

    // Crash between the effects: a successor resumes the reset itself.
    let w2 = f.store.acquire(&f.target, "w2", LEASE + 1, LEASE)?;
    assert_eq!(w2.recovery_only, Some(reset.id.clone()));
    assert_eq!(f.guard()?.state, TargetState::Resetting);
    let now = LEASE + 2;
    f.effect(
        "reset-aux",
        &w2.token,
        now,
        &reset.id,
        EffectStep::ResetAuxiliary,
    )?;
    let guard = f.store.finalize_reset(&w2.token, now, new)?;
    assert_eq!(guard.state, TargetState::Running);
    assert_eq!(guard.active_intent, None);

    // The new generation writes the recreated auxiliary from its own head.
    f.checkpoint(1);
    let f2 = Fixture {
        auxiliary: guard.auxiliary.clone(),
        ..f
    };
    f2.run_batch(&w2.token, now, 0)?;
    assert_eq!(f2.head(&f2.auxiliary), Some(1));
    assert_eq!(f2.head(&SinkIdentity("delta:bbbb".into())), Some(2));
    assert_eq!(f2.store.ack(&format!("{}/ack", old.0)), None);
    assert!(f2.store.intent(&old).is_some(), "retained as evidence");
    assert!(f2.store.receipt(&old, EffectStep::Auxiliary).is_some());
    Ok(())
}

/// Fault matrix: a lost ack response and a delayed old-generation ack. Ack
/// uncertainty never touches committed progress.
#[test]
fn acks_are_generation_scoped_and_cannot_undo_data() -> Result {
    let f = fixture()?;
    let w1 = f.store.acquire(&f.target, "w1", 0, LEASE)?;
    let done = f.run_batch(&w1.token, 1, 0)?;
    let ack = done.ack.id;
    assert_eq!(f.store.record_ack(&w1.token, 2, &ack)?.attempts, 1);
    let unknown = f.store.resolve_ack(&w1.token, 3, &ack, false)?;
    assert_eq!(unknown.state, AckState::Unknown);
    let checkpoint = f.store.checkpoint(&f.target, 0, &f.flow.0);
    assert_eq!(checkpoint, Some(done.checkpoint));
    assert_eq!(f.head(&f.sink), Some(1));

    let token = f.store.begin_reset(&w1.token, 4, f.reset(1))?;
    assert!(matches!(
        f.store.resolve_ack(&w1.token, 5, &ack, true),
        Err(Outcome::Fenced { .. })
    ));
    assert!(matches!(
        f.store.record_ack(&token, 5, &ack),
        Err(Outcome::Fenced { .. })
    ));
    Ok(())
}

#[test]
fn a_confirmed_ack_is_terminal() -> Result {
    let f = fixture()?;
    let w1 = f.store.acquire(&f.target, "w1", 0, LEASE)?;
    let ack = f.run_batch(&w1.token, 1, 0)?.ack.id;
    f.store.resolve_ack(&w1.token, 2, &ack, true)?;
    let later = f.store.resolve_ack(&w1.token, 3, &ack, false)?;
    assert_eq!(later.state, AckState::Confirmed);
    assert_eq!(f.store.record_ack(&w1.token, 3, &ack)?.attempts, 0);
    Ok(())
}

/// Each field of the token is checked on its own: a forged or stale token
/// that differs in one field never passes on the strength of the others.
#[test]
fn every_token_field_is_fenced_independently() -> Result {
    let f = fixture()?;
    let token = f.store.acquire(&f.target, "w1", 0, LEASE)?.token;
    let guard = f.guard()?;
    check_fence(&guard, &token, 1)?;
    let stale = [
        FenceToken {
            authority: AuthorityIncarnation("restored".into()),
            ..token.clone()
        },
        FenceToken {
            generation: token.generation + 1,
            ..token.clone()
        },
        FenceToken {
            epoch: token.epoch + 1,
            ..token.clone()
        },
        FenceToken {
            owner: "w2".into(),
            ..token.clone()
        },
    ];
    for bad in stale {
        assert!(
            matches!(check_fence(&guard, &bad, 1), Err(Outcome::Fenced { .. })),
            "{bad:?}"
        );
    }
    let other = FenceToken {
        target: TargetKey("t-2".into()),
        ..token.clone()
    };
    assert!(matches!(
        check_fence(&guard, &other, 1),
        Err(Outcome::IdentityMismatch { .. })
    ));
    Ok(())
}

/// A process restarting under the same owner name takes a new epoch, so its
/// predecessor's in-flight request is still fenced.
#[test]
fn a_restarted_owner_fences_its_own_previous_incarnation() -> Result {
    let f = fixture()?;
    let old = f.store.acquire(&f.target, "w1", 0, LEASE)?.token;
    let id = f.seal(&old, 1, 0)?;
    f.store
        .prepare_effect(&old, 1, f.prepare(&id, EffectStep::Auxiliary, 0))?;
    let now = LEASE + 1;
    let new = f.store.acquire(&f.target, "w1", now, LEASE)?.token;
    assert_eq!(new.owner, old.owner);
    assert!(matches!(
        f.step("aux-0", &old, now, &id, EffectStep::Auxiliary, Effect::Data),
        Err(Outcome::Fenced { .. })
    ));
    assert_eq!(f.head(&f.auxiliary), Some(0));
    Ok(())
}

/// Expiry alone stops an owner, with no takeover having happened: only the
/// authority's clock decides. Renewal under a live lease keeps the epoch.
#[test]
fn an_expired_lease_fences_without_a_takeover() -> Result {
    let f = fixture()?;
    let w1 = f.store.acquire(&f.target, "w1", 0, LEASE)?.token;
    let renewed = f.store.acquire(&f.target, "w1", LEASE - 1, LEASE)?;
    assert_eq!(renewed.token, w1);
    assert!(matches!(
        f.store.seal_intent(&w1, 2 * LEASE - 1, f.intent(0, 0)),
        Err(Outcome::Fenced { .. })
    ));
    Ok(())
}

/// A current token cannot ratify an intent that is not in the slot, even
/// where every other record lines up.
#[test]
fn ratification_needs_the_active_intent() -> Result {
    let f = fixture()?;
    let token = f.store.acquire(&f.target, "w1", 0, LEASE)?.token;
    let guard = f.guard()?;
    let mut intent = f.intent(0, 0);
    intent.prepared_digest = Some(PREPARED.into());
    let prepare = f.prepare(&intent.id, EffectStep::Auxiliary, 0);
    let head = f
        .store
        .head(&f.auxiliary)
        .ok_or_else(|| Outcome::EvidenceUnavailable {
            reason: "head".into(),
        })?;
    assert!(matches!(
        transitions::ratify_data(
            &guard,
            &token,
            1,
            Sealed {
                intent: &intent,
                prepare: &prepare,
                head: &head,
            },
            Attestation {
                action_digest: &prepare.action_digest,
                prepared_digest: PREPARED,
            },
        ),
        Err(Outcome::Conflict { .. })
    ));
    Ok(())
}

/// A batch step cannot be ratified under a reset, even when it was prepared
/// against the reset's own intent.
#[test]
fn a_reset_ratifies_only_reset_steps() -> Result {
    let f = fixture()?;
    let w1 = f.store.acquire(&f.target, "w1", 0, LEASE)?;
    let reset = f.reset(1);
    let token = f.store.begin_reset(&w1.token, 1, reset.clone())?;
    f.store
        .prepare_effect(&token, 2, f.prepare(&reset.id, EffectStep::Auxiliary, 0))?;
    assert!(matches!(
        f.step(
            "aux",
            &token,
            3,
            &reset.id,
            EffectStep::Auxiliary,
            Effect::Data
        ),
        Err(Outcome::Conflict { .. })
    ));
    assert_eq!(f.head(&f.auxiliary), Some(0));
    Ok(())
}

/// Completion rechecks where the flow stands against the sealed intent:
/// progress and sequence both, each on its own.
#[test]
fn completion_refuses_moved_progress_or_sequence() -> Result {
    let f = fixture()?;
    let token = f.store.acquire(&f.target, "w1", 0, LEASE)?.token;
    let id = f.seal(&token, 1, 0)?;
    f.effect("aux-0", &token, 2, &id, EffectStep::Auxiliary)?;
    f.effect("target-0", &token, 2, &id, EffectStep::Target)?;
    let base = f.store.checkpoint(&f.target, 0, &f.flow.0).ok_or_else(|| {
        Outcome::EvidenceUnavailable {
            reason: "checkpoint".into(),
        }
    })?;
    let moved = [
        FlowCheckpoint {
            progress_digest: "moved".into(),
            ..base.clone()
        },
        FlowCheckpoint {
            next_batch_sequence: 5,
            ..base.clone()
        },
    ];
    for (i, checkpoint) in moved.into_iter().enumerate() {
        f.store.put_checkpoint(checkpoint.clone());
        let name = format!("complete-{i}");
        assert!(matches!(
            f.store.complete_batch(&request(&name), &token, 3, &id),
            Err(Outcome::Conflict { .. })
        ));
        assert_eq!(
            f.store.checkpoint(&f.target, 0, &f.flow.0),
            Some(checkpoint)
        );
    }
    Ok(())
}

/// The worked recovery trace (design, "Recovery after a crash between
/// ratifications"): auxiliary ratified, its response lost, crash before the
/// target effect; a new process recovers and completes.
#[test]
fn recovery_trace_against_the_memory_store() -> Result {
    let f = fixture()?;
    let a = f.store.acquire(&f.target, "proc-a", 0, LEASE)?;
    let id = f.seal(&a.token, 1, 0)?;
    let aux = f.effect("aux-0", &a.token, 2, &id, EffectStep::Auxiliary)?;
    assert_eq!(
        aux.kind,
        ReceiptKind::Data {
            ratified_version: 1
        }
    );

    assert!(matches!(
        f.store.acquire(&f.target, "proc-b", 5, LEASE),
        Err(Outcome::Conflict { .. })
    ));
    let b = f.store.acquire(&f.target, "proc-b", LEASE + 1, LEASE)?;
    assert_eq!(
        b.recovery_only,
        Some(id.clone()),
        "recovery before new work"
    );

    // The sealed intent and its prepared digest are the record: the
    // auxiliary receipt exists, so only the target effect runs.
    let now = LEASE + 2;
    let resumed = f.store.seal_intent(&b.token, now, f.intent(0, 0))?;
    assert_eq!(resumed.prepared_digest.as_deref(), Some(PREPARED));
    assert_eq!(f.store.receipt(&id, EffectStep::Auxiliary), Some(aux));
    assert_eq!(f.store.receipt(&id, EffectStep::Target), None);
    let target = f.effect("target-0", &b.token, now, &id, EffectStep::Target)?;
    assert_eq!(
        target.kind,
        ReceiptKind::Data {
            ratified_version: 1
        }
    );
    let done = f
        .store
        .complete_batch(&request("complete-0"), &b.token, now, &id)?;
    assert_eq!(done.checkpoint.next_batch_sequence, 1);

    // A lost completion response replays; a delayed one from A is fenced.
    let replay = f
        .store
        .complete_batch(&request("complete-0"), &b.token, now, &id)?;
    assert_eq!(replay, done);
    assert!(matches!(
        f.store
            .complete_batch(&request("complete-0-a"), &a.token, now, &id),
        Err(Outcome::Fenced { .. })
    ));
    assert_eq!(f.head(&f.sink), Some(1));
    assert_eq!(f.head(&f.auxiliary), Some(1));
    assert_eq!(f.guard()?.active_intent, None);

    f.run_batch(&b.token, now, 1)?;
    assert_eq!(f.head(&f.sink), Some(2));
    Ok(())
}

/// The same trace against a real authority is the SQL adapter unit's target
/// (CONTROL-STORE-REVIEW.md, build plan B2); it is not runnable here.
#[test]
#[ignore = "requires the transactional authority adapter (build unit B2)"]
fn recovery_trace_against_a_real_authority() {}
