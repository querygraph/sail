//! The fault matrix's interleavings and the worked recovery trace of
//! `CDC-CONTROL-STORE-DESIGN.md`. Every scenario runs against each backend
//! through one driver, so the in-process store and the authority adapter are
//! held to the same behaviour.

use std::sync::Arc;
use std::sync::atomic::{AtomicI64, Ordering};

use serde_json::json;

use crate::control::{Control, Effect, RequestId};
use crate::digest::{canonical, digest};
use crate::identity::{
    AuthorityIncarnation, EffectStep, FenceToken, FlowKey, IntentId, SinkIdentity, TargetKey,
};
use crate::memory::{AUTHORITY_STATEMENT, UnfencedMemoryStore};
use crate::records::{
    Ack, AckState, Completion, EffectPrepare, EffectReceipt, FlowCheckpoint, Intent, IntentKind,
    ReceiptKind, SinkHead, TargetGuard, TargetState,
};
use crate::store::{Clock, Store};
use crate::transitions::{
    self, Acquired, Attestation, Lookup, Outcome, ResetIdentities, Sealed, check_fence,
};

type Result<T = ()> = std::result::Result<T, Outcome>;

const LEASE: i64 = 10_000;
const PREPARED: &str = "prepared";
const PHYSICAL: &str = "s3://bucket/target";

#[derive(Default)]
struct ManualClock(AtomicI64);

impl Clock for ManualClock {
    fn now(&self) -> i64 {
        self.0.load(Ordering::SeqCst)
    }
}

/// Drives the async operations with a chosen authority time.
struct Driver<S> {
    control: Control<S>,
    clock: Arc<ManualClock>,
    runtime: tokio::runtime::Runtime,
}

impl<S: Store> Driver<S> {
    fn new(store: S) -> Self {
        let clock = Arc::new(ManualClock::default());
        Self {
            control: Control::new(store, clock.clone()),
            clock,
            runtime: tokio::runtime::Builder::new_current_thread()
                .enable_time()
                .build()
                .unwrap_or_else(|e| unreachable!("{e}")),
        }
    }

    fn at(&self, now: i64) -> &Control<S> {
        self.clock.0.store(now, Ordering::SeqCst);
        &self.control
    }

    fn run<T>(&self, future: impl Future<Output = T>) -> T {
        self.runtime.block_on(future)
    }

    fn enroll(
        &self,
        target: TargetKey,
        physical: &str,
        sink: SinkIdentity,
        auxiliary: SinkIdentity,
    ) -> Result<TargetGuard> {
        self.run(
            self.control
                .enroll(&target, physical, &sink, &auxiliary, (0, 0)),
        )
    }

    fn put_checkpoint(&self, checkpoint: FlowCheckpoint) -> Result {
        self.run(self.control.put_checkpoint(&checkpoint))
    }

    fn guard(&self, target: &TargetKey) -> Option<TargetGuard> {
        self.run(self.control.guard(target)).ok().flatten()
    }

    fn head(&self, sink: &SinkIdentity) -> Option<SinkHead> {
        self.run(self.control.head(sink)).ok().flatten()
    }

    fn receipt(&self, intent: &IntentId, step: EffectStep) -> Option<EffectReceipt> {
        self.run(self.control.receipt(intent, step)).ok().flatten()
    }

    fn intent(&self, id: &IntentId) -> Option<Intent> {
        self.run(self.control.intent(id)).ok().flatten()
    }

    fn ack(&self, id: &str) -> Option<Ack> {
        self.run(self.control.ack(id)).ok().flatten()
    }

    fn checkpoint(
        &self,
        target: &TargetKey,
        generation: u64,
        flow: &str,
    ) -> Option<FlowCheckpoint> {
        let flow = FlowKey(flow.to_string());
        self.run(self.control.checkpoint(target, generation, &flow))
            .ok()
            .flatten()
    }

    fn acquire(&self, target: &TargetKey, owner: &str, now: i64, lease: i64) -> Result<Acquired> {
        self.run(self.at(now).acquire(target, owner, lease))
    }

    fn release(&self, token: &FenceToken, now: i64) -> Result {
        self.run(self.at(now).release(token))
    }

    fn renew(&self, target: &TargetKey, owner: &str, now: i64, lease: i64) -> Result<i64> {
        self.run(self.at(now).renew(target, owner, lease))
    }

    fn seal_intent(&self, token: &FenceToken, now: i64, intent: Intent) -> Result<Intent> {
        self.run(self.at(now).seal_intent(token, &intent))
    }

    fn record_prepared(
        &self,
        token: &FenceToken,
        now: i64,
        intent: &IntentId,
        digest: &str,
    ) -> Result<Intent> {
        self.run(self.at(now).record_prepared(token, intent, digest))
    }

    fn prepare_effect(
        &self,
        token: &FenceToken,
        now: i64,
        prepare: EffectPrepare,
    ) -> Result<EffectPrepare> {
        self.run(self.at(now).prepare_effect(token, &prepare))
    }

    fn ratify(
        &self,
        request: &RequestId,
        token: &FenceToken,
        now: i64,
        (intent, step): (&IntentId, EffectStep),
        attestation: Attestation<'_>,
        effect: Effect,
    ) -> Result<EffectReceipt> {
        self.run(
            self.at(now)
                .ratify(request, token, intent, step, attestation, effect),
        )
    }

    fn complete_batch(
        &self,
        request: &RequestId,
        token: &FenceToken,
        now: i64,
        intent: &IntentId,
    ) -> Result<Completion> {
        self.run(self.at(now).complete_batch(request, token, intent))
    }

    fn complete_initialization(
        &self,
        token: &FenceToken,
        now: i64,
        intent: &IntentId,
    ) -> Result<TargetGuard> {
        self.run(self.at(now).complete_initialization(token, intent))
    }

    fn begin_reset(&self, token: &FenceToken, now: i64, reset: Intent) -> Result<FenceToken> {
        self.run(self.at(now).begin_reset(token, &reset))
    }

    fn finalize_reset(
        &self,
        token: &FenceToken,
        now: i64,
        identities: ResetIdentities,
    ) -> Result<TargetGuard> {
        self.run(self.at(now).finalize_reset(token, &identities))
    }

    fn record_ack(&self, token: &FenceToken, now: i64, id: &str) -> Result<Ack> {
        self.run(self.at(now).record_ack(token, id))
    }

    fn resolve_ack(&self, token: &FenceToken, now: i64, id: &str, confirmed: bool) -> Result<Ack> {
        self.run(self.at(now).resolve_ack(token, id, confirmed))
    }

    fn lookup(&self, request: &RequestId) -> Lookup {
        self.run(self.control.lookup(request))
            .unwrap_or(Lookup::Unknown)
    }
}

struct Fixture<S> {
    store: Driver<S>,
    target: TargetKey,
    flow: FlowKey,
    sink: SinkIdentity,
    auxiliary: SinkIdentity,
}

fn fixture<S: Store>(store: S) -> Result<Fixture<S>> {
    let f = Fixture {
        store: Driver::new(store),
        target: TargetKey("t-1".into()),
        flow: FlowKey("pipe.flow".into()),
        sink: SinkIdentity("delta:aaaa".into()),
        auxiliary: SinkIdentity("delta:bbbb".into()),
    };
    f.store.enroll(
        f.target.clone(),
        PHYSICAL,
        f.sink.clone(),
        f.auxiliary.clone(),
    )?;
    f.checkpoint(0)?;
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

impl<S: Store> Fixture<S> {
    fn checkpoint(&self, generation: u64) -> Result {
        self.store.put_checkpoint(FlowCheckpoint {
            target: self.target.clone(),
            generation,
            flow: self.flow.clone(),
            definition_digest: "def".into(),
            next_batch_sequence: 0,
            progress: json!({"offset": 0}),
            progress_digest: "p0".into(),
        })
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
            EffectStep::Auxiliary
            | EffectStep::ResetAuxiliary
            | EffectStep::InitializeAuxiliary => self.auxiliary.clone(),
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

/// Names containing the separator cannot alias another identity.
#[test]
fn names_with_separators_do_not_alias() {
    let one = IntentId::new(&TargetKey("a/0/b".into()), 0, &FlowKey("c".into()), 1);
    let two = IntentId::new(&TargetKey("a".into()), 0, &FlowKey("b/0/c".into()), 1);
    assert_ne!(one, two);
    let flow = FlowKey("c/1".into());
    let three = IntentId::new(&TargetKey("a".into()), 0, &flow, 2);
    let four = IntentId::new(&TargetKey("a".into()), 0, &FlowKey("c".into()), 2);
    assert_ne!(three, four);
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

fn batches_run_to_completion_and_take_the_sealed_end<S: Store>(f: Fixture<S>) -> Result {
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

fn enrollment_binds_identities_once<S: Store>(f: Fixture<S>) -> Result {
    let again = f.store.enroll(
        f.target.clone(),
        PHYSICAL,
        f.sink.clone(),
        f.auxiliary.clone(),
    )?;
    assert_eq!(again.revision, 1);
    let recreated = f.store.enroll(
        f.target.clone(),
        PHYSICAL,
        SinkIdentity("delta:cccc".into()),
        f.auxiliary.clone(),
    );
    assert!(matches!(recreated, Err(Outcome::IdentityMismatch { .. })));
    let alias = f.store.enroll(
        TargetKey("alias-of-t-1".into()),
        PHYSICAL,
        f.sink.clone(),
        f.auxiliary.clone(),
    );
    assert!(matches!(alias, Err(Outcome::IdentityMismatch { .. })));
    Ok(())
}

/// Fault matrix: takeover after the auxiliary effect, and a stale commit
/// racing the takeover.
fn a_renewal_extends_a_live_lease_and_refuses_an_expired_one<S: Store>(f: Fixture<S>) -> Result {
    let w1 = f.store.acquire(&f.target, "w1", 0, LEASE)?;
    assert_eq!(
        f.store.renew(&f.target, "w1", LEASE / 2, LEASE)?,
        LEASE / 2 + LEASE
    );
    let guard = f.guard()?;
    assert_eq!(guard.epoch, w1.token.epoch, "a renewal keeps the epoch");
    // Another owner cannot renew, and neither can the owner once expired.
    assert!(matches!(
        f.store.renew(&f.target, "w2", LEASE / 2, LEASE),
        Err(Outcome::Fenced { .. })
    ));
    assert!(matches!(
        f.store.renew(&f.target, "w1", LEASE / 2 + LEASE + 1, LEASE),
        Err(Outcome::Fenced { .. })
    ));
    let guard = f.guard()?;
    assert_eq!(guard.lease_deadline, Some(LEASE / 2 + LEASE));
    assert_eq!(
        guard.epoch, w1.token.epoch,
        "a refused renewal changes nothing"
    );
    Ok(())
}

fn expired_owner_is_fenced_and_takeover_preserves_the_intent<S: Store>(f: Fixture<S>) -> Result {
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
fn ratification_retry_replays_the_receipt<S: Store>(f: Fixture<S>) -> Result {
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
fn a_new_request_id_cannot_repeat_an_effect<S: Store>(f: Fixture<S>) -> Result {
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

fn moved_head_is_a_conflict<S: Store>(f: Fixture<S>) -> Result {
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

fn ordering_is_enforced_by_evidence<S: Store>(f: Fixture<S>) -> Result {
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

fn a_sealed_intent_is_immutable_exclusive_and_finishes_once<S: Store>(f: Fixture<S>) -> Result {
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

fn prepared_input_mismatch_is_refused<S: Store>(f: Fixture<S>) -> Result {
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
fn an_empty_effect_records_a_noop_receipt<S: Store>(f: Fixture<S>) -> Result {
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
fn reset_fences_the_old_generation_and_resumes_after_a_crash<S: Store>(f: Fixture<S>) -> Result {
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
        auxiliary_head: 0,
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
    f.checkpoint(1)?;
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
fn acks_are_generation_scoped_and_cannot_undo_data<S: Store>(f: Fixture<S>) -> Result {
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

fn a_confirmed_ack_is_terminal<S: Store>(f: Fixture<S>) -> Result {
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
fn every_token_field_is_fenced_independently<S: Store>(f: Fixture<S>) -> Result {
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
fn a_restarted_owner_fences_its_own_previous_incarnation<S: Store>(f: Fixture<S>) -> Result {
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
fn an_expired_lease_fences_without_a_takeover<S: Store>(f: Fixture<S>) -> Result {
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
fn ratification_needs_the_active_intent<S: Store>(f: Fixture<S>) -> Result {
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
                auxiliary_receipt: None,
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
fn a_reset_ratifies_only_reset_steps<S: Store>(f: Fixture<S>) -> Result {
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
fn completion_refuses_moved_progress_or_sequence<S: Store>(f: Fixture<S>) -> Result {
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
        f.store.put_checkpoint(checkpoint.clone())?;
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
fn recovery_trace<S: Store>(f: Fixture<S>) -> Result {
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

/// A target step is refused at ratification without the auxiliary receipt,
/// even with a preparation in hand.
fn ratification_rechecks_the_auxiliary_receipt<S: Store>(f: Fixture<S>) -> Result {
    let token = f.store.acquire(&f.target, "w1", 0, LEASE)?.token;
    let id = f.seal(&token, 1, 0)?;
    let intent = f
        .store
        .intent(&id)
        .ok_or_else(|| Outcome::EvidenceUnavailable {
            reason: "intent".into(),
        })?;
    let prepare = f.prepare(&id, EffectStep::Target, 0);
    let head = SinkHead {
        sink: f.sink.clone(),
        version: 0,
    };
    let refused = transitions::ratify_data(
        &f.guard()?,
        &token,
        1,
        Sealed {
            intent: &intent,
            prepare: &prepare,
            head: &head,
            auxiliary_receipt: None,
        },
        Attestation {
            action_digest: &prepare.action_digest,
            prepared_digest: PREPARED,
        },
    );
    assert!(matches!(refused, Err(Outcome::EvidenceUnavailable { .. })));
    Ok(())
}

/// A released lease is free at once, and the epoch still moves, so the
/// finished owner's token is dead.
fn a_released_lease_is_free_and_its_token_is_fenced<S: Store>(f: Fixture<S>) -> Result {
    let w1 = f.store.acquire(&f.target, "w1", 0, LEASE)?.token;
    f.store.release(&w1, 1)?;
    let w2 = f.store.acquire(&f.target, "w2", 2, LEASE)?.token;
    assert_eq!(w2.epoch, w1.epoch + 1);
    assert!(matches!(
        f.store.seal_intent(&w1, 3, f.intent(0, 0)),
        Err(Outcome::Fenced { .. })
    ));
    Ok(())
}

/// A short Initialization intent occupies the same slot, needs both effects
/// (a no-op counts, explicitly), and advances neither progress nor sequence.
fn initialization_holds_the_slot_and_advances_nothing<S: Store>(f: Fixture<S>) -> Result {
    let token = f.store.acquire(&f.target, "w1", 0, LEASE)?.token;
    let init = Intent {
        id: IntentId(format!("{}/init", f.target.0)),
        kind: IntentKind::Initialization,
        ..f.intent(0, 0)
    };
    f.store.seal_intent(&token, 1, init.clone())?;
    assert!(matches!(
        f.store.seal_intent(&token, 1, f.intent(0, 0)),
        Err(Outcome::Conflict { .. })
    ));
    f.effect(
        "init-target",
        &token,
        2,
        &init.id,
        EffectStep::InitializeTarget,
    )?;
    assert!(matches!(
        f.store.complete_initialization(&token, 3, &init.id),
        Err(Outcome::EvidenceUnavailable { .. })
    ));
    f.store.prepare_effect(
        &token,
        3,
        f.prepare(&init.id, EffectStep::InitializeAuxiliary, 0),
    )?;
    f.step(
        "init-aux",
        &token,
        3,
        &init.id,
        EffectStep::InitializeAuxiliary,
        Effect::Noop,
    )?;
    assert!(matches!(
        f.store
            .complete_batch(&request("not-a-batch"), &token, 4, &init.id),
        Err(Outcome::UnsupportedCapability { .. })
    ));
    f.store.complete_initialization(&token, 4, &init.id)?;
    assert_eq!(f.guard()?.active_intent, None);
    let checkpoint = f.store.checkpoint(&f.target, 0, &f.flow.0);
    assert_eq!(checkpoint.map(|c| c.next_batch_sequence), Some(0));
    f.run_batch(&token, 5, 0)?;
    Ok(())
}

macro_rules! scenarios {
    ($($name:ident),* $(,)?) => {
        mod in_memory {
            use super::*;
            $(#[test]
            fn $name() -> Result {
                super::$name(fixture(UnfencedMemoryStore::new())?)
            })*
        }

        #[cfg(feature = "turso")]
        mod on_turso {
            use super::*;
            use super::authority::open;
            $(#[test]
            fn $name() -> Result {
                let (dir, store) = open()?;
                let result = super::$name(fixture(store)?);
                drop(dir);
                result
            })*
        }
    };
}

#[cfg(feature = "turso")]
mod authority;

scenarios!(
    batches_run_to_completion_and_take_the_sealed_end,
    enrollment_binds_identities_once,
    expired_owner_is_fenced_and_takeover_preserves_the_intent,
    a_renewal_extends_a_live_lease_and_refuses_an_expired_one,
    ratification_retry_replays_the_receipt,
    a_new_request_id_cannot_repeat_an_effect,
    moved_head_is_a_conflict,
    ordering_is_enforced_by_evidence,
    a_sealed_intent_is_immutable_exclusive_and_finishes_once,
    prepared_input_mismatch_is_refused,
    an_empty_effect_records_a_noop_receipt,
    reset_fences_the_old_generation_and_resumes_after_a_crash,
    acks_are_generation_scoped_and_cannot_undo_data,
    a_confirmed_ack_is_terminal,
    every_token_field_is_fenced_independently,
    a_restarted_owner_fences_its_own_previous_incarnation,
    an_expired_lease_fences_without_a_takeover,
    ratification_needs_the_active_intent,
    a_reset_ratifies_only_reset_steps,
    completion_refuses_moved_progress_or_sequence,
    recovery_trace,
    initialization_holds_the_slot_and_advances_nothing,
    ratification_rechecks_the_auxiliary_receipt,
    a_released_lease_is_free_and_its_token_is_fenced,
);
