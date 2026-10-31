//! What only a durable authority can show: state surviving a reopen, the
//! schema check, and owners racing through the real transactions.

use std::sync::Mutex;
use std::sync::atomic::{AtomicBool, AtomicI64, Ordering};
use std::time::{Duration, Instant};

use async_trait::async_trait;
use tempfile::TempDir;

use super::*;
use crate::store::{Fail, Table, Tx};
use crate::turso_store::{TursoStore, TursoTx};

fn blocking<T>(future: impl Future<Output = T>) -> T {
    tokio::runtime::Builder::new_current_thread()
        .enable_time()
        .build()
        .unwrap_or_else(|e| unreachable!("{e}"))
        .block_on(future)
}

fn path_in(dir: &TempDir) -> String {
    dir.path().join("authority.db").display().to_string()
}

pub(super) fn open() -> Result<(TempDir, TursoStore)> {
    let dir = TempDir::new().map_err(|e| Outcome::StateUnknown {
        reason: e.to_string(),
    })?;
    let store = blocking(TursoStore::open(&path_in(&dir), "auth-1"))?;
    Ok((dir, store))
}

fn reopen(dir: &TempDir, incarnation: &str) -> Result<TursoStore> {
    blocking(TursoStore::open(&path_in(dir), incarnation))
}

/// The worked recovery trace across a real reopen: every record process B
/// relies on came back from the database, and the original incarnation
/// survives a caller passing another one.
#[test]
fn recovery_trace_survives_a_reopen() -> Result {
    let (dir, store) = open()?;
    let a = fixture(store)?;
    let token = a.store.acquire(&a.target, "proc-a", 0, LEASE)?.token;
    let id = a.seal(&token, 1, 0)?;
    let aux = a.effect("aux-0", &token, 2, &id, EffectStep::Auxiliary)?;
    drop(a);

    let store = reopen(&dir, "ignored")?;
    assert_eq!(store.authority(), AuthorityIncarnation("auth-1".into()));
    let b = Fixture {
        store: Driver::new(store),
        target: TargetKey("t-1".into()),
        flow: FlowKey("pipe.flow".into()),
        sink: SinkIdentity("delta:aaaa".into()),
        auxiliary: SinkIdentity("delta:bbbb".into()),
    };
    assert!(matches!(
        b.store.acquire(&b.target, "proc-b", 5, LEASE),
        Err(Outcome::Conflict { .. })
    ));
    let now = LEASE + 1;
    let acquired = b.store.acquire(&b.target, "proc-b", now, LEASE)?;
    assert_eq!(acquired.recovery_only, Some(id.clone()));
    let resumed = b.store.seal_intent(&acquired.token, now, b.intent(0, 0))?;
    assert_eq!(resumed.prepared_digest.as_deref(), Some(PREPARED));
    assert_eq!(b.store.receipt(&id, EffectStep::Auxiliary), Some(aux));
    b.effect("target-0", &acquired.token, now, &id, EffectStep::Target)?;
    let done = b
        .store
        .complete_batch(&request("complete-0"), &acquired.token, now, &id)?;
    assert_eq!(done.checkpoint.next_batch_sequence, 1);
    assert_eq!(b.head(&b.sink), Some(1));
    assert_eq!(b.head(&b.auxiliary), Some(1));
    assert!(matches!(
        b.store.lookup(&request("aux-0")),
        Lookup::Confirmed(_)
    ));
    assert_eq!(b.store.ack(&done.ack.id), Some(done.ack));
    Ok(())
}

#[test]
fn a_failed_operation_leaves_nothing_behind() -> Result {
    let (_dir, store) = open()?;
    let f = fixture(store)?;
    let token = f.store.acquire(&f.target, "w1", 0, LEASE)?.token;
    let before = f.guard()?;
    assert!(matches!(
        f.store.seal_intent(&token, LEASE + 1, f.intent(0, 0)),
        Err(Outcome::Fenced { .. })
    ));
    assert_eq!(f.guard()?, before);
    assert_eq!(f.store.intent(&f.intent(0, 0).id), None);
    Ok(())
}

#[test]
fn an_unknown_schema_version_is_refused() -> Result {
    let (dir, store) = open()?;
    drop(store);
    let path = path_in(&dir);
    blocking(async {
        let database = turso::Builder::new_local(&path).build().await?;
        let connection = database.connect()?;
        connection
            .execute("UPDATE sdp_authority SET schema_version = 99", ())
            .await?;
        Ok::<_, turso::Error>(())
    })
    .map_err(|e| Outcome::StateUnknown {
        reason: e.to_string(),
    })?;
    assert!(matches!(
        reopen(&dir, "auth-1"),
        Err(Outcome::UnsupportedCapability { .. })
    ));
    Ok(())
}

/// One attempt at the next batch by `owner`. `Ok(None)` means it lost the
/// race at some step, which is expected here.
fn attempt<S: Store>(
    f: &Fixture<S>,
    owner: &str,
    tick: &AtomicI64,
    nonce: &AtomicI64,
) -> Result<Option<u64>> {
    let now = || tick.fetch_add(1, Ordering::SeqCst);
    let lost = |outcome: Outcome| match outcome {
        Outcome::Conflict { .. } | Outcome::Fenced { .. } | Outcome::EvidenceUnavailable { .. } => {
            Ok(None)
        }
        other => Err(other),
    };
    macro_rules! step {
        ($call:expr) => {
            match $call {
                Ok(value) => value,
                Err(outcome) => return lost(outcome),
            }
        };
    }
    let acquired = step!(f.store.acquire(&f.target, owner, now(), 40));
    let token = acquired.token;
    let flow = step!(
        f.store
            .checkpoint(&f.target, 0, &f.flow.0)
            .ok_or(Outcome::Conflict {
                reason: "no checkpoint".into()
            })
    );
    let sequence = flow.next_batch_sequence;
    let sealed = step!(f.store.seal_intent(&token, now(), f.intent(0, sequence)));
    step!(f.store.record_prepared(&token, now(), &sealed.id, PREPARED));
    for effect in [EffectStep::Auxiliary, EffectStep::Target] {
        if f.store.receipt(&sealed.id, effect).is_some() {
            continue;
        }
        let name = format!(
            "{owner}-{effect:?}-{}",
            nonce.fetch_add(1, Ordering::SeqCst)
        );
        step!(f.effect(&name, &token, now(), &sealed.id, effect));
    }
    let name = format!("{owner}-complete-{}", nonce.fetch_add(1, Ordering::SeqCst));
    let done = step!(
        f.store
            .complete_batch(&request(&name), &token, now(), &sealed.id)
    );
    Ok(Some(done.checkpoint.next_batch_sequence - 1))
}

/// Owners race through whole batches with short leases, so takeovers land
/// between every pair of steps. Each batch must complete exactly once and
/// each effect must land exactly once, whoever finishes it.
#[test]
fn racing_owners_complete_each_batch_exactly_once() -> Result {
    const WORKERS: usize = 8;
    const BATCHES: u64 = 60;
    let (_dir, store) = open()?;
    let f = fixture(store)?;
    let (tick, nonce) = (AtomicI64::new(1), AtomicI64::new(0));
    let completed = Mutex::new(Vec::new());
    std::thread::scope(|scope| {
        let mut workers = Vec::new();
        for worker in 0..WORKERS {
            let (f, tick, nonce, completed) = (&f, &tick, &nonce, &completed);
            workers.push(scope.spawn(move || -> Result {
                let owner = format!("w{worker}");
                let deadline = Instant::now() + Duration::from_secs(120);
                while Instant::now() < deadline {
                    if let Some(sequence) = attempt(f, &owner, tick, nonce)? {
                        let mut completed = completed.lock().unwrap_or_else(|e| e.into_inner());
                        completed.push(sequence);
                    }
                    let finished = f
                        .store
                        .checkpoint(&f.target, 0, &f.flow.0)
                        .is_some_and(|c| c.next_batch_sequence >= BATCHES);
                    if finished {
                        break;
                    }
                }
                Ok(())
            }));
        }
        for worker in workers {
            worker.join().unwrap_or_else(|_| {
                Err(Outcome::StateUnknown {
                    reason: "worker panicked".into(),
                })
            })?;
        }
        Ok::<_, Outcome>(())
    })?;
    let mut sequences = completed.into_inner().unwrap_or_else(|e| e.into_inner());
    sequences.sort_unstable();
    let total = sequences.len() as u64;
    assert!(total >= BATCHES, "completed {total}");
    let expected: Vec<u64> = (0..total).collect();
    assert_eq!(sequences, expected, "each batch completes exactly once");
    // An unfinished last batch may hold effects, never more than its own.
    let sink = f.head(&f.sink).unwrap_or_default();
    let auxiliary = f.head(&f.auxiliary).unwrap_or_default();
    assert!((total..=total + 1).contains(&sink), "sink head {sink}");
    assert!(
        (sink..=total + 1).contains(&auxiliary),
        "auxiliary head {auxiliary}"
    );
    Ok(())
}

/// Commits a takeover in its own transaction the first time an armed
/// operation has read the guard row, so the operation's snapshot holds the
/// old guard. An operation that does not write the guard row would then
/// commit under the stale epoch: the write skew the design rules out.
struct TakeoverAfterGuardRead {
    inner: Arc<TursoStore>,
    armed: Arc<AtomicBool>,
}

struct HookedTx {
    inner: TursoTx,
    store: Arc<TursoStore>,
    armed: Arc<AtomicBool>,
}

async fn take_over(store: &TursoStore, key: &str) -> std::result::Result<(), Fail> {
    let tx = store.begin().await?;
    let body = tx.get(Table::Target, key).await?;
    let body = body.ok_or(Fail::Retry)?;
    let guard: TargetGuard = serde_json::from_str(&body).map_err(|_| Fail::Retry)?;
    let (next, _) = transitions::acquire(&guard, "usurper", 10 * LEASE, LEASE)?;
    let body = serde_json::to_string(&next).map_err(|_| Fail::Retry)?;
    tx.put(Table::Target, key, None, &body).await?;
    tx.commit().await
}

#[async_trait]
impl Tx for HookedTx {
    async fn get(&self, table: Table, key: &str) -> std::result::Result<Option<String>, Fail> {
        let body = self.inner.get(table, key).await?;
        if table == Table::Target && self.armed.swap(false, Ordering::SeqCst) {
            take_over(&self.store, key).await?;
        }
        Ok(body)
    }

    async fn list(&self, table: Table, link: &str) -> std::result::Result<Vec<String>, Fail> {
        self.inner.list(table, link).await
    }

    async fn put(
        &self,
        table: Table,
        key: &str,
        link: Option<&str>,
        body: &str,
    ) -> std::result::Result<(), Fail> {
        self.inner.put(table, key, link, body).await
    }

    async fn commit(&self) -> std::result::Result<(), Fail> {
        self.inner.commit().await
    }

    async fn rollback(&self) {
        self.inner.rollback().await;
    }
}

#[async_trait]
impl Store for TakeoverAfterGuardRead {
    type Tx = HookedTx;

    fn authority(&self) -> AuthorityIncarnation {
        self.inner.authority()
    }

    async fn begin(&self) -> std::result::Result<HookedTx, Fail> {
        Ok(HookedTx {
            inner: self.inner.begin().await?,
            store: self.inner.clone(),
            armed: self.armed.clone(),
        })
    }
}

fn hooked() -> Result<(TempDir, Fixture<TakeoverAfterGuardRead>, Arc<AtomicBool>)> {
    let (dir, store) = open()?;
    let armed = Arc::new(AtomicBool::new(false));
    let f = fixture(TakeoverAfterGuardRead {
        inner: Arc::new(store),
        armed: armed.clone(),
    })?;
    Ok((dir, f, armed))
}

/// The stale operation lost the guard row, retried on a fresh snapshot and
/// was fenced; the takeover stands.
fn assert_fenced_by_takeover<S: Store, T: std::fmt::Debug>(
    f: &Fixture<S>,
    armed: &AtomicBool,
    result: Result<T>,
) -> Result {
    assert!(!armed.load(Ordering::SeqCst), "the hook must have fired");
    assert!(matches!(result, Err(Outcome::Fenced { .. })), "{result:?}");
    assert_eq!(f.guard()?.owner.as_deref(), Some("usurper"));
    Ok(())
}

#[test]
fn a_takeover_during_ratification_fences_it() -> Result {
    let (_dir, f, armed) = hooked()?;
    let token = f.store.acquire(&f.target, "w1", 0, LEASE)?.token;
    let id = f.seal(&token, 1, 0)?;
    f.store
        .prepare_effect(&token, 1, f.prepare(&id, EffectStep::Auxiliary, 0))?;
    armed.store(true, Ordering::SeqCst);
    let result = f.step("aux-0", &token, 2, &id, EffectStep::Auxiliary, Effect::Data);
    assert_fenced_by_takeover(&f, &armed, result)?;
    assert_eq!(f.head(&f.auxiliary), Some(0));
    assert_eq!(f.store.receipt(&id, EffectStep::Auxiliary), None);
    assert_eq!(
        f.store.lookup(&request("aux-0")),
        Lookup::AbsentWithinCoverage
    );
    Ok(())
}

#[test]
fn a_takeover_during_a_noop_ratification_fences_it() -> Result {
    let (_dir, f, armed) = hooked()?;
    let token = f.store.acquire(&f.target, "w1", 0, LEASE)?.token;
    let id = f.seal(&token, 1, 0)?;
    f.store
        .prepare_effect(&token, 1, f.prepare(&id, EffectStep::Auxiliary, 0))?;
    armed.store(true, Ordering::SeqCst);
    let result = f.step("aux-0", &token, 2, &id, EffectStep::Auxiliary, Effect::Noop);
    assert_fenced_by_takeover(&f, &armed, result)?;
    assert_eq!(f.store.receipt(&id, EffectStep::Auxiliary), None);
    Ok(())
}

#[test]
fn a_takeover_during_sealing_fences_it() -> Result {
    let (_dir, f, armed) = hooked()?;
    let token = f.store.acquire(&f.target, "w1", 0, LEASE)?.token;
    armed.store(true, Ordering::SeqCst);
    let result = f.store.seal_intent(&token, 1, f.intent(0, 0));
    assert_fenced_by_takeover(&f, &armed, result)?;
    assert_eq!(f.store.intent(&f.intent(0, 0).id), None);
    Ok(())
}

#[test]
fn a_takeover_during_prepared_recording_fences_it() -> Result {
    let (_dir, f, armed) = hooked()?;
    let token = f.store.acquire(&f.target, "w1", 0, LEASE)?.token;
    let sealed = f.store.seal_intent(&token, 1, f.intent(0, 0))?;
    armed.store(true, Ordering::SeqCst);
    let result = f.store.record_prepared(&token, 2, &sealed.id, PREPARED);
    assert_fenced_by_takeover(&f, &armed, result)?;
    let stored = f.store.intent(&sealed.id);
    assert_eq!(stored.and_then(|i| i.prepared_digest), None);
    Ok(())
}

#[test]
fn a_takeover_during_preparation_fences_it() -> Result {
    let (_dir, f, armed) = hooked()?;
    let token = f.store.acquire(&f.target, "w1", 0, LEASE)?.token;
    let id = f.seal(&token, 1, 0)?;
    armed.store(true, Ordering::SeqCst);
    let prepare = f.prepare(&id, EffectStep::Auxiliary, 0);
    let result = f.store.prepare_effect(&token, 2, prepare);
    assert_fenced_by_takeover(&f, &armed, result)
}

#[test]
fn a_takeover_during_completion_fences_it() -> Result {
    let (_dir, f, armed) = hooked()?;
    let token = f.store.acquire(&f.target, "w1", 0, LEASE)?.token;
    let id = f.seal(&token, 1, 0)?;
    f.effect("aux-0", &token, 2, &id, EffectStep::Auxiliary)?;
    f.effect("target-0", &token, 2, &id, EffectStep::Target)?;
    armed.store(true, Ordering::SeqCst);
    let result = f
        .store
        .complete_batch(&request("complete-0"), &token, 3, &id);
    assert_fenced_by_takeover(&f, &armed, result)?;
    let checkpoint = f.store.checkpoint(&f.target, 0, &f.flow.0);
    assert_eq!(checkpoint.map(|c| c.next_batch_sequence), Some(0));
    Ok(())
}

#[test]
fn a_takeover_during_ack_work_fences_it() -> Result {
    let (_dir, f, armed) = hooked()?;
    let token = f.store.acquire(&f.target, "w1", 0, LEASE)?.token;
    let ack = f.run_batch(&token, 1, 0)?.ack.id;
    armed.store(true, Ordering::SeqCst);
    let result = f.store.resolve_ack(&token, 2, &ack, true);
    assert_fenced_by_takeover(&f, &armed, result)?;
    let stored = f.store.ack(&ack).map(|ack| ack.state);
    assert_eq!(stored, Some(AckState::Pending));
    Ok(())
}
