use std::cell::Cell;

use fastrace::collector::{Config, SpanRecord, TestReporter};
use fastrace::future::FutureExt;

use super::*;

fn state(enabled: bool) -> State {
    let state = State::new();
    state.enabled.store(enabled, Ordering::Relaxed);
    state
}

fn guard(state: &State) -> Guard<'_> {
    Guard::make(
        state,
        Phase::Preparation,
        || Identity::CausalContext,
        true,
        || (Span::noop(), true),
    )
}

#[test]
fn disabled_rpc_context_skips_properties_and_span_creation() {
    let state = state(false);
    let calls = Cell::new(0);
    for rpc in [Rpc::ReleaseExecute, Rpc::RegisterWorker] {
        let span = rpc_span_in(&state, rpc, || {
            calls.set(calls.get() + 1);
            vec![("request", "must not allocate".into())]
        });
        assert!(SpanContext::from_span(&span).is_none());
    }
    assert_eq!(calls.get(), 0);
    assert_eq!(state.summary().created, 0);
}

#[tokio::test]
async fn release_and_initial_worker_registration_have_real_rpc_parents() -> Result<(), &'static str>
{
    for (rpc, phases) in [
        (
            Rpc::ReleaseExecute,
            vec![Phase::ReleaseResponseBuffer, Phase::ReleaseOperation],
        ),
        (Rpc::RegisterWorker, vec![Phase::Schedule]),
    ] {
        let state = state(true);
        let span = rpc_span_in(&state, rpc, || vec![("request", "actual request".into())]);
        let rpc_context = SpanContext::from_span(&span).ok_or("enabled RPC span")?;
        let original: Result<(), &str> = async {
            // Yield across a poll boundary: the RPC future must restore its
            // parent on each poll, including an error return.
            tokio::task::yield_now().await;
            let parent = SpanContext::current_local_parent().ok_or("RPC local parent")?;
            assert_eq!(parent.trace_id, rpc_context.trace_id);
            assert_eq!(parent.span_id, rpc_context.span_id);
            for phase in phases {
                let parent = SpanContext::current_local_parent().ok_or("guard parent")?;
                let observation = Guard::make(
                    &state,
                    phase,
                    || match rpc {
                        Rpc::ReleaseExecute => Identity::Operation {
                            session: "session",
                            operation: "operation",
                        },
                        Rpc::RegisterWorker => Identity::Session("session"),
                    },
                    true,
                    || (Span::root(phase.name(), parent), true),
                );
                observation.finish(Outcome::Succeeded);
            }
            Err("original RPC error")
        }
        .in_span(span)
        .await;
        assert_eq!(original, Err("original RPC error"));
        let summary = state.summary();
        assert_eq!(summary.created, summary.ended);
        assert_eq!(summary.incomplete_identity, 0);
        assert_eq!(summary.outstanding, 0);
        assert_eq!(
            summary.created,
            match rpc {
                Rpc::ReleaseExecute => 2,
                Rpc::RegisterWorker => 1,
            }
        );
        assert!(SpanContext::current_local_parent().is_none());
    }
    Ok(())
}

#[test]
fn disabled_guard_skips_identity_span_and_detail_callbacks() {
    let state = state(false);
    let calls = Cell::new(0);
    let observation = Guard::make(
        &state,
        Phase::Resolve,
        || {
            calls.set(calls.get() + 1);
            Identity::CausalContext
        },
        true,
        || {
            calls.set(calls.get() + 1);
            (Span::noop(), true)
        },
    );
    assert!(observation.active.is_none());
    observation.detail("extra", || {
        calls.set(calls.get() + 1);
        String::new()
    });
    observation.finish(Outcome::Succeeded);
    assert_eq!(calls.get(), 0);
    assert_eq!(state.summary().created, 0);
}

#[test]
fn observed_call_delegates_once_and_preserves_original_error() {
    for enabled in [false, true] {
        let state = state(enabled);
        let calls = Cell::new(0);
        let observation = guard(&state);
        let original: Result<u64, &str> = {
            calls.set(calls.get() + 1);
            Err("original error")
        };
        observation.finish_result(&original);
        assert_eq!(calls.get(), 1);
        assert_eq!(original, Err("original error"));
        assert_eq!(state.summary().failed, u64::from(enabled));
    }
}

#[test]
fn all_terminal_outcomes_close_exactly_one_guard() {
    let state = state(true);
    guard(&state).finish(Outcome::Succeeded);
    let error: Result<(), &str> = Err("source error");
    guard(&state).finish_result(&error);
    guard(&state).finish(Outcome::Cancelled);
    drop(guard(&state));
    let summary = state.summary();
    assert_eq!(summary.created, 4);
    assert_eq!(summary.ended, 4);
    assert_eq!(summary.failed, 1);
    assert_eq!(summary.cancelled, 1);
    assert_eq!(summary.abandoned, 1);
    assert_eq!(summary.outstanding, 0);
    assert!(summary.counters_complete);
}

#[test]
fn pending_guard_is_outstanding_until_its_owner_drops() {
    let state = state(true);
    let observation = guard(&state);
    assert_eq!(state.summary().outstanding, 1);
    drop(observation);
    assert_eq!(state.summary().outstanding, 0);
    assert_eq!(state.summary().abandoned, 1);
}

#[test]
fn point_binding_has_no_duration_clock() {
    let state = state(true);
    let observation = Guard::make(
        &state,
        Phase::JobAccepted,
        || Identity::Job {
            session: "s",
            job: 17,
        },
        false,
        || (Span::noop(), true),
    );
    assert!(
        observation
            .active
            .as_ref()
            .is_some_and(|active| active.clock.is_none())
    );
    observation.finish(Outcome::Succeeded);
    assert_eq!(
        state.summary().phase_created[Phase::JobAccepted as usize],
        1
    );
}

#[test]
fn operation_and_job_identities_have_no_mutable_current_request_slot() {
    let (first, first_complete) = Identity::Operation {
        session: "s1",
        operation: "op1",
    }
    .properties();
    let (second, second_complete) = Identity::Operation {
        session: "s2",
        operation: "op2",
    }
    .properties();
    let (job, job_complete) = Identity::Job {
        session: "s1",
        job: 29,
    }
    .properties();
    assert!(first_complete && second_complete && job_complete);
    assert!(
        first
            .iter()
            .any(|(key, value)| *key == "c2.operation_id" && value == "op1")
    );
    assert!(
        second
            .iter()
            .any(|(key, value)| *key == "c2.operation_id" && value == "op2")
    );
    assert!(
        job.iter()
            .any(|(key, value)| *key == "session.id" && value == "s1")
    );
    assert!(
        job.iter()
            .any(|(key, value)| *key == "execution.job.id" && value == "29")
    );
    assert!(!job.iter().any(|(key, _)| *key == "c2.operation_id"));
}

#[test]
fn task_identity_preserves_actual_attempt_and_worker_placement() {
    let (values, complete) = Identity::Task {
        session: "s",
        job: u64::MAX,
        stage: 4,
        partition: 31,
        attempt: 2,
        placement: Placement::Worker(9),
    }
    .properties();
    assert!(complete);
    for (key, expected) in [
        ("execution.job.id", u64::MAX.to_string()),
        ("execution.partition", "31".into()),
        ("execution.attempt", "2".into()),
        ("cluster.worker.id", "9".into()),
    ] {
        assert!(
            values
                .iter()
                .any(|(actual_key, value)| *actual_key == key && value == &expected)
        );
    }
}

#[test]
fn oversized_or_empty_identity_is_bounded_and_explicitly_incomplete() {
    let large = "λ".repeat(ID_BYTES);
    let (properties, complete) = Identity::Operation {
        session: "",
        operation: &large,
    }
    .properties();
    assert!(!complete);
    assert!(properties.iter().all(|(_, value)| value.len() <= ID_BYTES));
    assert!(
        properties
            .iter()
            .all(|(_, value)| value.is_char_boundary(value.len()))
    );
}

#[test]
fn missing_causal_parent_cannot_qualify_as_complete_identity() {
    let state = state(true);
    Guard::make(
        &state,
        Phase::JobAccepted,
        || Identity::Job {
            session: "s",
            job: 1,
        },
        false,
        || (Span::noop(), false),
    )
    .finish(Outcome::Succeeded);
    assert_eq!(state.summary().incomplete_identity, 1);
}

#[test]
fn saturated_sequence_is_explicit_and_never_wraps_to_a_fresh_id() {
    let state = state(true);
    state.sequence.store(u64::MAX, Ordering::Relaxed);
    guard(&state).finish(Outcome::Succeeded);
    assert_eq!(state.summary().created, u64::MAX);
    assert!(!state.summary().counters_complete);
}

#[test]
fn named_owner_drop_closes_its_lifetime_without_claiming_allocator_release() {
    let state = state(true);
    let owner = NamedOwner {
        guard: Some(Guard::make(
            &state,
            Phase::ExecutorOwner,
            || Identity::Operation {
                session: "s",
                operation: "op",
            },
            true,
            || (Span::noop(), true),
        )),
    };
    assert_eq!(state.summary().outstanding, 1);
    drop(owner);
    let summary = state.summary();
    assert_eq!(summary.created, 1);
    assert_eq!(summary.ended, 1);
    assert_eq!(summary.abandoned, 0);
    assert_eq!(summary.outstanding, 0);
}

#[test]
fn job_scoped_cleanup_does_not_invent_a_stage() {
    let (values, complete) = Identity::CausalJob { job: 17 }.properties();
    assert!(complete);
    assert!(
        values
            .iter()
            .any(|(key, value)| *key == "execution.job.id" && value == "17")
    );
    assert!(!values.iter().any(|(key, _)| *key == "execution.stage"));
}

#[test]
fn every_phase_has_a_counter_slot() {
    assert_eq!(Phase::ExecutorBufferOwner as usize + 1, PHASE_COUNT);
}

fn require_send_sync<T: Send + Sync>() {}

#[test]
fn observation_and_named_owner_preserve_send_and_sync() {
    require_send_sync::<Guard<'static>>();
    require_send_sync::<NamedOwner<'static>>();
}

#[test]
fn enabled_noop_guard_skips_detail_callback() -> Result<(), &'static str> {
    let state = state(true);
    let calls = Cell::new(0);
    let observation = guard(&state);
    observation.detail("noop", || {
        calls.set(calls.get() + 1);
        "must not evaluate".into()
    });
    let child = observation
        .child_span("noop child")
        .ok_or("enabled guard retains its child-span API")?;
    assert!(SpanContext::from_span(&child).is_none());
    drop(observation);
    assert_eq!(calls.get(), 0);
    assert_eq!(state.summary().abandoned, 1);
    assert_eq!(state.summary().outstanding, 0);
    Ok(())
}

fn property<'a>(record: &'a SpanRecord, key: &str) -> Result<&'a str, &'static str> {
    let mut values = record.properties.iter().filter(|(name, _)| name == key);
    let (_, value) = values.next().ok_or("exported property is missing")?;
    assert!(
        values.next().is_none(),
        "duplicate exported property: {key}"
    );
    Ok(value)
}

#[test]
fn cross_thread_guard_export_preserves_initial_details_and_terminal_fields()
-> Result<(), &'static str> {
    let (reporter, records) = TestReporter::new();
    fastrace::set_reporter(reporter, Config::default());
    let state = state(true);
    let parent = Span::root("guard_export_parent", SpanContext::random());
    let context = SpanContext::from_span(&parent).ok_or("recording parent")?;
    let make = |phase: Phase, timed: bool| {
        Guard::make(
            &state,
            phase,
            || Identity::Task {
                session: "export-session",
                job: 17,
                stage: 4,
                partition: 31,
                attempt: 2,
                placement: Placement::Worker(9),
            },
            timed,
            || (Span::root(phase.name(), context), true),
        )
    };
    let owner = make(Phase::StreamOwner, true);
    let consumption = make(Phase::StreamConsumption, true);
    let abandoned = make(Phase::ReaderOpen, true);
    let cancelled = make(Phase::TaskTerminal, false);
    owner.detail("c2.owner_role", || {
        // A detail callback may add another detail without holding our mutex.
        owner.detail("c2.open_partition", || "31".into());
        "opened_shuffle_source".into()
    });
    consumption.detail("c2.owner_role", || "opened_shuffle_source".into());
    let owner_context = owner
        .active
        .as_ref()
        .and_then(|active| SpanContext::from_span(&active.span))
        .ok_or("recording owner")?;
    let child = owner.child_span("guard_export_child").ok_or("child span")?;
    std::thread::scope(|scope| -> Result<(), &'static str> {
        scope
            .spawn(move || {
                drop(child);
                owner.detail("c2.terminal_reason", || "named_wrapper_owner_drop".into());
                owner.finish(Outcome::Succeeded);
                consumption.detail("c2.rows", || "0".into());
                consumption.detail("c2.terminal_reason", || "inner_stream_error".into());
                let original: Result<(), &str> = Err("original stream error");
                consumption.finish_result(&original);
                assert_eq!(original, Err("original stream error"));
                drop(abandoned);
                cancelled.finish(Outcome::Cancelled);
            })
            .join()
            .map_err(|_| "guard export thread panicked")?;
        Ok(())
    })?;
    drop(parent);
    fastrace::flush();
    let records = records.lock();
    let observed: Vec<_> = records
        .iter()
        .filter(|record| record.trace_id == context.trace_id && record.name.starts_with("c2."))
        .collect();
    assert_eq!(observed.len(), 4);
    let pid = std::process::id().to_string();
    for (phase, sequence, outcome, timed) in [
        (Phase::StreamOwner, "1", "succeeded", true),
        (Phase::StreamConsumption, "2", "failed", true),
        (Phase::ReaderOpen, "3", "abandoned", true),
        (Phase::TaskTerminal, "4", "cancelled", false),
    ] {
        let record = observed
            .iter()
            .copied()
            .find(|record| record.name == phase.name())
            .ok_or("exported observation is missing")?;
        assert_eq!(record.parent_id, context.span_id);
        for (key, expected) in [
            ("c2.identity_kind", "task"),
            ("session.id", "export-session"),
            ("execution.job.id", "17"),
            ("execution.stage", "4"),
            ("execution.partition", "31"),
            ("execution.attempt", "2"),
            ("c2.placement", "worker"),
            ("cluster.worker.id", "9"),
            ("c2.schema_version", "1"),
            ("c2.observation_id", sequence),
            ("c2.pid", pid.as_str()),
            ("c2.causal_parent_present", "true"),
            ("c2.identity_complete", "true"),
            ("c2.kind", if timed { "interval" } else { "event" }),
            ("c2.outcome", outcome),
        ] {
            assert_eq!(property(record, key)?, expected, "{key}");
        }
        if timed {
            property(record, "c2.elapsed_ns")?
                .parse::<u64>()
                .map_err(|_| "exported duration is invalid")?;
        } else {
            assert!(
                !record
                    .properties
                    .iter()
                    .any(|(key, _)| key == "c2.elapsed_ns")
            );
        }
        if record.name == Phase::StreamOwner.name() {
            assert_eq!(property(record, "c2.owner_role")?, "opened_shuffle_source");
            assert_eq!(property(record, "c2.open_partition")?, "31");
            assert_eq!(
                property(record, "c2.terminal_reason")?,
                "named_wrapper_owner_drop"
            );
        } else if record.name == Phase::StreamConsumption.name() {
            assert_eq!(property(record, "c2.owner_role")?, "opened_shuffle_source");
            assert_eq!(property(record, "c2.rows")?, "0");
            assert_eq!(
                property(record, "c2.terminal_reason")?,
                "inner_stream_error"
            );
        }
    }
    let child = records
        .iter()
        .find(|record| record.trace_id == context.trace_id && record.name == "guard_export_child")
        .ok_or("exported child is missing")?;
    assert_eq!(child.parent_id, owner_context.span_id);
    let summary = state.summary();
    assert_eq!(summary.created, 4);
    assert_eq!(summary.ended, 4);
    assert_eq!(summary.failed, 1);
    assert_eq!(summary.cancelled, 1);
    assert_eq!(summary.abandoned, 1);
    assert_eq!(summary.outstanding, 0);
    Ok(())
}
