//! Converged relays preserve state and producer barriers within a fixed quota.
use super::*;
use grust_procedures::ExecutionContext;

const LIMIT: usize = 64 << 20;
const RELAYS: usize = 4;

fn converged(
    n: usize,
    width: usize,
    algorithm: SsspAlgorithm,
) -> (Operation, Vec<SsspPartition>, Resources, ExecutionContext) {
    let (r, usage, _) = support::resources(LIMIT);
    let op = support::operation(width, n as u64);
    let mut ids = vec![-5];
    ids.extend(0..n as i64 - 1);
    let mut ps = parts(
        &op,
        &ids,
        &[(-5, 0, 1.0), (0, 1, 2.0), (1, 2, 0.0), (-5, 2, 7.0)],
        options(algorithm),
        &r,
    );
    loop {
        stats(&op, &mut ps).unwrap();
        exchange(&op, &mut ps).unwrap();
        if ps.iter().all(|p| p.active_count() == 0) {
            break;
        }
        assert!(ps[0].rounds() < 10, "fixture must converge before its cap");
    }
    (op, ps, r, usage)
}

#[derive(Debug, PartialEq)]
struct RelayCost {
    work: usize,
    peak_admitted: usize,
}

fn relay_cost(n: usize, width: usize, algorithm: SsspAlgorithm, headroom: usize) -> RelayCost {
    let (op, mut ps, r, usage) = converged(n, width, algorithm);
    let rows = ps
        .iter()
        .flat_map(SsspPartition::state_rows)
        .collect::<Vec<_>>();
    let origins = ps.iter().map(SsspPartition::origin).collect::<Vec<_>>();
    let rounds = ps[0].rounds();
    let first_phase = ps[0].next_phase();
    let live = usage.usage().unwrap().live_bytes;
    // Raise the cumulative peak above setup, then hold a fixed headroom. This
    // measures admitted relay scratch, not setup's peak, allocator bytes or RSS.
    let floor = LIMIT - headroom;
    assert!(floor > usage.usage().unwrap().peak_bytes);
    let blocker = usage.reserve(floor - live).unwrap();
    let before = usage.usage().unwrap();
    for relay in 0..RELAYS {
        stats(&op, &mut ps).unwrap();
        assert_eq!(exchange(&op, &mut ps).unwrap(), (SsspMode::Done, None));
        for p in &ps {
            assert_eq!(p.rounds(), rounds);
            assert_eq!(p.next_phase(), first_phase + relay as u64 + 1);
            assert_eq!(p.active_count(), 0);
            assert_eq!(p.last_work(), SsspWork::default());
            assert!(p.row_cursor().is_err(), "Done is not a sealed result");
        }
    }
    let after = usage.usage().unwrap();
    let cost = RelayCost {
        work: after.counted_work().unwrap() - before.counted_work().unwrap(),
        peak_admitted: after.peak_bytes - before.peak_bytes,
    };
    println!(
        "SSSP_DONE_COUNTERS algorithm={algorithm:?} vertices={n} partitions={width} relays={RELAYS} work_units={} peak_admitted_bytes={}",
        cost.work, cost.peak_admitted
    );
    assert_eq!(
        after.live_bytes, before.live_bytes,
        "no retained relay scratch"
    );
    assert_eq!(
        ps.iter()
            .flat_map(SsspPartition::state_rows)
            .collect::<Vec<_>>(),
        rows
    );
    assert_eq!(
        ps.iter().map(SsspPartition::origin).collect::<Vec<_>>(),
        origins
    );
    drop(blocker);
    stats(&op, &mut ps).unwrap();
    let phase = phase(&op, &ps);
    for p in &mut ps {
        assert_eq!(
            p.seal(&phase).unwrap(),
            Some(SsspConvergence { rounds, reached: 4 })
        );
    }
    let actual = collected(&ps);
    assert_eq!(actual[&-5], Some((0.0, 0, -5)));
    assert_eq!(actual[&0], Some((1.0, 1, -5)));
    assert_eq!(actual[&1], Some((3.0, 2, 0)));
    assert_eq!(actual[&2], Some((3.0, 3, 1)));
    assert!(actual.iter().all(|(&id, value)| id <= 2 || value.is_none()));
    drop(ps);
    drop(r);
    assert_eq!(usage.usage().unwrap().live_bytes, 0);
    cost
}

#[test]
fn done_relay_cost_is_independent_of_vertex_count() {
    let mut costs = vec![];
    for algorithm in [SsspAlgorithm::Reference, SsspAlgorithm::DeltaStar] {
        for width in [1, 3, 8, 32] {
            for n in [16, 65_536] {
                costs.push(relay_cost(n, width, algorithm, LIMIT / 2));
            }
        }
    }
    // Gather every matched cell before asserting, so a regression retains
    // counter evidence for both sizes and both methods instead of one failure.
    for pair in costs.chunks_exact(2) {
        assert_eq!(pair[0], pair[1], "fixed owner count must bound relay cost");
    }
}

#[test]
fn done_relays_fit_partition_sized_headroom_with_exact_results() {
    for algorithm in [SsspAlgorithm::Reference, SsspAlgorithm::DeltaStar] {
        for width in [1, 3, 8, 32] {
            // A 65,536-row candidate vector alone is 2 MiB on this layout.
            // Allow protocol metadata per owner, not another graph-sized array.
            relay_cost(65_536, width, algorithm, width * 32 * 1024);
        }
    }
}

#[test]
fn done_statistics_still_obey_the_shared_work_limit() {
    for algorithm in [SsspAlgorithm::Reference, SsspAlgorithm::DeltaStar] {
        let (op, mut ps, r, usage) = converged(16, 3, algorithm);
        let before = ps
            .iter()
            .flat_map(SsspPartition::state_rows)
            .collect::<Vec<_>>();
        let phase = phase(&op, &ps);
        let remaining = usize::MAX - usage.usage().unwrap().counted_work().unwrap();
        usage.charge_work(remaining).unwrap();
        assert!(stats(&op, &mut ps).unwrap_err().contains("work"));
        assert_eq!(
            ps.iter()
                .flat_map(SsspPartition::state_rows)
                .collect::<Vec<_>>(),
            before
        );
        assert!(
            ps.iter()
                .all(|p| p.next_phase() == phase.number && p.row_cursor().is_err())
        );
        drop(ps);
        drop(r);
        assert_eq!(usage.usage().unwrap().live_bytes, 0);
    }
}

fn done_completions(op: &Operation, ps: &mut [SsspPartition]) -> Vec<SsspCompletion> {
    stats(op, ps).unwrap();
    let phase = phase(op, ps);
    ps.iter_mut()
        .map(|p| {
            let mut cursor = p.start_emission(&phase).unwrap();
            assert_eq!(cursor.mode(), SsspMode::Done);
            assert!(cursor.next_values().unwrap().is_none());
            cursor.finish().unwrap()
        })
        .collect()
}

#[test]
fn done_relays_reject_missing_replayed_foreign_and_nonempty_producers() {
    for fault in 0..9 {
        let (op, mut ps, _, _) = converged(16, 3, SsspAlgorithm::DeltaStar);
        let phase = phase(&op, &ps);
        let before = ps[0].state_rows().collect::<Vec<_>>();
        let rounds = ps[0].rounds();
        let mut completions = done_completions(&op, &mut ps);
        let other_origin = ps[1].origin();
        let error = match fault {
            0 => ps[0].finish(&phase), // A local EOF is not a complete barrier.
            1 => {
                ps[0].finish_producer(&completions[0]).unwrap();
                ps[0].finish_producer(&completions[0])
            }
            2 => {
                completions[0].sequences[0] = 1;
                ps[0].finish_producer(&completions[0])
            }
            3 => {
                completions[0].origin.adjacency_id += 1;
                ps[0].finish_producer(&completions[0])
            }
            4 => {
                completions[0].mode = SsspMode::DeltaStar;
                ps[0].finish_producer(&completions[0])
            }
            5 => {
                completions[0].bucket = Some(0.0);
                ps[0].finish_producer(&completions[0])
            }
            6 => {
                let mut foreign = phase.clone();
                foreign.operation.generation += 1;
                ps[0].finish(&foreign)
            }
            7 | 8 => ps[0].receive_values(
                &phase,
                SsspMessageValues {
                    origin: other_origin,
                    producer: 1,
                    recipient: 0,
                    sequence: 0,
                    mode: SsspMode::Done,
                    bucket: None,
                    payload: if fault == 7 {
                        SsspPayload::Topology {
                            source: 1,
                            target: 0,
                        }
                    } else {
                        SsspPayload::Candidate {
                            target: 0,
                            label: SsspLabel::from_parts(1.0, 1, 1).unwrap(),
                        }
                    },
                },
            ),
            _ => unreachable!(),
        };
        assert!(error.is_err(), "fault {fault} must fail");
        assert_eq!(ps[0].state_rows().collect::<Vec<_>>(), before);
        assert_eq!(ps[0].next_phase(), phase.number);
        assert_eq!(ps[0].rounds(), rounds);
        assert!(ps[0].row_cursor().is_err());
        assert!(ps[0].statistics().is_err());
    }
}

#[test]
fn done_publication_still_requires_local_eof_admission_and_cancellation_checks() {
    for fault in 0..4 {
        let (op, mut ps, r, usage) = converged(16, 1, SsspAlgorithm::Reference);
        let phase = phase(&op, &ps);
        let before = ps[0].state_rows().collect::<Vec<_>>();
        let rounds = ps[0].rounds();
        let mut cursor = None;
        if fault == 0 {
            stats(&op, &mut ps).unwrap();
            cursor = Some(ps[0].start_emission(&phase).unwrap());
            // A producer marker cannot substitute for the local cursor's EOF.
            let origin = ps[0].origin();
            ps[0]
                .finish_producer_values(
                    &phase,
                    SsspCompletionValues {
                        origin,
                        producer: 0,
                        mode: SsspMode::Done,
                        bucket: None,
                        sequence: 0,
                        total_messages: 0,
                    },
                )
                .unwrap();
        } else {
            let completions = done_completions(&op, &mut ps);
            ps[0].finish_producer(&completions[0]).unwrap();
        }
        let blocker = match fault {
            1 => Some(
                usage
                    .reserve(LIMIT - usage.usage().unwrap().live_bytes)
                    .unwrap(),
            ),
            2 => {
                usage.cancel().unwrap();
                None
            }
            _ => None,
        };
        let result = ps[0].finish(&phase);
        if fault == 3 {
            assert!(result.is_ok(), "unfaulted control must publish");
            assert_eq!(ps[0].next_phase(), phase.number + 1);
        } else {
            assert!(result.is_err(), "fault {fault} must prevent publication");
            assert_eq!(ps[0].next_phase(), phase.number);
            assert!(ps[0].row_cursor().is_err());
        }
        assert_eq!(ps[0].state_rows().collect::<Vec<_>>(), before);
        assert_eq!(ps[0].rounds(), rounds);
        drop(cursor);
        drop(blocker);
        drop(ps);
        drop(r);
        assert_eq!(usage.usage().unwrap().live_bytes, 0);
    }
}
