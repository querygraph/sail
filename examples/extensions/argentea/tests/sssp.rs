#[path = "sssp_done/mod.rs"]
mod done;
#[path = "delta_support/mod.rs"]
mod support;
use sail_argentea_core::*;
use std::collections::{BTreeMap, BTreeSet};
fn options(algorithm: SsspAlgorithm) -> SsspOptions {
    SsspOptions {
        source: -5,
        algorithm,
        max_rounds: 100,
        delta: 4.0,
    }
}
fn parts(
    op: &Operation,
    ids: &[i64],
    edges: &[(i64, i64, f64)],
    options: SsspOptions,
    r: &Resources,
) -> Vec<SsspPartition> {
    (0..op.partitions)
        .map(|p| {
            SsspPartition::build(
                op.clone(),
                p,
                100 + p as u64,
                &ids.iter()
                    .copied()
                    .filter(|&id| op.owner(id) == p)
                    .collect::<Vec<_>>(),
                &edges
                    .iter()
                    .copied()
                    .filter(|&(s, _, _)| op.owner(s) == p)
                    .collect::<Vec<_>>(),
                options,
                r.clone(),
            )
            .unwrap()
        })
        .collect()
}
fn phase(op: &Operation, ps: &[SsspPartition]) -> Round {
    Round {
        operation: op.clone(),
        number: ps[0].next_phase(),
    }
}
fn stats(op: &Operation, ps: &mut [SsspPartition]) -> Result<()> {
    let reports = ps
        .iter()
        .map(SsspPartition::statistics)
        .collect::<Result<Vec<_>>>()?;
    let phase = phase(op, ps);
    for (p, state) in ps.iter_mut().enumerate() {
        for j in 0..reports.len() {
            state.receive_statistics(&reports[(p + j) % reports.len()])?;
        }
        state.finish_statistics(&phase)?;
    }
    Ok(())
}
fn exchange(op: &Operation, ps: &mut [SsspPartition]) -> Result<(SsspMode, Option<f64>)> {
    let phase = phase(op, ps);
    let mut cursors = ps
        .iter_mut()
        .map(|p| p.start_emission(&phase))
        .collect::<Result<Vec<_>>>()?;
    let mode = cursors[0].mode();
    let bucket = cursors[0].bucket();
    assert!(
        cursors
            .iter()
            .all(|c| c.mode() == mode && c.bucket() == bucket)
    );
    loop {
        let mut any = false;
        for c in cursors.iter_mut().rev() {
            if let Some(m) = c.next_update()? {
                any = true;
                ps[m.values.recipient].receive(&m)?;
            }
        }
        if !any {
            break;
        }
    }
    for c in cursors {
        let complete = c.finish()?;
        for p in &mut *ps {
            p.finish_producer(&complete)?;
        }
    }
    for p in ps {
        p.finish(&phase)?;
    }
    Ok((mode, bucket))
}
fn run(op: &Operation, ps: &mut [SsspPartition]) -> Result<Vec<(SsspMode, Option<f64>)>> {
    let mut trace = vec![];
    loop {
        stats(op, ps)?;
        let phase = phase(op, ps);
        let seals = ps
            .iter_mut()
            .map(|p| p.seal(&phase))
            .collect::<Result<Vec<_>>>()?;
        if seals[0].is_some() {
            assert!(seals.iter().all(|s| *s == seals[0]));
            return Ok(trace);
        }
        assert!(seals.iter().all(Option::is_none));
        trace.push(exchange(op, ps)?);
        assert!(trace.len() < 1000);
    }
}
fn collected(ps: &[SsspPartition]) -> BTreeMap<i64, Option<(f64, u64, i64)>> {
    let mut result = BTreeMap::new();
    for p in ps {
        let mut c = p.row_cursor().unwrap();
        while let Some(r) = c.next_row().unwrap() {
            assert!(
                result
                    .insert(r.id, r.label.map(|l| (l.distance(), l.hops(), l.parent())))
                    .is_none()
            );
        }
    }
    result
}
fn oracle(
    ids: &[i64],
    edges: &[(i64, i64, f64)],
    source: i64,
) -> BTreeMap<i64, Option<(f64, u64, i64)>> {
    // Independent sequential Dijkstra, without the production label/bucket API.
    let mut labels: BTreeMap<_, Option<(f64, u64, i64)>> = ids.iter().map(|&v| (v, None)).collect();
    labels.insert(source, Some((0.0, 0, source)));
    let mut pending: BTreeSet<_> = ids.iter().copied().collect();
    loop {
        let next = pending
            .iter()
            .filter_map(|&v| labels[&v].map(|l| (v, l)))
            .min_by(|a, b| a.1.partial_cmp(&b.1).unwrap());
        let Some((v, (distance, hops, _))) = next else {
            break;
        };
        pending.remove(&v);
        for &(s, t, w) in edges {
            if s == v {
                let candidate = (distance + w, hops + 1, v);
                if labels[&t].is_none_or(|old| candidate < old) {
                    labels.insert(t, Some(candidate));
                }
            }
        }
    }
    labels
}
const IDS: [i64; 10] = [-5, 0, 1, 2, 3, 4, 9, 10, i64::MIN, i64::MAX];
const EDGES: [(i64, i64, f64); 14] = [
    (-5, 0, 12.0),
    (-5, 1, 1.0),
    (-5, 2, 1.0),
    (1, 0, 1.0),
    (1, 2, 0.0),
    (2, 1, 0.0),
    (1, 3, 1.0),
    (2, 3, 1.0),
    (0, 3, 0.0),
    (3, 4, 0.25),
    (4, 4, 0.0),
    (-5, i64::MAX, 5.0),
    (i64::MAX, i64::MIN, 0.0),
    (9, 10, 0.0),
];
#[test]
fn both_methods_match_independent_dijkstra_across_owners_and_bucket_widths() {
    let expected = oracle(&IDS, &EDGES, -5);
    assert_eq!(expected[&0], Some((2.0, 2, 1)));
    assert_eq!(expected[&3], Some((2.0, 2, 1)));
    assert_eq!(expected[&9], None);
    for algorithm in [SsspAlgorithm::Reference, SsspAlgorithm::DeltaStar] {
        for width in [1, 2, 3, 11] {
            for delta in [0.25, 1.0, 4.0, 32.0] {
                let (r, usage, _) = support::resources(16 << 20);
                let op = support::operation(width, IDS.len() as u64);
                let mut opts = options(algorithm);
                opts.delta = delta;
                let mut ps = parts(&op, &IDS, &EDGES, opts, &r);
                let origins = ps.iter().map(SsspPartition::origin).collect::<Vec<_>>();
                let trace = run(&op, &mut ps).unwrap();
                assert_eq!(collected(&ps), expected);
                assert_eq!(
                    ps.iter().map(SsspPartition::origin).collect::<Vec<_>>(),
                    origins
                );
                if algorithm == SsspAlgorithm::DeltaStar && delta == 4.0 {
                    let buckets = trace.iter().filter_map(|(_, b)| *b).collect::<Vec<_>>();
                    assert!(
                        buckets.windows(2).any(|x| x[0] == x[1]),
                        "fixture must exercise same-bucket reactivation"
                    );
                    assert!(buckets.windows(2).all(|x| x[0] <= x[1]));
                    assert!(buckets.contains(&1.0));
                }
                drop(ps);
                assert_eq!(usage.usage().unwrap().live_bytes, 0);
            }
        }
    }
}
#[test]
fn unknown_unreachable_destination_and_missing_source_fail_topology_or_statistics() {
    for algorithm in [SsspAlgorithm::Reference, SsspAlgorithm::DeltaStar] {
        let (r, _, _) = support::resources(1 << 20);
        let op = support::operation(3, 3);
        let mut ps = parts(&op, &[-5, 0, 9], &[(9, 99, 1.0)], options(algorithm), &r);
        stats(&op, &mut ps).unwrap();
        assert!(exchange(&op, &mut ps).unwrap_err().contains("destination"));
        let (r, _, _) = support::resources(1 << 20);
        let mut ps = parts(&op, &[0, 1, 9], &[], options(algorithm), &r);
        assert!(
            stats(&op, &mut ps)
                .unwrap_err()
                .contains("exactly one source")
        );
    }
}
#[test]
fn cap_has_complete_topology_and_no_partial_result() {
    for algorithm in [SsspAlgorithm::Reference, SsspAlgorithm::DeltaStar] {
        let (r, _, _) = support::resources(1 << 20);
        let op = support::operation(3, 2);
        let mut opts = options(algorithm);
        opts.max_rounds = 0;
        let mut ps = parts(&op, &[-5, 0], &[(-5, 0, 1.0)], opts, &r);
        stats(&op, &mut ps).unwrap();
        exchange(&op, &mut ps).unwrap();
        stats(&op, &mut ps).unwrap();
        let phase = phase(&op, &ps);
        for p in &mut ps {
            let c = p.cap_failure(&phase).unwrap().unwrap();
            assert_eq!((c.rounds, c.active, c.reached), (0, 1, 1));
            assert!(p.row_cursor().is_err());
            assert!(p.seal(&phase).is_err());
        }
    }
}
#[test]
fn dropping_incomplete_cursor_cancels_shared_domain_and_releases_snapshots() {
    use std::sync::atomic::Ordering;
    let (r, usage, drops) = support::resources(1 << 20);
    let op = support::operation(2, 2);
    let mut ps = parts(
        &op,
        &[-5, 0],
        &[(-5, 0, 1.0)],
        options(SsspAlgorithm::DeltaStar),
        &r,
    );
    stats(&op, &mut ps).unwrap();
    let phase = phase(&op, &ps);
    let cursor = ps[0].start_emission(&phase).unwrap();
    drop(ps);
    drop(r);
    assert!(usage.usage().unwrap().live_bytes > 0);
    assert_eq!(drops.load(Ordering::SeqCst), 0);
    drop(cursor);
    assert!(usage.checkpoint().is_err());
    assert_eq!(usage.usage().unwrap().live_bytes, 0);
    assert_eq!(drops.load(Ordering::SeqCst), 1);
}
#[test]
fn retained_certified_rows_hold_storage_and_host_lease_until_final_drop() {
    use std::sync::atomic::Ordering;
    let (r, usage, drops) = support::resources(1 << 20);
    let op = support::operation(1, 2);
    let mut ps = parts(
        &op,
        &[-5, 0],
        &[(-5, 0, 1.0)],
        options(SsspAlgorithm::Reference),
        &r,
    );
    run(&op, &mut ps).unwrap();
    let mut cursor = ps[0].row_cursor().unwrap();
    drop(ps);
    drop(r);
    assert_eq!(drops.load(Ordering::SeqCst), 0);
    assert!(usage.usage().unwrap().live_bytes > 0);
    assert_eq!(cursor.next_row().unwrap().unwrap().id, -5);
    drop(cursor);
    assert_eq!(usage.usage().unwrap().live_bytes, 0);
    assert_eq!(drops.load(Ordering::SeqCst), 1);
}

#[test]
fn delta_star_reactivates_an_already_expanded_vertex_inside_one_bucket() {
    let ids = [-5, 0, 1, 2, 3];
    let edges = [
        (-5, 0, 3.0),
        (-5, 1, 0.0),
        (1, 2, 0.0),
        (2, 0, 1.0),
        (0, 3, 0.0),
    ];
    let (r, _, _) = support::resources(2 << 20);
    let op = support::operation(3, 5);
    let mut ps = parts(&op, &ids, &edges, options(SsspAlgorithm::DeltaStar), &r);
    stats(&op, &mut ps).unwrap();
    exchange(&op, &mut ps).unwrap();
    let mut history = vec![];
    loop {
        stats(&op, &mut ps).unwrap();
        let phase = phase(&op, &ps);
        let seals = ps
            .iter_mut()
            .map(|p| p.seal(&phase).unwrap())
            .collect::<Vec<_>>();
        if seals[0].is_some() {
            break;
        }
        let (mode, bucket) = exchange(&op, &mut ps).unwrap();
        assert_eq!((mode, bucket), (SsspMode::DeltaStar, Some(0.0)));
        history.push(
            ps.iter()
                .flat_map(SsspPartition::state_rows)
                .find(|r| r.id == 0)
                .unwrap()
                .label
                .unwrap()
                .distance(),
        );
    }
    assert_eq!(&history[..3], &[3.0, 3.0, 1.0]);
    assert_eq!(collected(&ps), oracle(&ids, &edges, -5));
    assert_eq!(collected(&ps)[&3], Some((1.0, 4, 0)));
}

#[path = "sssp_protocol/mod.rs"]
mod protocol;

#[test]
fn varied_weighted_graphs_match_oracle_with_skew_and_empty_owners() {
    // A fixed generator makes every failing graph reproducible. Quarter-integer
    // weights permit exact FLOAT64 comparisons; the oracle uses no core labels.
    let mut seed = 0x53535350_u64;
    let mut random = || {
        seed = seed.wrapping_mul(6364136223846793005).wrapping_add(1);
        seed >> 32
    };
    for case in 0..40 {
        let n = 2 + random() as usize % 23;
        // Multiples of 5 * 29 put every ID on one owner at both tested
        // widths. The empty owners must still close every barrier.
        let ids = (0..n).map(|i| 145 * i as i64 - 5).collect::<Vec<_>>();
        let mut edges = vec![];
        for &source in &ids {
            for &target in &ids {
                if random() % 5 == 0 {
                    edges.push((source, target, (random() % 33) as f64 / 4.0));
                    if random() % 7 == 0 {
                        edges.push(*edges.last().unwrap());
                    }
                }
            }
        }
        let expected = oracle(&ids, &edges, -5);
        for algorithm in [SsspAlgorithm::Reference, SsspAlgorithm::DeltaStar] {
            for width in [1, 5, 29] {
                let (r, usage, _) = support::resources(32 << 20);
                let op = support::operation(width, n as u64);
                assert!(ids.iter().all(|&id| op.owner(id) == op.owner(-5)));
                let mut opts = options(algorithm);
                opts.delta = [0.25, 1.0, 4.0, 16.0][case % 4];
                let mut ps = parts(&op, &ids, &edges, opts, &r);
                run(&op, &mut ps).unwrap();
                assert_eq!(
                    collected(&ps),
                    expected,
                    "case={case}, width={width}, algorithm={algorithm:?}"
                );
                drop(ps);
                assert_eq!(usage.usage().unwrap().live_bytes, 0);
            }
        }
    }
}
