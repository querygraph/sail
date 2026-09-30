mod allocations;
use crate::{Operation, Resources, WeightedAdjacency, adjacency::Adjacency};
use grust_procedures::{ExecutionContext, ExecutionLimits};
use sail_native_resource_ffi::MemoryLease;
use std::sync::Arc;

fn operation(partitions: usize, vertices: usize) -> Operation {
    Operation {
        package: "csr-test".into(),
        session: "session".into(),
        operation: "operation".into(),
        snapshot: "snapshot".into(),
        generation: 1,
        partitions,
        vertices: vertices.max(1) as u64,
    }
}
fn resources(bytes: usize, work_units: usize) -> Resources {
    Resources::new(
        ExecutionContext::new(ExecutionLimits {
            memory_bytes: bytes,
            work_units,
            batch_rows: 16,
            deadline: None,
        })
        .unwrap(),
        MemoryLease::new(Arc::new(()), bytes as u64),
    )
    .unwrap()
}
fn check_graph(ids: &[i64], p: usize) {
    let op = operation(p, ids.len());
    let owner = ids.first().map_or(0, |id| op.owner(*id));
    let r = resources(1 << 24, usize::MAX);
    // Reverse source order, parallel arcs, self loops, isolates, remote targets,
    // and repeated source runs distinguish offsets from end offsets.
    let weighted: Vec<_> = ids
        .iter()
        .rev()
        .enumerate()
        .filter(|(i, _)| i % 3 != 0)
        .flat_map(|(_, &source)| [(source, i64::MAX, 2.5), (source, source, -0.0)])
        .chain(ids.first().map(|&source| (source, i64::MIN, 4.0)))
        .collect();
    let edges: Vec<_> = weighted.iter().map(|&(s, t, _)| (s, t)).collect();
    let graph = Adjacency::build(&op, owner, ids, &edges, &r).unwrap();
    let weights = WeightedAdjacency::build(&op, owner, ids, &weighted, &r).unwrap();
    let mut sorted = ids.to_vec();
    sorted.sort_unstable();
    assert_eq!(graph.vertices, sorted);
    assert_eq!(weights.vertices(), sorted);
    assert_eq!(graph.offsets.len(), ids.len() + 1);
    assert_eq!(graph.offsets[0], 0);
    assert_eq!(*graph.offsets.last().unwrap(), edges.len());
    for (i, &id) in sorted.iter().enumerate() {
        let expected: Vec<_> = weighted
            .iter()
            .filter(|(source, _, _)| *source == id)
            .map(|&(_, target, weight)| (target, if weight == 0.0 { 0.0 } else { weight }))
            .collect();
        let actual = weights.outgoing(id).unwrap();
        assert_eq!(actual, expected);
        for (a, b) in actual.iter().zip(&expected) {
            assert_eq!(a.1.to_bits(), b.1.to_bits());
        }
        let expected_targets: Vec<_> = expected.iter().map(|&(target, _)| target).collect();
        assert_eq!(
            &graph.targets[graph.offsets[i]..graph.offsets[i + 1]],
            expected_targets
        );
    }
    drop((graph, weights));
    assert_eq!(r.execution.usage().unwrap().live_bytes, 0);
}

#[test]
fn adjacency_restores_offsets_and_preserves_arc_order_for_all_id_layouts() {
    for p in [1, 3, 32, 65_536] {
        let ids: Vec<_> = (0..37).rev().map(|i| (i - 19) * p as i64).collect();
        check_graph(&ids, p);
        let irregular: Vec<_> = ids.iter().copied().filter(|id| *id != 0).collect();
        check_graph(&irregular, p);
    }
    for ids in [
        vec![],
        vec![i64::MIN],
        vec![i64::MAX],
        vec![i64::MIN, 0, i64::MAX],
        vec![i64::MIN + 2, i64::MIN + 1, i64::MIN],
        vec![i64::MAX, i64::MAX - 1, i64::MAX - 2],
    ] {
        check_graph(&ids, 1);
    }
}

#[test]
fn adjacency_lookup_rejects_missing_and_foreign_sources_without_leaks() {
    let r = resources(1 << 24, usize::MAX);
    for (p, ids, sources) in [
        (
            3,
            vec![-6, -3, 0, 3],
            vec![-9, -5, -2, 6, i64::MIN, i64::MAX],
        ),
        (3, vec![-6, 0, 3], vec![-3, -5, 6]),
        (1, vec![i64::MIN], vec![i64::MIN + 1, i64::MAX]),
        (1, vec![i64::MAX], vec![i64::MIN, i64::MAX - 1]),
        (1, vec![], vec![0]),
    ] {
        let op = operation(p, ids.len());
        let owner = ids.first().map_or(0, |id| op.owner(*id));
        for source in sources {
            assert_eq!(
                Adjacency::build(&op, owner, &ids, &[(source, 0)], &r).unwrap_err(),
                "source is not owned"
            );
            assert_eq!(
                WeightedAdjacency::build(&op, owner, &ids, &[(source, 0, 1.0)], &r).unwrap_err(),
                "weighted source is not owned"
            );
            assert_eq!(r.execution.usage().unwrap().live_bytes, 0);
        }
    }
    for ids in [&[0, 0][..], &[0, 1][..]] {
        let op = operation(3, ids.len());
        assert!(Adjacency::build(&op, 0, ids, &[], &r).is_err());
        assert!(WeightedAdjacency::build(&op, 0, ids, &[], &r).is_err());
        assert_eq!(r.execution.usage().unwrap().live_bytes, 0);
    }
}

#[test]
fn adjacency_build_allocation_and_work_counters() {
    let mut cells = Vec::new();
    for p in [1, 3, 32] {
        for dense in [true, false] {
            for n in [16usize, 65_536] {
                let stride = p * if dense { 1 } else { 2 };
                let ids: Vec<_> = (0..n)
                    .rev()
                    .map(|i| (i as i64 - 7) * stride as i64)
                    .collect();
                let edges: Vec<_> = ids.iter().flat_map(|&id| [(id, 0); 4]).collect();
                let weighted: Vec<_> = edges.iter().map(|&(s, t)| (s, t, 1.0)).collect();
                let op = operation(p, n);
                for is_weighted in [false, true] {
                    let r = resources(1 << 28, usize::MAX);
                    let counts = if is_weighted {
                        let (graph, counts) = allocations::measure(|| {
                            WeightedAdjacency::build(&op, 0, &ids, &weighted, &r).unwrap()
                        });
                        assert_eq!(graph.arc_count(), weighted.len());
                        drop(graph);
                        counts
                    } else {
                        let (graph, counts) = allocations::measure(|| {
                            Adjacency::build(&op, 0, &ids, &edges, &r).unwrap()
                        });
                        assert_eq!(graph.targets.len(), edges.len());
                        drop(graph);
                        counts
                    };
                    let usage = r.execution.usage().unwrap();
                    let work = usage.counted_work().unwrap();
                    println!(
                        "CSR_COUNTER p={p} dense={dense} n={n} m={} weighted={is_weighted} allocation_calls={} allocated_bytes={} peak_requested_bytes={} peak_admitted_bytes={} work={work}",
                        edges.len(),
                        counts.calls,
                        counts.bytes,
                        counts.peak,
                        usage.peak_bytes
                    );
                    assert_eq!(usage.live_bytes, 0);
                    cells.push((p, dense, n, is_weighted, counts, work));
                }
            }
        }
    }
    // No additional V-sized allocation beyond the final CSR, regardless of ID
    // layout. Compare two sizes to exclude fixed metadata/reservation overhead.
    for pair in cells.chunks_exact(4) {
        for weighted in 0..2 {
            let (_, dense, n0, _, c0, _) = pair[weighted];
            let (_, _, n1, _, c1, work) = pair[weighted + 2];
            let width = 8 + size_of::<usize>() + 4 * if weighted == 1 { 16 } else { 8 };
            assert_eq!(c1.bytes - c0.bytes, (n1 - n0) * width);
            assert_eq!(c1.peak - c0.peak, (n1 - n0) * width);
            let bits = n1.ilog2() as usize + 1;
            let expected = if weighted == 1 {
                n1 * bits + 2 * n1 + 8 * n1 * if dense { 1 } else { bits }
            } else {
                10 * n1
            };
            assert_eq!(work, expected);
        }
    }
}

#[test]
fn adjacency_reduced_scratch_admission_and_weighted_work_budget_are_enforced() {
    let n = 8192;
    let ids: Vec<_> = (0..n as i64).collect();
    let op = operation(1, n);
    let budget = n * (64 - size_of::<usize>()) + 4096 + size_of::<usize>();
    for weighted in [false, true] {
        let r = resources(budget, usize::MAX);
        if weighted {
            drop(WeightedAdjacency::build(&op, 0, &ids, &[], &r).unwrap());
        } else {
            drop(Adjacency::build(&op, 0, &ids, &[], &r).unwrap());
        }
        assert_eq!(r.execution.usage().unwrap().live_bytes, 0);
        let r = resources(budget - 1, usize::MAX);
        let error = if weighted {
            WeightedAdjacency::build(&op, 0, &ids, &[], &r).unwrap_err()
        } else {
            Adjacency::build(&op, 0, &ids, &[], &r).unwrap_err()
        };
        assert!(error.contains("memory"));
        assert_eq!(r.execution.usage().unwrap().live_bytes, 0);
    }
    let edges: Vec<_> = ids.iter().map(|&id| (id, 0, 1.0)).collect();
    let work = n * (n.ilog2() as usize + 1) + 4 * n;
    let r = resources(1 << 24, work);
    drop(WeightedAdjacency::build(&op, 0, &ids, &edges, &r).unwrap());
    assert_eq!(r.execution.usage().unwrap().counted_work(), Some(work));
    let r = resources(1 << 24, work - 1);
    assert!(
        WeightedAdjacency::build(&op, 0, &ids, &edges, &r)
            .unwrap_err()
            .contains("work")
    );
    assert_eq!(r.execution.usage().unwrap().live_bytes, 0);
}
