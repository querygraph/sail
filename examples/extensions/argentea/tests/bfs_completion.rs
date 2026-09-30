mod bfs_support;
use bfs_support::*;
use sail_argentea_core::*;

#[test]
fn suppressed_candidates_and_rewritten_completion_cannot_certify_false_convergence() {
    let mut accepted = Vec::new();
    for algorithm in [
        BfsAlgorithm::Reference,
        BfsAlgorithm::Frontier,
        BfsAlgorithm::DirectionOptimizing,
    ] {
        for p in [1, 3] {
            for scalar_wire in [false, true] {
                let op = operation(p, 3);
                let (res, _, _) = resources(1 << 20);
                let mut opts = options(0, algorithm);
                // One frontier arc and one disconnected arc keep direction BFS
                // in Push, so all three algorithms exercise the candidate path.
                opts.alpha = 1;
                let mut ps = parts(&op, &[0, 1, 2], &[(0, 1), (2, 2)], opts, &res).unwrap();
                setup(&op, &mut ps).unwrap();
                stats(&op, &mut ps).unwrap();
                let phase = phase(&op, &ps);
                let before = ps
                    .iter()
                    .map(|part| part.state_rows().collect::<Vec<_>>())
                    .collect::<Vec<_>>();
                let mut completions = Vec::new();
                let mut discarded = 0;
                for part in &mut ps {
                    let mut cursor = part.start_emission(&phase).unwrap();
                    assert!(matches!(cursor.mode(), BfsMode::Reference | BfsMode::Push));
                    while let Some(message) = cursor.next_values().unwrap() {
                        assert_eq!(message.producer, 0);
                        assert!(matches!(
                            message.payload,
                            BfsPayload::Candidate { target: 1, .. }
                        ));
                        discarded += 1;
                    }
                    let mut completion = cursor.finish().unwrap();
                    // The original two-vertex counterexample rewrites the
                    // producer's completion to agree with the truncated stream.
                    completion.sequences.fill(0);
                    completions.push(completion);
                }
                assert_eq!(discarded, 1);
                let mut rejected = 0;
                for (recipient, part) in ps.iter_mut().enumerate() {
                    let completion = &completions[0];
                    let result = if scalar_wire {
                        part.finish_producer_values(
                            &phase,
                            BfsCompletionValues {
                                origin: completion.origin,
                                producer: completion.producer,
                                mode: completion.mode,
                                sequence: 0,
                                total_messages: 0,
                            },
                        )
                    } else {
                        part.finish_producer(completion)
                    };
                    if let Err(error) = result {
                        assert_eq!(
                            error,
                            "BFS completion count does not match producer statistics"
                        );
                        rejected += 1;
                        assert!(part.finish(&phase).is_err());
                        assert!(part.row_cursor().is_err());
                        assert_eq!(part.state_rows().collect::<Vec<_>>(), before[recipient]);
                        assert_eq!(part.next_phase(), phase.number);
                        assert_eq!(part.levels(), 0);
                    }
                }
                println!(
                    "BFS_TRUNCATED_COMPLETION algorithm={algorithm:?} partitions={p} scalar_wire={scalar_wire} discarded={discarded} rejected={rejected}"
                );
                if rejected != p {
                    // Preserve the pre-fix false certificate as regression
                    // evidence instead of stopping at the first failing cell.
                    accepted.push((algorithm, p, scalar_wire));
                    for completion in &completions[1..] {
                        for part in &mut ps {
                            part.finish_producer(completion).unwrap();
                        }
                    }
                    for part in &mut ps {
                        part.finish(&phase).unwrap();
                    }
                    stats(&op, &mut ps).unwrap();
                    let final_phase = bfs_support::phase(&op, &ps);
                    let convergence = ps[0].seal(&final_phase).unwrap().unwrap();
                    assert_eq!(convergence.reached, 1);
                    println!("BFS_FALSE_CERTIFICATE {convergence:?}");
                }
            }
        }
    }
    assert!(
        accepted.is_empty(),
        "accepted truncated candidate streams: {accepted:?}"
    );
}

#[test]
fn candidate_completion_totals_count_parallel_and_already_reached_targets() {
    for algorithm in [BfsAlgorithm::Reference, BfsAlgorithm::Frontier] {
        for p in [1, 3, 8] {
            let op = operation(p, 4);
            let (res, _, _) = resources(1 << 20);
            let ids = [0, 1, 2, 3];
            // Candidates include self loops, parallel arcs and targets reached
            // in earlier levels. The declared count is frontier arcs, not the
            // number of new labels, total graph arcs, or local-recipient rows.
            let arcs = [(0, 0), (0, 1), (0, 1), (1, 0), (1, 2), (3, 3)];
            let mut ps = parts(&op, &ids, &arcs, options(0, algorithm), &res).unwrap();
            run(&op, &mut ps).unwrap();
            let rows = collected(&ps);
            assert_eq!(rows, oracle(&ids, &arcs, 0));
            assert!(certificate(&rows, &arcs, 0));
        }
    }
}
