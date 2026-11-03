use super::*;
use grust_algorithms::{Orientation, PageRankOptions, ProjectionEdge};

pub(super) fn graph(ids: &[i64], edges: &[(usize, usize)], width: usize) -> GraphProjection {
    let context = ExecutionContext::new(ExecutionLimits {
        memory_bytes: 256 << 20,
        work_units: usize::MAX,
        batch_rows: 257,
        deadline: None,
    })
    .unwrap()
    .with_concurrency(width)
    .unwrap();
    GraphProjection::from_topology(
        SnapshotIdentity::new("optimized-test".into(), "1".into(), "test".into()).unwrap(),
        ids.iter().map(|id| id.to_string().into()).collect(),
        edges
            .iter()
            .enumerate()
            .map(|(ordinal, &(source, target))| ProjectionEdge {
                source,
                target,
                ordinal,
                id: None,
            })
            .collect(),
        None,
        Orientation::Outgoing,
        &context,
    )
    .unwrap()
}

pub(super) fn execute(
    graph: &GraphProjection,
    name: &str,
    options: serde_json::Value,
) -> (Vec<RecordBatch>, serde_json::Value) {
    let query = Query {
        context: graph.execution().clone(),
        diagnostics: Default::default(),
    };
    let args = validate(name, options.as_object().unwrap()).unwrap();
    let mut batches = Vec::new();
    run(name, graph, &args, &query, &mut |batch| {
        batches.push(batch);
        Ok(true)
    })
    .unwrap();
    let diagnostics = query.diagnostics.lock().unwrap().clone();
    (batches, diagnostics)
}

fn scores(batches: &[RecordBatch]) -> Vec<f64> {
    batches
        .iter()
        .flat_map(|batch| {
            batch
                .column(1)
                .as_any()
                .downcast_ref::<Float64Array>()
                .unwrap()
                .values()
                .iter()
                .copied()
        })
        .collect()
}

#[test]
fn delta_matches_reference_with_dangling_isolates_duplicates_and_loop() {
    let graph = graph(
        &[-5, 0, 1, 2, 9, 10, 11, 12, 13],
        &[
            (6, 4),
            (3, 5),
            (0, 3),
            (0, 5),
            (6, 4),
            (1, 3),
            (5, 3),
            (7, 6),
            (7, 2),
            (4, 2),
            (3, 4),
            (6, 6),
            (5, 3),
            (2, 7),
            (1, 0),
            (1, 2),
        ],
        1,
    );
    let (batches, diagnostics) = execute(
        &graph,
        "pagerankDelta",
        serde_json::json!({"tolerance": 1e-11}),
    );
    let reference = grust_algorithms::pagerank(
        &graph,
        PageRankOptions {
            tolerance: 1e-13,
            ..PageRankOptions::default()
        },
    )
    .unwrap();
    let actual = scores(&batches);
    let error: f64 = actual
        .iter()
        .zip(reference.values())
        .map(|(a, b)| (a - b).abs())
        .sum();
    assert!(error <= 1e-10, "L1={error}");
    assert!((actual.iter().sum::<f64>() - 1.0).abs() < 1e-13);
    assert!(diagnostics["residual"].as_f64().unwrap() <= 1e-11);
    assert_eq!(diagnostics["converged"], true);
    assert!(
        diagnostics["rounds"]
            .as_array()
            .unwrap()
            .iter()
            .any(|round| round["active_edges"].as_u64().unwrap() < graph.edge_count() as u64)
    );
    assert!(
        diagnostics["rounds"]
            .as_array()
            .unwrap()
            .iter()
            .any(|round| round["reactivated_vertices"].as_u64().unwrap() > 0)
    );
}

#[test]
fn delta_uses_tolerance_scaled_cutoff_and_reduces_frontier_work() {
    // The short components settle immediately; a longer directed path still
    // propagates corrections. This pins actual inactive-edge avoidance rather
    // than inferring it from an algorithm name or elapsed time.
    let n = 128;
    let ids: Vec<i64> = (0..n as i64).collect();
    let mut edges: Vec<_> = (0..63).map(|i| (i, i + 1)).collect();
    edges.push((63, 63));
    for i in (64..n).step_by(2) {
        edges.extend([(i, i + 1), (i + 1, i + 1)]);
    }
    let graph = graph(&ids, &edges, 1);
    let tolerance = 1e-8;
    let (_, diagnostics) = execute(
        &graph,
        "pagerankDelta",
        serde_json::json!({"tolerance": tolerance}),
    );
    assert_eq!(diagnostics["converged"], true);
    assert_eq!(
        diagnostics["activation_policy"],
        "tolerance-capped-local-residual"
    );
    let rounds = diagnostics["rounds"].as_array().unwrap();
    assert!(rounds.len() > 2);
    for round in rounds {
        let mass = round["activation_mass"].as_f64().unwrap();
        let residual = round["activation_residual_l1"].as_f64().unwrap();
        let expected = (residual / (2.0 * n as f64)).min(tolerance * mass / (4.0 * n as f64));
        assert_eq!(round["activation_threshold"].as_f64().unwrap(), expected);
    }
    let first_edges = rounds[0]["active_edges"].as_u64().unwrap();
    assert!(first_edges > 2);
    assert!(
        rounds[1..]
            .iter()
            .any(|round| round["active_edges"].as_u64().unwrap() < first_edges)
    );
    assert!(diagnostics["residual"].as_f64().unwrap() <= tolerance);
}

#[test]
fn delta_is_bitwise_stable_across_actual_parallel_widths() {
    // Eight nonempty fixed source blocks: this reaches width=8, not a serial
    // fixture that happens to run with eight as a configuration value.
    let n = BLOCK * 8;
    let ids: Vec<i64> = (0..n as i64).collect();
    let mut edges = Vec::new();
    for i in 0..n {
        if i % 11 != 0 {
            edges.push((i, (i * 17 + 3) % n));
            edges.push((i, (i + 7) % n));
            if i % 7 == 0 {
                edges.push((i, (i + 7) % n));
            }
        }
    }
    let mut baseline = None;
    for width in [1, 2, 8] {
        let graph = graph(&ids, &edges, width);
        let (batches, diagnostics) = execute(&graph, "pagerankDelta", serde_json::json!({}));
        assert_eq!(diagnostics["kernel_threads"], width);
        assert!(diagnostics["iterations"].as_u64().unwrap() > 1);
        let actual = scores(&batches)
            .iter()
            .map(|value| value.to_bits())
            .collect::<Vec<_>>();
        if let Some(expected) = &baseline {
            assert_eq!(&actual, expected);
        } else {
            baseline = Some(actual);
        }
    }
}

#[test]
fn delta_zero_push_stationary_and_capped_failure_are_explicit() {
    let cycle = graph(&[0, 1, 2], &[(0, 1), (1, 2), (2, 0)], 1);
    let (_, diagnostics) = execute(&cycle, "pagerankDelta", serde_json::json!({}));
    assert_eq!(diagnostics["iterations"], 0);
    assert_eq!(diagnostics["converged"], true);
    let path = graph(&[0, 1, 2, 3], &[(0, 1), (1, 2), (2, 3)], 1);
    let (batches, diagnostics) = execute(
        &path,
        "pagerankDelta",
        serde_json::json!({"maxIterations": 1}),
    );
    assert_eq!(diagnostics["converged"], false);
    assert!(diagnostics["residual"].as_f64().unwrap() > 1e-8);
    assert!((scores(&batches).iter().sum::<f64>() - 1.0).abs() < 1e-14);
}

#[test]
fn contraction_preserves_components_and_numeric_minimum_labels() {
    let graph = graph(
        &[9, 10, -8, 3, 20, 21, 99],
        &[(0, 1), (1, 2), (1, 2), (2, 2), (3, 4), (4, 5)],
        1,
    );
    for seed in [0, 42, -1, i64::MIN, i64::MAX] {
        let (batches, diagnostics) =
            execute(&graph, "wccRandomized", serde_json::json!({"seed": seed}));
        let labels: Vec<&str> = batches
            .iter()
            .flat_map(|batch| {
                batch
                    .column(1)
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .unwrap()
                    .iter()
                    .map(Option::unwrap)
            })
            .collect();
        assert_eq!(labels, ["-8", "-8", "-8", "3", "3", "3", "99"]);
        assert_eq!(diagnostics["converged"], true);
    }
}

#[test]
fn unsigned_maximum_seed_preserves_every_bit() {
    let graph = graph(
        &[i64::MIN, 0, 9, 10, i64::MAX],
        &[(0, 1), (1, 2), (2, 3)],
        1,
    );
    let (positive, positive_diagnostics) = execute(
        &graph,
        "wccRandomized",
        serde_json::json!({"seed": u64::MAX}),
    );
    let (signed, signed_diagnostics) =
        execute(&graph, "wccRandomized", serde_json::json!({"seed": -1}));
    assert_eq!(positive, signed);
    assert_eq!(positive_diagnostics, signed_diagnostics);
    assert_eq!(positive_diagnostics["seed_bits"], u64::MAX.to_string());
    assert!(
        validate(
            "wccRandomized",
            serde_json::json!({"seed": 42.5}).as_object().unwrap()
        )
        .is_err()
    );
}

#[test]
fn contraction_reduces_a_long_path_and_is_deterministic() {
    let n = BLOCK * 2 + 1;
    let ids: Vec<i64> = (0..n as i64).collect();
    let edges: Vec<_> = (1..n).map(|i| (i - 1, i)).collect();
    let mut baseline = None;
    for width in [1, 2, 8] {
        let graph = graph(&ids, &edges, width);
        let (batches, diagnostics) =
            execute(&graph, "wccRandomized", serde_json::json!({"seed": 42}));
        assert_eq!(diagnostics["kernel_threads"], width.min(3));
        assert!(diagnostics["iterations"].as_u64().unwrap() < 40);
        assert!(batches.iter().all(|batch| {
            batch
                .column(1)
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap()
                .iter()
                .all(|value| value == Some("0"))
        }));
        let rounds = diagnostics["rounds"].clone();
        if let Some(expected) = &baseline {
            assert_eq!(&rounds, expected);
        } else {
            baseline = Some(rounds);
        }
    }
}

#[test]
fn output_retains_reservation_and_limits_leave_registry_usable() {
    prepare_output_schemas().unwrap();
    let registry = SessionRegistry::new(16 << 20);
    let mapping = ColumnMapping::default();
    let mut tx = registry.replacing("g", &mapping, &mapping, StageOrder::Canonical);
    let edges = RecordBatch::try_from_iter([
        (
            "source",
            Arc::new(StringArray::from(vec!["0", "1", "2"])) as ArrayRef,
        ),
        (
            "target",
            Arc::new(StringArray::from(vec!["1", "2", "3"])) as ArrayRef,
        ),
    ])
    .unwrap();
    tx.push_edges(&edges).unwrap();
    tx.finish().unwrap();
    let refused = registry
        .algorithm(
            "pagerankDelta",
            "g",
            serde_json::json!({"memoryLimitBytes": 1024})
                .as_object()
                .unwrap(),
            ColumnNames::Grust,
        )
        .unwrap();
    assert!(
        refused
            .batches()
            .unwrap_err()
            .to_string()
            .contains("memory")
    );
    let table = registry
        .algorithm(
            "pagerankDelta",
            "g",
            &Default::default(),
            ColumnNames::Grust,
        )
        .unwrap();
    let query = table.query().unwrap();
    let batches = table.batches_for(&query).unwrap();
    assert_eq!(registry.reads().unwrap().len(), 2);
    let used = query.usage().unwrap().live_bytes;
    assert!(used > 0, "exported buffers must retain their reservation");
    drop(batches);
    assert_eq!(query.usage().unwrap().live_bytes, 0);
    let cancelled = table.query().unwrap();
    cancelled.cancel().unwrap();
    assert!(
        table
            .batches_for(&cancelled)
            .unwrap_err()
            .to_string()
            .contains("cancelled")
    );
    assert!(!table.batches().unwrap().is_empty());
}

#[test]
fn unsupported_optimized_options_are_rejected_before_execution() {
    for (algorithm, options) in [
        ("pagerankDelta", serde_json::json!({"precision":"f32"})),
        (
            "pagerankDelta",
            serde_json::json!({"personalization":[1.0]}),
        ),
        ("pagerankDelta", serde_json::json!({"damping":1.0})),
        ("pagerankDelta", serde_json::json!({"tolerance":0.0})),
        ("wccRandomized", serde_json::json!({"maxIterations":0})),
        (
            "wccRandomized",
            serde_json::json!({"orientation":"undirected"}),
        ),
    ] {
        assert!(validate(algorithm, options.as_object().unwrap()).is_err());
    }
}

#[test]
fn empty_graphs_keep_declared_output_schemas() {
    let graph = graph(&[], &[], 1);
    for name in NAMES {
        let (batches, _) = execute(&graph, name, serde_json::json!({}));
        assert_eq!(batches.len(), 1);
        assert_eq!(batches[0].num_rows(), 0);
        assert_eq!(batches[0].schema(), schema(name).unwrap());
    }
}

#[test]
fn local_catalog_is_documented_separately_from_upstream() {
    let document = include_str!("../../OPTIMIZED_ALGORITHMS.md");
    for name in NAMES {
        assert!(algorithm_names().contains(&name));
        assert!(document.contains(&format!("## `{name}`")));
        assert_eq!(definition_of(name).unwrap().provider, "nutmeg.experimental");
    }
    assert!(algorithm_names().contains(&"pagerank"));
    assert!(algorithm_names().contains(&"wcc"));
}

#[test]
fn parallel_push_work_limit_and_stream_cancellation_are_query_scoped() {
    let n = BLOCK * 2;
    let ids: Vec<i64> = (0..n as i64).collect();
    // A large hub crosses the block's cancellation/work checkpoints once per
    // edge, rather than only checking once before the entire adjacency.
    let mut edges: Vec<_> = (1..n).map(|target| (0, target)).collect();
    edges.extend(
        (1..n)
            .filter(|i| i % 7 != 0)
            .map(|source| (source, source / 2)),
    );
    let graph = graph(&ids, &edges, 2);
    let context = graph
        .execution()
        .child(ChildLimits {
            work_units: 3 * edges.len() + 2 * n + 100,
            concurrency: Some(2),
            ..ChildLimits::default()
        })
        .unwrap();
    let view = graph.with_execution(&context).unwrap();
    let query = Query {
        context,
        diagnostics: Default::default(),
    };
    let args = validate("pagerankDelta", &Default::default()).unwrap();
    let error = run("pagerankDelta", &view, &args, &query, &mut |_| Ok(true)).unwrap_err();
    assert!(error.to_string().contains("work budget"), "{error}");
    drop(view);
    assert_eq!(query.usage().unwrap().live_bytes, 0);

    let context = graph.execution().child(ChildLimits::default()).unwrap();
    let view = graph.with_execution(&context).unwrap();
    let query = Query {
        context,
        diagnostics: Default::default(),
    };
    let args = validate("wccRandomized", &Default::default()).unwrap();
    let mut batches = 0;
    let error = run("wccRandomized", &view, &args, &query, &mut |_| {
        batches += 1;
        query.cancel()?;
        Ok(true)
    })
    .unwrap_err();
    assert!(error.to_string().contains("cancelled"), "{error}");
    assert_eq!(batches, 1);
    assert_eq!(query.usage().unwrap().live_bytes, 0);
    // Cancellation and a work refusal did not poison the projection owner.
    let (_, diagnostics) = execute(&graph, "wccRandomized", serde_json::json!({}));
    assert_eq!(diagnostics["converged"], true);
}
