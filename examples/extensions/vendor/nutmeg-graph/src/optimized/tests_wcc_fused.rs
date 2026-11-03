use super::tests::{execute, graph};
use super::*;

const ORIGINAL: &str = "wccRandomized";
const FUSED: &str = "wccRandomizedFused";

fn labels(batches: &[RecordBatch]) -> Vec<i64> {
    batches
        .iter()
        .flat_map(|batch| {
            batch
                .column(1)
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap()
                .iter()
                .map(|value| value.unwrap().parse::<i64>().unwrap())
        })
        .collect()
}

fn equivalent_rounds(original: &serde_json::Value, fused: &serde_json::Value) {
    let original = original["rounds"].as_array().unwrap();
    let fused = fused["rounds"].as_array().unwrap();
    assert_eq!(original.len(), fused.len());
    for (index, (a, b)) in original.iter().zip(fused).enumerate() {
        let mut a = a.clone();
        let mut b = b.clone();
        // Before the first contraction only, the fused input retains duplicates
        // and reverse edges. Every other trace field must match exactly.
        if index == 0 {
            a.as_object_mut().unwrap().remove("edges_before");
            b.as_object_mut().unwrap().remove("edges_before");
        }
        assert_eq!(a, b, "round {}", index + 1);
    }
}

#[test]
fn fused_matches_complete_contractions_and_labels_with_full_width_ids_and_seeds() {
    let mut ids: Vec<i64> = (1000..1129).collect();
    ids[0] = i64::MIN;
    ids[17] = i64::MAX;
    ids[96] = -9;
    let mut edges = Vec::new();
    for i in 1..96 {
        edges.extend([(i - 1, i), (i, i - 1), (i - 1, i), (i, i)]);
    }
    for i in 97..128 {
        edges.extend([(i - 1, i), (i, i - 1)]);
    }
    edges.push((128, 128)); // A loop-only vertex remains isolated.
    let input_edges = edges.iter().filter(|(a, b)| a != b).count();
    let graph = graph(&ids, &edges, 1);
    let expected: Vec<i64> = (0..ids.len())
        .map(|i| match i {
            0..96 => i64::MIN,
            96..128 => -9,
            _ => ids[i],
        })
        .collect();
    for seed in [0, 42, i64::MAX as u64, 1_u64 << 63, u64::MAX] {
        let options = serde_json::json!({"seed": seed});
        let (old, old_diagnostics) = execute(&graph, ORIGINAL, options.clone());
        let (new, new_diagnostics) = execute(&graph, FUSED, options);
        assert_eq!(labels(&old), expected);
        assert_eq!(new, old);
        assert_eq!(new_diagnostics["seed_bits"], seed.to_string());
        assert_eq!(new_diagnostics["initial_edge_policy"], "raw-non-loop");
        assert_eq!(new_diagnostics["rounds"][0]["edges_before"], input_edges);
        assert!(
            old_diagnostics["rounds"][0]["edges_before"]
                .as_u64()
                .unwrap()
                < input_edges as u64
        );
        equivalent_rounds(&old_diagnostics, &new_diagnostics);
        if seed == u64::MAX {
            let (signed, diagnostics) = execute(&graph, FUSED, serde_json::json!({"seed": -1}));
            assert_eq!(new, signed);
            assert_eq!(new_diagnostics, diagnostics);
        }
    }
}

#[test]
fn fused_matches_at_actual_parallel_widths_and_with_only_loops() {
    let n = BLOCK * 8;
    let ids: Vec<i64> = (0..n as i64).collect();
    let mut edges: Vec<_> = (1..n).flat_map(|i| [(i - 1, i), (i, i - 1)]).collect();
    edges.extend((0..n).map(|i| (i, i)));
    let mut expected = None;
    for width in [1, 2, 8] {
        let graph = graph(&ids, &edges, width);
        let (result, diagnostics) = execute(&graph, FUSED, serde_json::json!({}));
        assert_eq!(diagnostics["kernel_threads"], width);
        assert!(labels(&result).iter().all(|&label| label == 0));
        let (_, original) = execute(&graph, ORIGINAL, serde_json::json!({}));
        equivalent_rounds(&original, &diagnostics);
        let rounds = diagnostics["rounds"].clone();
        if let Some(expected) = &expected {
            assert_eq!(&rounds, expected);
        } else {
            expected = Some(rounds);
        }
    }
    let graph = graph(&[i64::MIN, 0, i64::MAX], &[(0, 0), (0, 0), (2, 2)], 1);
    let (result, diagnostics) = execute(&graph, FUSED, serde_json::json!({}));
    assert_eq!(labels(&result), [i64::MIN, 0, i64::MAX]);
    assert_eq!(diagnostics["iterations"], 0);
    assert_eq!(diagnostics["converged"], true);
}

#[test]
fn fused_caps_preserve_diagnostics_and_release_scratch_without_output() {
    let ids: Vec<i64> = (0..512).collect();
    let edges: Vec<_> = (1..512).flat_map(|i| [(i - 1, i), (i, i - 1)]).collect();
    let graph = graph(&ids, &edges, 1);
    let mut diagnostics = Vec::new();
    for name in [ORIGINAL, FUSED] {
        let context = graph.execution().child(ChildLimits::default()).unwrap();
        let view = graph.with_execution(&context).unwrap();
        let query = Query {
            context,
            diagnostics: Default::default(),
        };
        let args = validate(
            name,
            serde_json::json!({"maxIterations": 1}).as_object().unwrap(),
        )
        .unwrap();
        let error = run(name, &view, &args, &query, &mut |_| {
            panic!("capped WCC must not emit an answer")
        })
        .unwrap_err();
        assert!(error.to_string().contains("did not converge"), "{error}");
        assert_eq!(query.usage().unwrap().live_bytes, 0);
        diagnostics.push(query.diagnostics.lock().unwrap().clone());
        assert_eq!(diagnostics.last().unwrap()["converged"], false);
    }
    equivalent_rounds(&diagnostics[0], &diagnostics[1]);
    let (_, diagnostics) = execute(&graph, FUSED, serde_json::json!({}));
    assert_eq!(diagnostics["converged"], true);
}

#[test]
fn fused_refusals_cancellation_and_output_ownership_leave_registry_usable() {
    prepare_output_schemas().unwrap();
    let registry = SessionRegistry::new(16 << 20);
    let mapping = ColumnMapping::default();
    let mut tx = registry.replacing("g", &mapping, &mapping, StageOrder::Canonical);
    let edges = RecordBatch::try_from_iter([
        (
            "source",
            Arc::new(StringArray::from_iter_values(
                (0..1024).map(|i| i.to_string()),
            )) as ArrayRef,
        ),
        (
            "target",
            Arc::new(StringArray::from_iter_values(
                (1..1025).map(|i| i.to_string()),
            )) as ArrayRef,
        ),
    ])
    .unwrap();
    tx.push_edges(&edges).unwrap();
    tx.finish().unwrap();
    for (options, message) in [
        (serde_json::json!({"memoryLimitBytes": 1024}), "memory"),
        (serde_json::json!({"workLimit": 1}), "work budget"),
    ] {
        let table = registry
            .algorithm(FUSED, "g", options.as_object().unwrap(), ColumnNames::Grust)
            .unwrap();
        let query = table.query().unwrap();
        let error = table.batches_for(&query).unwrap_err();
        assert!(error.to_string().contains(message), "{error}");
        assert_eq!(query.usage().unwrap().live_bytes, 0);
    }
    let table = registry
        .algorithm(FUSED, "g", &Default::default(), ColumnNames::Grust)
        .unwrap();
    let cancelled = table.query().unwrap();
    cancelled.cancel().unwrap();
    assert!(
        table
            .batches_for(&cancelled)
            .unwrap_err()
            .to_string()
            .contains("cancelled")
    );
    let query = table.query().unwrap();
    let batches = table.batches_for(&query).unwrap();
    assert!(labels(&batches).iter().all(|&label| label == 0));
    assert!(query.usage().unwrap().live_bytes > 0);
    drop(batches);
    assert_eq!(query.usage().unwrap().live_bytes, 0);
}

#[test]
fn fused_stream_cancellation_stops_between_admitted_output_batches() {
    let ids: Vec<i64> = (0..1024).collect();
    let edges: Vec<_> = (1..1024).map(|i| (i - 1, i)).collect();
    let graph = graph(&ids, &edges, 1);
    let context = graph.execution().child(ChildLimits::default()).unwrap();
    let view = graph.with_execution(&context).unwrap();
    let query = Query {
        context,
        diagnostics: Default::default(),
    };
    let args = validate(FUSED, &Default::default()).unwrap();
    let mut batches = 0;
    let error = run(FUSED, &view, &args, &query, &mut |_| {
        batches += 1;
        query.cancel()?;
        Ok(true)
    })
    .unwrap_err();
    assert!(error.to_string().contains("cancelled"), "{error}");
    assert_eq!(batches, 1);
    assert_eq!(query.usage().unwrap().live_bytes, 0);
    let (_, diagnostics) = execute(&graph, FUSED, serde_json::json!({}));
    assert_eq!(diagnostics["converged"], true);
}
