//! Tolerance-scaled residual pushes, reactivation, and a final certificate.
use super::*;
use rayon::prelude::*;

pub(super) fn run(
    graph: &GraphProjection,
    args: &ValidatedArguments,
    query: &Query,
    emit: &mut dyn FnMut(RecordBatch) -> Result<bool>,
) -> Result<bool> {
    let context = graph.execution();
    let n = graph.node_count();
    let damping = number(args, "damping")?;
    let tolerance = number(args, "tolerance")?;
    let limit = integer(args, "maxIterations")? as usize;
    // CSR, all dense state, fixed-block descriptors, and at most one sparse
    // contribution pair per input edge. No reservation depends on worker count.
    let _scratch = admitted(context, n, graph.edge_count(), 128, 24)?;
    let (workers, threads) = pool(context, n)?;
    let mut offsets = allocated(n + 1, 0usize)?;
    let mut meter = context.work_meter();
    for edge in graph.edges() {
        meter.charge(1).map_err(err)?;
        offsets[edge.source + 1] += 1;
    }
    for i in 0..n {
        offsets[i + 1] += offsets[i];
    }
    let mut cursor = offsets[..n].to_vec();
    let mut targets = allocated(graph.edge_count(), 0usize)?;
    for edge in graph.edges() {
        meter.charge(1).map_err(err)?;
        targets[cursor[edge.source]] = edge.target;
        cursor[edge.source] += 1;
    }
    drop(cursor);
    let mut x = allocated(n, if n == 0 { 0.0 } else { 1.0 / n as f64 })?;
    let mut residual = allocated(n, 0.0f64)?;
    let mut push = allocated(n, 0.0f64)?;
    let mut normalized = allocated(n, 0.0f64)?;
    let mut certificate = allocated(n, 0.0f64)?;
    let mut ever_active = allocated(n, false)?;
    let mut was_active = allocated(n, false)?;
    if n > 0 {
        let mass = x.iter().sum::<f64>();
        for value in &mut x {
            *value /= mass;
        }
    }
    transition(context, &offsets, &targets, &x, damping, &mut residual)?;
    for i in 0..n {
        residual[i] -= x[i];
    }
    let mut norm = residual.iter().map(|v| v.abs()).sum::<f64>();
    let mut true_residual = norm;
    let mut converged = norm <= tolerance;
    let mut rounds = Vec::new();
    let _trace = context.reserve(limit.saturating_mul(1024)).map_err(err)?;
    rounds.try_reserve_exact(limit).map_err(err)?;
    let mut iterations = 0;
    let mut certificate_passes = 1usize;
    let mut frontier_edges = 0usize;
    if converged {
        normalized.copy_from_slice(&x);
    }
    while !converged && iterations < limit {
        context.checkpoint().map_err(err)?;
        let activation_mass = x.iter().sum::<f64>();
        let activation_norm = norm;
        // A tolerance-scaled cutoff lets small corrections become inactive as
        // the solution settles. The relative cap keeps inactive L1 <= norm/2,
        // preserving contraction even after a certificate rebase.
        let threshold =
            (norm / (2.0 * n as f64)).min(tolerance * activation_mass / (4.0 * n as f64));
        let mut active = 0usize;
        let mut active_edges = 0usize;
        let mut reactivated = 0usize;
        let mut dangling_push = 0.0;
        for i in 0..n {
            meter.charge(1).map_err(err)?;
            push[i] = if residual[i].abs() > threshold {
                residual[i]
            } else {
                0.0
            };
            if push[i] != 0.0 {
                if ever_active[i] && !was_active[i] {
                    reactivated += 1;
                }
                ever_active[i] = true;
                active += 1;
                active_edges += offsets[i + 1] - offsets[i];
                if offsets[i] == offsets[i + 1] {
                    dangling_push += push[i];
                }
                x[i] += push[i];
                residual[i] -= push[i];
            }
            was_active[i] = push[i] != 0.0;
        }
        // Each source block keeps its own sparse messages. Reduce targets within
        // that fixed block; merge blocks in source order after all workers join.
        // Inactive sources do not traverse their adjacency.
        let build = |block: usize| -> Result<Vec<(usize, f64)>> {
            let mut work = context.work_meter();
            let start = block * BLOCK;
            let end = n.min(start + BLOCK);
            let count = (start..end)
                .filter(|&i| push[i] != 0.0)
                .map(|i| offsets[i + 1] - offsets[i])
                .sum();
            let mut messages = Vec::new();
            messages.try_reserve_exact(count).map_err(err)?;
            for i in start..end {
                work.charge(1).map_err(err)?;
                let degree = offsets[i + 1] - offsets[i];
                if push[i] == 0.0 || degree == 0 {
                    continue;
                }
                let value = damping * push[i] / degree as f64;
                for &target in &targets[offsets[i]..offsets[i + 1]] {
                    work.charge(1).map_err(err)?;
                    messages.push((target, value));
                }
            }
            context
                .charge_work(
                    messages
                        .len()
                        .saturating_mul(messages.len().max(1).ilog2() as usize + 1),
                )
                .map_err(err)?;
            messages.sort_unstable_by_key(|pair| pair.0);
            context.checkpoint().map_err(err)?;
            let mut kept = 0;
            for index in 0..messages.len() {
                if kept > 0 && messages[kept - 1].0 == messages[index].0 {
                    messages[kept - 1].1 += messages[index].1;
                } else {
                    messages[kept] = messages[index];
                    kept += 1;
                }
            }
            messages.truncate(kept);
            Ok(messages)
        };
        let blocks = n.div_ceil(BLOCK);
        let messages: Vec<Vec<(usize, f64)>> = match &workers {
            Some(pool) => pool.install(|| {
                (0..blocks)
                    .into_par_iter()
                    .map(build)
                    .collect::<Result<_>>()
            })?,
            None => (0..blocks).map(build).collect::<Result<_>>()?,
        };
        for block in messages {
            for (target, value) in block {
                meter.charge(1).map_err(err)?;
                residual[target] += value;
            }
        }
        let uniform = damping * dangling_push / n as f64;
        for value in &mut residual {
            *value += uniform;
        }
        norm = residual.iter().map(|v| v.abs()).sum();
        let mass = x.iter().sum::<f64>();
        if !mass.is_finite() || mass <= 0.0 || !norm.is_finite() {
            return exec_err!("nutmeg: nonfinite or nonpositive delta PageRank state");
        }
        iterations += 1;
        frontier_edges = frontier_edges.saturating_add(active_edges);
        let certify = 2.0 * norm / mass <= tolerance || iterations == limit;
        if certify {
            certificate_passes += 1;
            for i in 0..n {
                normalized[i] = x[i] / mass;
            }
            transition(
                context,
                &offsets,
                &targets,
                &normalized,
                damping,
                &mut certificate,
            )?;
            true_residual = certificate
                .iter()
                .zip(&normalized)
                .map(|(a, b)| (a - b).abs())
                .sum();
            converged = true_residual <= tolerance;
            // A failed cheap certificate rebases both maintained quantities;
            // otherwise accumulated rounding error could stall the frontier.
            x.copy_from_slice(&normalized);
            for i in 0..n {
                residual[i] = certificate[i] - normalized[i];
            }
            norm = true_residual;
        }
        rounds.push(
            serde_json::json!({"iteration": iterations, "active_vertices": active,
            "active_edges": active_edges, "residual_l1": norm, "certified": certify}),
        );
        if let Some(round) = rounds.last_mut() {
            round["reactivated_vertices"] = serde_json::json!(reactivated);
            round["activation_threshold"] = serde_json::json!(threshold);
            round["activation_mass"] = serde_json::json!(activation_mass);
            round["activation_residual_l1"] = serde_json::json!(activation_norm);
        }
    }
    save_diagnostics(
        query,
        serde_json::json!({"variant": "delta-frontier-v1",
        "activation_policy": "tolerance-capped-local-residual",
        "kernel_threads": threads, "requested_concurrency": context.concurrency(),
        "partition_sources": BLOCK, "iterations": iterations, "rounds": rounds,
        "residual_kind": "normalized fixed-point L1", "residual": true_residual,
        "converged": converged, "frontier_edges": frontier_edges,
        "certificate_passes": certificate_passes,
        "certificate_edges": graph.edge_count().saturating_mul(certificate_passes)}),
    )?;
    emit_result(
        graph,
        "pagerankDelta",
        Some((&normalized, iterations, converged, true_residual)),
        None,
        emit,
    )
}

fn transition(
    context: &ExecutionContext,
    offsets: &[usize],
    targets: &[usize],
    x: &[f64],
    damping: f64,
    output: &mut [f64],
) -> Result<()> {
    let n = x.len();
    if n == 0 {
        return Ok(());
    }
    let mut work = context.work_meter();
    let dangling = (0..n)
        .filter(|&i| offsets[i] == offsets[i + 1])
        .map(|i| x[i])
        .sum::<f64>();
    output.fill((1.0 - damping + damping * dangling) / n as f64);
    for source in 0..n {
        work.charge(1).map_err(err)?;
        let degree = offsets[source + 1] - offsets[source];
        if degree == 0 {
            continue;
        }
        let value = damping * x[source] / degree as f64;
        for &target in &targets[offsets[source]..offsets[source + 1]] {
            work.charge(1).map_err(err)?;
            output[target] += value;
        }
    }
    Ok(())
}
