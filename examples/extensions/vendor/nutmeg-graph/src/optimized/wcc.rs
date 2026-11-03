//! Seeded one-hop random contraction, history back-propagation, numeric labels.
use super::*;
use rayon::prelude::*;

fn splitmix(state: &mut u64) -> u64 {
    *state = state.wrapping_add(0x9e3779b97f4a7c15);
    let mut z = *state;
    z = (z ^ (z >> 30)).wrapping_mul(0xbf58476d1ce4e5b9);
    z = (z ^ (z >> 27)).wrapping_mul(0x94d049bb133111eb);
    z ^ (z >> 31)
}

// Same polynomial and full-width bit semantics as Sail's gf_axpb scalar.
fn multiply(mut a: u64, mut x: u64) -> u64 {
    let mut result = 0;
    for _ in 0..64 {
        result ^= a & 0_u64.wrapping_sub(x & 1);
        let high = a >> 63;
        a = (a << 1) ^ (0x1b & 0_u64.wrapping_sub(high));
        x >>= 1;
    }
    result
}

pub(super) fn run(
    algorithm: &str,
    graph: &GraphProjection,
    args: &ValidatedArguments,
    query: &Query,
    emit: &mut dyn FnMut(RecordBatch) -> Result<bool>,
) -> Result<bool> {
    let fused = algorithm == "wccRandomizedFused";
    let variant = if fused {
        "randomized-contraction-fused-v1"
    } else {
        "randomized-contraction-v1"
    };
    let initial_edge_policy = if fused {
        "raw-non-loop"
    } else {
        "canonical-undirected-deduplicated"
    };
    let context = graph.execution();
    let n = graph.node_count();
    let limit = integer(args, "maxIterations")? as usize;
    let seed = integer(args, "seed")? as u64;
    let _scratch = admitted(context, n, graph.edge_count(), 128, 16)?;
    let (workers, threads) = pool(context, n)?;
    let mut numeric = Vec::new();
    numeric.try_reserve_exact(n).map_err(err)?;
    let mut work = context.work_meter();
    for id in graph.node_ids() {
        work.charge(1).map_err(err)?;
        numeric.push(
            id.as_str()
                .parse::<i64>()
                .map_err(|_| err(format!("{algorithm} requires numeric BIGINT IDs")))?,
        );
    }
    let mut unique = numeric.clone();
    unique.sort_unstable();
    if unique.windows(2).any(|pair| pair[0] == pair[1]) {
        return exec_err!("nutmeg: {algorithm} IDs must remain unique when parsed as BIGINT");
    }
    drop(unique);
    let mut edges = Vec::new();
    edges.try_reserve_exact(graph.edge_count()).map_err(err)?;
    for edge in graph.edges() {
        work.charge(1).map_err(err)?;
        if edge.source != edge.target {
            edges.push(if fused {
                (edge.source, edge.target)
            } else {
                (edge.source.min(edge.target), edge.source.max(edge.target))
            });
        }
    }
    // Choices already inspect both endpoints. Repeated or reversed input edges
    // cannot change a minimum, so the fused variant defers canonicalization
    // until after its first contraction. Subsequent edge sets are identical.
    if !fused {
        edges.sort_unstable();
        edges.dedup();
    }
    let mut choice = allocated(n, 0usize)?;
    let mut priority = allocated(n, 0i64)?;
    let mut present = allocated(n, false)?;
    let mut active = Vec::new();
    active.try_reserve_exact(n).map_err(err)?;
    let _metadata = context.reserve(limit.saturating_mul(2048)).map_err(err)?;
    let mut history = Vec::new();
    history.try_reserve_exact(limit).map_err(err)?;
    let mut rounds = Vec::new();
    rounds.try_reserve_exact(limit).map_err(err)?;
    let mut state = seed;
    while !edges.is_empty() {
        context.checkpoint().map_err(err)?;
        if history.len() == limit {
            save_diagnostics(
                query,
                serde_json::json!({"variant": variant, "initial_edge_policy": initial_edge_policy,
                "kernel_threads": threads, "requested_concurrency": context.concurrency(),
                "seed_bits": seed.to_string(), "rounds": rounds, "converged": false}),
            )?;
            return exec_err!("nutmeg: {algorithm} did not converge within maxIterations={limit}");
        }
        present.fill(false);
        for &(a, b) in &edges {
            work.charge(1).map_err(err)?;
            present[a] = true;
            present[b] = true;
        }
        active.clear();
        active.extend((0..n).filter(|&id| present[id]));
        let mut a = splitmix(&mut state);
        while a == 0 {
            a = splitmix(&mut state);
        }
        let b = splitmix(&mut state);
        let hash = |(index, value): (usize, &mut i64)| -> Result<()> {
            if present[index] {
                context.charge_work(64).map_err(err)?;
                *value = (multiply(a, numeric[index] as u64) ^ b) as i64;
            }
            Ok(())
        };
        match &workers {
            Some(pool) => {
                pool.install(|| priority.par_iter_mut().enumerate().try_for_each(hash))?
            }
            None => priority.iter_mut().enumerate().try_for_each(hash)?,
        }
        for &id in &active {
            choice[id] = id;
        }
        // Both endpoints inspect their CLOSED neighborhood, and choose once.
        // Do not shortcut a choice's parent inside this round: the next round
        // handles those chains, matching the relational contraction algorithm.
        for &(a, b) in &edges {
            work.charge(1).map_err(err)?;
            if (priority[b], numeric[b]) < (priority[choice[a]], numeric[choice[a]]) {
                choice[a] = b;
            }
            if (priority[a], numeric[a]) < (priority[choice[b]], numeric[choice[b]]) {
                choice[b] = a;
            }
        }
        let token = admitted(context, active.len(), 0, 16, 0)?;
        let mut mapping = Vec::new();
        mapping.try_reserve_exact(active.len()).map_err(err)?;
        mapping.extend(active.iter().map(|&id| (id, choice[id])));
        let before_edges = edges.len();
        let relabel = |chunk: &mut [(usize, usize)]| -> Result<()> {
            let mut meter = context.work_meter();
            for edge in chunk {
                meter.charge(1).map_err(err)?;
                let (a, b) = (choice[edge.0], choice[edge.1]);
                *edge = (a.min(b), a.max(b));
            }
            Ok(())
        };
        match &workers {
            Some(pool) => pool.install(|| edges.par_chunks_mut(BLOCK).try_for_each(relabel))?,
            None => edges.chunks_mut(BLOCK).try_for_each(relabel)?,
        }
        edges.retain(|(a, b)| a != b);
        context
            .charge_work(
                edges
                    .len()
                    .saturating_mul(edges.len().max(1).ilog2() as usize + 1),
            )
            .map_err(err)?;
        edges.sort_unstable();
        edges.dedup();
        context.checkpoint().map_err(err)?;
        // Endpoint count after contraction excludes newly terminal components.
        present.fill(false);
        for &(a, b) in &edges {
            present[a] = true;
            present[b] = true;
        }
        rounds.push(serde_json::json!({"iteration": history.len() + 1,
            "vertices_before": active.len(), "edges_before": before_edges,
            "vertices_after": present.iter().filter(|&&yes| yes).count(), "edges_after": edges.len(),
            "a_bits": a.to_string(), "b_bits": b.to_string()}));
        history.push((mapping, token));
    }
    let iterations = history.len();
    let mut labels: Vec<usize> = (0..n).collect();
    let mut next = Vec::new();
    next.try_reserve_exact(n).map_err(err)?;
    while let Some((mapping, _token)) = history.pop() {
        next.clear();
        // Read the whole next-level mapping before modifying any same-level ID.
        for &(_, representative) in &mapping {
            work.charge(1).map_err(err)?;
            next.push(labels[representative]);
        }
        for ((id, _), label) in mapping.iter().zip(&next) {
            labels[*id] = *label;
        }
    }
    let mut minima = allocated(n, i64::MAX)?;
    for i in 0..n {
        work.charge(1).map_err(err)?;
        minima[labels[i]] = minima[labels[i]].min(numeric[i]);
    }
    for i in 0..n {
        numeric[i] = minima[labels[i]];
    }
    save_diagnostics(
        query,
        serde_json::json!({"variant": variant, "initial_edge_policy": initial_edge_policy,
        "kernel_threads": threads, "requested_concurrency": context.concurrency(),
        "seed_bits": seed.to_string(), "iterations": iterations, "rounds": rounds,
        "converged": true, "labels": "minimum numeric vertex ID",
        "serial_phases": ["closed-neighborhood choices", "deduplication", "back-propagation"]}),
    )?;
    emit_result(graph, algorithm, None, Some(&numeric), emit)
}

#[cfg(test)]
#[test]
fn coefficient_and_field_vectors() {
    let mut state = 0;
    assert_eq!(splitmix(&mut state), 0xe220a8397b1dcdaf);
    assert_eq!(splitmix(&mut state), 0x6e789e6aa1b965f4);
    assert_eq!(multiply(1 << 63, 2), 0x1b);
    assert_eq!(multiply(u64::MAX, 2), 0xffffffffffffffe5);
}
