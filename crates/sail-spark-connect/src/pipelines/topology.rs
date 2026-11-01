use std::collections::{BTreeMap, BTreeSet};

use grust_topology::{DirectedGraph, Outcome, Scratch, Visit};

use super::graph::Name;
use crate::error::SparkResult;

struct Topology {
    offsets: Vec<usize>,
    targets: Vec<usize>,
}

impl DirectedGraph for Topology {
    fn node_count(&self) -> usize {
        self.offsets.len() - 1
    }

    fn outgoing(&self, node: usize) -> std::ops::Range<usize> {
        self.offsets[node]..self.offsets[node + 1]
    }

    fn target(&self, arc: usize) -> usize {
        self.targets[arc]
    }
}

/// A cycle is reported between datasets, so a flow stands for its target.
pub(super) fn order(
    dependencies: &BTreeMap<Name, BTreeSet<Name>>,
    targets: &BTreeMap<Name, Name>,
) -> SparkResult<Vec<Name>> {
    let names: Vec<_> = dependencies.keys().cloned().collect();
    let indices: BTreeMap<_, _> = names
        .iter()
        .enumerate()
        .map(|(i, name)| (name, i))
        .collect();
    // Producer-to-consumer arcs: a complete topological order is executable.
    let mut consumers = vec![vec![]; names.len()];
    for (target, inputs) in dependencies {
        for input in inputs {
            if let (Some(&source), Some(&target)) = (indices.get(input), indices.get(target)) {
                consumers[source].push(target);
            }
        }
    }
    let mut topology = Topology {
        offsets: vec![0],
        targets: vec![],
    };
    for edges in consumers {
        topology.targets.extend(edges);
        topology.offsets.push(topology.targets.len());
    }
    let mut visits = vec![Visit::Unseen; names.len()];
    let mut order = Vec::with_capacity(names.len());
    let mut stack = Vec::with_capacity(names.len());
    let outcome = grust_topology::topological_sort(
        &topology,
        Scratch {
            visits: &mut visits,
            order: &mut order,
            stack: &mut stack,
        },
        |_| Ok::<_, crate::error::SparkError>(()),
    )?;
    if let Outcome::Cycle { entry } = outcome {
        // The last dataset on the path reads the first, which closes the cycle.
        let upstream = stack.last().map_or(entry, |(node, _)| *node);
        let dataset = |node: usize| targets.get(&names[node]).unwrap_or(&names[node]).join(".");
        let (downstream, upstream) = (dataset(entry), dataset(upstream));
        return Err(crate::error::SparkError::AnalysisError(format!(
            "The downstream table '{downstream}' is referenced when creating the upstream table \
             or view '{upstream}'. Circular dependencies are not supported in a pipeline. Please \
             remove the dependency between '{upstream}' and '{downstream}'."
        )));
    }
    Ok(order.into_iter().map(|i| names[i].clone()).collect())
}
