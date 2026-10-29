use std::collections::{BTreeMap, BTreeSet};

use grust_topology::{DirectedGraph, Outcome, Scratch, Visit};

use super::graph::{Name, analysis, quoted};
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

pub(super) fn order(dependencies: &BTreeMap<Name, BTreeSet<Name>>) -> SparkResult<Vec<Name>> {
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
        let witness = stack
            .iter()
            .skip_while(|(node, _)| *node != entry)
            .map(|(node, _)| quoted(&names[*node]))
            .chain([quoted(&names[entry])])
            .collect::<Vec<_>>()
            .join(" -> ");
        return Err(analysis("PIPELINE_CIRCULAR_DEPENDENCY", witness));
    }
    Ok(order.into_iter().map(|i| names[i].clone()).collect())
}
