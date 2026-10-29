use std::collections::BTreeSet;

use super::analysis::AnalyzedGraph;
use super::graph::{GraphDefinition, Name, parse_name};
use crate::error::{SparkError, SparkResult};
use crate::spark::connect::OutputType;
use crate::spark::connect::pipeline_command::StartRun;

pub(super) struct Selection {
    pub refresh: BTreeSet<Name>,
    pub reset: BTreeSet<Name>,
    pub required: BTreeSet<Name>,
}

impl Selection {
    pub fn new(
        graph: &GraphDefinition,
        analyzed: &AnalyzedGraph,
        run: &StartRun,
    ) -> SparkResult<Self> {
        let parse = |names: &[String]| -> SparkResult<BTreeSet<Name>> {
            names
                .iter()
                .map(|name| graph.qualify(&parse_name(name)?))
                .collect()
        };
        let mut refresh = parse(&run.refresh_selection)?;
        let mut reset = parse(&run.full_refresh_selection)?;
        if run.full_refresh_all.unwrap_or(false) {
            if !refresh.is_empty() {
                return Err(SparkError::invalid(
                    "Cannot specify a subset to refresh when full refresh all is set to true.",
                ));
            }
            if !reset.is_empty() {
                return Err(SparkError::invalid(
                    "Cannot specify a subset to full refresh when full refresh all is set to true.",
                ));
            }
            reset.extend(
                graph
                    .outputs
                    .iter()
                    .filter(|x| x.kind != OutputType::TemporaryView)
                    .map(|x| x.name.clone()),
            );
        } else if refresh.is_empty() && reset.is_empty() {
            refresh.extend(
                graph
                    .outputs
                    .iter()
                    .filter(|x| x.kind != OutputType::TemporaryView)
                    .map(|x| x.name.clone()),
            );
        }
        if let Some(overlap) = refresh.intersection(&reset).next() {
            return Err(SparkError::invalid(format!(
                "Datasets specified for refresh and full refresh cannot overlap: {}",
                overlap.join(".")
            )));
        }
        // A selection names only datasets to run, not their upstream or
        // downstream tables. Temporary views are expanded to discover the
        // existing materialized outputs that selected flows need to scan.
        let mut required: BTreeSet<_> = refresh.union(&reset).cloned().collect();
        let mut pending: Vec<_> = required.iter().cloned().collect();
        while let Some(name) = pending.pop() {
            for input in analyzed.dependencies.get(&name).into_iter().flatten() {
                if required.insert(input.clone())
                    && graph
                        .outputs
                        .iter()
                        .any(|x| x.name == *input && x.kind == OutputType::TemporaryView)
                {
                    pending.push(input.clone());
                }
            }
        }
        Ok(Self {
            refresh,
            reset,
            required,
        })
    }

    pub fn runs(&self, name: &Name) -> bool {
        self.refresh.contains(name) || self.reset.contains(name)
    }
}
