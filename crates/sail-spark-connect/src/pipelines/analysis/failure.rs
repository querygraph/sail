//! Resolution diagnostics own errors and names, never working plans/providers.
use std::collections::{BTreeMap, BTreeSet};
use std::fmt;

use super::super::config::resolve::FailedFlow;
use super::super::graph::{GraphDefinition, Name};
use crate::error::{SparkError, SparkResult, StructuredSparkError};

#[derive(Debug, thiserror::Error)]
pub(in crate::pipelines) enum AnalysisError {
    #[error(transparent)]
    Other(#[from] SparkError),
    #[error(transparent)]
    Unresolved(#[from] ResolutionFailures),
}

impl From<AnalysisError> for SparkError {
    fn from(error: AnalysisError) -> Self {
        match error {
            AnalysisError::Other(error) => error,
            AnalysisError::Unresolved(failures) => failures.into_error(),
        }
    }
}

#[derive(Debug)]
pub(in crate::pipelines) struct ResolutionFailures {
    pub direct: BTreeMap<Name, SparkError>,
    pub downstream: BTreeMap<Name, SparkError>,
}
impl ResolutionFailures {
    pub fn new(graph: &GraphDefinition, failures: BTreeMap<Name, FailedFlow>) -> SparkResult<Self> {
        // Match GraphValidations: a failed flow is downstream if any failed
        // producer writes an input it requested. Classify by flow, not output:
        // successful producers of an unresolved multi-flow output are excluded.
        let dependencies: BTreeMap<_, BTreeSet<_>> = failures
            .iter()
            .map(|(name, failure)| {
                let inputs = graph
                    .flows
                    .iter()
                    .filter(|producer| {
                        failures.contains_key(&producer.name)
                            && failure.reads.contains(&producer.target)
                    })
                    .map(|producer| producer.name.clone())
                    .collect();
                (name.clone(), inputs)
            })
            .collect();
        super::super::topology::order(&dependencies)?;
        let mut direct = BTreeMap::new();
        let mut downstream = BTreeMap::new();
        for (name, failure) in failures {
            if dependencies[&name].is_empty() {
                direct.insert(name, failure.error);
            } else {
                downstream.insert(name, failure.error);
            }
        }
        Ok(Self { direct, downstream })
    }

    pub fn into_error(self) -> SparkError {
        Box::new(StructuredSparkError {
            class: "org.apache.spark.sql.pipelines.graph.UnresolvedPipelineException".into(),
            condition: String::new(),
            sql_state: String::new(),
            message: self.to_string(),
            parameters: Default::default(),
        })
        .into()
    }

    pub fn events(&self) -> impl Iterator<Item = String> + '_ {
        // PipelineExecution emits downstream failures before direct failures.
        self.downstream
            .iter()
            .map(|(name, error)| {
                format!(
                    "Failed to resolve flow due to upstream failure: '{}'.\nError: {}",
                    name.join("."),
                    diagnostic(error)
                )
            })
            .chain(self.direct.iter().map(|(name, error)| {
                format!(
                    "Failed to resolve flow: '{}'.\nError: {}",
                    name.join("."),
                    diagnostic(error)
                )
            }))
    }
}

fn diagnostic(error: &SparkError) -> String {
    match error {
        SparkError::AnalysisError(message) => message.clone(),
        SparkError::Structured(error) => error.to_string(),
        _ => error.to_string(),
    }
}
fn names(failures: &BTreeMap<Name, SparkError>) -> String {
    // Spark sorts the complete unquoted strings, not identifier-part vectors.
    let mut names: Vec<_> = failures.keys().map(|name| name.join(".")).collect();
    names.sort();
    names.join(", ")
}
impl fmt::Display for ResolutionFailures {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "\nFailed to resolve flows in the pipeline.\n\n\
A flow can fail to resolve because the flow itself contains errors or because it reads\n\
from an upstream flow which failed to resolve.\n\n\
Flows with errors: {}\n\
Flows that failed due to upstream errors: {}\n\n\
To view the exceptions that were raised while resolving these flows, look for flow\n\
failures that precede this log.",
            names(&self.direct),
            names(&self.downstream)
        )
    }
}
impl std::error::Error for ResolutionFailures {}
