//! First-pass configuration and post-success merge, as Spark 4.2.1 does them.
//!
//! Spark (`FlowAnalysis.readGraphInput`, `CoreDataflowNodeProcessor
//! .attemptResolveFlow`) analyzes a flow twice at most. The first pass runs
//! under the flow's declared configuration, overwritten by the accepted
//! configuration of each graph view input as it is bound, in binding order,
//! repeats included ([`first_pass_map`]). After that pass succeeds, the
//! declared configuration and the accepted configurations of the distinct
//! views actually used are merged: a key carrying more than one distinct
//! value across contributors is `DUPLICATE_FLOW_SQL_CONF`, otherwise the
//! union is the flow's accepted configuration, and a changed union triggers a
//! second analysis ([`merge`]). Tables contribute nothing; callers exclude
//! them. Keys and values are raw strings and are never normalized.

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::fmt;

/// Raw SQL-conf entries, keyed and valued by the strings as declared; the
/// same shape as `Flow.sql_conf`, `GraphDefinition.sql_conf` and the overrides
/// `SparkSession::plan_config_with_overrides` takes, so callers convert nothing.
pub type ConfMap = HashMap<String, String>;

/// One immediate graph-view input's accepted configuration, named by that
/// view's flow as the caller displays it (Spark: `identifier.unquotedString`).
/// Provenance is this immediate flow for every key it carries, inherited
/// keys included; the ancestor that first set a key is not reported. The
/// name is display and provenance only: distinct inputs are the caller's to
/// supply by canonical identity (Spark deduplicates `f.inputs` by
/// `TableIdentifier`, and display strings of distinct quoted multipart
/// identifiers can collide), so nothing here deduplicates by it.
#[derive(Debug, Clone, Copy)]
pub struct ViewContribution<'a> {
    pub flow_name: &'a str,
    pub accepted: &'a ConfMap,
}

/// The private configuration of the first semantic pass: the declared map,
/// then each contribution overwriting it in binding order. A view bound twice
/// contributes twice; a later binding of the same key wins.
pub fn first_pass_map<'a>(
    declared: &ConfMap,
    ordered: impl IntoIterator<Item = ViewContribution<'a>>,
) -> ConfMap {
    let mut map = declared.clone();
    for contribution in ordered {
        for (key, value) in contribution.accepted {
            map.insert(key.clone(), value.clone());
        }
    }
    map
}

/// Spark's `DUPLICATE_FLOW_SQL_CONF`, with its four parameter strings
/// verbatim: the key, the consuming flow's display name, and the two
/// contributing flows that set the key, sorted by value (SQLSTATE 42710).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DuplicateFlowSqlConf {
    pub key: String,
    pub dataset_name: String,
    pub flow_name1: String,
    pub flow_name2: String,
}

impl fmt::Display for DuplicateFlowSqlConf {
    /// Spark's message template for the condition.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "Found duplicate sql conf for dataset '{}': '{}' is defined by both '{}' and '{}'",
            self.dataset_name, self.key, self.flow_name1, self.flow_name2
        )
    }
}

impl std::error::Error for DuplicateFlowSqlConf {}

/// The accepted configuration of a flow after the merge, and whether it
/// differs from the declared one (in which case Spark analyzes again).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Accepted {
    pub map: ConfMap,
    pub changed: bool,
}

/// Merge the consuming flow's declared configuration (named `flow_name`,
/// displayed as `dataset_name`) with the accepted configurations of the graph
/// views it used. Every supplied contribution is kept: the caller provides
/// the distinct used inputs by canonical identity, and identical repeats are
/// harmless because equal values never conflict. Any key set to more than one
/// distinct value across the contributors is a conflict; otherwise the result
/// is the union.
///
/// For a conflicting key the two reported flows are the first and the last
/// of that key's contributors sorted by value; the sort is stable, so equal
/// values keep contribution order. When several keys conflict, Spark's
/// grouped-map `find` gives no ordering guarantee; this function reports the
/// smallest key in byte order, one permitted outcome, chosen only so the
/// result is deterministic here.
pub fn merge<'a>(
    dataset_name: &str,
    flow_name: &str,
    declared: &ConfMap,
    used: impl IntoIterator<Item = ViewContribution<'a>>,
) -> Result<Accepted, DuplicateFlowSqlConf> {
    // key -> (value, contributing flow), in contribution order; keys ordered
    // so that a multi-key conflict is reported deterministically.
    let mut by_key: BTreeMap<&str, Vec<(&str, &str)>> = BTreeMap::new();
    for (key, value) in declared {
        by_key
            .entry(key.as_str())
            .or_default()
            .push((value.as_str(), flow_name));
    }
    for contribution in used {
        for (key, value) in contribution.accepted {
            by_key
                .entry(key.as_str())
                .or_default()
                .push((value.as_str(), contribution.flow_name));
        }
    }
    let mut map = ConfMap::new();
    for (key, mut entries) in by_key {
        let distinct = entries
            .iter()
            .map(|(value, _)| *value)
            .collect::<BTreeSet<_>>();
        if distinct.len() > 1 {
            entries.sort_by(|a, b| a.0.cmp(b.0));
            let (Some(first), Some(last)) = (entries.first(), entries.last()) else {
                // Unreachable: more than one distinct value implies entries.
                continue;
            };
            return Err(DuplicateFlowSqlConf {
                key: key.to_string(),
                dataset_name: dataset_name.to_string(),
                flow_name1: first.1.to_string(),
                flow_name2: last.1.to_string(),
            });
        }
        if let Some((value, _)) = entries.first() {
            map.insert(key.to_string(), (*value).to_string());
        }
    }
    let changed = &map != declared;
    Ok(Accepted { map, changed })
}

#[cfg(test)]
#[path = "merge_tests.rs"]
mod tests;
