//! Pure lowering of one flow over one batch intent
//! (BOUNDED-PLAN-ADMISSION-DESIGN §5 as narrowed in §10 after CF-0136).
//!
//! `lower` binds the flow's reads before resolution, resolves the flow with
//! the authoritative session context and the caller's plan configuration
//! through a [`BoundedScope`] composed with the graph scope, admits the
//! resolved plan, and returns the bounded logical plan with its admission
//! evidence and a snapshot of what it was lowered under. It has no target
//! effect: no write, no checkpoint, no acknowledgement, no frontier
//! capture, and it never creates or seals a resource scope.

use std::collections::BTreeMap;
use std::fmt;
use std::sync::{Arc, Mutex};

use datafusion::common::TableReference;
use datafusion::datasource::{MemTable, TableProvider, provider_as_source};
use datafusion::logical_expr::{LogicalPlan, LogicalPlanBuilder};
use datafusion::prelude::SessionContext;
use sail_common::spec;
use sail_common_datafusion::rename::logical_plan::rename_logical_plan;
use sail_common_datafusion::task_resources::TaskResourceScope;
use sail_plan::config::PlanConfig;
use sail_plan::error::{PlanError, PlanResult};
use sail_plan::resolver::PlanResolver;
use sail_plan::resolver::scope::RelationScope;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use super::admission::{AdmissionEvidence, Bindings, admit};
use super::binding::{
    BindingError, LocalBinding, ReadSet, SourceBinding, StaticBinding, validate_reads,
};
use super::source::Range;
use crate::error::SparkError;
use crate::pipelines::analysis::GraphScope;
use crate::pipelines::graph::{Flow, GraphDefinition, Name};

/// One bound source as it was lowered: its name, the SHA-256 of the
/// canonical JSON of its captured descriptor, and the ranges of the intent.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct BindingSnapshot {
    pub name: Name,
    pub descriptor_digest: String,
    pub ranges: Vec<Range>,
}

/// A declared identity the flow read, as the caller supplied it: a typed
/// entry rather than a map keyed by a multi-part name, so the snapshot
/// serialises losslessly.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct IdentitySnapshot {
    pub name: Name,
    pub identity: String,
}

/// What the plan was lowered under, captured after admission from the
/// caller's authoritative plan configuration and the flow's overrides. It
/// records these fields; it does not claim they exhaust every replay
/// dependency, and it changes nothing in the unit-5 seal.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct LoweringSnapshot {
    pub case_sensitive: bool,
    pub ansi_mode: bool,
    pub session_timezone: String,
    pub sql_conf: BTreeMap<String, String>,
    pub bindings: Vec<BindingSnapshot>,
    /// The declared identities of the static bindings the flow read, sorted
    /// by name: what the caller asserted, carried so later integration can
    /// bind what was supplied; not a claim that the backend is pinned.
    pub statics: Vec<IdentitySnapshot>,
    /// The declared identities of the local outputs the flow read, sorted
    /// by name.
    pub locals: Vec<IdentitySnapshot>,
}

pub struct Lowered {
    pub plan: LogicalPlan,
    pub evidence: AdmissionEvidence,
    pub snapshot: LoweringSnapshot,
}

#[derive(Debug)]
pub enum LowerError {
    /// Refused before resolution.
    Binding(BindingError),
    /// The resolver failed on the bound flow.
    Resolution(SparkError),
    /// Resolved but outside the admitted subset; the evidence lists why.
    NotAdmitted(Box<AdmissionEvidence>),
}

impl fmt::Display for LowerError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Binding(error) => {
                write!(f, "bounded lowering refused before resolution: {error}")
            }
            Self::Resolution(error) => write!(f, "bounded lowering failed to resolve: {error}"),
            Self::NotAdmitted(evidence) => {
                let rejected: Vec<String> = evidence
                    .operators
                    .iter()
                    .filter(|op| op.decision == super::admission::Decision::Reject)
                    .map(|op| format!("{}: {}", op.operator, op.reason))
                    .collect();
                write!(f, "bounded plan not admitted: {}", rejected.join("; "))
            }
        }
    }
}

impl std::error::Error for LowerError {}

/// Lowers the flow of `inputs` over its bindings.
///
/// `config` is the caller's plan configuration with the flow's overrides
/// already applied, captured after admission and any authoritative session
/// mutation; `_scope` is the preparation's resource scope, borrowed and
/// never created or sealed here, under which the adapter unit will spawn
/// asynchronous provider calls (the in-memory bindings of this component
/// need none).
/// What one lowering is built from: the graph and flow, the installed
/// local outputs the graph scope resolves (the materialised reads described
/// by `locals`; a schema-only analysis placeholder is rejected by
/// admission), and the caller-supplied bindings.
pub struct Inputs<'a> {
    pub graph: &'a GraphDefinition,
    pub flow: &'a Flow,
    pub relations: &'a BTreeMap<Name, LogicalPlan>,
    pub sources: &'a [SourceBinding],
    pub statics: &'a [StaticBinding],
    pub locals: &'a [LocalBinding],
}

pub async fn lower(
    ctx: &SessionContext,
    config: Arc<PlanConfig>,
    _scope: &TaskResourceScope,
    inputs: &Inputs<'_>,
) -> Result<Lowered, LowerError> {
    let Inputs {
        graph,
        flow,
        relations,
        sources,
        statics,
        locals,
    } = *inputs;
    let reads: ReadSet = validate_reads(ctx, graph, flow, &flow.query, sources, statics, locals)
        .map_err(LowerError::Binding)?;
    let scope = BoundedScope {
        graph: GraphScope::new(graph, relations, flow),
        validated: &reads,
        sources,
        statics,
        minted: Mutex::default(),
    };
    let resolved = PlanResolver::new(ctx, config.clone())
        .with_relation_scope(&scope)
        .resolve_named_plan(spec::Plan::Query(flow.query.clone()))
        .await
        .map_err(|error| LowerError::Resolution(error.into()))?;
    let plan = match resolved.fields {
        Some(fields) => rename_logical_plan(resolved.plan, &fields)
            .map_err(|error| LowerError::Resolution(error.into()))?,
        None => resolved.plan,
    };
    let minted = scope
        .minted
        .into_inner()
        .map_err(|e| LowerError::Resolution(SparkError::internal(e.to_string())))?;
    let evidence = admit(
        &plan,
        &reads,
        &Bindings {
            sources,
            statics,
            locals,
            empty_scans: &minted,
        },
    );
    if !evidence.admitted {
        return Err(LowerError::NotAdmitted(Box::new(evidence)));
    }
    let bindings = sources
        .iter()
        .filter(|binding| reads.streaming.contains(&binding.name))
        .map(|binding| {
            let json = serde_json::to_vec(&binding.descriptor)
                .map_err(|error| LowerError::Resolution(SparkError::internal(error.to_string())))?;
            Ok(BindingSnapshot {
                name: binding.name.clone(),
                descriptor_digest: Sha256::digest(&json)
                    .iter()
                    .map(|byte| format!("{byte:02x}"))
                    .collect(),
                ranges: binding.intent.ranges.clone(),
            })
        })
        .collect::<Result<Vec<_>, LowerError>>()?;
    let snapshot = LoweringSnapshot {
        case_sensitive: config.case_sensitive,
        ansi_mode: config.ansi_mode,
        session_timezone: config.session_timezone.to_string(),
        sql_conf: flow
            .sql_conf
            .iter()
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect(),
        bindings,
        statics: identities(
            statics
                .iter()
                .filter(|binding| reads.statics.contains(&binding.name))
                .map(|binding| (&binding.name, &binding.identity)),
        ),
        locals: identities(
            locals
                .iter()
                .filter(|binding| reads.local.contains(&binding.name))
                .map(|binding| (&binding.name, &binding.identity)),
        ),
    };
    Ok(Lowered {
        plan,
        evidence,
        snapshot,
    })
}

/// Declared identities as a sequence sorted by name.
fn identities<'a>(entries: impl Iterator<Item = (&'a Name, &'a String)>) -> Vec<IdentitySnapshot> {
    let mut snapshots: Vec<IdentitySnapshot> = entries
        .map(|(name, identity)| IdentitySnapshot {
            name: name.clone(),
            identity: identity.clone(),
        })
        .collect();
    snapshots.sort_by(|a, b| a.name.cmp(&b.name));
    snapshots
}

/// The relation scope of one lowering, dispatching on the validated read
/// set rather than on output membership: a name the flow reads as a stream
/// resolves to the union of its range scans (an empty scan with the source
/// schema for an empty intent) even when that name is also a graph output;
/// a local batch read resolves through the graph scope; a static read to
/// its pinned provider. Anything else falls to the resolver's ordinary
/// path, which the binding step has already ruled out.
struct BoundedScope<'a> {
    graph: GraphScope<'a>,
    validated: &'a ReadSet,
    sources: &'a [SourceBinding],
    statics: &'a [StaticBinding],
    /// Empty scans minted for sources with an empty recorded intent, handed
    /// to admission so their identity can be verified like any scan.
    minted: Mutex<Vec<(Name, Arc<dyn TableProvider>)>>,
}

impl BoundedScope<'_> {
    fn qualified(&self, name: &[String]) -> PlanResult<Name> {
        let raw: Name = name.iter().map(|x| x.to_ascii_lowercase()).collect();
        if self.graph.graph.outputs.iter().any(|x| x.name == raw) {
            Ok(raw)
        } else {
            self.graph.qualify(&raw)
        }
    }

    fn scan_source(&self, binding: &SourceBinding) -> PlanResult<LogicalPlan> {
        let reference = reference(&binding.name)?;
        let mut plans = binding.scans.iter().map(|provider| {
            LogicalPlanBuilder::scan(
                reference.clone(),
                provider_as_source(provider.clone()),
                None,
            )
        });
        let Some(first) = plans.next() else {
            let empty: Arc<dyn TableProvider> =
                Arc::new(MemTable::try_new(binding.schema.clone(), vec![vec![]])?);
            self.minted
                .lock()
                .map_err(|e| PlanError::internal(e.to_string()))?
                .push((binding.name.clone(), empty.clone()));
            return Ok(
                LogicalPlanBuilder::scan(reference, provider_as_source(empty), None)?.build()?,
            );
        };
        let mut union = first?;
        for plan in plans {
            union = union.union(plan?.build()?)?;
        }
        Ok(union.build()?)
    }
}

fn reference(name: &Name) -> PlanResult<TableReference> {
    match name.as_slice() {
        [catalog, database, table] => Ok(TableReference::full(
            catalog.as_str(),
            database.as_str(),
            table.as_str(),
        )),
        [database, table] => Ok(TableReference::partial(database.as_str(), table.as_str())),
        [table] => Ok(TableReference::bare(table.as_str())),
        _ => Err(PlanError::internal(format!(
            "bound name with {} parts",
            name.len()
        ))),
    }
}

impl RelationScope for BoundedScope<'_> {
    fn resolve(&self, name: &[String]) -> PlanResult<Option<LogicalPlan>> {
        let qualified = self.qualified(name)?;
        if self.validated.streaming.contains(&qualified) {
            if let Some(binding) = self.sources.iter().find(|b| b.name == qualified) {
                return self.scan_source(binding).map(Some);
            }
            return Err(PlanError::internal(format!(
                "validated streaming read {} has no source binding",
                qualified.join(".")
            )));
        }
        if self.validated.local.contains(&qualified) {
            return self.graph.resolve(name);
        }
        if self.validated.statics.contains(&qualified) {
            if let Some(binding) = self.statics.iter().find(|b| b.name == qualified) {
                let plan = LogicalPlanBuilder::scan(
                    reference(&binding.name)?,
                    provider_as_source(binding.provider.clone()),
                    None,
                )?
                .build()?;
                return Ok(Some(plan));
            }
            return Err(PlanError::internal(format!(
                "validated static read {} has no static binding",
                qualified.join(".")
            )));
        }
        Ok(None)
    }

    fn qualify(&self, name: &[String]) -> PlanResult<Name> {
        self.graph.qualify(name)
    }

    fn default_namespace(&self) -> Option<Vec<String>> {
        self.graph.default_namespace()
    }
}

#[cfg(test)]
#[path = "lower_tests.rs"]
mod tests;
