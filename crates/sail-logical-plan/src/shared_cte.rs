use std::fmt::Formatter;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use datafusion_common::{DFSchemaRef, Result, internal_err};
use datafusion_expr::{Expr, LogicalPlan, UserDefinedLogicalNodeCore};
use educe::Educe;

static NEXT_ID: AtomicU64 = AtomicU64::new(1);

/// A fresh id for one CTE definition.
pub fn next_shared_cte_id() -> u64 {
    NEXT_ID.fetch_add(1, Ordering::Relaxed)
}

/// A reference to a CTE that its query references more than once.
///
/// Sail resolves a CTE into a plan and would otherwise inline it at every
/// reference, computing it once per reference and making the plan as large as
/// all the copies together. A shared CTE is instead defined once, by the
/// [`WithSharedCtesNode`] of the scope that defines it, and every reference is
/// this leaf, which reads the definition's result.
#[derive(Clone, Debug, Eq, PartialEq, Hash, Educe)]
#[educe(PartialOrd)]
pub struct SharedCteRefNode {
    id: u64,
    name: String,
    #[educe(PartialOrd(ignore))]
    schema: DFSchemaRef,
}

impl SharedCteRefNode {
    pub fn new(id: u64, name: String, schema: DFSchemaRef) -> Self {
        Self { id, name, schema }
    }

    pub fn id(&self) -> u64 {
        self.id
    }

    pub fn cte_name(&self) -> &str {
        &self.name
    }
}

impl UserDefinedLogicalNodeCore for SharedCteRefNode {
    fn name(&self) -> &str {
        "SharedCteRef"
    }

    fn inputs(&self) -> Vec<&LogicalPlan> {
        vec![]
    }

    fn schema(&self) -> &DFSchemaRef {
        &self.schema
    }

    fn expressions(&self) -> Vec<Expr> {
        vec![]
    }

    fn fmt_for_explain(&self, f: &mut Formatter) -> std::fmt::Result {
        write!(f, "SharedCteRef: name={}, id={}", self.name, self.id)
    }

    fn with_exprs_and_inputs(&self, exprs: Vec<Expr>, inputs: Vec<LogicalPlan>) -> Result<Self> {
        if !exprs.is_empty() || !inputs.is_empty() {
            return internal_err!("SharedCteRef takes no expressions or inputs");
        }
        Ok(self.clone())
    }
}

/// The definitions of the shared CTEs of one `WITH` scope, and the plan that
/// reads them. The inputs are the definitions, in definition order, followed by
/// the plan; the output is the plan's.
#[derive(Clone, Debug, Eq, PartialEq, PartialOrd, Hash)]
pub struct WithSharedCtesNode {
    ids: Vec<u64>,
    names: Vec<String>,
    definitions: Vec<Arc<LogicalPlan>>,
    input: Arc<LogicalPlan>,
}

impl WithSharedCtesNode {
    pub fn new(
        ids: Vec<u64>,
        names: Vec<String>,
        definitions: Vec<Arc<LogicalPlan>>,
        input: Arc<LogicalPlan>,
    ) -> Self {
        Self {
            ids,
            names,
            definitions,
            input,
        }
    }

    pub fn ids(&self) -> &[u64] {
        &self.ids
    }

    pub fn names(&self) -> &[String] {
        &self.names
    }
}

impl UserDefinedLogicalNodeCore for WithSharedCtesNode {
    fn name(&self) -> &str {
        "WithSharedCtes"
    }

    fn inputs(&self) -> Vec<&LogicalPlan> {
        self.definitions
            .iter()
            .map(|d| d.as_ref())
            .chain(std::iter::once(self.input.as_ref()))
            .collect()
    }

    fn schema(&self) -> &DFSchemaRef {
        self.input.schema()
    }

    fn expressions(&self) -> Vec<Expr> {
        vec![]
    }

    fn fmt_for_explain(&self, f: &mut Formatter) -> std::fmt::Result {
        write!(f, "WithSharedCtes: names={:?}", self.names)
    }

    fn with_exprs_and_inputs(
        &self,
        exprs: Vec<Expr>,
        mut inputs: Vec<LogicalPlan>,
    ) -> Result<Self> {
        if !exprs.is_empty() || inputs.len() != self.ids.len() + 1 {
            return internal_err!(
                "WithSharedCtes takes no expressions and one input per CTE plus one"
            );
        }
        let input = Arc::new(inputs.remove(self.ids.len()));
        Ok(Self {
            ids: self.ids.clone(),
            names: self.names.clone(),
            definitions: inputs.into_iter().map(Arc::new).collect(),
            input,
        })
    }
}
