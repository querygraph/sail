use std::collections::HashSet;
use std::sync::Arc;

use datafusion_expr::{LogicalPlan, SubqueryAlias};
use sail_common::spec;

use crate::error::PlanResult;
use crate::resolver::PlanResolver;
use crate::resolver::state::PlanResolverState;

/// Whether a named relation is a CTE definition or a parameter view.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(in crate::resolver) enum CteKind {
    /// A CTE defined by a `WITH` clause.
    Definition,
    /// A DataFrame argument of `spark.sql`, referenced by its view name.
    ParameterView,
}

/// A CTE definition or a parameter view, with the attribute identities of its output.
#[derive(Debug)]
pub(in crate::resolver) struct CteInfo {
    pub(super) plan: Arc<LogicalPlan>,
    /// Identifies the definition, so that references to it can share one result.
    pub(super) id: u64,
    /// Whether this is a CTE definition or a parameter view.
    kind: CteKind,
    /// The attribute identity of each output field.
    origins: Vec<usize>,
    /// The DataFrame plan IDs bound to the output attributes when the CTE is defined.
    bindings: datafusion_common::HashMap<usize, datafusion_common::HashSet<i64>>,
}

impl CteInfo {
    pub(super) fn is_definition(&self) -> bool {
        self.kind == CteKind::Definition
    }

    pub(in crate::resolver) fn try_new(
        plan: LogicalPlan,
        kind: CteKind,
        state: &PlanResolverState,
    ) -> PlanResult<Self> {
        let mut origins = Vec::with_capacity(plan.schema().fields().len());
        let mut bindings = datafusion_common::HashMap::new();
        for field in plan.schema().fields() {
            let info = state.get_field_info(field.name())?;
            origins.push(info.origin());
            let plan_ids = info.plan_ids().collect::<datafusion_common::HashSet<_>>();
            if !plan_ids.is_empty() {
                bindings.insert(info.origin(), plan_ids);
            }
        }
        Ok(Self {
            plan: Arc::new(plan),
            id: sail_logical_plan::shared_cte::next_shared_cte_id(),
            kind,
            origins,
            bindings,
        })
    }

    /// Spark renews repeated CTERelationRef and parameter-view attributes
    /// separately. Identity projections preserve the original attribute IDs.
    pub(super) fn renew_reference(
        &self,
        state: &mut PlanResolverState,
    ) -> PlanResult<Option<Vec<String>>> {
        let origins = match self.kind {
            CteKind::Definition => state.cte_reference_origins_mut(),
            CteKind::ParameterView => state.parameter_view_origins_mut(),
        };
        if !self.origins.iter().any(|origin| origins.contains(origin)) {
            // SQL outputs may acquire a DataFrame plan ID only after this query
            // finishes. Their repeated CTE references already need fresh identities.
            origins.extend(self.origins.iter().copied());
            return Ok(None);
        }
        let mut renewed = datafusion_common::HashMap::with_capacity(self.origins.len());
        let mut names = Vec::with_capacity(self.origins.len());
        for (field, &original) in self.plan.schema().fields().iter().zip(&self.origins) {
            let info = state.get_field_info(field.name())?;
            let (name, hidden) = (info.name().to_string(), info.is_hidden());
            // Spark's CTERelationRef.newInstance preserves duplicate attributes
            // within one output while giving the reference fresh identities.
            let origin = *renewed
                .entry(original)
                .or_insert_with(|| state.next_origin());
            let plan_ids = match self.kind {
                CteKind::Definition => datafusion_common::HashSet::new(),
                CteKind::ParameterView => self.bindings.get(&original).cloned().unwrap_or_default(),
            };
            names.push(state.register_field_with_origin(name, hidden, origin, plan_ids));
            if self.kind == CteKind::ParameterView {
                state.parameter_view_origins_mut().insert(origin);
            }
        }
        Ok(Some(names))
    }
}

impl PlanResolver<'_> {
    pub(super) async fn resolve_query_with_ctes(
        &self,
        input: spec::QueryPlan,
        recursive: bool,
        ctes: Vec<(spec::Identifier, spec::QueryPlan)>,
        state: &mut PlanResolverState,
    ) -> PlanResult<LogicalPlan> {
        // Deduplicate CTEs - keep the last occurrence of each name (shadowing behavior)
        // This matches Spark's behavior where later CTEs can shadow earlier ones
        let mut seen_names: HashSet<spec::Identifier> = HashSet::new();
        let ctes: Vec<_> = ctes
            .into_iter()
            .rev() // Reverse to keep last occurrence
            .filter(|(name, _)| seen_names.insert(name.clone()))
            .collect::<Vec<_>>()
            .into_iter()
            .rev() // Reverse back to original order
            .collect();
        let mut scope = state.enter_cte_scope();
        let state = scope.state();
        let mut defined = Vec::new();
        for (name, query) in ctes.into_iter() {
            let reference = self.resolve_table_reference(&spec::ObjectName::bare(name.clone()))?;
            defined.push(reference.clone());
            let plan = if recursive {
                self.resolve_recursive_query_plan(&reference, query, state)
                    .await?
            } else {
                self.resolve_query_plan(query, state).await?
            };
            let plan = LogicalPlan::SubqueryAlias(SubqueryAlias::try_new(
                Arc::new(plan),
                reference.clone(),
            )?);
            // Coerce the definition's types now: a shared reference is a leaf
            // that keeps the definition's schema, and the analyzer's type
            // coercion would otherwise change the definition (say, a CASE of
            // INT and BIGINT becoming BIGINT) but not the leaf.
            let session = self.ctx.state();
            let plan =
                session
                    .analyzer()
                    .execute_and_check(plan, session.config_options(), |_, _| {})?;
            state.insert_cte(reference, plan, CteKind::Definition)?;
        }
        let plan = self.resolve_query_plan(input, state).await?;
        // A CTE referenced more than once is computed once; one referenced once
        // is inlined, so the optimizer can push work into it.
        let definitions = defined
            .iter()
            .filter_map(|reference| {
                state.get_cte(reference).and_then(|cte| {
                    cte.is_definition().then(|| {
                        (
                            cte.id,
                            reference.table().to_string(),
                            cte.plan.as_ref().clone(),
                        )
                    })
                })
            })
            .collect::<Vec<_>>();
        let plan = share_or_inline(plan, definitions)?;
        // Spark's `WithCTE` also has the CTE definitions as children, so
        // missing-reference recovery resolves only against the query output.
        Self::restore_cte_output_bindings(&plan, state)?;
        state.register_missing_input_boundary(&plan);
        Ok(plan)
    }

    /// A Union is a plan-ID lookup leaf, but WithCTE also exposes its definitions.
    /// Restore only bindings whose original attributes survive in the output.
    fn restore_cte_output_bindings(
        plan: &LogicalPlan,
        state: &mut PlanResolverState,
    ) -> PlanResult<()> {
        let definitions = state
            .ctes()
            .filter(|cte| cte.kind == CteKind::Definition && !cte.bindings.is_empty())
            .cloned()
            .collect::<Vec<_>>();
        if definitions.is_empty() {
            return Ok(());
        }
        for field in plan.schema().fields() {
            let origin = state.get_field_info(field.name())?.origin();
            for cte in &definitions {
                for &plan_id in cte.bindings.get(&origin).into_iter().flatten() {
                    state.register_plan_id_for_field(field.name(), plan_id)?;
                }
            }
        }
        Ok(())
    }
}

/// Decides, for the CTEs of one `WITH` scope (in definition order), whether
/// each is shared or inlined. A CTE can only be referenced by the CTEs defined
/// after it and by the query, so going from the last CTE to the first gives
/// each its final count: none, and it is dropped; one, and its reference
/// becomes its plan; more, and its references read one result, computed by the
/// `WithSharedCtesNode` placed over the query.
fn share_or_inline(
    plan: LogicalPlan,
    definitions: Vec<(u64, String, LogicalPlan)>,
) -> PlanResult<LogicalPlan> {
    use datafusion_common::tree_node::{Transformed, TransformedResult, TreeNodeRecursion};
    use datafusion_expr::Extension;
    use sail_logical_plan::shared_cte::{SharedCteRefNode, WithSharedCtesNode};

    fn ref_id(node: &LogicalPlan) -> Option<u64> {
        match node {
            LogicalPlan::Extension(Extension { node }) => node
                .as_any()
                .downcast_ref::<SharedCteRefNode>()
                .map(|r| r.id()),
            _ => None,
        }
    }
    fn count(plan: &LogicalPlan, id: u64) -> PlanResult<usize> {
        let mut n = 0;
        plan.apply_with_subqueries(|node| {
            if ref_id(node) == Some(id) {
                n += 1;
            }
            Ok(TreeNodeRecursion::Continue)
        })?;
        Ok(n)
    }
    fn inline(plan: LogicalPlan, id: u64, definition: &LogicalPlan) -> PlanResult<LogicalPlan> {
        Ok(plan
            .transform_up_with_subqueries(|node| {
                if ref_id(&node) == Some(id) {
                    Ok(Transformed::yes(definition.clone()))
                } else {
                    Ok(Transformed::no(node))
                }
            })
            .data()?)
    }

    if definitions.is_empty() {
        return Ok(plan);
    }
    let mut plan = plan;
    // (id, name, definition) of the shared CTEs, last first.
    let mut shared: Vec<(u64, String, LogicalPlan)> = Vec::new();
    for (id, name, definition) in definitions.into_iter().rev() {
        let mut n = count(&plan, id)?;
        for (_, _, d) in &shared {
            n += count(d, id)?;
        }
        match n {
            0 => {}
            1 => {
                plan = inline(plan, id, &definition)?;
                shared = shared
                    .into_iter()
                    .map(|(i, n, d)| Ok((i, n, inline(d, id, &definition)?)))
                    .collect::<PlanResult<Vec<_>>>()?;
            }
            _ => shared.push((id, name, definition)),
        }
    }
    if shared.is_empty() {
        return Ok(plan);
    }
    shared.reverse();
    let (ids, names, definitions): (Vec<_>, Vec<_>, Vec<_>) = shared.into_iter().fold(
        (vec![], vec![], vec![]),
        |(mut ids, mut names, mut defs), (i, n, d)| {
            ids.push(i);
            names.push(n);
            defs.push(std::sync::Arc::new(d));
            (ids, names, defs)
        },
    );
    Ok(LogicalPlan::Extension(Extension {
        node: std::sync::Arc::new(WithSharedCtesNode::new(
            ids,
            names,
            definitions,
            std::sync::Arc::new(plan),
        )),
    }))
}
