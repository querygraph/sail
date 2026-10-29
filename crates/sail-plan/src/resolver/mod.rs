use std::sync::Arc;

use datafusion::prelude::SessionContext;

use crate::config::PlanConfig;

mod command;
mod constraint;
mod data_type;
mod expression;
mod function;
mod lakehouse;
mod literal;
pub mod plan;
mod query;
mod schema;
pub mod scope;
mod state;
mod tree;

pub struct PlanResolver<'a> {
    ctx: &'a SessionContext,
    config: Arc<PlanConfig>,
    relation_scope: Option<&'a dyn scope::RelationScope>,
}

impl<'a> PlanResolver<'a> {
    pub fn new(ctx: &'a SessionContext, config: Arc<PlanConfig>) -> Self {
        Self {
            ctx,
            config,
            relation_scope: None,
        }
    }

    /// Resolve request-local relations without registering them in a catalog.
    pub fn with_relation_scope(mut self, scope: &'a dyn scope::RelationScope) -> Self {
        self.relation_scope = Some(scope);
        self
    }
}
