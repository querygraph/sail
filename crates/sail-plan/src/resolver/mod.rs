use std::sync::Arc;

use datafusion::catalog::Session;
use datafusion::prelude::SessionContext;

use crate::config::PlanConfig;

mod command;
mod constant;
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
mod session;
mod state;
mod tree;

#[cfg(test)]
mod time_tests;

pub struct PlanResolver<'a> {
    ctx: &'a SessionContext,
    config: Arc<PlanConfig>,
    relation_scope: Option<&'a dyn scope::RelationScope>,
    query_session: Option<&'a dyn Session>,
}

impl<'a> PlanResolver<'a> {
    pub fn new(ctx: &'a SessionContext, config: Arc<PlanConfig>) -> Self {
        Self {
            ctx,
            config,
            relation_scope: None,
            query_session: None,
        }
    }

    /// Resolve request-local relations without registering them in a catalog.
    pub fn with_relation_scope(mut self, scope: &'a dyn scope::RelationScope) -> Self {
        self.relation_scope = Some(scope);
        self
    }

    /// Use a caller-captured Session for read/write factories and queries executed
    /// during resolution.
    /// The caller retains admission/cleanup and chooses the capture boundary;
    /// authoritative session commands and catalog name resolution still use ctx.
    /// Procedure/row-level factories and final execution require their own propagation.
    pub fn with_query_session(mut self, session: &'a dyn Session) -> Self {
        self.query_session = Some(session);
        self
    }
}
