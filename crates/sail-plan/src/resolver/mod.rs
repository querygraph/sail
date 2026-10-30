use std::sync::Arc;

use datafusion::catalog::Session;
use datafusion::prelude::SessionContext;
use sail_common_datafusion::session::scoped::ScopedSession;

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
    source_read_case_sensitive: Option<bool>,
    relation_scope: Option<&'a dyn scope::RelationScope>,
    query_session: Option<session::QuerySession<'a>>,
}

impl<'a> PlanResolver<'a> {
    pub fn new(ctx: &'a SessionContext, config: Arc<PlanConfig>) -> Self {
        Self {
            ctx,
            config,
            source_read_case_sensitive: None,
            relation_scope: None,
            query_session: None,
        }
    }

    /// Override case matching passed to query source factories independently of
    /// semantic column resolution. Ordinary callers inherit `PlanConfig`.
    /// The caller chooses the metadata phase; this does not mutate the Session,
    /// change source options or override write/row-operation schema matching.
    pub fn with_source_read_case_sensitive(mut self, case_sensitive: bool) -> Self {
        self.source_read_case_sensitive = Some(case_sensitive);
        self
    }

    fn source_read_case_sensitive(&self) -> bool {
        self.source_read_case_sensitive
            .unwrap_or(self.config.case_sensitive)
    }

    /// Resolve request-local relations without registering them in a catalog.
    pub fn with_relation_scope(mut self, scope: &'a dyn scope::RelationScope) -> Self {
        self.relation_scope = Some(scope);
        self
    }

    /// Use a caller-captured Session for read/write, row-operation, and procedure
    /// planning callbacks and queries executed during resolution.
    /// The caller retains admission/cleanup and chooses the capture boundary;
    /// authoritative session commands and catalog name resolution still use ctx.
    /// Native command analysis and final execution require their own propagation.
    pub fn with_query_session(mut self, session: &'a dyn Session) -> Self {
        self.query_session = Some(session::QuerySession::Generic(session));
        self
    }

    /// Also use the captured native analyzer for command validation. The caller
    /// retains admission/cleanup, chooses the capture boundary, and must keep
    /// providers valid for this owner. This does not migrate catalog authority,
    /// session commands, or final execution. The last Session setter wins.
    pub fn with_scoped_query_session(mut self, session: &'a ScopedSession) -> Self {
        self.query_session = Some(session::QuerySession::Scoped(session));
        self
    }
}
