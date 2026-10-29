use either::Either;
use sail_sql_macro::{TreeParser, TreeSyntax, TreeText};

use crate::ast::data_type::DataType;
use crate::ast::expression::Expr;
use crate::ast::identifier::{Ident, ObjectName};
use crate::ast::keywords::{Create, Exists, If, Materialized, Not, Streaming, Table, Using, View};
use crate::ast::query::Query;
use crate::ast::statement::{AsQueryClause, ColumnDefinitionList, CreateTableClause};
use crate::combinator::compose;

/// A pipeline dataset declaration. Lowering it requires a pipeline registration
/// context; ordinary SQL execution must not silently create a regular table.
#[derive(Debug, Clone, TreeParser, TreeSyntax, TreeText)]
#[parser(dependency = "(Query, Expr, DataType)")]
pub struct CreatePipelineDataset {
    pub create: Create,
    pub kind: Either<(Materialized, View), (Streaming, Table)>,
    pub if_not_exists: Option<(If, Not, Exists)>,
    pub name: ObjectName,
    #[parser(function = |(_, e, d), o| compose((e, d), o))]
    pub columns: Option<ColumnDefinitionList>,
    pub using: Option<(Using, Ident)>,
    #[parser(function = |(q, e, d), o| compose((e, q, d), o))]
    pub clauses: Vec<CreateTableClause>,
    #[parser(function = |(q, _, _), o| compose(q, o))]
    pub r#as: Option<AsQueryClause>,
}
