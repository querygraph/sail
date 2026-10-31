//! INSERT modes share an AST, but OVERWRITE couples IF NOT EXISTS to PARTITION.

use chumsky::Parser;
use chumsky::extra::ParserExtra;
use chumsky::input::{Input, ValueInput};
use chumsky::label::LabelError;
use either::Either;

use super::{PartitionClause, Statement};
use crate::ast::expression::Expr;
use crate::ast::identifier::ObjectName;
use crate::ast::keywords::{By, Exists, If, Insert, Into, Name, Not, Overwrite, Table};
use crate::ast::query::{IdentList, Query, WithClause};
use crate::options::ParserOptions;
use crate::span::TokenSpan;
use crate::token::{Token, TokenLabel};
use crate::tree::TreeParser;

pub(super) fn statement<'a, I, E, Q, X>(
    (query, expression): (Q, X),
    options: &'a ParserOptions,
) -> impl Parser<'a, I, Statement, E> + Clone
where
    I: Input<'a, Token = Token<'a>> + ValueInput<'a>,
    I::Span: std::convert::Into<TokenSpan> + Clone,
    E: ParserExtra<'a, I> + 'a,
    E::Error: LabelError<'a, I, TokenLabel>,
    Q: Parser<'a, I, Query, E> + Clone + 'a,
    X: Parser<'a, I, Expr, E> + Clone + 'a,
{
    let target = Option::<Table>::parser((), options).then(ObjectName::parser((), options));
    let partition = PartitionClause::parser(expression, options);
    let if_not_exists = <(If, Not, Exists)>::parser((), options);
    let into = Into::parser((), options)
        .map(Either::Left)
        .then(target.clone())
        .then(
            partition
                .clone()
                .or_not()
                .then(if_not_exists.clone().or_not()),
        );
    let overwrite = Overwrite::parser((), options)
        .map(Either::Right)
        .then(target)
        .then(
            partition
                .then(if_not_exists.or_not())
                .or_not()
                .map(|clause| match clause {
                    Some((partition, if_not_exists)) => (Some(partition), if_not_exists),
                    None => (None, None),
                }),
        );
    WithClause::parser(query.clone(), options)
        .or_not()
        .then(Insert::parser((), options))
        .then(into.or(overwrite))
        .then(Option::<Either<(By, Name), IdentList>>::parser((), options))
        .then(query)
        .map(
            |(
                (
                    (
                        (with, insert),
                        ((into_or_overwrite, (table, name)), (partition, if_not_exists)),
                    ),
                    columns,
                ),
                query,
            )| Statement::InsertInto {
                with,
                insert,
                into_or_overwrite,
                table,
                name,
                partition,
                if_not_exists,
                columns,
                query,
            },
        )
}
