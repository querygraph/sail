//! Original SQL text and the lexer spans used to parse it.

use chumsky::Parser;
use chumsky::input::Input;
use chumsky::span::SimpleSpan;
use sail_sql_parser::ast::statement::Statement;
use sail_sql_parser::options::ParserOptions;
use sail_sql_parser::parser::{create_parser, create_parser_with_spans};
pub use sail_sql_parser::span::TokenSpan;
use sail_sql_parser::token::{Keyword, Punctuation, Token};

use crate::error::{SqlError, SqlResult};
use crate::parser::{map_parser_input, tokenize};

/// A SQL input tokenized once, retaining original text for source-aware lowering.
/// AST spans passed to the analyzer must come from this same input.
pub struct SqlSource<'a> {
    text: &'a str,
    options: ParserOptions,
    tokens: Vec<(Token<'a>, SimpleSpan<usize>)>,
}

/// A statement's original text and one-based line within its SQL input.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct StatementOrigin<'a> {
    pub text: &'a str,
    pub line: usize,
}

impl<'a> StatementOrigin<'a> {
    /// Recover an origin after parsing without retaining or rebuilding lexer tokens.
    pub fn from_span(input: &'a str, span: TokenSpan) -> SqlResult<Self> {
        let invalid = || SqlError::invalid("statement span does not match SQL source");
        if span.is_empty() {
            return Err(invalid());
        }
        let text = input.get(span.start..span.end).ok_or_else(invalid)?;
        let prefix = input.get(..span.start).ok_or_else(invalid)?;
        Ok(Self {
            text,
            line: 1 + prefix.bytes().filter(|byte| *byte == b'\n').count(),
        })
    }
}

impl<'a> SqlSource<'a> {
    pub fn new(text: &'a str) -> SqlResult<Self> {
        let options = ParserOptions::default();
        let tokens = tokenize(text, &options)?;
        Ok(Self {
            text,
            options,
            tokens,
        })
    }

    pub fn parse_statements(&self) -> SqlResult<Vec<Statement>> {
        let length = self.text.len();
        let tokens = self
            .tokens
            .as_slice()
            .map((length..length).into(), map_parser_input);
        let statements =
            create_parser::<_, chumsky::extra::Err<chumsky::error::Rich<_, _>>>(&self.options)
                .parse(tokens)
                .into_result()
                .map_err(SqlError::parser)?;
        for statement in &statements {
            crate::validation::validate_insert_modes(statement)?;
        }
        Ok(statements)
    }

    pub fn parse_one_statement(&self) -> SqlResult<Statement> {
        let mut statements = self.parse_statements()?;
        match (statements.pop(), statements.is_empty()) {
            (Some(statement), true) => Ok(statement),
            _ => Err(SqlError::invalid("expected one statement")),
        }
    }

    pub fn parse_statements_with_spans(&self) -> SqlResult<Vec<(Statement, TokenSpan)>> {
        let length = self.text.len();
        let tokens = self
            .tokens
            .as_slice()
            .map((length..length).into(), map_parser_input);
        let statements = create_parser_with_spans::<
            _,
            chumsky::extra::Err<chumsky::error::Rich<_, _>>,
        >(&self.options)
        .parse(tokens)
        .into_result()
        .map_err(SqlError::parser)?;
        for (statement, _) in &statements {
            crate::validation::validate_insert_modes(statement)?;
        }
        Ok(statements)
    }

    /// Statement intervals retain trailing trivia, unlike a view's query text.
    pub fn statement_origin(&self, span: TokenSpan) -> SqlResult<StatementOrigin<'a>> {
        StatementOrigin::from_span(self.text, span)
    }

    /// The current CREATE VIEW grammar ends with AS query. Keep the query's
    /// original token interval, excluding surrounding trivia and its separator.
    pub(crate) fn view_query(&self, r#as: TokenSpan) -> SqlResult<&'a str> {
        let index = self
            .tokens
            .partition_point(|(_, span)| span.start < r#as.start);
        let Some((
            Token::Word {
                keyword: Some(Keyword::As),
                ..
            },
            span,
        )) = self.tokens.get(index)
        else {
            return Err(SqlError::invalid("view AS span does not match SQL source"));
        };
        if (span.start, span.end) != (r#as.start, r#as.end) {
            return Err(SqlError::invalid("view AS span does not match SQL source"));
        }
        let mut query = self.tokens[index + 1..]
            .iter()
            .take_while(|(token, _)| !matches!(token, Token::Punctuation(Punctuation::Semicolon)))
            .filter(|(token, _)| !token.is_whitespace());
        let Some((_, first)) = query.next() else {
            return Err(SqlError::invalid("missing view query in SQL source"));
        };
        let last = query.map(|(_, span)| span).last().unwrap_or(first);
        self.text
            .get(first.start..last.end)
            .ok_or_else(|| SqlError::invalid("view query span does not match SQL source"))
    }
}

#[cfg(test)]
mod tests {
    use sail_common::spec;
    use sail_sql_parser::tree::TreeText;

    use super::*;
    use crate::statement::{from_ast_statement, from_ast_statement_with_source};

    #[test]
    fn statement_origins_preserve_trivia_unicode_and_separators() -> SqlResult<()> {
        let source = SqlSource::new(
            "/* 雪 */;\n  CREATE VIEW `v;雪` AS SELECT ';' AS x /* tail ; */ ;\n\
             -- next\n  SELECT 2 -- tail\n",
        )?;
        let statements = source.parse_statements_with_spans()?;
        let origins = statements
            .iter()
            .map(|(_, span)| source.statement_origin(*span))
            .collect::<SqlResult<Vec<_>>>()?;
        assert_eq!(
            origins,
            vec![
                StatementOrigin {
                    text: "CREATE VIEW `v;雪` AS SELECT ';' AS x /* tail ; */ ",
                    line: 2
                },
                StatementOrigin {
                    text: "SELECT 2 -- tail\n",
                    line: 4
                },
            ]
        );
        assert_eq!(
            statements
                .iter()
                .map(|(statement, _)| statement.text())
                .collect::<Vec<_>>(),
            source
                .parse_statements()?
                .iter()
                .map(TreeText::text)
                .collect::<Vec<_>>(),
        );
        Ok(())
    }

    #[test]
    fn statement_origins_include_raw_configuration_values() -> SqlResult<()> {
        let source = SqlSource::new(
            ";; SET conf.test = 'a;b' /* configuration */;\r\n\
             SET CATALOG `catalog`; USE db; SELECT 1; -- after\n",
        )?;
        let origins = source
            .parse_statements_with_spans()?
            .iter()
            .map(|(_, span)| source.statement_origin(*span))
            .collect::<SqlResult<Vec<_>>>()?;
        assert_eq!(
            origins.iter().map(|origin| origin.text).collect::<Vec<_>>(),
            vec![
                "SET conf.test = 'a;b' /* configuration */",
                "SET CATALOG `catalog`",
                "USE db",
                "SELECT 1",
            ]
        );
        assert_eq!(
            origins.iter().map(|origin| origin.line).collect::<Vec<_>>(),
            vec![1, 2, 2, 2]
        );
        Ok(())
    }

    #[test]
    fn statement_origins_reject_invalid_byte_intervals() -> SqlResult<()> {
        let source = SqlSource::new("SELECT '雪'")?;
        for span in [
            TokenSpan::default(),
            TokenSpan { start: 9, end: 10 },
            TokenSpan {
                start: 0,
                end: usize::MAX,
            },
        ] {
            assert!(source.statement_origin(span).is_err());
        }
        Ok(())
    }

    fn view(plan: spec::Plan) -> SqlResult<spec::ViewDefinition> {
        match plan {
            spec::Plan::Command(spec::CommandPlan {
                node: spec::CommandNode::CreateView { definition, .. },
                ..
            }) => Ok(definition),
            _ => Err(SqlError::invalid("expected persisted view")),
        }
    }

    #[test]
    fn persisted_views_preserve_query_source_and_typed_plan() -> SqlResult<()> {
        for (sql, expected) in [
            (
                "CREATE VIEW b COMMENT 'my persisted comment' AS SELECT * FROM range(1, 4);",
                "SELECT * FROM range(1, 4)",
            ),
            (
                "CREATE VIEW b AS /* leading */ SELECT  1 /* inside */ + 2 /* trailing */ ;",
                "SELECT  1 /* inside */ + 2",
            ),
            (
                "CREATE VIEW `雪` AS SELECT 'é;雪' AS `值`; -- after",
                "SELECT 'é;雪' AS `值`",
            ),
            (
                "CREATE VIEW b AS\nWITH c AS (SELECT 1 AS x)\nSELECT x FROM c\n-- trailing\n;",
                "WITH c AS (SELECT 1 AS x)\nSELECT x FROM c",
            ),
            (
                "CREATE VIEW b AS SELECT /*+ REPARTITION(1) */ * FROM range(2);",
                "SELECT /*+ REPARTITION(1) */ * FROM range(2)",
            ),
            ("CREATE VIEW b AS VALUES (1), (2)", "VALUES (1), (2)"),
            ("CREATE VIEW b AS SELECT 1", "SELECT 1"),
        ] {
            let source = SqlSource::new(sql)?;
            let statement = source.parse_one_statement()?;
            let mut reconstructed = view(from_ast_statement(statement.clone())?)?;
            let original = view(from_ast_statement_with_source(statement, &source)?)?;
            assert_eq!(original.definition, expected, "{sql}");
            assert!(reconstructed.definition.ends_with(' '), "{sql}");
            // Only the source text changes; query, columns, flags and metadata agree.
            reconstructed.definition.clone_from(&original.definition);
            assert_eq!(original, reconstructed, "{sql}");
        }
        Ok(())
    }

    #[test]
    fn multiple_views_have_separate_query_intervals() -> SqlResult<()> {
        let source = SqlSource::new(
            "/* 雪 */ ;; CREATE VIEW a AS SELECT ';' AS x; -- next\n\
             CREATE VIEW b AS SELECT /* ; */ x FROM a;;;",
        )?;
        let definitions = source
            .parse_statements()?
            .into_iter()
            .map(|statement| view(from_ast_statement_with_source(statement, &source)?))
            .collect::<SqlResult<Vec<_>>>()?;
        assert_eq!(definitions.len(), 2);
        assert_eq!(definitions[0].definition, "SELECT ';' AS x");
        assert_eq!(definitions[1].definition, "SELECT /* ; */ x FROM a");
        Ok(())
    }

    #[test]
    fn explain_preserves_nested_view_source() -> SqlResult<()> {
        let source = SqlSource::new("EXPLAIN CREATE VIEW b AS SELECT * FROM range(1, 4);")?;
        let plan = from_ast_statement_with_source(source.parse_one_statement()?, &source)?;
        let spec::Plan::Command(spec::CommandPlan {
            node: spec::CommandNode::Explain { input, .. },
            ..
        }) = plan
        else {
            return Err(SqlError::invalid("expected EXPLAIN"));
        };
        assert_eq!(view(*input)?.definition, "SELECT * FROM range(1, 4)");
        Ok(())
    }

    #[test]
    fn parsing_and_source_validation_reject_invalid_inputs() -> SqlResult<()> {
        for sql in ["", ";;;", "SELECT 1; SELECT 2", "CREATE VIEW b AS"] {
            assert!(SqlSource::new(sql)?.parse_one_statement().is_err(), "{sql}");
        }
        let source = SqlSource::new("CREATE VIEW b AS SELECT 1")?;
        for span in [
            TokenSpan::default(),
            TokenSpan {
                start: usize::MAX,
                end: usize::MAX,
            },
        ] {
            assert!(source.view_query(span).is_err());
        }
        Ok(())
    }
}
