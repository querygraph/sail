//! Owned parser failures retained before the borrowed token input is released.

use std::fmt;

use chumsky::error::{Rich, RichPattern, RichReason};
use sail_sql_parser::span::TokenSpan;

use super::ParserErrorDisplay;

mod spark;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ParserFailureReason {
    ExpectedFound {
        expected: Vec<String>,
        expects_end_of_input: bool,
        found: Option<String>,
    },
    Custom(String),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ParserFailure {
    pub reason: ParserFailureReason,
    pub span: TokenSpan,
    pub contexts: Vec<(String, TokenSpan)>,
    /// The existing human diagnostic, preserved independently of transport.
    pub message: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ParserDiagnostics {
    failures: Vec<ParserFailure>,
    source: Option<String>,
}

impl ParserDiagnostics {
    pub fn failures(&self) -> &[ParserFailure] {
        &self.failures
    }

    pub(super) fn with_source(mut self, source: &str) -> Self {
        self.source = Some(source.to_owned());
        self
    }

    pub(super) fn from_errors<T, S>(errors: Vec<Rich<'_, T, S>>) -> Self
    where
        T: fmt::Display,
        S: Into<TokenSpan> + Clone,
    {
        Self {
            source: None,
            failures: errors
                .into_iter()
                .map(|error| ParserFailure {
                    reason: match error.reason() {
                        RichReason::ExpectedFound { expected, found } => {
                            ParserFailureReason::ExpectedFound {
                                expected: expected.iter().map(ToString::to_string).collect(),
                                expects_end_of_input: expected
                                    .iter()
                                    .any(|pattern| matches!(pattern, RichPattern::EndOfInput)),
                                found: found.as_ref().map(|token| token.to_string()),
                            }
                        }
                        RichReason::Custom(message) => ParserFailureReason::Custom(message.clone()),
                    },
                    span: error.span().clone().into(),
                    contexts: error
                        .contexts()
                        .map(|(label, span)| (label.to_string(), span.clone().into()))
                        .collect(),
                    message: ParserErrorDisplay(&error).to_string(),
                })
                .collect(),
        }
    }
}

impl fmt::Display for ParserDiagnostics {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        for (index, failure) in self.failures.iter().enumerate() {
            if index != 0 {
                f.write_str("; ")?;
            }
            f.write_str(&failure.message)?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests;
