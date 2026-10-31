//! Owned parser failures retained before the borrowed token input is released.

use std::fmt;

use chumsky::error::{Rich, RichReason};
use sail_sql_parser::span::TokenSpan;

use super::ParserErrorDisplay;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ParserFailureReason {
    ExpectedFound {
        expected: Vec<String>,
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
}

impl ParserDiagnostics {
    pub fn failures(&self) -> &[ParserFailure] {
        &self.failures
    }

    pub(super) fn from_errors<T, S>(errors: Vec<Rich<'_, T, S>>) -> Self
    where
        T: fmt::Display,
        S: Into<TokenSpan> + Clone,
    {
        Self {
            failures: errors
                .into_iter()
                .map(|error| ParserFailure {
                    reason: match error.reason() {
                        RichReason::ExpectedFound { expected, found } => {
                            ParserFailureReason::ExpectedFound {
                                expected: expected.iter().map(ToString::to_string).collect(),
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
