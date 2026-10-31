use std::collections::HashMap;

use sail_common::error::StructuredSparkError;

use super::{ParserDiagnostics, ParserFailureReason};

impl ParserDiagnostics {
    /// Render a source-backed token mismatch. Custom/lexical failures keep their
    /// existing diagnostics. Chumsky has no ANTLR insertion/deletion recovery;
    /// those additional Spark hints require separate qualification.
    pub fn spark_syntax_error(&self) -> Option<StructuredSparkError> {
        let source = self.source.as_deref()?;
        let failure = self.failures.first()?;
        let ParserFailureReason::ExpectedFound {
            expected,
            expects_end_of_input,
            found,
        } = &failure.reason
        else {
            return None;
        };
        // A trailing token after a complete statement can require ANTLR's
        // extra-input recovery hint. Do not classify that recovery as hint-free.
        if expected.is_empty() || *expects_end_of_input {
            return None;
        }
        let error = match found {
            Some(token) => {
                let text = source.get(failure.span.start..failure.span.end)?;
                // Hidden-channel trivia is not a Spark error token. A mismatch
                // between source and found token must not invent a location.
                if text != token
                    || text.trim().is_empty()
                    || text.starts_with("--")
                    || text.starts_with("/*")
                {
                    return None;
                }
                format!(
                    "'{}'",
                    text.replace('\n', "\\n")
                        .replace('\r', "\\r")
                        .replace('\t', "\\t")
                )
            }
            None if failure.span.is_empty() && failure.span.start == source.len() => {
                "end of input".to_owned()
            }
            None => return None,
        };
        Some(StructuredSparkError {
            class: "org.apache.spark.sql.catalyst.parser.ParseException".into(),
            condition: "PARSE_SYNTAX_ERROR".into(),
            sql_state: "42601".into(),
            message: format!("Syntax error at or near {error}."),
            parameters: HashMap::from([("error".into(), error), ("hint".into(), String::new())]),
        })
    }
}
