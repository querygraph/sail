use chumsky::Parser;
use chumsky::error::Rich;
use chumsky::prelude::{end, just};
use chumsky::span::SimpleSpan;

use super::*;
use crate::error::SqlError;
use crate::source::SqlSource;

type TestResult = Result<(), Box<dyn std::error::Error>>;

#[test]
fn owned_failures_preserve_custom_reasons_order_spans_and_display() {
    let errors = vec![
        Rich::<char, SimpleSpan>::custom((2..5).into(), "first"),
        Rich::<char, SimpleSpan>::custom((8..8).into(), "second"),
    ];
    let diagnostic = ParserDiagnostics::from_errors(errors);
    assert_eq!(diagnostic.to_string(), "first at 2:5; second");
    assert_eq!(diagnostic.failures().len(), 2);
    assert_eq!(
        diagnostic.failures()[0].reason,
        ParserFailureReason::Custom("first".into())
    );
    assert_eq!(
        diagnostic.failures()[0].span,
        TokenSpan { start: 2, end: 5 }
    );
    assert_eq!(
        diagnostic.failures()[1].reason,
        ParserFailureReason::Custom("second".into())
    );
    assert!(diagnostic.failures()[1].span.is_empty());
}

#[test]
fn expected_found_keeps_token_and_eof_distinct() -> TestResult {
    for (input, found) in [("b", Some("b")), ("", None)] {
        let errors = just::<_, _, chumsky::extra::Err<Rich<_, _>>>('a')
            .then_ignore(end())
            .parse(input)
            .into_result()
            .err()
            .ok_or("expected invalid input")?;
        let diagnostic = ParserDiagnostics::from_errors(errors);
        let [failure] = diagnostic.failures() else {
            return Err("expected one failure".into());
        };
        let ParserFailureReason::ExpectedFound {
            expected,
            found: actual,
        } = &failure.reason
        else {
            return Err("expected a structured token mismatch".into());
        };
        assert!(!expected.is_empty());
        assert_eq!(actual.as_deref(), found);
        assert_eq!(failure.span.start, 0);
        assert_eq!(failure.span.end, input.len());
    }
    Ok(())
}

#[test]
fn sql_failure_outlives_source_through_both_parse_routes() -> TestResult {
    for spanned in [false, true] {
        let error = {
            let text = String::from("CREATE FLOW f AS INSERT INTO output BY NAME SELECT )");
            let source = SqlSource::new(&text)?;
            if spanned {
                source.parse_statements_with_spans().err()
            } else {
                source.parse_statements().err()
            }
            .ok_or("expected parser failure")?
        };
        let SqlError::SqlParserError(diagnostic) = error else {
            return Err("expected retained parser diagnostics".into());
        };
        assert!(!diagnostic.failures().is_empty());
        assert!(diagnostic.failures().iter().any(|failure| matches!(
            &failure.reason,
            ParserFailureReason::ExpectedFound { found: Some(token), .. } if token == ")"
        )));
        assert!(diagnostic.to_string().contains("found )"));
    }
    Ok(())
}

#[test]
fn owned_failures_retain_emitted_parser_contexts() -> TestResult {
    // The SQL grammar currently emits no Rich contexts for the source fixture
    // above. Exercise an explicit Chumsky context rather than inventing one.
    let errors = just::<_, _, chumsky::extra::Err<Rich<_, _>>>('a')
        .then_ignore(just('b'))
        .labelled("pair")
        .as_context()
        .then_ignore(end())
        .parse("ac")
        .into_result()
        .err()
        .ok_or("expected contextual parser failure")?;
    let diagnostic = ParserDiagnostics::from_errors(errors);
    let [failure] = diagnostic.failures() else {
        return Err("expected one contextual failure".into());
    };
    assert_eq!(
        failure.contexts,
        // A Chumsky context covers the consumed prefix; the unexpected token
        // has its own span and is not part of that prefix.
        vec![("pair".into(), TokenSpan { start: 0, end: 1 })]
    );
    assert_eq!(failure.span, TokenSpan { start: 1, end: 2 });
    assert!(diagnostic.to_string().contains("in pair at 0:1"));
    Ok(())
}
