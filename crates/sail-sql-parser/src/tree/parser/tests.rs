use chumsky::Parser;
use chumsky::input::Input;
use sail_sql_macro::TreeParser;

use super::TreeParser;
use crate::ast::identifier::Ident;
use crate::ast::keywords::{Create, Explain, Insert, Overwrite, Select};
use crate::lexer::create_lexer;
use crate::options::ParserOptions;

// Five choices exercise the derive's boxed-enum path. The custom named fields
// include bool, which has no TreeParser: the hook must construct the AST itself.
#[derive(Debug, Clone, TreeParser)]
#[parser(dependency = "Ident")]
enum Fixture {
    Default(Select),
    #[parser(function = |ident, options| {
        Create::parser((), options).ignore_then(ident).map(move |value| Self::Custom {
            value,
            enabled: options.allow_double_quote_identifier,
        })
    })]
    Custom {
        value: Ident,
        enabled: bool,
    },
    Insert(Insert),
    Overwrite(Overwrite),
    Explain(Explain),
}

fn parse(sql: &str, enabled: bool) -> Result<Fixture, String> {
    let options = ParserOptions {
        allow_double_quote_identifier: enabled,
        ..Default::default()
    };
    let tokens = create_lexer::<_, chumsky::extra::Err<chumsky::error::Rich<_, _>>>(&options)
        .parse(sql)
        .into_result()
        .map_err(|error| format!("{error:?}"))?;
    let input = tokens
        .as_slice()
        .map((sql.len()..sql.len()).into(), |(token, span)| (token, span));
    <Fixture as TreeParser<'_, _, chumsky::extra::Err<chumsky::error::Rich<_, _>>, _>>::parser(
        Ident::parser((), &options),
        &options,
    )
    .parse(input)
    .into_result()
    .map_err(|error: Vec<chumsky::error::Rich<_, _>>| format!("{error:?}"))
}

#[derive(Debug, Clone, TreeParser)]
#[parser(dependency = "(Ident, Ident)")]
enum PairFixture {
    #[parser(function = |_, options| Explain::parser((), options).map(|_| Self::Unit))]
    Unit,
    #[parser(function = |(left, right), _| chumsky::Parser::then(left, right).map(|(a, b)| Self::Pair(a, b)))]
    Pair(Ident, Ident),
}

#[test]
fn unboxed_variants_forward_tuple_dependencies_and_support_custom_unit() -> Result<(), String> {
    let options = ParserOptions::default();
    for (sql, expected) in [
        ("EXPLAIN", None),
        ("first second", Some(("first", "second"))),
    ] {
        let tokens = create_lexer::<_, chumsky::extra::Err<chumsky::error::Rich<_, _>>>(&options)
            .parse(sql)
            .into_result()
            .map_err(|error| format!("{error:?}"))?;
        let input = tokens
            .as_slice()
            .map((sql.len()..sql.len()).into(), |(token, span)| (token, span));
        let parser = <PairFixture as TreeParser<
            '_,
            _,
            chumsky::extra::Err<chumsky::error::Rich<_, _>>,
            _,
        >>::parser(
            (Ident::parser((), &options), Ident::parser((), &options)),
            &options,
        );
        let actual = parser
            .parse(input)
            .into_result()
            .map_err(|error| format!("{error:?}"))?;
        match (actual, expected) {
            (PairFixture::Unit, None) => {}
            (PairFixture::Pair(a, b), Some((left, right))) => {
                assert_eq!(a.value, left);
                assert_eq!(b.value, right);
            }
            _ => return Err("wrong custom tuple/unit variant".into()),
        }
    }
    Ok(())
}

#[test]
fn custom_variant_forwards_dependency_and_options() -> Result<(), String> {
    for enabled in [false, true] {
        let Fixture::Custom {
            value,
            enabled: flag,
        } = parse("CREATE item", enabled)?
        else {
            return Err("expected custom named variant".into());
        };
        assert_eq!(value.value, "item");
        assert_eq!(flag, enabled);
    }
    assert!(parse("CREATE \"item\"", false).is_err());
    let Fixture::Custom { value, enabled } = parse("CREATE \"item\"", true)? else {
        return Err("expected option-dependent custom parser".into());
    };
    assert_eq!(value.value, "item");
    assert!(enabled);
    assert!(parse("CREATE", true).is_err());
    Ok(())
}

#[test]
fn default_variants_remain_choices_alongside_custom_parser() -> Result<(), String> {
    let variants = [
        parse("SELECT", false)?,
        parse("INSERT", false)?,
        parse("OVERWRITE", false)?,
        parse("EXPLAIN", false)?,
    ];
    let spans = variants.map(|variant| match variant {
        Fixture::Default(keyword) => keyword.span,
        Fixture::Insert(keyword) => keyword.span,
        Fixture::Overwrite(keyword) => keyword.span,
        Fixture::Explain(keyword) => keyword.span,
        Fixture::Custom { value, .. } => value.span,
    });
    assert_eq!(spans.map(|span| span.start), [0; 4]);
    assert_eq!(spans.map(|span| span.end), [6, 6, 9, 7]);
    Ok(())
}
