use super::{
    java_utf8_roundtrip, java_whitespace, monge_tokens, score, token_set, whitespace_tokens,
    MongeInnerMetric, TokenConfig, TokenMetric,
};

const SET_METRICS: [TokenMetric; 5] = [
    TokenMetric::Jaccard,
    TokenMetric::Cosine,
    TokenMetric::SorensenDice,
    TokenMetric::OverlapCoefficient,
    TokenMetric::BraunBlanquet,
];

fn assert_close(actual: f64, expected: f64) {
    assert!((actual - expected).abs() <= 1e-12, "{actual} != {expected}");
}

fn grams(ngram_size: usize) -> TokenConfig {
    TokenConfig {
        ngram_size,
        ..TokenConfig::default()
    }
}

#[test]
fn all_metrics_preserve_raw_empty_boundaries_and_case() {
    for metric in SET_METRICS.into_iter().chain([TokenMetric::MongeElkan]) {
        let config = TokenConfig::default();
        assert_eq!(score(metric, "", "", &config), 1.0);
        assert_eq!(score(metric, "", "hello", &config), 0.0);
        assert_eq!(score(metric, "hello", "", &config), 0.0);
        assert_eq!(score(metric, "hello world", "hello world", &config), 1.0);
        assert_eq!(score(metric, "A", "a", &config), 0.0);
    }
}

#[test]
fn set_metrics_match_hand_computed_cardinality_formulas() {
    let config = TokenConfig::default();
    let cases = [
        (TokenMetric::Jaccard, 0.5, 1.0 / 3.0),
        (TokenMetric::Cosine, 2.0 / 3.0, 0.5),
        (TokenMetric::SorensenDice, 4.0 / 6.0, 0.5),
        (TokenMetric::OverlapCoefficient, 2.0 / 3.0, 0.5),
        (TokenMetric::BraunBlanquet, 2.0 / 3.0, 0.5),
    ];
    for (metric, partial, duplicates) in cases {
        assert_close(score(metric, "a b c", "a b d", &config), partial);
        assert_close(score(metric, "a a b", "a c c", &config), duplicates);
        assert_eq!(score(metric, "a a a b", "a b", &config), 1.0);
        assert_eq!(score(metric, "alpha", "omega", &config), 0.0);
    }
    assert_close(
        score(TokenMetric::Cosine, "a b", "a", &config),
        1.0 / 2.0_f64.sqrt(),
    );
    assert_eq!(
        score(TokenMetric::BraunBlanquet, "a b c d", "a b", &config),
        0.5
    );
    assert_eq!(
        score(TokenMetric::OverlapCoefficient, "a b c d", "a b", &config),
        1.0
    );
}

#[test]
fn raw_empty_and_whitespace_only_are_distinct_upstream_cases() {
    let config = TokenConfig::default();
    for metric in SET_METRICS {
        assert_eq!(score(metric, " \t\n", "\r ", &config), 1.0);
        assert_eq!(score(metric, "", " \t", &config), 0.0);
        let expected = match metric {
            TokenMetric::Cosine | TokenMetric::OverlapCoefficient => 1.0,
            _ => 0.0,
        };
        assert_eq!(score(metric, " \t", "alpha", &config), expected);
        assert_eq!(score(metric, "alpha", " \t", &config), expected);
    }
    assert_eq!(score(TokenMetric::MongeElkan, "", " \t", &config), 1.0);
    assert_eq!(score(TokenMetric::MongeElkan, " \t", "alpha", &config), 0.0);
}

#[test]
fn java_whitespace_delimiters_and_exclusions_are_explicit() {
    let delimiters = [
        0x0009, 0x000A, 0x000B, 0x000C, 0x000D, 0x001C, 0x001D, 0x001E, 0x001F, 0x0020, 0x1680,
        0x2000, 0x2001, 0x2002, 0x2003, 0x2004, 0x2005, 0x2006, 0x2008, 0x2009, 0x200A, 0x2028,
        0x2029, 0x205F, 0x3000,
    ];
    for unit in delimiters {
        assert!(java_whitespace(unit));
        let separator = char::from_u32(u32::from(unit)).expect("BMP delimiter");
        let value = format!("alpha{separator}beta");
        for metric in SET_METRICS {
            assert_eq!(
                score(metric, &value, "alpha beta", &TokenConfig::default()),
                1.0
            );
        }
    }
    for unit in [
        0x0085, 0x00A0, 0x180E, 0x2007, 0x200B, 0x202F, 0x2060, 0xFEFF,
    ] {
        assert!(!java_whitespace(unit));
        let separator = char::from_u32(u32::from(unit)).expect("BMP non-delimiter");
        let value = format!("alpha{separator}beta");
        assert_eq!(token_set(&value, 0).len(), 1);
        assert_eq!(
            score(
                TokenMetric::Jaccard,
                &value,
                "alpha beta",
                &TokenConfig::default()
            ),
            0.0
        );
    }
}

#[test]
fn whitespace_sequences_preserve_repetitions_and_punctuation() {
    let sequence = whitespace_tokens("alpha beta alpha gamma");
    let expected: Vec<Vec<u16>> = ["alpha", "beta", "alpha", "gamma"]
        .into_iter()
        .map(|token| token.encode_utf16().collect())
        .collect();
    assert_eq!(sequence, expected);
    assert_eq!(token_set("alpha alpha alpha beta", 0).len(), 2);
    assert_eq!(token_set("alpha,beta", 0).len(), 1);
    assert_eq!(token_set(" \t\n ", 0).len(), 0);
}

#[test]
fn ngrams_match_upstream_fixtures_and_preserve_raw_spaces() {
    let config = grams(2);
    for (left, right, expected) in [
        ("abcd", "abce", 0.5),
        ("abc", "abc", 1.0),
        ("abc", "xyz", 0.0),
        ("ab", "abc", 0.5),
        ("a b", "a c", 1.0 / 3.0),
    ] {
        assert_close(score(TokenMetric::Jaccard, left, right, &config), expected);
    }
    assert!(token_set("", 2).is_empty());
    assert_eq!(
        token_set("ab", 3),
        std::collections::BTreeSet::from([vec![97, 98]])
    );
    assert_eq!(token_set("aaaa", 2).len(), 1);
    for metric in SET_METRICS {
        assert_eq!(score(metric, "ab", "ab", &grams(3)), 1.0);
        assert_eq!(score(metric, "", "ab", &grams(3)), 0.0);
    }
}

#[test]
fn ngrams_use_utf16_units_without_normalization() {
    assert_eq!(
        token_set("😀", 1),
        std::collections::BTreeSet::from([vec![0xD83D], vec![0xDE00]])
    );
    assert_eq!(
        token_set("😀", 2),
        std::collections::BTreeSet::from([vec![0xD83D, 0xDE00]])
    );
    // These emoji share one surrogate, not one Unicode scalar token.
    assert_close(
        score(TokenMetric::Jaccard, "😀", "😁", &grams(1)),
        1.0 / 3.0,
    );
    assert_eq!(score(TokenMetric::Jaccard, "😀", "😁", &grams(2)), 0.0);
    assert_eq!(
        score(
            TokenMetric::Jaccard,
            "café",
            "cafe\u{0301}",
            &TokenConfig::default()
        ),
        0.0
    );
    assert_eq!(
        score(
            TokenMetric::Jaccard,
            "北京大学",
            "北京大学",
            &TokenConfig::default()
        ),
        1.0
    );
}

#[test]
fn monge_surrogate_roundtrip_matches_java_utf8_replacement() {
    assert_eq!(java_utf8_roundtrip(&[0xD83D]), "?");
    assert_eq!(java_utf8_roundtrip(&[0xDE00]), "?");
    assert_eq!(java_utf8_roundtrip(&[0xD83D, 0xDE00]), "😀");
    assert_eq!(java_utf8_roundtrip(&[0xD83D, 0x0061, 0xDE00]), "?a?");
    // Deduplicate the Java Strings before their UTF8 conversion, not afterwards.
    assert_eq!(monge_tokens("😀", 1), ["?", "?"]);
    assert_eq!(score(TokenMetric::MongeElkan, "😀", "😁", &grams(1)), 1.0);
}

#[test]
fn monge_is_symmetric_and_repetitions_affect_directed_averages() {
    let config = TokenConfig {
        inner_metric: MongeInnerMetric::Levenshtein,
        ..TokenConfig::default()
    };
    assert_close(
        score(TokenMetric::MongeElkan, "a a b", "a c", &config),
        7.0 / 12.0,
    );
    assert_close(
        score(TokenMetric::MongeElkan, "a c", "a a b", &config),
        7.0 / 12.0,
    );
    assert_eq!(score(TokenMetric::MongeElkan, "a b", "a c", &config), 0.5);
    assert_eq!(
        score(
            TokenMetric::MongeElkan,
            "alpha beta gamma",
            "gamma beta alpha",
            &config
        ),
        1.0
    );
    assert!(score(TokenMetric::MongeElkan, "alpha,beta", "alpha beta", &config) < 1.0);
}

#[test]
fn monge_accepts_all_five_inner_defaults_and_ngram_mode() {
    for inner_metric in [
        MongeInnerMetric::JaroWinkler,
        MongeInnerMetric::Jaro,
        MongeInnerMetric::Levenshtein,
        MongeInnerMetric::NeedlemanWunsch,
        MongeInnerMetric::SmithWaterman,
    ] {
        let config = TokenConfig {
            ngram_size: 2,
            inner_metric,
        };
        assert_eq!(score(TokenMetric::MongeElkan, "abc", "abc", &config), 1.0);
        let value = score(
            TokenMetric::MongeElkan,
            "stephen smyth",
            "steven smith",
            &config,
        );
        assert!((0.0..=1.0).contains(&value));
        assert_eq!(
            value,
            score(
                TokenMetric::MongeElkan,
                "stephen smyth",
                "steven smith",
                &config
            )
        );
    }
    let jaro = TokenConfig {
        inner_metric: MongeInnerMetric::Jaro,
        ..TokenConfig::default()
    };
    assert_ne!(
        score(
            TokenMetric::MongeElkan,
            "stephen smyth",
            "steven smith",
            &jaro
        ),
        score(
            TokenMetric::MongeElkan,
            "stephen smyth",
            "steven smith",
            &TokenConfig::default()
        ),
    );
}
