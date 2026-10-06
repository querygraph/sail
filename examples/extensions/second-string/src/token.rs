//! Token similarities from spark-second-string at a35db39fa8e9b65db2d201a45b86d11a6ca34b98.
//!
//! Inputs are non-null and case-sensitive. The caller handles SQL nulls and
//! validates configuration once. N-grams count Java UTF-16 code units rather
//! than Unicode scalar values; whitespace mode uses `Character.isWhitespace`.

use std::collections::BTreeSet;

use crate::matrix::{self, MatrixMetric};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum TokenMetric {
    Jaccard,
    Cosine,
    SorensenDice,
    OverlapCoefficient,
    BraunBlanquet,
    MongeElkan,
}

/// Only the inner metrics accepted by upstream Monge-Elkan.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum MongeInnerMetric {
    #[default]
    JaroWinkler,
    Jaro,
    Levenshtein,
    NeedlemanWunsch,
    SmithWaterman,
}

impl MongeInnerMetric {
    fn matrix_metric(self) -> MatrixMetric {
        match self {
            Self::JaroWinkler => MatrixMetric::JaroWinkler,
            Self::Jaro => MatrixMetric::Jaro,
            Self::Levenshtein => MatrixMetric::Levenshtein,
            Self::NeedlemanWunsch => MatrixMetric::NeedlemanWunsch,
            Self::SmithWaterman => MatrixMetric::SmithWaterman,
        }
    }
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct TokenConfig {
    /// Zero selects whitespace tokens; positive values select UTF-16 n-grams.
    pub ngram_size: usize,
    /// Used only by Monge-Elkan, with the inner metric's upstream defaults.
    pub inner_metric: MongeInnerMetric,
}

/// Compute an upstream token score for two non-null strings.
pub fn score(metric: TokenMetric, left: &str, right: &str, config: &TokenConfig) -> f64 {
    if metric == TokenMetric::MongeElkan {
        return monge_elkan(left, right, config);
    }

    // The five set metrics check raw strings before tokenization. In
    // particular, a nonempty whitespace-only string is not a raw empty string.
    if left.is_empty() && right.is_empty() {
        return 1.0;
    }
    if left.is_empty() || right.is_empty() {
        return 0.0;
    }

    let left_tokens = token_set(left, config.ngram_size);
    let right_tokens = token_set(right, config.ngram_size);
    let intersection = left_tokens.intersection(&right_tokens).count();
    let left_size = left_tokens.len();
    let right_size = right_tokens.len();

    let (numerator, denominator) = match metric {
        TokenMetric::Jaccard => (
            intersection as f64,
            (left_size + right_size - intersection) as f64,
        ),
        TokenMetric::Cosine => (
            intersection as f64,
            ((left_size as f64) * (right_size as f64)).sqrt(),
        ),
        TokenMetric::SorensenDice => (2.0 * (intersection as f64), (left_size + right_size) as f64),
        TokenMetric::OverlapCoefficient => (intersection as f64, left_size.min(right_size) as f64),
        TokenMetric::BraunBlanquet => (intersection as f64, left_size.max(right_size) as f64),
        TokenMetric::MongeElkan => unreachable!("Monge-Elkan returns before set scoring"),
    };
    if denominator == 0.0 {
        1.0
    } else {
        numerator / denominator
    }
}

/// Java's whitespace predicate on a UTF-16 code unit. In particular, NBSP,
/// figure space, narrow NBSP and NEL are not delimiters; U+001C..U+001F are.
fn java_whitespace(unit: u16) -> bool {
    matches!(
        unit,
        0x0009..=0x000D
            | 0x001C..=0x0020
            | 0x1680
            | 0x2000..=0x2006
            | 0x2008..=0x200A
            | 0x2028..=0x2029
            | 0x205F
            | 0x3000
    )
}

fn whitespace_tokens(value: &str) -> Vec<Vec<u16>> {
    let units: Vec<u16> = value.encode_utf16().collect();
    units
        .split(|unit| java_whitespace(*unit))
        .filter(|token| !token.is_empty())
        .map(<[u16]>::to_vec)
        .collect()
}

fn token_set(value: &str, ngram_size: usize) -> BTreeSet<Vec<u16>> {
    if ngram_size == 0 {
        return whitespace_tokens(value).into_iter().collect();
    }

    let units: Vec<u16> = value.encode_utf16().collect();
    if units.is_empty() {
        BTreeSet::new()
    } else if units.len() < ngram_size {
        BTreeSet::from([units])
    } else {
        units.windows(ngram_size).map(<[u16]>::to_vec).collect()
    }
}

/// Match Java String.getBytes(UTF_8), used by UTF8String.fromString in Monge:
/// malformed UTF-16 (possible after an n-gram splits a surrogate pair) becomes
/// ASCII '?'. Replacing it with U+FFFD would change the inner metric's score.
fn java_utf8_roundtrip(units: &[u16]) -> String {
    char::decode_utf16(units.iter().copied())
        .map(|result| result.unwrap_or('?'))
        .collect()
}

fn monge_tokens(value: &str, ngram_size: usize) -> Vec<String> {
    let tokens = if ngram_size == 0 {
        // Sequence tokenization retains repetitions and input order.
        whitespace_tokens(value)
    } else {
        // N-gram mode removes repetitions. A fixed UTF-16 order makes
        // floating reduction deterministic; Java HashSet does not expose an
        // ordering contract, so cross-runtime scores use a numeric tolerance.
        token_set(value, ngram_size).into_iter().collect()
    };
    tokens
        .iter()
        .map(|token| java_utf8_roundtrip(token))
        .collect()
}

fn directed_similarity(left: &[String], right: &[String], metric: MatrixMetric) -> f64 {
    let mut sum_best = 0.0;
    for left_token in left {
        let mut best = 0.0;
        for right_token in right {
            let candidate = matrix::score(metric, left_token, right_token);
            if candidate > best {
                best = candidate;
            }
        }
        sum_best += best;
    }
    sum_best / (left.len() as f64)
}

fn monge_elkan(left: &str, right: &str, config: &TokenConfig) -> f64 {
    let left_tokens = monge_tokens(left, config.ngram_size);
    let right_tokens = monge_tokens(right, config.ngram_size);
    if left_tokens.is_empty() && right_tokens.is_empty() {
        return 1.0;
    }
    if left_tokens.is_empty() || right_tokens.is_empty() {
        return 0.0;
    }

    let metric = config.inner_metric.matrix_metric();
    let raw_score = (directed_similarity(&left_tokens, &right_tokens, metric)
        + directed_similarity(&right_tokens, &left_tokens, metric))
        / 2.0;
    raw_score.clamp(0.0, 1.0)
}

#[cfg(test)]
#[path = "token_tests.rs"]
mod tests;
