//! Matrix metrics ported from spark-second-string a35db39fa8e9b65db2d201a45b86d11a6ca34b98.
//!
//! Java `String` indexing is by UTF-16 code unit, including inside surrogate pairs.
//! Parameter validation belongs to the SQL call boundary, as in the Scala expressions.

use std::mem;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum MatrixMetric {
    Levenshtein,
    LcsSimilarity,
    Jaro,
    JaroWinkler,
    NeedlemanWunsch,
    SmithWaterman,
    AffineGap,
}

#[derive(Clone, Copy, Debug, PartialEq)]
pub struct JaroWinklerParams {
    pub prefix_scale: f64,
    pub prefix_cap: usize,
}

impl Default for JaroWinklerParams {
    fn default() -> Self {
        Self {
            prefix_scale: 0.1,
            prefix_cap: 4,
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct AlignmentParams {
    pub match_score: i32,
    pub mismatch_penalty: i32,
    pub gap_penalty: i32,
}

impl AlignmentParams {
    pub const NEEDLEMAN_WUNSCH: Self = Self {
        match_score: 1,
        mismatch_penalty: -1,
        gap_penalty: -1,
    };
    pub const SMITH_WATERMAN: Self = Self {
        match_score: 2,
        mismatch_penalty: -1,
        gap_penalty: -1,
    };
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct AffineGapParams {
    pub mismatch_penalty: i32,
    pub gap_open_penalty: i32,
    pub gap_extend_penalty: i32,
}

impl Default for AffineGapParams {
    fn default() -> Self {
        Self {
            mismatch_penalty: -1,
            gap_open_penalty: -2,
            gap_extend_penalty: -1,
        }
    }
}

pub fn score(metric: MatrixMetric, left: &str, right: &str) -> f64 {
    match metric {
        MatrixMetric::Levenshtein => levenshtein(left, right),
        MatrixMetric::LcsSimilarity => lcs_similarity(left, right),
        MatrixMetric::Jaro => jaro(left, right),
        MatrixMetric::JaroWinkler => jaro_winkler(left, right),
        MatrixMetric::NeedlemanWunsch => needleman_wunsch(left, right),
        MatrixMetric::SmithWaterman => smith_waterman(left, right),
        MatrixMetric::AffineGap => affine_gap(left, right),
    }
}

fn resolve(left: &str, right: &str) -> (Vec<u16>, Vec<u16>) {
    (
        left.encode_utf16().collect(),
        right.encode_utf16().collect(),
    )
}

fn boundary(left: usize, right: usize) -> Option<f64> {
    match (left, right) {
        (0, 0) => Some(1.0),
        (0, _) | (_, 0) => Some(0.0),
        _ => None,
    }
}

fn clamp(value: f64) -> f64 {
    // These comparisons deliberately preserve NaN, like the Scala helper.
    value.clamp(0.0, 1.0)
}

fn normalize_distance(distance: i32, left: usize, right: usize) -> f64 {
    let maximum = left.max(right);
    if maximum == 0 {
        1.0
    } else {
        clamp(1.0 - f64::from(distance) / maximum as f64)
    }
}

pub fn levenshtein(left: &str, right: &str) -> f64 {
    let (left, right) = resolve(left, right);
    if let Some(result) = boundary(left.len(), right.len()) {
        return result;
    }
    let mut previous: Vec<i32> = (0..=right.len()).map(|index| index as i32).collect();
    let mut current = vec![0_i32; right.len() + 1];
    for (i, left_unit) in left.iter().enumerate() {
        current[0] = (i + 1) as i32;
        for (j, right_unit) in right.iter().enumerate() {
            let substitution = i32::from(left_unit != right_unit);
            current[j + 1] = previous[j + 1]
                .wrapping_add(1)
                .min(current[j].wrapping_add(1))
                .min(previous[j].wrapping_add(substitution));
        }
        mem::swap(&mut previous, &mut current);
    }
    normalize_distance(previous[right.len()], left.len(), right.len())
}

pub fn lcs_similarity(left: &str, right: &str) -> f64 {
    let (left, right) = resolve(left, right);
    if let Some(result) = boundary(left.len(), right.len()) {
        return result;
    }
    let mut previous = vec![0_i32; right.len() + 1];
    let mut current = vec![0_i32; right.len() + 1];
    for left_unit in &left {
        current[0] = 0;
        for (j, right_unit) in right.iter().enumerate() {
            current[j + 1] = if left_unit == right_unit {
                previous[j].wrapping_add(1)
            } else {
                previous[j + 1].max(current[j])
            };
        }
        mem::swap(&mut previous, &mut current);
    }
    clamp(f64::from(previous[right.len()]) / left.len().max(right.len()) as f64)
}

pub fn jaro(left: &str, right: &str) -> f64 {
    let (left, right) = resolve(left, right);
    jaro_units(&left, &right)
}

fn jaro_units(left: &[u16], right: &[u16]) -> f64 {
    if let Some(result) = boundary(left.len(), right.len()) {
        return result;
    }
    let radius = (left.len().max(right.len()) / 2).saturating_sub(1);
    let mut left_matched = vec![false; left.len()];
    let mut right_matched = vec![false; right.len()];
    let mut matches = 0_usize;
    for (i, left_unit) in left.iter().enumerate() {
        let start = i.saturating_sub(radius);
        let end = (i + radius + 1).min(right.len());
        for j in start..end {
            if !right_matched[j] && *left_unit == right[j] {
                left_matched[i] = true;
                right_matched[j] = true;
                matches += 1;
                break;
            }
        }
    }
    if matches == 0 {
        return 0.0;
    }
    let mut right_cursor = 0_usize;
    let mut transpositions = 0_usize;
    for (i, left_unit) in left.iter().enumerate() {
        if left_matched[i] {
            while !right_matched[right_cursor] {
                right_cursor += 1;
            }
            if *left_unit != right[right_cursor] {
                transpositions += 1;
            }
            right_cursor += 1;
        }
    }
    let matches = matches as f64;
    let transpositions = transpositions as f64 / 2.0;
    clamp(
        ((matches / left.len() as f64)
            + (matches / right.len() as f64)
            + ((matches - transpositions) / matches))
            / 3.0,
    )
}

pub fn jaro_winkler(left: &str, right: &str) -> f64 {
    jaro_winkler_with_params(left, right, JaroWinklerParams::default())
}

pub fn jaro_winkler_with_params(left: &str, right: &str, params: JaroWinklerParams) -> f64 {
    let (left, right) = resolve(left, right);
    if let Some(result) = boundary(left.len(), right.len()) {
        return result;
    }
    let jaro = jaro_units(&left, &right);
    if jaro <= 0.0 {
        return 0.0;
    }
    let prefix = left
        .iter()
        .zip(&right)
        .take(params.prefix_cap)
        .take_while(|(left_unit, right_unit)| left_unit == right_unit)
        .count();
    clamp(jaro + prefix as f64 * params.prefix_scale * (1.0 - jaro))
}

pub fn needleman_wunsch(left: &str, right: &str) -> f64 {
    needleman_wunsch_with_params(left, right, AlignmentParams::NEEDLEMAN_WUNSCH)
}

pub fn needleman_wunsch_with_params(left: &str, right: &str, params: AlignmentParams) -> f64 {
    let (left, right) = resolve(left, right);
    if let Some(result) = boundary(left.len(), right.len()) {
        return result;
    }
    let mut previous: Vec<i32> = (0..=right.len())
        .map(|index| (index as i32).wrapping_mul(params.gap_penalty))
        .collect();
    let mut current = vec![0_i32; right.len() + 1];
    for (i, left_unit) in left.iter().enumerate() {
        current[0] = ((i + 1) as i32).wrapping_mul(params.gap_penalty);
        for (j, right_unit) in right.iter().enumerate() {
            let substitution = if left_unit == right_unit {
                params.match_score
            } else {
                params.mismatch_penalty
            };
            current[j + 1] = previous[j]
                .wrapping_add(substitution)
                .max(previous[j + 1].wrapping_add(params.gap_penalty))
                .max(current[j].wrapping_add(params.gap_penalty));
        }
        mem::swap(&mut previous, &mut current);
    }
    let maximum = left.len().max(right.len()) as f64;
    // The upstream normalization intentionally does not scale with match_score.
    clamp((f64::from(previous[right.len()]) + maximum) / (2.0 * maximum))
}

pub fn smith_waterman(left: &str, right: &str) -> f64 {
    smith_waterman_with_params(left, right, AlignmentParams::SMITH_WATERMAN)
}

pub fn smith_waterman_with_params(left: &str, right: &str, params: AlignmentParams) -> f64 {
    let (left, right) = resolve(left, right);
    if let Some(result) = boundary(left.len(), right.len()) {
        return result;
    }
    let mut previous = vec![0_i32; right.len() + 1];
    let mut current = vec![0_i32; right.len() + 1];
    let mut best = 0_i32;
    for left_unit in &left {
        current[0] = 0;
        for (j, right_unit) in right.iter().enumerate() {
            let substitution = if left_unit == right_unit {
                params.match_score
            } else {
                params.mismatch_penalty
            };
            let cell = 0
                .max(previous[j].wrapping_add(substitution))
                .max(previous[j + 1].wrapping_add(params.gap_penalty))
                .max(current[j].wrapping_add(params.gap_penalty));
            current[j + 1] = cell;
            best = best.max(cell);
        }
        mem::swap(&mut previous, &mut current);
    }
    let maximum = f64::from(params.match_score) * left.len().min(right.len()) as f64;
    if maximum <= 0.0 {
        1.0
    } else {
        clamp(f64::from(best) / maximum)
    }
}

pub fn affine_gap(left: &str, right: &str) -> f64 {
    affine_gap_with_params(left, right, AffineGapParams::default())
}

const AFFINE_INFINITY: i32 = i32::MAX / 4;

fn affine_plus(value: i32, increment: i32) -> i32 {
    if value >= AFFINE_INFINITY.wrapping_sub(increment) {
        AFFINE_INFINITY
    } else {
        value.wrapping_add(increment)
    }
}

fn gap_cost(length: usize, open: i32, extend: i32) -> i32 {
    open.wrapping_add((length as i32).wrapping_mul(extend))
}

pub fn affine_gap_with_params(left: &str, right: &str, params: AffineGapParams) -> f64 {
    let (left, right) = resolve(left, right);
    if let Some(result) = boundary(left.len(), right.len()) {
        return result;
    }
    // Scala Int arithmetic wraps, including negation of Int.MinValue.
    let mismatch = params.mismatch_penalty.wrapping_neg();
    let open = params.gap_open_penalty.wrapping_neg();
    let extend = params.gap_extend_penalty.wrapping_neg();
    let open_and_extend = open.wrapping_add(extend);
    let mut previous_match = vec![AFFINE_INFINITY; right.len() + 1];
    let mut current_match = vec![AFFINE_INFINITY; right.len() + 1];
    let mut previous_left = vec![AFFINE_INFINITY; right.len() + 1];
    let mut current_left = vec![AFFINE_INFINITY; right.len() + 1];
    let mut previous_right = vec![AFFINE_INFINITY; right.len() + 1];
    let mut current_right = vec![AFFINE_INFINITY; right.len() + 1];
    previous_match[0] = 0;
    for (j, cell) in previous_left.iter_mut().enumerate().skip(1) {
        *cell = gap_cost(j, open, extend);
    }
    for (i, left_unit) in left.iter().enumerate() {
        current_match[0] = AFFINE_INFINITY;
        current_left[0] = AFFINE_INFINITY;
        current_right[0] = gap_cost(i + 1, open, extend);
        for (j, right_unit) in right.iter().enumerate() {
            let substitution = if left_unit == right_unit { 0 } else { mismatch };
            current_match[j + 1] = affine_plus(
                previous_match[j]
                    .min(previous_left[j])
                    .min(previous_right[j]),
                substitution,
            );
            current_left[j + 1] = affine_plus(current_match[j], open_and_extend)
                .min(affine_plus(current_left[j], extend))
                .min(affine_plus(current_right[j], open_and_extend));
            current_right[j + 1] = affine_plus(previous_match[j + 1], open_and_extend)
                .min(affine_plus(previous_right[j + 1], extend))
                .min(affine_plus(previous_left[j + 1], open_and_extend));
        }
        mem::swap(&mut previous_match, &mut current_match);
        mem::swap(&mut previous_left, &mut current_left);
        mem::swap(&mut previous_right, &mut current_right);
    }
    let distance = previous_match[right.len()]
        .min(previous_left[right.len()])
        .min(previous_right[right.len()]);
    normalize_distance(distance, left.len(), right.len())
}

#[cfg(test)]
#[path = "matrix_tests.rs"]
mod tests;
