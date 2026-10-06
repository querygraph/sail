use super::{
    affine_gap, affine_gap_with_params, jaro, jaro_winkler, jaro_winkler_with_params,
    lcs_similarity, levenshtein, needleman_wunsch, needleman_wunsch_with_params, score,
    smith_waterman, smith_waterman_with_params, AffineGapParams, AlignmentParams,
    JaroWinklerParams, MatrixMetric,
};

const METRICS: [MatrixMetric; 7] = [
    MatrixMetric::Levenshtein,
    MatrixMetric::LcsSimilarity,
    MatrixMetric::Jaro,
    MatrixMetric::JaroWinkler,
    MatrixMetric::NeedlemanWunsch,
    MatrixMetric::SmithWaterman,
    MatrixMetric::AffineGap,
];

fn close(actual: f64, expected: f64) {
    assert!(
        (actual - expected).abs() <= 1e-12,
        "actual {actual}, expected {expected}"
    );
}

#[test]
fn shared_empty_boundaries_and_identical_strings() {
    for metric in METRICS {
        assert_eq!(score(metric, "", ""), 1.0);
        assert_eq!(score(metric, "", "spark"), 0.0);
        assert_eq!(score(metric, "spark", ""), 0.0);
        assert_eq!(score(metric, "spark", "spark"), 1.0);
        assert_eq!(score(metric, "東京😀", "東京😀"), 1.0);
    }
}

#[test]
fn upstream_levenshtein_examples() {
    close(levenshtein("spark", "spork"), 0.8);
    close(levenshtein("kitten", "sitting"), 1.0 - 3.0 / 7.0);
    close(levenshtein("ab", "abc"), 1.0 - 1.0 / 3.0);
}

#[test]
fn upstream_lcs_is_subsequence_normalized_by_longer_input() {
    close(lcs_similarity("abcde", "axxxx"), 1.0 / 5.0);
    close(lcs_similarity("abcd", "dcba"), 0.25);
    close(lcs_similarity("abcdef", "ace"), 0.5);
    close(lcs_similarity("ace", "abcdef"), 0.5);
}

#[test]
fn upstream_jaro_examples_and_repeated_matches() {
    close(jaro("martha", "marhta"), 17.0 / 18.0);
    close(jaro("dwayne", "duane"), 37.0 / 45.0);
    close(jaro("aaaa", "aaab"), 5.0 / 6.0);
    close(jaro("abcd", "abc"), 11.0 / 12.0);
    assert_eq!(jaro("abc", "xyz"), 0.0);
}

#[test]
fn jaro_uses_half_a_transposition_for_an_odd_mismatch_count() {
    // The matched sequences abcdef/bcadef differ at three positions. Scala
    // divides that count by 2.0, rather than truncating it with integer division.
    close(jaro("abcdef", "bcadef"), 11.0 / 12.0);
}

#[test]
fn upstream_jaro_winkler_examples() {
    close(jaro_winkler("martha", "marhta"), 0.9611111111111111);
    close(jaro_winkler("dwayne", "duane"), 0.8400000000000001);
    close(jaro_winkler("aaaa", "aaab"), 0.8833333333333333);
    assert_eq!(jaro_winkler("abc", "xyz"), 0.0);
}

#[test]
fn winkler_boost_has_no_point_seven_threshold() {
    close(jaro("ab", "ac"), 2.0 / 3.0);
    close(jaro_winkler("ab", "ac"), 0.7);
}

#[test]
fn winkler_configured_prefix_and_final_clamp() {
    close(
        jaro_winkler_with_params(
            "martha",
            "marhta",
            JaroWinklerParams {
                prefix_scale: 0.2,
                prefix_cap: 6,
            },
        ),
        44.0 / 45.0,
    );
    assert_eq!(
        jaro_winkler_with_params(
            "abcdefghijx",
            "abcdefghijy",
            JaroWinklerParams {
                prefix_scale: 0.25,
                prefix_cap: 10,
            },
        ),
        1.0
    );
}

#[test]
fn winkler_nan_arithmetic_matches_source_comparisons() {
    let params = JaroWinklerParams {
        prefix_scale: f64::NAN,
        prefix_cap: 4,
    };
    assert!(jaro_winkler_with_params("same", "same", params).is_nan());
    assert!(jaro_winkler_with_params("ab", "ac", params).is_nan());
    assert_eq!(jaro_winkler_with_params("ab", "xy", params), 0.0);
    assert_eq!(jaro_winkler_with_params("", "", params), 1.0);
}

#[test]
fn upstream_global_alignment_examples() {
    assert_eq!(needleman_wunsch("abc", "xyz"), 0.0);
    close(needleman_wunsch("aaaa", "aaab"), 0.75);
    close(needleman_wunsch("abcdef", "abc"), 0.5);
}

#[test]
fn global_alignment_config_keeps_fixed_source_normalization() {
    let params = AlignmentParams {
        match_score: 2,
        mismatch_penalty: -2,
        gap_penalty: -1,
    };
    // Global score 2 - 1 = 1, normalized by (score + max_length)/(2*max_length).
    close(needleman_wunsch_with_params("a", "ab", params), 0.75);
    assert_eq!(needleman_wunsch_with_params("a", "a", params), 1.0);
    assert_eq!(needleman_wunsch_with_params("a", "z", params), 0.0);
}

#[test]
fn upstream_local_alignment_examples() {
    close(smith_waterman("ACACACTA", "AGCACACA"), 0.75);
    assert_eq!(smith_waterman("abc", "xabcx"), 1.0);
    assert_eq!(smith_waterman("abc", "xyz"), 0.0);
}

#[test]
fn local_alignment_zero_penalties_and_configured_match_scale() {
    let free_gaps = AlignmentParams {
        match_score: 3,
        mismatch_penalty: 0,
        gap_penalty: 0,
    };
    assert_eq!(smith_waterman_with_params("ab", "axxb", free_gaps), 1.0);
    let costly_gaps = AlignmentParams {
        match_score: 3,
        mismatch_penalty: -1,
        gap_penalty: -2,
    };
    close(smith_waterman_with_params("ab", "axxb", costly_gaps), 0.5);
}

#[test]
fn affine_gap_charges_opening_and_every_extension() {
    // One dropped suffix costs 2 + 1*1 = 3, so a/ab clamps to zero.
    assert_eq!(affine_gap("a", "ab"), 0.0);
    // Three dropped suffix units cost 2 + 3*1 = 5.
    close(affine_gap("abcdef", "abc"), 1.0 / 6.0);
    close(affine_gap("aaaa", "aaab"), 0.75);
    let params = AffineGapParams {
        mismatch_penalty: -2,
        gap_open_penalty: -3,
        gap_extend_penalty: -2,
    };
    close(affine_gap_with_params("aa", "ab", params), 0.0);
    assert_eq!(affine_gap_with_params("a", "ab", params), 0.0);
}

#[test]
fn explicit_default_parameters_match_default_entry_points() {
    for (left, right) in [("spark", "spork"), ("😀", "😁"), ("", "abc")] {
        assert_eq!(
            jaro_winkler(left, right),
            jaro_winkler_with_params(left, right, JaroWinklerParams::default())
        );
        assert_eq!(
            needleman_wunsch(left, right),
            needleman_wunsch_with_params(left, right, AlignmentParams::NEEDLEMAN_WUNSCH)
        );
        assert_eq!(
            smith_waterman(left, right),
            smith_waterman_with_params(left, right, AlignmentParams::SMITH_WATERMAN)
        );
        assert_eq!(
            affine_gap(left, right),
            affine_gap_with_params(left, right, AffineGapParams::default())
        );
    }
}

#[test]
fn supplementary_characters_are_two_utf16_units() {
    // These emoji share the high surrogate and differ only in the low surrogate.
    let expected = [0.5, 0.5, 2.0 / 3.0, 0.7, 0.5, 0.5, 0.5];
    for (metric, expected) in METRICS.into_iter().zip(expected) {
        close(score(metric, "😀", "😁"), expected);
    }
    close(levenshtein("😀", "😀a"), 2.0 / 3.0);
    close(lcs_similarity("😀", "😀a"), 2.0 / 3.0);
}

#[test]
fn no_unicode_normalization_or_case_folding() {
    close(levenshtein("cafe\u{0301}", "café"), 0.6);
    close(lcs_similarity("cafe\u{0301}", "café"), 0.6);
    close(levenshtein("東京", "大阪"), 0.0);
    close(levenshtein("Spark", "spark"), 0.8);
    close(levenshtein("a\u{200b}b", "ab"), 2.0 / 3.0);
}

#[test]
fn source_unicode_fixtures_are_bounded_and_repeatable() {
    let fixtures = [
        ("cafe", "cafe"),
        ("café", "cafe"),
        ("cafe\u{0301}", "café"),
        ("résumé", "resume"),
        ("Müller", "Mueller"),
        ("東京", "大阪"),
        ("東京 都", "東京"),
        ("hello 世界", "hello world"),
        ("John Смит", "John Smith"),
        ("alpha\u{200b}beta", "alpha beta"),
        ("alpha\u{2060}beta", "alpha beta"),
        ("北京大学", "北京大学院"),
    ];
    for metric in METRICS {
        for (left, right) in fixtures {
            let actual = score(metric, left, right);
            assert!((0.0..=1.0).contains(&actual));
            assert_eq!(actual, score(metric, left, right));
        }
    }
}

#[test]
fn custom_int_arithmetic_wraps_as_java_int() {
    let params = AlignmentParams {
        match_score: 1,
        mismatch_penalty: -1,
        gap_penalty: i32::MIN,
    };
    // In the global recurrence, two gap penalties wrap to zero. Saturating
    // arithmetic would instead pick a mismatch and produce zero similarity.
    close(needleman_wunsch_with_params("a", "b", params), 0.5);
    assert_eq!(smith_waterman_with_params("a", "b", params), 0.0);
    let affine = AffineGapParams {
        mismatch_penalty: i32::MIN,
        gap_open_penalty: -2,
        gap_extend_penalty: -1,
    };
    // Negating Int.MinValue wraps back to Int.MinValue. The upstream safePlus
    // threshold also wraps, so it selects Infinity rather than a negative cost.
    assert_eq!(affine_gap_with_params("a", "b", affine), 0.0);
}

#[test]
fn configured_empty_boundaries_precede_alignment_arithmetic() {
    let params = AlignmentParams {
        match_score: i32::MAX,
        mismatch_penalty: i32::MIN,
        gap_penalty: i32::MIN,
    };
    assert_eq!(needleman_wunsch_with_params("", "", params), 1.0);
    assert_eq!(needleman_wunsch_with_params("", "a", params), 0.0);
    assert_eq!(smith_waterman_with_params("a", "", params), 0.0);
    assert_eq!(
        affine_gap_with_params(
            "",
            "a",
            AffineGapParams {
                mismatch_penalty: i32::MIN,
                gap_open_penalty: i32::MIN,
                gap_extend_penalty: i32::MIN,
            },
        ),
        0.0
    );
}
