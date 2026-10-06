"""Spark Connect expressions evaluated by native Sail scalar functions.

Strings name columns; use ``pyspark.sql.connect.functions.lit`` for string
values. Options are Python values validated before constructing an expression.
Default calls keep the sixteen original upstream SQL names and arities.
"""

from typing import Literal

from pyspark.sql import Column
from pyspark.sql.connect import functions as F

type ColumnInput = str | Column
type MongeInnerMetric = Literal[
    "jaro_winkler", "jaro", "levenshtein", "needleman_wunsch", "smith_waterman"
]

_INNER_METRICS = frozenset(
    ("jaro_winkler", "jaro", "levenshtein", "needleman_wunsch", "smith_waterman")
)


def _int32(name: str, value: int) -> int:
    if type(value) is not int:
        raise TypeError(f"{name} must be an integer")
    if not -(2**31) <= value < 2**31:
        raise ValueError(f"{name} must fit a Scala Int")
    return value


def _ngram(value: int) -> int:
    value = _int32("ngram_size", value)
    if value < 0:
        raise ValueError("ngram_size must be >= 0")
    return value


def _integer_literal(value: int) -> Column:
    return F.lit(value)


def _token(name: str, left: ColumnInput, right: ColumnInput, ngram_size: int) -> Column:
    size = _ngram(ngram_size)
    if size == 0:
        return F.call_function(name, left, right)
    return F.call_function(f"{name}_with_options", left, right, _integer_literal(size))


def jaccard(left: ColumnInput, right: ColumnInput, *, ngram_size: int = 0) -> Column:
    return _token("ss_jaccard", left, right, ngram_size)


def sorensen_dice(left: ColumnInput, right: ColumnInput, *, ngram_size: int = 0) -> Column:
    return _token("ss_sorensen_dice", left, right, ngram_size)


def overlap_coefficient(left: ColumnInput, right: ColumnInput, *, ngram_size: int = 0) -> Column:
    return _token("ss_overlap_coefficient", left, right, ngram_size)


def cosine(left: ColumnInput, right: ColumnInput, *, ngram_size: int = 0) -> Column:
    return _token("ss_cosine", left, right, ngram_size)


def braun_blanquet(left: ColumnInput, right: ColumnInput, *, ngram_size: int = 0) -> Column:
    return _token("ss_braun_blanquet", left, right, ngram_size)


def monge_elkan(
    left: ColumnInput,
    right: ColumnInput,
    *,
    inner_metric: MongeInnerMetric = "jaro_winkler",
    ngram_size: int = 0,
) -> Column:
    size = _ngram(ngram_size)
    if not isinstance(inner_metric, str) or inner_metric not in _INNER_METRICS:
        raise ValueError("inner_metric must name one of the five upstream Monge-Elkan metrics")
    if inner_metric == "jaro_winkler" and size == 0:
        return F.call_function("ss_monge_elkan", left, right)
    return F.call_function(
        "ss_monge_elkan_with_options", left, right, F.lit(inner_metric), _integer_literal(size)
    )


def levenshtein(left: ColumnInput, right: ColumnInput) -> Column:
    """Normalized Levenshtein similarity, rather than Spark's edit-distance integer."""
    return F.call_function("ss_levenshtein", left, right)


def lcs_similarity(left: ColumnInput, right: ColumnInput) -> Column:
    return F.call_function("ss_lcs_similarity", left, right)


def jaro(left: ColumnInput, right: ColumnInput) -> Column:
    return F.call_function("ss_jaro", left, right)


def jaro_winkler(
    left: ColumnInput,
    right: ColumnInput,
    *,
    prefix_scale: float = 0.1,
    prefix_cap: int = 4,
) -> Column:
    cap = _int32("prefix_cap", prefix_cap)
    if type(prefix_scale) not in (float, int):
        raise TypeError("prefix_scale must be a number")
    scale = float(prefix_scale)
    # Preserve the upstream constructor comparisons, including NaN acceptance.
    if scale <= 0.0 or scale > 0.25 or not 1 <= cap <= 10:
        raise ValueError("prefix_scale must be in (0, 0.25] and prefix_cap in [1, 10]")
    if scale == 0.1 and cap == 4:
        return F.call_function("ss_jaro_winkler", left, right)
    return F.call_function(
        "ss_jaro_winkler_with_options", left, right, F.lit(scale), _integer_literal(cap)
    )


def _alignment(
    name: str,
    left: ColumnInput,
    right: ColumnInput,
    match_score: int,
    mismatch_penalty: int,
    gap_penalty: int,
    *,
    local: bool,
) -> Column:
    match_score = _int32("match_score", match_score)
    mismatch_penalty = _int32("mismatch_penalty", mismatch_penalty)
    gap_penalty = _int32("gap_penalty", gap_penalty)
    valid_penalties = (
        mismatch_penalty <= 0 and gap_penalty <= 0
        if local
        else mismatch_penalty < 0 and gap_penalty < 0
    )
    if match_score <= 0 or not valid_penalties:
        raise ValueError("invalid alignment scoring parameters")
    defaults = (2 if local else 1, -1, -1)
    if (match_score, mismatch_penalty, gap_penalty) == defaults:
        return F.call_function(name, left, right)
    return F.call_function(
        f"{name}_with_options",
        left,
        right,
        _integer_literal(match_score),
        _integer_literal(mismatch_penalty),
        _integer_literal(gap_penalty),
    )


def needleman_wunsch(
    left: ColumnInput,
    right: ColumnInput,
    *,
    match_score: int = 1,
    mismatch_penalty: int = -1,
    gap_penalty: int = -1,
) -> Column:
    return _alignment(
        "ss_needleman_wunsch", left, right, match_score, mismatch_penalty, gap_penalty, local=False
    )


def smith_waterman(
    left: ColumnInput,
    right: ColumnInput,
    *,
    match_score: int = 2,
    mismatch_penalty: int = -1,
    gap_penalty: int = -1,
) -> Column:
    return _alignment(
        "ss_smith_waterman", left, right, match_score, mismatch_penalty, gap_penalty, local=True
    )


def affine_gap(
    left: ColumnInput,
    right: ColumnInput,
    *,
    mismatch_penalty: int = -1,
    gap_open_penalty: int = -2,
    gap_extend_penalty: int = -1,
) -> Column:
    mismatch_penalty = _int32("mismatch_penalty", mismatch_penalty)
    gap_open_penalty = _int32("gap_open_penalty", gap_open_penalty)
    gap_extend_penalty = _int32("gap_extend_penalty", gap_extend_penalty)
    if mismatch_penalty >= 0 or gap_open_penalty >= 0 or gap_extend_penalty >= 0:
        raise ValueError("affine gap penalties must be < 0")
    if (mismatch_penalty, gap_open_penalty, gap_extend_penalty) == (-1, -2, -1):
        return F.call_function("ss_affine_gap", left, right)
    return F.call_function(
        "ss_affine_gap_with_options",
        left,
        right,
        _integer_literal(mismatch_penalty),
        _integer_literal(gap_open_penalty),
        _integer_literal(gap_extend_penalty),
    )


def soundex(value: ColumnInput) -> Column:
    return F.call_function("ss_soundex", value)


def refined_soundex(value: ColumnInput) -> Column:
    return F.call_function("ss_refined_soundex", value)


def double_metaphone(value: ColumnInput) -> Column:
    return F.call_function("ss_double_metaphone", value)


# Scala DSL spellings share the same typed Python implementations and options.
sorensenDice = sorensen_dice
overlapCoefficient = overlap_coefficient
braunBlanquet = braun_blanquet
mongeElkan = monge_elkan
lcsSimilarity = lcs_similarity
jaroWinkler = jaro_winkler
needlemanWunsch = needleman_wunsch
smithWaterman = smith_waterman
affineGap = affine_gap
refinedSoundex = refined_soundex
doubleMetaphone = double_metaphone

__all__ = [
    "ColumnInput",
    "MongeInnerMetric",
    "affineGap",
    "affine_gap",
    "braunBlanquet",
    "braun_blanquet",
    "cosine",
    "doubleMetaphone",
    "double_metaphone",
    "jaccard",
    "jaro",
    "jaroWinkler",
    "jaro_winkler",
    "lcsSimilarity",
    "lcs_similarity",
    "levenshtein",
    "mongeElkan",
    "monge_elkan",
    "needlemanWunsch",
    "needleman_wunsch",
    "overlapCoefficient",
    "overlap_coefficient",
    "refinedSoundex",
    "refined_soundex",
    "smithWaterman",
    "smith_waterman",
    "sorensenDice",
    "sorensen_dice",
    "soundex",
]
