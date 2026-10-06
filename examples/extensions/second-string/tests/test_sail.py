"""Live native tests using a root-provided external Spark Connect session fixture."""

from dataclasses import dataclass

import pytest
from pyspark.sql.connect import functions as F
from pyspark.sql.connect.session import SparkSession
from sail_second_string import functions as ss


@dataclass(frozen=True, slots=True)
class SqlCase:
    expression: str
    expected: float | str


DEFAULT_CASES = [
    SqlCase("ss_jaccard('a b', 'b c')", 1.0 / 3.0),
    SqlCase("ss_sorensen_dice('a b', 'b c')", 0.5),
    SqlCase("ss_overlap_coefficient('a b', 'b c')", 0.5),
    SqlCase("ss_cosine('a b', 'b c')", 0.5),
    SqlCase("ss_braun_blanquet('a b', 'b c')", 0.5),
    SqlCase("ss_monge_elkan('same', 'same')", 1.0),
    SqlCase("ss_levenshtein('kitten', 'sitting')", 1.0 - 3.0 / 7.0),
    SqlCase("ss_lcs_similarity('abcdef', 'ace')", 0.5),
    SqlCase("ss_jaro('martha', 'marhta')", 17.0 / 18.0),
    SqlCase("ss_jaro_winkler('martha', 'marhta')", 0.9611111111111111),
    SqlCase("ss_needleman_wunsch('aaaa', 'aaab')", 0.75),
    SqlCase("ss_smith_waterman('abc', 'xabcx')", 1.0),
    SqlCase("ss_affine_gap('abcdef', 'abc')", 1.0 / 6.0),
    SqlCase("ss_soundex('Robert')", "R163"),
    SqlCase("ss_refined_soundex('Robert')", "R01093"),
    SqlCase("ss_double_metaphone('testing')", "TSTN"),
]

CONFIGURED_SQL_CASES = [
    SqlCase("ss_jaccard_with_options('abc', 'abd', 2)", 1.0 / 3.0),
    SqlCase("ss_sorensen_dice_with_options('abc', 'abd', 2)", 0.5),
    SqlCase("ss_overlap_coefficient_with_options('abc', 'abd', 2)", 0.5),
    SqlCase("ss_cosine_with_options('abc', 'abd', 2)", 0.5),
    SqlCase("ss_braun_blanquet_with_options('abc', 'abd', 2)", 0.5),
    SqlCase("ss_monge_elkan_with_options('ab ab', 'ab ac', 'levenshtein', 0)", 0.875),
    SqlCase("ss_jaro_winkler_with_options('martha', 'marhta', 0.2, 6)", 44.0 / 45.0),
    SqlCase("ss_needleman_wunsch_with_options('a', 'ab', 2, -2, -1)", 0.75),
    SqlCase("ss_smith_waterman_with_options('ab', 'axxb', 3, -1, -2)", 0.5),
    SqlCase("ss_affine_gap_with_options('aa', 'ab', -2, -2, -1)", 0.0),
]


@pytest.mark.parametrize("case", DEFAULT_CASES)
def test_original_sql_names_use_upstream_defaults(spark: SparkSession, case: SqlCase) -> None:
    actual = spark.sql(f"SELECT {case.expression} AS result").collect()[0]["result"]
    if isinstance(case.expected, float):
        assert actual == pytest.approx(case.expected, abs=1e-12)
    else:
        assert actual == case.expected


@pytest.mark.parametrize("case", CONFIGURED_SQL_CASES)
def test_configured_sql_accepts_ordinary_numeric_literals(
    spark: SparkSession, case: SqlCase
) -> None:
    actual = spark.sql(f"SELECT {case.expression} AS result").collect()[0]["result"]
    assert actual == pytest.approx(case.expected, abs=1e-12)


@pytest.mark.parametrize("case", DEFAULT_CASES)
def test_all_native_defaults_propagate_sql_nulls(spark: SparkSession, case: SqlCase) -> None:
    name = case.expression.split("(", 1)[0]
    arguments = (
        "CAST(NULL AS STRING)"
        if "soundex" in name or "metaphone" in name
        else "CAST(NULL AS STRING), 'text'"
    )
    assert spark.sql(f"SELECT {name}({arguments}) AS result").collect()[0]["result"] is None


def test_configurable_python_helpers_and_utf16_scores(spark: SparkSession) -> None:
    row = (
        spark.range(1)
        .select(
            ss.jaccard(F.lit("abc"), F.lit("abd"), ngram_size=2).alias("jaccard"),
            ss.sorensen_dice(F.lit("abc"), F.lit("abd"), ngram_size=2).alias("dice"),
            ss.overlap_coefficient(F.lit("abc"), F.lit("abd"), ngram_size=2).alias("overlap"),
            ss.cosine(F.lit("abc"), F.lit("abd"), ngram_size=2).alias("cosine"),
            ss.braun_blanquet(F.lit("abc"), F.lit("abd"), ngram_size=2).alias("braun"),
            ss.monge_elkan(F.lit("ab ab"), F.lit("ab ac"), inner_metric="levenshtein").alias(
                "monge"
            ),
            ss.jaro_winkler(F.lit("martha"), F.lit("marhta"), prefix_scale=0.2, prefix_cap=6).alias(
                "winkler"
            ),
            ss.needleman_wunsch(F.lit("a"), F.lit("ab"), match_score=2, mismatch_penalty=-2).alias(
                "global"
            ),
            ss.smith_waterman(F.lit("ab"), F.lit("axxb"), match_score=3, gap_penalty=-2).alias(
                "local"
            ),
            ss.affine_gap(F.lit("aa"), F.lit("ab"), mismatch_penalty=-2).alias("affine"),
            ss.levenshtein(F.lit("😀"), F.lit("😁")).alias("utf16"),
        )
        .collect()[0]
    )
    expected = {
        "jaccard": 1.0 / 3.0,
        "dice": 0.5,
        "overlap": 0.5,
        "cosine": 0.5,
        "braun": 0.5,
        "monge": 0.875,
        "winkler": 44.0 / 45.0,
        "global": 0.75,
        "local": 0.5,
        "affine": 0.0,
        "utf16": 0.5,
    }
    for name, value in expected.items():
        assert row[name] == pytest.approx(value, abs=1e-12)


def test_native_helpers_after_join_and_shuffle(spark: SparkSession) -> None:
    left = (
        spark.range(64)
        .select((F.col("id") % 4).alias("key"), F.lit("martha").alias("left"))
        .repartition(4, "key")
    )
    right = (
        spark.range(4)
        .select(F.col("id").alias("key"), F.lit("marhta").alias("right"))
        .repartition(4, "key")
    )
    rows = left.join(right, "key").select(ss.jaro_winkler("left", "right").alias("score")).collect()
    assert len(rows) == 64
    assert all(row["score"] == pytest.approx(0.9611111111111111, abs=1e-12) for row in rows)


def test_explain_has_native_function_without_python_udf(
    spark: SparkSession, capsys: pytest.CaptureFixture[str]
) -> None:
    frame = spark.createDataFrame(
        [("martha", "marhta"), ("dwayne", "duane")], "left STRING, right STRING"
    ).select(ss.jaro_winkler("left", "right"))
    frame.explain(extended=True)
    explanation = capsys.readouterr().out.lower()
    assert "ss_jaro_winkler" in explanation
    for python_node in ("pythonudf", "batchevalpython", "arrowevalpython", "evalpython"):
        assert python_node not in explanation


@pytest.mark.parametrize(
    "expression",
    ["ss_jaccard('a')", "ss_soundex('a', 'b')", "ss_jaro_winkler('a', 'b', 0.1)"],
)
def test_original_sql_arity_is_not_changed(spark: SparkSession, expression: str) -> None:
    with pytest.raises(Exception, match="(?i)(argument|arity|signature|expects)"):
        spark.sql(f"SELECT {expression}").collect()


def test_configurable_options_must_be_literals(spark: SparkSession) -> None:
    query = """
        SELECT ss_jaccard_with_options(left, right, n)
        FROM VALUES ('abc', 'abd', 1), ('abc', 'abd', 2)
        AS t(left, right, n)
    """
    with pytest.raises(Exception, match="(?i)literal"):
        spark.sql(query).collect()


@pytest.mark.parametrize(
    "expression",
    [
        "ss_jaccard_with_options('a', 'a', -1)",
        "ss_jaro_winkler_with_options('a', 'a', 0.0, 4)",
        "ss_needleman_wunsch_with_options('a', 'a', 1, 0, -1)",
        "ss_smith_waterman_with_options('a', 'a', 2, 1, -1)",
        "ss_affine_gap_with_options('a', 'a', -1, 0, -1)",
        "ss_monge_elkan_with_options('a', 'a', 'affine_gap', 0)",
    ],
)
def test_sql_option_domains_are_validated(spark: SparkSession, expression: str) -> None:
    with pytest.raises(Exception, match="(?i)(ngram|prefix|scoring|penalt|inner metric)"):
        spark.sql(f"SELECT {expression}").collect()
