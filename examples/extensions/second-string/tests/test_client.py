"""Expression serialization and free option validation; no server is required."""

import importlib
import math
import sys
from collections.abc import Callable
from dataclasses import dataclass
from types import ModuleType
from typing import TYPE_CHECKING, cast

import pytest
from pyspark.sql import Column
from pyspark.sql.connect import functions as F
from pyspark.sql.connect.column import Column as ConnectColumn
from pyspark.sql.connect.expressions import CallFunction
from sail_second_string import ExtensionManifest, extension
from sail_second_string import functions as ss

if TYPE_CHECKING:
    from pyspark.sql.connect.client import SparkConnectClient


@dataclass(frozen=True, slots=True)
class ExpressionCase:
    column: Column
    name: str
    arity: int


DEFAULT_CASES = [
    ExpressionCase(ss.jaccard("left", "right"), "ss_jaccard", 2),
    ExpressionCase(ss.sorensen_dice("left", "right"), "ss_sorensen_dice", 2),
    ExpressionCase(ss.overlap_coefficient("left", "right"), "ss_overlap_coefficient", 2),
    ExpressionCase(ss.cosine("left", "right"), "ss_cosine", 2),
    ExpressionCase(ss.braun_blanquet("left", "right"), "ss_braun_blanquet", 2),
    ExpressionCase(ss.monge_elkan("left", "right"), "ss_monge_elkan", 2),
    ExpressionCase(ss.levenshtein("left", "right"), "ss_levenshtein", 2),
    ExpressionCase(ss.lcs_similarity("left", "right"), "ss_lcs_similarity", 2),
    ExpressionCase(ss.jaro("left", "right"), "ss_jaro", 2),
    ExpressionCase(ss.jaro_winkler("left", "right"), "ss_jaro_winkler", 2),
    ExpressionCase(ss.needleman_wunsch("left", "right"), "ss_needleman_wunsch", 2),
    ExpressionCase(ss.smith_waterman("left", "right"), "ss_smith_waterman", 2),
    ExpressionCase(ss.affine_gap("left", "right"), "ss_affine_gap", 2),
    ExpressionCase(ss.soundex("left"), "ss_soundex", 1),
    ExpressionCase(ss.refined_soundex("left"), "ss_refined_soundex", 1),
    ExpressionCase(ss.double_metaphone("left"), "ss_double_metaphone", 1),
]

CONFIGURED_CASES = [
    ExpressionCase(ss.jaccard("left", "right", ngram_size=2), "ss_jaccard_with_options", 3),
    ExpressionCase(
        ss.sorensen_dice("left", "right", ngram_size=2), "ss_sorensen_dice_with_options", 3
    ),
    ExpressionCase(
        ss.overlap_coefficient("left", "right", ngram_size=2),
        "ss_overlap_coefficient_with_options",
        3,
    ),
    ExpressionCase(ss.cosine("left", "right", ngram_size=2), "ss_cosine_with_options", 3),
    ExpressionCase(
        ss.braun_blanquet("left", "right", ngram_size=2), "ss_braun_blanquet_with_options", 3
    ),
    ExpressionCase(
        ss.monge_elkan("left", "right", inner_metric="levenshtein", ngram_size=2),
        "ss_monge_elkan_with_options",
        4,
    ),
    ExpressionCase(
        ss.jaro_winkler("left", "right", prefix_scale=0.2, prefix_cap=6),
        "ss_jaro_winkler_with_options",
        4,
    ),
    ExpressionCase(
        ss.needleman_wunsch("left", "right", match_score=2), "ss_needleman_wunsch_with_options", 5
    ),
    ExpressionCase(
        ss.smith_waterman("left", "right", match_score=3), "ss_smith_waterman_with_options", 5
    ),
    ExpressionCase(
        ss.affine_gap("left", "right", gap_open_penalty=-3), "ss_affine_gap_with_options", 5
    ),
]


@pytest.mark.parametrize("case", DEFAULT_CASES + CONFIGURED_CASES, ids=lambda case: case.name)
def test_helpers_serialize_as_named_native_function_calls(case: ExpressionCase) -> None:
    assert isinstance(case.column, ConnectColumn)
    assert isinstance(case.column._expr, CallFunction)
    plan = case.column._expr.to_plan(cast("SparkConnectClient", None))
    assert plan.WhichOneof("expr_type") == "call_function"
    function = plan.call_function
    assert function.function_name == case.name
    assert len(function.arguments) == case.arity
    for argument, name in zip(function.arguments[:2], ("left", "right"), strict=False):
        assert argument.unresolved_attribute.unparsed_identifier == name


def test_column_inputs_and_literal_values_stay_column_expressions() -> None:
    result = ss.levenshtein(F.col("left"), F.lit("spark"))
    assert isinstance(result, ConnectColumn)
    plan = result._expr.to_plan(cast("SparkConnectClient", None))
    arguments = plan.call_function.arguments
    assert arguments[0].unresolved_attribute.unparsed_identifier == "left"
    assert arguments[1].literal.string == "spark"


def test_custom_integer_options_are_scala_int_literals() -> None:
    result = ss.monge_elkan("left", "right", inner_metric="jaro", ngram_size=2)
    assert isinstance(result, ConnectColumn)
    plan = result._expr.to_plan(cast("SparkConnectClient", None))
    options = plan.call_function.arguments[2:]
    assert options[0].literal.string == "jaro"
    assert options[1].WhichOneof("expr_type") == "literal"
    assert options[1].literal.integer == 2


def test_manifest_is_available_without_loading_native(monkeypatch: pytest.MonkeyPatch) -> None:
    def forbidden_import(name: str, package: str | None = None) -> ModuleType:
        raise AssertionError(f"unexpected native import: {name}, {package}")

    monkeypatch.setattr(importlib, "import_module", forbidden_import)
    assert extension.manifest() == {
        "name": "second_string",
        "version": "0.1.0",
        "api_version": 1,
        "datafusion_version": "55.1.0",
        "arrow_version": "59.3.0",
        "placement": "any",
        "relation_types": [],
    }
    first = ExtensionManifest().to_wire()
    first["relation_types"] = ["mutated"]
    assert extension.manifest()["relation_types"] == []


def test_binding_imports_native_only_when_requested(monkeypatch: pytest.MonkeyPatch) -> None:
    class FakeBinding:
        def __init__(self, session_id: str) -> None:
            self.session_id = session_id

        def scalar_udfs(self) -> list[object]:
            return []

    class FakeNativeModule(ModuleType):
        BoundSecondString = FakeBinding

    native = FakeNativeModule("sail_second_string._native")
    monkeypatch.setitem(sys.modules, "sail_second_string._native", native)
    binding = extension.bind("session-42")
    assert binding.session_id == "session-42"
    assert binding.scalar_udfs() == []


@dataclass(frozen=True, slots=True)
class InvalidCase:
    helper: Callable[..., Column]
    options: dict[str, object]
    error: type[Exception] = ValueError


INVALID_CASES = [
    InvalidCase(ss.jaccard, {"ngram_size": -1}),
    InvalidCase(ss.cosine, {"ngram_size": 2**31}),
    InvalidCase(ss.sorensen_dice, {"ngram_size": True}, TypeError),
    InvalidCase(ss.overlap_coefficient, {"ngram_size": 1.0}, TypeError),
    InvalidCase(ss.monge_elkan, {"inner_metric": "lcs_similarity"}),
    InvalidCase(ss.monge_elkan, {"inner_metric": "JARO"}),
    InvalidCase(ss.jaro_winkler, {"prefix_scale": 0.0}),
    InvalidCase(ss.jaro_winkler, {"prefix_scale": 0.251}),
    InvalidCase(ss.jaro_winkler, {"prefix_scale": math.inf}),
    InvalidCase(ss.jaro_winkler, {"prefix_scale": True}, TypeError),
    InvalidCase(ss.jaro_winkler, {"prefix_cap": 0}),
    InvalidCase(ss.jaro_winkler, {"prefix_cap": 11}),
    InvalidCase(ss.jaro_winkler, {"prefix_cap": True}, TypeError),
    InvalidCase(ss.needleman_wunsch, {"match_score": 0}),
    InvalidCase(ss.needleman_wunsch, {"mismatch_penalty": 0}),
    InvalidCase(ss.needleman_wunsch, {"gap_penalty": 0}),
    InvalidCase(ss.needleman_wunsch, {"match_score": 2**31}),
    InvalidCase(ss.smith_waterman, {"match_score": -1}),
    InvalidCase(ss.smith_waterman, {"mismatch_penalty": 1}),
    InvalidCase(ss.smith_waterman, {"gap_penalty": 1}),
    InvalidCase(ss.affine_gap, {"mismatch_penalty": 0}),
    InvalidCase(ss.affine_gap, {"gap_open_penalty": 0}),
    InvalidCase(ss.affine_gap, {"gap_extend_penalty": 0}),
    InvalidCase(ss.affine_gap, {"gap_extend_penalty": -(2**31) - 1}),
]


@pytest.mark.parametrize("case", INVALID_CASES)
def test_invalid_options_fail_before_a_server_call(case: InvalidCase) -> None:
    with pytest.raises(case.error):
        case.helper("left", "right", **case.options)


def test_upstream_zero_local_penalties_and_nan_prefix_remain_admitted() -> None:
    assert isinstance(ss.smith_waterman("left", "right", mismatch_penalty=0, gap_penalty=0), Column)
    assert isinstance(ss.jaro_winkler("left", "right", prefix_scale=math.nan), Column)
    assert isinstance(ss.affine_gap("left", "right", mismatch_penalty=-(2**31)), Column)


def test_scala_style_aliases_use_the_same_helpers() -> None:
    aliases = [
        (ss.sorensenDice, ss.sorensen_dice),
        (ss.overlapCoefficient, ss.overlap_coefficient),
        (ss.braunBlanquet, ss.braun_blanquet),
        (ss.mongeElkan, ss.monge_elkan),
        (ss.lcsSimilarity, ss.lcs_similarity),
        (ss.jaroWinkler, ss.jaro_winkler),
        (ss.needlemanWunsch, ss.needleman_wunsch),
        (ss.smithWaterman, ss.smith_waterman),
        (ss.affineGap, ss.affine_gap),
        (ss.refinedSoundex, ss.refined_soundex),
        (ss.doubleMetaphone, ss.double_metaphone),
    ]
    for alias, helper in aliases:
        assert alias is helper
