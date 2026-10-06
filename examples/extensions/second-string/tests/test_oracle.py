"""Compare shuffled Spark Connect batches against the pinned compiled Scala oracle."""

import json
import math
from dataclasses import dataclass
from pathlib import Path

from pyspark.sql import Column
from pyspark.sql.connect import functions as F
from pyspark.sql.connect.session import SparkSession

REFERENCE_COMMIT = "a35db39fa8e9b65db2d201a45b86d11a6ca34b98"
ABSOLUTE_TOLERANCE = 1e-12
DEFAULT_FUNCTIONS = frozenset(
    (
        "ss_jaccard",
        "ss_cosine",
        "ss_sorensen_dice",
        "ss_overlap_coefficient",
        "ss_braun_blanquet",
        "ss_monge_elkan",
        "ss_levenshtein",
        "ss_lcs_similarity",
        "ss_jaro",
        "ss_jaro_winkler",
        "ss_needleman_wunsch",
        "ss_smith_waterman",
        "ss_affine_gap",
        "ss_soundex",
        "ss_refined_soundex",
        "ss_double_metaphone",
    )
)
UNARY_FUNCTIONS = frozenset(("ss_soundex", "ss_refined_soundex", "ss_double_metaphone"))
type Parameter = int | float | str
type Expected = float | str


@dataclass(frozen=True, slots=True)
class Pair:
    left: str
    right: str | None


@dataclass(frozen=True, slots=True)
class Definition:
    function: str
    parameters: tuple[Parameter, ...]

    @property
    def sql_name(self) -> str:
        return f"{self.function}_with_options" if self.parameters else self.function


@dataclass(frozen=True, slots=True)
class Case:
    id: str
    definition: Definition
    pair: Pair
    expected: Expected


@dataclass(frozen=True, slots=True)
class Oracle:
    cases: tuple[Case, ...]


def record(value: object) -> dict[str, object]:
    if not isinstance(value, dict) or any(not isinstance(key, str) for key in value):
        raise ValueError("Expected a JSON object with string keys")
    return dict(value)


def sequence(value: object) -> list[object]:
    if not isinstance(value, list):
        raise TypeError("Expected a JSON array")
    return list(value)


def text(value: object) -> str:
    if not isinstance(value, str):
        raise TypeError("Expected a string")
    return value


def integer(value: object) -> int:
    if type(value) is not int:
        raise ValueError("Expected an integer")
    return value


def parameter(value: object) -> Parameter:
    if isinstance(value, str):
        return value
    if type(value) is int:
        return value
    if isinstance(value, float) and math.isfinite(value):
        return value
    raise ValueError("Expected a finite numeric or string option")


def expected(value: object, unary: bool) -> Expected:
    if unary:
        return text(value)
    if not isinstance(value, (float, int)) or isinstance(value, bool):
        raise TypeError("Expected a numeric reference answer")
    result = float(value)
    if not math.isfinite(result):
        raise ValueError("Reference answer is not finite")
    return result


def load_oracle(path: Path) -> Oracle:
    root = record(json.loads(path.read_text(encoding="utf-8")))
    reference = record(root["reference"])
    if reference["commit"] != REFERENCE_COMMIT:
        raise ValueError("Fixture is not from the pinned Scala source")
    if reference["numeric_absolute_tolerance"] != ABSOLUTE_TOLERANCE:
        raise ValueError("Fixture declares a different numerical comparison")
    pairs: list[Pair] = []
    for item in sequence(root["pairs"]):
        pair_record = record(item)
        pairs.append(
            Pair(
                text(pair_record["left"]),
                None if pair_record["right"] is None else text(pair_record["right"]),
            )
        )
    if integer(reference["pair_count"]) != len(pairs):
        raise ValueError("Fixture pair count differs")
    cases: list[Case] = []
    for item in sequence(root["cases"]):
        case = record(item)
        function = text(case["function"])
        if function not in DEFAULT_FUNCTIONS:
            raise ValueError(f"Unknown oracle function: {function}")
        index = integer(case["pair"])
        if not 0 <= index < len(pairs):
            raise ValueError("Oracle case names an unknown pair")
        unary = function in UNARY_FUNCTIONS
        params = tuple(parameter(value) for value in sequence(case["parameters"]))
        if unary and params:
            raise ValueError("Unary oracle function has unexpected options")
        pair = pairs[index]
        if not unary and pair.right is None:
            raise ValueError("Binary non-null oracle case has no right input")
        cases.append(
            Case(
                text(case["id"]),
                Definition(function, params),
                pair,
                expected(case["expected"], unary),
            )
        )
    if integer(reference["case_count"]) != len(cases) or len({case.id for case in cases}) != len(
        cases
    ):
        raise ValueError("Fixture case count or unique IDs differ")
    definitions = {case.definition for case in cases}
    defaults = {definition.function for definition in definitions if not definition.parameters}
    if defaults != DEFAULT_FUNCTIONS or len(definitions) != 55 or len(cases) != 7296:
        raise ValueError(
            "Fixture does not cover all sixteen defaults and thirty-nine option groups"
        )
    return Oracle(tuple(cases))


def literal(value: Parameter) -> Column:
    return F.lit(value)


def test_all_compiled_scala_answers_through_shuffled_native_batches(spark: SparkSession) -> None:
    oracle = load_oracle(Path(__file__).parent / "fixtures/oracle.json")
    groups: dict[Definition, list[Case]] = {}
    for case in oracle.cases:
        groups.setdefault(case.definition, []).append(case)
    checked: set[str] = set()
    for definition, cases in groups.items():
        batch = spark.createDataFrame(
            [(case.id, case.pair.left, case.pair.right) for case in cases],
            "case_id STRING, left STRING, right STRING",
        ).repartition(4, "case_id")
        inputs = [F.col("left")]
        if definition.function not in UNARY_FUNCTIONS:
            inputs.append(F.col("right"))
        inputs.extend(literal(value) for value in definition.parameters)
        rows = batch.select(
            "case_id", F.call_function(definition.sql_name, *inputs).alias("actual")
        ).collect()
        answers = {case.id: case.expected for case in cases}
        ids = [text(row["case_id"]) for row in rows]
        assert len(rows) == len(cases), f"Wrong row count for {definition}"
        assert len(set(ids)) == len(ids), f"Repeated result ID for {definition}"
        assert set(ids) == answers.keys(), f"Missing or extra result ID for {definition}"
        for row, case_id in zip(rows, ids, strict=True):
            reference_answer = answers[case_id]
            actual = row["actual"]
            if isinstance(reference_answer, str):
                assert actual == reference_answer, f"{case_id}: {definition}, {actual!r}"
            else:
                assert type(actual) is float and math.isfinite(actual), f"{case_id}: {actual!r}"
                assert abs(actual - reference_answer) <= ABSOLUTE_TOLERANCE, (
                    f"{case_id}: {definition}, actual={actual!r}, Scala={reference_answer!r}"
                )
        assert checked.isdisjoint(ids)
        checked.update(ids)
    assert len(checked) == len(oracle.cases)
