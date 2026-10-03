"""Free argument-domain admission for the new Pregel distance programs."""

from __future__ import annotations

from typing import cast

import pytest
from pyspark_pecan import GraphAlgorithms, PregelSsspOptions, ShortestPathsOptions
from pyspark_pecan.shortest_pregel import landmarks_program, sssp_program
from pyspark_pecan.types import INT64_MAX, INT64_MIN


@pytest.mark.parametrize("source", [INT64_MIN, INT64_MAX])
def test_source_full_signed_domain_and_scalar_message(source: int) -> None:
    graph = cast(GraphAlgorithms, object())
    options = PregelSsspOptions(source=source)
    program = sssp_program(graph, options)
    assert [column.name for column in program.columns] == ["distance"]
    assert program.edge_columns == ["src", "dst", "weight"]
    assert program.destination_state is False and program.vote is not None
    assert len(program.messages) == len(program.aggregates) == 1


def test_fixed_budget_and_sorted_per_landmark_columns() -> None:
    graph = cast(GraphAlgorithms, object())
    options = ShortestPathsOptions(landmarks=(INT64_MAX, INT64_MIN), vote_to_halt=False)
    program = landmarks_program(graph, options)
    assert [column.name for column in program.columns] == [f"dist_{INT64_MIN}", f"dist_{INT64_MAX}"]
    assert program.destination_state is False and program.vote is None
    assert len(program.messages) == len(program.aggregates) == 2
    assert sssp_program(graph, PregelSsspOptions(source=0, vote_to_halt=False)).vote is None


@pytest.mark.parametrize("source", [INT64_MIN - 1, INT64_MAX + 1, True])
def test_invalid_source_arguments_need_no_data_operation(source: int) -> None:
    with pytest.raises(ValueError):
        PregelSsspOptions(source=source)


@pytest.mark.parametrize("landmarks", [(), (INT64_MIN - 1,), (INT64_MAX + 1,), (True,)])
def test_invalid_landmark_arguments_need_no_data_operation(landmarks: tuple[int, ...]) -> None:
    with pytest.raises(ValueError):
        ShortestPathsOptions(landmarks=landmarks)
