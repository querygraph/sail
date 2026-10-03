"""Full-output independent distance oracles for the common Pregel programs."""

from __future__ import annotations

import heapq
from collections import deque
from typing import TYPE_CHECKING

import pytest
from pyspark.sql.types import DoubleType, IntegerType, LongType
from pyspark_pecan import CancellationToken, ConvergenceError, GraphAlgorithms, GraphCancelledError
from pyspark_pecan.shortest_pregel import MAX_HOPS

if TYPE_CHECKING:
    from pyspark.sql import DataFrame
    from pyspark.sql.connect.session import SparkSession

pytestmark = pytest.mark.integration

IDS = [-(1 << 63), -9, 0, 2, 3, 5, (1 << 63) - 1]
EDGES = [
    (-(1 << 63), -9, 1.0),
    (-(1 << 63), 2, 5.0),
    (-9, 0, 0.0),
    (0, -9, 0.0),
    (0, 2, 1.0),
    (0, 2, 3.0),
    (2, 3, 0.5),
    (3, 3, 0.0),
    (5, (1 << 63) - 1, 2.0),
]


def frames(spark: SparkSession, ids: list[int], edges: list[tuple[int, int, float]]) -> tuple[DataFrame, DataFrame]:
    return (
        spark.createDataFrame([(key,) for key in ids], "id long"),
        spark.createDataFrame(edges, "src long, dst long, weight double"),
    )


def dijkstra(
    ids: list[int], edges: list[tuple[int, int, float]], source: int, directed: bool
) -> dict[int, float | None]:
    """Heap Dijkstra, independent of the Pregel superstep recurrence."""
    adjacency: dict[int, list[tuple[int, float]]] = {key: [] for key in ids}
    for start, end, weight in edges:
        adjacency[start].append((end, weight))
        if not directed:
            adjacency[end].append((start, weight))
    found = {source: 0.0}
    queue = [(0.0, source)]
    while queue:
        distance, vertex = heapq.heappop(queue)
        if distance != found[vertex]:
            continue
        for target, weight in adjacency[vertex]:
            candidate = distance + weight
            if target not in found or candidate < found[target]:
                found[target] = candidate
                heapq.heappush(queue, (candidate, target))
    return {key: found.get(key) for key in ids}


def hop_distances(ids: list[int], edges: list[tuple[int, int, float]], source: int, reverse: bool) -> dict[int, int]:
    """Queue BFS from one landmark, independently of the column program."""
    adjacency: dict[int, list[int]] = {key: [] for key in ids}
    for start, end, _ in edges:
        if reverse:
            start, end = end, start
        adjacency[start].append(end)
    found = {source: 0}
    queue = deque([source])
    while queue:
        vertex = queue.popleft()
        for target in adjacency[vertex]:
            if target not in found:
                found[target] = found[vertex] + 1
                queue.append(target)
    return {key: found.get(key, MAX_HOPS) for key in ids}


@pytest.mark.parametrize("directed", [True, False])
@pytest.mark.parametrize("partitions", [1, 3])
def test_sssp_full_distance_domain_signed_ids_cycles_and_parallel_edges(
    spark: SparkSession, directed: bool, partitions: int
) -> None:
    with GraphAlgorithms(spark, snapshot_inputs=False).sssp(
        *frames(spark, IDS, EDGES), source=IDS[0], method="pregel", directed=directed, partitions=partitions
    ) as result:
        rows = result.frame.collect()
        assert result.frame.columns == ["id", "distance"]
        assert isinstance(result.frame.schema["id"].dataType, LongType)
        assert isinstance(result.frame.schema["distance"].dataType, DoubleType)
        assert len(rows) == len(IDS)
        assert {row.id for row in rows} == set(IDS)
        assert {row.id: row.distance for row in rows} == dijkstra(IDS, EDGES, IDS[0], directed)
        assert result.algorithm == "sssp-pregel" and result.method == "pregel"
        assert result.converged is True


def test_sssp_fixed_budget_returns_bounded_state_without_count(
    spark: SparkSession, monkeypatch: pytest.MonkeyPatch
) -> None:
    vertices, edges = frames(spark, [1, 2, 3], [(1, 2, 1.0), (2, 3, 1.0)])

    def unexpected_count(frame: DataFrame) -> int:
        raise AssertionError("fixed-budget Pregel must not count")

    monkeypatch.setattr(type(vertices), "count", unexpected_count)
    with GraphAlgorithms(spark).sssp(
        vertices, edges, source=1, method="pregel", max_iterations=1, vote_to_halt=False
    ) as result:
        assert {row.id: row.distance for row in result.frame.collect()} == {1: 0.0, 2: 1.0, 3: None}
        assert result.iterations == 1 and result.converged is None


def test_sssp_cap_is_not_reported_as_convergence(spark: SparkSession) -> None:
    with pytest.raises(ConvergenceError, match="active vertices after 1 iterations"):
        GraphAlgorithms(spark).sssp(
            *frames(spark, [1, 2, 3], [(1, 2, 1.0), (2, 3, 1.0)]), source=1, method="pregel", max_iterations=1
        )


def test_sssp_isolated_source_and_empty_adjacency(spark: SparkSession) -> None:
    with GraphAlgorithms(spark).sssp(*frames(spark, [0, -9, 8], []), source=-9, method="pregel") as result:
        assert {row.id: row.distance for row in result.frame.collect()} == {-9: 0.0, 0: None, 8: None}
        assert result.iterations == 1 and result.converged is True


@pytest.mark.parametrize("reverse", [True, False])
def test_landmarks_have_independent_forward_and_backward_hops(spark: SparkSession, reverse: bool) -> None:
    landmarks = [5, IDS[0]]
    with GraphAlgorithms(spark, snapshot_inputs=False).shortest_paths(
        *frames(spark, IDS, EDGES), landmarks=landmarks, to_landmarks=reverse, partitions=3
    ) as result:
        rows = result.frame.collect()
        assert len(rows) == len(IDS) and {row.id for row in rows} == set(IDS)
        assert result.frame.columns == ["id", *[f"dist_{landmark}" for landmark in sorted(landmarks)]]
        assert isinstance(result.frame.schema["id"].dataType, LongType)
        for landmark in landmarks:
            column = f"dist_{landmark}"
            assert isinstance(result.frame.schema[column].dataType, IntegerType)
            assert {row.id: row[column] for row in rows} == hop_distances(IDS, EDGES, landmark, reverse)
        assert result.algorithm == "shortest-paths-pregel" and result.converged is True


def test_landmarks_match_graphframes_rs_small_graph(spark: SparkSession) -> None:
    edges = [(1, 2, 1.0), (2, 3, 1.0), (2, 4, 1.0), (3, 4, 1.0), (4, 1, 1.0), (4, 2, 1.0), (2, 1, 1.0), (3, 2, 1.0)]
    with GraphAlgorithms(spark).shortest_paths(
        *frames(spark, [1, 2, 3, 4], edges), landmarks=[4, 1], to_landmarks=True
    ) as result:
        got = {row.id: (row.dist_1, row.dist_4) for row in result.frame.collect()}
        assert got == {1: (0, 2), 2: (1, 1), 3: (2, 1), 4: (1, 0)}


def test_landmarks_fixed_zero_budget_and_disconnected_isolates(
    spark: SparkSession, monkeypatch: pytest.MonkeyPatch
) -> None:
    vertices, edges = frames(spark, [-9, 0, 8], [])

    def unexpected_count(frame: DataFrame) -> int:
        raise AssertionError("fixed-budget Pregel must not count")

    monkeypatch.setattr(type(vertices), "count", unexpected_count)
    with GraphAlgorithms(spark).shortest_paths(
        vertices, edges, landmarks=[-9], max_iterations=0, vote_to_halt=False
    ) as result:
        assert {row.id: row["dist_-9"] for row in result.frame.collect()} == {-9: 0, 0: MAX_HOPS, 8: MAX_HOPS}
        assert result.iterations == 0 and result.converged is None


def test_landmark_cap_and_duplicate_arguments(spark: SparkSession) -> None:
    vertices, edges = frames(spark, [1, 2, 3], [(1, 2, 1.0), (2, 3, 1.0)])
    with pytest.raises(ConvergenceError, match="active vertices after 1 iterations"):
        GraphAlgorithms(spark).shortest_paths(vertices, edges, landmarks=[1], max_iterations=1)
    with pytest.raises(ValueError, match="landmarks must be unique"):
        GraphAlgorithms(spark).shortest_paths(vertices, edges, landmarks=[1, 1])


def test_cancellation_uses_the_common_owned_loop(spark: SparkSession) -> None:
    cancellation = CancellationToken()

    def cancel(event: object) -> None:
        cancellation.cancel()

    with pytest.raises(GraphCancelledError):
        GraphAlgorithms(spark, observer=cancel).sssp(
            *frames(spark, IDS, EDGES), source=IDS[0], method="pregel", cancellation=cancellation
        )
