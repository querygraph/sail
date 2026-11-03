"""Delta PageRank certificates and actual frontier behavior on Sail."""

import numpy as np
import pytest

from pyspark_pecan import CancellationToken, ConvergenceError, GraphAlgorithms, GraphCancelledError
from pyspark_pecan.pagerank_delta import execute
from pyspark_pecan.staging import StagingRun


FRONTIER_EDGES = [
    (0, 5), (1, 0), (2, 4), (3, 11), (4, 0), (5, 7), (6, 2), (7, 8),
    (8, 7), (9, 9), (10, 7), (11, 9), (11, 1), (7, 11), (2, 3),
    (10, 7), (7, 8), (1, 8), (11, 1), (0, 10),
]


def frames(spark, ids, edges):
    return (spark.createDataFrame([(value,) for value in ids], "id long"),
            spark.createDataFrame(edges, "src long, dst long"))


def transition(ids, edges):
    index = {value: position for position, value in enumerate(ids)}
    matrix = np.zeros((len(ids), len(ids)), dtype=np.float64)
    degree = {value: 0 for value in ids}
    for source, _ in edges:
        degree[source] += 1
    for source, target in edges:
        matrix[index[target], index[source]] += 1.0 / degree[source]
    for source in ids:
        if degree[source] == 0:
            matrix[:, index[source]] = 1.0 / len(ids)
    return matrix


def assert_certificate(result, ids, edges, reset, tolerance):
    actual = {row.id: row.pagerank for row in result.frame.collect()}
    assert set(actual) == set(ids)
    values = np.array([actual[value] for value in ids])
    matrix = transition(ids, edges)
    restart = np.full(len(ids), reset / len(ids))
    # A direct linear solve is independent of frontier and power iteration.
    expected = np.linalg.solve(np.eye(len(ids)) - (1.0 - reset) * matrix, restart)
    true_residual = np.abs(restart + (1.0 - reset) * matrix @ values - values).sum()
    assert values.min() >= 0
    assert values.sum() == pytest.approx(1.0, abs=1e-12)
    assert true_residual <= tolerance + 2e-14
    assert result.residual == pytest.approx(true_residual, abs=2e-14)
    assert result.error_bound == pytest.approx(result.residual / reset, abs=2e-14)
    assert np.abs(values - expected).sum() <= result.error_bound + 2e-13
    assert result.converged is True
    assert result.algorithm == "pagerank-delta"
    return values


@pytest.mark.parametrize("options", [
    {"tolerance": None}, {"tolerance": 0}, {"tolerance": -1},
    {"tolerance": True}, {"tolerance": float("inf")}, {"tolerance": float("nan")},
    {"tolerance": 1e-6, "max_iterations": 0},
    {"tolerance": 1e-6, "reset_probability": 0},
])
def test_delta_options_fail_before_any_graph_or_server_access(options):
    with pytest.raises(ValueError):
        execute(None, None, None, **options)


@pytest.mark.integration
def test_delta_matches_stationary_solution_and_power_with_dangling_vertices(spark):
    ids = [-2**63, -5, 0, 1, 2, 3, 7, 2**63 - 1]
    edges = [(0, 1), (1, 2), (1, 2), (2, 3), (2, 2**63 - 1), (3, 1), (7, 7)]
    inputs = frames(spark, ids, edges)
    graph = GraphAlgorithms(spark)
    reset, tolerance = 0.5, 1e-7
    with graph.pagerank(*inputs, method="delta", reset_probability=reset, tolerance=tolerance,
                        max_iterations=80, partitions=3) as result:
        delta = assert_certificate(result, ids, edges, reset, tolerance)
    # The unchanged default power method remains an independent implementation.
    with graph.pagerank(*inputs, reset_probability=reset, tolerance=1e-10,
                        max_iterations=80, partitions=3) as result:
        by_id = {row.id: row.pagerank for row in result.frame.collect()}
        power = np.array([by_id[value] for value in ids])
    assert np.abs(delta - power).sum() <= tolerance / reset + 1e-9


@pytest.mark.integration
def test_frontier_shrinks_and_reactivates_without_losing_accumulated_residual(spark, monkeypatch):
    events, retained_stages = [], []
    original = StagingRun.materialize

    def materialize(run, frame, **options):
        result = original(run, frame, **options)
        retained_stages.append(len(run._stages))
        return result

    monkeypatch.setattr(StagingRun, "materialize", materialize)
    graph = GraphAlgorithms(spark, observer=events.append)
    ids = list(range(12))
    # The standard reset leaves enough tail rounds for this graph to exercise
    # both a genuinely small frontier and later reactivation under the new
    # tolerance-scaled cutoff. Keep the strict work/activation assertions below.
    with execute(graph, *frames(spark, ids, FRONTIER_EDGES), reset_probability=0.15,
                 tolerance=1e-7, max_iterations=100, partitions=3) as result:
        assert_certificate(result, ids, FRONTIER_EDGES, 0.15, 1e-7)
        assert len(result._run._stages) == 1
    iterations = [event for event in events if event["kind"] == "iteration_end"]
    assert len(iterations) > 3
    assert iterations[0]["frontier_size"] > len(ids) // 2
    assert all(event["activation_threshold"] <= 1e-7 * event["activation_mass"] / (4 * len(ids))
               for event in iterations)
    assert min(event["frontier_size"] for event in iterations) < len(ids) // 2
    assert min(event["active_edges"] for event in iterations) < len(FRONTIER_EDGES) // 2
    assert any(event["reactivated_vertices"] > 0 for event in iterations)
    assert any(after["frontier_size"] > before["frontier_size"]
               for before, after in zip(iterations, iterations[1:]))
    assert max(retained_stages) <= 5  # snapshots, old/new state, current frontier
    assert events[-1]["kind"] == "certificate"
    assert events[-1]["residual"] <= 1e-7


@pytest.mark.integration
@pytest.mark.parametrize("ids, edges, reset", [
    ([], [], 0.15), ([0, 1, 2], [(0, 1), (1, 2), (2, 0)], 0.15),
    ([0, 1, 2], [], 0.15), ([0, 1, 2], [(0, 1)], 1.0),
])
def test_uniform_stationary_initialization_needs_no_frontier_pushes(spark, ids, edges, reset):
    with GraphAlgorithms(spark).pagerank(*frames(spark, ids, edges), method="delta",
                                        reset_probability=reset, tolerance=1e-12, max_iterations=1) as result:
        assert result.iterations == 0
        assert result.residual <= 1e-12
        if ids:
            assert_certificate(result, ids, edges, reset, 1e-12)
        else:
            assert result.frame.columns == ["id", "pagerank"]
            assert result.frame.count() == 0


@pytest.mark.integration
def test_iteration_cap_still_runs_the_true_certificate(spark):
    ids, reset = list(range(12)), 0.5
    matrix = transition(ids, FRONTIER_EDGES)
    initial = np.full(len(ids), 1.0 / len(ids))
    residual = reset / len(ids) + (1 - reset) * matrix @ initial - initial
    initial_norm = np.abs(residual).sum()
    # This fixture's true one-step residual is below .15, while its cheap
    # bound is above .15. Thus the cap must still run the actual certificate.
    tolerance = 0.15
    threshold = min(initial_norm / (2 * len(ids)), tolerance * initial.sum() / (4 * len(ids)))
    pushed = np.where(np.abs(residual) > threshold, residual, 0)
    updated = initial + pushed
    pending = residual - pushed + (1 - reset) * matrix @ pushed
    bound = 2 * np.abs(pending).sum() / updated.sum()
    normalized = updated / updated.sum()
    certificate = np.abs(reset / len(ids) + (1 - reset) * matrix @ normalized - normalized).sum()
    assert certificate < tolerance < bound
    assert tolerance < initial_norm
    with execute(GraphAlgorithms(spark), *frames(spark, ids, FRONTIER_EDGES),
                 reset_probability=reset, tolerance=tolerance, max_iterations=1) as result:
        assert result.iterations == 1
        assert_certificate(result, ids, FRONTIER_EDGES, reset, tolerance)


@pytest.mark.integration
def test_uncertified_limit_and_observer_cancellation_clean_the_owned_run(spark, monkeypatch):
    graph = GraphAlgorithms(spark)
    allocated = []
    original = graph.utils.allocate

    def allocate(**options):
        run = original(**options)
        allocated.append(run)
        return run

    monkeypatch.setattr(graph.utils, "allocate", allocate)
    with pytest.raises(ConvergenceError, match="global residual"):
        execute(graph, *frames(spark, list(range(12)), FRONTIER_EDGES),
                tolerance=1e-14, max_iterations=1)
    assert graph.utils.remove(*allocated[-1]) == 0
    token = CancellationToken()
    graph.observer = lambda event: token.cancel() if event["kind"] == "iteration_end" else None
    with pytest.raises(GraphCancelledError):
        execute(graph, *frames(spark, list(range(12)), FRONTIER_EDGES),
                tolerance=1e-14, max_iterations=20, cancellation=token)
    assert len(allocated) == 2
    assert graph.utils.remove(*allocated[-1]) == 0
