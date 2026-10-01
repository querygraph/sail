import pytest

from pyspark_pecan import (
    CancellationToken, ConvergenceError, GraphAlgorithms, GraphCancelledError,
)

pytestmark = pytest.mark.integration

IDS = [-5, 0, 1, 2, 3, 7, 9]
EDGES = [(0, 1), (1, 2), (1, 2), (2, 3), (2, 9), (3, 1), (7, 7)]


def frames(spark, ids=IDS, edges=EDGES):
    return (spark.createDataFrame([(i,) for i in ids], "id long"),
            spark.createDataFrame(edges, "src long, dst long"))


def pagerank_reference(ids, edges, iterations, reset=0.15):
    rank = {key: 1.0 / len(ids) for key in ids}
    degree = {key: 0 for key in ids}
    for source, _ in edges:
        degree[source] += 1
    for _ in range(iterations):
        dangling = sum(rank[key] for key in ids if not degree[key]) / len(ids)
        updated = {key: reset / len(ids) + (1 - reset) * dangling for key in ids}
        for source, target in edges:
            updated[target] += (1 - reset) * rank[source] / degree[source]
        rank = updated
    return rank


def test_pagerank_dangling_isolates_parallel_edges_and_loop(spark):
    graph = GraphAlgorithms(spark)
    vertices, edges = frames(spark)
    expected = pagerank_reference(IDS, EDGES, 8)
    with graph.pagerank(vertices, edges, max_iterations=8, partitions=3) as result:
        actual = {row.id: row.pagerank for row in result.frame.collect()}
        assert actual == pytest.approx(expected, rel=1e-11, abs=1e-13)
        assert sum(actual.values()) == pytest.approx(1.0, abs=1e-12)
        assert result.iterations == 8
        assert result.converged is None
    with pytest.raises(RuntimeError, match="closed"):
        _ = result.frame
    assert graph.utils.remove(result._run.path, result._run.token) == 0


def test_wcc_exact_minimum_labels_and_result_retention(spark):
    graph = GraphAlgorithms(spark)
    vertices, edges = frames(spark)
    with graph.wcc(vertices, edges, partitions=3) as result:
        expected = {-5: -5, 0: 0, 1: 0, 2: 0, 3: 0, 7: 7, 9: 0}
        assert {row.id: row.component for row in result.frame.collect()} == expected
        assert result.converged is True
        assert result.algorithm == "wcc-min-label"
        entries = graph.utils.ls(result._run.path, result._run.token)
        files = [row.path for row in entries if row.kind == "entry"]
        assert files
        assert all(path.startswith(result.path.rstrip("/") + "/") for path in files)


@pytest.mark.parametrize("algorithm", ["pagerank", "wcc"])
def test_empty_graph(spark, algorithm):
    graph = GraphAlgorithms(spark)
    with getattr(graph, algorithm)(*frames(spark, [], []), partitions=2) as result:
        assert result.frame.count() == 0
        assert result.iterations == 0


def test_tolerance_and_limit_are_explicit(spark):
    graph = GraphAlgorithms(spark)
    cycle = frames(spark, [0, 1, 2], [(0, 1), (1, 2), (2, 0)])
    with graph.pagerank(*cycle, tolerance=1e-12, max_iterations=2) as result:
        assert result.converged is True
        assert result.iterations == 1
    with pytest.raises(ConvergenceError, match="PageRank"):
        graph.pagerank(*frames(spark), tolerance=1e-15, max_iterations=1)
    with pytest.raises(ConvergenceError, match="WCC"):
        graph.wcc(*frames(spark), max_iterations=1)


def test_cancel_between_iterations_cleans_stages(spark, monkeypatch):
    token = CancellationToken()
    events = []
    def observe(event):
        events.append(event.as_dict())
        if event.kind == "iteration_end":
            token.cancel()
    graph = GraphAlgorithms(spark, observer=observe)
    allocated = []
    original = graph.utils.allocate
    def allocate(**kwargs):
        run = original(**kwargs)
        allocated.append(run)
        return run
    monkeypatch.setattr(graph.utils, "allocate", allocate)
    with pytest.raises(GraphCancelledError):
        graph.pagerank(*frames(spark), max_iterations=8, cancellation=token)
    assert [event["iteration"] for event in events if event["kind"] == "iteration_end"] == [1]
    assert graph.utils.remove(*allocated[0]) == 0


def test_foreign_session_frames_are_rejected_before_staging(spark):
    # Session isolation itself is tested by the host suite. The portable client
    # also rejects mixing frames before it allocates any staging state.
    class ForeignFrame:
        sparkSession = object()
    with pytest.raises(ValueError, match="same|this Spark session"):
        GraphAlgorithms(spark).wcc(ForeignFrame(), ForeignFrame())


@pytest.mark.integration
def test_pregel_pagerank_matches_power_without_dangling_vertices(spark):
    graph = GraphAlgorithms(spark)
    cycle = frames(spark, [0, 1, 2, 3], [(0, 1), (1, 2), (2, 3), (3, 0), (0, 2)])
    with graph.pagerank(*cycle, max_iterations=6) as power, \
            graph.pagerank(*cycle, max_iterations=6, method="pregel") as pregel:
        assert pregel.algorithm == "pagerank-pregel" and pregel.iterations == 6 and pregel.converged is None
        a = {r.id: r.pagerank for r in power.frame.collect()}
        b = {r.id: r.pagerank for r in pregel.frame.collect()}
        assert all(abs(a[i] - b[i]) < 1e-12 for i in a)


@pytest.mark.integration
def test_pregel_pagerank_keeps_order_and_normalizes_with_dangling_vertices(spark):
    graph = GraphAlgorithms(spark)
    dangling = frames(spark, [0, 1, 2, 3], [(0, 1), (1, 2), (2, 1), (3, 1)])
    with graph.pagerank(*dangling, max_iterations=10, method="pregel") as raw, \
            graph.pagerank(*dangling, max_iterations=10, method="pregel", normalize=True) as normalized, \
            graph.pagerank(*dangling, max_iterations=10) as power:
        r = {x.id: x.pagerank for x in raw.frame.collect()}
        n = {x.id: x.pagerank for x in normalized.frame.collect()}
        p = {x.id: x.pagerank for x in power.frame.collect()}
        assert sum(r.values()) < 1.0 and abs(sum(n.values()) - 1.0) < 1e-12
        # Vertices 0 and 3 are structurally identical, so order ties by id.
        order = lambda ranks: sorted(ranks, key=lambda i: (round(ranks[i], 12), i))  # noqa: E731
        assert order(r) == order(n) == order(p)

