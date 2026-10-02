"""GraphX's dynamic PageRank in the form of graphframes-rs (method="pregel_delta")."""
import pytest
from pyspark_pecan import ConvergenceError, GraphAlgorithms

pytestmark = pytest.mark.integration

IDS = [-5, 0, 1, 2, 3, 7, 9]
EDGES = [(0, 1), (1, 2), (1, 2), (2, 3), (2, 9), (3, 1), (7, 7)]
# The sink graph of the graphframes-rs test `test_pagerank_with_sink_vertices`.
SINK_IDS = [1, 2, 3, 4]
SINK_EDGES = [(1, 2), (1, 3), (2, 4), (3, 4)]
# reset 0.15, tolerance 0.01, by hand: after step 1 the ranks are
# (0.15, 0.21375, 0.21375, 0.405); step 2 moves 2*0.06375 into the sink; in
# step 3 only the sink is above the tolerance and it has no out-edge.
SINK_RAW = {1: 0.15, 2: 0.21375, 3: 0.21375, 4: 0.513375}


def frames(spark, ids, edges):
    return (spark.createDataFrame([(i,) for i in ids], "id long"),
            spark.createDataFrame(edges, "src long, dst long"))


def vertex_program(ids, edges, steps, tolerance, reset=0.15):
    """The reference recurrence on Python floats: the state after `steps` supersteps."""
    degree = {key: 0 for key in ids}
    for source, _ in edges:
        degree[source] += 1
    rank = {key: reset for key in ids}
    delta = {key: reset for key in ids}
    for index in range(steps):
        received = {key: 0.0 for key in ids}
        for source, target in edges:
            if index == 0 or delta[source] > tolerance:
                received[target] += delta[source] / degree[source]
        delta = {key: (1 - reset) * received[key] for key in ids}
        rank = {key: rank[key] + delta[key] for key in ids}
    return rank


def collect(result):
    return {row.id: row.pagerank for row in result.frame.collect()}


def test_fixed_budget_matches_the_vertex_program_with_loops_parallel_edges_and_isolates(spark):
    graph = GraphAlgorithms(spark)
    expected = vertex_program(IDS, EDGES, 8, 0.01)
    with graph.pagerank(*frames(spark, IDS, EDGES), method="pregel_delta", tolerance=0.01,
                        max_iterations=8, partitions=3) as result:
        assert result.algorithm == "pagerank-pregel-delta" and result.method == "pregel_delta"
        assert result.iterations == 8 and result.converged is None
        assert result.frame.columns == ["id", "pagerank"]
        assert collect(result) == pytest.approx(expected, rel=1e-12, abs=1e-15)
    # An isolated vertex and a vertex without in-edges keep the initial rank.
    assert expected[-5] == expected[0] == 0.15


def test_sink_graph_matches_the_hand_computation_and_the_graphframes_rs_expectation(spark):
    graph = GraphAlgorithms(spark)
    inputs = frames(spark, SINK_IDS, SINK_EDGES)
    with graph.pagerank(*inputs, method="pregel_delta", tolerance=0.01, max_iterations=14) as raw, \
            graph.pagerank(*inputs, method="pregel_delta", tolerance=0.01, max_iterations=14,
                           normalize=True) as normalized:
        assert collect(raw) == pytest.approx(SINK_RAW, rel=1e-12)
        ranks = collect(normalized)
        assert sum(ranks.values()) == pytest.approx(1.0, abs=1e-12)
        # The expected values of the graphframes-rs test, to its own tolerance.
        assert ranks == pytest.approx({1: 0.1375042970, 2: 0.1959436232, 3: 0.1959436232, 4: 0.4706084565},
                                      abs=1e-9)


def test_vote_to_halt_stops_when_no_delta_exceeds_the_tolerance(spark):
    events = []
    graph = GraphAlgorithms(spark, observer=events.append)
    with graph.pagerank(*frames(spark, SINK_IDS, SINK_EDGES), method="pregel_delta", tolerance=0.01,
                        max_iterations=14, vote_to_halt=True) as result:
        assert result.iterations == 3 and result.converged is True
        assert collect(result) == pytest.approx(SINK_RAW, rel=1e-12)
    ends = [event for event in events if event.kind == "iteration_end"]
    # The frontier shrinks: three gaining vertices, then the sink alone, then none.
    assert [event.frontier_size for event in ends] == [3, 1, 0]


def test_fixed_budget_counts_nothing_and_reports_no_frontier(spark):
    events = []
    graph = GraphAlgorithms(spark, observer=events.append)
    with graph.pagerank(*frames(spark, SINK_IDS, SINK_EDGES), method="pregel_delta", tolerance=0.01,
                        max_iterations=4):
        pass
    assert [event.kind for event in events] == ["iteration_start", "iteration_end"] * 4
    assert all(event.frontier_size is None for event in events)


def test_vote_to_halt_raises_at_the_cap(spark):
    graph = GraphAlgorithms(spark)
    with pytest.raises(ConvergenceError, match="active vertices after 2 iterations"):
        graph.pagerank(*frames(spark, SINK_IDS, SINK_EDGES), method="pregel_delta", tolerance=0.01,
                       max_iterations=2, vote_to_halt=True)


def test_argument_domains(spark):
    graph = GraphAlgorithms(spark)
    inputs = frames(spark, SINK_IDS, SINK_EDGES)
    with pytest.raises(ValueError, match="requires a positive tolerance"):
        graph.pagerank(*inputs, method="pregel_delta")
    with pytest.raises(ValueError, match="vote_to_halt applies only"):
        graph.pagerank(*inputs, vote_to_halt=True)


def test_graph_without_vertices_or_without_edges(spark):
    graph = GraphAlgorithms(spark)
    with graph.pagerank(*frames(spark, [], []), method="pregel_delta", tolerance=0.01, max_iterations=2,
                        normalize=True) as empty:
        assert collect(empty) == {}
    with graph.pagerank(*frames(spark, [4, 6], []), method="pregel_delta", tolerance=0.01, max_iterations=2,
                        normalize=True) as isolated:
        assert collect(isolated) == pytest.approx({4: 0.5, 6: 0.5})
