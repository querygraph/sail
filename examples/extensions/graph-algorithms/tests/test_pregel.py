"""The Pregel loop, against the unit tests of graphframes-rs's `pregel.rs`."""
import pytest
from pyspark.sql.connect import functions as F
from pyspark_pecan import GraphAlgorithms
from pyspark_pecan.pregel import dst, msg, src

pytestmark = pytest.mark.integration


def frames(spark, ids, edges):
    return (spark.createDataFrame([(i,) for i in ids], "id long"),
            spark.createDataFrame(edges, "src long, dst long"))


def column(result, name):
    return [row[name] for row in result.frame.orderBy("id").collect()]


def test_zero_iterations_keeps_the_initial_state_and_debug_columns(spark):
    program = (GraphAlgorithms(spark).pregel().max_iterations(0)
               .participation("participation", F.lit(True), F.lit(True))
               .vote_to_halt("activity", F.lit(True))
               .vertex_column("value", F.lit(0), F.col("value"))
               .message(F.lit(1), "src_to_dst"))
    with program.run(*frames(spark, [1, 2, 3], [(1, 2), (2, 3)]), include_debug_columns=True) as result:
        assert result.frame.columns == ["id", "value", "activity", "participation"]
        assert result.iterations == 0


def test_in_degree(spark):
    program = (GraphAlgorithms(spark).pregel().max_iterations(1)
               .vertex_column("in_degree", F.lit(0), F.col("in_degree") + F.coalesce(msg(), F.lit(0)))
               .message(F.lit(1), "src_to_dst").aggregate(F.sum(msg())).skip_destination_state())
    with program.run(*frames(spark, [1, 2, 3], [(1, 2), (2, 3), (1, 3)])) as result:
        assert result.frame.columns == ["in_degree", "id"]
        assert column(result, "in_degree") == [0, 1, 2]
        assert result.iterations == 1 and result.converged is None


def test_out_degree_with_destination_state(spark):
    program = (GraphAlgorithms(spark).pregel().max_iterations(1)
               .vertex_column("out_degree", F.lit(0), F.col("out_degree") + F.coalesce(msg(), F.lit(0)))
               .message(F.lit(1), "dst_to_src").aggregate(F.sum(msg())))
    with program.run(*frames(spark, [1, 2, 3], [(1, 2), (2, 3), (1, 3)])) as result:
        assert column(result, "out_degree") == [2, 1, 0]


def test_self_loop(spark):
    program = (GraphAlgorithms(spark).pregel().max_iterations(1)
               .vertex_column("loop", F.lit(0), F.col("loop") + msg())
               .message(F.lit(1), "src_to_dst").aggregate(F.sum(msg())).skip_destination_state())
    with program.run(*frames(spark, [1], [(1, 1)])) as result:
        assert column(result, "loop") == [1]


def test_no_edges_leaves_the_state_unchanged(spark):
    # graphframes-rs's test adds a null message to 0; with COALESCE the value stays.
    program = (GraphAlgorithms(spark).pregel().max_iterations(1)
               .vertex_column("value", F.lit(0), F.col("value") + F.coalesce(msg(), F.lit(0)))
               .message(F.lit(1), "src_to_dst").aggregate(F.sum(msg())).skip_destination_state())
    with program.run(*frames(spark, [1, 2], [])) as result:
        assert column(result, "value") == [0, 0]


def chain(spark):
    return frames(spark, [1, 2, 3, 4], [(1, 2), (2, 3), (3, 4)])


def test_chain_propagation_halts_by_vote(spark):
    program = (GraphAlgorithms(spark).pregel().max_iterations(100)
               .vertex_column("value", F.when(F.col("id") == 1, 1).otherwise(0),
                              F.when(msg() > F.col("value"), msg()).otherwise(F.col("value")))
               .vote_to_halt("active", F.col("value") != msg())
               .message(src("value"), "src_to_dst").aggregate(F.max(msg())).skip_destination_state())
    with program.run(*chain(spark)) as result:
        assert result.iterations == 4 and result.converged is True
        assert column(result, "value") == [1, 1, 1, 1]
        assert result.frame.columns == ["value", "id"]


def test_back_chain_propagation_reads_destination_state(spark):
    program = (GraphAlgorithms(spark).pregel().max_iterations(100)
               .vertex_column("value", F.when(F.col("id") == 4, 1).otherwise(0),
                              F.when(msg() > F.col("value"), msg()).otherwise(F.col("value")))
               .vote_to_halt("active", F.col("value") != msg())
               .message(dst("value"), "dst_to_src").aggregate(F.max(msg())))
    with program.run(*chain(spark)) as result:
        assert result.iterations == 4 and result.converged is True
        assert column(result, "value") == [1, 1, 1, 1]


def test_reaching_the_cap_with_an_active_vote_reports_not_converged(spark):
    program = (GraphAlgorithms(spark).pregel().max_iterations(2)
               .vertex_column("value", F.when(F.col("id") == 1, 1).otherwise(0),
                              F.when(msg() > F.col("value"), msg()).otherwise(F.col("value")))
               .vote_to_halt("active", F.col("value") != msg())
               .message(src("value"), "src_to_dst").aggregate(F.max(msg())).skip_destination_state())
    with program.run(*chain(spark)) as result:
        assert result.iterations == 2 and result.converged is False
        assert column(result, "value") == [1, 1, 1, 0]


def test_several_named_messages_one_aggregate(spark):
    program = (GraphAlgorithms(spark).pregel().max_iterations(1)
               .vertex_column("va", F.lit(0).cast("long"), F.col("va") + F.coalesce(msg(), F.lit(0)))
               .message(F.lit(1).cast("long"), "src_to_dst", name="a")
               .message(F.lit(10).cast("long"), "src_to_dst", name="b")
               .aggregate(F.sum(msg("a"))).skip_destination_state())
    with program.run(*frames(spark, [1, 2, 3], [(1, 2), (1, 3)])) as result:
        assert column(result, "va") == [0, 1, 1]


def test_several_named_messages_several_aggregates(spark):
    program = (GraphAlgorithms(spark).pregel().max_iterations(1)
               .vertex_column("va", F.lit(0).cast("long"), F.col("va") + F.coalesce(msg("a"), F.lit(0)))
               .vertex_column("vb", F.lit(0).cast("long"), F.col("vb") + F.coalesce(msg("b"), F.lit(0)))
               .message(F.lit(1).cast("long"), "src_to_dst", name="a")
               .message(F.lit(10).cast("long"), "src_to_dst", name="b")
               .aggregate(F.sum(msg("a")), name="a").aggregate(F.max(msg("b")), name="b")
               .skip_destination_state())
    with program.run(*frames(spark, [1, 2, 3], [(1, 2), (1, 3)])) as result:
        assert column(result, "va") == [0, 1, 1]
        assert column(result, "vb") == [0, 10, 10]


def test_forty_bidirectional_iterations_on_a_ring(spark):
    n = 100
    edges = [(i, (i + 1) % n) for i in range(n)] + [(i, (i + n - 1) % n) for i in range(n)]
    program = (GraphAlgorithms(spark).pregel().max_iterations(40)
               .vertex_column("value", F.lit(0), F.col("value") + msg())
               .message(F.lit(1), "bidirectional").aggregate(F.sum(msg())).skip_destination_state())
    with program.run(*frames(spark, list(range(n)), edges)) as result:
        assert set(column(result, "value")) == {160}


def test_participation_shrinks_the_sources_and_vertex_attributes_are_readable(spark):
    events = []
    vertices = spark.createDataFrame([(1, 5), (2, 0), (3, 0)], "id long, seed long")
    edges = spark.createDataFrame([(1, 2), (2, 3)], "src long, dst long")
    program = (GraphAlgorithms(spark, observer=events.append).pregel(algorithm="spread").max_iterations(5)
               .vertex_column("value", F.col("seed"), F.greatest(F.col("value"), F.coalesce(msg(), F.lit(0))))
               .participation("changed", F.col("seed") > 0, F.coalesce(msg(), F.lit(0)) > F.col("value"))
               .vote_to_halt("active", F.coalesce(msg(), F.lit(0)) > F.col("value"))
               .message(src("value"), "src_to_dst").aggregate(F.max(msg())).skip_destination_state())
    with program.run(vertices, edges) as result:
        assert column(result, "value") == [5, 5, 5]
        assert result.algorithm == "spread" and result.converged is True and result.iterations == 3
    assert [event.frontier_size for event in events if event.kind == "iteration_end"] == [1, 1, 0]


def test_program_arguments(spark):
    graph = GraphAlgorithms(spark)
    inputs = frames(spark, [1, 2], [(1, 2)])
    with pytest.raises(ValueError, match="at least one message"):
        graph.pregel().max_iterations(1).run(*inputs)
    with pytest.raises(ValueError, match="need an aggregate"):
        graph.pregel().max_iterations(1).message(F.lit(1), "src_to_dst", name="a").message(
            F.lit(1), "src_to_dst", name="b").run(*inputs)
    with pytest.raises(ValueError, match="max_iterations or a vote"):
        graph.pregel().message(F.lit(1), "src_to_dst").run(*inputs)
    with pytest.raises(ValueError, match="negative"):
        graph.pregel().max_iterations(-1)
