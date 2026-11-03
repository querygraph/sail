"""Relational graph results, physical placement and explicit native snapshots."""
from pathlib import Path

import pytest
from pyspark.sql import functions as F
from pyspark.sql.connect.session import SparkSession
from sail_nutmeg import Nutmeg

from conftest import start_server


def multigraph(spark):
    nodes = spark.createDataFrame([(0, "a"), (1, "b"), (2, "c"), (3, "isolate")],
                                  "node_id long, label string").repartition(4)
    edges = spark.createDataFrame([(0, 1), (0, 1), (1, 2), (2, 0)],
                                  "source long, target long").repartition(4)
    return nodes, edges


def check_relational_graph(spark):
    nm = Nutmeg(spark)
    nodes, edges = multigraph(spark)
    graph = nm.tables(nodes, edges).validate()
    degree = graph.degrees().orderBy("nodeId").collect()
    assert [tuple(row) for row in degree] == [(0, 2, 1, 3), (1, 1, 2, 3), (2, 1, 1, 2), (3, 0, 0, 0)]
    walks = graph.walks(2).groupBy("source", "target").count().orderBy("source", "target").collect()
    assert [tuple(row) for row in walks] == [(0, 2, 2), (1, 0, 1), (2, 1, 2)]
    assert graph.closed_walks(3).count() == 6
    triplets = graph.triplets().select("src.label", "dst.label").orderBy("src.label", "dst.label").collect()
    assert [tuple(row) for row in triplets] == [("a", "b"), ("a", "b"), ("b", "c"), ("c", "a")]
    graph.degrees().createOrReplaceTempView("relational_graph_degree")
    plan = "\n".join(row[0] for row in spark.sql("EXPLAIN SELECT * FROM relational_graph_degree").collect())
    assert "AggregateExec" in plan and "HashJoinExec" in plan
    assert "DriverExtensionExec" not in plan and "NutmegAlgorithmExec" not in plan
    return graph


def test_graph_tables_use_host_relational_plans(spark):
    graph = check_relational_graph(spark)
    graph.nodes.createOrReplaceTempView("canonical_nodes")
    graph.edges.createOrReplaceTempView("canonical_edges")
    assert Nutmeg(spark).tables("canonical_nodes", "canonical_edges").out_degrees().count() == 4


def test_graph_tables_work_without_native_extensions(request, tmp_path):
    binary = str(Path(request.config.getoption("--sail-binary")).resolve())
    mode = request.config.getoption("--execution-mode")
    with start_server(binary, tmp_path / "server", mode=mode,
                      extra_env={"SAIL_EXPERIMENTAL_EXTENSIONS": "0"}) as endpoint:
        spark = SparkSession.builder.remote(endpoint).create()
        try:
            check_relational_graph(spark)
        finally:
            spark.stop()


def test_native_snapshot_is_explicit_and_scannable(spark):
    nm = Nutmeg(spark)
    graph = nm.tables(*multigraph(spark))
    nm.stage("relational_snapshot", graph.nodes, graph.edges)
    assert nm.status()["graphs"][0]["projections"] == 0
    nodes, edges = nm.nodes("relational_snapshot"), nm.edges("relational_snapshot")
    assert nodes.count() == 4 and edges.count() == 4
    degree = nm.tables(nodes, edges).out_degrees().orderBy("nodeId").collect()
    assert [tuple(row) for row in degree] == [("0", 2), ("1", 1), ("2", 1), ("3", 0)]
    assert nodes.filter(F.col("label") == "isolate").count() == 1
    assert nm.status()["graphs"][0]["projections"] == 0
    assert nm.run("relational_snapshot", "degree").count() == 4
    assert nm.status()["graphs"][0]["projections"] == 1
    assert nm.run("relational_snapshot", "degree").count() == 4
    assert nm.status()["graphs"][0]["projections"] == 1


def test_empty_native_scans_keep_declared_properties(spark):
    nm = Nutmeg(spark)
    nm.stage("empty_properties", spark.createDataFrame([], "node_id string, score long"),
             spark.createDataFrame([], "source string, target string, weight double"))
    nodes, edges = nm.nodes("empty_properties"), nm.edges("empty_properties")
    assert {"node_id", "label", "property.score", "present.score"} <= set(nodes.columns)
    assert {"source", "target", "property.weight", "present.weight"} <= set(edges.columns)
    assert nodes.count() == 0 and edges.count() == 0
    assert nm.status()["graphs"][0]["projections"] == 0


def test_empty_graph_and_self_loops(spark):
    nm = Nutmeg(spark)
    nodes = spark.createDataFrame([], "node_id long")
    edges = spark.createDataFrame([], "source long, target long")
    empty = nm.tables(nodes, edges).validate()
    assert empty.degrees().count() == 0 and empty.walks(2).count() == 0
    assert empty.triplets().count() == 0
    loop = nm.tables(spark.createDataFrame([(1,)], "node_id long"),
                     spark.createDataFrame([(1, 1), (1, 1)], "source long, target long"))
    assert [tuple(row) for row in loop.degrees().collect()] == [(1, 2, 2, 4)]
    assert loop.closed_walks(2).count() == 4


@pytest.mark.parametrize("nodes,edges,error", [
    ([(None,)], [], "must not be null"),
    ([(1,), (1,)], [], "must be unique"),
    ([(1,)], [(1, None)], "must not be null"),
    ([(1,)], [(2, 1)], "source endpoint"),
    ([(1,)], [(1, 2)], "target endpoint"),
])
def test_graph_validation_rejects_invalid_relations(spark, nodes, edges, error):
    graph = Nutmeg(spark).tables(spark.createDataFrame(nodes, "node_id long"),
                                 spark.createDataFrame(edges, "source long, target long"))
    with pytest.raises(ValueError, match=error):
        graph.validate()
