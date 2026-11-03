import math
import pytest
from pyspark.sql import functions as F
from pyspark.sql.connect.session import SparkSession
from sail_nutmeg.client import Nutmeg


def cycle(spark):
    nodes = spark.range(0, 3, numPartitions=4).select(F.col("id").cast("string").alias("node_id"))
    edges = spark.range(0, 3, numPartitions=4).select(F.col("id").cast("string").alias("source"), ((F.col("id") + 1) % 3).cast("string").alias("target"))
    return nodes, edges


def test_all_partitions_schema_and_algorithms(spark):
    nm = Nutmeg(spark)
    nodes, edges = cycle(spark)
    staged = nm.stage("cycle", nodes, edges)
    assert staged.nodeCount == 3 and staged.edgeCount == 3 and staged.revision == 1
    result = nm.run("cycle", "pagerank")
    assert "nodeId" in result.columns and "score" in result.columns
    rows = result.select("nodeId", "score").orderBy("nodeId").collect()
    assert [r.nodeId for r in rows] == ["0", "1", "2"]
    # Symmetric directed cycle: stationary normalized PageRank is exactly 1/3.
    assert all(math.isclose(r.score, 1 / 3, abs_tol=1e-10) for r in rows)
    degree = nm.run("cycle", "degree").orderBy("nodeId").collect()
    assert [(row.nodeId, row.degree) for row in degree] == [("0", 1), ("1", 1), ("2", 1)]
    assert len(nm.run("cycle", "wcc").collect()) == 3
    assert result.filter(F.col("score") > 0).join(nodes, result.nodeId == nodes.node_id).count() == 3
    assert nm.drop("cycle").dropped
    with pytest.raises(Exception, match="no graph named `cycle`"):
        nm.run("cycle", "degree").collect()


def test_analysis_has_no_mutation_and_bad_overwrite_is_atomic(spark):
    nm = Nutmeg(spark)
    nodes, edges = cycle(spark)
    lazy_stage = nm._relation("stage", "analysis", inputs=(nodes, edges))
    assert lazy_stage.columns == ["graph", "nodeCount", "edgeCount", "revision"]
    lazy_stage.explain()
    with pytest.raises(Exception, match="no graph named `analysis`"):
        nm.run("analysis", "degree").collect()
    lazy_stage.collect()
    before = nm.run("analysis", "degree").orderBy("nodeId").collect()
    invalid = spark.createDataFrame([(None, "0")], "source string, target string")
    with pytest.raises(Exception):
        nm.stage("analysis", nodes, invalid)
    assert nm.run("analysis", "degree").orderBy("nodeId").collect() == before
    drop = nm._relation("drop", "analysis")
    _ = drop.schema
    assert nm.run("analysis", "degree").count() == 3
    assert drop.collect()[0].dropped


def test_same_graph_name_is_session_scoped(spark, endpoint):
    other = SparkSession.builder.remote(endpoint).create()
    try:
        a, b = Nutmeg(spark), Nutmeg(other)
        a.stage("same", *cycle(spark))
        b.stage("same", other.createDataFrame([("solo",)], "node_id string"), other.createDataFrame([], "source string,target string"))
        assert a.run("same", "degree").count() == 3
        assert [r.nodeId for r in b.run("same", "degree").collect()] == ["solo"]
        b.drop("same")
        assert a.run("same", "degree").count() == 3
    finally:
        other.stop()


def test_invalid_payload_and_options_are_named(spark):
    nm = Nutmeg(spark)
    with pytest.raises(Exception, match="two inputs"):
        nm._relation("stage", "bad").collect()
    nm.stage("valid", *cycle(spark))
    with pytest.raises(Exception, match="unknown|unsupported|not found"):
        nm.run("valid", "does_not_exist").collect()
    with pytest.raises(Exception, match="dampng"):
        nm.run("valid", "pagerank", dampng=0.8).collect()


def test_early_limit_then_full_read(spark):
    nm = Nutmeg(spark)
    nodes = spark.range(0, 50, numPartitions=4).select(F.col("id").cast("string").alias("node_id"))
    edges = spark.range(0, 49, numPartitions=4).select(F.col("id").cast("string").alias("source"), (F.col("id") + 1).cast("string").alias("target"))
    nm.stage("early", nodes, edges)
    assert len(nm.run("early", "allPairsShortestPaths").limit(1).collect()) == 1
    # Native FFI tests separately prove kernel termination/accounting release.
    assert nm.run("early", "degree").count() == 50
