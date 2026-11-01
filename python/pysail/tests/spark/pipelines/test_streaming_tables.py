"""Triggered streaming tables across runs, measured on Spark 4.2.1 and on Sail."""

import json
from pathlib import Path

import pytest
from pyspark.errors.exceptions.connect import AnalysisException
from pyspark.pipelines.spark_connect_graph_element_registry import SparkConnectGraphElementRegistry
from pyspark.pipelines.spark_connect_pipeline import create_dataflow_graph, start_run

from pysail.testing.spark.utils.common import is_jvm_spark

GRAPH = "CREATE STREAMING TABLE b AS SELECT id, id * 10 AS x FROM STREAM(src)"


@pytest.fixture
def space(spark, tmp_path, request):
    schema = "st_" + "".join(c if c.isalnum() else "_" for c in request.node.name).lower()[:48]
    spark.sql(f"DROP SCHEMA IF EXISTS {schema} CASCADE")
    spark.sql(f"CREATE SCHEMA {schema}")
    spark.sql(f"CREATE TABLE {schema}.src(id INT) USING parquet")
    spark.sql(f"INSERT INTO {schema}.src VALUES (1), (2)")
    yield Space(spark, schema, tmp_path / "storage")
    spark.sql(f"DROP SCHEMA IF EXISTS {schema} CASCADE")


class Space:
    def __init__(self, spark, schema, storage):
        self.spark = spark
        self.schema = schema
        self.storage = storage.as_uri()
        self.path = storage

    def run(self, sql, *, full_refresh_all=False, full_refresh=None, refresh=None, storage=None):
        graph = create_dataflow_graph(self.spark, self.spark.catalog.currentCatalog(), self.schema, None)
        registry = SparkConnectGraphElementRegistry(self.spark, graph)
        registry.register_sql(sql, Path("pipeline.sql"))
        list(start_run(self.spark, graph, full_refresh, full_refresh_all, refresh, False, storage or self.storage))

    def rows(self, table):
        return sorted(tuple(row) for row in self.spark.table(f"{self.schema}.{table}").collect())

    def insert(self, *values):
        joined = ", ".join(f"({value})" for value in values)
        self.spark.sql(f"INSERT INTO {self.schema}.src VALUES {joined}")


def test_a_run_reads_what_exists(space):
    space.run(GRAPH)
    assert space.rows("b") == [(1, 10), (2, 20)]


def test_a_second_run_with_no_new_data_adds_nothing(space):
    space.run(GRAPH)
    space.run(GRAPH)
    assert space.rows("b") == [(1, 10), (2, 20)]


def test_a_later_run_reads_only_new_data(space):
    space.run(GRAPH)
    space.insert(3)
    space.run(GRAPH)
    assert space.rows("b") == [(1, 10), (2, 20), (3, 30)]


def test_full_refresh_replaces_the_table_from_a_new_checkpoint(space):
    space.run(GRAPH)
    space.insert(3)
    space.run(GRAPH)
    space.run(GRAPH, full_refresh_all=True)
    assert space.rows("b") == [(1, 10), (2, 20), (3, 30)]
    flow = space.path / "_checkpoints"
    versions = {path.name for path in flow.glob("*/*/b/b/*")}
    assert versions == {"0", "1"}


def test_a_full_refresh_is_incremental_afterwards(space):
    space.run(GRAPH, full_refresh_all=True)
    space.insert(3)
    space.run(GRAPH)
    assert space.rows("b") == [(1, 10), (2, 20), (3, 30)]


def test_full_refresh_of_a_reset_protected_table_keeps_its_data(space):
    graph = (
        "CREATE STREAMING TABLE b TBLPROPERTIES ('pipelines.reset.allowed' = 'false') "
        "AS SELECT id, id * 10 AS x FROM STREAM(src)"
    )
    space.run(graph)
    space.insert(3)
    space.run(graph, full_refresh_all=True)
    assert space.rows("b") == [(1, 10), (2, 20), (3, 30)]


def test_overwriting_a_source_adds_only_the_new_rows(space):
    space.run(GRAPH)
    space.spark.sql(f"INSERT OVERWRITE {space.schema}.src VALUES (7)")
    space.run(GRAPH)
    assert space.rows("b") == [(1, 10), (2, 20), (7, 70)]


def test_a_stream_of_a_materialized_view(space):
    space.run(
        "CREATE MATERIALIZED VIEW mv AS SELECT id FROM src;"
        "CREATE STREAMING TABLE c AS SELECT id FROM STREAM(mv)"
    )
    assert space.rows("c") == [(1,), (2,)]


def test_a_chain_of_streaming_tables(space):
    space.run(
        "CREATE STREAMING TABLE d AS SELECT id FROM STREAM(src);"
        "CREATE STREAMING TABLE e AS SELECT id FROM STREAM(d)"
    )
    assert space.rows("e") == [(1,), (2,)]


def test_a_stream_static_join_reads_the_latest_static_side(space):
    graph = "CREATE STREAMING TABLE j AS SELECT s.id, t.id AS t FROM STREAM(src) s JOIN src t ON s.id = t.id"
    space.run(graph)
    assert space.rows("j") == [(1, 1), (2, 2)]
    space.insert(3)
    space.run(graph)
    assert space.rows("j") == [(1, 1), (2, 2), (3, 3)]


def test_a_batch_query_cannot_feed_a_streaming_table(space):
    with pytest.raises(AnalysisException) as error:
        space.run("CREATE STREAMING TABLE g AS SELECT id FROM src")
    assert error.value.getCondition() == "INVALID_FLOW_QUERY_TYPE.BATCH_RELATION_FOR_STREAMING_TABLE"


def test_an_aggregate_without_a_watermark_is_refused(space):
    with pytest.raises(AnalysisException) as error:
        space.run("CREATE STREAMING TABLE h AS SELECT id, count(*) AS c FROM STREAM(src) GROUP BY id")
    assert error.value.getCondition() == "STREAMING_OUTPUT_MODE.UNSUPPORTED_OPERATION"
    assert "not supported for streaming aggregations without watermark" in str(error.value)


def test_each_flow_keeps_its_own_progress(space):
    graph = (
        "CREATE STREAMING TABLE f;"
        "CREATE FLOW f1 AS INSERT INTO f BY NAME SELECT id FROM STREAM(src)"
    )
    space.run(graph)
    space.insert(3)
    space.run(graph)
    assert space.rows("f") == [(1,), (2,), (3,)]


@pytest.mark.skipif(is_jvm_spark(), reason="Spark's concurrent flows lose one flow's rows here")
def test_two_flows_into_one_table_both_land(space):
    space.run(
        "CREATE STREAMING TABLE f;"
        "CREATE FLOW f1 AS INSERT INTO f BY NAME SELECT id FROM STREAM(src);"
        "CREATE FLOW f2 AS INSERT INTO f BY NAME SELECT id + 100 AS id FROM STREAM(src)"
    )
    assert space.rows("f") == [(1,), (2,), (101,), (102,)]


@pytest.mark.skipif(is_jvm_spark(), reason="Sail's checkpoint format")
def test_an_unfinished_batch_replays_exactly_its_files(space):
    space.run(GRAPH)
    space.insert(3)
    state_file = next(space.path.glob("_checkpoints/*/*/b/b/0/state.json"))
    state = json.loads(state_file.read_text())
    source = next(iter(state["seen"]))
    # A crash after recording the batch: these files are not marked read.
    state["pending"] = {source: sorted(state["seen"][source])}
    state["seen"][source] = []
    state_file.write_text(json.dumps(state))
    space.run(GRAPH)
    # The replay is at-least-once, and the new row waits for the next run.
    assert space.rows("b") == [(1, 10), (1, 10), (2, 20), (2, 20)]
    space.run(GRAPH)
    assert space.rows("b") == [(1, 10), (1, 10), (2, 20), (2, 20), (3, 30)]
