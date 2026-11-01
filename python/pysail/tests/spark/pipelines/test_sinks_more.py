"""SinkExecutionSuite's memory sink and SystemMetadataSuite's console sink, through the public client.

MemoryStream is unavailable through Connect, so the source is a parquet table.
"""

import pyspark.pipelines as dp
import pytest
from pyspark.pipelines.add_pipeline_analysis_context import add_pipeline_analysis_context
from pyspark.pipelines.graph_element_registry import graph_element_registration_context
from pyspark.pipelines.spark_connect_graph_element_registry import SparkConnectGraphElementRegistry
from pyspark.pipelines.spark_connect_pipeline import create_dataflow_graph, start_run


class Sinks:
    def __init__(self, spark, schema, root):
        self.spark = spark
        self.schema = schema
        self.root = root
        spark.sql(f"CREATE TABLE {schema}.src (value INT) USING parquet")
        spark.sql(f"INSERT INTO {schema}.src VALUES (1), (2), (3)")

    def run(self, sink, flow, sink_format, *, full_all=False):
        graph = create_dataflow_graph(self.spark, self.spark.catalog.currentCatalog(), self.schema, None)
        registry = SparkConnectGraphElementRegistry(self.spark, graph)
        spark, source = self.spark, f"{self.schema}.src"
        with (
            add_pipeline_analysis_context(spark=spark, dataflow_graph_id=graph, flow_name=None),
            graph_element_registration_context(registry),
        ):

            @dp.temporary_view
            def a():
                return spark.readStream.table(source)

            dp.create_sink(sink, format=sink_format)

            @dp.append_flow(target=sink, name=flow)
            def writer():
                return spark.readStream.table("a")

        stream = start_run(spark, graph, None, full_all, None, False, self.root.as_uri())
        return [item["pipeline_event_result"].event.message for item in stream if "pipeline_event_result" in item]

    def versions(self, *parts):
        return sorted(path.name for path in self.root.joinpath("_checkpoints", *parts).glob("*"))


@pytest.fixture
def sinks(spark, tmp_path, request):
    schema = "sk_" + "".join(c if c.isalnum() else "_" for c in request.node.name).lower()[:48]
    spark.sql(f"DROP SCHEMA IF EXISTS {schema} CASCADE")
    spark.sql(f"CREATE SCHEMA {schema}")
    yield Sinks(spark, schema, tmp_path / "storage")
    spark.sql(f"DROP SCHEMA IF EXISTS {schema} CASCADE")


def test_writing_to_an_external_memory_sink(sinks):
    messages = sinks.run("sink_a", "flow_to_sink_a", "memory")
    assert messages[-1].startswith("Run is COMPLETED"), messages
    assert sinks.versions("sink_a", "flow_to_sink_a") == ["0"]
    assert sorted(tuple(row) for row in sinks.spark.sql("SELECT * FROM flow_to_sink_a").collect()) == [(1,), (2,), (3,)]


def test_a_console_sink_flow_checkpoint_follows_full_refresh(sinks):
    messages = sinks.run("sink", "sink", "console")
    assert messages[-1].startswith("Run is COMPLETED"), messages
    assert sinks.versions("sink", "sink") == ["0"]
    messages = sinks.run("sink", "sink", "console", full_all=True)
    assert messages[-1].startswith("Run is COMPLETED"), messages
    assert sinks.versions("sink", "sink") == ["0", "1"]
