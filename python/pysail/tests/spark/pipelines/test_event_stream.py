"""PipelineEventStreamSuite, through the public client."""

import re

import pytest
from pyspark.errors.exceptions.connect import AnalysisException
from pyspark.pipelines.spark_connect_graph_element_registry import SparkConnectGraphElementRegistry
from pyspark.pipelines.spark_connect_pipeline import create_dataflow_graph, start_run
from pyspark.sql.connect import proto as pb


class Events:
    def __init__(self, spark, schema, storage):
        self.spark = spark
        self.schema = schema
        self.storage = storage
        self.catalog = spark.catalog.currentCatalog()

    def graph(self, *outputs):
        graph = create_dataflow_graph(self.spark, self.catalog, self.schema, None)
        for name, kind, query in outputs:
            command = pb.Command()
            command.pipeline_command.define_output.CopyFrom(
                pb.PipelineCommand.DefineOutput(dataflow_graph_id=graph, output_name=name, output_type=kind)
            )
            self.spark.client.execute_command(command)
        for name, _, query in outputs:
            command = pb.Command()
            command.pipeline_command.define_flow.CopyFrom(
                pb.PipelineCommand.DefineFlow(
                    dataflow_graph_id=graph,
                    flow_name=name,
                    target_dataset_name=name,
                    relation_flow_details=pb.PipelineCommand.DefineFlow.WriteRelationFlowDetails(
                        relation=pb.Relation(sql=pb.SQL(query=query))
                    ),
                )
            )
            self.spark.client.execute_command(command)
        return graph

    def collect(self, graph, dry, into):
        for item in start_run(self.spark, graph, None, False, None, dry, self.storage):
            if "pipeline_event_result" in item:
                into.append(item["pipeline_event_result"].event.message)
        return into

    def full(self, name):
        return f"{self.catalog}.{self.schema}.{name}"


@pytest.fixture
def events(spark, tmp_path, request):
    schema = "ev_" + "".join(c if c.isalnum() else "_" for c in request.node.name).lower()[:48]
    spark.sql(f"DROP SCHEMA IF EXISTS {schema} CASCADE")
    spark.sql(f"CREATE SCHEMA {schema}")
    yield Events(spark, schema, (tmp_path / "storage").as_uri())
    spark.sql(f"DROP SCHEMA IF EXISTS {schema} CASCADE")


def healthy():
    return [
        ("a", pb.MATERIALIZED_VIEW, "SELECT * FROM RANGE(5)"),
        ("b", pb.TABLE, "SELECT * FROM STREAM a"),
    ]


def test_expected_events_are_streamed_back_to_the_client(events):
    messages = events.collect(events.graph(*healthy()), False, [])
    a, b = events.full("a"), events.full("b")
    for expected in [
        f"Flow {a} is QUEUED",
        f"Flow {b} is QUEUED",
        f"Flow {a} is PLANNING",
        f"Flow {a} is STARTING",
        f"Flow {a} is RUNNING",
        f"Flow {a} has COMPLETED",
        f"Flow {b} is STARTING",
        f"Flow {b} is RUNNING",
        f"Flow {b} has COMPLETED",
        "Run is COMPLETED",
    ]:
        assert any(expected in message for message in messages), f"{expected} not in {messages}"


@pytest.mark.parametrize("dry", [True, False])
def test_flow_resolution_failure(events, dry):
    graph = events.graph(
        ("a", pb.MATERIALIZED_VIEW, "SELECT * FROM unknown_table"),
        ("b", pb.TABLE, "SELECT * FROM STREAM a"),
    )
    messages = []
    with pytest.raises(AnalysisException) as error:
        events.collect(graph, dry, messages)
    summary = "Failed to resolve flows in the pipeline"
    assert summary in error.value.getMessage()
    read_failure = re.compile(rf"Failed to resolve flow.*Failed to read dataset '{re.escape(events.full('a'))}'", re.S)
    not_found = re.compile(r"Failed to resolve flow.*TABLE_OR_VIEW_NOT_FOUND", re.S)
    for pattern in [read_failure, not_found]:
        assert any(pattern.search(message) for message in messages), f"{pattern.pattern} not in {messages}"
    # The error that fails the run is not surfaced twice.
    assert not any(summary in message for message in messages)


def test_successful_dry_run(events):
    messages = events.collect(events.graph(*healthy()), True, [])
    assert any("Run is COMPLETED" in message for message in messages), messages
    assert not any("is QUEUED" in message for message in messages)
