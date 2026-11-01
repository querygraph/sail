"""SparkDeclarativePipelinesServerSuite, through the public client.

The Scala suite resolves the graph held by the server and counts its flows. Here the same
commands are sent over the wire, and the flows are read from the run's events.
"""

from pathlib import Path
from uuid import uuid4

import grpc
import pytest

from pysail.testing.spark.utils.common import is_jvm_spark
from pyspark.errors.exceptions.connect import AnalysisException
from pyspark.pipelines.spark_connect_graph_element_registry import SparkConnectGraphElementRegistry
from pyspark.pipelines.spark_connect_pipeline import create_dataflow_graph, start_run
from pyspark.sql.connect import proto as pb


def sql_relation(query):
    return pb.Relation(sql=pb.SQL(query=query))


def range_relation(end):
    literal = pb.Expression(literal=pb.Expression.Literal(integer=end))
    return pb.Relation(
        unresolved_table_valued_function=pb.UnresolvedTableValuedFunction(function_name="range", arguments=[literal])
    )


class Server:
    def __init__(self, spark, schema, storage):
        self.spark = spark
        self.schema = schema
        self.storage = storage
        self.catalog = spark.catalog.currentCatalog()

    def graph(self):
        return create_dataflow_graph(self.spark, self.catalog, self.schema, None)

    def output(self, graph, name, kind, relation):
        """TestPipelineDefinition.createTable and createView: an output and its implicit flow."""
        command = pb.Command()
        command.pipeline_command.define_output.CopyFrom(
            pb.PipelineCommand.DefineOutput(dataflow_graph_id=graph, output_name=name, output_type=kind)
        )
        self.spark.client.execute_command(command)
        command = pb.Command()
        command.pipeline_command.define_flow.CopyFrom(
            pb.PipelineCommand.DefineFlow(
                dataflow_graph_id=graph,
                flow_name=name,
                target_dataset_name=name,
                relation_flow_details=pb.PipelineCommand.DefineFlow.WriteRelationFlowDetails(relation=relation),
            )
        )
        self.spark.client.execute_command(command)

    def run(self, graph, *, dry=False):
        stream = start_run(self.spark, graph, None, False, None, dry, self.storage)
        return [item["pipeline_event_result"].event.message for item in stream if "pipeline_event_result" in item]

    def full(self, name):
        return f"{self.catalog}.{self.schema}.{name}"

    def rows(self, table):
        return sorted(tuple(row) for row in self.spark.table(f"{self.schema}.{table}").collect())


@pytest.fixture
def server(spark, tmp_path, request):
    schema = "sr_" + "".join(c if c.isalnum() else "_" for c in request.node.name).lower()[:48]
    spark.sql(f"DROP SCHEMA IF EXISTS {schema} CASCADE")
    spark.sql(f"CREATE SCHEMA {schema}")
    yield Server(spark, schema, (tmp_path / "storage").as_uri())
    spark.sql(f"DROP SCHEMA IF EXISTS {schema} CASCADE")


def test_cross_dependency_between_sql_and_non_sql_datasets(server):
    """The Scala graph only resolves: f reads mv, and mv2 has two flows, which validation then refuses."""
    graph = server.graph()
    server.output(graph, "mv", pb.MATERIALIZED_VIEW, range_relation(5))
    registry = SparkConnectGraphElementRegistry(server.spark, graph)
    registry.register_sql(
        "CREATE MATERIALIZED VIEW mv2 AS SELECT 2;\nCREATE FLOW f AS INSERT INTO mv2 BY NAME SELECT * FROM mv\n",
        Path("table.sql"),
    )
    messages = []
    with pytest.raises(AnalysisException) as error:
        messages = server.run(graph, dry=True)
    assert error.value.getCondition() == "MATERIALIZED_VIEW_WITH_MULTIPLE_QUERIES"
    assert error.value.getMessageParameters() == {
        "tableName": server.full("mv2"),
        "flows": f"{server.full('f')}, {server.full('mv2')}",
    }
    assert not [message for message in messages if "Failed to resolve flow" in message]


def test_cross_dependency_runs_when_the_target_takes_flows(server):
    """The same SQL flow over a non-SQL dataset, into a target that validation accepts."""
    graph = server.graph()
    server.output(graph, "mv", pb.MATERIALIZED_VIEW, range_relation(5))
    registry = SparkConnectGraphElementRegistry(server.spark, graph)
    registry.register_sql(
        "CREATE STREAMING TABLE st;\nCREATE FLOW f AS INSERT INTO st BY NAME SELECT * FROM STREAM mv\n",
        Path("table.sql"),
    )
    messages = server.run(graph)
    for name in ["mv", "f"]:
        assert any(f"Flow {server.full(name)} is QUEUED" in message for message in messages), messages
    assert server.rows("st") == [(i,) for i in range(5)]


def test_simple_graph_resolution(server):
    """tableC reads no stream, so the graph resolves and a dry run then refuses a batch streaming table."""
    graph = server.graph()
    server.output(graph, "tableA", pb.MATERIALIZED_VIEW, sql_relation("SELECT * FROM RANGE(5)"))
    server.output(graph, "viewB", pb.TEMPORARY_VIEW, sql_relation("SELECT * FROM tableA"))
    server.output(graph, "tableC", pb.TABLE, sql_relation("SELECT * FROM tableA, viewB"))
    messages = []
    with pytest.raises(AnalysisException) as error:
        messages = server.run(graph, dry=True)
    assert error.value.getCondition() == "INVALID_FLOW_QUERY_TYPE.BATCH_RELATION_FOR_STREAMING_TABLE"
    assert not [message for message in messages if "Failed to resolve flow" in message]
    assert not server.spark.catalog.tableExists(f"{server.schema}.tableA")


def test_execute_pipeline_end_to_end(server):
    graph = server.graph()
    server.output(graph, "tableA", pb.MATERIALIZED_VIEW, sql_relation("SELECT * FROM RANGE(5)"))
    server.output(graph, "tableB", pb.TABLE, sql_relation("SELECT * FROM STREAM tableA"))
    server.output(graph, "tableC", pb.MATERIALIZED_VIEW, sql_relation("SELECT * FROM tableB"))
    server.run(graph)
    for table in ["tableA", "tableB", "tableC"]:
        assert server.spark.table(f"{server.catalog}.{server.schema}.{table}").count() == 5


def test_streaming_tables_materialized_views_and_temporary_views(server, spark):
    current = f"{server.schema}_curr"
    other = f"{server.schema}_other"
    for schema in [current, other]:
        spark.sql(f"CREATE SCHEMA IF NOT EXISTS {schema}")
    try:
        graph = server.graph()
        server.output(graph, f"{current}.tableA", pb.MATERIALIZED_VIEW, sql_relation("SELECT * FROM RANGE(5)"))
        server.output(graph, f"{current}.tableB", pb.TABLE, sql_relation(f"SELECT * FROM STREAM {current}.tableA"))
        server.output(graph, "viewC", pb.TEMPORARY_VIEW, sql_relation(f"SELECT * FROM {current}.tableB"))
        server.output(graph, f"{other}.tableD", pb.MATERIALIZED_VIEW, sql_relation("SELECT * FROM viewC"))
        server.run(graph)
        for table in [f"{current}.tableA", f"{current}.tableB", f"{other}.tableD"]:
            assert spark.table(f"{server.catalog}.{table}").count() == 5
    finally:
        for schema in [current, other]:
            spark.sql(f"DROP SCHEMA IF EXISTS {schema} CASCADE")


def execute(stub, session_id, user_id, command):
    request = pb.ExecutePlanRequest(
        session_id=session_id,
        user_context=pb.UserContext(user_id=user_id),
        operation_id=str(uuid4()),
        plan=pb.Plan(command=command),
    )
    return list(stub.ExecutePlan(request))


def define(graph):
    command = pb.Command()
    command.pipeline_command.define_output.CopyFrom(
        pb.PipelineCommand.DefineOutput(dataflow_graph_id=graph, output_name="test_table", output_type=pb.MATERIALIZED_VIEW)
    )
    return command


@pytest.mark.xfail(not is_jvm_spark(), strict=True, reason="Sail reports a closed session without INVALID_HANDLE.SESSION_CLOSED")
def test_dataflow_graphs_are_cleaned_up_when_session_is_closed(spark):
    """The registry is internal. A client sees the closed session refused, and its graph gone for a new one."""
    stub = spark.client._stub
    user = "test_user"
    session = str(uuid4())
    create = pb.Command()
    create.pipeline_command.create_dataflow_graph.SetInParent()
    responses = execute(stub, session, user, create)
    graph = next(
        r.pipeline_command_result.create_dataflow_graph_result.dataflow_graph_id
        for r in responses
        if r.HasField("pipeline_command_result")
    )
    assert any(r.HasField("pipeline_command_result") for r in execute(stub, session, user, define(graph)))
    stub.ReleaseSession(pb.ReleaseSessionRequest(session_id=session, user_context=pb.UserContext(user_id=user)))
    with pytest.raises(grpc.RpcError) as error:
        execute(stub, session, user, define(graph))
    assert "INVALID_HANDLE.SESSION_CLOSED" in error.value.details()
    replacement = str(uuid4())
    try:
        with pytest.raises(grpc.RpcError) as error:
            execute(stub, replacement, user, define(graph))
        assert "DATAFLOW_GRAPH_NOT_FOUND" in error.value.details()
    finally:
        stub.ReleaseSession(pb.ReleaseSessionRequest(session_id=replacement, user_context=pb.UserContext(user_id=user)))
