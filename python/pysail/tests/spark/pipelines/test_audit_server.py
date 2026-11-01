"""The Connect server, event and lifecycle suites, where the Scala tests call server internals.

Each test states the behavior a client sees for one case of SparkDeclarativePipelinesServerSuite,
PipelineEventSenderSuite, PipelinesHandlerSuite, PipelineUpdateContextImplSuite,
ConstructPipelineEventSuite, SparkConnectSessionHolderSuite or SparkConnectSessionManagerSuite.
"""

import threading
from pathlib import Path
from uuid import uuid4

import grpc
import pytest
from google.protobuf import any_pb2
from pysail.testing.spark.utils.common import is_jvm_spark
from pyspark.errors.exceptions import connect as errors
from pyspark.pipelines.spark_connect_graph_element_registry import SparkConnectGraphElementRegistry
from pyspark.pipelines.spark_connect_pipeline import create_dataflow_graph, start_run
from pyspark.sql.connect import proto as pb

ROOTS_ACCEPTED = [
    "file:///tmp/test",
    "hdfs://localhost:9000/pipelines",
    "s3a://my-bucket/pipelines",
    "abfss://container@account.dfs.core.windows.net/pipelines",
]
ROOTS_RELATIVE = ["relative/path", "./relative/path", "../relative/path", "pipelines"]
ROOTS_WITHOUT_SCHEME = ["/tmp/test", "/absolute/path", "/pipelines/storage"]
STORAGE_ROOT_INVALID = "PIPELINE_STORAGE_ROOT_INVALID"


class Audit:
    def __init__(self, spark, schema, storage):
        self.spark = spark
        self.schema = schema
        self.storage = storage
        self.catalog = spark.catalog.currentCatalog()

    def graph(self, sql=None):
        graph = create_dataflow_graph(self.spark, self.catalog, self.schema, None)
        if sql:
            registry = SparkConnectGraphElementRegistry(self.spark, graph)
            registry.register_sql(sql, Path("audit.sql"))
        return graph

    def define(self, graph, name="t"):
        command = pb.Command()
        command.pipeline_command.define_output.CopyFrom(
            pb.PipelineCommand.DefineOutput(dataflow_graph_id=graph, output_name=name, output_type=pb.MATERIALIZED_VIEW)
        )
        return self.spark.client.execute_command(command)

    def drop(self, graph):
        command = pb.Command()
        command.pipeline_command.drop_dataflow_graph.dataflow_graph_id = graph
        return self.spark.client.execute_command(command)

    def events(self, graph, *, dry=False, storage=None):
        stream = start_run(self.spark, graph, None, False, None, dry, storage or self.storage)
        return [item["pipeline_event_result"].event for item in stream if "pipeline_event_result" in item]

    def messages(self, graph, *, dry=False, storage=None):
        return [event.message for event in self.events(graph, dry=dry, storage=storage)]

    def full(self, name):
        return f"{self.catalog}.{self.schema}.{name}"

    def start_run_command(self, graph):
        command = pb.Command()
        run = command.pipeline_command.start_run
        run.dataflow_graph_id = graph
        run.storage = self.storage
        return command


@pytest.fixture
def audit(spark, tmp_path, request):
    schema = "as_" + "".join(c if c.isalnum() else "_" for c in request.node.name).lower()[:48]
    spark.sql(f"DROP SCHEMA IF EXISTS {schema} CASCADE")
    spark.sql(f"CREATE SCHEMA {schema}")
    yield Audit(spark, schema, (tmp_path / "storage").as_uri())
    spark.sql(f"DROP SCHEMA IF EXISTS {schema} CASCADE")


def execute_plan(stub, session, user, plan, extensions=()):
    context = pb.UserContext(user_id=user)
    context.extensions.extend(extensions)
    request = pb.ExecutePlanRequest(session_id=session, user_context=context, operation_id=str(uuid4()), plan=plan)
    return stub.ExecutePlan(request)


def analysis_context(graph, flow=None):
    context = pb.PipelineAnalysisContext(dataflow_graph_id=graph)
    if flow is not None:
        context.flow_name = flow
    packed = any_pb2.Any()
    packed.Pack(context)
    return packed


def sql_command(relation):
    return pb.Plan(command=pb.Command(sql_command=pb.SqlCommand(input=relation)))


def test_resolved_graph_runs_its_views_and_cross_joins_both_inputs(audit):
    """SparkDeclarativePipelinesServerSuite:199: tableC reads viewB and tableA, and viewB reads tableA.

    The Scala test declares tableC a TABLE, which a run refuses as a batch streaming table; as a
    materialized view the same graph runs, and the row count shows that both inputs were read.
    """
    graph = audit.graph(
        """
        CREATE MATERIALIZED VIEW tableA AS SELECT * FROM RANGE(5);
        CREATE TEMPORARY VIEW viewB AS SELECT * FROM tableA;
        CREATE MATERIALIZED VIEW tableC AS SELECT a.id AS a, b.id AS b FROM tableA a, viewB b
        """
    )
    audit.messages(graph)
    assert audit.spark.table(audit.full("tableC")).count() == 25


def test_a_dropped_graph_is_gone_from_the_session(audit):
    """SparkDeclarativePipelinesServerSuite:502: the registry no longer holds a dropped graph."""
    graph = audit.graph()
    audit.define(graph, "test_table")
    audit.drop(graph)
    with pytest.raises(errors.SparkException) as error:
        audit.define(graph, "test_table")
    assert error.value.getCondition() == "DATAFLOW_GRAPH_NOT_FOUND"
    assert error.value.getMessageParameters() == {"graphId": graph}


def test_sql_inside_a_flow_function_returns_its_relation(audit):
    """SparkDeclarativePipelinesServerSuite:855 (SPARK-54452): the command is deferred, not run."""
    spark = audit.spark
    graph = audit.graph()
    relation = pb.Relation(read=pb.Read(named_table=pb.Read.NamedTable(unparsed_identifier="table")))
    responses = execute_plan(
        spark.client._stub,
        spark.client._session_id,
        "test_user",
        sql_command(relation),
        [analysis_context(graph, "flow1")],
    )
    results = [r.sql_command_result for r in responses if r.HasField("sql_command_result")]
    assert len(results) == 1
    assert results[0].relation == relation


def test_sql_outside_a_flow_function_returns_its_relation(audit):
    """SparkDeclarativePipelinesServerSuite:905 (SPARK-54452): a query is returned, not executed."""
    spark = audit.spark
    graph = audit.graph()
    relation = pb.Relation(sql=pb.SQL(query="SELECT * FROM RANGE(5)"))
    responses = execute_plan(
        spark.client._stub,
        spark.client._session_id,
        "test_user",
        sql_command(relation),
        [analysis_context(graph)],
    )
    results = [r.sql_command_result for r in responses if r.HasField("sql_command_result")]
    assert len(results) == 1
    assert results[0].relation == relation


def test_event_responses_carry_the_session_ids(audit):
    """PipelineEventSenderSuite:72: each event comes in a response naming both session ids."""
    spark = audit.spark
    graph = audit.graph("CREATE MATERIALIZED VIEW a AS SELECT 1 AS x")
    responses = list(
        execute_plan(
            spark.client._stub,
            spark.client._session_id,
            spark.client._user_id or "",
            pb.Plan(command=audit.start_run_command(graph)),
        )
    )
    events = [r for r in responses if r.HasField("pipeline_event_result")]
    assert any(f"Flow {audit.full('a')} has COMPLETED" in r.pipeline_event_result.event.message for r in events)
    for response in events:
        assert response.session_id == spark.client._session_id
        assert response.server_side_session_id
    assert len({r.server_side_session_id for r in responses}) == 1


def test_all_events_arrive_in_order_before_the_stream_ends(audit):
    """PipelineEventSenderSuite:97: shutdown waits for queued events, which keep their order."""
    graph = audit.graph("CREATE MATERIALIZED VIEW a AS SELECT 1 AS x; CREATE MATERIALIZED VIEW b AS SELECT x FROM a")
    messages = audit.messages(graph)
    a, b = audit.full("a"), audit.full("b")
    expected = [
        f"Flow {a} is STARTING",
        f"Flow {a} is RUNNING",
        f"Flow {a} has COMPLETED",
        f"Flow {b} is STARTING",
        f"Flow {b} is RUNNING",
        f"Flow {b} has COMPLETED",
        "Run is COMPLETED",
    ]
    positions = [next(i for i, m in enumerate(messages) if text in m) for text in expected]
    assert positions == sorted(positions), messages
    assert "Run is COMPLETED" in messages[-1]


def test_a_full_event_queue_keeps_terminal_events(audit):
    """PipelineEventSenderSuite:136: terminal flow events and run events are never dropped."""
    spark = audit.spark
    key = "spark.sql.pipelines.event.queue.capacity"
    spark.conf.set(key, "1")
    try:
        names = [f"m{i}" for i in range(4)]
        sql = ";".join(f"CREATE MATERIALIZED VIEW {name} AS SELECT 1 AS x" for name in names)
        messages = audit.messages(audit.graph(sql))
    finally:
        spark.conf.unset(key)
    for name in names:
        assert any(f"Flow {audit.full(name)} has COMPLETED" in m for m in messages), messages
    assert "Run is COMPLETED" in messages[-1]


def test_a_run_failure_rethrows_the_flow_error(audit):
    """PipelinesHandlerSuite:49: the client gets the cause of a failed run, here the flow's error."""
    audit.spark.sql(f"CREATE TABLE {audit.schema}.source AS SELECT 1 AS id")
    graph = audit.graph(
        f"""
        SET spark.sql.pipelines.maxFlowRetryAttempts = 0;
        CREATE MATERIALIZED VIEW f AS SELECT CAST('x' AS INT) AS d FROM {audit.schema}.source
        """
    )
    messages = []
    with pytest.raises(errors.SparkConnectGrpcException) as error:
        for item in start_run(audit.spark, graph, None, False, None, False, audit.storage):
            if "pipeline_event_result" in item:
                messages.append(item["pipeline_event_result"].event.message)
    assert error.value.getCondition() == "CAST_INVALID_INPUT"
    assert not any("Run is" in message for message in messages), messages


@pytest.mark.parametrize("root", ROOTS_ACCEPTED)
def test_a_storage_root_with_a_scheme_is_accepted(audit, root):
    """PipelineUpdateContextImplSuite:26. A dry run validates the root without touching storage."""
    graph = audit.graph("CREATE MATERIALIZED VIEW a AS SELECT 1 AS x")
    assert "Run is COMPLETED" in audit.messages(graph, dry=True, storage=root)[-1]


@pytest.mark.parametrize("root", ROOTS_RELATIVE + ROOTS_WITHOUT_SCHEME)
def test_an_invalid_storage_root_names_itself(audit, root):
    """PipelineUpdateContextImplSuite:37 and :54: the condition and its storage_root parameter."""
    graph = audit.graph("CREATE MATERIALIZED VIEW a AS SELECT 1 AS x")
    with pytest.raises(errors.SparkConnectGrpcException) as error:
        audit.messages(graph, storage=root)
    assert error.value.getCondition() == STORAGE_ROOT_INVALID
    assert error.value.getMessageParameters() == {"storage_root": root}


def test_a_run_validates_its_storage_root_and_the_graph_stays_usable(audit):
    """PipelineUpdateContextImplSuite:70: a run needs a valid root, and a refused one leaves no run."""
    graph = audit.graph("CREATE MATERIALIZED VIEW a AS SELECT 1 AS x")
    with pytest.raises(errors.SparkConnectGrpcException) as error:
        audit.messages(graph, storage="/tmp/test")
    assert error.value.getCondition() == STORAGE_ROOT_INVALID
    assert error.value.getMessageParameters() == {"storage_root": "/tmp/test"}
    assert "Run is COMPLETED" in audit.messages(graph)[-1]


def test_events_carry_a_timestamp_and_a_message(audit):
    """ConstructPipelineEventSuite:27 and :55: all that reaches a client of a constructed event."""
    graph = audit.graph("CREATE MATERIALIZED VIEW a AS SELECT 1 AS x")
    events = audit.events(graph)
    assert events
    assert any(f"Flow {audit.full('a')} has COMPLETED" in event.message for event in events)
    assert all(event.HasField("message") and event.message for event in events)
    stamps = [event.timestamp.ToNanoseconds() for event in events]
    assert all(event.HasField("timestamp") and event.timestamp.seconds > 0 for event in events)
    assert stamps == sorted(stamps)


def test_a_graph_holds_one_execution_after_its_run(audit):
    """SparkConnectSessionHolderSuite:676: a run stays cached, so a second one on its graph is refused."""
    graph = audit.graph("CREATE MATERIALIZED VIEW a AS SELECT 1 AS x")
    assert "Run is COMPLETED" in audit.messages(graph)[-1]
    with pytest.raises(errors.SparkConnectGrpcException) as error:
        audit.messages(graph)
    assert (
        error.value.getCondition() == "SPARK_CONNECT_ILLEGAL_STATE.EXECUTION_STATE_EXECUTE_HOLDER_ALREADY_EXISTS_GRAPH"
    )
    assert error.value.getMessageParameters() == {"graphId": graph}


def release_during_a_run(audit):
    spark = audit.spark
    stub = spark.client._stub
    user = "audit_user"
    session = str(uuid4())
    schema = pb.Relation(sql=pb.SQL(query=f"CREATE SCHEMA IF NOT EXISTS {audit.schema}"))
    list(execute_plan(stub, session, user, sql_command(schema)))
    create = pb.Command()
    create.pipeline_command.create_dataflow_graph.default_catalog = audit.catalog
    create.pipeline_command.create_dataflow_graph.default_database = audit.schema
    responses = list(execute_plan(stub, session, user, pb.Plan(command=create)))
    graph = next(
        r.pipeline_command_result.create_dataflow_graph_result.dataflow_graph_id
        for r in responses
        if r.HasField("pipeline_command_result")
    )
    define = pb.Command()
    define.pipeline_command.define_sql_graph_elements.dataflow_graph_id = graph
    define.pipeline_command.define_sql_graph_elements.sql_file_path = "audit.sql"
    define.pipeline_command.define_sql_graph_elements.sql_text = (
        "CREATE MATERIALIZED VIEW slow AS "
        "SELECT sum(a.id * b.id * c.id) AS s FROM range(100000) a, range(100000) b, range(100000) c"
    )
    list(execute_plan(stub, session, user, pb.Plan(command=define)))
    run = pb.Command()
    run.pipeline_command.start_run.dataflow_graph_id = graph
    run.pipeline_command.start_run.storage = audit.storage

    started = threading.Event()
    seen = []
    failure = []

    def consume():
        try:
            for response in execute_plan(stub, session, user, pb.Plan(command=run)):
                if response.HasField("pipeline_event_result"):
                    seen.append(response.pipeline_event_result.event.message)
                    if "is RUNNING" in seen[-1]:
                        started.set()
        except grpc.RpcError as error:
            failure.append(error)
        finally:
            started.set()

    reader = threading.Thread(target=consume, daemon=True)
    reader.start()
    assert started.wait(120), seen
    stub.ReleaseSession(pb.ReleaseSessionRequest(session_id=session, user_context=pb.UserContext(user_id=user)))
    reader.join(120)
    assert not reader.is_alive(), seen
    return seen, failure


def test_closing_a_session_ends_its_run(audit):
    """SparkConnectSessionManagerSuite:158: closing the holder ends the run of a batch flow.

    Whether the query stops is not visible here. test_audit_exec.py::test_releasing_a_session_lets_its_run_finish
    measures a streaming flow on Spark, whose run finishes after the release.
    """
    seen, _ = release_during_a_run(audit)
    assert not any("has COMPLETED" in message for message in seen), seen
    assert not any("Run is COMPLETED" in message for message in seen), seen


@pytest.mark.xfail(
    not is_jvm_spark(),
    strict=True,
    reason="depends on the session lifecycle change: the stream of a released session ends without an error",
)
def test_closing_a_session_fails_the_stream_of_its_run(audit):
    """SparkConnectSessionManagerSuite:158: the client reading the run gets an error."""
    seen, failure = release_during_a_run(audit)
    assert failure, seen
