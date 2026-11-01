"""What the audit of MaterializeTablesSuite, TriggeredGraphExecutionSuite and SystemMetadataSuite found uncovered.

Each case is measured on Spark 4.2.1 through the public client. `MemoryStream` is replaced by a parquet table
read as a stream, and a Scala UDF that sleeps by a Python one.
"""

import os
import sys
import threading
import time
from pathlib import Path

import pyspark.pipelines as dp
import pytest
from pysail.testing.spark.utils.common import is_jvm_spark
from pyspark.errors.exceptions.connect import IllegalArgumentException
from pyspark.pipelines.add_pipeline_analysis_context import add_pipeline_analysis_context
from pyspark.pipelines.graph_element_registry import graph_element_registration_context
from pyspark.pipelines.spark_connect_graph_element_registry import SparkConnectGraphElementRegistry
from pyspark.pipelines.spark_connect_pipeline import create_dataflow_graph, start_run
from pyspark.sql import functions as F
from pyspark.sql.connect.session import SparkSession as ConnectSparkSession
from pyspark.sql.types import BooleanType

# A local Spark server starts its Python workers with this, and otherwise takes whatever `python3` is on the path.
os.environ.setdefault("PYSPARK_PYTHON", sys.executable)


class Suite:
    def __init__(self, spark, schema, root):
        self.spark = spark
        self.schema = schema
        self.catalog = spark.catalog.currentCatalog()
        self.root = root

    def name(self, dataset):
        return f"{self.catalog}.{self.schema}.{dataset}"

    def build(self, sql=None, python=None):
        graph = create_dataflow_graph(self.spark, self.catalog, self.schema, None)
        registry = SparkConnectGraphElementRegistry(self.spark, graph)
        if sql is not None:
            registry.register_sql(sql, Path("pipeline.sql"))
        if python is not None:
            with (
                add_pipeline_analysis_context(spark=self.spark, dataflow_graph_id=graph, flow_name=None),
                graph_element_registration_context(registry),
            ):
                exec(python, {"spark": self.spark, "dp": dp})  # noqa: S102 the graph definition under test
        return graph

    def stream(self, graph, full=None, full_all=False, refresh=None):
        for item in start_run(self.spark, graph, full, full_all, refresh, False, self.root.as_uri()):
            if "pipeline_event_result" in item:
                yield item["pipeline_event_result"].event.message

    def attempt(self, sql=None, python=None, **options):
        """A run's event messages and terminal error."""
        messages = []
        error = None
        try:
            messages.extend(self.stream(self.build(sql, python), **options))
        except Exception as caught:  # noqa: BLE001 the failure is an observation
            error = caught
        return messages, error

    def run(self, sql=None, python=None, **options):
        messages, error = self.attempt(sql, python, **options)
        assert error is None, (str(error), "\n".join(messages))
        return messages

    def columns(self, table):
        return [(c.name, c.dataType) for c in self.spark.catalog.listColumns(f"{self.schema}.{table}")]

    def exists(self, table):
        return self.spark.catalog.tableExists(self.name(table))

    def count(self, table):
        return self.spark.table(f"{self.schema}.{table}").count()

    def versions(self, *parts):
        return sorted(path.name for path in self.root.joinpath("_checkpoints", *parts).glob("*"))

    def source(self, table="src", rows=3):
        self.spark.sql(f"CREATE TABLE {self.schema}.{table} USING parquet AS SELECT id FROM RANGE({rows})")


@pytest.fixture
def suite(spark, tmp_path, request):
    schema = "ax_" + "".join(c if c.isalnum() else "_" for c in request.node.name).lower()[:48]
    spark.sql(f"DROP SCHEMA IF EXISTS {schema} CASCADE")
    spark.sql(f"CREATE SCHEMA {schema}")
    yield Suite(spark, schema, tmp_path / "storage")
    spark.sql(f"DROP SCHEMA IF EXISTS {schema} CASCADE")


def test_an_invalid_reset_allowed_names_the_property(suite):
    """MaterializeTablesSuite:530: the value 123 fails with an IllegalArgumentException naming the property."""
    suite.source()
    with pytest.raises(IllegalArgumentException, match="pipelines.reset.allowed"):
        list(
            suite.stream(
                suite.build(
                    "CREATE STREAMING TABLE a TBLPROPERTIES ('pipelines.reset.allowed' = '123') "
                    "AS SELECT * FROM STREAM(src)"
                )
            )
        )


@pytest.mark.parametrize("full_refresh", [False, True])
def test_complete_tables_take_each_new_schema(suite, full_refresh):
    """MaterializeTablesSuite:568: an identical rerun keeps x, and a changed query replaces it with y."""
    view = "CREATE TEMPORARY VIEW a AS SELECT * FROM VALUES (1, 2), (2, 3) AS t(x, y);"
    first = f"{view} CREATE MATERIALIZED VIEW b AS SELECT x FROM a"
    suite.run(first)
    suite.run(first, full_all=full_refresh)
    assert suite.columns("b") == [("x", "int")]
    suite.run(f"{view} CREATE MATERIALIZED VIEW b AS SELECT y FROM a")
    assert suite.columns("b") == [("y", "int")]


@pytest.mark.parametrize("option", ["refresh", "full"])
def test_a_selection_naming_no_table_materializes_nothing(suite, option):
    """MaterializeTablesSuite:829: the nearest a client gets to refreshTables = NoTables."""
    messages = suite.run("CREATE MATERIALIZED VIEW a AS SELECT 1 AS x", **{option: ["elsewhere"]})
    assert messages == [f"Flow '{suite.name('a')}' is EXCLUDED.", "Run is COMPLETED."]
    assert not suite.exists("a")


@pytest.mark.xfail(
    not is_jvm_spark(),
    strict=True,
    reason="Sail refuses the column at registration with its own IllegalArgumentException text",
)
def test_a_missing_cluster_column_is_an_analysis_exception(suite):
    """MaterializeTablesSuite:1070: the cause of the materialization failure is an AnalysisException."""
    suite.source()
    _, error = suite.attempt(
        python=f"""
@dp.table(name = "t", cluster_by = ["nonexistent_column"])
def query():
  return spark.readStream.table("{suite.schema}.src")
"""
    )
    assert "AnalysisException: Couldn't find column nonexistent_column" in str(error)


def fails(text, source):
    return f"SELECT CASE WHEN id IS NULL THEN 1 ELSE raise_error('{text}') END AS d FROM {source}"


def test_the_earliest_failed_flow_is_the_runs_failure(suite):
    """TriggeredGraphExecutionSuite:1088: the earliest failure wins, here against an earlier flow name."""
    suite.source()
    messages, error = suite.attempt(
        f"""
        SET spark.sql.pipelines.maxFlowRetryAttempts = 0;
        CREATE MATERIALIZED VIEW z_early AS {fails("early failure", "src")};
        CREATE MATERIALIZED VIEW gate1 AS SELECT id FROM src;
        CREATE MATERIALIZED VIEW gate2 AS SELECT id FROM gate1;
        CREATE MATERIALIZED VIEW a_late AS SELECT d FROM ({fails("late failure", "src")}) JOIN gate2
        """
    )
    joined = "\n".join(messages)
    assert f"Flow '{suite.name('a_late')}' has FAILED." in joined
    assert f"Flow '{suite.name('z_early')}' has FAILED." in joined
    assert "early failure" in str(error)
    assert "late failure" not in str(error)


@pytest.mark.xfail(
    not is_jvm_spark(),
    strict=True,
    reason="Sail ends the run with STREAMING_SOURCES_CHANGED, and Spark with the query's STREAM_FAILED",
)
def test_a_source_change_fails_the_run_with_its_own_error(suite):
    """TriggeredGraphExecutionSuite:419: the run's cause reaches the client; its Run is FAILED event does not."""
    suite.source("src1", 10)
    suite.source("src2", 10)
    table = "CREATE STREAMING TABLE input_table AS SELECT * FROM STREAM(input_view)"
    suite.run(
        f"CREATE TEMPORARY VIEW input_view AS SELECT * FROM STREAM(src1) UNION ALL SELECT * FROM STREAM(src2); {table}"
    )
    _, error = suite.attempt(f"CREATE TEMPORARY VIEW input_view AS SELECT * FROM STREAM(src2); {table}")
    assert error.getCondition() == "STREAM_FAILED"
    assert "There are [2] sources in the checkpoint offsets and now there are [1] sources" in str(error)


@pytest.mark.parametrize(("full_refresh", "expected"), [(False, ["0"]), (True, ["0", "1"])])
def test_a_named_flow_checkpoint_follows_full_refresh(suite, full_refresh, expected):
    """SystemMetadataSuite:103: the directory is under the table, then the flow, then a version."""
    suite.source()
    graph = (
        "CREATE TEMPORARY VIEW a AS SELECT * FROM STREAM(src);"
        "CREATE STREAMING TABLE st;"
        "CREATE FLOW st_flow AS INSERT INTO st BY NAME SELECT * FROM STREAM(a)"
    )
    suite.run(graph)
    parts = [suite.catalog, suite.schema, "st", "st_flow"]
    assert suite.versions(*parts) == ["0"]
    suite.run(graph, full_all=full_refresh)
    assert suite.versions(*parts) == expected


def delayed_run(suite, stop):
    """A run with a slow second flow and a third queued behind it: `stop(session)` during the second, if given."""
    suite.source("src1", 10)
    spark, schema = suite.spark, suite.schema
    delay = F.udf(lambda _: time.sleep(1) or True, BooleanType())
    graph = create_dataflow_graph(spark, suite.catalog, schema, None)
    registry = SparkConnectGraphElementRegistry(spark, graph)
    with (
        add_pipeline_analysis_context(spark=spark, dataflow_graph_id=graph, flow_name=None),
        graph_element_registration_context(registry),
    ):

        @dp.table
        def input_table():
            return spark.readStream.table(f"{schema}.src1")

        @dp.table
        def query_with_delay():
            return spark.readStream.table("input_table").filter(delay("id"))

        @dp.table
        def x():
            return spark.readStream.table("query_with_delay")

    messages = []
    errors = []

    def read():
        try:
            for message in suite.stream(graph):
                messages.append(message)  # noqa: PERF402 another thread reads it while it grows
        except Exception as caught:  # noqa: BLE001 the failure is an observation
            errors.append(caught)

    thread = threading.Thread(target=read)
    thread.start()
    running = f"Flow {suite.name('query_with_delay')} is RUNNING."
    deadline = time.monotonic() + 60
    while running not in messages and time.monotonic() < deadline:
        time.sleep(0.05)
    assert running in messages, messages
    assert f"Flow {suite.name('input_table')} has COMPLETED." in messages
    if stop:
        stop(spark)
    thread.join(60)
    assert not thread.is_alive()
    return messages, errors[0] if errors else None


def test_an_uninterrupted_delayed_run_writes_every_table(suite):
    """The control for the two below: the slow query takes about ten seconds and writes rows."""
    messages, error = delayed_run(suite, None)
    assert error is None, "\n".join(messages)
    assert messages[-1] == "Run is COMPLETED."
    assert suite.count("query_with_delay") == 10
    assert suite.count("x") == 10


@pytest.mark.xfail(
    not is_jvm_spark(),
    strict=True,
    reason="Sail stops the flows and reports `operation not found`; Spark cancels only the client's operation",
)
def test_interrupting_a_run_cancels_the_operation_and_not_the_flows(suite):
    """TriggeredGraphExecutionSuite:551: no client request reaches `graphExecution.stop()`.

    Spark's interrupt ends the client's stream with OPERATION_CANCELED. The flows go on and write their rows, so
    the Scala test's STOPPED and SKIPPED states have no client form.
    """
    messages, error = delayed_run(suite, lambda session: session.interruptAll())
    assert error.getCondition() == "OPERATION_CANCELED"
    assert not [message for message in messages if message.startswith("Run is")]
    deadline = time.monotonic() + 30
    while suite.count("query_with_delay") < 10 and time.monotonic() < deadline:
        time.sleep(0.2)
    assert suite.count("query_with_delay") == 10


@pytest.mark.xfail(
    not is_jvm_spark(),
    strict=True,
    reason="depends on the session lifecycle change: removing a session cancels its flows, which then fail and retry",
)
def test_releasing_a_session_lets_its_run_finish(suite, remote):
    """TriggeredGraphExecutionSuite:551: the other way a client can try to stop a run."""
    address = "sc://localhost:15002" if remote.startswith("local") else remote
    session = ConnectSparkSession.builder.remote(address).create()
    session.sql(f"CREATE SCHEMA IF NOT EXISTS {suite.schema}")
    released = Suite(session, suite.schema, suite.root)
    # The stream itself may then end with INVALID_HANDLE.SESSION_CLOSED on Spark, or not, as measured.
    messages, _ = delayed_run(released, lambda stopped: stopped.client.release_session())
    assert messages[-1] == "Run is COMPLETED."
