"""TriggeredGraphExecutionSuite, through the public client.

The Scala suite reads `pipelineState` and `EventLevel` from the graph execution.
A client sees neither: it gets each event's message and the run's terminal error,
and the tables. So a flow's state is read from its lifecycle words, and an event's
level is dropped.

`MemoryStream` is replaced by a parquet table read as a file stream, with
`INSERT INTO` between runs. Where the Scala test only needs rows to arrive once,
this proves the same thing. It changes what `partial graph updates` proves: new
data is a new file a run discovers, not an offset a test pushed, so that test also
covers file discovery. It changes what the source-change test proves: the two
plans differ in file sources, not in memory streams.

A Scala UDF that throws becomes `raise_error`, evaluated per row by the executor,
so a failure still comes from the running query and not from analysis.

Flows run one at a time on Sail and concurrently on Spark, so nothing here depends
on the order of two flows' events.
"""

from pathlib import Path

import pyspark.pipelines as dp
import pytest

from pysail.testing.spark.utils.common import is_jvm_spark
from pyspark.pipelines.add_pipeline_analysis_context import add_pipeline_analysis_context
from pyspark.pipelines.graph_element_registry import graph_element_registration_context
from pyspark.pipelines.spark_connect_graph_element_registry import SparkConnectGraphElementRegistry
from pyspark.pipelines.spark_connect_pipeline import create_dataflow_graph, start_run
from pyspark.sql.types import IntegerType, StringType, StructField, StructType

BATCH = ["QUEUED", "PLANNING", "STARTING", "RUNNING", "COMPLETED"]
STREAM = ["QUEUED", "STARTING", "RUNNING", "COMPLETED"]


def fail(message, column):
    """An expression that fails for each row, whatever the row holds."""
    return f"CASE WHEN {column} IS NULL THEN true ELSE raise_error('{message}') END"


def assert_run_failed(messages, error, text):
    """A failed run reaches a client as its cause, and never as the `Run is FAILED` event Scala reads."""
    assert not [message for message in messages if message.startswith("Run is")], "\n".join(messages)
    assert text in str(error), str(error)


def row_order(row):
    return tuple((value is None, 0 if value is None else value) for value in row)


class Suite:
    def __init__(self, spark, schema, storage):
        self.spark = spark
        self.schema = schema
        self.catalog = spark.catalog.currentCatalog()
        self.storage = storage

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

    def attempt(self, sql=None, python=None, full_all=False, refresh=None):
        """A run's event messages and terminal error."""
        graph = self.build(sql, python)
        messages = []
        error = None
        try:
            for item in start_run(self.spark, graph, None, full_all, refresh, False, self.storage):
                if "pipeline_event_result" in item:
                    messages.append(item["pipeline_event_result"].event.message)
        except Exception as caught:  # noqa: BLE001 the failure is an observation
            error = caught
        return messages, error

    def run(self, sql=None, python=None, full_all=False, refresh=None):
        messages, error = self.attempt(sql, python, full_all, refresh)
        assert error is None, (str(error), "\n".join(messages))
        return messages

    def frame(self, dataset):
        return self.spark.table(f"{self.schema}.{dataset}")

    def rows(self, dataset):
        return sorted((tuple(row) for row in self.frame(dataset).collect()), key=row_order)

    def events(self, messages, dataset):
        name = self.name(dataset)
        return [message for message in messages if message.startswith((f"Flow {name} ", f"Flow '{name}' "))]

    def statuses(self, messages, dataset):
        """A flow's lifecycle words, as FlowProgressEventLogger reports them."""
        return [message.split("\n")[0].rstrip(".").split(" ")[-1] for message in self.events(messages, dataset)]

    def source(self, values="(1), (2)", table="src"):
        self.spark.sql(f"CREATE TABLE {self.schema}.{table}(value INT) USING parquet")
        if values:
            self.insert(values, table)

    def insert(self, values, table="src"):
        self.spark.sql(f"INSERT INTO {self.schema}.{table} VALUES {values}")

    def range_source(self, table, column="id"):
        self.spark.sql(f"CREATE TABLE {self.schema}.{table} USING PARQUET AS SELECT id AS {column} FROM RANGE(10)")

    def assert_skipped(self, messages, dataset):
        events = self.events(messages, dataset)
        assert any("SKIPPED" in message for message in events), events
        assert "STARTING" not in self.statuses(messages, dataset)


@pytest.fixture
def suite(spark, tmp_path, request):
    schema = "tg_" + "".join(c if c.isalnum() else "_" for c in request.node.name).lower()[:48]
    spark.sql(f"DROP SCHEMA IF EXISTS {schema} CASCADE")
    spark.sql(f"CREATE SCHEMA {schema}")
    yield Suite(spark, schema, (tmp_path / "storage").as_uri())
    spark.sql(f"DROP SCHEMA IF EXISTS {schema} CASCADE")


def test_basic_graph_resolution_and_execution(suite):
    messages = suite.run(
        """
        CREATE MATERIALIZED VIEW a AS SELECT * FROM VALUES (1), (2) AS t(x);
        CREATE MATERIALIZED VIEW b AS SELECT * FROM a
        """
    )
    assert suite.statuses(messages, "a") == BATCH
    assert suite.statuses(messages, "b") == BATCH
    assert suite.rows("b") == [(1,), (2,)]


def test_graph_materialization_with_streams(suite):
    messages = suite.run(
        """
        CREATE MATERIALIZED VIEW a AS SELECT * FROM VALUES (1), (2) AS t(x);
        CREATE MATERIALIZED VIEW b AS SELECT * FROM a;
        CREATE TEMPORARY VIEW c AS SELECT * FROM STREAM(a);
        CREATE STREAMING TABLE d AS SELECT * FROM STREAM(c)
        """
    )
    assert suite.statuses(messages, "a") == BATCH
    assert suite.statuses(messages, "b") == BATCH
    assert suite.statuses(messages, "d") == STREAM
    # No flow progress for c, a temporary view.
    assert not [message for message in messages if message.startswith(("Flow c ", "Flow 'c' "))]
    assert suite.statuses(messages, "c") == []
    assert suite.rows("a") == [(1,), (2,)]
    assert suite.rows("d") == [(1,), (2,)]


def test_three_hop_pipeline(suite):
    suite.source(", ".join(f"({value})" for value in range(1, 10)))
    messages = suite.run(
        """
        CREATE TEMPORARY VIEW input AS SELECT * FROM STREAM(src);
        CREATE STREAMING TABLE eights AS SELECT value * 2 AS value FROM STREAM(fours);
        CREATE STREAMING TABLE fours AS SELECT value * 2 AS value FROM STREAM(evens);
        CREATE STREAMING TABLE evens AS SELECT * FROM STREAM(input) WHERE value % 2 = 0
        """
    )
    for table in ["evens", "fours", "eights"]:
        assert suite.statuses(messages, table) == STREAM
    assert suite.rows("evens") == [(2,), (4,), (6,), (8,)]
    assert suite.rows("fours") == [(4,), (8,), (12,), (16,)]
    assert suite.rows("eights") == [(8,), (16,), (24,), (32,)]


def test_all_events_are_emitted_even_if_there_is_no_data(suite):
    suite.source(values=None)
    messages = suite.run(
        """
        CREATE TEMPORARY VIEW input AS SELECT * FROM STREAM(src);
        CREATE STREAMING TABLE evens AS SELECT * FROM STREAM(input) WHERE value % 2 = 0
        """
    )
    assert suite.statuses(messages, "evens") == STREAM
    assert suite.rows("evens") == []


def test_stream_failure_causes_its_downstream_to_be_skipped(suite):
    suite.range_source("failing")
    suite.source()
    messages, error = suite.attempt(
        f"""
        CREATE TEMPORARY VIEW input_view AS SELECT * FROM STREAM(src);
        CREATE STREAMING TABLE input_table AS SELECT * FROM STREAM(input_view);
        CREATE STREAMING TABLE branch_1 AS SELECT * FROM STREAM(input_table);
        CREATE STREAMING TABLE branch_2 AS SELECT * FROM STREAM(failing) WHERE {fail("Test error", "id")};
        CREATE STREAMING TABLE x AS SELECT * FROM STREAM(branch_2);
        CREATE TEMPORARY VIEW y AS SELECT * FROM STREAM(x);
        CREATE STREAMING TABLE z AS SELECT * FROM STREAM(x)
        """
    )
    assert error is not None
    assert suite.statuses(messages, "input_table")[-1] == "COMPLETED"
    assert suite.statuses(messages, "branch_1")[-1] == "COMPLETED"
    failures = [message for message in suite.events(messages, "branch_2") if "FAILED" in message]
    assert len(failures) == 3, "\n".join(messages)
    assert all("Test error" in message for message in failures)
    suite.assert_skipped(messages, "x")
    suite.assert_skipped(messages, "z")
    assert suite.rows("input_table") == [(1,), (2,)]
    assert suite.rows("branch_1") == [(1,), (2,)]
    assert_run_failed(messages, error, "Test error")


def test_stream_failure_on_a_source_change_gives_a_clear_error(suite):
    suite.range_source("src1")
    suite.range_source("src2")
    table = "CREATE STREAMING TABLE input_table AS SELECT * FROM STREAM(input_view)"
    first = suite.run(
        f"""
        CREATE TEMPORARY VIEW input_view AS SELECT * FROM STREAM(src1) UNION ALL SELECT * FROM STREAM(src2);
        {table}
        """
    )
    assert suite.statuses(first, "input_table")[-1] == "COMPLETED"

    messages, error = suite.attempt(
        f"""
        CREATE TEMPORARY VIEW input_view AS SELECT * FROM STREAM(src2);
        {table}
        """
    )
    assert error is not None, "\n".join(messages)
    name = suite.name("input_table")
    # Unrecoverable without a full refresh: exactly one failure, and no retry.
    failures = [message for message in suite.events(messages, "input_table") if "sources added or removed" in message]
    assert len(failures) == 1, "\n".join(messages)
    assert f"Flow '{name}' had streaming sources added or removed." in failures[0]
    assert not [message for message in suite.events(messages, "input_table") if "has FAILED" in message]
    assert_run_failed(messages, error, "")


def test_user_specified_schema_is_applied_to_table(suite):
    suite.run(
        """
        CREATE MATERIALIZED VIEW specified_schema (x INT) AS SELECT * FROM VALUES (1), (2) AS t(x);
        CREATE MATERIALIZED VIEW specified_schema_stream (x INT) AS SELECT * FROM VALUES (1), (2) AS t(x);
        CREATE MATERIALIZED VIEW specified_schema_downstream (x INT) AS SELECT * FROM specified_schema;
        CREATE MATERIALIZED VIEW specified_schema_downbatch (x INT) AS SELECT * FROM specified_schema_stream
        """,
        full_all=True,
    )
    expected = StructType([StructField("x", IntegerType(), True)])
    for table in [
        "specified_schema",
        "specified_schema_stream",
        "specified_schema_downstream",
        "specified_schema_downbatch",
    ]:
        assert suite.frame(table).schema == expected
        columns = suite.spark.catalog.listColumns(f"{suite.schema}.{table}")
        assert [(column.name, column.dataType, column.nullable) for column in columns] == [("x", "int", True)]


@pytest.mark.skip(reason="calls graphExecution.stop(); a client has no stop request, and closing a session ends events")
def test_stopping_a_pipeline_mid_execution():
    pass


def test_two_hop_pipeline_with_partitioned_graph(suite):
    messages = suite.run(
        """
        CREATE MATERIALIZED VIEW integer_input AS SELECT * FROM VALUES (1), (2), (3), (4) AS t(value);
        CREATE MATERIALIZED VIEW `double` AS SELECT value * 2 AS value FROM integer_input;
        CREATE MATERIALIZED VIEW string_input AS SELECT * FROM VALUES ('a'), ('b'), ('c'), ('d') AS t(value);
        CREATE MATERIALIZED VIEW append_x AS SELECT CONCAT(value, 'x') AS value FROM string_input
        """
    )
    for table in ["integer_input", "string_input", "double", "append_x"]:
        assert suite.statuses(messages, table)[-1] == "COMPLETED"
    assert suite.rows("double") == [(2,), (4,), (6,), (8,)]
    assert suite.rows("append_x") == [("ax",), ("bx",), ("cx",), ("dx",)]


def test_multiple_hop_pipeline_with_merge_from_multiple_sources(suite):
    messages = suite.run(
        """
        CREATE MATERIALIZED VIEW integer_input AS SELECT * FROM VALUES (1), (2), (3), (4) AS t(nums);
        CREATE MATERIALIZED VIEW `double` AS SELECT nums * 2 AS nums FROM integer_input;
        CREATE MATERIALIZED VIEW string_input AS SELECT * FROM VALUES ('a'), ('b'), ('c'), ('d') AS t(text);
        CREATE MATERIALIZED VIEW append_x AS SELECT CONCAT(text, 'x') AS text FROM string_input;
        CREATE MATERIALIZED VIEW merged AS SELECT * FROM `double` FULL OUTER JOIN append_x ON nums::STRING = text
        """
    )
    for table in ["integer_input", "string_input", "double", "append_x", "merged"]:
        assert suite.statuses(messages, table)[-1] == "COMPLETED"
    assert suite.frame("merged").schema == StructType(
        [StructField("nums", IntegerType()), StructField("text", StringType())]
    )
    collected = suite.frame("merged").collect()
    assert {tuple(row) for row in collected} == {
        (2, None),
        (4, None),
        (6, None),
        (8, None),
        (None, "ax"),
        (None, "bx"),
        (None, "cx"),
        (None, "dx"),
    }
    assert len(collected) == 8


def test_multiple_hop_pipeline_with_split_and_merge_from_a_single_source(suite):
    messages = suite.run(
        """
        CREATE MATERIALIZED VIEW input_table AS SELECT * FROM VALUES (1, 1), (1, 2), (2, 3), (2, 4) AS t(x, y);
        CREATE MATERIALIZED VIEW left_split AS SELECT x FROM input_table WHERE x IS NOT NULL;
        CREATE MATERIALIZED VIEW right_split AS SELECT y FROM input_table WHERE y IS NOT NULL;
        CREATE MATERIALIZED VIEW merged AS SELECT * FROM left_split FULL OUTER JOIN right_split ON x = y
        """
    )
    for table in ["input_table", "left_split", "right_split", "merged"]:
        assert suite.statuses(messages, table)[-1] == "COMPLETED"
    assert suite.frame("merged").schema == StructType(
        [StructField("x", IntegerType()), StructField("y", IntegerType())]
    )
    collected = sorted(tuple(row) for row in suite.frame("merged").filter("x IS NOT NULL").collect())
    assert collected == [(1, 1), (1, 1), (2, 2), (2, 2)]
    unmatched = sorted(tuple(row) for row in suite.frame("merged").filter("x IS NULL").collect())
    assert unmatched == [(None, 3), (None, 4)]


def test_default_flow_retry_is_2(suite):
    suite.range_source("failing", "value")
    messages, error = suite.attempt(
        "CREATE STREAMING TABLE a AS SELECT "
        f"{fail('Intentionally failing UDF.', 'value')} AS value FROM STREAM(failing)"
    )
    assert error is not None
    # Three attempts: one original and two retries. The Scala test also checks WARN and ERROR levels.
    failures = [message for message in suite.events(messages, "a") if "FAILED" in message]
    assert len(failures) == 3, "\n".join(messages)
    final = [message for message in failures if "has FAILED more than 2 times and will not be restarted" in message]
    assert len(final) == 1, "\n".join(messages)
    assert_run_failed(messages, error, "Intentionally failing UDF.")


def test_partial_graph_updates(suite):
    suite.source("(1), (2), (3)")
    graph = """
    CREATE STREAMING TABLE source AS SELECT * FROM STREAM(src);
    CREATE STREAMING TABLE all AS SELECT * FROM STREAM(source);
    CREATE TEMPORARY VIEW evens AS SELECT * FROM all WHERE value % 2 = 0;
    CREATE MATERIALIZED VIEW max_evens AS SELECT MAX(value) FROM evens
    """

    # The first update excludes max_evens.
    first = suite.run(graph, refresh=["source", "all"])
    assert suite.statuses(first, "source") == STREAM
    assert suite.statuses(first, "all") == STREAM
    assert suite.rows("source") == [(1,), (2,), (3,)]
    assert suite.rows("all") == [(1,), (2,), (3,)]
    assert any("EXCLUDED" in message for message in suite.events(first, "max_evens")), "\n".join(first)
    assert not suite.spark.catalog.tableExists(suite.name("max_evens"))

    # The second update excludes all, which keeps its old data.
    suite.insert("(4)")
    second = suite.run(graph, refresh=["source", "max_evens"])
    assert suite.rows("source") == [(1,), (2,), (3,), (4,)]
    assert suite.rows("all") == [(1,), (2,), (3,)]
    assert suite.rows("max_evens") == [(2,)]
    assert any("EXCLUDED" in message for message in suite.events(second, "all")), "\n".join(second)
    assert "STARTING" not in suite.statuses(second, "all")
    assert suite.statuses(second, "max_evens") == BATCH
    assert suite.statuses(second, "source") == STREAM


def test_flow_fails_to_resolve(suite):
    messages, error = suite.attempt(
        """
        CREATE STREAMING TABLE table1 AS SELECT * FROM nonexistent_src1;
        CREATE STREAMING TABLE table2 AS SELECT * FROM nonexistent_src2;
        CREATE STREAMING TABLE table3 AS SELECT * FROM table1
        """
    )
    assert error is not None

    def event(text):
        found = [message for message in messages if text in message]
        assert found, (text, "\n".join(messages))
        return found[0]

    for table, source in [("table1", "nonexistent_src1"), ("table2", "nonexistent_src2")]:
        assert (
            f"The table or view `{suite.catalog}`.`{suite.schema}`.`{source}` cannot be found"
            in event(f"Failed to resolve flow: '{suite.name(table)}'")
        )
    assert (
        f"Failed to read dataset '{suite.name('table1')}'. "
        "Dataset is defined in the pipeline but could not be resolved."
        in event(f"Failed to resolve flow due to upstream failure: '{suite.name('table3')}'")
    )


@pytest.mark.xfail(not is_jvm_spark(), strict=True, reason="Sail reports a bad partition column once, while materializing, before any flow")
def test_consecutive_failures_are_each_reported(suite):
    # The Scala test counts two WARN events and one ERROR event; a client sees their messages.
    messages, error = suite.attempt(
        python="""
@dp.materialized_view(partition_cols = ["nonexistent_col"])
def retry_test():
  return spark.range(5).selectExpr("id", "CAST(id % 2 AS INT) AS id_mod")
"""
    )
    assert error is not None
    failures = [message for message in suite.events(messages, "retry_test") if "FAILED" in message]
    assert len(failures) == 3, ("\n".join(messages), str(error))


@pytest.mark.skip(reason="unit test of TriggeredGraphExecution.chooseRunTerminationReason over hand-built failures")
def test_choose_run_termination_reason_surfaces_the_earliest_failure():
    pass


@pytest.mark.skip(reason="unit test of TriggeredGraphExecution.chooseRunTerminationReason over hand-built failures")
def test_choose_run_termination_reason_breaks_ties_by_flow_name():
    pass


@pytest.mark.skip(reason="unit test of TriggeredGraphExecution.chooseRunTerminationReason over hand-built failures")
def test_choose_run_termination_reason_has_no_reason_without_a_stopped_flow():
    pass
