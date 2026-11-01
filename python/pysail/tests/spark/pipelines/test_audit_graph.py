"""Gaps left by the Connect*PipelineSuite, GraphExecutionSuite and SchemaInferenceUtilsSuite ports.

Each test states what the older port did not assert. Everything here is what a client reads: a table's
schema and rows, a run's events, and its terminal error. `MemoryStream` is a parquet table read as a file
stream, a per-flow conf is a SET in its own SQL file, and `spark.sql.session.timeZone` is the conf whose
value shows in rows, because casting an instant to a string is resolved with the flow's conf.
"""

import re
from decimal import Decimal
from pathlib import Path

import pyspark.pipelines as dp
import pytest
from pysail.testing.spark.utils.common import is_jvm_spark
from pyspark.errors.exceptions.connect import AnalysisException
from pyspark.pipelines.add_pipeline_analysis_context import add_pipeline_analysis_context
from pyspark.pipelines.graph_element_registry import graph_element_registration_context
from pyspark.pipelines.spark_connect_graph_element_registry import SparkConnectGraphElementRegistry
from pyspark.pipelines.spark_connect_pipeline import create_dataflow_graph, start_run
from pyspark.sql.types import IntegerType, StringType, StructField, StructType

TZ = "spark.sql.session.timeZone"
RETRIES = "spark.sql.pipelines.maxFlowRetryAttempts"
INSTANT = "CAST(TIMESTAMP '2020-01-01 00:00:00 UTC' AS STRING)"
ZONES = {
    "Asia/Tokyo": "2020-01-01 09:00:00",
    "America/Lima": "2019-12-31 19:00:00",
    "Asia/Dubai": "2020-01-01 04:00:00",
    "Europe/Paris": "2020-01-01 01:00:00",
    "Asia/Kolkata": "2020-01-01 05:30:00",
    "Australia/Sydney": "2020-01-01 11:00:00",
    "Pacific/Honolulu": "2019-12-31 14:00:00",
    "Europe/Moscow": "2020-01-01 03:00:00",
}
TOKYO, LIMA = "Asia/Tokyo", "America/Lima"
SOURCE_CHANGE = "sources added or removed"
SOURCE_SET_TEXT = (
    "There are [2] sources in the checkpoint offsets and now there are [3] sources requested by the query. "
    "Cannot continue."
)


def row_order(row):
    return tuple((value is None, str(value)) for value in row)


def zone(name):
    return f"SET {TZ} = {name};"


def fail(message, column):
    """An expression that fails for each row, whatever the row holds."""
    return f"CASE WHEN {column} IS NULL THEN true ELSE raise_error('{message}') END"


def fields(*columns):
    return StructType([StructField(name, kind, nullable) for name, kind, nullable in columns])


class Suite:
    def __init__(self, spark, schema, storage):
        self.spark = spark
        self.schema = schema
        self.catalog = spark.catalog.currentCatalog()
        self.storage = storage

    def name(self, dataset):
        return f"{self.catalog}.{self.schema}.{dataset}"

    def build(self, *sql, python=None, conf=None):
        graph = create_dataflow_graph(self.spark, self.catalog, self.schema, conf)
        registry = SparkConnectGraphElementRegistry(self.spark, graph)
        for index, text in enumerate(sql):
            registry.register_sql(text, Path(f"pipeline{index}.sql"))
        if python is not None:
            with (
                add_pipeline_analysis_context(spark=self.spark, dataflow_graph_id=graph, flow_name=None),
                graph_element_registration_context(registry),
            ):
                python(self.spark)
        return graph

    def attempt(self, *sql, python=None, conf=None, dry=False, full_all=False):
        """A run's event messages and terminal error."""
        graph = self.build(*sql, python=python, conf=conf)
        messages = []
        error = None
        try:
            for item in start_run(self.spark, graph, None, full_all, None, dry, self.storage):
                if "pipeline_event_result" in item:
                    messages.append(item["pipeline_event_result"].event.message)
        except Exception as caught:  # noqa: BLE001 the failure is an observation
            error = caught
        return messages, error

    def run(self, *sql, python=None, conf=None, full_all=False):
        messages, error = self.attempt(*sql, python=python, conf=conf, full_all=full_all)
        assert error is None, (str(error), "\n".join(messages))
        return messages

    def frame(self, dataset):
        return self.spark.table(f"{self.schema}.{dataset}")

    def rows(self, dataset):
        return sorted((tuple(row) for row in self.frame(dataset).collect()), key=row_order)

    def columns(self, dataset):
        return [(field.name, field.dataType.simpleString()) for field in self.frame(dataset).schema]

    def comments(self, dataset):
        return [field.metadata.get("comment") for field in self.frame(dataset).schema]

    def table(self, ddl="value INT", values="(1), (2)", name="src"):
        self.spark.sql(f"CREATE TABLE {self.schema}.{name}({ddl}) USING parquet")
        if values:
            self.spark.sql(f"INSERT INTO {self.schema}.{name} VALUES {values}")

    def events(self, messages, dataset):
        return [message for message in messages if self.name(dataset) in message.split("\n")[0]]

    def failures(self, messages, dataset):
        return [message for message in self.events(messages, dataset) if "FAILED" in message.split("\n")[0]]

    def in_session_zone(self):
        return self.spark.sql(f"SELECT {INSTANT}").first()[0]


@pytest.fixture
def suite(spark, tmp_path, request):
    schema = "ag_" + "".join(c if c.isalnum() else "_" for c in request.node.name).lower()[:48]
    spark.sql(f"DROP SCHEMA IF EXISTS {schema} CASCADE")
    spark.sql(f"CREATE SCHEMA {schema}")
    yield Suite(spark, schema, (tmp_path / "storage").as_uri())
    spark.sql(f"DROP SCHEMA IF EXISTS {schema} CASCADE")


def summary_line(error, title):
    found = re.search(rf"^{title}: (.*)$", str(error), re.MULTILINE)
    assert found, str(error)
    return found.group(1)


def test_a_multi_flow_table_names_exactly_its_failed_flow(suite):
    """ConnectInvalidPipelineSuite:137: directFailures and downstreamFailures are exactly these key sets."""
    suite.table("id BIGINT")
    _, error = suite.attempt(
        """
        CREATE STREAMING TABLE a;
        CREATE FLOW a AS INSERT INTO a BY NAME SELECT id FROM STREAM(src);
        CREATE FLOW a_2 AS INSERT INTO a BY NAME SELECT non_existent_col FROM STREAM(src);
        CREATE STREAMING TABLE b AS SELECT * FROM STREAM(a)
        """
    )
    assert summary_line(error, "Flows with errors") == suite.name("a_2")
    assert summary_line(error, "Flows that failed due to upstream errors") == suite.name("b")


def test_auto_cdc_flow_targeting_a_persisted_view(suite):
    """ConnectInvalidPipelineSuite:608: a client's persisted view has a query, so the AutoCDC flow is its second.

    Spark fails a `require` before it reaches STREAMING_RELATION_FOR_PERSISTED_VIEW, which a client cannot reach.
    """
    suite.table("id INT, seq INT", "(1, 1)")

    def define(session):
        @dp.table
        def events():
            return session.readStream.table(f"{suite.schema}.src")

        dp.create_auto_cdc_flow(target="target_view", source="events", keys=["id"], sequence_by="seq", name="cdc")

    _, error = suite.attempt("CREATE VIEW target_view AS SELECT 1 AS id, 1 AS seq", python=define, dry=True)
    assert "requirement failed: Found multiple flows to view" in str(error)


INT = IntegerType()
TEXT = StringType()


def assert_schema(suite, dataset, *columns):
    assert suite.frame(dataset).schema == fields(*columns)


def test_view_flow_schemas_keep_non_nullable_columns(suite):
    """ConnectValidPipelineSuite:38,50,74,90,108,129: the Scala schemas are exact, nullability included."""
    suite.run(
        """
        CREATE VIEW a AS SELECT * FROM VALUES (1), (2), (3) AS t(x);
        CREATE VIEW b AS SELECT x AS y FROM a;
        CREATE VIEW c AS SELECT y AS z FROM b;
        CREATE VIEW d AS SELECT * FROM VALUES (1) AS t(y);
        CREATE VIEW e AS SELECT * FROM VALUES ((1)) OUTER JOIN d ON false;
        CREATE VIEW f AS SELECT * FROM e;
        CREATE VIEW g AS SELECT * FROM a CROSS JOIN d;
        CREATE VIEW h AS SELECT * FROM VALUES (1, 'a'), (2, NULL), (3, 'c') AS t(x, y);
        CREATE VIEW i AS SELECT * FROM VALUES (2, 'm'), (3, NULL), (4, 'o') AS t(x, z);
        CREATE VIEW j AS SELECT * FROM h JOIN i USING (x)
        """
    )
    assert_schema(suite, "a", ("x", INT, False))
    assert_schema(suite, "b", ("y", INT, False))
    assert_schema(suite, "c", ("z", INT, False))
    assert_schema(suite, "e", ("col1", INT, False), ("y", INT, False))
    assert_schema(suite, "f", ("col1", INT, False), ("y", INT, False))
    assert_schema(suite, "g", ("x", INT, False), ("y", INT, False))
    assert_schema(suite, "j", ("x", INT, False), ("y", TEXT, True), ("z", TEXT, True))


UNION_OF_STREAM_AND_BATCH = """
CREATE TEMPORARY VIEW a AS SELECT * FROM STREAM(src);
CREATE TEMPORARY VIEW b AS SELECT * FROM VALUES (1), (2), (3) AS t(value);
CREATE TEMPORARY VIEW c AS SELECT * FROM STREAM(a) UNION ALL SELECT * FROM b;
CREATE STREAMING TABLE d AS SELECT * FROM STREAM(c)
"""


def test_a_union_of_streaming_and_batch_views_has_the_merged_schema(suite):
    """ConnectValidPipelineSuite:159: the flow resolves with one `value` column, which the table publishes."""
    suite.table()
    suite.attempt(UNION_OF_STREAM_AND_BATCH)
    assert suite.columns("d") == [("value", "int")]


@pytest.mark.xfail(
    not is_jvm_spark(),
    strict=True,
    reason="Sail starts a stream unioned with a batch relation, and the run completes",
)
def test_a_union_of_streaming_and_batch_views_cannot_start(suite):
    """ConnectValidPipelineSuite:159: resolution accepts this union, and Structured Streaming then refuses it."""
    suite.table()
    _, error = suite.attempt(UNION_OF_STREAM_AND_BATCH)
    assert "Union between streaming and batch DataFrames/Datasets is not supported" in str(error)


def test_a_default_and_a_named_flow_each_contribute_their_columns(suite):
    """ConnectValidPipelineSuite:360: flow b yields `y` and flow b2 yields `value`, so the table holds both."""
    suite.table()
    suite.run(
        """
        CREATE TEMPORARY VIEW a AS SELECT * FROM STREAM(src);
        CREATE STREAMING TABLE b;
        CREATE FLOW b AS INSERT INTO b BY NAME SELECT value AS y FROM STREAM(src);
        CREATE FLOW b2 AS INSERT INTO b BY NAME SELECT * FROM STREAM(a)
        """
    )
    assert sorted(suite.columns("b")) == [("value", "int"), ("y", "int")]


def auto_cdc_over(suite, *, prefix, source):
    """Declares `src` and `target` under `prefix`, and an AutoCDC flow that reads `source`."""
    suite.table("id INT, seq INT", "(1, 1)", name="src_table")
    table = f"{suite.schema}.src_table"

    def define(session):
        @dp.table(name=f"{prefix}src")
        def irrelevant():
            return session.readStream.table(table)

        dp.create_streaming_table(name=f"{prefix}target")
        dp.create_auto_cdc_flow(target=f"{prefix}target", source=source, keys=["id"], sequence_by="seq")

    return suite.attempt(python=define, dry=True)


def test_auto_cdc_single_part_source_inherits_the_graph_default(suite):
    """PythonPipelineSuite:1186. The graph's database is not the session's, and `src` exists only there."""
    assert not suite.spark.catalog.tableExists(f"{suite.spark.catalog.currentDatabase()}.src")
    messages, error = auto_cdc_over(suite, prefix="", source="src")
    assert error is None, (str(error), messages)


@pytest.mark.parametrize("parts", [2, 3], ids=["schema", "catalog-and-schema"])
def test_auto_cdc_multi_part_source_resolves_to_its_qualified_dataset(suite, parts):
    """PythonPipelineSuite:1218, within one catalog."""
    prefix = f"{suite.schema}." if parts == 2 else f"{suite.catalog}.{suite.schema}."
    messages, error = auto_cdc_over(suite, prefix=prefix, source=f"{prefix}src")
    assert error is None, (str(error), messages)


def test_two_flows_to_one_table_publish_their_common_schema(suite):
    """ConnectValidPipelineSuite:392: the table's schema is what both flows produce."""
    suite.table("id BIGINT")
    suite.run(
        """
        CREATE STREAMING TABLE a;
        CREATE FLOW a AS INSERT INTO a BY NAME SELECT id FROM STREAM(src);
        CREATE FLOW a2 AS INSERT INTO a BY NAME SELECT id FROM STREAM(src)
        """
    )
    assert suite.columns("a") == [("id", "bigint")]


def test_two_complete_flows_to_a_streaming_table_fail_validation(suite):
    """ConnectValidPipelineSuite:392: the Scala test stops at resolution; a client reaches validation."""
    _, error = suite.attempt(
        """
        CREATE STREAMING TABLE a;
        CREATE FLOW a AS INSERT INTO a BY NAME SELECT * FROM RANGE(5);
        CREATE FLOW a2 AS INSERT INTO a BY NAME SELECT * FROM RANGE(6)
        """,
        dry=True,
    )
    assert isinstance(error, AnalysisException)
    assert error.getCondition() == "MATERIALIZED_VIEW_WITH_MULTIPLE_QUERIES"
    assert error.getMessageParameters() == {
        "tableName": suite.name("a"),
        "flows": f"{suite.name('a')}, {suite.name('a2')}",
    }


def test_confs_stop_at_a_table_and_still_arrive_from_a_sibling_view(suite):
    """ConnectValidPipelineSuite:289: d takes c's conf and not a's, so their two values do not conflict."""
    suite.run(
        f"{zone(TOKYO)} CREATE VIEW a AS SELECT 1 AS x",
        f"CREATE MATERIALIZED VIEW b AS SELECT x, {INSTANT} AS tz FROM a",
        f"{zone(LIMA)} CREATE VIEW c AS SELECT 2 AS x",
        f"CREATE MATERIALIZED VIEW d AS SELECT x, {INSTANT} AS tz FROM (SELECT x FROM b UNION SELECT x FROM c)",
    )
    assert suite.rows("b") == [(1, ZONES[TOKYO])]
    assert suite.rows("d") == [(1, ZONES[LIMA]), (2, ZONES[LIMA])]


def test_the_same_conf_with_the_same_value_reaches_the_flow(suite):
    """ConnectValidPipelineSuite:322: the fused map is that one entry, and the flow reads its value."""
    suite.run(
        f"{zone(TOKYO)} CREATE VIEW a AS SELECT * FROM VALUES (1) AS t(x)",
        f"{zone(TOKYO)} CREATE VIEW b AS SELECT * FROM VALUES (2) AS t(x)",
        f"{zone(TOKYO)} CREATE MATERIALIZED VIEW c AS "
        f"SELECT x, {INSTANT} AS tz FROM (SELECT * FROM a UNION SELECT * FROM b)",
    )
    assert suite.rows("c") == [(1, ZONES[TOKYO]), (2, ZONES[TOKYO])]


def test_per_flow_confs_stay_off_the_run_session_for_a_whole_run(suite, spark):
    """ConnectValidPipelineSuite:625: the analyzer reads the flow's value, the session never does.

    A client cannot run code inside analysis, so the session is read after the run. It cannot be read during
    one: Spark was measured showing a flow's time zone on the session while a flow ran.
    """
    key = "pipelines.test.flowConfIsolation"
    before = spark.conf.get(TZ)
    assert spark.conf.get(key, None) is None
    assert before != TOKYO
    graph = suite.build(
        f"{zone(TOKYO)} SET {key} = flowValue; CREATE MATERIALIZED VIEW a AS SELECT {INSTANT} AS tz",
        f"CREATE MATERIALIZED VIEW b AS SELECT {INSTANT} AS tz",
    )
    list(start_run(spark, graph, None, False, None, False, suite.storage))
    assert (spark.conf.get(TZ), spark.conf.get(key, None)) == (before, None)
    assert suite.rows("a") == [(ZONES[TOKYO],)]
    assert suite.rows("b") == [(suite.in_session_zone(),)]


def test_per_flow_confs_stay_isolated_between_sql_flows(suite, spark):
    """ConnectValidPipelineSuite:664: eight flows, each with its own value, each read their own value."""
    before = spark.conf.get(TZ)
    suite.run(
        *[
            f"{zone(name)} CREATE MATERIALIZED VIEW f{index} AS SELECT {INSTANT} AS tz"
            for index, name in enumerate(ZONES)
        ]
    )
    for index, instant in enumerate(ZONES.values()):
        assert suite.rows(f"f{index}") == [(instant,)]
    assert spark.conf.get(TZ) == before


def test_per_flow_confs_stay_isolated_between_python_flows(suite, spark):
    """ConnectValidPipelineSuite:664: the same through `spark_conf`, which reaches the server another way."""
    before = spark.conf.get(TZ)

    def define(session):
        for index, name in enumerate(ZONES):
            dp.materialized_view(name=f"f{index}", spark_conf={TZ: name})(
                lambda: session.sql(f"SELECT {INSTANT} AS tz")
            )

    suite.run(python=define)
    for index, instant in enumerate(ZONES.values()):
        assert suite.rows(f"f{index}") == [(instant,)]
    assert spark.conf.get(TZ) == before


def test_a_per_flow_case_sensitivity_fails_with_the_unresolved_column_condition(suite, spark):
    """ConnectValidPipelineSuite:723: the flow's failure is UNRESOLVED_COLUMN.WITH_SUGGESTION."""
    key = "spark.sql.caseSensitive"
    assert spark.conf.get(key) == "false"
    messages, error = suite.attempt(
        "CREATE VIEW src AS SELECT id AS foo FROM RANGE(1)",
        f"SET {key} = true; CREATE VIEW consumer AS SELECT Foo FROM src",
        dry=True,
    )
    assert isinstance(error, AnalysisException)
    assert summary_line(error, "Flows with errors") == suite.name("consumer")
    assert "UNRESOLVED_COLUMN.WITH_SUGGESTION" in "\n".join(suite.events(messages, "consumer"))
    assert spark.conf.get(key) == "false"


def test_an_error_quoting_the_source_change_text_is_retried(suite):
    """GraphExecutionSuite:38: only Structured Streaming's assertion matches, not the same words elsewhere."""
    suite.table("value BIGINT", "(1)")
    messages, error = suite.attempt(
        f"CREATE STREAMING TABLE a AS SELECT {fail(SOURCE_SET_TEXT, 'value')} AS value FROM STREAM(src)"
    )
    assert error is not None
    assert len(suite.failures(messages, "a")) == 3, "\n".join(messages)
    assert not [message for message in messages if SOURCE_CHANGE in message], "\n".join(messages)


@pytest.mark.parametrize("budget", [1, 3])
def test_other_errors_are_retried_until_a_flow_conf_budget_runs_out(suite, budget):
    """GraphExecutionSuite:69: one attempt plus `budget` retries, stopped as retries exceeded."""
    suite.table("value BIGINT", "(1)")
    messages, error = suite.attempt(
        f"SET {RETRIES} = {budget};"
        f"CREATE STREAMING TABLE a AS SELECT {fail('transient', 'value')} AS value FROM STREAM(src)"
    )
    assert "transient" in str(error)
    failures = suite.failures(messages, "a")
    assert len(failures) == budget + 1, "\n".join(messages)
    assert [message for message in failures if f"has FAILED more than {budget} times" in message] == failures[-1:]
    assert not [message for message in messages if SOURCE_CHANGE in message], "\n".join(messages)


def test_a_pipeline_conf_budget_applies_when_the_flow_sets_none(suite):
    """GraphExecutionSuite:69: the budget of three, set for the whole pipeline."""
    suite.table("value BIGINT", "(1)")
    messages, error = suite.attempt(
        f"CREATE STREAMING TABLE a AS SELECT {fail('transient', 'value')} AS value FROM STREAM(src)",
        conf={RETRIES: "3"},
    )
    assert error is not None
    assert len(suite.failures(messages, "a")) == 4, "\n".join(messages)


def test_a_source_change_stops_a_flow_with_retries_left(suite):
    """GraphExecutionSuite:49: with a budget of five, the source change still stops at the first failure."""
    suite.table("id BIGINT", "(1)", "src1")
    suite.table("id BIGINT", "(2)", "src2")
    table = "CREATE STREAMING TABLE t AS SELECT * FROM STREAM(v)"
    suite.run(f"CREATE TEMPORARY VIEW v AS SELECT * FROM STREAM(src1) UNION ALL SELECT * FROM STREAM(src2); {table}")
    messages, error = suite.attempt(
        f"SET {RETRIES} = 5; CREATE TEMPORARY VIEW v AS SELECT * FROM STREAM(src2); {table}"
    )
    assert error is not None
    events = [message for message in messages if SOURCE_CHANGE in message]
    assert len(events) == 1, "\n".join(messages)
    assert f"Flow '{suite.name('t')}' had streaming sources added or removed." in events[0]
    assert not suite.failures(messages, "t"), "\n".join(messages)


MV = "MATERIALIZED VIEW"
ST = "STREAMING TABLE"
BOTH = pytest.mark.parametrize("kind", [MV, ST], ids=["mv", "st"])


class Change:
    """A run, a new source row, and a run of the changed definition of table t."""

    def __init__(self, suite):
        self.suite = suite
        suite.table("id BIGINT, a BIGINT, b BIGINT, c BIGINT, d BIGINT", "(1, 10, 100, 1000, 10000)")

    def define(self, kind, text):
        source = f"STREAM {self.suite.schema}.src" if kind == ST else f"{self.suite.schema}.src"
        return f"CREATE {kind} t {text.format(src=source)}"

    def rerun(self, kind, first, second):
        self.suite.run(self.define(kind, first))
        self.suite.spark.sql(f"INSERT INTO {self.suite.schema}.src VALUES (2, 20, 200, 2000, 20000)")
        return self.suite.attempt(self.define(kind, second))


@pytest.fixture
def change(suite):
    return Change(suite)


BEFORE_ADD = "AS SELECT id, a AS name FROM {src}"
AFTER_ADD = (
    "(id BIGINT, name BIGINT, age BIGINT, email BIGINT COMMENT 'Email address') "
    "AS SELECT id, a AS name, b AS age, c AS email FROM {src}"
)


@BOTH
def test_added_columns_are_nullable_and_keep_their_comments(suite, change, kind):
    """SchemaInferenceUtilsSuite:26: AddColumn carries the type, nullability and comment of each column."""
    _, error = change.rerun(kind, BEFORE_ADD, AFTER_ADD)
    assert error is None
    assert suite.columns("t") == [("id", "bigint"), ("name", "bigint"), ("age", "bigint"), ("email", "bigint")]
    assert suite.comments("t") == [None, None, None, "Email address"]
    assert all(field.nullable for field in suite.frame("t").schema)
    old = (1, 10, None, None) if kind == ST else (1, 10, 100, 1000)
    assert suite.rows("t") == [old, (2, 20, 200, 2000)]


TS = "TIMESTAMP '2020-01-01 00:00:00' AS ts"
TYPES_BEFORE = f"AS SELECT CAST(id AS INT) AS id, CAST(a AS DOUBLE) AS amount, {TS} FROM {{src}}"
TYPES_AFTER = f"AS SELECT id, CAST(a AS DECIMAL(10,2)) AS amount, {TS} FROM {{src}}"


def test_a_materialized_view_changes_two_types_and_leaves_a_third(suite, change):
    """SchemaInferenceUtilsSuite:70: two changed columns, one unchanged, in one run."""
    _, error = change.rerun(MV, TYPES_BEFORE, TYPES_AFTER)
    assert error is None
    assert suite.columns("t") == [("id", "bigint"), ("amount", "decimal(10,2)"), ("ts", "timestamp")]
    assert [row[:2] for row in suite.rows("t")] == [(1, Decimal("10.00")), (2, Decimal("20.00"))]


NULLS_BEFORE = (
    "(id BIGINT NOT NULL, name BIGINT, description BIGINT COMMENT 'Item description') "
    "AS SELECT coalesce(id, 0) AS id, a AS name, b AS description FROM {src}"
)
NULLS_AFTER = (
    "(id BIGINT, name BIGINT NOT NULL, description BIGINT COMMENT 'Product description') "
    "AS SELECT id, coalesce(a, 0) AS name, b AS description FROM {src}"
)


@pytest.mark.parametrize(("kind", "comment"), [(MV, "Product description"), (ST, "Item description")], ids=["mv", "st"])
def test_nullability_and_a_comment_change_in_one_run(suite, change, kind, comment):
    """SchemaInferenceUtilsSuite:109: three columns change at once, and the rows stay."""
    _, error = change.rerun(kind, NULLS_BEFORE, NULLS_AFTER)
    assert error is None
    assert suite.comments("t") == [None, None, comment]
    assert suite.rows("t") == [(1, 10, 100), (2, 20, 200)]


UNCHANGED = "(id BIGINT COMMENT 'key', name BIGINT) AS SELECT id, a AS name FROM {src}"


@BOTH
def test_a_rerun_of_the_same_schema_changes_nothing(suite, change, kind):
    """SchemaInferenceUtilsSuite:200: no changes, so the second run only adds the new rows."""
    _, error = change.rerun(kind, UNCHANGED, UNCHANGED)
    assert error is None
    assert suite.columns("t") == [("id", "bigint"), ("name", "bigint")]
    assert suite.comments("t") == ["key", None]
    assert suite.rows("t") == [(1, 10), (2, 20)]


WIDE = "AS SELECT id, a AS name, b AS age, c AS email, d AS phone FROM {src}"
NARROW = "AS SELECT id, a AS name FROM {src}"


@pytest.mark.parametrize(
    ("kind", "columns", "rows"),
    [
        (MV, ["id", "name"], [(1, 10), (2, 20)]),
        (ST, ["id", "name", "age", "email", "phone"], [(1, 10, 100, 1000, 10000), (2, 20, None, None, None)]),
    ],
    ids=["mv", "st"],
)
def test_deleting_three_columns(suite, change, kind, columns, rows):
    """SchemaInferenceUtilsSuite:211: three deletes; a streaming table keeps them, since it merges."""
    _, error = change.rerun(kind, WIDE, NARROW)
    assert error is None
    assert [name for name, _ in suite.columns("t")] == columns
    assert suite.rows("t") == rows


MIXED_BEFORE = "AS SELECT id, a AS first_name, b AS last_name, c AS age FROM {src}"
MIXED_AFTER = "AS SELECT id, a AS full_name, b AS email, c AS age FROM {src}"


def by_name(suite):
    return sorted((row.asDict() for row in suite.frame("t").collect()), key=lambda row: row["id"])


def test_a_materialized_view_mixes_additions_and_deletions(suite, change):
    """SchemaInferenceUtilsSuite:238: two adds and two deletes; the kept column keeps its place."""
    _, error = change.rerun(MV, MIXED_BEFORE, MIXED_AFTER)
    assert error is None
    assert [name for name, _ in suite.columns("t")] == ["id", "age", "full_name", "email"]
    assert suite.rows("t") == [(1, 1000, 10, 100), (2, 2000, 20, 200)]


def test_a_streaming_table_mixes_additions_and_deletions(suite, change):
    """SchemaInferenceUtilsSuite:238: as sets, like the Scala test; a merge keeps the two deleted columns."""
    _, error = change.rerun(ST, MIXED_BEFORE, MIXED_AFTER)
    assert error is None
    names = [name for name, _ in suite.columns("t")]
    assert names[:4] == ["id", "first_name", "last_name", "age"]
    assert set(names[4:]) == {"full_name", "email"}
    assert by_name(suite) == [
        {"id": 1, "first_name": 10, "last_name": 100, "age": 1000, "full_name": None, "email": None},
        {"id": 2, "first_name": None, "last_name": None, "age": 2000, "full_name": 20, "email": 200},
    ]


@pytest.mark.xfail(
    not is_jvm_spark(),
    strict=True,
    reason="Sail appends added columns in target order; Spark iterates a Map, hash-ordered past four fields",
)
def test_a_streaming_table_appends_added_columns_in_field_map_order(suite, change):
    """SchemaInferenceUtilsSuite:238: diffSchemas walks a field map, so six fields come out as email, full_name."""
    _, error = change.rerun(ST, MIXED_BEFORE, MIXED_AFTER)
    assert error is None
    assert [name for name, _ in suite.columns("t")] == [
        "id",
        "first_name",
        "last_name",
        "age",
        "email",
        "full_name",
    ]
