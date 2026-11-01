"""Delta tables as streaming sources of pipeline flows.

Each expected outcome was measured on Spark 4.2.1 with Delta 4.4.1. One row per insert keeps one
row per file on both engines, so a delete of a row deletes a file instead of rewriting one.
"""

import re

import pyspark.pipelines as dp
import pytest
from pyspark.errors import AnalysisException
from pyspark.errors.exceptions.connect import SparkConnectGrpcException
from pyspark.pipelines.add_pipeline_analysis_context import add_pipeline_analysis_context
from pyspark.pipelines.graph_element_registry import graph_element_registration_context
from pyspark.pipelines.spark_connect_graph_element_registry import SparkConnectGraphElementRegistry
from pyspark.pipelines.spark_connect_pipeline import create_dataflow_graph, start_run

from pysail.testing.spark.utils.common import is_jvm_spark

pytestmark = pytest.mark.skipif(is_jvm_spark(), reason="needs a server with delta-spark on its classpath")

NO_RETRY = {"spark.sql.pipelines.maxFlowRetryAttempts": "0"}
CREATE = "CREATE TABLE src (id INT, v STRING) USING delta LOCATION '{path}'"
DELETION_VECTORS = f"{CREATE} TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')"
CHANGE_FEED = f"{CREATE} TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true')"
MAPPED = (
    f"{CREATE} TBLPROPERTIES ('delta.columnMapping.mode' = 'name', "
    "'delta.minReaderVersion' = '2', 'delta.minWriterVersion' = '5')"
)


class Source:
    def __init__(self, spark, schema, directory):
        self.spark = spark
        self.schema = schema
        self.path = (directory / "src").as_uri()
        self.storage = (directory / "storage").as_uri()

    def sql(self, statement):
        self.spark.sql(statement.replace("{path}", self.path))

    def create(self, statement=CREATE):
        self.sql(statement)

    def rows(self, *rows):
        for row in rows:
            self.sql(f"INSERT INTO src VALUES {row}")

    def run(self, read, *, full=False, target="@dp.table\ndef t():\n  return {read}\n"):
        body = target.replace("{read}", read).replace("{path}", self.path)
        graph = create_dataflow_graph(self.spark, self.spark.catalog.currentCatalog(), self.schema, NO_RETRY)
        registry = SparkConnectGraphElementRegistry(self.spark, graph)
        with (
            add_pipeline_analysis_context(spark=self.spark, dataflow_graph_id=graph, flow_name=None),
            graph_element_registration_context(registry),
        ):
            exec(body, {"spark": self.spark, "dp": dp})  # noqa: S102 the graph under test
        list(start_run(self.spark, graph, None, full, None, False, self.storage))

    def failure(self, read):
        with pytest.raises((AnalysisException, SparkConnectGrpcException)) as error:
            self.run(read)
        return error.value

    def target(self, table="t"):
        return sorted(tuple(row) for row in self.spark.table(f"{self.schema}.{table}").collect())


@pytest.fixture
def source(spark, tmp_path, request):
    schema = "ds_" + re.sub(r"\W", "_", request.node.name).lower()[:48]
    spark.sql(f"DROP SCHEMA IF EXISTS {schema} CASCADE")
    spark.sql(f"CREATE SCHEMA {schema}")
    spark.catalog.setCurrentDatabase(schema)
    yield Source(spark, schema, tmp_path)
    spark.sql("USE default")
    spark.sql(f"DROP SCHEMA IF EXISTS {schema} CASCADE")


def read(**options):
    chain = "".join(f'.option("{key}", "{value}")' for key, value in options.items())
    return f'spark.readStream.format("delta"){chain}.load("{{path}}")'


TABLE = 'spark.readStream.table("src")'
A, B, C, D = "(1, 'a')", "(2, 'b')", "(3, 'c')", "(4, 'd')"
ROWS = [(1, "a"), (2, "b"), (3, "c"), (4, "d")]


def inner_condition(error):
    return re.search(r"terminated with exception: \[([A-Z_]+)\]", str(error)).group(1)


@pytest.mark.parametrize("stream", [TABLE, read()], ids=["table", "path"])
def test_a_snapshot_then_the_new_commits(source, stream):
    source.create()
    source.rows(A, B)
    source.run(stream)
    assert source.target() == ROWS[:2]
    source.rows(C, D)
    source.run(stream)
    assert source.target() == ROWS
    source.run(stream)
    assert source.target() == ROWS


def test_an_empty_table_is_a_start(source):
    source.create()
    source.run(read())
    assert source.target() == []
    source.rows(A)
    source.run(read())
    assert source.target() == ROWS[:1]


def test_a_full_refresh_starts_from_a_new_snapshot(source):
    source.create()
    source.rows(A)
    source.run(read())
    source.rows(B)
    source.run(read())
    source.run(read(), full=True)
    assert source.target() == ROWS[:2]
    source.rows(C)
    source.run(read())
    assert source.target() == ROWS[:3]


@pytest.mark.parametrize(
    ("change", "condition"),
    [
        ("DELETE FROM src WHERE id = 1", "DELTA_SOURCE_IGNORE_DELETE"),
        ("UPDATE src SET v = 'z' WHERE id = 1", "DELTA_SOURCE_TABLE_IGNORE_CHANGES"),
    ],
    ids=["delete", "update"],
)
def test_a_change_fails_until_it_is_allowed(source, change, condition):
    source.create()
    source.rows(A, B)
    source.run(read())
    source.sql(change)
    source.rows(C)
    for _ in range(2):
        error = source.failure(read())
        assert error.getCondition() == "STREAM_FAILED"
        assert inner_condition(error) == condition
    assert source.target() == ROWS[:2]


@pytest.mark.parametrize(
    ("options", "rows"),
    [
        ({"skipChangeCommits": "true"}, [*ROWS[:2], (3, "c")]),
        ({"ignoreChanges": "true"}, [(1, "a"), (1, "z"), *ROWS[1:3]]),
    ],
    ids=["skipped", "reemitted"],
)
def test_an_update_when_allowed(source, options, rows):
    source.create()
    source.rows(A, B)
    source.run(read(**options))
    source.sql("UPDATE src SET v = 'z' WHERE id = 1")
    source.rows(C)
    source.run(read(**options))
    assert source.target() == rows


def test_ignore_deletes_does_not_cover_an_update(source):
    source.create()
    source.rows(A, B)
    source.run(read(ignoreDeletes="true"))
    source.sql("UPDATE src SET v = 'z' WHERE id = 1")
    assert inner_condition(source.failure(read(ignoreDeletes="true"))) == "DELTA_SOURCE_TABLE_IGNORE_CHANGES"


def test_ignore_deletes_covers_a_delete(source):
    source.create()
    source.rows(A, B)
    source.run(read(ignoreDeletes="true"))
    source.sql("DELETE FROM src WHERE id = 1")
    source.rows(C)
    source.run(read(ignoreDeletes="true"))
    assert source.target() == ROWS[:3]


def test_commits_without_data_changes_add_nothing(source):
    source.create()
    source.rows(A, B)
    source.run(read())
    source.sql("OPTIMIZE src")
    source.sql("ALTER TABLE src SET TBLPROPERTIES ('x.y' = '1')")
    source.run(read())
    assert source.target() == ROWS[:2]
    source.rows(C)
    source.run(read())
    assert source.target() == ROWS[:3]


def test_a_snapshot_applies_deletion_vectors(source):
    source.create(DELETION_VECTORS)
    source.rows("(1, 'a'), (2, 'b')")
    source.sql("DELETE FROM src WHERE id = 1")
    source.run(read())
    assert source.target() == [(2, "b")]
    source.rows(C)
    source.run(read())
    assert source.target() == [(2, "b"), (3, "c")]


def test_the_same_file_under_two_deletion_vectors(source):
    source.create(DELETION_VECTORS)
    source.rows("(1, 'a'), (2, 'b'), (3, 'c')")
    source.sql("OPTIMIZE src")
    source.run(read(ignoreChanges="true"))
    source.sql("DELETE FROM src WHERE id = 1")
    source.sql("DELETE FROM src WHERE id = 2")
    source.run(read(ignoreChanges="true"))
    assert source.target() == sorted([*ROWS[:3], (2, "b"), (3, "c"), (3, "c")])


@pytest.mark.parametrize(
    ("starting", "rows"),
    [("0", ROWS[:3]), ("1", ROWS[:3]), ("2", ROWS[1:3]), ("latest", [])],
)
def test_starting_version(source, starting, rows):
    source.create()
    source.rows(A, B, C)
    source.run(read(startingVersion=starting))
    assert source.target() == rows


def test_starting_latest_records_nothing_until_a_batch_reads_something(source):
    source.create()
    source.rows(A)
    source.run(read(startingVersion="latest"))
    source.rows(B)
    source.run(read(startingVersion="latest"))
    assert source.target() == []


def test_a_starting_version_applies_only_to_a_first_run(source):
    source.create()
    source.rows(A)
    source.run(read(startingVersion="0"))
    source.rows(B)
    source.run(read(startingVersion="latest"))
    source.rows(C)
    source.run(read())
    assert source.target() == ROWS[:3]


def test_a_starting_version_passes_earlier_changes_only(source):
    source.create()
    source.rows(A)
    source.sql("UPDATE src SET v = 'z' WHERE id = 1")
    source.rows(C)
    assert inner_condition(source.failure(read(startingVersion="1"))) == "DELTA_SOURCE_TABLE_IGNORE_CHANGES"


def test_a_starting_version_after_a_change(source):
    source.create()
    source.rows(A)
    source.sql("UPDATE src SET v = 'z' WHERE id = 1")
    source.rows(C)
    source.run(read(startingVersion="3"))
    assert source.target() == [(3, "c")]


@pytest.mark.parametrize(
    ("options", "condition"),
    [
        ({"startingVersion": "99"}, "DELTA_VERSION_NOT_FOUND"),
        ({"startingVersion": "-1"}, "DELTA_ILLEGAL_OPTION"),
        ({"startingVersion": "abc"}, "DELTA_ILLEGAL_OPTION"),
        ({"startingVersion": "0", "startingTimestamp": "2000-01-01"}, "DELTA_STARTING_VERSION_AND_TIMESTAMP_BOTH_SET"),
        ({"startingTimestamp": "2999-01-01"}, "DELTA_TIMESTAMP_GREATER_THAN_COMMIT"),
        ({"startingTimestamp": "garbage"}, "DELTA_TIMESTAMP_INVALID"),
        ({"maxFilesPerTrigger": "0"}, "DELTA_ILLEGAL_OPTION"),
        ({"maxFilesPerTrigger": "x"}, "DELTA_ILLEGAL_OPTION"),
        ({"maxBytesPerTrigger": "x"}, "DELTA_ILLEGAL_OPTION"),
        ({"ignoreChanges": "x"}, "DELTA_ILLEGAL_OPTION"),
        ({"skipChangeCommits": "x"}, "DELTA_ILLEGAL_OPTION"),
        ({"ignoreDeletes": "x"}, "DELTA_ILLEGAL_OPTION"),
        ({"failOnDataLoss": "x"}, "DELTA_ILLEGAL_OPTION"),
    ],
)
def test_a_bad_option_fails_the_stream(source, options, condition):
    source.create()
    source.rows(A)
    error = source.failure(read(**options))
    assert error.getCondition() == "STREAM_FAILED"
    assert inner_condition(error) == condition


def test_time_travel_is_refused_outside_the_stream(source):
    source.create()
    source.rows(A)
    assert source.failure(read(versionAsOf="1")).getCondition() == "DELTA_UNSUPPORTED_TIME_TRAVEL_VIEWS"
    assert source.failure(read(timestampAsOf="2000-01-01")).getCondition() == "DELTA_UNSUPPORTED_TIME_TRAVEL_VIEWS"


def test_a_starting_timestamp_before_the_table(source):
    source.create()
    source.rows(A)
    source.run(read(startingTimestamp="2000-01-01"))
    assert source.target() == ROWS[:1]


@pytest.mark.parametrize(
    "options",
    [
        {"maxFilesPerTrigger": "1"},
        {"maxBytesPerTrigger": "1b"},
        {"failOnDataLoss": "false"},
        {"withEventTimeOrder": "true"},
        {"no_such_option": "x"},
    ],
)
def test_options_that_change_nothing_here(source, options):
    source.create()
    source.rows(A, B)
    source.run(read(**options))
    source.rows(C)
    source.run(read(**options))
    assert source.target() == ROWS[:3]


def test_exclude_regex_drops_files(source):
    source.create()
    source.rows(A)
    source.run(read(excludeRegex=".*"))
    assert source.target() == []


def test_a_change_feed_snapshot_then_changes(source):
    source.create(CHANGE_FEED)
    source.rows(A, B)
    source.run(read(readChangeFeed="true"))
    assert [row[:4] for row in source.target()] == [(1, "a", "insert", 2), (2, "b", "insert", 2)]
    source.sql("UPDATE src SET v = 'z' WHERE id = 1")
    source.run(read(readChangeFeed="true"))
    changes = {(row[0], row[1], row[2]) for row in source.target()}
    assert {(1, "a", "update_preimage"), (1, "z", "update_postimage")} <= changes


def test_a_change_feed_from_a_version(source):
    source.create(CHANGE_FEED)
    source.rows(A, B, C)
    source.run(read(readChangeFeed="true", startingVersion="2"))
    assert [row[:3] for row in source.target()] == [(2, "b", "insert"), (3, "c", "insert")]
    assert source.spark.table(f"{source.schema}.t").columns == [
        "id",
        "v",
        "_change_type",
        "_commit_version",
        "_commit_timestamp",
    ]


def test_a_change_feed_that_was_never_enabled(source):
    source.create()
    source.rows(A)
    assert inner_condition(source.failure(read(readChangeFeed="true"))) == "DELTA_MISSING_CHANGE_DATA"


def test_partition_columns(source):
    source.create(CREATE.replace("USING delta", "USING delta PARTITIONED BY (v)"))
    source.rows(A)
    source.run(read())
    source.rows(B)
    source.run(read())
    assert source.target() == ROWS[:2]


def test_column_mapping(source):
    source.create(MAPPED)
    source.rows(A)
    source.run(read())
    source.rows(B)
    source.run(read())
    assert source.target() == ROWS[:2]


@pytest.mark.parametrize("change", ["RENAME COLUMN v TO w", "DROP COLUMN v"], ids=["rename", "drop"])
def test_a_mapped_rename_or_drop_stops_the_stream(source, change):
    source.create(MAPPED)
    source.rows(A)
    source.run(read())
    source.sql(f"ALTER TABLE src {change}")
    condition = inner_condition(source.failure(read()))
    assert condition == "DELTA_STREAMING_INCOMPATIBLE_SCHEMA_CHANGE_USE_SCHEMA_LOG"


def test_an_added_column_widens_the_target(source):
    source.create()
    source.rows(A)
    source.run(read())
    source.sql("ALTER TABLE src ADD COLUMN w INT")
    source.rows("(4, 'd', 7)")
    source.run(read())
    assert source.target() == [(1, "a", None), (4, "d", 7)]


def test_a_replaced_table_is_a_different_table(source):
    source.create()
    source.rows(A)
    source.run(read())
    source.sql(f"CREATE OR REPLACE TABLE src (id INT, v STRING) USING delta LOCATION '{source.path}'")
    source.rows(B)
    assert inner_condition(source.failure(read())) == "DIFFERENT_DELTA_TABLE_READ_BY_STREAMING_SOURCE"


def test_through_a_view(source):
    view = f'@dp.temporary_view\ndef v():\n  return {read()}\n@dp.table\ndef t():\n  return spark.readStream.table("v")\n'
    source.create()
    source.rows(A)
    source.run("", target=view)
    source.rows(B)
    source.run("", target=view)
    assert source.target() == ROWS[:2]


def test_joined_with_a_batch_table(source):
    source.spark.sql(f"CREATE TABLE {source.schema}.dim (id INT, n STRING) USING parquet")
    source.spark.sql(f"INSERT INTO {source.schema}.dim VALUES (1, 'x'), (3, 'y')")
    join = read() + f'.join(spark.read.table("{source.schema}.dim"), "id")'
    source.create()
    source.rows(A)
    source.run(join)
    source.rows(C)
    source.run(join)
    assert source.target() == [(1, "a", "x"), (3, "c", "y")]


def test_the_same_source_twice_reads_it_once_for_each(source):
    both = f"{read()}.union({read()})"
    source.create()
    source.rows(A)
    source.run(both)
    source.rows(B)
    source.run(both)
    assert source.target() == [(1, "a"), (1, "a"), (2, "b"), (2, "b")]


@pytest.mark.parametrize("stream", [TABLE, read()], ids=["table", "path"])
def test_a_watermarked_window(source, stream):
    window = (
        f'{stream}.withWatermark("ts", "1 second")'
        '.groupBy(__import__("pyspark").sql.functions.window("ts", "2 seconds")).count()'
    )
    source.create("CREATE TABLE src (ts TIMESTAMP, id INT) USING delta LOCATION '{path}'")
    source.rows("(CAST(1 AS TIMESTAMP), 1)", "(CAST(2 AS TIMESTAMP), 2)")
    source.run(window)
    assert source.target() == []
    source.rows("(CAST(9 AS TIMESTAMP), 9)")
    source.run(window)
    assert [row[1] for row in source.target()] == [1, 1]


# Spark 4.2.1 fails this second run with DELTA_CANNOT_CHANGE_PROVIDER, a defect of its own.
def test_a_delta_target_runs_twice(source):
    target = '@dp.table(format="delta")\ndef t():\n  return {read}\n'
    source.create()
    source.rows(A)
    source.run(TABLE, target=target)
    source.rows(B)
    source.run(TABLE, target=target)
    assert source.target() == ROWS[:2]
