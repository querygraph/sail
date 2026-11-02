"""AutoCDC behaviour at the Spark pin, run end to end through the durable path.

Spark's Scd1ForeachBatchHandlerSuite calls the handler directly over tables it
prepares. Here each case reaches the same state through earlier runs, so it
also covers enrollment, sealing and recovery. Sources are files, so nothing
here needs a broker.
"""

from __future__ import annotations

import json
import json
import os

import pytest
from pyspark.errors import AnalysisException
from pyspark.errors.exceptions.base import PySparkException
from pyspark.errors.exceptions.connect import SparkConnectGrpcException
from pyspark.sql import functions as sf

try:
    from pyspark import pipelines as dp
    from pyspark.pipelines.graph_element_registry import graph_element_registration_context
except ImportError:
    pytest.skip("Declarative pipelines require Spark 4.1+", allow_module_level=True)

FAILURE = (PySparkException, SparkConnectGrpcException)
SCHEMA = "id INT, name STRING, seq BIGINT, is_delete BOOLEAN"
# Spark 4.2.1 refuses a Delta target, whose table has no row-level operations.
# As an oracle it runs this file over its test catalog: set this to `cat.ns1`.
ORACLE = os.environ.get("AUTOCDC_ORACLE_NAMESPACE")
PREFIX = f"{ORACLE}." if ORACLE else ""
# Spark-verified expectations also run on Sail's other target formats.
TARGET_FORMAT = os.environ.get("AUTOCDC_TARGET_FORMAT", "delta")
FORMAT = {} if ORACLE else {"format": TARGET_FORMAT}
TARGET = f"{PREFIX}target"
AUXILIARY = f"{PREFIX}__spark_autocdc_aux_state_target"
META = ("__spark_autocdc_metadata.deleteSequence", "__spark_autocdc_metadata.upsertSequence")


class Cdc:
    def __init__(self, spark, factory, root, schema=SCHEMA):
        self.spark = spark
        self.factory = factory
        self.landing = str(root / "landing")
        self.schema = schema
        self.keys = ["id"]
        self.sequence = "seq"
        self.arguments = {"apply_as_deletes": sf.expr("is_delete"), "except_column_list": ["is_delete"]}
        spark.createDataFrame([], schema).write.mode("append").parquet(self.landing)

    def send(self, *rows):
        self.spark.createDataFrame(list(rows), self.schema).write.mode("append").parquet(self.landing)

    def run(self, **options):
        spark, landing, schema = self.spark, self.landing, self.schema
        _, registry, start = self.factory()
        with graph_element_registration_context(registry):

            @dp.temporary_view
            def changes():
                return spark.readStream.format("parquet").schema(schema).load(landing)

            dp.create_streaming_table(TARGET, **FORMAT)
            dp.create_auto_cdc_flow(
                target=TARGET, source="changes", keys=self.keys, sequence_by=self.sequence, **self.arguments
            )
        start(**options)

    def target(self, *columns):
        return sorted(tuple(row) for row in self.spark.table(TARGET).select(*columns).collect())

    def tombstones(self):
        frame = self.spark.table(AUXILIARY)
        return sorted((row["id"], row["__spark_autocdc_metadata"]["deleteSequence"]) for row in frame.collect())

    def auxiliary(self, *keys):
        return sorted(tuple(row) for row in self.spark.table(AUXILIARY).select(*keys, *META).collect())


def recorded_keys(spark):
    properties = {row[0]: row[1] for row in spark.sql(f"SHOW TBLPROPERTIES {AUXILIARY}").collect()}
    return json.loads(properties["pipelines.autocdc.keyColumnNames"])


def clean(spark):
    if ORACLE:
        spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {ORACLE}")
    for name in ["target", "t_a", "t_b"]:
        spark.sql(f"DROP TABLE IF EXISTS {PREFIX}{name}")
        spark.sql(f"DROP TABLE IF EXISTS {PREFIX}__spark_autocdc_aux_state_{name}")


@pytest.fixture
def cdc(pipeline_spark, pipeline_factory, tmp_path):
    clean(pipeline_spark)
    yield Cdc(pipeline_spark, pipeline_factory, tmp_path)
    clean(pipeline_spark)


def test_a_tombstone_drops_a_stale_row_and_a_fresh_upsert_is_written(cdc):
    cdc.send((1, "unused", 10, True))
    cdc.run()
    cdc.send((1, "stale", 5, False), (2, "fresh", 20, False))
    cdc.run()
    assert cdc.target("id", "name", *META) == [(2, "fresh", None, 20)]
    assert cdc.auxiliary("id") == [(1, 10, None)]


def test_a_newer_delete_removes_the_row_and_records_a_tombstone(cdc):
    cdc.send((1, "old", 10, False))
    cdc.run()
    cdc.send((1, "unused", 20, True))
    cdc.run()
    assert cdc.target("id") == []
    assert cdc.tombstones() == [(1, 20)]


def test_duplicate_keys_in_a_batch_merge_once_over_an_existing_row(cdc):
    cdc.send((1, "old", 10, False))
    cdc.run()
    cdc.send((1, "ignored-older", 15, False), (1, "newer", 20, False))
    cdc.run()
    assert cdc.target("id", "name", *META) == [(1, "newer", None, 20)]
    assert cdc.tombstones() == []


def test_a_late_delete_leaves_the_row_and_still_records_a_tombstone(cdc):
    cdc.send((1, "alice", 5, False))
    cdc.run()
    cdc.send((1, "alice", 2, True))
    cdc.run()
    assert cdc.target("id", "name", *META) == [(1, "alice", None, 5)]
    assert cdc.auxiliary("id") == [(1, 2, None)]


def test_a_delete_losing_within_a_batch_leaves_no_tombstone(cdc):
    cdc.send((1, "alice", 2, True), (1, "bob", 3, False))
    cdc.run()
    assert cdc.target("id", "name", *META) == [(1, "bob", None, 3)]
    assert cdc.tombstones() == []


def test_a_stale_upsert_is_ignored(cdc):
    cdc.send((1, "alice", 5, False))
    cdc.run()
    cdc.send((1, "bob", 2, False))
    cdc.run()
    assert cdc.target("id", "name") == [(1, "alice")]
    assert cdc.tombstones() == []


def test_out_of_order_batches_converge_with_the_sequence_excluded(cdc):
    cdc.arguments["except_column_list"] = ["seq", "is_delete"]
    for batch in [
        [(1, "v3", 3, False)],
        [(1, "v1", 1, False)],
        [(2, "b2", 2, False)],
        [(2, "gone", 5, True)],
        [(2, "b4", 4, False)],
        [(1, "v6", 6, False), (3, "c1", 1, False)],
    ]:
        cdc.send(*batch)
        cdc.run()
    assert cdc.target("id", "name") == [(1, "v6"), (3, "c1")]
    assert "seq" not in cdc.spark.table(TARGET).columns


def test_six_out_of_order_batches_keep_the_highest_sequence(cdc):
    cdc.arguments["except_column_list"] = ["seq", "is_delete"]
    for batch in [
        [(1, "alice", 1, False), (1, "bob", 3, False)],
        [(1, "carol", 2, False)],
        [(1, "dave", 4, False)],
        [(1, None, 2, True)],
    ]:
        cdc.send(*batch)
        cdc.run()
    assert cdc.target("id", "name", *META) == [(1, "dave", None, 4)]
    for batch in [[(1, None, 5, True)], [(1, "ghost", 4, False)]]:
        cdc.send(*batch)
        cdc.run()
    assert cdc.target("id") == []
    assert cdc.auxiliary("id") == [(1, 5, None)]
    assert "seq" not in cdc.spark.table(TARGET).columns


def test_a_null_sequence_fails_before_any_merge(cdc):
    cdc.send((1, "alice", 1, False))
    cdc.run()
    cdc.send((1, "bob", None, False))
    with pytest.raises(
        FAILURE, match=r"(?s)NULL_SEQUENCE.*`target` in batch 1 .*contains 1 row\(s\) with a null sequencing value"
    ):
        cdc.run()
    assert cdc.target("id", "name", *META) == [(1, "alice", None, 1)]
    assert cdc.auxiliary("id") == []


def test_a_null_key_fails_before_any_merge(cdc):
    cdc.send((1, "alice", 1, False))
    cdc.run()
    cdc.send((None, "no-id-1", 5, False), (2, "ok", 6, False), (None, "no-id-2", 7, True))
    with pytest.raises(FAILURE, match=r"(?s)NULL_KEY.*`id`=2"):
        cdc.run()
    assert cdc.target("id", "name", *META) == [(1, "alice", None, 1)]
    assert cdc.auxiliary("id") == []


def test_a_composite_null_key_reports_every_column(pipeline_spark, pipeline_factory, tmp_path):
    clean(pipeline_spark)
    schema = "country STRING, city STRING, seq BIGINT, is_delete BOOLEAN"
    cdc = Cdc(pipeline_spark, pipeline_factory, tmp_path, schema)
    cdc.keys = ["country", "city"]
    cdc.send(("US", "Boston", 1, False))
    cdc.run()
    cdc.send((None, "Boston", 1, False), ("US", None, 2, False), (None, None, 3, False))
    with pytest.raises(FAILURE, match=r"(?s)NULL_KEY.*`country`=2, `city`=2"):
        cdc.run()
    assert cdc.target("country", "city") == [("US", "Boston")]
    assert cdc.auxiliary("country", "city") == []


def test_a_map_sequence_fails_before_any_merge(cdc):
    cdc.sequence = sf.create_map(sf.lit("k"), sf.col("id"))
    cdc.send((1, "alice", 1, False))
    with pytest.raises(FAILURE, match=r"(?s)NON_ORDERABLE_SEQUENCE.*map<string,int>"):
        cdc.run()
    assert cdc.target("id") == []
    assert cdc.auxiliary("id") == []


def test_a_composite_key_isolates_rows_by_the_full_key(pipeline_spark, pipeline_factory, tmp_path):
    clean(pipeline_spark)
    schema = "country STRING, city STRING, population BIGINT, seq BIGINT, is_delete BOOLEAN"
    cdc = Cdc(pipeline_spark, pipeline_factory, tmp_path, schema)
    cdc.keys = ["country", "city"]
    cdc.arguments["except_column_list"] = ["seq", "is_delete"]
    cdc.send(
        ("US", "New York", 8000000, 1, False),
        ("US", "Los Angeles", 4000000, 1, False),
        ("UK", "London", 9000000, 1, False),
    )
    cdc.run()
    assert cdc.target("country", "city", "population", *META) == [
        ("UK", "London", 9000000, None, 1),
        ("US", "Los Angeles", 4000000, None, 1),
        ("US", "New York", 8000000, None, 1),
    ]
    assert cdc.auxiliary("country", "city") == []


def test_keys_may_be_given_in_another_case(cdc):
    cdc.send((1, "alice", 1, False))
    cdc.run()
    cdc.keys = ["ID"]
    cdc.send((1, "bob", 2, False))
    cdc.run()
    assert cdc.target("id", "name") == [(1, "bob")]


def test_a_sequence_expression_types_the_metadata(cdc):
    cdc.sequence = sf.col("seq").cast("int")
    cdc.send((1, "alice", 1, False))
    cdc.run()
    metadata = cdc.spark.table(TARGET).schema["__spark_autocdc_metadata"].dataType
    assert metadata["upsertSequence"].dataType.simpleString() == "int"
    assert metadata.simpleString() == "struct<deleteSequence:int,upsertSequence:int>"


def test_reordering_composite_keys_is_not_drift(pipeline_spark, pipeline_factory, tmp_path):
    clean(pipeline_spark)
    cdc = Cdc(pipeline_spark, pipeline_factory, tmp_path, "region STRING, id INT, name STRING, seq BIGINT")
    cdc.arguments = {}
    cdc.keys = ["id", "region"]
    cdc.send(("eu", 1, "a", 1))
    cdc.run()
    cdc.keys = ["region", "id"]
    cdc.send(("eu", 1, "b", 2))
    cdc.run()
    assert cdc.target("region", "id", "name") == [("eu", 1, "b")]


def test_the_auxiliary_table_records_keys_in_declared_order(pipeline_spark, pipeline_factory, tmp_path):
    clean(pipeline_spark)
    cdc = Cdc(pipeline_spark, pipeline_factory, tmp_path, "id INT, region STRING, seq BIGINT")
    cdc.arguments = {}
    cdc.keys = ["region", "id"]
    cdc.send((1, "eu", 1))
    cdc.run()
    assert pipeline_spark.table(AUXILIARY).columns == ["region", "id", "__spark_autocdc_metadata"]
    assert recorded_keys(pipeline_spark) == ["region", "id"]


def test_special_characters_in_key_names_survive_two_runs(pipeline_spark, pipeline_factory, tmp_path):
    clean(pipeline_spark)
    names = ["it's", "name with spaces", 'a"b', "c\\d"]
    schema = ", ".join(f"`{name}` STRING" for name in names) + ", seq BIGINT"
    cdc = Cdc(pipeline_spark, pipeline_factory, tmp_path, schema)
    cdc.arguments = {}
    cdc.keys = [f"`{name}`" for name in names]
    cdc.send(("v1", "v2", "v3", "v4", 1))
    cdc.run()
    assert recorded_keys(pipeline_spark) == names
    cdc.send(("v1", "v2", "v3", "v4", 2))
    cdc.run()
    assert recorded_keys(pipeline_spark) == names
    assert [row.seq for row in pipeline_spark.table(TARGET).collect()] == [2]


def two_targets(spark, factory, left, right, **options):
    _, registry, start = factory()
    with graph_element_registration_context(registry):

        @dp.temporary_view
        def left_changes():
            return spark.readStream.format("parquet").schema("id INT, seq BIGINT").load(left)

        @dp.temporary_view
        def right_changes():
            return spark.readStream.format("parquet").schema("id INT, seq BIGINT").load(right)

        dp.create_streaming_table(f"{PREFIX}t_a", **FORMAT)
        dp.create_streaming_table(f"{PREFIX}t_b", **FORMAT)
        dp.create_auto_cdc_flow(
            target=f"{PREFIX}t_a", source="left_changes", keys=["id"], sequence_by="seq", name="flow_a"
        )
        dp.create_auto_cdc_flow(
            target=f"{PREFIX}t_b", source="right_changes", keys=["id"], sequence_by="seq", name="flow_b"
        )
    start(**options)


def test_two_flows_in_one_pipeline_and_a_selective_full_refresh(pipeline_spark, pipeline_factory, tmp_path):
    spark = pipeline_spark
    clean(spark)
    left, right = str(tmp_path / "left"), str(tmp_path / "right")
    write = lambda path, *rows: spark.createDataFrame(list(rows), "id INT, seq BIGINT").write.mode("append").parquet(path)  # noqa: E501
    write(left, (1, 10), (2, 10))
    write(right, (1, 10))
    two_targets(spark, pipeline_factory, left, right)
    read = lambda table: sorted(
        tuple(row) for row in spark.table(f"{PREFIX}{table}").select("id", "seq", *META).collect()
    )
    assert read("t_a") == [(1, 10, None, 10), (2, 10, None, 10)]
    assert read("t_b") == [(1, 10, None, 10)]
    assert spark.catalog.tableExists(f"{PREFIX}__spark_autocdc_aux_state_t_a")
    assert spark.catalog.tableExists(f"{PREFIX}__spark_autocdc_aux_state_t_b")

    # Only the refreshed target forgets the watermark of 10.
    fresh = str(tmp_path / "fresh")
    write(fresh, (1, 5))
    write(right, (1, 5))
    two_targets(spark, pipeline_factory, fresh, right, full_refresh=[f"{PREFIX}t_a"], refresh=[f"{PREFIX}t_b"])
    assert read("t_a") == [(1, 5, None, 5)]
    assert read("t_b") == [(1, 10, None, 10)]
    clean(spark)


def test_a_mis_cased_key_fails_when_names_are_case_sensitive(cdc):
    cdc.spark.conf.set("spark.sql.caseSensitive", "true")
    cdc.keys = ["ID"]
    cdc.send((1, "alice", 1, False))
    with pytest.raises(AnalysisException, match=r"(?s)AUTOCDC_KEY_NOT_IN_SELECTED_SCHEMA.*case-sensitive.*`ID`"):
        cdc.run()


def test_an_upper_case_selection_fails_when_names_are_case_sensitive(cdc):
    cdc.spark.conf.set("spark.sql.caseSensitive", "true")
    cdc.keys, cdc.sequence = ["ID"], "SEQ"
    cdc.arguments = {"apply_as_deletes": sf.expr("IS_DELETE"), "column_list": ["ID", "NAME"]}
    cdc.send((1, "bob", 2, False))
    expected = (
        r"(?s)AUTOCDC_COLUMNS_NOT_FOUND_IN_SCHEMA.*case-sensitive.*"
        r"not present in the changeDataFeed schema: ID, NAME\. Available columns: id, name, seq, is_delete"
    )
    with pytest.raises(AnalysisException, match=expected):
        cdc.run()


def test_an_upper_case_selection_resolves_at_every_stage(cdc):
    cdc.keys, cdc.sequence = ["ID"], "SEQ"
    cdc.arguments = {"apply_as_deletes": sf.expr("IS_DELETE"), "column_list": ["ID", "NAME"]}
    cdc.send((1, "alice", 1, False))
    cdc.run()
    cdc.send((1, "bob", 2, False))
    cdc.run()
    assert cdc.target("id", "name", *META) == [(1, "bob", None, 2)]
    assert cdc.auxiliary("id") == []


def test_an_upper_case_flow_runs_when_names_are_case_insensitive(cdc):
    cdc.keys = ["ID"]
    cdc.sequence = "SEQ"
    cdc.arguments = {"apply_as_deletes": sf.expr("IS_DELETE"), "except_column_list": ["IS_DELETE"]}
    cdc.send((1, "alice", 1, False), (2, "bob", 1, False))
    cdc.run()
    cdc.send((1, "unused", 2, True))
    cdc.run()
    assert cdc.target("id", "name") == [(2, "bob")]
    assert cdc.tombstones() == [(1, 2)]


def test_a_string_sequence_orders_as_a_string(cdc):
    cdc.sequence = sf.col("name")
    cdc.arguments = {"apply_as_deletes": sf.expr("is_delete"), "except_column_list": ["is_delete"]}
    cdc.send((1, "b", 1, False))
    cdc.run()
    cdc.send((1, "a", 2, False))
    cdc.run()
    assert cdc.target("id", "name") == [(1, "b")]


@pytest.mark.parametrize(
    ("selection", "kept"),
    [
        ({"column_list": ["id", "seq"]}, [("id", "int"), ("seq", "bigint")]),
        ({"except_column_list": ["name"]}, [("id", "int"), ("seq", "bigint")]),
    ],
)
def test_a_selection_fixes_the_target_schema(pipeline_spark, pipeline_factory, tmp_path, selection, kept):
    clean(pipeline_spark)
    cdc = Cdc(pipeline_spark, pipeline_factory, tmp_path, "id INT, name STRING, seq BIGINT")
    cdc.arguments = selection
    cdc.send((1, "a", 1))
    cdc.run()
    metadata = ("__spark_autocdc_metadata", "struct<deleteSequence:bigint,upsertSequence:bigint>")
    assert [(f.name, f.dataType.simpleString()) for f in pipeline_spark.table(TARGET).schema] == [*kept, metadata]


def test_a_literal_dot_key_resolves_only_when_quoted(pipeline_spark, pipeline_factory, tmp_path):
    clean(pipeline_spark)
    cdc = Cdc(pipeline_spark, pipeline_factory, tmp_path, "`a.b` INT, c INT, seq BIGINT")
    cdc.arguments = {}
    cdc.keys = ["`a.b`"]
    cdc.send((1, 2, 1), (3, 4, 1))
    cdc.run()
    frame = pipeline_spark.table(TARGET)
    assert sorted(tuple(row) for row in frame.select("`a.b`").collect()) == [(1,), (3,)]
    with pytest.raises(FAILURE, match="UNRESOLVED_COLUMN.WITH_SUGGESTION"):
        frame.select("a.b").collect()


STATEFUL = "id INT, name STRING, seq BIGINT, ts TIMESTAMP"


def stateful_run(spark, factory, landing, transform):
    _, registry, start = factory()
    with graph_element_registration_context(registry):

        @dp.temporary_view
        def changes():
            return transform(spark.readStream.format("json").schema(STATEFUL).load(landing))

        dp.create_streaming_table(TARGET, **FORMAT)
        dp.create_auto_cdc_flow(target=TARGET, source="changes", keys=["id"], sequence_by="seq")
    start()


@pytest.fixture
def stateful(pipeline_spark, pipeline_factory, tmp_path):
    clean(pipeline_spark)
    landing = tmp_path / "stateful"
    landing.mkdir()

    def send(name, *rows):
        lines = [
            json.dumps({"id": key, "name": value, "seq": seq, "ts": ts}) + "\n"
            for key, value, seq, ts in rows
        ]
        (landing / f"{name}.json").write_text("".join(lines))

    def run(transform):
        stateful_run(pipeline_spark, pipeline_factory, str(landing), transform)
        rows = pipeline_spark.table(TARGET).select("id", "seq").collect()
        return sorted(tuple(row) for row in rows)

    yield send, run
    clean(pipeline_spark)


T0 = "2024-01-01T00:00:00Z"


@pytest.mark.parametrize(
    "transform",
    [
        pytest.param(lambda frame: frame.dropDuplicates(["id", "seq"]), id="keys"),
        pytest.param(lambda frame: frame.dropDuplicates(), id="all-columns"),
        pytest.param(lambda frame: frame.withWatermark("ts", "1 hour").dropDuplicates(["id", "seq", "ts"]), id="watermark"),
    ],
)
def test_a_deduplicated_stream_feeds_a_flow_across_runs(stateful, transform):
    send, run = stateful
    send("a", (1, "a", 1, T0), (1, "a", 1, T0))
    assert run(transform) == [(1, 1)]
    send("b", (1, "a", 1, T0), (2, "b", 1, T0))
    assert run(transform) == [(1, 1), (2, 1)]
    send("c", (1, "c", 2, T0))
    assert run(transform) == [(1, 2), (2, 1)]


def test_a_stream_joined_with_itself_feeds_a_flow_across_runs(stateful):
    send, run = stateful
    transform = lambda frame: frame.join(frame.select("id").distinct(), "id")  # noqa: E731
    send("a", (1, "a", 1, T0))
    assert run(transform) == [(1, 1)]
    send("b", (2, "b", 1, T0))
    assert run(transform) == [(1, 1), (2, 1)]


def test_a_stream_join_does_not_reread_earlier_batches(stateful, tmp_path):
    send, run = stateful
    transform = lambda frame: frame.join(frame.select("id", sf.col("seq").alias("other")), "id")  # noqa: E731
    send("a", (1, "a", 1, T0))
    assert run(transform) == [(1, 1)]
    (tmp_path / "stateful" / "a.json").unlink()
    send("b", (2, "b", 1, T0))
    assert run(transform) == [(1, 1), (2, 1)]


def test_a_windowed_aggregate_feeds_a_flow_once_its_window_closes(stateful):
    send, run = stateful

    def transform(frame):
        grouped = frame.withWatermark("ts", "1 hour").groupBy("id", sf.window("ts", "1 minute"))
        return grouped.agg(sf.max("seq").alias("seq"))

    send("a", (1, "a", 1, T0), (1, "a", 3, T0))
    assert run(transform) == []
    send("b", (2, "b", 9, "2024-01-01T02:00:00Z"))
    assert run(transform) == [(1, 3)]


def test_a_deduplication_keeps_the_first_row_of_a_key_across_runs(stateful):
    send, run = stateful
    transform = lambda frame: frame.dropDuplicates(["id"])  # noqa: E731
    send("a", (1, "a", 1, T0))
    assert run(transform) == [(1, 1)]
    send("b", (1, "b", 2, T0), (2, "b", 1, T0))
    assert run(transform) == [(1, 1), (2, 1)]


def test_a_row_behind_the_watermark_changes_only_an_open_window(stateful):
    send, run = stateful

    def transform(frame):
        grouped = frame.withWatermark("ts", "1 hour").groupBy("id", sf.window("ts", "1 minute"))
        return grouped.agg(sf.max("seq").alias("seq"))

    send("a", (1, "a", 1, T0))
    assert run(transform) == []
    # The watermark moves to 01:00:30, which closes the window of 00:00.
    send("b", (2, "b", 9, "2024-01-01T02:00:30Z"))
    assert run(transform) == [(1, 1)]
    # A row for the closed window is late. A row at 01:00:10 is behind the
    # watermark too, but its window ends after it, so it counts.
    send("c", (1, "c", 7, T0), (3, "c", 4, "2024-01-01T01:00:10Z"))
    assert run(transform) == [(1, 1)]
    send("d", (4, "d", 2, "2024-01-01T03:00:00Z"))
    assert run(transform) == [(1, 1), (3, 4)]


def test_an_aggregate_without_a_watermark_is_refused(stateful):
    send, run = stateful
    send("a", (1, "a", 1, T0))
    with pytest.raises(FAILURE, match="STREAMING_OUTPUT_MODE.UNSUPPORTED_OPERATION"):
        run(lambda frame: frame.groupBy("id").agg(sf.max("seq").alias("seq")))
