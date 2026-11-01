"""AutoCDC behaviour at the Spark pin, run end to end through the durable path.

Spark's Scd1ForeachBatchHandlerSuite calls the handler directly over tables it
prepares. Here each case reaches the same state through earlier runs, so it
also covers enrollment, sealing and recovery. Sources are files, so nothing
here needs a broker.
"""

from __future__ import annotations

import os

import pytest
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
FORMAT = {} if ORACLE else {"format": "delta"}
TARGET = f"{PREFIX}target"
AUXILIARY = f"{PREFIX}__spark_autocdc_aux_state_target"


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
    assert cdc.target("id", "name") == [(2, "fresh")]
    assert cdc.tombstones() == [(1, 10)]


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
    assert cdc.target("id", "name", "__spark_autocdc_metadata.upsertSequence") == [(1, "newer", 20)]
    assert cdc.tombstones() == []


def test_a_late_delete_leaves_the_row_and_still_records_a_tombstone(cdc):
    cdc.send((1, "alice", 5, False))
    cdc.run()
    cdc.send((1, "alice", 2, True))
    cdc.run()
    assert cdc.target("id", "name") == [(1, "alice")]
    assert cdc.tombstones() == [(1, 2)]


def test_a_delete_losing_within_a_batch_leaves_no_tombstone(cdc):
    cdc.send((1, "alice", 2, True), (1, "bob", 3, False))
    cdc.run()
    assert cdc.target("id", "name") == [(1, "bob")]
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


def test_a_null_sequence_fails_before_any_merge(cdc):
    cdc.send((1, "alice", 1, False))
    cdc.run()
    cdc.send((1, "bob", None, False))
    with pytest.raises(FAILURE, match="AUTOCDC_"):
        cdc.run()
    assert cdc.target("id", "name") == [(1, "alice")]
    assert cdc.tombstones() == []


def test_a_null_key_fails_before_any_merge(cdc):
    cdc.send((1, "alice", 1, False))
    cdc.run()
    cdc.send((None, "bob", 2, False))
    with pytest.raises(FAILURE, match="AUTOCDC_"):
        cdc.run()
    assert cdc.target("id", "name") == [(1, "alice")]


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
    assert pipeline_spark.table(AUXILIARY).columns[:2] == ["region", "id"]


def test_special_characters_in_key_names_survive_two_runs(pipeline_spark, pipeline_factory, tmp_path):
    clean(pipeline_spark)
    schema = "`it's` INT, `name with spaces` INT, seq BIGINT"
    cdc = Cdc(pipeline_spark, pipeline_factory, tmp_path, schema)
    cdc.arguments = {}
    cdc.keys = ["`it's`", "`name with spaces`"]
    cdc.send((1, 2, 1))
    cdc.run()
    cdc.send((1, 2, 2))
    cdc.run()
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
    write(left, (1, 10))
    write(right, (1, 10))
    two_targets(spark, pipeline_factory, left, right)
    read = lambda table: sorted(tuple(row) for row in spark.table(f"{PREFIX}{table}").select("id", "seq").collect())
    assert (read("t_a"), read("t_b")) == ([(1, 10)], [(1, 10)])

    # Only the refreshed target forgets the watermark of 10.
    fresh = str(tmp_path / "fresh")
    write(fresh, (1, 5))
    write(right, (1, 5))
    two_targets(spark, pipeline_factory, fresh, right, full_refresh=[f"{PREFIX}t_a"], refresh=[f"{PREFIX}t_b"])
    assert read("t_a") == [(1, 5)]
    assert read("t_b") == [(1, 10)]
    clean(spark)
