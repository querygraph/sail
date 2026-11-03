"""AutoCdcScd1TargetTableDurabilitySuite, AutoCdcScd1MultiPipelineSuite and
AutoCdcScd1SchemaEvolutionSuite at the Spark pin.

A MemoryStream becomes a directory of JSON files, so a schema change between
runs is a change to the read's declared schema. The same file runs on Spark:
see AUTOCDC_ORACLE_NAMESPACE in `test_autocdc_semantics.py`. Spark refuses a
Delta target, so there it uses its row-level test catalog.
"""

from __future__ import annotations

import json
import os
import re
import time
import uuid

import pytest
from pyspark.errors.exceptions.base import PySparkException
from pyspark.errors.exceptions.connect import SparkConnectGrpcException
from pyspark.sql import functions as sf

try:
    from pyspark import pipelines as dp
    from pyspark.pipelines.graph_element_registry import graph_element_registration_context
except ImportError:
    pytest.skip("Declarative pipelines require Spark 4.1+", allow_module_level=True)

ORACLE = os.environ.get("AUTOCDC_ORACLE_NAMESPACE")
PREFIX = f"{ORACLE}." if ORACLE else ""
# Spark-verified expectations also run on Sail's other target formats.
TARGET_FORMAT = os.environ.get("AUTOCDC_TARGET_FORMAT", "delta")
FORMAT = {} if ORACLE else {"format": TARGET_FORMAT}
USING = "" if ORACLE else f"USING {TARGET_FORMAT}"
TARGET = f"{PREFIX}target"
AUXILIARY = f"{PREFIX}__spark_autocdc_aux_state_target"
delta_only = pytest.mark.skipif(TARGET_FORMAT != "delta", reason="about Delta targets or sources")
sail_only = pytest.mark.skipif(ORACLE is not None, reason="behaviour Spark's test catalog does not have")


class Landing:
    """A directory a stream reads, written one file per send."""

    def __init__(self, root):
        self.url = str(root / "landing")
        (root / "landing").mkdir()
        self.path = root / "landing"

    def send(self, *rows):
        (self.path / f"{uuid.uuid4().hex}.json").write_text("".join(json.dumps(row) + "\n" for row in rows))


@pytest.fixture
def broker(tmp_path):
    return Landing(tmp_path)


@pytest.fixture
def second(tmp_path):
    """A landing of its own: Spark gives each pipeline a separate stream."""
    root = tmp_path / "second"
    root.mkdir()
    return Landing(root)


META = ("__spark_autocdc_metadata.deleteSequence", "__spark_autocdc_metadata.upsertSequence")
METADATA = "__spark_autocdc_metadata STRUCT<deleteSequence:BIGINT,upsertSequence:BIGINT>"
FAILURE = (PySparkException, SparkConnectGrpcException)


def clean(spark):
    if ORACLE:
        spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {ORACLE}")
    for name in ["target", "t_a", "t_b", "downstream", "bronze", "events"]:
        spark.sql(f"DROP TABLE IF EXISTS {PREFIX}{name}")
        spark.sql(f"DROP TABLE IF EXISTS {PREFIX}__spark_autocdc_aux_state_{name}")


@pytest.fixture
def space(pipeline_spark):
    clean(pipeline_spark)
    yield pipeline_spark
    clean(pipeline_spark)


def run(
    spark, factory, url, schema, *, keys="id", sequence="seq", name="target", columns=None, flow=None, **options
):
    keys = [keys] if isinstance(keys, str) else list(keys)
    _, registry, start = factory()
    with graph_element_registration_context(registry):

        @dp.temporary_view
        def changes():
            return (
                spark.readStream.format("json")
                .schema(schema)
                .load(url)
            )

        dp.create_streaming_table(f"{PREFIX}{name}", **FORMAT)
        dp.create_auto_cdc_flow(
            target=f"{PREFIX}{name}",
            source="changes",
            keys=keys,
            sequence_by=sequence,
            name=flow,
            column_list=columns,
        )
    start(**options)


def rows(spark, *columns, table="target"):
    return sorted(tuple(row) for row in spark.table(f"{PREFIX}{table}").select(*columns).collect())


def preload(spark, values, sequence):
    spark.sql(
        f"INSERT INTO {TARGET} SELECT {values}, "
        f"named_struct('deleteSequence', CAST(NULL AS BIGINT), 'upsertSequence', CAST({sequence} AS BIGINT))"
    )


BASE = "id INT, name STRING, seq BIGINT"
PLAIN = "id INT NOT NULL, name STRING, seq BIGINT NOT NULL"
WITH_EMAIL = "id INT NOT NULL, name STRING, email STRING, seq BIGINT NOT NULL"


def test_a_preloaded_row_outranks_a_lower_sequence(space, pipeline_factory, broker):  # noqa: F811
    space.sql(f"CREATE TABLE {TARGET} (id INT NOT NULL, name STRING, seq BIGINT NOT NULL, {METADATA}) {USING}")
    preload(space, "1, 'alice', 5", 5)
    preload(space, "2, 'bob', 5", 5)
    broker.send({"id": 1, "name": "stale", "seq": 2}, {"id": 2, "name": "bob2", "seq": 10})
    run(space, pipeline_factory, broker.url, BASE)
    assert rows(space, "id", "name", "seq", *META) == [(1, "alice", 5, None, 5), (2, "bob2", 10, None, 10)]


def test_a_target_without_the_metadata_column_gains_it(space, pipeline_factory, broker):  # noqa: F811
    space.sql(f"CREATE TABLE {TARGET} (id INT NOT NULL, name STRING, seq BIGINT NOT NULL) {USING}")
    broker.send({"id": 1, "name": "alice", "seq": 1})
    run(space, pipeline_factory, broker.url, BASE)
    assert space.table(TARGET).columns == ["id", "name", "seq", "__spark_autocdc_metadata"]
    assert rows(space, "id", "name", "seq", *META) == [(1, "alice", 1, None, 1)]


def test_a_nullable_column_merges_mixed_values(space, pipeline_factory, broker):  # noqa: F811
    schema = "id INT, name STRING, email STRING, seq BIGINT"
    space.sql(f"CREATE TABLE {TARGET} ({WITH_EMAIL}, {METADATA}) {USING}")
    broker.send({"id": 1, "name": "alice", "email": None, "seq": 1})
    run(space, pipeline_factory, broker.url, schema)
    assert rows(space, "*") == [(1, "alice", None, 1, (None, 1))]
    broker.send({"id": 1, "name": "alice2", "email": "a@x.com", "seq": 2})
    run(space, pipeline_factory, broker.url, schema)
    assert rows(space, "*") == [(1, "alice2", "a@x.com", 2, (None, 2))]


@pytest.mark.parametrize(
    ("first", "second", "value", "left", "right"),
    [
        ("INT", "BIGINT", 31, "INT", "BIGINT"),
        ("BIGINT", "INT", 5, "BIGINT", "INT"),
        ("TIMESTAMP", "STRING", "2024-01-02T11:00:00Z", "TIMESTAMP", "STRING"),
    ],
)
def test_a_type_change_between_runs_is_refused(space, pipeline_factory, broker, first, second, value, left, right):  # noqa: F811, PLR0913
    original = "2024-01-01T10:00:00Z" if first == "TIMESTAMP" else 30
    broker.send({"id": 1, "age": original, "seq": 1})
    run(space, pipeline_factory, broker.url, f"id INT, age {first}, seq BIGINT")
    broker.send({"id": 1, "age": value, "seq": 2})
    expected = (
        rf'(?s)CANNOT_MERGE_INCOMPATIBLE_DATA_TYPE.*Failed to merge incompatible data types "{left}" and "{right}"'
        r".*SQLSTATE: 42825"
    )
    with pytest.raises(FAILURE, match=expected):
        run(space, pipeline_factory, broker.url, f"id INT, age {second}, seq BIGINT")


def test_a_new_source_column_is_added_to_the_target(space, pipeline_factory, broker):  # noqa: F811
    space.sql(f"CREATE TABLE {TARGET} ({PLAIN}, {METADATA}) {USING}")
    broker.send({"id": 1, "name": "alice", "seq": 1})
    run(space, pipeline_factory, broker.url, BASE)
    assert rows(space, "*") == [(1, "alice", 1, (None, 1))]
    broker.send({"id": 2, "name": "bob", "email": "b@x.com", "seq": 2})
    run(space, pipeline_factory, broker.url, "id INT, name STRING, email STRING, seq BIGINT")
    assert rows(space, "*") == [(1, "alice", 1, (None, 1), None), (2, "bob", 2, (None, 2), "b@x.com")]


def test_a_dropped_source_column_keeps_its_rows_and_is_null_on_new_ones(space, pipeline_factory, broker):  # noqa: F811
    space.sql(f"CREATE TABLE {TARGET} ({PLAIN}, {METADATA}) {USING}")
    broker.send({"id": 1, "name": "alice", "email": "a@x.com", "seq": 1})
    run(space, pipeline_factory, broker.url, "id INT, name STRING, email STRING, seq BIGINT")
    assert rows(space, "*") == [(1, "alice", 1, (None, 1), "a@x.com")]
    broker.send({"id": 2, "name": "bob", "seq": 2})
    run(space, pipeline_factory, broker.url, BASE)
    assert rows(space, "*") == [(1, "alice", 1, (None, 1), "a@x.com"), (2, "bob", 2, (None, 2), None)]


def test_broadening_the_selection_adds_the_column(space, pipeline_factory, broker):  # noqa: F811
    schema = "id INT, name STRING, email STRING, seq BIGINT"
    space.sql(f"CREATE TABLE {TARGET} ({PLAIN}, {METADATA}) {USING}")
    broker.send({"id": 1, "name": "alice", "email": "ignored", "seq": 1})
    run(space, pipeline_factory, broker.url, schema, columns=["id", "name", "seq"])
    assert rows(space, "*") == [(1, "alice", 1, (None, 1))]
    broker.send({"id": 2, "name": "bob", "email": "b@x.com", "seq": 2})
    run(space, pipeline_factory, broker.url, schema)
    assert rows(space, "*") == [(1, "alice", 1, (None, 1), None), (2, "bob", 2, (None, 2), "b@x.com")]


def test_narrowing_the_selection_keeps_the_column(space, pipeline_factory, broker):  # noqa: F811
    schema = "id INT, name STRING, email STRING, seq BIGINT"
    space.sql(f"CREATE TABLE {TARGET} ({WITH_EMAIL}, {METADATA}) {USING}")
    broker.send({"id": 1, "name": "alice", "email": "a@x.com", "seq": 1})
    run(space, pipeline_factory, broker.url, schema)
    assert rows(space, "*") == [(1, "alice", "a@x.com", 1, (None, 1))]
    broker.send({"id": 2, "name": "bob", "email": "ignored", "seq": 2})
    run(space, pipeline_factory, broker.url, schema, columns=["id", "name", "seq"])
    assert rows(space, "*") == [(1, "alice", "a@x.com", 1, (None, 1)), (2, "bob", None, 2, (None, 2))]


def test_a_column_differing_from_the_target_by_case_is_ambiguous(space, pipeline_factory, broker):  # noqa: F811
    space.sql(f"CREATE TABLE {TARGET} (id INT NOT NULL, seq BIGINT NOT NULL, value STRING, {METADATA}) {USING}")
    broker.send({"id": 1, "seq": 1, "Value": "alice"})
    with pytest.raises(FAILURE, match="AMBIGUOUS_REFERENCE"):
        run(space, pipeline_factory, broker.url, "id INT, seq BIGINT, Value STRING")


def test_extra_target_columns_survive_the_merge(space, pipeline_factory, broker):  # noqa: F811
    space.sql(
        f"CREATE TABLE {TARGET} (id INT NOT NULL, name STRING, seq BIGINT NOT NULL, extra INT, {METADATA}) {USING}"
    )
    preload(space, "1, 'preloaded', 0, 42", 0)
    broker.send({"id": 1, "name": "alice", "seq": 1}, {"id": 2, "name": "bob", "seq": 1})
    run(space, pipeline_factory, broker.url, BASE)
    assert rows(space, "id", "name", "seq", "extra") == [(1, "alice", 1, 42), (2, "bob", 1, None)]


def test_two_pipelines_merge_into_a_shared_target(space, pipeline_factory, broker, second):  # noqa: F811
    space.sql(f"CREATE TABLE {TARGET} (id INT NOT NULL, name STRING, seq BIGINT NOT NULL, {METADATA}) {USING}")
    broker.send({"id": 1, "name": "alice", "seq": 1}, {"id": 2, "name": "bob", "seq": 1})
    run(space, pipeline_factory, broker.url, BASE, flow="flow_one")
    assert rows(space, "*") == [(1, "alice", 1, (None, 1)), (2, "bob", 1, (None, 1))]
    second.send({"id": 2, "name": "bob-v2", "seq": 2}, {"id": 3, "name": "carol", "seq": 1})
    run(space, pipeline_factory, second.url, BASE, flow="flow_two")
    assert rows(space, "*") == [
        (1, "alice", 1, (None, 1)),
        (2, "bob-v2", 2, (None, 2)),
        (3, "carol", 1, (None, 1)),
    ]
    assert space.catalog.tableExists(AUXILIARY)


def refused_for_other_keys(space, factory, broker, second):
    space.sql(f"CREATE TABLE {TARGET} (id INT NOT NULL, name STRING NOT NULL, seq BIGINT NOT NULL, {METADATA}) {USING}")
    broker.send({"id": 1, "name": "alice", "seq": 1})
    run(space, factory, broker.url, BASE, flow="flow_one")
    second.send({"id": 2, "name": "alice", "seq": 1})
    with pytest.raises(FAILURE) as failure:
        run(space, factory, second.url, BASE, keys="name", flow="flow_two")
    return failure.value


def test_a_second_pipeline_with_other_keys_is_refused(space, pipeline_factory, broker, second):  # noqa: F811
    expected = (
        r"(?s)AUTOCDC_INVALID_STATE\.KEY_SCHEMA_DRIFT.*AutoCDC flow \S*flow_two detected.*"
        r"current key columns name STRING do not match the keys recorded in the auxiliary table "
        r"\S*__spark_autocdc_aux_state_target \(recorded keys id INT\)"
    )
    failure = refused_for_other_keys(space, pipeline_factory, broker, second)
    assert re.search(expected, str(failure))
    assert failure.getSqlState() == "42000"


def test_a_key_drift_failure_renders_its_sqlstate(space, pipeline_factory, broker, second):  # noqa: F811
    assert "SQLSTATE: 42000" in str(refused_for_other_keys(space, pipeline_factory, broker, second))


def test_preloaded_rows_merge_and_the_auxiliary_table_appears_on_the_first_run(space, pipeline_factory, broker):  # noqa: F811
    space.sql(f"CREATE TABLE {TARGET} (id INT NOT NULL, name STRING, seq BIGINT NOT NULL, {METADATA}) {USING}")
    preload(space, "1 AS id, 'alice' AS name, 1 AS seq", 1)
    auxiliary = AUXILIARY
    assert not space.catalog.tableExists(auxiliary)
    broker.send({"id": 1, "name": "bob", "seq": 2})
    run(space, pipeline_factory, broker.url, BASE)
    assert rows(space, "id", "name", "seq", *META) == [(1, "bob", 2, None, 2)]
    assert space.catalog.tableExists(auxiliary)


def test_a_later_pipeline_reads_a_target_without_its_metadata_column(space, pipeline_factory, broker):  # noqa: F811
    space.sql(f"CREATE TABLE {TARGET} ({PLAIN}, {METADATA}) {USING}")
    broker.send({"id": 1, "name": "alice", "seq": 1}, {"id": 2, "name": "bob", "seq": 1})
    run(space, pipeline_factory, broker.url, BASE)
    _, registry, start = pipeline_factory()
    with graph_element_registration_context(registry):

        @dp.materialized_view(name=f"{PREFIX}downstream")
        def downstream():
            return space.read.table(TARGET).select("id", "name", "seq")

    start()
    assert rows(space, "*", table="downstream") == [(1, "alice", 1), (2, "bob", 1)]


def test_two_pipelines_with_other_columns_evolve_a_shared_target(space, pipeline_factory, broker, second):  # noqa: F811
    space.sql(f"CREATE TABLE {TARGET} (id INT NOT NULL, name STRING, seq BIGINT NOT NULL, {METADATA}) {USING}")
    broker.send({"id": 1, "name": "alice", "seq": 1}, {"id": 2, "name": "bob", "seq": 1})
    run(space, pipeline_factory, broker.url, BASE, flow="flow_one")
    assert rows(space, "*") == [(1, "alice", 1, (None, 1)), (2, "bob", 1, (None, 1))]
    second.send({"id": 2, "name": "bob-v2", "age": 25, "seq": 2}, {"id": 3, "name": "carol", "age": 30, "seq": 1})
    run(space, pipeline_factory, second.url, "id INT, name STRING, age INT, seq BIGINT", flow="flow_two")
    assert rows(space, "*") == [
        (1, "alice", 1, (None, 1), None),
        (2, "bob-v2", 2, (None, 2), 25),
        (3, "carol", 1, (None, 1), 30),
    ]
    # The first pipeline returns on its old schema, and the evolved column persists.
    broker.send({"id": 1, "name": "alice-v2", "seq": 2}, {"id": 4, "name": "dave", "seq": 1})
    run(space, pipeline_factory, broker.url, BASE, flow="flow_one")
    assert rows(space, "*") == [
        (1, "alice-v2", 2, (None, 2), None),
        (2, "bob-v2", 2, (None, 2), 25),
        (3, "carol", 1, (None, 1), 30),
        (4, "dave", 1, (None, 1), None),
    ]


def test_an_autocdc_target_feeds_a_materialized_view_in_the_same_graph(space, pipeline_factory, broker):  # noqa: F811
    broker.send({"id": 1, "name": "alice", "seq": 1}, {"id": 1, "name": "alicia", "seq": 2})
    _, registry, start = pipeline_factory()
    with graph_element_registration_context(registry):

        @dp.temporary_view
        def changes():
            return (
                space.readStream.format("json")
                .schema(BASE)
                .load(broker.url)
            )

        dp.create_streaming_table(TARGET, **FORMAT)
        dp.create_auto_cdc_flow(target=TARGET, source="changes", keys=["id"], sequence_by="seq")

        @dp.materialized_view(name=f"{PREFIX}downstream")
        def downstream():
            return space.read.table(TARGET).select("id", "name")

    start()
    assert rows(space, "id", "name", table="downstream") == [(1, "alicia")]


def test_two_targets_keep_independent_state(space, pipeline_factory, broker, second):  # noqa: F811
    space.sql(f"CREATE TABLE {PREFIX}t_a ({PLAIN}, {METADATA}) {USING}")
    space.sql(f"CREATE TABLE {PREFIX}t_b ({PLAIN}, {METADATA}) {USING}")
    broker.send({"id": 1, "name": "alice", "seq": 100})
    run(space, pipeline_factory, broker.url, BASE, name="t_a", flow="flow_a")
    second.send({"id": 9, "name": "bob", "seq": 1})
    run(space, pipeline_factory, second.url, BASE, name="t_b", flow="flow_b")
    assert rows(space, "id", "name", "seq", *META, table="t_a") == [(1, "alice", 100, None, 100)]
    assert rows(space, "id", "name", "seq", *META, table="t_b") == [(9, "bob", 1, None, 1)]
    assert space.catalog.tableExists(f"{PREFIX}__spark_autocdc_aux_state_t_a")
    assert space.catalog.tableExists(f"{PREFIX}__spark_autocdc_aux_state_t_b")


def cdc_view_graph(spark, factory, url, transform):
    _, registry, start = factory()
    with graph_element_registration_context(registry):

        @dp.temporary_view
        def raw():
            return (
                spark.readStream.format("json")
                .schema("id INT, name STRING, seq BIGINT, noise STRING")
                .load(url)
            )

        @dp.temporary_view
        def changes():
            return transform(spark.readStream.table("raw"))

        dp.create_streaming_table(TARGET, **FORMAT)
        dp.create_auto_cdc_flow(target=TARGET, source="changes", keys=["id"], sequence_by="seq")
    start()


def test_key_nullability_and_metadata_changes_are_not_drift(space, pipeline_factory, broker):  # noqa: F811
    space.sql(f"CREATE TABLE {TARGET} (id INT, name STRING, seq BIGINT NOT NULL, {METADATA}) {USING}")
    broker.send({"id": 1, "name": "a", "seq": 1, "noise": "x"})

    def not_null(frame):
        return frame.select(sf.coalesce("id", sf.lit(0)).alias("id"), "name", "seq")

    def described(frame):
        return frame.select(sf.col("id").alias("id", metadata={"description": "primary key"}), "name", "seq")

    cdc_view_graph(space, pipeline_factory, broker.url, not_null)
    broker.send({"id": 2, "name": "b", "seq": 2, "noise": "x"})
    cdc_view_graph(space, pipeline_factory, broker.url, described)
    assert rows(space, "id", "name") == [(1, "a"), (2, "b")]


def test_a_flow_reads_through_a_transforming_view(space, pipeline_factory, broker):  # noqa: F811
    def transform(frame):
        return frame.where("noise <> 'skip'").select("id", sf.upper("name").alias("name"), "seq")

    broker.send(
        {"id": 1, "name": "alice", "seq": 1, "noise": "keep"},
        {"id": 2, "name": "bob", "seq": 1, "noise": "skip"},
    )
    cdc_view_graph(space, pipeline_factory, broker.url, transform)
    assert rows(space, "id", "name", "seq") == [(1, "ALICE", 1)]
    broker.send({"id": 1, "name": "alicia", "seq": 2, "noise": "keep"})
    cdc_view_graph(space, pipeline_factory, broker.url, transform)
    assert rows(space, "id", "name", "seq") == [(1, "ALICIA", 2)]


def test_a_nested_struct_merges_through_a_projection(space, pipeline_factory, broker):  # noqa: F811
    def transform(frame):
        return frame.select("id", "seq", sf.struct("name", "noise").alias("value"))

    broker.send({"id": 1, "name": "alice", "seq": 1, "noise": "x"})
    cdc_view_graph(space, pipeline_factory, broker.url, transform)
    assert rows(space, "id", "value.name", "value.noise") == [(1, "alice", "x")]


def test_a_flow_reads_a_file_table_incrementally(space, pipeline_factory):
    space.sql(f"CREATE TABLE {PREFIX}events (id INT, name STRING, seq BIGINT) {'' if ORACLE else 'USING parquet'}")
    space.sql(f"INSERT INTO {PREFIX}events VALUES (1, 'alice', 1), (2, 'bob', 1)")

    def go():
        _, registry, start = pipeline_factory()
        with graph_element_registration_context(registry):

            @dp.temporary_view
            def changes():
                return space.readStream.table(f"{PREFIX}events")

            dp.create_streaming_table(TARGET, **FORMAT)
            dp.create_auto_cdc_flow(target=TARGET, source="changes", keys=["id"], sequence_by="seq")
        start()

    go()
    assert rows(space, "id", "name") == [(1, "alice"), (2, "bob")]
    space.sql(f"INSERT INTO {PREFIX}events VALUES (1, 'alicia', 2)")
    go()
    assert rows(space, "id", "name") == [(1, "alicia"), (2, "bob")]
    go()
    assert rows(space, "id", "name") == [(1, "alicia"), (2, "bob")]


def test_a_flow_reads_files_under_a_path(space, pipeline_factory, tmp_path):
    path = str(tmp_path / "landing")
    space.createDataFrame([(1, "alice", 1)], "id INT, name STRING, seq BIGINT").write.mode("append").parquet(path)

    def go():
        _, registry, start = pipeline_factory()
        with graph_element_registration_context(registry):

            @dp.temporary_view
            def changes():
                return space.readStream.format("parquet").schema(BASE).load(path)

            dp.create_streaming_table(TARGET, **FORMAT)
            dp.create_auto_cdc_flow(target=TARGET, source="changes", keys=["id"], sequence_by="seq")
        start()

    go()
    assert rows(space, "id", "name") == [(1, "alice")]
    space.createDataFrame([(2, "bob", 1)], "id INT, name STRING, seq BIGINT").write.mode("append").parquet(path)
    go()
    assert rows(space, "id", "name") == [(1, "alice"), (2, "bob")]


def test_a_flow_unions_two_streams(space, pipeline_factory, tmp_path):
    left, right = str(tmp_path / "left"), str(tmp_path / "right")
    space.createDataFrame([(1, "left", 1)], "id INT, name STRING, seq BIGINT").write.parquet(left)
    space.createDataFrame([(1, "right", 2), (2, "right", 1)], "id INT, name STRING, seq BIGINT").write.parquet(right)
    _, registry, start = pipeline_factory()
    with graph_element_registration_context(registry):

        @dp.temporary_view
        def changes():
            one = space.readStream.format("parquet").schema(BASE).load(left)
            two = space.readStream.format("parquet").schema(BASE).load(right)
            return one.unionByName(two)

        dp.create_streaming_table(TARGET, **FORMAT)
        dp.create_auto_cdc_flow(target=TARGET, source="changes", keys=["id"], sequence_by="seq")
    start()
    assert rows(space, "id", "name") == [(1, "right"), (2, "right")]


@sail_only
def test_an_unknown_streaming_format_is_refused(space, pipeline_factory):
    _, registry, start = pipeline_factory()
    with graph_element_registration_context(registry):

        @dp.temporary_view
        def changes():
            return space.readStream.format("socket").option("host", "localhost").option("port", "1").load()

        dp.create_streaming_table(TARGET, **FORMAT)
        dp.create_auto_cdc_flow(target=TARGET, source="changes", keys=["value"], sequence_by="value")
    with pytest.raises(FAILURE, match="incremental pipeline execution"):
        start()


def test_a_flow_streams_from_a_table_the_same_pipeline_writes(space, pipeline_factory):
    _, registry, start = pipeline_factory()
    with graph_element_registration_context(registry):

        @dp.materialized_view(name=f"{PREFIX}bronze")
        def bronze():
            return space.sql("SELECT * FROM VALUES (1, 'alice', 1L), (1, 'alicia', 2L) AS t(id, name, seq)")

        @dp.temporary_view
        def changes():
            return space.readStream.table(f"{PREFIX}bronze")

        dp.create_streaming_table(TARGET, **FORMAT)
        dp.create_auto_cdc_flow(target=TARGET, source="changes", keys=["id"], sequence_by="seq")
    start()
    assert rows(space, "id", "name") == [(1, "alicia")]


def test_a_full_refresh_changes_the_keys_and_the_types(space, pipeline_factory, broker):  # noqa: F811
    broker.send({"id": 1, "name": "alice", "seq": 1})
    run(space, pipeline_factory, broker.url, "id INT, name STRING, seq BIGINT")
    broker.send({"id": 2, "name": "bob", "seq": 2})
    schema = "id BIGINT, name STRING, seq BIGINT"
    with pytest.raises(FAILURE, match="CANNOT_MERGE_INCOMPATIBLE_DATA_TYPE"):
        run(space, pipeline_factory, broker.url, schema)
    run(space, pipeline_factory, broker.url, schema, keys="name", full_refresh_all=True)
    assert rows(space, "id", "name") == [(1, "alice"), (2, "bob")]
    assert space.table(TARGET).schema["id"].dataType.simpleString() == "bigint"
    broker.send({"id": 9, "name": "bob", "seq": 3})
    run(space, pipeline_factory, broker.url, schema, keys="name")
    assert rows(space, "id", "name") == [(1, "alice"), (9, "bob")]


def test_a_dropped_auxiliary_table_is_recreated(space, pipeline_factory, broker):  # noqa: F811
    broker.send({"id": 1, "name": "alice", "seq": 1})
    run(space, pipeline_factory, broker.url, BASE)
    space.sql(f"DROP TABLE {AUXILIARY}")
    broker.send({"id": 1, "name": "alicia", "seq": 2})
    run(space, pipeline_factory, broker.url, BASE)
    assert rows(space, "id", "name", "seq", *META) == [(1, "alicia", 2, None, 2)]
    assert space.catalog.tableExists(AUXILIARY)


def test_a_full_refresh_creates_a_target_that_does_not_exist(space, pipeline_factory, broker):  # noqa: F811
    broker.send({"id": 1, "name": "alice", "seq": 1})
    run(space, pipeline_factory, broker.url, BASE, full_refresh_all=True)
    assert rows(space, "id", "name") == [(1, "alice")]


@sail_only
@delta_only
def test_an_alter_of_an_enrolled_table_is_refused(space, pipeline_factory, broker):  # noqa: F811
    broker.send({"id": 1, "name": "alice", "seq": 1})
    run(space, pipeline_factory, broker.url, BASE)
    with pytest.raises(FAILURE, match="enrolled"):
        space.sql("ALTER TABLE target SET TBLPROPERTIES ('owner' = 'me')").collect()


@pytest.mark.parametrize(
    ("keys", "columns", "condition"),
    [
        ("missing", None, "AUTOCDC_"),
        ("id", ["id", "seq", "missing"], "AUTOCDC_"),
    ],
)
def test_a_dry_run_reports_what_a_run_would(space, pipeline_factory, tmp_path, keys, columns, condition):
    path = str(tmp_path / "landing")
    space.createDataFrame([(1, "alice", 1)], BASE).write.parquet(path)
    _, registry, start = pipeline_factory()
    with graph_element_registration_context(registry):

        @dp.temporary_view
        def changes():
            return space.readStream.format("parquet").schema(BASE).load(path)

        dp.create_streaming_table(TARGET, **FORMAT)
        dp.create_auto_cdc_flow(
            target=TARGET, source="changes", keys=[keys], sequence_by="seq", column_list=columns
        )
    with pytest.raises(FAILURE, match=condition):
        start(dry=True)
    assert not space.catalog.tableExists("target")


def test_a_view_downstream_of_a_target_sees_what_the_flow_writes(space, pipeline_factory, tmp_path):
    path = str(tmp_path / "landing")
    space.createDataFrame([(1, "alice", 1)], BASE).write.parquet(path)
    _, registry, start = pipeline_factory()
    with graph_element_registration_context(registry):

        @dp.temporary_view
        def changes():
            return space.readStream.format("parquet").schema(BASE).load(path)

        dp.create_streaming_table(TARGET, **FORMAT)
        dp.create_auto_cdc_flow(
            target=TARGET, source="changes", keys=["id"], sequence_by="seq", except_column_list=["name"]
        )

        @dp.materialized_view(name=f"{PREFIX}downstream")
        def downstream():
            return space.read.table(TARGET).select("id", "__spark_autocdc_metadata.upsertSequence")

    start()
    assert rows(space, "id", "upsertSequence", table="downstream") == [(1, 1)]


NESTED = "key INT, seq BIGINT, a INT, b_c INT, b_d INT"


def nested_run(spark, factory, url, transform):
    _, registry, start = factory()
    with graph_element_registration_context(registry):

        @dp.temporary_view
        def raw():
            return (
                spark.readStream.format("json")
                .schema(NESTED)
                .load(url)
            )

        @dp.temporary_view
        def changes():
            return transform(spark.readStream.table("raw"))

        dp.create_streaming_table(TARGET, **FORMAT)
        dp.create_auto_cdc_flow(target=TARGET, source="changes", keys=["key"], sequence_by="seq")
    start()


def _event(key, seq, a, c, d):
    return {"key": key, "seq": seq, "a": a, "b_c": c, "b_d": d}


def _struct(*fields):
    return lambda frame: frame.select(
        "key",
        "seq",
        sf.struct("a", sf.struct(*[sf.col(source).alias(name) for name, source in fields]).alias("b")).alias("value"),
    )


def _array(*fields):
    return lambda frame: frame.select(
        "key",
        "seq",
        sf.array(
            sf.struct("a", sf.struct(*[sf.col(source).alias(name) for name, source in fields]).alias("b"))
        ).alias("vals"),
    )


def test_dropping_a_nested_struct_field_is_refused(space, pipeline_factory, broker):  # noqa: F811
    broker.send(_event(1, 1, 1, 1, 1), _event(2, 1, 2, 2, 2))
    nested_run(space, pipeline_factory, broker.url, _struct(("c", "b_c"), ("d", "b_d")))
    broker.send(_event(1, 2, 10, 99, 10), _event(3, 1, 3, 99, 3))
    expected = r"(?s)INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_FIND_DATA.*table ``: Cannot find data for the output column `value`\.`b`\.`c`"  # noqa: E501
    with pytest.raises(FAILURE, match=expected):
        nested_run(space, pipeline_factory, broker.url, _struct(("d", "b_d")))


def test_a_new_field_in_an_array_of_structs_is_added(space, pipeline_factory, broker):  # noqa: F811
    broker.send(_event(1, 1, 1, 1, 99))
    nested_run(space, pipeline_factory, broker.url, _array(("c", "b_c")))
    broker.send(_event(1, 2, 1, 1, 2), _event(3, 1, 3, 3, 3))
    nested_run(space, pipeline_factory, broker.url, _array(("c", "b_c"), ("d", "b_d")))
    exploded = space.table(TARGET).selectExpr("key", "inline(vals) AS (a, b)").select("key", "a", "b.c", "b.d")
    assert sorted(tuple(row) for row in exploded.collect()) == [(1, 1, 1, 2), (3, 3, 3, 3)]


def test_dropping_a_field_in_an_array_of_structs_is_refused(space, pipeline_factory, broker):  # noqa: F811
    broker.send(_event(1, 1, 1, 1, 1), _event(2, 1, 2, 2, 2))
    nested_run(space, pipeline_factory, broker.url, _array(("c", "b_c"), ("d", "b_d")))
    broker.send(_event(1, 2, 10, 10, 99), _event(3, 1, 3, 3, 99))
    expected = r"(?s)INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_FIND_DATA.*table ``: Cannot find data for the output column `vals`\.`element`\.`b`\.`d`"  # noqa: E501
    with pytest.raises(FAILURE, match=expected):
        nested_run(space, pipeline_factory, broker.url, _array(("c", "b_c")))


def test_a_composite_key_merges_on_every_key(space, pipeline_factory, broker):  # noqa: F811
    schema = "region STRING, id INT, name STRING, seq BIGINT"
    broker.send(
        {"region": "eu", "id": 1, "name": "a", "seq": 1},
        {"region": "us", "id": 1, "name": "b", "seq": 1},
    )
    run(space, pipeline_factory, broker.url, schema, keys=["region", "id"])
    broker.send({"region": "eu", "id": 1, "name": "c", "seq": 2})
    run(space, pipeline_factory, broker.url, schema, keys=["region", "id"])
    assert rows(space, "region", "id", "name") == [("eu", 1, "c"), ("us", 1, "b")]


@sail_only
@delta_only
def test_a_flow_reads_a_delta_table_incrementally(space, pipeline_factory):
    space.sql(f"CREATE TABLE {PREFIX}bronze (id INT, name STRING, seq BIGINT) {USING}")
    space.sql(f"INSERT INTO {PREFIX}bronze VALUES (1, 'alice', 1), (2, 'bob', 1)")

    def go():
        _, registry, start = pipeline_factory()
        with graph_element_registration_context(registry):

            @dp.temporary_view
            def changes():
                return space.readStream.table(f"{PREFIX}bronze").where("name <> 'skip'")

            dp.create_streaming_table(TARGET, **FORMAT)
            dp.create_auto_cdc_flow(target=TARGET, source="changes", keys=["id"], sequence_by="seq")
        start()

    go()
    assert rows(space, "id", "name") == [(1, "alice"), (2, "bob")]
    space.sql(f"INSERT INTO {PREFIX}bronze VALUES (1, 'alicia', 2), (2, 'skip', 2)")
    go()
    assert rows(space, "id", "name") == [(1, "alicia"), (2, "bob")]
    go()
    assert rows(space, "id", "name") == [(1, "alicia"), (2, "bob")]


@sail_only
def test_an_iceberg_target_merges_evolves_and_refreshes(space, pipeline_factory, broker):  # noqa: F811
    def go(schema, keys="id", **options):
        _, registry, start = pipeline_factory()
        with graph_element_registration_context(registry):

            @dp.temporary_view
            def changes():
                return space.readStream.format("json").schema(schema).load(broker.url)

            dp.create_streaming_table("target", format="iceberg")
            dp.create_auto_cdc_flow(target="target", source="changes", keys=[keys], sequence_by="seq")
        start(**options)

    space.sql("DROP TABLE IF EXISTS target")
    broker.send({"id": 1, "name": "alice", "seq": 1}, {"id": 2, "name": "bob", "seq": 1})
    go(BASE)
    assert rows(space, "id", "name") == [(1, "alice"), (2, "bob")]
    broker.send({"id": 1, "name": "alicia", "seq": 2, "age": 30})
    go("id INT, name STRING, seq BIGINT, age INT")
    assert rows(space, "id", "name", "age") == [(1, "alicia", 30), (2, "bob", None)]
    go("id BIGINT, name STRING, seq BIGINT", keys="name", full_refresh_all=True)
    assert rows(space, "id", "name") == [(1, "alice"), (1, "alicia"), (2, "bob")]
    assert space.table("target").schema["id"].dataType.simpleString() == "bigint"


def test_a_rate_source_feeds_a_flow(space, pipeline_factory):
    def go():
        _, registry, start = pipeline_factory()
        with graph_element_registration_context(registry):

            @dp.temporary_view
            def changes():
                return space.readStream.format("rate").option("rowsPerSecond", "5").load()

            dp.create_streaming_table(TARGET, **FORMAT)
            dp.create_auto_cdc_flow(target=TARGET, source="changes", keys=["value"], sequence_by="timestamp")
        start()

    go()
    time.sleep(2.2)
    go()
    assert space.table(TARGET).columns[:2] == ["timestamp", "value"]
    values = [row[0] for row in space.table(TARGET).select("value").orderBy("value").collect()]
    assert len(values) >= 5  # noqa: PLR2004
    assert values == list(range(values[0], values[0] + len(values)))


def direct_source(spark, factory, target, source, bronze, **defaults):
    """The flow is declared before `bronze`, so order cannot stand in for the dependency."""
    _, registry, start = factory(**defaults)
    with graph_element_registration_context(registry):
        dp.create_streaming_table(target, **FORMAT)
        dp.create_auto_cdc_flow(target=target, source=source, keys=["id"], sequence_by="seq")

        @dp.materialized_view(name=bronze)
        def bronze_rows():
            return spark.sql("SELECT * FROM VALUES (1, 'alice', 1L), (1, 'alicia', 2L) AS t(id, name, seq)")

    start()


@pytest.mark.parametrize("qualified", [False, True])
def test_a_source_named_directly_is_the_flows_input(space, pipeline_factory, qualified):
    prefix = f"{ORACLE or space.catalog.currentDatabase()}." if qualified else PREFIX
    direct_source(space, pipeline_factory, f"{prefix}target", f"{prefix}bronze", f"{prefix}bronze")
    assert rows(space, "id", "name") == [(1, "alicia")]


def test_a_single_part_source_inherits_the_graph_defaults(space, pipeline_factory):
    catalog, database = ORACLE.split(".") if ORACLE else (space.catalog.currentCatalog(), "cdc_defaults")
    space.sql(f"CREATE NAMESPACE IF NOT EXISTS {catalog}.{database}")
    try:
        direct_source(space, pipeline_factory, "target", "bronze", "bronze", catalog=catalog, database=database)
        frame = space.table(f"{catalog}.{database}.target")
        assert sorted(tuple(row) for row in frame.select("id", "name").collect()) == [(1, "alicia")]
    finally:
        for table in ["target", "bronze", "__spark_autocdc_aux_state_target"]:
            space.sql(f"DROP TABLE IF EXISTS {catalog}.{database}.{table}")


def test_a_key_with_a_dot_needs_quoting_as_a_column(space, pipeline_factory, broker):  # noqa: F811
    broker.send({"a.b": 1, "c": "x", "seq": 1})
    run(space, pipeline_factory, broker.url, "`a.b` INT, c STRING, seq BIGINT", keys="`a.b`")
    assert [tuple(row) for row in space.table(TARGET).select("`a.b`", "c").collect()] == [(1, "x")]
    with pytest.raises(FAILURE, match="UNRESOLVED_COLUMN.WITH_SUGGESTION") as failure:
        space.table(TARGET).select("a.b").collect()
    assert "42703" in str(failure.value)
    assert "`a`.`b`" in str(failure.value)


@sail_only
def test_state_written_by_another_query_asks_for_a_reset(space, pipeline_factory, broker):  # noqa: F811
    """Spark refuses to read operator state under a changed schema; so does Sail."""
    schema = "id INT, name STRING, seq BIGINT"

    def go(key, reset=False):
        _, registry, start = pipeline_factory()
        with graph_element_registration_context(registry):

            @dp.temporary_view
            def changes():
                return space.readStream.format("json").schema(schema).load(broker.url).dropDuplicates([key])

            dp.create_streaming_table(TARGET, **FORMAT)
            dp.create_auto_cdc_flow(target=TARGET, source="changes", keys=["id"], sequence_by="seq")
        start(full_refresh_all=True) if reset else start()

    broker.send({"id": 1, "name": "a", "seq": 1})
    go("id")
    broker.send({"id": 2, "name": "b", "seq": 1})
    with pytest.raises((PySparkException, SparkConnectGrpcException), match="reset the flow"):
        go("name")
    go("name", reset=True)
    assert rows(space, "id", "seq") == [(1, 1), (2, 1)]


@sail_only
def test_another_session_cannot_write_an_enrolled_target(space, pipeline_factory, broker, remote, tmp_path):  # noqa: F811
    """The target names the authority that fences it, so a session that never ran
    the pipeline, with no authority of its own, is refused a write to it."""
    schema = "id INT, name STRING, seq BIGINT"
    _, registry, start = pipeline_factory()
    with graph_element_registration_context(registry):

        @dp.temporary_view
        def changes():
            return space.readStream.format("json").schema(schema).load(broker.url)

        dp.create_streaming_table(TARGET, **FORMAT)
        dp.create_auto_cdc_flow(target=TARGET, source="changes", keys=["id"], sequence_by="seq")

    broker.send({"id": 1, "name": "a", "seq": 1})
    start()
    [table] = [d for d in (tmp_path / "warehouse").iterdir() if d.name.startswith("target-") and (d / "_delta_log").is_dir()]
    from pysail.testing.spark.session import spark_session_factory

    with spark_session_factory(remote) as sessions:
        other = sessions.create()
        frame = other.createDataFrame([(2, "b", 2)], schema)
        with pytest.raises((PySparkException, SparkConnectGrpcException), match="enrolled with AutoCDC authority"):
            frame.write.format("delta").mode("append").save(table.as_uri())
        with pytest.raises((PySparkException, SparkConnectGrpcException), match="enrolled"):
            other.sql(f"ALTER TABLE delta.`{table.as_uri()}` SET TBLPROPERTIES ('x' = 'y')")
    assert rows(space, "id", "seq") == [(1, 1)]


def test_a_dropped_auxiliary_table_forgets_its_tombstones(space, pipeline_factory, broker):  # noqa: F811
    schema = "id INT, name STRING, seq BIGINT, is_delete BOOLEAN"

    def go():
        _, registry, start = pipeline_factory()
        with graph_element_registration_context(registry):

            @dp.temporary_view
            def changes():
                return space.readStream.format("json").schema(schema).load(broker.url)

            dp.create_streaming_table(TARGET, **FORMAT)
            dp.create_auto_cdc_flow(
                target=TARGET,
                source="changes",
                keys=["id"],
                sequence_by="seq",
                apply_as_deletes=sf.expr("is_delete"),
                except_column_list=["is_delete"],
            )
        start()

    broker.send({"id": 1, "name": "gone", "seq": 10, "is_delete": True})
    go()
    space.sql(f"DROP TABLE {AUXILIARY}")
    broker.send({"id": 1, "name": "late", "seq": 5, "is_delete": False})
    go()
    assert rows(space, "id", "name", "seq") == [(1, "late", 5)]
