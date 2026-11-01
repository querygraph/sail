"""AutoCdcScd1TargetTableDurabilitySuite, AutoCdcScd1MultiPipelineSuite and
AutoCdcScd1SchemaEvolutionSuite at the Spark pin, driven through a real broker.

A MemoryStream becomes a topic, so a schema change between runs is a change to
the view's declared schema. Cases that project a struct over the source are not
here: an admitted source is a bare streaming read, so they have no public form.
"""

from __future__ import annotations

import pytest
from pyspark.errors.exceptions.base import PySparkException
from pyspark.errors.exceptions.connect import SparkConnectGrpcException
from pyspark.sql import functions as sf

from pysail.tests.spark.pipelines.test_autocdc_iggy import STREAM, TOPIC, broker  # noqa: F401
from pysail.tests.spark.pipelines.test_autocdc_iggy import dp, graph_element_registration_context

METADATA = "__spark_autocdc_metadata STRUCT<deleteSequence:BIGINT,upsertSequence:BIGINT>"
FAILURE = (PySparkException, SparkConnectGrpcException)


@pytest.fixture
def space(pipeline_spark):
    pipeline_spark.sql("DROP TABLE IF EXISTS target")
    yield pipeline_spark
    pipeline_spark.sql("DROP TABLE IF EXISTS target")


def run(spark, factory, url, schema, *, keys="id", sequence="seq", name="target", columns=None, flow=None):
    keys = [keys] if isinstance(keys, str) else list(keys)
    _, registry, start = factory()
    with graph_element_registration_context(registry):

        @dp.temporary_view
        def changes():
            return (
                spark.readStream.format("iggy")
                .option("url", url)
                .option("stream", STREAM)
                .option("topic", TOPIC)
                .option("timeout_ms", "5000")
                .schema(schema)
                .load()
            )

        dp.create_streaming_table(name, format="delta")
        dp.create_auto_cdc_flow(
            target=name,
            source="changes",
            keys=keys,
            sequence_by=sequence,
            name=flow,
            column_list=columns,
        )
    start()


def rows(spark, *columns, table="target"):
    return sorted(tuple(row) for row in spark.table(table).select(*columns).collect())


def preload(spark, values, sequence):
    spark.sql(
        f"INSERT INTO target SELECT {values}, "
        f"named_struct('deleteSequence', CAST(NULL AS BIGINT), 'upsertSequence', CAST({sequence} AS BIGINT))"
    )


BASE = "id INT, name STRING, seq BIGINT"


def test_a_preloaded_row_outranks_a_lower_sequence(space, pipeline_factory, broker):  # noqa: F811
    space.sql(f"CREATE TABLE target (id INT NOT NULL, name STRING, seq BIGINT NOT NULL, {METADATA}) USING delta")
    preload(space, "1, 'alice', 5", 5)
    preload(space, "2, 'bob', 5", 5)
    broker.send({"id": 1, "name": "stale", "seq": 2}, {"id": 2, "name": "bob2", "seq": 10})
    run(space, pipeline_factory, broker.url, BASE)
    assert rows(space, "id", "name", "seq") == [(1, "alice", 5), (2, "bob2", 10)]


def test_a_target_without_the_metadata_column_gains_it(space, pipeline_factory, broker):  # noqa: F811
    space.sql("CREATE TABLE target (id INT NOT NULL, name STRING, seq BIGINT NOT NULL) USING delta")
    broker.send({"id": 1, "name": "alice", "seq": 1})
    run(space, pipeline_factory, broker.url, BASE)
    assert "__spark_autocdc_metadata" in space.table("target").columns
    assert rows(space, "id", "name", "seq") == [(1, "alice", 1)]


def test_a_nullable_column_merges_mixed_values(space, pipeline_factory, broker):  # noqa: F811
    schema = "id INT, name STRING, email STRING, seq BIGINT"
    broker.send({"id": 1, "name": "alice", "email": None, "seq": 1})
    run(space, pipeline_factory, broker.url, schema)
    broker.send({"id": 1, "name": "alice2", "email": "a@x.com", "seq": 2})
    run(space, pipeline_factory, broker.url, schema)
    assert rows(space, "id", "name", "email", "seq") == [(1, "alice2", "a@x.com", 2)]


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
    with pytest.raises(FAILURE, match="CANNOT_MERGE_INCOMPATIBLE_DATA_TYPE") as failure:
        run(space, pipeline_factory, broker.url, f"id INT, age {second}, seq BIGINT")
    assert f'"{left}"' in str(failure.value)
    assert f'"{right}"' in str(failure.value)


def test_a_new_source_column_is_added_to_the_target(space, pipeline_factory, broker):  # noqa: F811
    broker.send({"id": 1, "name": "alice", "seq": 1})
    run(space, pipeline_factory, broker.url, BASE)
    broker.send({"id": 2, "name": "bob", "email": "b@x.com", "seq": 2})
    run(space, pipeline_factory, broker.url, "id INT, name STRING, email STRING, seq BIGINT")
    assert rows(space, "id", "email") == [(1, None), (2, "b@x.com")]


def test_a_dropped_source_column_keeps_its_rows_and_is_null_on_new_ones(space, pipeline_factory, broker):  # noqa: F811
    broker.send({"id": 1, "name": "alice", "email": "a@x.com", "seq": 1})
    run(space, pipeline_factory, broker.url, "id INT, name STRING, email STRING, seq BIGINT")
    broker.send({"id": 2, "name": "bob", "seq": 2})
    run(space, pipeline_factory, broker.url, BASE)
    assert rows(space, "id", "email") == [(1, "a@x.com"), (2, None)]


def test_broadening_the_selection_adds_the_column(space, pipeline_factory, broker):  # noqa: F811
    schema = "id INT, name STRING, email STRING, seq BIGINT"
    broker.send({"id": 1, "name": "alice", "email": "ignored", "seq": 1})
    run(space, pipeline_factory, broker.url, schema, columns=["id", "name", "seq"])
    broker.send({"id": 2, "name": "bob", "email": "b@x.com", "seq": 2})
    run(space, pipeline_factory, broker.url, schema)
    assert rows(space, "id", "email") == [(1, None), (2, "b@x.com")]


def test_narrowing_the_selection_keeps_the_column(space, pipeline_factory, broker):  # noqa: F811
    schema = "id INT, name STRING, email STRING, seq BIGINT"
    broker.send({"id": 1, "name": "alice", "email": "a@x.com", "seq": 1})
    run(space, pipeline_factory, broker.url, schema)
    broker.send({"id": 2, "name": "bob", "email": "ignored", "seq": 2})
    run(space, pipeline_factory, broker.url, schema, columns=["id", "name", "seq"])
    assert rows(space, "id", "email") == [(1, "a@x.com"), (2, None)]


def test_a_column_differing_from_the_target_by_case_is_ambiguous(space, pipeline_factory, broker):  # noqa: F811
    space.sql(f"CREATE TABLE target (id INT NOT NULL, seq BIGINT NOT NULL, value STRING, {METADATA}) USING delta")
    broker.send({"id": 1, "seq": 1, "Value": "alice"})
    with pytest.raises(FAILURE, match="AMBIGUOUS_REFERENCE"):
        run(space, pipeline_factory, broker.url, "id INT, seq BIGINT, Value STRING")


def test_extra_target_columns_survive_the_merge(space, pipeline_factory, broker):  # noqa: F811
    space.sql(
        f"CREATE TABLE target (id INT NOT NULL, name STRING, seq BIGINT NOT NULL, extra INT, {METADATA}) USING delta"
    )
    preload(space, "1, 'preloaded', 0, 42", 0)
    broker.send({"id": 1, "name": "alice", "seq": 1}, {"id": 2, "name": "bob", "seq": 1})
    run(space, pipeline_factory, broker.url, BASE)
    assert rows(space, "id", "name", "seq", "extra") == [(1, "alice", 1, 42), (2, "bob", 1, None)]


def test_two_pipelines_merge_into_a_shared_target(space, pipeline_factory, broker):  # noqa: F811
    space.sql(f"CREATE TABLE target (id INT NOT NULL, name STRING, seq BIGINT NOT NULL, {METADATA}) USING delta")
    broker.send({"id": 1, "name": "alice", "seq": 1}, {"id": 2, "name": "bob", "seq": 1})
    run(space, pipeline_factory, broker.url, BASE, flow="flow_one")
    broker.send({"id": 2, "name": "bob-v2", "seq": 2}, {"id": 3, "name": "carol", "seq": 1})
    run(space, pipeline_factory, broker.url, BASE, flow="flow_two")
    assert rows(space, "id", "name", "seq") == [(1, "alice", 1), (2, "bob-v2", 2), (3, "carol", 1)]


def test_a_second_pipeline_with_other_keys_is_refused(space, pipeline_factory, broker):  # noqa: F811
    space.sql(f"CREATE TABLE target (id INT NOT NULL, name STRING NOT NULL, seq BIGINT NOT NULL, {METADATA}) USING delta")
    broker.send({"id": 1, "name": "alice", "seq": 1})
    run(space, pipeline_factory, broker.url, BASE, flow="flow_one")
    broker.send({"id": 2, "name": "alice", "seq": 1})
    with pytest.raises(FAILURE, match="KEY_SCHEMA_DRIFT"):
        run(space, pipeline_factory, broker.url, BASE, keys="name", flow="flow_two")


def test_preloaded_rows_merge_and_the_auxiliary_table_appears_on_the_first_run(space, pipeline_factory, broker):  # noqa: F811
    space.sql(f"CREATE TABLE target (id INT NOT NULL, name STRING, seq BIGINT NOT NULL, {METADATA}) USING delta")
    preload(space, "1 AS id, 'alice' AS name, 1 AS seq", 1)
    auxiliary = "__spark_autocdc_aux_state_target"
    assert not space.catalog.tableExists(auxiliary)
    broker.send({"id": 1, "name": "bob", "seq": 2})
    run(space, pipeline_factory, broker.url, BASE)
    assert rows(space, "id", "name", "seq") == [(1, "bob", 2)]
    assert space.catalog.tableExists(auxiliary)


def test_a_later_pipeline_reads_a_target_without_its_metadata_column(space, pipeline_factory, broker):  # noqa: F811
    broker.send({"id": 1, "name": "alice", "seq": 1}, {"id": 2, "name": "bob", "seq": 1})
    run(space, pipeline_factory, broker.url, BASE)
    _, registry, start = pipeline_factory()
    with graph_element_registration_context(registry):

        @dp.materialized_view
        def downstream():
            return space.read.table("target").select("id", "name", "seq")

    start()
    assert rows(space, "id", "name", "seq", table="downstream") == [(1, "alice", 1), (2, "bob", 1)]
    space.sql("DROP TABLE IF EXISTS downstream")


def test_two_pipelines_with_other_columns_evolve_a_shared_target(space, pipeline_factory, broker):  # noqa: F811
    space.sql(f"CREATE TABLE target (id INT NOT NULL, name STRING, seq BIGINT NOT NULL, {METADATA}) USING delta")
    broker.send({"id": 1, "name": "alice", "seq": 1}, {"id": 2, "name": "bob", "seq": 1})
    run(space, pipeline_factory, broker.url, BASE, flow="flow_one")
    broker.send({"id": 2, "name": "bob-v2", "age": 25, "seq": 2}, {"id": 3, "name": "carol", "age": 30, "seq": 1})
    run(space, pipeline_factory, broker.url, "id INT, name STRING, age INT, seq BIGINT", flow="flow_two")
    assert rows(space, "id", "name", "seq", "age") == [
        (1, "alice", 1, None),
        (2, "bob-v2", 2, 25),
        (3, "carol", 1, 30),
    ]
