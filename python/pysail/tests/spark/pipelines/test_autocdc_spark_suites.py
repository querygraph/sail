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


def run(
    spark, factory, url, schema, *, keys="id", sequence="seq", name="target", columns=None, flow=None, **options
):
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
    start(**options)


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


def test_an_autocdc_target_feeds_a_materialized_view_in_the_same_graph(space, pipeline_factory, broker):  # noqa: F811
    broker.send({"id": 1, "name": "alice", "seq": 1}, {"id": 1, "name": "alicia", "seq": 2})
    _, registry, start = pipeline_factory()
    with graph_element_registration_context(registry):

        @dp.temporary_view
        def changes():
            return (
                space.readStream.format("iggy")
                .option("url", broker.url)
                .option("stream", STREAM)
                .option("topic", TOPIC)
                .option("timeout_ms", "5000")
                .schema(BASE)
                .load()
            )

        dp.create_streaming_table("target", format="delta")
        dp.create_auto_cdc_flow(target="target", source="changes", keys=["id"], sequence_by="seq")

        @dp.materialized_view
        def downstream():
            return space.read.table("target").select("id", "name")

    start()
    assert rows(space, "id", "name", table="downstream") == [(1, "alicia")]
    space.sql("DROP TABLE IF EXISTS downstream")


def test_two_targets_keep_independent_state(space, pipeline_factory, broker):  # noqa: F811
    broker.send({"id": 1, "name": "alice", "seq": 100})
    run(space, pipeline_factory, broker.url, BASE, name="t_a", flow="flow_a")
    broker.send({"id": 9, "name": "bob", "seq": 1})
    run(space, pipeline_factory, broker.url, BASE, name="t_b", flow="flow_b")
    assert rows(space, "id", "name", "seq", table="t_a") == [(1, "alice", 100)]
    assert rows(space, "id", "name", "seq", table="t_b") == [(1, "alice", 100), (9, "bob", 1)]
    assert space.catalog.tableExists("__spark_autocdc_aux_state_t_a")
    assert space.catalog.tableExists("__spark_autocdc_aux_state_t_b")
    space.sql("DROP TABLE IF EXISTS t_a")
    space.sql("DROP TABLE IF EXISTS t_b")


def cdc_view_graph(spark, factory, url, transform):
    _, registry, start = factory()
    with graph_element_registration_context(registry):

        @dp.temporary_view
        def raw():
            return (
                spark.readStream.format("iggy")
                .option("url", url)
                .option("stream", STREAM)
                .option("topic", TOPIC)
                .option("timeout_ms", "5000")
                .schema("id INT, name STRING, seq BIGINT, noise STRING")
                .load()
            )

        @dp.temporary_view
        def changes():
            return transform(spark.readStream.table("raw"))

        dp.create_streaming_table("target", format="delta")
        dp.create_auto_cdc_flow(target="target", source="changes", keys=["id"], sequence_by="seq")
    start()


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
    space.sql("DROP TABLE IF EXISTS events")
    space.sql("CREATE TABLE events (id INT, name STRING, seq BIGINT) USING parquet")
    space.sql("INSERT INTO events VALUES (1, 'alice', 1), (2, 'bob', 1)")

    def go():
        _, registry, start = pipeline_factory()
        with graph_element_registration_context(registry):

            @dp.temporary_view
            def changes():
                return space.readStream.table("events")

            dp.create_streaming_table("target", format="delta")
            dp.create_auto_cdc_flow(target="target", source="changes", keys=["id"], sequence_by="seq")
        start()

    go()
    assert rows(space, "id", "name") == [(1, "alice"), (2, "bob")]
    space.sql("INSERT INTO events VALUES (1, 'alicia', 2)")
    go()
    assert rows(space, "id", "name") == [(1, "alicia"), (2, "bob")]
    go()
    assert rows(space, "id", "name") == [(1, "alicia"), (2, "bob")]
    space.sql("DROP TABLE IF EXISTS events")


def test_a_flow_reads_files_under_a_path(space, pipeline_factory, tmp_path):
    path = str(tmp_path / "landing")
    space.createDataFrame([(1, "alice", 1)], "id INT, name STRING, seq BIGINT").write.mode("append").parquet(path)

    def go():
        _, registry, start = pipeline_factory()
        with graph_element_registration_context(registry):

            @dp.temporary_view
            def changes():
                return space.readStream.format("parquet").schema(BASE).load(path)

            dp.create_streaming_table("target", format="delta")
            dp.create_auto_cdc_flow(target="target", source="changes", keys=["id"], sequence_by="seq")
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

        dp.create_streaming_table("target", format="delta")
        dp.create_auto_cdc_flow(target="target", source="changes", keys=["id"], sequence_by="seq")
    start()
    assert rows(space, "id", "name") == [(1, "right"), (2, "right")]


def test_an_unknown_streaming_format_is_refused(space, pipeline_factory):
    _, registry, start = pipeline_factory()
    with graph_element_registration_context(registry):

        @dp.temporary_view
        def changes():
            return space.readStream.format("rate").load()

        dp.create_streaming_table("target", format="delta")
        dp.create_auto_cdc_flow(target="target", source="changes", keys=["value"], sequence_by="timestamp")
    with pytest.raises(FAILURE, match="incremental pipeline execution"):
        start()


def test_a_flow_streams_from_a_table_the_same_pipeline_writes(space, pipeline_factory):
    space.sql("DROP TABLE IF EXISTS bronze")
    _, registry, start = pipeline_factory()
    with graph_element_registration_context(registry):

        @dp.materialized_view
        def bronze():
            return space.sql("SELECT * FROM VALUES (1, 'alice', 1L), (1, 'alicia', 2L) AS t(id, name, seq)")

        @dp.temporary_view
        def changes():
            return space.readStream.table("bronze")

        dp.create_streaming_table("target", format="delta")
        dp.create_auto_cdc_flow(target="target", source="changes", keys=["id"], sequence_by="seq")
    start()
    assert rows(space, "id", "name") == [(1, "alicia")]
    space.sql("DROP TABLE IF EXISTS bronze")


def test_a_full_refresh_changes_the_keys_and_the_types(space, pipeline_factory, broker):  # noqa: F811
    broker.send({"id": 1, "name": "alice", "seq": 1})
    run(space, pipeline_factory, broker.url, "id INT, name STRING, seq BIGINT")
    broker.send({"id": 2, "name": "bob", "seq": 2})
    schema = "id BIGINT, name STRING, seq BIGINT"
    with pytest.raises(FAILURE, match="CANNOT_MERGE_INCOMPATIBLE_DATA_TYPE"):
        run(space, pipeline_factory, broker.url, schema)
    run(space, pipeline_factory, broker.url, schema, keys="name", full_refresh_all=True)
    assert rows(space, "id", "name") == [(1, "alice"), (2, "bob")]
    assert space.table("target").schema["id"].dataType.simpleString() == "bigint"
    broker.send({"id": 9, "name": "bob", "seq": 3})
    run(space, pipeline_factory, broker.url, schema, keys="name")
    assert rows(space, "id", "name") == [(1, "alice"), (9, "bob")]


def test_a_dropped_auxiliary_table_is_recreated(space, pipeline_factory, broker):  # noqa: F811
    broker.send({"id": 1, "name": "alice", "seq": 1})
    run(space, pipeline_factory, broker.url, BASE)
    space.sql("DROP TABLE __spark_autocdc_aux_state_target")
    broker.send({"id": 1, "name": "alicia", "seq": 2})
    run(space, pipeline_factory, broker.url, BASE)
    assert rows(space, "id", "name") == [(1, "alicia")]
    assert space.catalog.tableExists("__spark_autocdc_aux_state_target")


def test_a_full_refresh_creates_a_target_that_does_not_exist(space, pipeline_factory, broker):  # noqa: F811
    broker.send({"id": 1, "name": "alice", "seq": 1})
    run(space, pipeline_factory, broker.url, BASE, full_refresh_all=True)
    assert rows(space, "id", "name") == [(1, "alice")]


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

        dp.create_streaming_table("target", format="delta")
        dp.create_auto_cdc_flow(
            target="target", source="changes", keys=[keys], sequence_by="seq", column_list=columns
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

        dp.create_streaming_table("target", format="delta")
        dp.create_auto_cdc_flow(
            target="target", source="changes", keys=["id"], sequence_by="seq", except_column_list=["name"]
        )

        @dp.materialized_view
        def downstream():
            return space.read.table("target").select("id", "__spark_autocdc_metadata.upsertSequence")

    start()
    assert rows(space, "id", "upsertSequence", table="downstream") == [(1, 1)]
    space.sql("DROP TABLE IF EXISTS downstream")


NESTED = "key INT, seq BIGINT, a INT, b_c INT, b_d INT"


def nested_run(spark, factory, url, transform):
    _, registry, start = factory()
    with graph_element_registration_context(registry):

        @dp.temporary_view
        def raw():
            return (
                spark.readStream.format("iggy")
                .option("url", url)
                .option("stream", STREAM)
                .option("topic", TOPIC)
                .option("timeout_ms", "5000")
                .schema(NESTED)
                .load()
            )

        @dp.temporary_view
        def changes():
            return transform(spark.readStream.table("raw"))

        dp.create_streaming_table("target", format="delta")
        dp.create_auto_cdc_flow(target="target", source="changes", keys=["key"], sequence_by="seq")
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
    with pytest.raises(FAILURE, match="INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_FIND_DATA"):
        nested_run(space, pipeline_factory, broker.url, _struct(("d", "b_d")))


def test_a_new_field_in_an_array_of_structs_is_added(space, pipeline_factory, broker):  # noqa: F811
    broker.send(_event(1, 1, 1, 1, 99))
    nested_run(space, pipeline_factory, broker.url, _array(("c", "b_c")))
    broker.send(_event(1, 2, 1, 1, 2), _event(3, 1, 3, 3, 3))
    nested_run(space, pipeline_factory, broker.url, _array(("c", "b_c"), ("d", "b_d")))
    exploded = space.table("target").selectExpr("key", "inline(vals) AS (a, b)").select("key", "a", "b.c", "b.d")
    assert sorted(tuple(row) for row in exploded.collect()) == [(1, 1, 1, 2), (3, 3, 3, 3)]


def test_dropping_a_field_in_an_array_of_structs_is_refused(space, pipeline_factory, broker):  # noqa: F811
    broker.send(_event(1, 1, 1, 1, 1), _event(2, 1, 2, 2, 2))
    nested_run(space, pipeline_factory, broker.url, _array(("c", "b_c"), ("d", "b_d")))
    broker.send(_event(1, 2, 10, 10, 99), _event(3, 1, 3, 3, 99))
    with pytest.raises(FAILURE, match="INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_FIND_DATA"):
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


def test_a_flow_reads_a_delta_table_incrementally(space, pipeline_factory):
    space.sql("DROP TABLE IF EXISTS bronze")
    space.sql("CREATE TABLE bronze (id INT, name STRING, seq BIGINT) USING delta")
    space.sql("INSERT INTO bronze VALUES (1, 'alice', 1), (2, 'bob', 1)")

    def go():
        _, registry, start = pipeline_factory()
        with graph_element_registration_context(registry):

            @dp.temporary_view
            def changes():
                return space.readStream.table("bronze").where("name <> 'skip'")

            dp.create_streaming_table("target", format="delta")
            dp.create_auto_cdc_flow(target="target", source="changes", keys=["id"], sequence_by="seq")
        start()

    go()
    assert rows(space, "id", "name") == [(1, "alice"), (2, "bob")]
    space.sql("INSERT INTO bronze VALUES (1, 'alicia', 2), (2, 'skip', 2)")
    go()
    assert rows(space, "id", "name") == [(1, "alicia"), (2, "bob")]
    go()
    assert rows(space, "id", "name") == [(1, "alicia"), (2, "bob")]
    space.sql("DROP TABLE IF EXISTS bronze")
