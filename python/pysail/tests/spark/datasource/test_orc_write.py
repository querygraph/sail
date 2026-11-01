"""ORC writing, measured the same way on Spark 4.2.1 and on Sail."""

from datetime import datetime
from decimal import Decimal
from pathlib import Path

import pyarrow as pa
import pyarrow.orc
import pytest
from pyspark.errors import AnalysisException
from pyspark.pipelines.spark_connect_graph_element_registry import SparkConnectGraphElementRegistry
from pyspark.pipelines.spark_connect_pipeline import create_dataflow_graph, start_run
from pyspark.sql.types import (
    BinaryType,
    BooleanType,
    ByteType,
    DoubleType,
    FloatType,
    IntegerType,
    LongType,
    ShortType,
    StringType,
    StructField,
    StructType,
)

from pysail.testing.spark.utils.common import is_jvm_spark


def rows(frame, *order):
    return [tuple(row) for row in frame.orderBy(*order).collect()]


def test_range_round_trip(spark, tmp_path):
    path = str(tmp_path / "t")
    spark.range(3).write.format("orc").save(path)
    assert rows(spark.read.format("orc").load(path), "id") == [(0,), (1,), (2,)]


def test_primitive_columns_keep_their_types_and_nulls(spark, tmp_path):
    schema = StructType(
        [
            StructField("b", BooleanType()),
            StructField("t", ByteType()),
            StructField("s", ShortType()),
            StructField("i", IntegerType()),
            StructField("l", LongType()),
            StructField("f", FloatType()),
            StructField("d", DoubleType()),
            StructField("x", StringType()),
            StructField("y", BinaryType()),
        ]
    )
    data = [
        (True, 1, 2, 3, 4, 1.5, 2.5, "a", b"z"),
        (None, None, None, None, None, None, None, None, None),
    ]
    path = str(tmp_path / "t")
    spark.createDataFrame(data, schema).write.orc(path)
    read = spark.read.orc(path)
    assert read.schema.simpleString() == schema.simpleString()
    assert rows(read, "i")[1:] == [(True, 1, 2, 3, 4, 1.5, 2.5, "a", bytearray(b"z"))]
    assert read.filter("i IS NULL").count() == 1


def test_append_adds_and_overwrite_replaces(spark, tmp_path):
    path = str(tmp_path / "t")
    spark.range(2).write.orc(path)
    spark.range(2, 3).write.mode("append").orc(path)
    assert rows(spark.read.orc(path), "id") == [(0,), (1,), (2,)]
    spark.range(9, 10).write.mode("overwrite").orc(path)
    assert rows(spark.read.orc(path), "id") == [(9,)]


def test_an_existing_path_is_refused_by_default(spark, tmp_path):
    path = str(tmp_path / "t")
    spark.range(1).write.orc(path)
    with pytest.raises(AnalysisException):
        spark.range(1).write.orc(path)


def test_partition_values_are_in_the_path_not_the_file(spark, tmp_path):
    path = tmp_path / "t"
    spark.sql("SELECT id, id % 2 AS p FROM range(4)").write.partitionBy("p").orc(str(path))
    assert sorted(child.name for child in path.iterdir() if child.is_dir()) == ["p=0", "p=1"]
    inner = spark.read.orc(str(path / "p=0"))
    assert inner.columns == ["id"]
    # Sail infers every partition column as a string, for all formats.
    read = spark.read.orc(str(path)).selectExpr("id", "CAST(p AS INT) AS p")
    assert rows(read, "id") == [(0, 0), (1, 1), (2, 0), (3, 1)]


@pytest.mark.parametrize("codec", ["none", "uncompressed", "zlib", "snappy", "lz4", "zstd", "ZSTD"])
def test_codecs_round_trip(spark, tmp_path, codec):
    path = str(tmp_path / "t")
    spark.range(3).write.option("compression", codec).orc(path)
    assert spark.read.orc(path).count() == 3


def test_compression_outranks_orc_compress_which_may_then_be_invalid(spark, tmp_path):
    path = str(tmp_path / "t")
    spark.range(1).write.option("compression", "zlib").option("orc.compress", "gzip").orc(path)
    assert spark.read.orc(path).count() == 1


def test_an_invalid_codec_is_refused(spark, tmp_path):
    with pytest.raises(Exception, match="(?i)gzip"):
        spark.range(1).write.option("compression", "gzip").orc(str(tmp_path / "t"))


def test_a_table_can_be_created_and_read(spark):
    spark.sql("DROP TABLE IF EXISTS orc_written")
    try:
        spark.sql("CREATE TABLE orc_written USING orc AS SELECT id FROM range(3)")
        assert rows(spark.table("orc_written"), "id") == [(0,), (1,), (2,)]
        spark.sql("INSERT INTO orc_written VALUES (3)")
        assert spark.table("orc_written").count() == 4
    finally:
        spark.sql("DROP TABLE IF EXISTS orc_written")


def test_an_empty_write_keeps_the_schema(spark, tmp_path):
    path = str(tmp_path / "t")
    spark.range(0).write.orc(path)
    read = spark.read.orc(path)
    assert read.columns == ["id"]
    assert read.count() == 0


def test_dates_round_trip(spark, tmp_path):
    frame = spark.sql("SELECT DATE'2024-02-29' AS d, CAST(NULL AS DATE) AS n, DATE'1600-01-01' AS o")
    path = str(tmp_path / "t")
    frame.write.orc(path)
    assert spark.read.orc(path).collect() == frame.collect()
    assert spark.read.orc(path).schema.simpleString() == frame.schema.simpleString()


def independent_read(path):
    """The file's rows through pyarrow's reader, which shares no code with either engine."""
    tables = [pa.orc.read_table(str(file)) for file in sorted(Path(path).glob("*.orc"))]
    return pa.concat_tables(tables)


def strings(frame):
    return [row[0] for row in frame.selectExpr(f"CAST({frame.columns[0]} AS STRING)").collect()]


@pytest.fixture
def session_zone(spark):
    """Set the session time zone for a test, and restore it after."""
    zones = []

    def set_zone(zone):
        zones.append(spark.conf.get("spark.sql.session.timeZone"))
        spark.conf.set("spark.sql.session.timeZone", zone)

    yield set_zone
    for zone in reversed(zones):
        spark.conf.set("spark.sql.session.timeZone", zone)


# Wall clocks in the session zone. Those within a second before the epoch are
# apart: see `test_orc_cannot_represent_the_second_before_the_epoch`. Those before
# the Gregorian cutover are in `test_timestamps_before_the_gregorian_cutover`.
TIMESTAMPS = [
    "1970-01-01 00:00:00",
    "1969-12-31 23:59:58",
    "1969-12-31 23:59:58.500000",
    "1960-06-15 12:34:56.789012",
    "1900-01-01 00:00:00",
    "2014-12-31 23:59:59.500000",
    "2015-01-01 00:00:00",
    "2024-02-29 12:30:01.123456",
    "9999-12-31 23:59:59.999999",
]
PAST_EPOCH_IN_EVERY_ZONE = ["2024-02-29 12:30:01.123456", "1960-06-15 12:34:56.789012"]


@pytest.mark.parametrize("zone", ["UTC", "America/Los_Angeles", "Asia/Tokyo"])
def test_timestamps_read_back_as_written(spark, tmp_path, session_zone, zone):
    session_zone(zone)
    values = ", ".join(f"(TIMESTAMP'{value}')" for value in PAST_EPOCH_IN_EVERY_ZONE)
    frame = spark.sql(f"SELECT * FROM VALUES {values}, (NULL) AS t(t) ORDER BY t")
    path = str(tmp_path / "t")
    frame.write.orc(path)
    read = spark.read.orc(path)
    assert read.schema.simpleString() == "struct<t:timestamp>"
    assert strings(read.orderBy("t")) == strings(frame)


def test_timestamps_at_their_limits_in_utc(spark, tmp_path, session_zone):
    session_zone("UTC")
    values = ", ".join(f"(TIMESTAMP'{value}')" for value in TIMESTAMPS)
    frame = spark.sql(f"SELECT * FROM VALUES {values} AS t(t) ORDER BY t")
    path = str(tmp_path / "t")
    frame.write.orc(path)
    read = spark.read.orc(path).orderBy("t")
    assert read.collect() == frame.collect()
    assert strings(read) == strings(frame)


def test_a_timestamp_is_an_instant_across_session_zones(spark, tmp_path, session_zone):
    session_zone("Asia/Tokyo")
    path = str(tmp_path / "t")
    spark.sql("SELECT TIMESTAMP'2024-02-29 12:30:01' AS t").write.orc(path)
    session_zone("America/Los_Angeles")
    assert strings(spark.read.orc(path)) == ["2024-02-28 19:30:01"]


def test_timestamp_ntz_keeps_its_wall_clock_across_session_zones(spark, tmp_path, session_zone):
    session_zone("Asia/Tokyo")
    path = str(tmp_path / "t")
    spark.sql("SELECT TIMESTAMP_NTZ'2024-02-29 12:30:01.5' AS t").write.orc(path)
    session_zone("America/Los_Angeles")
    read = spark.read.orc(path)
    assert read.schema.simpleString() == "struct<t:timestamp_ntz>"
    assert strings(read) == ["2024-02-29 12:30:01.5"]


@pytest.mark.parametrize(
    "value",
    [
        "1969-12-31 23:59:59.500000",
        "1969-12-31 23:59:59.999999",
        "1969-12-31 23:59:58.500000",
        "1000-01-01 00:00:00.500000",
        "1582-10-10 00:00:00",
        "0001-01-01 00:00:00",
        "9999-12-31 23:59:59.999999",
    ],
)
def test_timestamp_ntz_at_its_limits(spark, tmp_path, session_zone, value):
    """Stored as a plain long: no calendar, zone or epoch has any effect on it."""
    session_zone("UTC")
    frame = spark.sql(f"SELECT TIMESTAMP_NTZ'{value}' AS t")
    path = str(tmp_path / "t")
    frame.write.orc(path)
    assert spark.read.orc(path).collect() == frame.collect()
    assert strings(spark.read.orc(path)) == strings(frame)


def test_orc_cannot_represent_the_second_before_the_epoch(spark, tmp_path, session_zone):
    """ORC-763: Spark's reader adds a second, so that Spark itself returns a value it
    never wrote. Sail refuses rather than publish a file that reads back wrongly."""
    session_zone("UTC")
    frame = spark.sql("SELECT TIMESTAMP'1969-12-31 23:59:59.5' AS t")
    path = tmp_path / "t"
    if is_jvm_spark():
        frame.write.orc(str(path))
        assert strings(spark.read.orc(str(path))) == ["1970-01-01 00:00:00.5"]
    else:
        with pytest.raises(Exception, match="Cannot encode timestamp"):
            frame.write.orc(str(path))
        assert not list(path.glob("*.orc"))


@pytest.mark.parametrize(
    "value",
    ["1582-10-04 00:00:00", "1500-02-28 12:00:00", "1000-01-01 00:00:00.5"],
)
def test_timestamps_before_the_gregorian_cutover(spark, tmp_path, session_zone, value):
    session_zone("America/Los_Angeles")
    frame = spark.sql(f"SELECT TIMESTAMP'{value}' AS t")
    path = str(tmp_path / "t")
    frame.write.orc(path)
    assert strings(spark.read.orc(path)) == strings(frame)


@pytest.mark.parametrize(
    ("data_type", "value"),
    [
        ("DECIMAL(1,0)", "9"),
        ("DECIMAL(1,1)", "0.9"),
        ("DECIMAL(10,2)", "-12.30"),
        ("DECIMAL(10,2)", "100.00"),
        ("DECIMAL(10,2)", "0.00"),
        ("DECIMAL(18,0)", "999999999999999999"),
        ("DECIMAL(19,0)", "9999999999999999999"),
        ("DECIMAL(38,0)", "99999999999999999999999999999999999999"),
        ("DECIMAL(38,0)", "-99999999999999999999999999999999999999"),
        ("DECIMAL(38,38)", "0.99999999999999999999999999999999999999"),
        ("DECIMAL(38,38)", "-0.00000000000000000000000000000000000001"),
        ("DECIMAL(38,19)", "-1234567890123456789.1234567890123456789"),
    ],
)
def test_decimals_at_their_limits(spark, tmp_path, data_type, value):
    frame = spark.sql(f"SELECT CAST('{value}' AS {data_type}) AS d")
    path = str(tmp_path / "t")
    frame.write.orc(path)
    read = spark.read.orc(path)
    assert read.schema.simpleString() == frame.schema.simpleString()
    assert read.collect()[0].d == Decimal(value)
    assert independent_read(path)["d"].to_pylist() == [Decimal(value)]


def test_a_null_decimal_differs_from_zero(spark, tmp_path):
    frame = spark.sql("SELECT * FROM VALUES (1, 0.00), (2, NULL), (3, 1.50) AS t(i, d)")
    path = str(tmp_path / "t")
    frame.write.orc(path)
    assert rows(spark.read.orc(path), "i") == [
        (1, Decimal("0.00")),
        (2, None),
        (3, Decimal("1.50")),
    ]


def test_arrays_keep_null_empty_and_null_elements_apart(spark, tmp_path):
    frame = spark.sql(
        """
        SELECT * FROM VALUES
            (1, array(1, NULL, 3)),
            (2, array(CAST(NULL AS INT))),
            (3, CAST(array() AS ARRAY<INT>)),
            (4, NULL)
        AS t(i, a)
        """
    )
    path = str(tmp_path / "t")
    frame.write.orc(path)
    read = spark.read.orc(path)
    assert read.schema.simpleString() == "struct<i:int,a:array<int>>"
    assert rows(read, "i") == [(1, [1, None, 3]), (2, [None]), (3, []), (4, None)]
    assert independent_read(path).sort_by("i")["a"].to_pylist() == [[1, None, 3], [None], [], None]


def test_maps_keep_null_empty_and_null_values_apart(spark, tmp_path):
    frame = spark.sql(
        """
        SELECT * FROM VALUES
            (1, map('a', 1, 'b', CAST(NULL AS INT))),
            (2, CAST(map() AS MAP<STRING, INT>)),
            (3, NULL)
        AS t(i, m)
        """
    )
    path = str(tmp_path / "t")
    frame.write.orc(path)
    read = spark.read.orc(path)
    assert read.schema.simpleString() == "struct<i:int,m:map<string,int>>"
    assert rows(read, "i") == [(1, {"a": 1, "b": None}), (2, {}), (3, None)]
    assert independent_read(path).sort_by("i")["m"].to_pylist() == [
        [("a", 1), ("b", None)],
        [],
        None,
    ]


def test_a_null_struct_differs_from_a_struct_of_nulls(spark, tmp_path):
    frame = spark.sql(
        """
        SELECT * FROM VALUES
            (1, named_struct('x', 1, 'y', 'a')),
            (2, named_struct('x', CAST(NULL AS INT), 'y', CAST(NULL AS STRING))),
            (3, NULL),
            (4, named_struct('x', 4, 'y', CAST(NULL AS STRING)))
        AS t(i, s)
        """
    )
    path = str(tmp_path / "t")
    frame.write.orc(path)
    read = spark.read.orc(path)
    assert read.schema.simpleString() == "struct<i:int,s:struct<x:int,y:string>>"
    assert [row.s and tuple(row.s) for row in read.orderBy("i").collect()] == [
        (1, "a"),
        (None, None),
        None,
        (4, None),
    ]
    assert independent_read(path).sort_by("i")["s"].to_pylist() == [
        {"x": 1, "y": "a"},
        {"x": None, "y": None},
        None,
        {"x": 4, "y": None},
    ]


def test_every_type_can_nest_in_every_container(spark, tmp_path, session_zone):
    session_zone("UTC")
    frame = spark.sql(
        """
        SELECT 1 AS i, array(map('k', array(named_struct(
            't', TIMESTAMP'1969-12-31 23:59:58',
            'n', TIMESTAMP_NTZ'1969-12-31 23:59:59.5',
            'd', CAST(1.50 AS DECIMAL(5, 2)),
            'a', array(array(1), CAST(array() AS ARRAY<INT>), NULL)
        )))) AS a
        """
    )
    path = str(tmp_path / "t")
    frame.write.orc(path)
    read = spark.read.orc(path)
    assert read.schema.simpleString() == frame.schema.simpleString()
    inner = read.collect()[0].a[0]["k"][0]
    assert inner.t == frame.collect()[0].a[0]["k"][0].t
    assert inner.n == datetime(1969, 12, 31, 23, 59, 59, 500000)  # noqa: DTZ001
    assert inner.d == Decimal("1.50")
    assert inner.a == [[1], [], None]


@pytest.mark.parametrize(
    "expression",
    [
        "map(named_struct('a', 1), 'v')",
        "map(1, map(2, 3))",
        "named_struct('a', named_struct('b', named_struct('c', 1)))",
        "array(array(array(1)))",
        "map(CAST(1.5 AS DECIMAL(3, 1)), TIMESTAMP_NTZ'2000-01-01 00:00:00')",
    ],
)
def test_container_shapes(spark, tmp_path, expression):
    frame = spark.sql(f"SELECT {expression} AS c")
    path = str(tmp_path / "t")
    frame.write.orc(path)
    read = spark.read.orc(path)
    assert read.schema.simpleString() == frame.schema.simpleString()
    assert read.collect() == frame.collect()


def test_nested_columns_stay_aligned_over_many_batches(spark, tmp_path):
    """Enough rows for the writer's batches and slices to split inside lists."""
    frame = spark.sql(
        """
        SELECT
            id,
            IF(id % 7 = 0, NULL, sequence(0L, id % 5)) AS a,
            IF(id % 11 = 0, NULL, named_struct('x', id, 'y', CAST(id AS STRING))) AS s,
            IF(id % 13 = 0, NULL, map(id, CAST(id AS DECIMAL(20, 2)))) AS m
        FROM range(10000)
        """
    )
    path = str(tmp_path / "t")
    frame.repartition(1).write.orc(path)
    read = spark.read.orc(path)
    assert read.count() == 10000
    expected = frame.orderBy("id").collect()
    assert read.orderBy("id").collect() == expected


def test_a_partitioned_write_of_nested_columns(spark, tmp_path):
    path = tmp_path / "t"
    frame = spark.sql("SELECT id, id % 2 AS p, array(id) AS a, CAST(id AS DECIMAL(5, 1)) AS d FROM range(4)")
    frame.write.partitionBy("p").orc(str(path))
    inner = spark.read.orc(str(path / "p=1"))
    assert inner.columns == ["id", "a", "d"]
    assert rows(inner, "id") == [(1, [1], Decimal("1.0")), (3, [3], Decimal("3.0"))]


def test_an_empty_write_keeps_a_nested_schema(spark, tmp_path):
    frame = spark.sql(
        """
        SELECT
            TIMESTAMP'2024-01-01 00:00:00' AS t,
            TIMESTAMP_NTZ'2024-01-01 00:00:00' AS n,
            CAST(1 AS DECIMAL(10, 2)) AS d,
            array(1) AS a,
            map('k', 1) AS m,
            named_struct('x', 1) AS s
        WHERE false
        """
    )
    path = str(tmp_path / "t")
    frame.write.orc(path)
    read = spark.read.orc(path)
    assert read.schema.simpleString() == frame.schema.simpleString()
    assert read.count() == 0


def test_filters_read_new_types_through_files_without_statistics(spark, tmp_path):
    path = str(tmp_path / "t")
    spark.sql(
        """
        SELECT id, CAST(id AS DECIMAL(10, 2)) AS d, TIMESTAMP_NTZ'2024-01-01 00:00:00' AS n,
            CAST(id * 86400 AS TIMESTAMP) AS t
        FROM range(5)
        """
    ).write.orc(path)
    read = spark.read.orc(path)
    assert read.filter("d > 2").count() == 2
    assert read.filter("n = TIMESTAMP_NTZ'2024-01-01 00:00:00'").count() == 5
    assert read.filter("t > CAST(86400 AS TIMESTAMP)").count() == 3


def test_a_table_of_every_new_type(spark):
    spark.sql("DROP TABLE IF EXISTS orc_new_types")
    try:
        spark.sql(
            """
            CREATE TABLE orc_new_types USING orc AS SELECT
                TIMESTAMP'2024-02-29 12:30:01' AS t,
                TIMESTAMP_NTZ'2024-02-29 12:30:01' AS n,
                CAST(12.34 AS DECIMAL(10, 2)) AS d,
                array(1, 2) AS a,
                map('k', 1) AS m,
                struct(1 AS x, 'z' AS y) AS s
            """
        )
        spark.sql(
            """
            INSERT INTO orc_new_types SELECT
                NULL AS t, NULL AS n, NULL AS d, NULL AS a, NULL AS m, NULL AS s
            """
        )
        assert spark.table("orc_new_types").count() == 2
        assert spark.table("orc_new_types").filter("d IS NULL").count() == 1
        assert spark.table("orc_new_types").filter("s.y = 'z'").count() == 1
    finally:
        spark.sql("DROP TABLE IF EXISTS orc_new_types")


def test_materialized_view_using_orc_of_every_new_type(spark, tmp_path):
    """The goal of this writer: a batch pipeline's output in ORC."""
    schema = "orc_pipeline_types"
    spark.sql(f"DROP SCHEMA IF EXISTS {schema} CASCADE")
    spark.sql(f"CREATE SCHEMA {schema}")
    query = """
        SELECT
            TIMESTAMP'2024-02-29 12:30:01' AS t,
            TIMESTAMP_NTZ'2024-02-29 12:30:01' AS n,
            CAST(12.34 AS DECIMAL(10, 2)) AS d,
            array(1, NULL) AS a,
            map('k', 1) AS m,
            struct(1 AS x, 'z' AS y) AS s
    """
    try:
        graph = create_dataflow_graph(spark, spark.catalog.currentCatalog(), schema, None)
        registry = SparkConnectGraphElementRegistry(spark, graph)
        registry.register_sql(f"CREATE MATERIALIZED VIEW v USING orc AS {query}", Path("v.sql"))
        list(start_run(spark, graph, None, False, None, False, (tmp_path / "storage").as_uri()))
        expected = spark.sql(query)
        table = spark.table(f"{schema}.v")
        assert table.schema.simpleString() == expected.schema.simpleString()
        assert table.collect() == expected.collect()
    finally:
        spark.sql(f"DROP SCHEMA IF EXISTS {schema} CASCADE")


@pytest.mark.parametrize(
    "expression",
    [
        "NULL",
        "array()",
        "map()",
        "array(named_struct('a', NULL))",
        "struct()",
        "make_interval(1, 2, 3, 4, 5, 6, 7)",
        "parse_json('1')",
        "array(parse_json('1'))",
    ],
)
def test_types_spark_refuses_too(spark, tmp_path, expression):
    path = tmp_path / "t"
    with pytest.raises(Exception, match="(?i)orc|unsupported|not supported"):
        spark.sql(f"SELECT {expression} AS c").write.orc(str(path))
    assert not list(path.glob("*.orc"))


@pytest.mark.parametrize(
    "expression",
    [
        "INTERVAL '1' DAY",
        "INTERVAL '1-2' YEAR TO MONTH",
        "array(INTERVAL '1' DAY)",
    ],
)
def test_interval_columns(spark, tmp_path, expression):
    """Spark stores these with a Catalyst annotation. Sail refuses them, never writes them
    as plain integers, since the interval would not read back."""
    frame = spark.sql(f"SELECT {expression} AS i")
    path = tmp_path / "t"
    if is_jvm_spark():
        frame.write.orc(str(path))
        assert spark.read.orc(str(path)).schema.simpleString() == frame.schema.simpleString()
    else:
        with pytest.raises(Exception, match="ORC writing does not support"):
            frame.write.orc(str(path))
        assert not list(path.glob("*.orc"))


def test_a_written_directory_feeds_a_table_and_a_materialized_view(spark, tmp_path):
    """The batch half of SqlPipelineSuite's file-format grid, for ORC."""
    source = str(tmp_path / "source")
    spark.sql("SELECT * FROM RANGE(3)").write.format("orc").mode("overwrite").save(source)
    schema = "orc_pipeline_write"
    spark.sql(f"DROP SCHEMA IF EXISTS {schema} CASCADE")
    spark.sql(f"CREATE SCHEMA {schema}")
    try:
        spark.sql(f"CREATE TABLE {schema}.t AS SELECT * FROM orc.`{source}`")
        assert rows(spark.table(f"{schema}.t"), "id") == [(0,), (1,), (2,)]
        graph = create_dataflow_graph(spark, spark.catalog.currentCatalog(), schema, None)
        registry = SparkConnectGraphElementRegistry(spark, graph)
        registry.register_sql(
            f"CREATE MATERIALIZED VIEW a AS SELECT * FROM orc.`{source}`", Path("a.sql")
        )
        list(start_run(spark, graph, None, False, None, False, (tmp_path / "storage").as_uri()))
        assert rows(spark.table(f"{schema}.a"), "id") == [(0,), (1,), (2,)]
    finally:
        spark.sql(f"DROP SCHEMA IF EXISTS {schema} CASCADE")
