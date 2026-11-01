"""ORC writing, measured the same way on Spark 4.2.1 and on Sail."""

from pathlib import Path

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


@pytest.mark.parametrize(
    "expression",
    [
        "TIMESTAMP'2024-02-29 12:30:01' AS t",
        "TIMESTAMP_NTZ'2024-02-29 12:30:01' AS t",
        "CAST(12.34 AS DECIMAL(10, 2)) AS n",
        "array(1, 2) AS a",
        "map('k', 1) AS m",
        "struct(1 AS x, 'z' AS y) AS s",
    ],
)
def test_types_the_native_writer_lacks(spark, tmp_path, expression):
    """These round-trip on Spark. Sail must refuse them, never write them wrongly."""
    frame = spark.sql(f"SELECT {expression}")
    path = str(tmp_path / "t")
    if is_jvm_spark():
        frame.write.orc(path)
        assert spark.read.orc(path).collect() == frame.collect()
    else:
        with pytest.raises(Exception, match="ORC writing does not support type"):
            frame.write.orc(path)


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
