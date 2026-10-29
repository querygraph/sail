import uuid

import pytest
from pyspark.errors.exceptions.connect import AnalysisException
from pyspark.sql import Row
from pyspark.sql import functions as F

from pysail.testing.spark.utils.sql import escape_sql_string_literal


@pytest.mark.parametrize(
    "schema, query",
    [
        ("id BIGINT, optional STRING", lambda spark: spark.range(2)),
        (
            "value STRUCT<amount: BIGINT, optional: STRING>",
            lambda spark: spark.range(2).select(F.struct(F.col("id").alias("amount")).alias("value")),
        ),
        (
            "value ARRAY<STRUCT<amount: BIGINT, optional: STRING>>",
            lambda spark: spark.range(2).select(F.array(F.struct(F.col("id").alias("amount"))).alias("value")),
        ),
        (
            "value MAP<STRING, STRUCT<amount: BIGINT, optional: STRING>>",
            lambda spark: spark.range(2).select(
                F.create_map(F.lit("key"), F.struct(F.col("id").alias("amount"))).alias("value")
            ),
        ),
        (
            "value STRUCT<amount: BIGINT>",
            lambda spark: spark.range(2).select(
                F.struct(F.col("id").alias("amount"), F.lit("extra").alias("extra")).alias("value")
            ),
        ),
    ],
)
def test_append_rejects_unmatched_columns(spark, tmp_path, schema, query):
    table = f"write_schema_{uuid.uuid4().hex}"
    location = escape_sql_string_literal((tmp_path / "table").as_uri())
    spark.sql(f"CREATE TABLE {table} ({schema}) USING parquet LOCATION '{location}'")
    try:
        with pytest.raises(AnalysisException):
            query(spark).write.mode("append").saveAsTable(table)
        assert spark.table(table).count() == 0
    finally:
        spark.sql(f"DROP TABLE {table}")


def test_append_matches_nested_fields_by_name_and_accepts_explicit_null(spark, tmp_path):
    table = f"write_schema_{uuid.uuid4().hex}"
    location = escape_sql_string_literal((tmp_path / "table").as_uri())
    spark.sql(
        f"CREATE TABLE {table} (value STRUCT<amount: BIGINT, optional: STRING>) USING parquet LOCATION '{location}'"
    )
    try:
        query = spark.range(1).select(
            F.struct(F.lit(None).cast("string").alias("optional"), F.col("id").alias("amount")).alias("value")
        )
        query.write.mode("append").saveAsTable(table)
        assert spark.table(table).collect() == [Row(value=Row(amount=0, optional=None))]
    finally:
        spark.sql(f"DROP TABLE {table}")
