"""ANSI cast failures retain their native Spark error identity and parameters."""

import pytest
from pyspark.errors.exceptions.connect import NumberFormatException, SparkRuntimeException


@pytest.fixture
def ansi_spark(spark):
    previous = spark.conf.get("spark.sql.ansi.enabled")
    spark.conf.set("spark.sql.ansi.enabled", "true")
    try:
        yield spark
    finally:
        spark.conf.set("spark.sql.ansi.enabled", previous)


def assert_invalid_input(error, target, expression="'bad'"):
    assert error.getCondition() == "CAST_INVALID_INPUT"
    assert error.getSqlState() == "22018"
    assert error.getMessageParameters() == {
        "expression": expression,
        "sourceType": '"STRING"',
        "targetType": f'"{target}"',
        "ansiConfig": '"spark.sql.ansi.enabled"',
    }


@pytest.mark.parametrize(
    ("target", "canonical"),
    [
        ("TINYINT", "TINYINT"),
        ("SMALLINT", "SMALLINT"),
        ("INT", "INT"),
        ("INTEGER", "INT"),
        ("BIGINT", "BIGINT"),
        ("FLOAT", "FLOAT"),
        ("DOUBLE", "DOUBLE"),
        ("BOOLEAN", "BOOLEAN"),
    ],
)
@pytest.mark.parametrize("context", ["constant", "rows"])
def test_invalid_ansi_cast_has_native_metadata(ansi_spark, target, canonical, context):
    exception = SparkRuntimeException if target == "BOOLEAN" else NumberFormatException
    valid = "true" if target == "BOOLEAN" else "1"
    sql = (
        f"SELECT CAST('bad' AS {target})"
        if context == "constant"
        else f"SELECT CAST(value AS {target}) FROM VALUES (CAST(NULL AS STRING)), ('{valid}'), ('bad') AS t(value)"
    )
    with pytest.raises(exception) as error:
        ansi_spark.sql(sql).collect()
    assert_invalid_input(error.value, canonical)


@pytest.mark.parametrize("outer", ["CAST", "TRY_CAST"])
def test_outer_cast_keeps_the_inner_ansi_failure(ansi_spark, outer):
    with pytest.raises(NumberFormatException) as error:
        ansi_spark.sql(f"SELECT {outer}(CAST('bad' AS INT) AS STRING)").collect()
    assert_invalid_input(error.value, "INT")


@pytest.mark.parametrize(
    ("sql", "expression"),
    [
        (r"SELECT CAST('a\'b' AS INT)", "'a\\'b'"),
        (r"SELECT CAST('a\nb' AS INT)", "'a\nb'"),
        (r"SELECT CAST('a\\b' AS INT)", r"'a\\b'"),
    ],
)
def test_ansi_cast_reports_the_actual_escaped_value(ansi_spark, sql, expression):
    with pytest.raises(NumberFormatException) as error:
        ansi_spark.sql(sql).collect()
    assert_invalid_input(error.value, "INT", expression)


@pytest.mark.parametrize(
    ("ansi", "expression", "expected"),
    [
        ("true", "TRY_CAST('bad' AS INT)", None),
        ("false", "CAST('bad' AS INT)", None),
        ("true", "CAST(CAST(NULL AS STRING) AS INT)", None),
        ("true", "CAST('12' AS INT)", 12),
    ],
)
def test_ansi_cast_controls_keep_null_and_valid_results(ansi_spark, ansi, expression, expected):
    ansi_spark.conf.set("spark.sql.ansi.enabled", ansi)
    frame = ansi_spark.sql(f"SELECT {expression} AS value")
    assert frame.schema.simpleString() == "struct<value:int>"
    assert [row.value for row in frame.collect()] == [expected]
