"""What one session sees of another session's writes when sessions share a catalog."""

import os

import pytest
from pyspark.sql import SparkSession

from pysail.testing.spark.utils.common import is_jvm_spark

SHARED_CATALOG = pytest.mark.skipif(
    not is_jvm_spark() and os.environ.get("SAIL_CATALOG__SHARED_MEMORY") != "true",
    reason="needs a server with catalog.shared_memory",
)


@pytest.fixture
def other(spark):
    url = os.environ.get("SPARK_REMOTE", "sc://localhost:15002")
    session = SparkSession.builder.remote(url).create()
    yield session
    session.stop()


@pytest.fixture
def table(spark):
    name = "shared_visibility"
    spark.sql(f"DROP TABLE IF EXISTS {name}")
    spark.sql(f"CREATE TABLE {name} (id INT) USING parquet")
    yield name
    spark.sql(f"DROP TABLE IF EXISTS {name}")


def rows(session, table):
    return sorted(row.id for row in session.table(table).collect())


@SHARED_CATALOG
def test_an_insert_in_another_session_is_visible_without_a_refresh(spark, other, table):
    assert rows(spark, table) == []
    assert rows(other, table) == []
    other.sql(f"INSERT INTO {table} VALUES (1), (2)")
    assert rows(spark, table) == [1, 2]
    spark.sql(f"INSERT INTO {table} VALUES (3)")
    assert rows(other, table) == [1, 2, 3]


@SHARED_CATALOG
def test_an_overwrite_in_another_session_is_visible_without_a_refresh(spark, other, table):
    spark.sql(f"INSERT INTO {table} VALUES (1), (2)")
    assert rows(other, table) == [1, 2]
    other.sql(f"INSERT OVERWRITE TABLE {table} VALUES (9)")
    assert rows(spark, table) == [9]
