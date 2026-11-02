"""Clears the default database between tests of the Hive metastore profile.

These tests start from a fresh catalog per session on Sail's default memory catalog. A metastore
outlives sessions, so what one test leaves in `default` would otherwise reach the next.
"""

import os

import pytest
from pyspark.sql import SparkSession


@pytest.fixture(autouse=True)
def clear_default_database():
    yield
    spark = SparkSession.builder.remote(os.environ["SPARK_REMOTE"]).getOrCreate()
    spark.sql("USE default")
    for row in spark.sql("SHOW TABLES IN default").collect():
        if row.tableName and not row.isTemporary:
            spark.sql(f"DROP TABLE IF EXISTS default.`{row.tableName}`")
            spark.sql(f"DROP VIEW IF EXISTS default.`{row.tableName}`")
