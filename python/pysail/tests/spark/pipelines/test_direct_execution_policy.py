"""Sail policy: rejected pipeline declarations must not execute child writes.

This is separate from Spark DDLSuite diagnostic conformance: Spark 4.2.1's
CREATE FLOW rejection does not assert preservation of the INSERT target.
"""

import pytest
from pyspark.errors.exceptions.connect import UnsupportedOperationException


def test_rejected_flow_does_not_execute_insert(spark):
    try:
        spark.sql("CREATE TABLE table1 AS SELECT 1").collect()
        spark.sql("CREATE TABLE table2 AS SELECT 2").collect()
        with pytest.raises(UnsupportedOperationException):
            spark.sql("CREATE FLOW f AS INSERT INTO table2 SELECT * FROM table1").collect()
        assert [tuple(row) for row in spark.table("table1").collect()] == [(1,)]
        assert [tuple(row) for row in spark.table("table2").collect()] == [(2,)]
    finally:
        spark.sql("DROP TABLE IF EXISTS table1").collect()
        spark.sql("DROP TABLE IF EXISTS table2").collect()
