"""Predicate replacement uses the existing writer's column matching policy."""

from uuid import uuid4

import pytest
from pyspark.sql import Row


@pytest.mark.parametrize("by_name", [False, True])
def test_insert_replace_where_aligns_columns(spark, tmp_path, by_name):
    table = "replace_columns_" + uuid4().hex
    location = str(tmp_path / "table").replace("'", "''")
    try:
        spark.sql(f"CREATE TABLE {table} (id INT, value STRING) USING delta LOCATION '{location}'").collect()
        spark.sql(f"INSERT INTO {table} VALUES (1, 'keep'), (2, 'old')").collect()
        clause = "BY NAME " if by_name else ""
        query = "SELECT 'new' AS value, 2 AS id" if by_name else "SELECT 2 AS id, 'new' AS value"
        spark.sql(f"INSERT INTO {table} {clause}REPLACE WHERE id=2 {query}").collect()
        assert spark.sql(f"SELECT * FROM {table} ORDER BY id").collect() == [
            Row(id=1, value="keep"),
            Row(id=2, value="new"),
        ]
    finally:
        spark.sql(f"DROP TABLE IF EXISTS {table}").collect()
