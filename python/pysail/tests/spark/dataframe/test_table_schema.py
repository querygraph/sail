import uuid

import pytest

from pysail.testing.spark.utils.sql import escape_sql_string_literal


@pytest.mark.parametrize("comment", ["identifier", 'a "quoted" comment'])
def test_dataframe_schema_preserves_catalog_column_comments(spark, tmp_path, comment):
    table = f"schema_{uuid.uuid4().hex}"
    location = escape_sql_string_literal((tmp_path / "table").as_uri())
    text = escape_sql_string_literal(comment)
    spark.sql(f"CREATE TABLE {table} (id BIGINT COMMENT '{text}') USING parquet LOCATION '{location}'")
    try:
        assert spark.table(table).schema["id"].metadata == {"comment": comment}
    finally:
        spark.sql(f"DROP TABLE {table}")
