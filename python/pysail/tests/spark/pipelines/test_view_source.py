"""Original view SQL survives ordinary and pipeline catalog publication."""

from uuid import uuid4

import pytest

pytest.importorskip("pyspark.pipelines")


@pytest.mark.parametrize("context", ["ordinary", "pipeline"])
@pytest.mark.parametrize(
    ("query", "expected_rows"),
    [
        ("SELECT * FROM range(1, 4)", [(1,), (2,), (3,)]),
        ("SELECT  /* inside ; */ 'é;雪' AS `值`", [("é;雪",)]),
    ],
)
def test_catalog_retains_original_view_query(pipeline_spark, pipeline_factory, tmp_path, context, query, expected_rows):
    name = "source_view_" + uuid4().hex
    sql = f"CREATE VIEW `{name}` COMMENT 'source comment' AS /* before */ {query} /* after */;"
    try:
        if context == "ordinary":
            pipeline_spark.sql(sql)
        else:
            _, registry, run = pipeline_factory()
            registry.register_sql(sql, tmp_path / "source.sql")
            run()
        details = {row.col_name: row.data_type for row in pipeline_spark.sql(f"DESCRIBE EXTENDED `{name}`").collect()}
        assert details["View Text"] == query
        assert details["Comment"] == "source comment"
        assert sorted(tuple(row) for row in pipeline_spark.table(name).collect()) == expected_rows
    finally:
        pipeline_spark.sql(f"DROP VIEW IF EXISTS `{name}`")
