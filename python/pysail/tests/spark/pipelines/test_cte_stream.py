"""STREAM references to batch CTEs are batch flows after CTE substitution."""

import pytest

pytest.importorskip("pyspark.pipelines")


@pytest.mark.parametrize(
    ("query", "expected"),
    [
        ("WITH x AS (SELECT 1 AS id) SELECT * FROM STREAM(x)", [1]),
        (
            "WITH x AS (SELECT 1 AS id), y AS (SELECT * FROM STREAM(x)) SELECT * FROM STREAM(y)",
            [1],
        ),
        (
            ("WITH x AS (SELECT 1 AS id) SELECT * FROM (WITH x AS (SELECT 2 AS id) SELECT * FROM STREAM(x))"),
            [2],
        ),
        (
            (
                "WITH x AS (SELECT 1 AS id) "
                "SELECT * FROM (WITH x AS (SELECT 2 AS id) SELECT * FROM STREAM(x)) "
                "UNION ALL SELECT * FROM STREAM(x)"
            ),
            [1, 2],
        ),
    ],
    ids=["batch", "sequential", "shadowing", "scope-restoration"],
)
def test_stream_of_batch_cte(pipeline_spark, pipeline, tmp_path, query, expected):
    _, registry, run = pipeline
    registry.register_sql(f"CREATE MATERIALIZED VIEW cte_output AS {query}", tmp_path / "cte_stream.sql")
    run()
    result = pipeline_spark.table("cte_output")
    assert not result.isStreaming
    assert [row.id for row in result.orderBy("id").collect()] == expected
