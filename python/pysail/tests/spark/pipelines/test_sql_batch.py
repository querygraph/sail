"""Batch query assertion ports from Spark's SqlPipelineSuite."""

import pytest

pytest.importorskip("pyspark.pipelines")


def test_pipeline_dataset_in_scalar_subquery(pipeline_spark, pipeline, tmp_path):
    _, registry, run = pipeline
    registry.register_sql(
        """
        CREATE MATERIALIZED VIEW subquery_a AS SELECT * FROM RANGE(5);
        CREATE MATERIALIZED VIEW subquery_b AS SELECT * FROM RANGE(5)
        WHERE id = (SELECT max(id) FROM subquery_a);
        """,
        tmp_path / "subquery.sql",
    )
    run()
    assert [tuple(row) for row in pipeline_spark.table("subquery_b").collect()] == [(4,)]


def test_pipeline_dataset_in_nested_cte(pipeline_spark, pipeline, tmp_path):
    _, registry, run = pipeline
    registry.register_sql(
        """
        CREATE MATERIALIZED VIEW cte_a AS SELECT 1;
        CREATE MATERIALIZED VIEW cte_d AS
        WITH c AS (
          WITH b AS (
            SELECT * FROM cte_a
          )
          SELECT * FROM b
        )
        SELECT * FROM c;
        """,
        tmp_path / "cte.sql",
    )
    run()
    assert [tuple(row) for row in pipeline_spark.table("cte_d").collect()] == [(1,)]


def test_pipeline_groupby_and_rollup(pipeline_spark, pipeline, tmp_path):
    _, registry, run = pipeline
    registry.register_sql(
        """
        CREATE MATERIALIZED VIEW rollup_src AS SELECT id FROM range(3);
        CREATE MATERIALIZED VIEW groupby_result AS
          SELECT id, SUM(id) AS sum_id, COUNT(*) AS cnt FROM rollup_src GROUP BY id;
        CREATE MATERIALIZED VIEW rollup_result AS
          SELECT id, SUM(id) AS sum_id, COUNT(*) AS cnt FROM rollup_src GROUP BY ROLLUP(id);
        """,
        tmp_path / "rollup.sql",
    )
    run()
    groupby = pipeline_spark.table("groupby_result")
    rollup = pipeline_spark.table("rollup_result")
    assert {row.id for row in groupby.select("id").collect()} == {0, 1, 2}
    assert rollup.count() == groupby.count() + 1
    total = rollup.where("id IS NULL").collect()
    assert len(total) == 1
    assert total[0].sum_id == total[0].cnt == 3
