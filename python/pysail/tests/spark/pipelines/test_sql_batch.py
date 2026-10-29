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


def test_static_sql_dataset(pipeline_spark, pipeline, tmp_path):
    _, registry, run = pipeline
    registry.register_sql(
        "CREATE MATERIALIZED VIEW static_a COMMENT 'this is a comment' AS SELECT * FROM range(1, 4)",
        tmp_path / "static.sql",
    )
    run()
    assert [tuple(row) for row in pipeline_spark.table("static_a").orderBy("id").collect()] == [(1,), (2,), (3,)]


def test_escaped_hyphenated_dataset_dependencies(pipeline_spark, pipeline, tmp_path):
    _, registry, run = pipeline
    registry.register_sql(
        """
        CREATE MATERIALIZED VIEW `hyphen-mv` AS SELECT * FROM range(1, 4);
        CREATE MATERIALIZED VIEW `other-hyphen-mv` AS SELECT * FROM `hyphen-mv`;
        """,
        tmp_path / "escaped.sql",
    )
    run()
    for name in ["hyphen-mv", "other-hyphen-mv"]:
        df = pipeline_spark.table(f"`{name}`")
        assert not df.isStreaming
        assert [tuple(row) for row in df.orderBy("id").collect()] == [(1,), (2,), (3,)]


def test_persisted_view_dependency_chain(pipeline_spark, pipeline, tmp_path):
    _, registry, run = pipeline
    registry.register_sql(
        """
        CREATE VIEW b AS SELECT * FROM a;
        CREATE VIEW a AS SELECT * FROM range(1, 4);
        CREATE VIEW c AS SELECT * FROM `b`;
        CREATE VIEW d AS SELECT * FROM c;
        """,
        tmp_path / "view_chain.sql",
    )
    run()
    for name in ["a", "b", "c", "d"]:
        assert [tuple(row) for row in pipeline_spark.table(name).orderBy("id").collect()] == [(1,), (2,), (3,)]


def test_sql_aggregation_over_pipeline_dataset(pipeline_spark, pipeline, tmp_path):
    _, registry, run = pipeline
    registry.register_sql(
        """
        CREATE MATERIALIZED VIEW aggregation_a AS SELECT id AS value, (id % 2) AS isOdd FROM range(1, 10);
        CREATE MATERIALIZED VIEW aggregation_b AS
          SELECT isOdd, max(value) AS maximum FROM aggregation_a GROUP BY isOdd LIMIT 2;
        """,
        tmp_path / "aggregation.sql",
    )
    run()
    assert [tuple(row) for row in pipeline_spark.table("aggregation_b").orderBy("isOdd").collect()] == [(0, 8), (1, 9)]


def test_sql_join_over_private_views(pipeline_spark, pipeline, tmp_path):
    _, registry, run = pipeline
    registry.register_sql(
        """
        CREATE TEMPORARY VIEW join_a AS SELECT id FROM range(1, 3);
        CREATE TEMPORARY VIEW join_b AS SELECT id FROM range(1, 3);
        CREATE MATERIALIZED VIEW join_c AS SELECT a.id AS id1, b.id AS id2
          FROM join_a a JOIN join_b b ON a.id = b.id;
        """,
        tmp_path / "join.sql",
    )
    run()
    assert [tuple(row) for row in pipeline_spark.table("join_c").orderBy("id1").collect()] == [(1, 1), (2, 2)]
    assert not pipeline_spark.catalog.tableExists("join_a")
    assert not pipeline_spark.catalog.tableExists("join_b")


def test_escaped_hyphenated_dataset_resolution(pipeline_spark, pipeline, tmp_path):
    _, registry, run = pipeline
    registry.register_sql(
        """
        CREATE MATERIALIZED VIEW `hyphen-mv` AS SELECT * FROM range(1, 4);
        CREATE MATERIALIZED VIEW `other-hyphen-mv` AS SELECT * FROM `hyphen-mv`;
        """,
        tmp_path / "escaped_resolution.sql",
    )
    run(dry=True)
    assert not pipeline_spark.catalog.tableExists("`hyphen-mv`")
    assert not pipeline_spark.catalog.tableExists("`other-hyphen-mv`")
