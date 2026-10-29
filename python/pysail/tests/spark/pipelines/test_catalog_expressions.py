"""SET CATALOG uses Spark's constant-expression semantics in SQL and pipelines."""

from uuid import uuid4

import pytest
from pyspark.errors.exceptions.connect import AnalysisException

pytest.importorskip("pyspark.pipelines")


@pytest.mark.parametrize("expression", ["CONCAT({catalog}, '')", "CAST({catalog} AS STRING)"])
def test_catalog_expression_in_ordinary_sql(pipeline_spark, expression):
    catalog = pipeline_spark.catalog.currentCatalog()
    quoted = "'" + catalog.replace("'", "''") + "'"
    pipeline_spark.sql("SET CATALOG " + expression.format(catalog=quoted)).collect()
    assert pipeline_spark.catalog.currentCatalog() == catalog


@pytest.mark.parametrize("expression", ["CONCAT({catalog}, '')", "CAST({catalog} AS STRING)"])
def test_catalog_expression_publishes_pipeline_output(pipeline_spark, pipeline, tmp_path, expression):
    _, registry, run = pipeline
    catalog = pipeline_spark.catalog.currentCatalog()
    database = pipeline_spark.catalog.currentDatabase()
    quoted = "'" + catalog.replace("'", "''") + "'"
    name = "catalog_expression_" + uuid4().hex
    output = f"`{catalog}`.`{database}`.`{name}`"
    sql = (
        "SET CATALOG " + expression.format(catalog=quoted) + "; "
        f"CREATE MATERIALIZED VIEW `{database}`.`{name}` AS SELECT 17 AS value"
    )
    try:
        registry.register_sql(sql, tmp_path / "catalog.sql")
        assert pipeline_spark.catalog.currentCatalog() == catalog
        assert pipeline_spark.catalog.currentDatabase() == database
        run()
        assert [row.value for row in pipeline_spark.table(output).collect()] == [17]
    finally:
        pipeline_spark.sql(f"DROP TABLE IF EXISTS {output}")


@pytest.mark.parametrize("expression", ["CONCAT(foo, '1')", "test.cat", "1", "CAST(NULL AS STRING)"])
def test_invalid_catalog_expression_has_native_pipeline_diagnostics(pipeline_spark, pipeline, tmp_path, expression):
    _, registry, _ = pipeline
    catalog = pipeline_spark.catalog.currentCatalog()
    database = pipeline_spark.catalog.currentDatabase()
    query = "SET CATALOG " + expression
    path = tmp_path / "catalog.sql"
    with pytest.raises(AnalysisException) as error:
        registry.register_sql("-- source\n" + query, path)
    assert error.value.getCondition() == "PIPELINE_SQL_GRAPH_ELEMENT_REGISTRATION_ERROR"
    assert error.value.getSqlState() == "42000"
    parameters = error.value.getMessageParameters()
    assert set(parameters) == {"message", "offendingQuery", "codeLocation"}
    assert parameters["message"].startswith("Failed to resolve catalog expression: ")
    assert parameters["offendingQuery"] == f"\nOffending query:\n{query}\n"
    assert parameters["codeLocation"] == f"Query defined at {path}:2"
    assert pipeline_spark.catalog.currentCatalog() == catalog
    assert pipeline_spark.catalog.currentDatabase() == database
