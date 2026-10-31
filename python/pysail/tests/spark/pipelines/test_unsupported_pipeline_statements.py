"""Original SqlPipelineSuite CTAS rejection and exact registration origins."""

from pathlib import Path

import pytest
from pyspark.errors.exceptions.connect import AnalysisException


@pytest.fixture
def original_external_tables(pipeline_spark):
    catalog = pipeline_spark.catalog.currentCatalog()
    database = pipeline_spark.catalog.currentDatabase()
    tables = [
        ".".join("`" + part.replace("`", "``") + "`" for part in [catalog, database, name])
        for name in ["external_t1", "external_t2"]
    ]
    try:
        for table, rows in zip(tables, [3, 4]):
            pipeline_spark.sql(f"CREATE TABLE {table} AS SELECT * FROM RANGE({rows})").collect()
        yield
    finally:
        for table in tables:
            pipeline_spark.sql(f"DROP TABLE IF EXISTS {table}").collect()


@pytest.mark.parametrize(
    ("sql", "offending", "line", "node"),
    [
        ("CREATE TABLE t AS SELECT 1", "CREATE TABLE t AS SELECT 1", 1, "CreateTableAsSelect"),
        (
            "-- 雪\n\nCREATE TABLE t AS SELECT 1 /* é; */;",
            "CREATE TABLE t AS SELECT 1 /* é; */",
            3,
            "CreateTableAsSelect",
        ),
        (
            "SET marker = 1;\n\n-- source\nCREATE TABLE t AS SELECT 1 -- trailing\n;",
            "CREATE TABLE t AS SELECT 1 -- trailing\n",
            4,
            "CreateTableAsSelect",
        ),
        (
            "CREATE TABLE IF NOT EXISTS t AS SELECT 1",
            "CREATE TABLE IF NOT EXISTS t AS SELECT 1",
            1,
            "CreateTableAsSelect",
        ),
        ("CREATE OR REPLACE TABLE t AS SELECT 1", "CREATE OR REPLACE TABLE t AS SELECT 1", 1, "ReplaceTableAsSelect"),
        ("REPLACE TABLE t AS SELECT 1", "REPLACE TABLE t AS SELECT 1", 1, "ReplaceTableAsSelect"),
    ],
    ids=["original-525", "unicode-comments", "second-statement", "if-not-exists", "create-or-replace", "replace"],
)
def test_unsupported_ctas_preserves_registration_diagnostic(
    pipeline_spark, pipeline, original_external_tables, sql, offending, line, node
):
    _, registry, _ = pipeline
    with pytest.raises(AnalysisException) as captured:
        registry.register_sql(sql, Path("a.sql"))
    error = captured.value
    assert error.getCondition() == "PIPELINE_SQL_GRAPH_ELEMENT_REGISTRATION_ERROR"
    assert error.getSqlState() == "42000"
    assert error.getMessageParameters() == {
        "message": f"Unsupported plan {node} parsed from SQL query",
        "offendingQuery": f"\nOffending query:\n{offending}\n",
        "codeLocation": f"Query defined at a.sql:{line}",
    }
    assert "Unsupported plan" in str(error)
    assert not pipeline_spark.catalog.tableExists("t")
