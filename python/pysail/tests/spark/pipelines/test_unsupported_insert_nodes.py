"""Ordinary INSERT plans are rejected as pipeline definitions before analysis."""

from pathlib import Path

import pytest
from pyspark.errors.exceptions.connect import AnalysisException


@pytest.mark.parametrize(
    ("sql", "node"),
    [
        ("INSERT INTO output BY NAME SELECT 1", "InsertIntoStatement"),
        ("INSERT OVERWRITE output PARTITION(p=1) IF NOT EXISTS SELECT 1", "InsertIntoStatement"),
        ("WITH s AS (SELECT 1) INSERT INTO output BY NAME SELECT * FROM s", "UnresolvedWith"),
        ("INSERT INTO output REPLACE WHERE true SELECT 1", "OverwriteByExpression"),
        ("WITH s AS (SELECT 1) INSERT INTO output REPLACE WHERE true SELECT * FROM s", "UnresolvedWith"),
        ("INSERT OVERWRITE DIRECTORY '/tmp/sdp-unwritten' USING parquet SELECT 1", "InsertIntoDir"),
        (
            "WITH s AS (SELECT 1) INSERT OVERWRITE DIRECTORY '/tmp/sdp-unwritten' USING parquet SELECT * FROM s",
            "UnresolvedWith",
        ),
    ],
)
def test_ordinary_insert_nodes_keep_exact_registration_diagnostics(pipeline_spark, pipeline, sql, node):
    _, registry, _ = pipeline
    with pytest.raises(AnalysisException) as captured:
        registry.register_sql(f"CREATE STREAMING TABLE output;\n{sql};", Path("unsupported.sql"))
    error = captured.value
    assert error.getCondition() == "PIPELINE_SQL_GRAPH_ELEMENT_REGISTRATION_ERROR"
    assert error.getSqlState() == "42000"
    assert error.getMessageParameters() == {
        "message": f"Unsupported plan {node} parsed from SQL query",
        "offendingQuery": f"\nOffending query:\n{sql}\n",
        "codeLocation": "Query defined at unsupported.sql:2",
    }
    assert not pipeline_spark.catalog.tableExists("output")
