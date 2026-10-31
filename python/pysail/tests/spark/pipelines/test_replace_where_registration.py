"""REPLACE WHERE is parsed, then rejected as an unsupported Flow operation."""

from pathlib import Path

import pytest
from pyspark.errors.exceptions.connect import AnalysisException


@pytest.mark.parametrize("by_name", [False, True])
@pytest.mark.parametrize("flow", [False, True])
def test_replace_where_registration_preserves_node_policy(pipeline, by_name, flow):
    _, registry, _ = pipeline
    clause = "BY NAME " if by_name else ""
    operation = f"INSERT INTO output {clause}REPLACE WHERE true SELECT 1 AS id"
    sql = f"CREATE FLOW f AS {operation}" if flow else operation
    message = (
        "Unable flow type. Only INSERT INTO flows are supported."
        if flow
        else "Unsupported plan OverwriteByExpression parsed from SQL query"
    )
    with pytest.raises(AnalysisException) as captured:
        registry.register_sql(sql, Path("replace.sql"))
    assert captured.value.getCondition() == "PIPELINE_SQL_GRAPH_ELEMENT_REGISTRATION_ERROR"
    assert captured.value.getSqlState() == "42000"
    assert captured.value.getMessageParameters() == {
        "message": message,
        "offendingQuery": f"\nOffending query:\n{sql}\n",
        "codeLocation": "Query defined at replace.sql:1",
    }
