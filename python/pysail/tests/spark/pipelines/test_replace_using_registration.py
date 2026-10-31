"""REPLACE USING preserves Spark's registration rejection and precedence."""

from pathlib import Path

import pytest
from pyspark.errors.exceptions.connect import AnalysisException


def _register_error(pipeline, sql):
    _, registry, _ = pipeline
    with pytest.raises(AnalysisException) as captured:
        registry.register_sql(sql, Path("using.sql"))
    return captured.value


def _assert_wrapper(error, sql, message):
    assert error.getCondition() == "PIPELINE_SQL_GRAPH_ELEMENT_REGISTRATION_ERROR"
    assert error.getSqlState() == "42000"
    assert error.getMessageParameters() == {
        "message": message,
        "offendingQuery": f"\nOffending query:\n{sql}\n",
        "codeLocation": "Query defined at using.sql:1",
    }


@pytest.mark.parametrize("by_name", [False, True])
@pytest.mark.parametrize("flow", [False, True])
def test_replace_using_registration_rejects_the_correct_node(pipeline, by_name, flow):
    clause = "BY NAME " if by_name else ""
    operation = f"INSERT INTO output {clause}REPLACE USING (id) SELECT 1 AS id"
    sql = f"CREATE FLOW f AS {operation}" if flow else operation
    message = (
        "INSERT OVERWRITE flows not supported."
        if flow
        else "Unsupported plan InsertIntoStatement parsed from SQL query"
    )
    _assert_wrapper(_register_error(pipeline, sql), sql, message)


@pytest.mark.parametrize("by_name", [False, True])
def test_flow_name_precedes_replace_using_policy(pipeline, by_name):
    clause = "BY NAME " if by_name else ""
    sql = f"CREATE FLOW d.f AS INSERT INTO output {clause}REPLACE USING (id) SELECT 1 AS id"
    error = _register_error(pipeline, sql)
    assert error.getCondition() == "MULTIPART_FLOW_NAME_NOT_SUPPORTED"
    assert error.getSqlState() == "0A000"
    assert error.getMessageParameters() == {"flowName": "d.f"}
    assert "using.sql" not in str(error)


def test_replace_using_overwrite_precedes_target_arity(pipeline):
    sql = "CREATE FLOW f AS INSERT INTO A.b.c.d REPLACE USING (id) SELECT 1 AS id"
    _assert_wrapper(_register_error(pipeline, sql), sql, "INSERT OVERWRITE flows not supported.")


def test_with_led_replace_using_keeps_its_outer_node(pipeline):
    sql = "WITH s AS (SELECT 1 AS id) INSERT INTO output REPLACE USING (id) SELECT * FROM s"
    _assert_wrapper(_register_error(pipeline, sql), sql, "Unsupported plan UnresolvedWith parsed from SQL query")
