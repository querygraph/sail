"""Raw Flow/INSERT syntax boundaries from Spark 4.2.1."""

from pathlib import Path

import pytest
from pyspark.errors.exceptions.connect import AnalysisException, ParseException


@pytest.mark.parametrize("api", ["sql", "pipeline"])
@pytest.mark.parametrize(
    ("sql", "token"),
    [
        ("INSERT OVERWRITE output IF NOT EXISTS SELECT 1", "IF"),
        ("INSERT OVERWRITE output IF NOT EXISTS BY NAME SELECT 1", "IF"),
        ("CREATE FLOW f AS INSERT OVERWRITE output IF NOT EXISTS BY NAME SELECT 1", "IF"),
        ("CREATE FLOW f AS SELECT 1", "SELECT"),
        ("CREATE FLOW f AS WITH s AS (SELECT 1) INSERT INTO output BY NAME SELECT * FROM s", "WITH"),
        ("CREATE FLOW f AS CREATE TABLE output AS SELECT 1", "CREATE"),
        ("CREATE FLOW f AS EXPLAIN INSERT INTO output BY NAME SELECT 1", "EXPLAIN"),
        ("CREATE FLOW f AS CREATE FLOW g AS INSERT INTO output BY NAME SELECT 1", "CREATE"),
        ("CREATE FLOW a.f AS INSERT OVERWRITE output IF NOT EXISTS SELECT 1", "IF"),
        ("/* 雪🦀 */ CREATE FLOW f AS /* SELECT */ select 1", "select"),
    ],
)
def test_insert_flow_grammar_has_raw_spark_syntax_fields(pipeline_spark, pipeline, api, sql, token):
    _, registry, _ = pipeline
    with pytest.raises(ParseException) as captured:
        if api == "sql":
            pipeline_spark.sql(sql).collect()
        else:
            registry.register_sql(sql, Path("syntax.sql"))
    error = captured.value
    assert error.getCondition() == "PARSE_SYNTAX_ERROR"
    assert error.getSqlState() == "42601"
    assert error.getMessageParameters() == {"error": f"'{token}'", "hint": ""}
    assert f"Syntax error at or near '{token}'." in str(error)
    assert "syntax.sql" not in str(error)
    assert not pipeline_spark.catalog.tableExists("output")


@pytest.mark.parametrize(
    "operation",
    [
        "INSERT OVERWRITE DIRECTORY '/tmp/sdp-unwritten' USING parquet SELECT 1",
        "INSERT OVERWRITE LOCAL DIRECTORY '/tmp/sdp-unwritten' STORED AS PARQUET SELECT 1",
        "INSERT INTO output REPLACE WHERE true SELECT 1",
    ],
)
def test_non_insert_statement_nodes_keep_the_unsupported_flow_wrapper(pipeline, operation):
    _, registry, _ = pipeline
    sql = f"CREATE FLOW f AS {operation}"
    with pytest.raises(AnalysisException) as captured:
        registry.register_sql(sql, Path("flow.sql"))
    assert captured.value.getCondition() == "PIPELINE_SQL_GRAPH_ELEMENT_REGISTRATION_ERROR"
    assert captured.value.getSqlState() == "42000"
    assert captured.value.getMessageParameters() == {
        "message": "Unable flow type. Only INSERT INTO flows are supported.",
        "offendingQuery": f"\nOffending query:\n{sql}\n",
        "codeLocation": "Query defined at flow.sql:1",
    }


def test_flow_insert_query_keeps_its_with_clause(pipeline):
    _, registry, _ = pipeline
    registry.register_sql(
        "CREATE STREAMING TABLE output;\n"
        "CREATE FLOW f AS INSERT INTO output BY NAME WITH s AS (SELECT 1) SELECT * FROM s",
        Path("flow.sql"),
    )
