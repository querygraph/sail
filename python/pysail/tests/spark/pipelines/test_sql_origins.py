"""SQL registration errors retain the original statement and source line."""

import pytest
from pyspark.errors.exceptions.connect import AnalysisException

pytest.importorskip("pyspark.pipelines")


@pytest.mark.parametrize(
    ("sql", "offending", "line"),
    [
        pytest.param(
            "-- 雪\nSELECT 'é;雪' /* trailing ; */;",
            "SELECT 'é;雪' /* trailing ; */",
            2,
            id="leading-trivia",
        ),
        pytest.param(
            "SET marker = 1;\n\n-- source\nSELECT 2 -- trailing\n;",
            "SELECT 2 -- trailing\n",
            4,
            id="second-statement",
        ),
    ],
)
def test_sql_registration_error_retains_statement_origin(pipeline, tmp_path, sql, offending, line):
    _, registry, _ = pipeline
    path = tmp_path / "é;origin.sql"
    with pytest.raises(AnalysisException) as error:
        registry.register_sql(sql, path)
    assert error.value.getCondition() == "PIPELINE_SQL_GRAPH_ELEMENT_REGISTRATION_ERROR"
    assert error.value.getSqlState() == "42000"
    parameters = error.value.getMessageParameters()
    assert set(parameters) == {"message", "offendingQuery", "codeLocation"}
    assert parameters["message"]
    assert parameters["offendingQuery"] == f"\nOffending query:\n{offending}\n"
    assert parameters["codeLocation"] == f"Query defined at {path}:{line}"
