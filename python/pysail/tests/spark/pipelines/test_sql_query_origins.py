"""SqlQueryOriginSuite, through the public client.

Origins reach a client only in the error for a statement that a pipeline file may not contain, which
carries the statement's text and its file and line. Each case keeps the Scala file's layout with such
a statement in the place being checked. Character offsets are never sent, so startPosition is not
asserted.
"""

import pytest
from pyspark.errors.exceptions.connect import AnalysisException
from pyspark.pipelines.spark_connect_graph_element_registry import SparkConnectGraphElementRegistry
from pyspark.pipelines.spark_connect_pipeline import create_dataflow_graph


@pytest.fixture
def schema(spark, request):
    name = "so_" + "".join(c if c.isalnum() else "_" for c in request.node.name).lower()[:48]
    spark.sql(f"DROP SCHEMA IF EXISTS {name} CASCADE")
    spark.sql(f"CREATE SCHEMA {name}")
    yield name
    spark.sql(f"DROP SCHEMA IF EXISTS {name} CASCADE")


@pytest.mark.parametrize(
    ("text", "offending", "line"),
    [
        pytest.param(
            "-- comment 1\nCREATE MATERIALIZED VIEW a.b.c AS SELECT 1;\n\nSELECT 2 ; -- comment 2\n",
            "SELECT 2 ",
            4,
            id="basic",
        ),
        pytest.param(
            'CREATE STREAMING TABLE `a.\\n` AS SELECT "\\n";\nSELECT * FROM `a.\\n`;\n',
            "SELECT * FROM `a.\\n`",
            2,
            id="escaped-newline-is-not-a-line",
        ),
        pytest.param(
            "\n     \tSELECT 1;\n",
            "SELECT 1",
            2,
            id="white-space",
        ),
        pytest.param(
            "\nSELECT\n1\nAS\nid;\n",
            "SELECT\n1\nAS\nid",
            2,
            id="multiline-statement-is-at-its-first-line",
        ),
        pytest.param(
            "\n-- comment\nSELECT 1 -- another comment\nAS id;\n",
            "SELECT 1 -- another comment\nAS id",
            3,
            id="preceding-comment-is-omitted",
        ),
        pytest.param(
            "SELECT 'my ; string';",
            "SELECT 'my ; string'",
            1,
            id="semicolon-in-a-string-literal",
        ),
    ],
)
def test_a_statement_keeps_its_origin(spark, schema, text, offending, line):
    graph = create_dataflow_graph(spark, spark.catalog.currentCatalog(), schema, None)
    registry = SparkConnectGraphElementRegistry(spark, graph)
    with pytest.raises(AnalysisException) as error:
        registry.register_sql(text, "file.sql")
    assert error.value.getCondition() == "PIPELINE_SQL_GRAPH_ELEMENT_REGISTRATION_ERROR"
    parameters = error.value.getMessageParameters()
    assert parameters["offendingQuery"] == f"\nOffending query:\n{offending}\n"
    assert parameters["codeLocation"] == f"Query defined at file.sql:{line}"
