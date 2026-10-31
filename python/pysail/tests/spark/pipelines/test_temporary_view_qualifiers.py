"""Original SqlPipelineSuite:599 rejection and shared session-name normalization."""

from pathlib import Path

import pytest
from pyspark.errors import ParseException
from pyspark.sql.connect import proto as pb

pytest.importorskip("pyspark.pipelines")


def _drop_graph(spark, graph_id):
    command = pb.Command()
    command.pipeline_command.drop_dataflow_graph.dataflow_graph_id = graph_id
    spark.client.execute_command(command)


@pytest.mark.parametrize("api", ["sql", "pipeline"])
def test_temporary_view_rejects_database_qualifier(pipeline_spark, pipeline, api):
    graph_id, registry, _ = pipeline
    sql = "CREATE TEMPORARY VIEW some_database.tv AS SELECT 1;"
    try:
        with pytest.raises(ParseException) as raised:
            if api == "sql":
                pipeline_spark.sql(sql).collect()
            else:
                registry.register_sql(
                    sql + " CREATE MATERIALIZED VIEW mv AS SELECT * FROM some_database.tv;",
                    Path("a.sql"),
                )
        assert raised.value.getCondition() == "INVALID_TEMP_OBJ_QUALIFIER"
        assert raised.value.getSqlState() == "42602"
        assert raised.value.getMessageParameters() == {
            "objectType": "VIEW",
            "objectName": "`tv`",
            "qualifier": "`some_database`",
        }
    finally:
        _drop_graph(pipeline_spark, graph_id)


@pytest.mark.parametrize("api", ["sql", "pipeline"])
@pytest.mark.parametrize("name", ["session.v", "system.session.v", "SeSsIoN.v", "`seſſıon`.v"])
def test_temporary_view_session_qualifier_normalizes_name(pipeline_spark, pipeline, api, name):
    graph_id, registry, run = pipeline
    try:
        sql = f"CREATE TEMPORARY VIEW {name} AS SELECT 1 AS value"
        if api == "sql":
            pipeline_spark.sql(sql).collect()
            assert [row.value for row in pipeline_spark.table("v").collect()] == [1]
        else:
            registry.register_sql(
                sql + "; CREATE MATERIALIZED VIEW qualified_output AS SELECT * FROM v;",
                Path("qualified.sql"),
            )
            run()
            assert [row.value for row in pipeline_spark.table("qualified_output").collect()] == [1]
            assert not pipeline_spark.catalog.tableExists("v")
    finally:
        _drop_graph(pipeline_spark, graph_id)
        if api == "sql":
            pipeline_spark.sql("DROP VIEW IF EXISTS v").collect()
        else:
            pipeline_spark.sql("DROP TABLE IF EXISTS qualified_output").collect()
