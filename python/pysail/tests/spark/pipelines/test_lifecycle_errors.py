import pytest
from pyspark.errors.exceptions import connect as errors
from pyspark.sql.connect import proto as pb

pytest.importorskip("pyspark.pipelines")


@pytest.mark.parametrize("operation", ["define_output", "define_flow", "define_sql_graph_elements", "start_run"])
def test_missing_graph_has_structured_error(pipeline_spark, operation):
    graph_id = "graph-that-does-not-exist"
    command = pb.Command()
    getattr(command.pipeline_command, operation).dataflow_graph_id = graph_id
    with pytest.raises(getattr(errors, "SparkException", errors.UnknownException)) as error:
        pipeline_spark.client.execute_command(command)
    assert error.value.getCondition() == "DATAFLOW_GRAPH_NOT_FOUND"
    assert error.value.getMessageParameters() == {"graphId": graph_id}
    assert error.value.getSqlState() == "KD011"


def test_dropping_missing_graph_is_idempotent(pipeline_spark):
    command = pb.Command()
    command.pipeline_command.drop_dataflow_graph.dataflow_graph_id = "graph-that-does-not-exist"
    pipeline_spark.client.execute_command(command)
    pipeline_spark.client.execute_command(command)
