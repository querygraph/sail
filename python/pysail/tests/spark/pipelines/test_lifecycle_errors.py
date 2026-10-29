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


@pytest.mark.parametrize("once", [True, False])
@pytest.mark.parametrize("graph_exists", [True, False])
@pytest.mark.parametrize("flow_name", [None, "named_flow"])
def test_once_option_is_rejected_before_graph_lookup(pipeline_spark, pipeline, once, graph_exists, flow_name):
    graph_id, _, _ = pipeline
    command = pb.Command()
    flow = command.pipeline_command.define_flow
    flow.dataflow_graph_id = graph_id if graph_exists else "graph-that-does-not-exist"
    flow.once = once
    if flow_name is not None:
        flow.flow_name = flow_name
    with pytest.raises(errors.AnalysisException) as error:
        pipeline_spark.client.execute_command(command)
    assert error.value.getCondition() == "DEFINE_FLOW_ONCE_OPTION_NOT_SUPPORTED"
    assert error.value.getMessageParameters() == {"flowName": flow_name or ""}
    assert error.value.getSqlState() == "0A000"
