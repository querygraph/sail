"""Protocol observations for session-owned and coexisting pipeline graphs."""

from uuid import uuid4

import grpc
import pytest
from pyspark.sql.connect import proto as pb

pytest.importorskip("pyspark.pipelines")


def _execute(stub, session_id, user_id, command):
    request = pb.ExecutePlanRequest(
        session_id=session_id,
        user_context=pb.UserContext(user_id=user_id),
        operation_id=str(uuid4()),
        plan=pb.Plan(command=command),
    )
    return list(stub.ExecutePlan(request))


def _create_graph(stub, session_id, user_id):
    command = pb.Command()
    command.pipeline_command.create_dataflow_graph.SetInParent()
    responses = _execute(stub, session_id, user_id, command)
    results = [r.pipeline_command_result for r in responses if r.HasField("pipeline_command_result")]
    assert len(results) == 1
    assert results[0].HasField("create_dataflow_graph_result")
    graph_id = results[0].create_dataflow_graph_result.dataflow_graph_id
    assert graph_id
    return graph_id


def _output(graph_id, name):
    command = pb.Command()
    command.pipeline_command.define_output.CopyFrom(
        pb.PipelineCommand.DefineOutput(
            dataflow_graph_id=graph_id,
            output_name=name,
            output_type=pb.MATERIALIZED_VIEW,
        )
    )
    return command


@pytest.mark.parametrize("different_users", [False, True])
def test_graph_registration_is_session_specific(pipeline_spark, different_users):
    stub = pipeline_spark.client._stub
    first = (str(uuid4()), "pipeline_user_1")
    second = (str(uuid4()), "pipeline_user_2" if different_users else first[1])
    try:
        graph1 = _create_graph(stub, *first)
        graph2 = _create_graph(stub, *second)
        assert graph1 != graph2
        _execute(stub, *first, _output(graph1, "session1_table"))
        _execute(stub, *second, _output(graph2, "session2_table"))
        for session, foreign_graph in [(first, graph2), (second, graph1)]:
            with pytest.raises(grpc.RpcError) as error:
                _execute(stub, *session, _output(foreign_graph, "foreign_table"))
            assert "DATAFLOW_GRAPH_NOT_FOUND" in error.value.details()
        # Dropping an absent graph in another session must leave its owner intact.
        command = pb.Command()
        command.pipeline_command.drop_dataflow_graph.dataflow_graph_id = graph1
        _execute(stub, *second, command)
        for session, graph_id in [(first, graph1), (second, graph2)]:
            responses = _execute(stub, *session, _output(graph_id, "owner_still_present"))
            assert any(
                response.HasField("pipeline_command_result")
                and response.pipeline_command_result.HasField("define_output_result")
                for response in responses
            )
    finally:
        for session_id, user_id in [first, second]:
            stub.ReleaseSession(
                pb.ReleaseSessionRequest(session_id=session_id, user_context=pb.UserContext(user_id=user_id))
            )


def test_multiple_graphs_keep_separate_outputs(pipeline_spark, pipeline_factory, tmp_path):
    graph1, registry1, run1 = pipeline_factory()
    graph2, registry2, run2 = pipeline_factory()
    assert graph1 != graph2
    registry1.register_sql("CREATE MATERIALIZED VIEW graph1_table AS SELECT 1 AS value", tmp_path / "graph1.sql")
    registry2.register_sql("CREATE MATERIALIZED VIEW graph2_table AS SELECT 2 AS value", tmp_path / "graph2.sql")
    run1()
    assert [row.value for row in pipeline_spark.table("graph1_table").collect()] == [1]
    assert not pipeline_spark.catalog.tableExists("graph2_table")
    command = pb.Command()
    command.pipeline_command.drop_dataflow_graph.dataflow_graph_id = graph1
    pipeline_spark.client.execute_command(command)
    run2()
    assert [row.value for row in pipeline_spark.table("graph2_table").collect()] == [2]
    assert [row.value for row in pipeline_spark.table("graph1_table").collect()] == [1]
