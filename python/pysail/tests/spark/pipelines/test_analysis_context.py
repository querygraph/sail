"""Exact deferred SQL response shapes from Spark's Connect pipeline server tests."""

from uuid import uuid4

import pytest
from google.protobuf.any_pb2 import Any
from pyspark.sql.connect import proto as pb

pytest.importorskip("pyspark.pipelines")


@pytest.mark.parametrize(
    "flow_name",
    [pytest.param("flow1", id="inside-flow"), pytest.param(None, id="outside-flow")],
)
def test_analysis_context_sql_returns_original_relation(pipeline_spark, pipeline, flow_name):
    graph_id, _, _ = pipeline
    context = pb.PipelineAnalysisContext(dataflow_graph_id=graph_id)
    relation = pb.Relation()
    if flow_name is not None:
        context.flow_name = flow_name
        relation.read.named_table.unparsed_identifier = "table"
    else:
        relation.sql.query = "SELECT * FROM RANGE(5)"
    extension = Any()
    extension.Pack(context)
    user = pb.UserContext(user_id="test_user", extensions=[extension])
    session_id = str(uuid4())
    command = pb.Command()
    command.sql_command.input.CopyFrom(relation)
    request = pb.ExecutePlanRequest(
        session_id=session_id,
        user_context=user,
        operation_id=str(uuid4()),
        plan=pb.Plan(command=command),
    )
    stub = pipeline_spark.client._stub
    try:
        responses = list(stub.ExecutePlan(request))
        assert responses
        assert responses[0].HasField("sql_command_result")
        assert responses[0].sql_command_result.relation == relation
    finally:
        stub.ReleaseSession(pb.ReleaseSessionRequest(session_id=session_id, user_context=user))
