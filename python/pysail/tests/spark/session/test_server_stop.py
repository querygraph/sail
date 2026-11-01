"""Stopping a server that is doing something. This starts its own Sail server.

An active response can only finish once its job is stopped, so a server that waits for its
transport to drain before it stops its sessions never exits.
"""

import os
import signal
import socket
import subprocess
import threading
import time
from uuid import uuid4

import grpc
import pytest
from pyspark.sql.connect import proto as pb
from pyspark.sql.connect.proto import base_pb2_grpc

USER = pb.UserContext(user_id="stop_user")
SLOW_QUERY = "SELECT sum(id) AS s FROM range(100000000000000)"

BINARY = os.environ.get("SAIL_TEST_SERVER_BINARY")
pytestmark = pytest.mark.skipif(not BINARY, reason="SAIL_TEST_SERVER_BINARY is not set")


@pytest.fixture
def server(tmp_path):
    with socket.socket() as probe:
        probe.bind(("127.0.0.1", 0))
        port = probe.getsockname()[1]
    with (tmp_path / "server.log").open("w") as log:
        process = subprocess.Popen(  # noqa: S603
            [BINARY, "spark", "server", "--port", str(port)],
            cwd=tmp_path,
            stdout=log,
            stderr=subprocess.STDOUT,
        )
    channel = grpc.insecure_channel(f"127.0.0.1:{port}")
    try:
        grpc.channel_ready_future(channel).result(timeout=60)
        yield process, base_pb2_grpc.SparkConnectServiceStub(channel), port
    finally:
        channel.close()
        if process.poll() is None:
            process.kill()
            process.wait(10)


def execute(stub, session, plan):
    return stub.ExecutePlan(pb.ExecutePlanRequest(session_id=session, user_context=USER, plan=plan))


def command(stub, session, build):
    message = pb.Command()
    build(message.pipeline_command)
    return list(execute(stub, session, pb.Plan(command=message)))


def start_slow_run(stub, session, storage):
    """Start a pipeline run, and wait until the server says that it is running."""
    responses = command(stub, session, lambda c: c.create_dataflow_graph.SetInParent())
    graph = next(
        r.pipeline_command_result.create_dataflow_graph_result.dataflow_graph_id
        for r in responses
        if r.HasField("pipeline_command_result")
    )
    command(
        stub,
        session,
        lambda c: c.define_output.CopyFrom(
            pb.PipelineCommand.DefineOutput(
                dataflow_graph_id=graph, output_name="slow", output_type=pb.MATERIALIZED_VIEW
            )
        ),
    )
    relation = pb.Relation(sql=pb.SQL(query=SLOW_QUERY))
    command(
        stub,
        session,
        lambda c: c.define_flow.CopyFrom(
            pb.PipelineCommand.DefineFlow(
                dataflow_graph_id=graph,
                flow_name="slow",
                target_dataset_name="slow",
                relation_flow_details=pb.PipelineCommand.DefineFlow.WriteRelationFlowDetails(relation=relation),
            )
        ),
    )
    message = pb.Command()
    message.pipeline_command.start_run.CopyFrom(pb.PipelineCommand.StartRun(dataflow_graph_id=graph, storage=storage))
    stream = execute(stub, session, pb.Plan(command=message))
    for response in stream:
        if response.HasField("pipeline_event_result") and "RUNNING" in response.pipeline_event_result.event.message:
            return stream
    pytest.fail("the run ended before it was stopped")
    return None


def drain(stream):
    """Read to the end, in the background, and report what ended the stream."""
    outcome = []

    def read():
        try:
            for _ in stream:
                pass
            outcome.append(None)
        except grpc.RpcError as error:
            outcome.append(error)

    thread = threading.Thread(target=read, daemon=True)
    thread.start()
    return thread, outcome


def assert_stops_cleanly(process, port):
    process.send_signal(signal.SIGINT)
    assert process.wait(30) == 0
    with socket.socket() as probe:
        probe.settimeout(1)
        assert probe.connect_ex(("127.0.0.1", port)) != 0


def test_server_stops_with_an_active_query(server):
    process, stub, port = server
    stream = execute(stub, str(uuid4()), pb.Plan(root=pb.Relation(sql=pb.SQL(query=SLOW_QUERY))))
    thread, outcome = drain(stream)
    # There is nothing to observe until a result exists, which is the point of the slow query.
    time.sleep(1)
    assert thread.is_alive()
    assert_stops_cleanly(process, port)
    thread.join(10)
    assert not thread.is_alive()
    assert isinstance(outcome[0], grpc.RpcError)


def test_server_stops_with_an_active_pipeline_run(server, tmp_path):
    process, stub, port = server
    stream = start_slow_run(stub, str(uuid4()), (tmp_path / "storage").as_uri())
    thread, outcome = drain(stream)
    assert_stops_cleanly(process, port)
    thread.join(10)
    assert not thread.is_alive()
    assert isinstance(outcome[0], grpc.RpcError)


def test_release_finishes_with_an_active_pipeline_run_and_the_session_stays_closed(server, tmp_path):
    process, stub, port = server
    session = str(uuid4())
    stream = start_slow_run(stub, session, (tmp_path / "storage").as_uri())
    thread, outcome = drain(stream)
    released = stub.ReleaseSession(pb.ReleaseSessionRequest(session_id=session, user_context=USER), timeout=30)
    assert released.server_side_session_id
    thread.join(10)
    assert not thread.is_alive()
    assert "INVALID_HANDLE.SESSION_CLOSED" in outcome[0].details()
    assert_stops_cleanly(process, port)
