"""ReleaseSession as a client sees it. These pass unchanged on Spark 4.2.1 and on Sail."""

from uuid import uuid4

import grpc
import pytest
from google.rpc import error_details_pb2, status_pb2
from pyspark.sql.connect import proto as pb

USER = pb.UserContext(user_id="release_user")


@pytest.fixture
def stub(spark):
    return spark.client._stub  # noqa: SLF001


def execute(stub, session_id):
    query = pb.Relation(sql=pb.SQL(query="SELECT 1"))
    request = pb.ExecutePlanRequest(session_id=session_id, user_context=USER, plan=pb.Plan(root=query))
    return list(stub.ExecutePlan(request))


def release(stub, session_id):
    return stub.ReleaseSession(pb.ReleaseSessionRequest(session_id=session_id, user_context=USER))


REQUESTS = {
    "ExecutePlan": lambda stub, session: execute(stub, session),
    "AnalyzePlan": lambda stub, session: stub.AnalyzePlan(
        pb.AnalyzePlanRequest(
            session_id=session, user_context=USER, spark_version=pb.AnalyzePlanRequest.SparkVersion()
        )
    ),
    "Config": lambda stub, session: stub.Config(
        pb.ConfigRequest(
            session_id=session,
            user_context=USER,
            operation=pb.ConfigRequest.Operation(get_all=pb.ConfigRequest.GetAll()),
        )
    ),
    "AddArtifacts": lambda stub, session: stub.AddArtifacts(
        iter([pb.AddArtifactsRequest(session_id=session, user_context=USER, batch=pb.AddArtifactsRequest.Batch())])
    ),
    "ArtifactStatus": lambda stub, session: stub.ArtifactStatus(
        pb.ArtifactStatusesRequest(session_id=session, user_context=USER, names=["a"])
    ),
    "Interrupt": lambda stub, session: stub.Interrupt(
        pb.InterruptRequest(
            session_id=session, user_context=USER, interrupt_type=pb.InterruptRequest.INTERRUPT_TYPE_ALL
        )
    ),
    "ReattachExecute": lambda stub, session: list(
        stub.ReattachExecute(pb.ReattachExecuteRequest(session_id=session, user_context=USER, operation_id=str(uuid4())))
    ),
    "ReleaseExecute": lambda stub, session: stub.ReleaseExecute(
        pb.ReleaseExecuteRequest(
            session_id=session,
            user_context=USER,
            operation_id=str(uuid4()),
            release_all=pb.ReleaseExecuteRequest.ReleaseAll(),
        )
    ),
    "FetchErrorDetails": lambda stub, session: stub.FetchErrorDetails(
        pb.FetchErrorDetailsRequest(session_id=session, user_context=USER, error_id="x")
    ),
    "GetStatus": lambda stub, session: stub.GetStatus(pb.GetStatusRequest(session_id=session, user_context=USER)),
    "CloneSession": lambda stub, session: stub.CloneSession(
        pb.CloneSessionRequest(session_id=session, user_context=USER)
    ),
}


@pytest.mark.parametrize("name", REQUESTS)
def test_every_request_on_a_released_session_reports_that_it_was_closed(stub, name):
    session = str(uuid4())
    execute(stub, session)
    release(stub, session)
    with pytest.raises(grpc.RpcError) as error:
        REQUESTS[name](stub, session)
    assert error.value.code() == grpc.StatusCode.INTERNAL
    assert error.value.details() == (
        f"[INVALID_HANDLE.SESSION_CLOSED] The handle {session} is invalid. Session was closed. SQLSTATE: HY000"
    )
    metadata = dict(error.value.trailing_metadata())
    status = status_pb2.Status.FromString(metadata["grpc-status-details-bin"])
    info = error_details_pb2.ErrorInfo()
    assert status.details[0].Unpack(info)
    assert info.reason == "org.apache.spark.SparkSQLException"
    assert info.metadata["errorClass"] == "INVALID_HANDLE.SESSION_CLOSED"
    assert info.metadata["sqlState"] == "HY000"
    assert info.metadata["messageParameters"] == f'{{"handle":"{session}"}}'


def test_repeated_release_succeeds_and_only_the_first_names_the_session(stub):
    session = str(uuid4())
    execute(stub, session)
    assert release(stub, session).server_side_session_id
    repeated = release(stub, session)
    assert repeated.session_id == session
    assert not repeated.server_side_session_id


def test_releasing_a_session_that_was_never_seen_leaves_no_tombstone(stub):
    session = str(uuid4())
    assert not release(stub, session).server_side_session_id
    assert not release(stub, session).server_side_session_id
    assert execute(stub, session)
    assert release(stub, session).server_side_session_id
