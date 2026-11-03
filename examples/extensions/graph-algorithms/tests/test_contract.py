import json

import pytest
from pyspark.sql import Row

from pyspark_pecan import CapabilityError, GraphAlgorithms
from pyspark_pecan import utils_pb2 as wire
from pyspark_pecan.utils import GraphUtils, TYPE_URL, _UtilsRelation


def test_connect_expressions_do_not_require_remote_environment_flag(monkeypatch):
    from pyspark.sql.connect.column import Column
    from pyspark_pecan.algorithms import F

    monkeypatch.delenv("SPARK_CONNECT_MODE_ENABLED", raising=False)
    assert isinstance(F.col("id"), Column)
    assert isinstance(F.sum(F.col("rank")), Column)
    assert isinstance(F.lit(0.15), Column)


@pytest.mark.parametrize("capabilities,version", [([], 1), (["fs"], 1), (["fs", "owned_runs_v1"], 2)])
def test_required_server_capabilities(monkeypatch, capabilities, version):
    monkeypatch.setattr(GraphUtils, "_request", lambda *_: [Row(
        kind="pong", capabilities=json.dumps(capabilities), protocol_version=version,
        path="file:///root", engine="test", lease_seconds=0,
    )])
    with pytest.raises(CapabilityError, match="requires protocol 1"):
        GraphUtils(None)


def test_missing_service_fails_before_graph_submission(monkeypatch):
    def unavailable(*_):
        raise RuntimeError("unknown relation")
    monkeypatch.setattr(GraphUtils, "_request", unavailable)
    with pytest.raises(CapabilityError, match="Ping failed") as error:
        GraphUtils(None)
    assert str(error.value.__cause__) == "unknown relation"


def test_relation_uses_versioned_protobuf_without_input_plans():
    request = wire.Request(mkdir=wire.Mkdir(request_id="dd0cf6ef-b8a0-4c48-a426-754afc02d1a8"))
    relation = _UtilsRelation(request).plan(None)
    assert relation.WhichOneof("rel_type") == "extension"
    assert relation.extension.type_url == TYPE_URL
    assert wire.Request.FromString(relation.extension.value) == request


def test_remove_receipt_count_is_a_column_not_tuple_method(monkeypatch):
    utils = GraphUtils.__new__(GraphUtils)
    monkeypatch.setattr(utils, "_request", lambda _: [Row(kind="rm", count=7)])
    assert utils.remove("file:///owned/run", "token") == 7


@pytest.mark.parametrize("kwargs", [
    {"max_iterations": 0}, {"max_iterations": True}, {"partitions": -1},
    {"reset_probability": 0}, {"reset_probability": float("nan")},
    {"tolerance": float("inf")}, {"tolerance": -1},
])
def test_invalid_pagerank_options_fail_before_server_execution(kwargs):
    graph = GraphAlgorithms.__new__(GraphAlgorithms)
    graph.spark = None
    with pytest.raises(ValueError):
        graph.pagerank(None, None, **kwargs)


@pytest.mark.parametrize("kwargs", [{"max_iterations": 0}, {"max_iterations": True}, {"partitions": 0}])
def test_invalid_wcc_options_fail_before_server_execution(kwargs):
    graph = GraphAlgorithms.__new__(GraphAlgorithms)
    graph.spark = None
    with pytest.raises(ValueError):
        graph.wcc(None, None, **kwargs)
