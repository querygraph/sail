"""The former import paths resolve to Pecan without duplicating protocol types."""

import importlib
import pickle

import pytest

from pyspark_pecan import GraphAlgorithms, GraphUtils
from pyspark_pecan import utils_pb2 as wire


def test_old_top_level_import_preserves_public_objects():
    import pyspark_graph_algorithms as previous

    assert previous.GraphAlgorithms is GraphAlgorithms
    assert previous.GraphUtils is GraphUtils


@pytest.mark.parametrize("name", ["algorithms", "lifecycle", "staging", "utils", "utils_pb2"])
def test_old_submodule_import_is_the_same_module(name):
    previous = importlib.import_module(f"pyspark_graph_algorithms.{name}")
    canonical = importlib.import_module(f"pyspark_pecan.{name}")
    assert previous is canonical


def test_old_protobuf_import_preserves_wire_and_pickling():
    from pyspark_graph_algorithms.utils_pb2 import Ping, Request

    request = Request(ping=Ping(client_version="0.1.0"))
    assert Request is wire.Request
    assert Request.DESCRIPTOR.full_name == "gf.utils.v1.Request"
    assert wire.Request.FromString(request.SerializeToString()) == request
    assert pickle.loads(pickle.dumps(request)) == request
