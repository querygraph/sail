"""A failed write RPC must not trigger deletion while writers may still run."""

import pytest
from pyspark.sql.types import LongType, StructField, StructType

from pyspark_pecan import CancellationToken, GraphAlgorithms, GraphCancelledError
from pyspark_pecan import algorithms


class ReceiptStore:
    def __init__(self):
        self.removed = []

    def allocate(self):
        return "file:///staging/owned-run", "run-token"

    def exists(self, *_):
        return True

    def remove(self, path, token):
        self.removed.append((path, token))
        return 1


class Frame:
    schema = StructType([StructField("id", LongType())])

    def __init__(self, write):
        self._write = write

    def repartition(self, _):
        return self

    @property
    def write(self):
        return self

    def mode(self, _):
        return self

    def parquet(self, path):
        self._write(path)


class Session:
    def __init__(self, stored):
        self.stored = stored
        self.interrupted = []

    def addTag(self, _):
        pass

    def removeTag(self, _):
        pass

    def interruptTag(self, tag):
        self.interrupted.append(tag)

    @property
    def read(self):
        return self

    def parquet(self, _):
        return self.stored


def controller(monkeypatch, frame, repartition_checkpoints):
    store = ReceiptStore()
    monkeypatch.setattr(algorithms, "GraphUtils", lambda _: store)
    graph = GraphAlgorithms(Session(frame), repartition_checkpoints=repartition_checkpoints)
    monkeypatch.setattr(algorithms, "_check_input_schema", lambda *_: None)

    def snapshot(run, *_, count_vertices=True, snapshot=True):
        run.materialize(frame)
        return None, None, 1

    monkeypatch.setattr(algorithms, "_snapshot", snapshot)
    return graph


@pytest.mark.parametrize("cancelled", [False, True])
@pytest.mark.parametrize("repartition_checkpoints", [True, False])
def test_failed_write_retains_owned_run_and_preserves_cause(monkeypatch, cancelled, repartition_checkpoints):
    token = CancellationToken()
    failure = RuntimeError("write RPC did not complete")

    def write(_):
        if cancelled:
            token.cancel()
        raise failure

    graph = controller(monkeypatch, Frame(write), repartition_checkpoints)
    with pytest.raises(GraphCancelledError if cancelled else RuntimeError) as raised:
        graph._run(None, None, 1, token, lambda *_: None)
    assert graph.utils.removed == []
    assert raised.value.cleanup_deferred is True
    assert raised.value.run_path == "file:///staging/owned-run"
    if cancelled:
        assert raised.value.__cause__ is failure
        assert len(graph.spark.interrupted) == 1
    else:
        assert raised.value is failure


@pytest.mark.parametrize("repartition_checkpoints", [True, False])
def test_algorithm_failure_after_completed_write_can_remove_run(monkeypatch, repartition_checkpoints):
    graph = controller(monkeypatch, Frame(lambda _: None), repartition_checkpoints)
    failure = ValueError("algorithm failed after snapshot")

    def fail_body(*_):
        raise failure

    with pytest.raises(ValueError) as raised:
        graph._run(None, None, 1, CancellationToken(), fail_body)
    assert raised.value is failure
    assert raised.value.cleanup_deferred is False
    assert graph.utils.removed == [("file:///staging/owned-run", "run-token")]


@pytest.mark.parametrize("repartition_checkpoints", [True, False])
def test_cancellation_after_successful_write_can_remove_run(monkeypatch, repartition_checkpoints):
    token = CancellationToken()
    graph = controller(monkeypatch, Frame(lambda _: token.cancel()), repartition_checkpoints)
    with pytest.raises(GraphCancelledError) as raised:
        graph._run(None, None, 1, token, lambda *_: None)
    assert raised.value.cleanup_deferred is False
    assert graph.utils.removed == [("file:///staging/owned-run", "run-token")]


@pytest.mark.parametrize("repartition_checkpoints", [True, False])
def test_cancellation_during_heartbeat_prevents_following_write(monkeypatch, repartition_checkpoints):
    token = CancellationToken()
    submitted = []
    graph = controller(monkeypatch, Frame(submitted.append), repartition_checkpoints)

    def heartbeat(*_):
        token.cancel()
        return True

    monkeypatch.setattr(graph.utils, "exists", heartbeat)
    with pytest.raises(GraphCancelledError) as raised:
        graph._run(None, None, 1, token, lambda *_: None)
    assert submitted == []
    assert raised.value.cleanup_deferred is False
    assert graph.utils.removed == [("file:///staging/owned-run", "run-token")]
