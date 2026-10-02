"""The opt-in writer path preserves the checkpoint publication contract."""
from types import SimpleNamespace

import pytest
from pyspark.sql.types import LongType, StringType, StructField, StructType

from pyspark_pecan import CancellationToken, GraphAlgorithms
from pyspark_pecan import algorithms
from pyspark_pecan.staging import StagingRun


SCHEMA = StructType([StructField("id", LongType(), False)])


class Frame:
    def __init__(self, calls, *, label="input", schema=SCHEMA, rows=3, failure=None):
        self.calls, self.label, self.schema = calls, label, schema
        self.rows, self.failure = rows, failure

    def repartition(self, partitions):
        self.calls.append(("repartition", partitions))
        return Frame(self.calls, label="repartitioned", schema=self.schema,
                     rows=self.rows, failure=self.failure)

    @property
    def write(self):
        self.calls.append(("writer", self.label))
        return self

    def mode(self, mode):
        assert mode == "error"
        return self

    def parquet(self, path):
        self.calls.append(("write", path))
        if self.failure:
            raise self.failure

    def count(self):
        self.calls.append(("count", self.rows))
        return self.rows


class Reader:
    def __init__(self, stored):
        self.stored = stored
        self.explicit_schema = None

    def schema(self, schema):
        self.explicit_schema = schema
        return self

    def parquet(self, path):
        self.stored.calls.append(("read", path))
        if self.explicit_schema is not None:
            self.stored.schema = self.explicit_schema
        return self.stored


def staging(stored, **options):
    utils = SimpleNamespace(allocate=lambda: ("file:///owned/run", "token"),
                            exists=lambda *_: True)
    session = SimpleNamespace(read=Reader(stored))
    return StagingRun(session, utils, CancellationToken(), 7, **options)


@pytest.mark.parametrize("value", [None, 0, 1, "false", [], {}])
def test_invalid_setting_is_rejected_before_graph_utils_ping(monkeypatch, value):
    def unexpected_ping(_):
        pytest.fail("invalid configuration must not issue a server request")

    monkeypatch.setattr(algorithms, "GraphUtils", unexpected_ping)
    with pytest.raises(ValueError, match="repartition_checkpoints"):
        GraphAlgorithms(object(), repartition_checkpoints=value)


@pytest.mark.parametrize("options,expected", [({}, True), ({"repartition_checkpoints": True}, True),
                                              ({"repartition_checkpoints": False}, False)])
def test_constructor_default_and_explicit_settings(monkeypatch, options, expected):
    monkeypatch.setattr(algorithms, "GraphUtils", lambda _: object())
    assert GraphAlgorithms(object(), **options).repartition_checkpoints is expected


@pytest.mark.parametrize("options,repartition", [({}, True), ({"repartition_checkpoints": True}, True),
                                                 ({"repartition_checkpoints": False}, False)])
def test_public_controller_propagates_setting_to_snapshot_and_body_writes(monkeypatch, options, repartition):
    calls = []
    session = SimpleNamespace(read=Reader(Frame(calls)), addTag=lambda _: None, removeTag=lambda _: None)
    utils = SimpleNamespace(allocate=lambda: ("file:///owned/run", "token"), exists=lambda *_: True)
    monkeypatch.setattr(algorithms, "GraphUtils", lambda _: utils)
    monkeypatch.setattr(algorithms, "_check_input_schema", lambda *_: None)

    def snapshot(run, *_, count_vertices=True, snapshot=True, vertex_columns=("id",)):
        run.materialize(Frame(calls))
        run.materialize(Frame(calls))
        return None, None, 3

    def body(run, _vertices, _edges, size):
        return run.materialize(Frame(calls))

    monkeypatch.setattr(algorithms, "_snapshot", snapshot)
    graph = GraphAlgorithms(session, **options)
    graph._run(None, None, 7, None, body)
    assert calls.count(("repartition", 7)) == (3 if repartition else 0)
    assert [value for kind, value in calls if kind == "writer"] == [
        "repartitioned" if repartition else "input"] * 3


@pytest.mark.parametrize("options,writer", [({}, "repartitioned"),
    ({"repartition_checkpoints": True}, "repartitioned"),
    ({"repartition_checkpoints": False}, "input")])
def test_only_selected_frame_is_written_and_committed(options, writer):
    calls = []
    source, stored = Frame(calls), Frame(calls, label="stored")
    run = staging(stored, **options)
    path, result = run.materialize(source)
    assert result is stored and run._stages[path] is stored
    assert run.write_uncertain is False
    assert calls == ([('repartition', 7)] if writer == "repartitioned" else []) + [
        ("writer", writer), ("write", path), ("read", path)]


@pytest.mark.parametrize("repartition", [True, False])
def test_empty_checkpoint_restores_schema_without_row_audit(repartition):
    calls = []
    source = Frame(calls, rows=0)
    stored = Frame(calls, schema=StructType([]), rows=0)
    run = staging(stored, repartition_checkpoints=repartition)
    path, result = run.materialize(source)
    assert result.schema == SCHEMA and run._stages[path] is stored
    assert calls.count(("read", path)) == 2
    assert not any(kind == "count" for kind, _ in calls)


@pytest.mark.parametrize("repartition", [True, False])
def test_nullability_widening_is_allowed_without_changing_columns(repartition):
    calls = []
    widened = StructType([StructField("id", LongType(), True)])
    stored = Frame(calls, schema=widened)
    run = staging(stored, repartition_checkpoints=repartition)
    assert run.materialize(Frame(calls))[1] is stored
    assert not any(kind == "count" for kind, _ in calls)


@pytest.mark.parametrize("repartition", [True, False])
@pytest.mark.parametrize("schema", [StructType([StructField("other", LongType())]),
                                   StructType([StructField("id", StringType())])])
def test_changed_column_name_or_type_is_not_committed(repartition, schema):
    run = staging(Frame([], schema=schema), repartition_checkpoints=repartition)
    with pytest.raises(RuntimeError, match="changed its schema"):
        run.materialize(Frame([]))
    assert list(run._stages.values()) == [None]
    assert run.write_uncertain is False


@pytest.mark.parametrize("repartition", [True, False])
def test_materialize_never_counts_rows(repartition: bool) -> None:
    calls = []
    stored = Frame(calls, rows=2)
    run = staging(stored, repartition_checkpoints=repartition)
    path, result = run.materialize(Frame(calls))
    assert result is stored and run._stages[path] is stored
    assert not any(kind == "count" for kind, _ in calls)


@pytest.mark.parametrize("repartition", [True, False])
def test_failed_writer_retains_uncommitted_stage_ownership(repartition):
    failure = RuntimeError("writer outcome unknown")
    calls = []
    run = staging(Frame(calls), repartition_checkpoints=repartition)
    with pytest.raises(RuntimeError) as raised:
        run.materialize(Frame(calls, failure=failure))
    assert raised.value is failure
    assert run._stages == {"file:///owned/run/stage-00000": None}
    assert run.write_uncertain is True
    assert not any(kind == "read" for kind, _ in calls)
