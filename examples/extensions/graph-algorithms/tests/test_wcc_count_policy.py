"""WCC omits a full vertex count while retaining its empty-result contract."""
from types import SimpleNamespace
from typing import Any

import pytest
from pyspark.sql.types import LongType, StructField, StructType

from pyspark_pecan import GraphAlgorithms, algorithms
from pyspark_pecan.lifecycle import CancellationToken


class SnapshotComplete(Exception):
    pass


@pytest.mark.parametrize('method', ['min_label', 'randomized', 'randomized_fused'])
def test_public_wcc_snapshots_without_counting_vertices(monkeypatch: Any, method: str) -> None:
    events: list[Any] = []
    spark = SimpleNamespace(addTag=lambda _: None, removeTag=lambda _: None)

    class Frame:
        def __init__(self, columns: list[str]) -> None:
            self.columns, self.sparkSession = columns, spark
            self.schema = StructType([StructField(name, LongType()) for name in columns])

        def select(self, *columns: str) -> Any:
            assert list(columns) == self.columns
            return self

        def count(self) -> int:
            pytest.fail('WCC must not count every vertex')

        def where(self, *args: Any) -> Any:
            pytest.fail('snapshot must not issue an input audit')

    class Run:
        path, write_uncertain = '/owned/run', False

        def __init__(self, _spark: Any, _utils: Any, cancellation: Any, _partitions: int,
                     *, repartition_checkpoints: bool = True) -> None:
            assert repartition_checkpoints is True
            self.cancellation = cancellation

        def materialize(self, frame: Any) -> tuple[str, Any]:
            events.append(('snapshot', tuple(frame.columns)))
            return '/owned/stage', frame

        def close(self) -> None:
            events.append('closed')

    monkeypatch.setattr(algorithms, 'GraphUtils', lambda _: SimpleNamespace(capabilities={'axpb'}))
    monkeypatch.setattr(algorithms, 'StagingRun', Run)
    snapshot = algorithms._snapshot

    def inspect_snapshot(*args: Any, **options: Any) -> Any:
        assert options == dict(count_vertices=False, snapshot=True, vertex_columns=("id",))
        _, _, size = snapshot(*args, **options)
        assert size is None
        raise SnapshotComplete

    monkeypatch.setattr(algorithms, '_snapshot', inspect_snapshot)
    with pytest.raises(SnapshotComplete):
        GraphAlgorithms(spark).wcc(Frame(['id']), Frame(['src', 'dst']), method=method)
    assert events == [('snapshot', ('id',)), ('snapshot', ('src', 'dst')), 'closed']


@pytest.mark.parametrize('rows', [0, 3])
def test_min_label_empty_probe_is_bounded_and_preserves_zero_iterations(monkeypatch: Any, rows: int) -> None:
    events: list[str] = []

    class Frame:
        def select(self, *args: Any) -> Any: return self
        def unionByName(self, other: Any) -> Any: return self
        def distinct(self) -> Any: return self
        def withColumn(self, *args: Any) -> Any: return self

        def limit(self, value: int) -> Any:
            assert value == 1
            events.append('bounded_empty_probe')
            return SimpleNamespace(count=lambda: min(rows, 1))

        def count(self) -> int:
            pytest.fail('empty detection must not count every label')

    frame = Frame()
    run = SimpleNamespace(cancellation=CancellationToken(),
        materialize=lambda value: ('/owned/labels', value),
        finish=lambda path, value, **metadata: metadata)
    monkeypatch.setattr(algorithms, 'GraphUtils', lambda _: object())
    graph = GraphAlgorithms(object())

    def execute(vertices: Any, edges: Any, partitions: int, cancellation: Any,
                body: Any, *, count_vertices: bool) -> Any:
        assert count_vertices is False
        return body(run, vertices, edges, None)

    def iteration_started(*args: Any, **kwargs: Any) -> None:
        raise SnapshotComplete

    monkeypatch.setattr(graph, '_run', execute)
    monkeypatch.setattr(graph, '_observe', iteration_started)
    if rows:
        with pytest.raises(SnapshotComplete):
            graph.wcc(frame, frame)
    else:
        assert graph.wcc(frame, frame) == dict(algorithm='wcc-min-label', iterations=0, converged=True)
    assert events == ['bounded_empty_probe']
