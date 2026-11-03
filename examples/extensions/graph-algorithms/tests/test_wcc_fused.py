"""Fused planning preserves representative choices and removes staging work."""
from types import SimpleNamespace

import pytest
from pyspark.errors import PySparkException

from pyspark_pecan import GraphAlgorithms
from pyspark_pecan.staging import StagingRun


@pytest.mark.parametrize('method', ['randomized', 'randomized_fused'])
def test_missing_axpb_fails_before_input_execution(method):
    graph = GraphAlgorithms.__new__(GraphAlgorithms)
    graph.utils = SimpleNamespace(capabilities=set())
    with pytest.raises(ValueError, match='requires the axpb capability'):
        graph.wcc(None, None, method=method)


@pytest.mark.integration
def test_fused_preserves_each_representative_map_and_removes_priority_writes(spark, monkeypatch):
    # Mixed orientation and duplicates deliberately distinguish raw edge rows
    # from the original plan's initial canonical distinct edge generation.
    ids = list(range(-32, 32)) + [1000]
    links = [(i, i + 1) if i % 2 else (i + 1, i) for i in range(-32, 31)]
    links += [(i, i + 1) for i in range(-32, 31)] + [(0, 0), (1000, 1000)]
    vertices = spark.createDataFrame([(i,) for i in ids], 'id long')
    edges = spark.createDataFrame(links, 'src long,dst long')
    expected = {i: -32 if i != 1000 else 1000 for i in ids}
    original = StagingRun.materialize
    captures = {}
    for method in ('randomized', 'randomized_fused'):
        capture = dict(stages=0, representative_maps=[], priorities=0)
        def record(run, frame, **kwargs):
            path, stored = original(run, frame, **kwargs)
            capture['stages'] += 1
            if stored.columns == ['id', 'representative']:
                capture['representative_maps'].append({r.id: r.representative for r in stored.collect()})
            if stored.columns == ['id', 'priority']:
                capture['priorities'] += 1
            return path, stored
        with monkeypatch.context() as patch:
            patch.setattr(StagingRun, 'materialize', record)
            graph = GraphAlgorithms(spark)
            with graph.wcc(vertices, edges, method=method, seed=42, max_iterations=32) as result:
                assert {r.id: r.component for r in result.frame.collect()} == expected
                assert result.converged
                capture['iterations'] = result.iterations
                capture['contractions'] = result.contractions
                run_path, token = result._run.path, result._run.token
            with pytest.raises(PySparkException, match='graph run has been released'):
                graph.utils.exists(run_path, token)
            assert graph.utils.remove(run_path, token) == 0
        captures[method] = capture
    original, fused = captures['randomized'], captures['randomized_fused']
    assert original['iterations'] == fused['iterations'] > 1
    assert original['representative_maps'] == fused['representative_maps']
    assert original['priorities'] == original['iterations']
    assert fused['priorities'] == 0
    assert original['stages'] - fused['stages'] == original['iterations'] + 1
    assert original['contractions'][0]['edges_before'] == 63
    assert fused['contractions'][0]['edges_before'] == 126
    assert original['contractions'][1:] == fused['contractions'][1:]
    assert {k: v for k, v in original['contractions'][0].items() if k != 'edges_before'} == {
        k: v for k, v in fused['contractions'][0].items() if k != 'edges_before'}
