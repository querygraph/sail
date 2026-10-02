"""Distance and parent-tree checks, including zero-weight cycles and ties."""
import pytest
from pyspark.sql.connect.dataframe import DataFrame
from pyspark_pecan.staging import StagingRun
from pyspark_pecan import CancellationToken, GraphAlgorithms, GraphCancelledError, ConvergenceError
from pyspark_pecan.algorithms import physical_plan

pytestmark = pytest.mark.integration


def frames(spark):
    return (spark.createDataFrame([(i,) for i in [-7, 0, 1, 2, 3, 4, 5]], 'id long'),
            spark.createDataFrame([(0, 1, 8.), (0, 2, 1.), (2, 1, 1.), (1, 3, 0.),
                                   (3, 1, 0.), (3, 4, 2.), (0, 2, 1.), (4, 4, 0.)],
                                  'src long, dst long, weight double'))


@pytest.mark.parametrize('method', ['reference', 'frontier'])
@pytest.mark.parametrize('algorithm', ['bfs', 'sssp'])
def test_distances_and_acyclic_parents(spark, method, algorithm):
    v, e = frames(spark)
    with getattr(GraphAlgorithms(spark), algorithm)(v, e, source=0, method=method, partitions=2) as result:
        rows = {r.id: r for r in result.frame.collect()}
        expected = {0: 0., 1: 1., 2: 1., 3: 2., 4: 3., -7: None, 5: None} if algorithm == 'bfs' else {
            0: 0., 1: 2., 2: 1., 3: 2., 4: 4., -7: None, 5: None}
        assert {k: r.distance for k, r in rows.items()} == expected
        links = {(r.src, r.dst): (1. if algorithm == 'bfs' else r.weight) for r in e.collect()}
        for key, row in rows.items():
            if row.distance is None:
                assert row.parent is None and row.hops is None
                continue
            seen = set()
            while key != 0:
                assert key not in seen
                seen.add(key)
                row = rows[key]; parent = rows[row.parent]
                assert row.distance == parent.distance + links[(row.parent, key)]
                assert row.hops == parent.hops + 1
                key = row.parent
        assert rows[0].parent == 0 and rows[0].hops == 0


def test_undirected_and_iteration_cap(spark):
    v,e=frames(spark)
    graph=GraphAlgorithms(spark)
    with graph.bfs(v,e,source=4,directed=False,partitions=2) as r:
        assert {x.id:x.distance for x in r.frame.collect()}[0] == 3.
    with pytest.raises(ConvergenceError):
        graph.bfs(v,e,source=0,max_iterations=1)




@pytest.mark.parametrize('delta', [0.25, 1., 3., 100.])
def test_delta_star_bucket_closure(spark, delta):
    v,e=frames(spark)
    with GraphAlgorithms(spark).sssp(v,e,source=0,method='delta_star',delta=delta,partitions=2) as r:
        rows={x.id:x for x in r.frame.collect()}
        assert {k:x.distance for k,x in rows.items()} == {0:0.,1:2.,2:1.,3:2.,4:4.,-7:None,5:None}
        assert rows[1].parent == 2 and rows[3].parent == 1 and rows[4].parent == 3
        assert r.converged


def test_push_pull_distances_and_trace(spark):
    v,e=frames(spark)
    events=[]
    def observe(event):
        if event.kind == 'iteration_end':
            events.append(event.as_dict())
    with GraphAlgorithms(spark,observer=observe).bfs(v,e,source=0,method='push_pull',partitions=2) as r:
        assert {x.id:x.distance for x in r.frame.collect()} == {0:0.,1:1.,2:1.,3:2.,4:3.,-7:None,5:None}
        assert len(events) == r.iterations
        assert any(x.get('direction')=='pull' for x in events)
        assert all(x['pull_early_exit'] is False for x in events if x['kind']=='iteration_end')


@pytest.mark.parametrize('method', ['reference', 'frontier', 'delta_star'])
def test_weighted_relaxation_preserves_hop_then_signed_parent_ties(spark, method):
    vertices = spark.createDataFrame([(i,) for i in [-5, 0, 1, 2, 3, 4, 9]], 'id long')
    edges = spark.createDataFrame([
        (0, -5, 1.), (0, 1, 1.), (0, 2, 1.), (0, 3, 2.),
        (-5, 3, 1.), (1, 3, 1.), (2, 3, 1.),
        (-5, 4, 1.), (1, 4, 1.), (2, 4, 1.), (-5, -5, 0.),
    ], 'src long, dst long, weight double')
    with GraphAlgorithms(spark).sssp(vertices, edges, source=0, method=method,
                                    delta=1., partitions=2) as result:
        rows = {row.id: row for row in result.frame.collect()}
        assert (rows[3].distance, rows[3].hops, rows[3].parent) == (2., 1, 0)
        assert (rows[4].distance, rows[4].hops, rows[4].parent) == (2., 2, -5)
        assert rows[9].distance is None and rows[9].parent is None and rows[9].hops is None


@pytest.mark.parametrize('method', ['reference', 'frontier', 'delta_star'])
def test_weighted_round_executes_only_one_expansion_action(spark, monkeypatch, method):

    materialize, count = StagingRun.materialize, DataFrame.count
    adjacency_path = None
    edge_stages = 0
    expansion_actions = []

    def reads_adjacency(frame):
        return adjacency_path is not None and adjacency_path in str(frame._plan.to_proto(spark.client))

    def trace_materialize(run, frame, **kwargs):
        nonlocal adjacency_path, edge_stages
        if reads_adjacency(frame):
            expansion_actions.append('materialize')
        result = materialize(run, frame, **kwargs)
        if frame.columns == ['src', 'dst', 'weight']:
            edge_stages += 1
            # The edge write that snapshots the input is the traversal's
            # adjacency for a directed graph; no second copy is written.
            # Count only expansion jobs.
            assert edge_stages == 1
            adjacency_path = result[0]
        return result

    def trace_count(frame):
        if reads_adjacency(frame):
            expansion_actions.append('count')
        return count(frame)

    monkeypatch.setattr(StagingRun, 'materialize', trace_materialize)
    monkeypatch.setattr(DataFrame, 'count', trace_count)
    with GraphAlgorithms(spark).sssp(*frames(spark), source=0, method=method,
                                    delta=1., partitions=2) as result:
        assert result.frame.columns == ['id', 'distance', 'hops', 'parent']
        rows = {row.id: row for row in result.frame.collect()}
        assert {key: row.distance for key, row in rows.items()} == {
            0: 0., 1: 2., 2: 1., 3: 2., 4: 4., -7: None, 5: None}
        assert (rows[1].parent, rows[3].parent, rows[4].parent) == (2, 1, 3)
        assert adjacency_path is not None
        assert expansion_actions == ['materialize'] * result.iterations


@pytest.mark.parametrize('algorithm,method', [('bfs','reference'),('bfs','frontier'),
    ('bfs','push_pull'),('sssp','reference'),('sssp','frontier'),('sssp','delta_star')])
def test_cancelled_traversal_releases_owned_stages(spark, monkeypatch, algorithm, method):
    token=CancellationToken()
    events=[]
    def observe(event):
        events.append(event.as_dict())
        if event.kind=='iteration_end':
            token.cancel()
    graph=GraphAlgorithms(spark,observer=observe)
    allocated=[]
    allocate=graph.utils.allocate
    def track(**kwargs):
        owned=allocate(**kwargs)
        allocated.append(owned)
        return owned
    monkeypatch.setattr(graph.utils,'allocate',track)
    with pytest.raises(GraphCancelledError):
        getattr(graph,algorithm)(*frames(spark),source=0,method=method,
                                partitions=2,cancellation=token)
    assert [e['iteration'] for e in events if e['kind']=='iteration_end']==[1]
    assert len(allocated)==1 and graph.utils.remove(*allocated[0])==0


@pytest.mark.parametrize('algorithm,method', [('bfs','push_pull'),('sssp','delta_star')])
def test_isolated_source_and_partition_invariance(spark, algorithm, method):
    v,e=frames(spark)
    observed=[]
    for partitions in (1,3):
        with getattr(GraphAlgorithms(spark),algorithm)(v,e,source=-7,method=method,
                                                       partitions=partitions) as result:
            rows={r.id:r.asDict() for r in result.frame.collect()}
            assert rows[-7]==dict(id=-7,distance=0.,hops=0,parent=-7)
            assert all(r['distance'] is None and r['hops'] is None and r['parent'] is None
                       for key,r in rows.items() if key!=-7)
            observed.append(rows)
    assert observed[0]==observed[1]


@pytest.mark.parametrize('delta', [0., -1., float('nan'), float('inf'), True])
def test_invalid_bucket_width(spark, delta):
    with pytest.raises(ValueError,match='delta'):
        GraphAlgorithms(spark).sssp(*frames(spark),source=0,method='delta_star',delta=delta)


def test_recorded_plans_are_optional_and_taken_from_the_frame_the_iteration_materializes():

    class Frame:
        def _explain_string(self, extended=False):
            return "== Parsed Logical Plan ==\nx\n== Physical Plan ==\nHashJoinExec: mode=CollectLeft\n"

    class Run:
        path = "memory:///run"

    assert physical_plan(Frame()) == "== Physical Plan ==\nHashJoinExec: mode=CollectLeft\n"
    events = []
    graph = GraphAlgorithms.__new__(GraphAlgorithms)
    graph.observer, graph.record_plans = (lambda e: events.append(e.as_dict())), False
    graph._observe(Run(), "bfs", 1, "iteration_start", plan_of=Frame(), active_vertices=3)
    assert events[-1] == {"kind": "iteration_start", "algorithm": "bfs", "iteration": 1, "run_path": "memory:///run",
                          "active_vertices": 3}
    graph.record_plans = True
    graph._observe(Run(), "bfs", 2, "iteration_start", plan_of=Frame())
    assert events[-1]["plan"].startswith("== Physical Plan ==") and "HashJoinExec" in events[-1]["plan"]
    graph._observe(Run(), "bfs", 2, "iteration_end", active_vertices=0)
    assert "plan" not in events[-1]
