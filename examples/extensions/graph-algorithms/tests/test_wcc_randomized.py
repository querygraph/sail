"""Contraction invariants, field arithmetic shared with the server, and real distributed results."""
import pytest
from pyspark.errors import PySparkException

from pyspark_pecan import CancellationToken, ConvergenceError, GraphAlgorithms, GraphCancelledError
from pyspark_pecan.types import WccOptions
from pyspark_pecan.wcc_randomized import MASK, SplitMix64, gf_axpb, gf_multiply, signed, unsigned


def test_splitmix64_shared_native_vectors():
    random = SplitMix64(0)
    assert random.next() == 0xE220A8397B1DCDAF
    assert random.next() == 0x6E789E6AA1B965F4
    assert signed(1 << 63) == -(1 << 63)
    assert signed(MASK) == -1
    a, _ = SplitMix64(42).coefficients()
    assert a != 0


@pytest.mark.parametrize('seed', [-1, 1 << 64, True, 1.5, '42'])
def test_invalid_seed(seed):
    with pytest.raises(ValueError, match='seed'):
        WccOptions(seed=seed)


def _remainder(value, modulus):
    while value.bit_length() >= modulus.bit_length():
        value ^= modulus << (value.bit_length() - modulus.bit_length())
    return value


def test_priority_polynomial_is_a_field_not_a_hash_with_zero_divisors():
    # Rabin's irreducibility criterion for degree 64=2^6: x^(2^64)=x
    # modulo p, and gcd(x^(2^32)-x,p)=1. This is independent polynomial
    # arithmetic; it underpins unique priorities for every nonzero multiplier.
    modulus = (1 << 64) | 0x1B
    x = 2
    middle = None
    for exponent in range(1, 65):
        squared = sum(((x >> bit) & 1) << (2 * bit) for bit in range(x.bit_length()))
        x = _remainder(squared, modulus)
        if exponent == 32:
            middle = x ^ 2
    assert x == 2
    a, b = modulus, middle
    while b:
        a, b = b, _remainder(a, b)
    assert a == 1


def test_client_field_arithmetic_matches_vectors():
    # The multiplication the server's gf_axpb uses: x^64 reduces to 0x1b.
    assert gf_multiply(1 << 63, 2) == 0x1B
    assert gf_multiply(1, 0xDEADBEEF) == 0xDEADBEEF
    assert gf_axpb(1, 5, 3) == 6
    # Affine maps compose: (a2 (a1 x + b1) + b2) == (a2 a1) x + (a2 b1 + b2).
    a1, b1, a2, b2, x = 0x9E3779B97F4A7C15, 0x1234, 0xBF58476D1CE4E5B9, 0x5678, 0xCAFEBABE
    inner = gf_axpb(a1, x, b1)
    assert gf_axpb(a2, inner, b2) == gf_axpb(gf_multiply(a2, a1), x, gf_axpb(a2, b1, b2))
    assert signed(unsigned(-1)) == -1 and unsigned(-1) == MASK


@pytest.mark.integration
def test_client_field_arithmetic_matches_server(spark):
    from pyspark.sql.connect import functions as F
    a, b = SplitMix64(7).coefficients()
    xs = [-(1 << 63), -1, 0, 1, 42, (1 << 63) - 1]
    frame = spark.createDataFrame([(x,) for x in xs], 'x long')
    rows = frame.select('x', F.call_function('gf_axpb', F.lit(a).cast('long'), F.col('x'), F.lit(b).cast('long'))
                        .alias('y')).collect()
    assert {r.x: r.y for r in rows} == {x: signed(gf_axpb(a, x, b)) for x in xs}


@pytest.mark.integration
def test_hashed_labels_name_the_same_partition(spark):
    ids = list(range(40))
    links = [(i, i + 1) for i in range(0, 19)] + [(i, i + 1) for i in range(20, 39)] + [(5, 5)]
    graph = GraphAlgorithms(spark)
    with graph.wcc(*frames(spark, ids, links), method='randomized', canonical_labels=False, partitions=2) as raw:
        labels = {r.id: r.component for r in raw.frame.collect()}
    assert len(set(labels.values())) == 2
    assert len({labels[i] for i in range(20)}) == 1 and len({labels[i] for i in range(20, 40)}) == 1
    with graph.wcc(*frames(spark, ids, links), method='randomized', partitions=2) as canonical:
        assert {r.id: r.component for r in canonical.frame.collect()} == {i: 0 if i < 20 else 20 for i in ids}


def frames(spark, ids, links):
    return spark.createDataFrame([(i,) for i in ids], 'id long'), spark.createDataFrame(links, 'src long,dst long')


@pytest.mark.integration
@pytest.mark.parametrize('seed', [0, 42, MASK])
@pytest.mark.parametrize('method', ['randomized', 'randomized_fused'])
def test_signed_extreme_ids_isolates_duplicates_and_components(spark, seed, method):
    low, high = -(1 << 63), (1 << 63) - 1
    ids = [low, -12, -1, 0, 7, 10, 100, high]
    links = [(low, -12), (-12, 7), (7, 7), (-12, 7), (7, -12), (0, 10), (10, high), (100, 100)]
    expected = {low: low, -12: low, -1: -1, 0: 0, 7: low, 10: 0, 100: 100, high: 0}
    graph = GraphAlgorithms(spark)
    with graph.wcc(*frames(spark, ids, links), method=method, seed=seed, partitions=3) as result:
        assert {r.id: r.component for r in result.frame.collect()} == expected
        assert result.method == method
        assert result.algorithm == 'wcc-randomized-contraction'
        assert result.seed == seed
        assert result.converged


@pytest.mark.integration
@pytest.mark.parametrize('seed', [42, 123])
@pytest.mark.parametrize('method', ['randomized', 'randomized_fused'])
def test_chain_contracts_and_backpropagates_in_bounded_rounds(spark, seed, method):
    # A minimum-label implementation needs 512 rounds on this fixture. The
    # small bound exercises genuine contraction and its multi-round reverse pass.
    size = 512
    graph = GraphAlgorithms(spark)
    with graph.wcc(*frames(spark, list(range(size)), [(i, i + 1) for i in range(size - 1)]),
                   method=method, seed=seed, max_iterations=32, partitions=4) as result:
        assert result.frame.count() == size
        assert [r.component for r in result.frame.select('component').distinct().collect()] == [0]
        assert 1 < result.iterations < 32
        assert result.contractions[0].edges_before == size - 1
        assert result.contractions[-1].edges_after == 0
        assert all(step.edges_after < step.edges_before for step in result.contractions)


@pytest.mark.integration
@pytest.mark.parametrize('ids,links', [([], []), ([0, 1, 7], []), ([0, 1], [(0, 0), (1, 1)])])
@pytest.mark.parametrize('method', ['randomized', 'randomized_fused'])
def test_empty_edgeless_and_loop_only(spark, ids, links, method):
    with GraphAlgorithms(spark).wcc(*frames(spark, ids, links), method=method) as result:
        assert {r.id: r.component for r in result.frame.collect()} == {i: i for i in ids}
        assert result.iterations == 0


@pytest.mark.integration
@pytest.mark.parametrize('method', ['randomized', 'randomized_fused'])
def test_cancellation_and_cap_retain_cleanup_contract(spark, method, monkeypatch):
    from pyspark_pecan.utils import GraphUtils
    allocations = []
    allocate = GraphUtils.allocate
    def record_allocation(utils):
        run = allocate(utils)
        allocations.append(run)
        return run
    monkeypatch.setattr(GraphUtils, 'allocate', record_allocation)
    vertices, edges = frames(spark, list(range(128)), [(i, i + 1) for i in range(127)])
    token = CancellationToken()
    def observe(event):
        if event.kind == 'iteration_end':
            token.cancel()
    graph = GraphAlgorithms(spark, observer=observe)
    with pytest.raises(GraphCancelledError) as cancelled:
        graph.wcc(vertices, edges, method=method, cancellation=token)
    assert cancelled.value.cleanup_deferred is False
    # Released-run tombstones only authorize idempotent Rm, not Exists.
    with pytest.raises(PySparkException, match='graph run has been released'):
        graph.utils.exists(*allocations[-1])
    assert graph.utils.remove(*allocations[-1]) == 0
    graph = GraphAlgorithms(spark)
    with pytest.raises(ConvergenceError) as limited:
        graph.wcc(vertices, edges, method=method, max_iterations=1)
    assert limited.value.cleanup_deferred is False
    with pytest.raises(PySparkException, match='graph run has been released'):
        graph.utils.exists(*allocations[-1])
    assert graph.utils.remove(*allocations[-1]) == 0


@pytest.mark.integration
@pytest.mark.parametrize('canonical', [True, False])
def test_isolated_id_equal_to_a_hashed_label_stays_its_own_component(spark, canonical):
    # Found on the gate: with seed 42 the hashed label of component {1, 2} equals
    # this isolated vertex's original id. Isolates are labelled in the hashed
    # space, so the two components can never share a label.
    isolate = -7694170072594669674
    graph = GraphAlgorithms(spark)
    with graph.wcc(*frames(spark, [1, 2, isolate], [(1, 2)]), method='randomized', seed=42,
                   canonical_labels=canonical) as result:
        labels = {r.id: r.component for r in result.frame.collect()}
    assert labels[1] == labels[2] and labels[isolate] != labels[1]
    if canonical:
        assert labels == {1: 1, 2: 1, isolate: isolate}
