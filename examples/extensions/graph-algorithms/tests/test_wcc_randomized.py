"""Contraction invariants, full-width priorities, and real distributed results."""
import pytest
from pyspark.errors import PySparkException

from pyspark_pecan import CancellationToken, ConvergenceError, GraphAlgorithms, GraphCancelledError
from pyspark_pecan.wcc_randomized import MASK, SplitMix64, signed


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
    with pytest.raises(ValueError, match='unsigned 64-bit'):
        SplitMix64(seed)


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
        assert result.algorithm == ('wcc-randomized-fused-contraction' if method == 'randomized_fused'
                                    else 'wcc-randomized-contraction')
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
        assert result.contractions[0]['edges_before'] == size - 1
        assert result.contractions[-1]['edges_after'] == 0
        assert all(step['edges_after'] < step['edges_before'] for step in result.contractions)


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
        if event['kind'] == 'iteration_end':
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
