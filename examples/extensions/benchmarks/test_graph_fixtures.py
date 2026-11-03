"""No Sail, Spark, or Java is required for fixture/reference tests."""

import hashlib
import json

import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from graph_fixtures import generate, pagerank_reference, prepare, wcc_reference


def test_reference_matches_independent_linear_system():
    # Parallel edges, a self-loop, a dangling target, an isolate, and a separate
    # component. Solve the stationary equations independently of iteration.
    source = np.array([0, 0, 0, 1, 2, 4], dtype=np.int64)
    target = np.array([1, 1, 2, 1, 3, 4], dtype=np.int64)
    count, damping = 6, 0.85
    transition = np.zeros((count, count), dtype=np.float64)
    for node in range(count):
        destinations = target[source == node]
        if len(destinations):
            for destination in destinations:
                transition[destination, node] += 1 / len(destinations)
        else:
            transition[:, node] = 1 / count
    expected = np.linalg.solve(np.eye(count) - damping * transition, np.full(count, (1 - damping) / count))
    actual = pagerank_reference(count, source, target, tolerance=1e-13)
    np.testing.assert_allclose(actual.scores, expected, rtol=0, atol=1e-12)
    assert actual.residual <= 1e-13
    assert actual.iterations > 1
    assert actual.scores.sum() == pytest.approx(1, abs=1e-13)
    np.testing.assert_array_equal(wcc_reference(count, source, target), [0, 0, 0, 0, 4, 5])


def test_wcc_labels_are_numeric_minimum_independent_of_union_order():
    source = np.array([9, 10, 8, 3, 3, 5], dtype=np.int64)
    target = np.array([10, 8, 7, 5, 5, 5], dtype=np.int64)
    expected = [0, 1, 2, 3, 4, 3, 6, 7, 7, 7, 7]
    np.testing.assert_array_equal(wcc_reference(11, source, target), expected)
    np.testing.assert_array_equal(wcc_reference(11, target[::-1], source[::-1]), expected)


def test_sparse_fixture_is_reproducible_and_has_disclosed_structure():
    source, target = generate(240, seed=42, block_size=64)
    same = generate(240, seed=42, block_size=64)
    different = generate(240, seed=43, block_size=64)
    assert np.array_equal(source, same[0]) and np.array_equal(target, same[1])
    assert not np.array_equal(source, different[0])
    assert len(source) == 240 * 8
    assert source.dtype == target.dtype == np.dtype("int64")
    assert np.all(source // 64 == target // 64)
    assert np.count_nonzero(source == target) >= 4
    assert len(np.unique(source * 240 + target)) < len(source)
    outdegree = np.bincount(source, minlength=240)
    indegree = np.bincount(target, minlength=240)
    assert np.count_nonzero((outdegree == 0) & (indegree == 0)) == 12
    assert np.count_nonzero(outdegree == 0) > 12
    assert outdegree.max() > 4 * outdegree.mean()
    labels = wcc_reference(240, source, target)
    expected = np.arange(240, dtype=np.int64)
    expected[:228] = expected[:228] // 64 * 64
    np.testing.assert_array_equal(labels, expected)


def test_chain_is_a_directed_path_with_known_components():
    source, target = generate(9, family="chain")
    np.testing.assert_array_equal(source, np.arange(8))
    np.testing.assert_array_equal(target, np.arange(1, 9))
    np.testing.assert_array_equal(wcc_reference(9, source, target), np.zeros(9, dtype=np.int64))
    actual = pagerank_reference(9, source, target)
    assert actual.iterations > 1
    assert np.all(np.diff(actual.scores) > 0)


def test_prepare_writes_typed_hashed_complete_reference(tmp_path):
    manifest = prepare(tmp_path / "first", vertices=80, seed=17, block_size=32)
    repeated = prepare(tmp_path / "second", vertices=80, seed=17, block_size=32)
    assert manifest == repeated
    assert json.loads((tmp_path / "first" / "manifest.json").read_text()) == manifest
    schemas = {
        "vertices.parquet": pa.schema([("id", pa.int64())]),
        "edges.parquet": pa.schema([("src", pa.int64()), ("dst", pa.int64())]),
        "reference.parquet": pa.schema([("id", pa.int64()), ("pagerank", pa.float64()), ("component", pa.int64())]),
    }
    for name, expected in schemas.items():
        path = tmp_path / "first" / name
        assert pq.read_schema(path) == expected
        assert hashlib.sha256(path.read_bytes()).hexdigest() == manifest["files"][name]["sha256"]
        assert path.stat().st_size == manifest["files"][name]["bytes"]
    reference = pq.read_table(tmp_path / "first" / "reference.parquet")
    np.testing.assert_array_equal(reference["id"].to_numpy(), np.arange(80))
    assert reference.num_rows == manifest["counts"]["vertices"]
    assert manifest["pagerank"]["converged"] is True
    assert manifest["pagerank"]["residual"] <= manifest["pagerank"]["tolerance"]
    assert manifest["counts"]["isolates"] == 4
    with pytest.raises(FileExistsError, match="absent or empty"):
        prepare(tmp_path / "first", vertices=80)


def test_exhausted_reference_does_not_publish_a_fixture(tmp_path):
    with pytest.raises(RuntimeError, match="did not converge"):
        prepare(tmp_path / "incomplete", vertices=30, family="chain", max_iterations=1)
    assert not (tmp_path / "incomplete").exists()


@pytest.mark.parametrize("arguments", [
    {"vertices": 0}, {"vertices": True}, {"vertices": 10, "degree": -1},
    {"vertices": 10, "degree": float("nan")}, {"vertices": 10, "degree": 0},
    {"vertices": 10, "block_size": 1}, {"vertices": 10, "seed": -1},
    {"vertices": 10, "family": "unknown"},
])
def test_generation_rejects_invalid_parameters(arguments):
    with pytest.raises(ValueError):
        generate(**arguments)


def test_reference_rejects_out_of_range_endpoints():
    with pytest.raises(ValueError, match="outside"):
        pagerank_reference(2, np.array([0]), np.array([2]))


def test_empty_reference_is_defined():
    empty = np.empty(0, dtype=np.int64)
    actual = pagerank_reference(0, empty, empty)
    assert actual.iterations == 0 and actual.residual == 0 and actual.scores.size == 0
    assert wcc_reference(0, empty, empty).size == 0
