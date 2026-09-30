"""Recorded identity audit controls using the real tiny fixture writers."""
from copy import deepcopy
import hashlib

import pytest

from certificate_identity import certificate_dataset_errors
import imported_snap_fixture
import imported_traversal_fixture
import traversal_fixture


@pytest.fixture
def imported(tmp_path):
    path = tmp_path / 'input.edges'
    path.write_bytes(b'6 6\n0 1\n0 1\n1 1\n1 2\n3 4\n4 3\n')
    digest = hashlib.sha256(path.read_bytes()).hexdigest()
    options = dict(family='edge-list-traversal', vertices=6, edge_file=str(path),
                   edge_sha256=digest, weight_policy='unit', chunk_edges=2,
                   chunk_vertices=2, validation='certificate', certificate_max_rounds=123)
    arguments = {k: v for k, v in options.items() if k not in
                 ('family', 'validation', 'certificate_max_rounds')}
    manifest = imported_traversal_fixture.prepare(tmp_path / 'data', **arguments)
    return manifest, options


def graph500():
    # Matches graph500_fixture.prepare's manifest layout without building or
    # invoking the external generator. Eight vertices, sixteen edge records.
    options = dict(family='graph500', vertices=8, scale=3, generator='/tools/generator',
                   edge_factor=2, source='max-degree', validation='certificate',
                   certificate_max_rounds=42, expected_edge_sha256='a' * 64)
    parameters = dict(scale=3, edge_factor=2, seed1=42, seed2=54, source='max-degree',
                      directed=False, chunk_edges=262144, chunk_vertices=262144,
                      reference=False, expected_edge_sha256='a' * 64)
    manifest = dict(schema_version=1, family='graph500', parameters=parameters,
                    counts=dict(vertices=8, edges=16),
                    traversal=dict(source=3, directed=False,
                                   source_policy='highest-degree vertex, lowest id among ties; not Graph500 root sampling'),
                    generator=dict(argv=['/tools/generator', '3', '2', '42', '54', '262144']),
                    canonical=dict(vertices=dict(sha256='b' * 64, bytes=64),
                                   edges=dict(sha256='a' * 64, bytes=384)),
                    files={f'{name}.parquet/part-00000000.parquet': dict(sha256='c' * 64, bytes=100)
                           for name in ('vertices', 'edges')})
    return manifest, options


def test_partitioned_import_and_optional_reference(imported):
    manifest, options = imported
    assert len(manifest['files']) == 6
    assert certificate_dataset_errors(manifest, options) == []
    manifest['files']['reference.parquet'] = dict(sha256='d' * 64, bytes=100)
    assert certificate_dataset_errors(manifest, options) == []
    options['expected_edge_sha256'] = manifest['canonical']['edges']['sha256']
    manifest['parameters']['expected_edge_sha256'] = options['expected_edge_sha256']
    assert certificate_dataset_errors(manifest, options) == []


@pytest.mark.parametrize('source', [0, 'max-degree'])
def test_real_snap_resolved_source_and_optional_canonical_pin(tmp_path, source):
    path = tmp_path / 'snap.txt'
    path.write_bytes(b'# SNAP fixture\n40 10\n40 20\n10 10\n20 40\n60 40\n20 40\n')
    digest = hashlib.sha256(path.read_bytes()).hexdigest()
    options = dict(family='snap-edge-list', vertices=4, edge_file=str(path),
                   edge_sha256=digest, weight_policy='unit', source=source,
                   chunk_edges=2, chunk_vertices=2, validation='certificate')
    arguments = {k: v for k, v in options.items() if k not in ('family', 'validation')}
    manifest = imported_snap_fixture.prepare(tmp_path / 'snap', **arguments)
    options['expected_edge_sha256'] = manifest['canonical']['edges']['sha256']
    assert certificate_dataset_errors(manifest, options) == []
    manifest['traversal']['source_policy'] = 'unrecorded'
    assert any('source policy' in error for error in certificate_dataset_errors(manifest, options))


def test_real_bounded_traversal_flat_inventory(tmp_path):
    manifest = traversal_fixture.prepare(tmp_path / 'tiny', vertices=8)
    options = dict(family='traversal', vertices=8)
    assert certificate_dataset_errors(manifest, options) == []
    manifest['seed'] = 43
    assert 'dataset configuration differs: seed' in certificate_dataset_errors(manifest, options)


def test_graph500_shape_defaults_and_policy():
    manifest, options = graph500()
    assert certificate_dataset_errors(manifest, options) == []
    manifest['parameters']['seed1'] = 43
    assert 'dataset configuration differs: seed1' in certificate_dataset_errors(manifest, options)


@pytest.mark.parametrize('path,value,diagnostic', [
    (('family',), 'snap-edge-list', 'family'),
    (('counts', 'vertices'), 7, 'vertices'),
    (('counts', 'edges'), -1, 'counts'),
    (('parameters', 'vertices'), 7, 'parameters.vertices'),
    (('parameters', 'weight_seed'), 43, 'weight_seed'),
    (('parameters', 'directed'), 1, 'directed'),
    (('parameters', 'source'), 1, 'source'),
    (('parameters', 'chunk_edges'), 3, 'chunk_edges'),
    (('input', 'sha256'), 'e' * 64, 'edge_sha256'),
    (('input', 'path'), '/different/input.edges', 'edge_file'),
    (('input', 'bytes'), True, 'byte count'),
    (('canonical', 'edges', 'sha256'), 'invalid', 'canonical edges hash'),
    (('canonical', 'vertices', 'bytes'), 47, 'canonical.vertices.bytes'),
    (('traversal', 'weight_policy'), 'splitmix64-1-16', 'traversal.weight_policy'),
    (('traversal', 'source'), 6, 'source'),
    (('traversal', 'source_policy'), 'highest-degree vertex', 'source policy'),
])
def test_configuration_and_internal_identity_corruption_rejected(imported, path, value, diagnostic):
    manifest, options = imported
    target = manifest
    for key in path[:-1]:
        target = target[key]
    target[path[-1]] = value
    assert any(diagnostic in error for error in certificate_dataset_errors(manifest, options))


@pytest.mark.parametrize('name', [
    '/edges.parquet/part-00000000.parquet', '../edges.parquet',
    'edges.parquet/../part-00000000.parquet', 'edges.parquet//part-00000000.parquet',
    'edges.parquet/./part-00000000.parquet', 'edges.parquet\\part-00000000.parquet',
    'unlisted.parquet', 'reference.parquet/part-00000000.parquet',
])
def test_unsafe_or_invalid_inventory_paths_rejected(imported, name):
    manifest, options = imported
    manifest['files'][name] = dict(sha256='a' * 64, bytes=100)
    assert certificate_dataset_errors(manifest, options)


@pytest.mark.parametrize('change', ['missing_edges', 'missing_partition', 'extra_partition',
                                    'flat_and_partitioned', 'bad_hash', 'bad_size'])
def test_inventory_shape_and_hash_corruption_rejected(imported, change):
    manifest, options = imported
    files = manifest['files']
    first = 'edges.parquet/part-00000000.parquet'
    if change == 'missing_edges':
        manifest['files'] = {k: v for k, v in files.items() if k.startswith('vertices')}
    elif change == 'missing_partition':
        del files[first]
    elif change == 'extra_partition':
        files['edges.parquet/part-00000003.parquet'] = deepcopy(files[first])
    elif change == 'flat_and_partitioned':
        files['edges.parquet'] = deepcopy(files[first])
    elif change == 'bad_hash':
        files[first]['sha256'] = 'A' * 64
    else:
        files[first]['bytes'] = -1
    assert certificate_dataset_errors(manifest, options)


@pytest.mark.parametrize('section', ['files', 'counts', 'parameters', 'traversal', 'canonical', 'input'])
def test_malformed_sections_return_errors(imported, section):
    manifest, options = imported
    manifest[section] = []
    assert certificate_dataset_errors(manifest, options)


def test_graph500_count_generator_and_canonical_pin_corruption():
    manifest, options = graph500()
    for path, value, diagnostic in [
        (('counts', 'edges'), 17, 'edge factor/count'),
        (('generator', 'argv'), ['/wrong/generator'], 'generator'),
        (('canonical', 'edges', 'sha256'), 'f' * 64, 'expected_edge_sha256'),
        (('traversal', 'source_policy'), 'explicit fixed vertex', 'source policy'),
    ]:
        changed = deepcopy(manifest)
        changed[path[0]][path[1]] = value
        assert any(diagnostic in error for error in certificate_dataset_errors(changed, options))
