"""Audit recorded traversal dataset identities without reopening dataset files.

The producer hashes the live inventory. This consumer checks that the recorded
inventory, generation parameters, and configured pins form a consistent record.
"""
from pathlib import PurePosixPath
import re


FAMILIES = ('traversal', 'graph500', 'edge-list-traversal', 'snap-edge-list')
IMPORTED = ('edge-list-traversal', 'snap-edge-list')


def sha256_value(value):
    return isinstance(value, str) and re.fullmatch(r'[a-f0-9]{64}', value) is not None


def same(left, right):
    return type(left) is type(right) and left == right


def certificate_dataset_errors(manifest, options) -> list[str]:
    errors = []
    if not isinstance(manifest, dict) or not isinstance(options, dict):
        return ['dataset manifest or configuration is malformed']
    family = options.get('family')
    if family not in FAMILIES:
        return ['unsupported certificate dataset family']

    def section(name):
        value = manifest.get(name)
        if not isinstance(value, dict):
            errors.append(f'dataset {name} is missing or malformed')
            return {}
        return value

    def check(label, actual, expected):
        if not same(actual, expected):
            errors.append(f'dataset configuration differs: {label}')

    check('family', manifest.get('family'), family)
    if family != 'traversal':
        check('schema_version', manifest.get('schema_version'), 1)
    counts, parameters, traversal = (section(name) for name in ('counts', 'parameters', 'traversal'))
    vertices, edges = counts.get('vertices'), counts.get('edges')
    valid_vertices = type(vertices) is int and 0 < vertices < 1 << 63
    valid_edges = type(edges) is int and 0 <= edges < 1 << 63
    if not valid_vertices or not valid_edges:
        errors.append('dataset counts are missing or malformed')
    check('vertices', vertices, options.get('vertices'))

    defaults = dict(source=0, directed=family != 'graph500')
    if family == 'traversal':
        defaults.update(degree=8, seed=42)
    else:
        defaults.update(chunk_edges=262144, chunk_vertices=262144)
    if family == 'graph500':
        defaults.update(edge_factor=16, seed1=42, seed2=54)
    if family in IMPORTED:
        defaults.update(weight_seed=42)
    expected = dict(defaults, **options)
    for key, value in expected.items():
        # These govern verification, not the generated dataset identity.
        if key in ('family', 'vertices', 'validation', 'certificate_max_rounds',
                   'source', 'expected_edge_sha256'):
            continue
        if key == 'generator':
            generator = section('generator')
            argv = generator.get('argv')
            actual = argv[0] if isinstance(argv, list) and argv else None
        elif key == 'seed' and family == 'traversal':
            actual = manifest.get('seed')
            check('traversal.seed', traversal.get('seed'), value)
        else:
            actual = parameters.get(key)
        check(key, actual, value)

    directed = expected['directed']
    if type(traversal.get('directed')) is not bool:
        errors.append('dataset traversal direction is missing or malformed')
    check('traversal.directed', traversal.get('directed'), directed)
    source, requested = traversal.get('source'), expected['source']
    if not (valid_vertices and type(source) is int and 0 <= source < vertices):
        errors.append('dataset traversal source is outside its vertex range')
    if requested == 'max-degree':
        if family not in ('graph500', 'snap-edge-list'):
            errors.append('dataset source policy does not support max-degree')
        policy = traversal.get('source_policy')
        if not isinstance(policy, str) or not policy.startswith('highest-degree'):
            errors.append('dataset source policy differs: max-degree')
    else:
        check('traversal.source', source, requested)
        if family != 'traversal':
            policy = traversal.get('source_policy')
            if not isinstance(policy, str) or not policy.startswith('explicit fixed'):
                errors.append('dataset source policy differs: explicit source')
    # SNAP records the resolved dense source; Graph500 retains the request.
    check('source', parameters.get('source'), source if family == 'snap-edge-list' else requested)
    if family == 'graph500':
        scale, factor = parameters.get('scale'), parameters.get('edge_factor')
        if type(scale) is not int or not 1 <= scale <= 40 or vertices != 1 << scale:
            errors.append('dataset Graph500 scale/count differs')
        if type(factor) is not int or factor <= 0 or not valid_vertices or edges != vertices * factor:
            errors.append('dataset Graph500 edge factor/count differs')
    if family in IMPORTED:
        check('parameters.vertices', parameters.get('vertices'), vertices)
        imported = section('input')
        for key, field in (('edge_file', 'path'), ('edge_sha256', 'sha256')):
            check(key + ' input', imported.get(field), options.get(key))
        path = imported.get('path')
        if (not isinstance(path, str) or not PurePosixPath(path).is_absolute() or
                '..' in PurePosixPath(path).parts):
            errors.append('dataset input path is missing or malformed')
        if not sha256_value(imported.get('sha256')):
            errors.append('dataset input hash is missing or malformed')
        if type(imported.get('bytes')) is not int or imported['bytes'] < 0:
            errors.append('dataset input byte count is missing or malformed')
        for key in ('weight_policy', 'weight_seed'):
            check('traversal.' + key, traversal.get(key), expected.get(key))

    if family != 'traversal':
        canonical = section('canonical')
        for name, count, width in (('vertices', vertices, 8), ('edges', edges, 24)):
            identity = canonical.get(name)
            if not isinstance(identity, dict) or not sha256_value(identity.get('sha256')):
                errors.append(f'dataset canonical {name} hash is missing or malformed')
            elif type(count) is int:
                check('canonical.' + name + '.bytes', identity.get('bytes'), count * width)
        if 'expected_edge_sha256' in options:
            identity = canonical.get('edges')
            actual = identity.get('sha256') if isinstance(identity, dict) else None
            check('expected_edge_sha256', actual, options['expected_edge_sha256'])
        if family in ('graph500', 'edge-list-traversal'):
            check('reference', parameters.get('reference'), options.get('validation') == 'reference')
            check('expected_edge_sha256 parameter', parameters.get('expected_edge_sha256'),
                  options.get('expected_edge_sha256'))

    files = section('files')
    inventory = {name: set() for name in ('vertices.parquet', 'edges.parquet', 'reference.parquet')}
    for name, identity in files.items():
        path = PurePosixPath(name) if isinstance(name, str) else None
        if (path is None or not path.parts or path.is_absolute() or '..' in path.parts or
                '\\' in name or path.as_posix() != name or path.parts[0] not in inventory or
                not (len(path.parts) == 1 or (len(path.parts) == 2 and
                     re.fullmatch(r'part-[0-9]{8,}\.parquet', path.name)))):
            errors.append(f'dataset file path is malformed: {name!r}')
        else:
            inventory[path.parts[0]].add(name)
        if (not isinstance(identity, dict) or not sha256_value(identity.get('sha256')) or
                type(identity.get('bytes')) is not int or identity['bytes'] <= 0):
            errors.append(f'dataset file identity is missing or malformed: {name!r}')
    for name, count, chunk_key in (('vertices.parquet', vertices, 'chunk_vertices'),
                                    ('edges.parquet', edges, 'chunk_edges')):
        if family == 'traversal':
            wanted = {name}
        else:
            chunk = parameters.get(chunk_key)
            if type(chunk) is not int or not 1 <= chunk <= 1 << 24:
                errors.append(f'dataset partition size is missing or malformed: {chunk_key}')
                continue
            if not valid_vertices or not valid_edges:
                continue
            # Compare count/names without constructing a huge expected inventory.
            parts = (max(count, 1) + chunk - 1) // chunk
            names = inventory[name]
            if len(names) != parts or any(f'{name}/part-{i:08d}.parquet' not in names for i in range(len(names))):
                errors.append(f'dataset partition inventory differs: {name}')
            continue
        if inventory[name] != wanted:
            errors.append(f'dataset file inventory differs: {name}')
    if inventory['reference.parquet'] not in (set(), {'reference.parquet'}):
        errors.append('dataset reference inventory is malformed')
    return errors
