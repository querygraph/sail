#!/usr/bin/env python3
"""Run every graph comparison cell sequentially in a fresh Docker container.

The configuration contains operator-specific paths; keep the actual file outside
this checkout. A dry run emits the complete deterministic order without calling
Docker. Nonzero cells retain their receipt, logs, inspect state, and outcome.
"""
import argparse
from collections import Counter
from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path, PurePosixPath
import random
import re
import subprocess
import sys
import time

from runtime import algorithm_method, validate_admission_settings


ENGINES = ('pecan', 'nutmeg-native', 'nutmeg-datafusion')
ALGORITHMS = ('pagerank', 'wcc')
VARIANTS = ('reference', 'optimized', 'fused')
DEFAULT_VARIANTS = ('reference', 'optimized')


def utc():
    return datetime.now(timezone.utc).isoformat()


def write_json(path, value):
    temporary = path.with_suffix(path.suffix + '.tmp')
    temporary.write_text(json.dumps(value, indent=2, sort_keys=True) + '\n')
    temporary.replace(path)


def configuration_fingerprint(config):
    return hashlib.sha256(json.dumps(config, sort_keys=True).encode()).hexdigest()


def validate_config(config):
    required = ('run_id', 'docker_context', 'image', 'target_volume', 'container_python',
                'container_repo', 'container_sail_binary', 'runtime_source_sha', 'native_source_sha',
                'harness_source_sha', 'container_root', 'host_output', 'seed',
                'limits', 'defaults', 'datasets', 'suites')
    for key in required:
        if key not in config:
            raise ValueError(f'missing configuration key: {key}')
    if not re.fullmatch(r'[a-z0-9][a-z0-9-]{0,39}', config['run_id']):
        raise ValueError('run_id must be 1..40 lowercase letters, digits or hyphens')
    for key in ('runtime_source_sha', 'native_source_sha', 'harness_source_sha'):
        if not re.fullmatch(r'[a-f0-9]{40}', config[key]):
            raise ValueError(f'{key} must be an exact Git commit')
    for key in ('container_python', 'container_repo', 'container_sail_binary', 'container_root'):
        path = PurePosixPath(config[key])
        if not path.is_absolute() or '..' in path.parts:
            raise ValueError(f'{key} must be an absolute container path without ..')
    if not Path(config['host_output']).is_absolute():
        raise ValueError('host_output must be an absolute host path')
    limits = config['limits']
    if limits['cpus'] <= 0 or limits['memory_gib'] <= 0 or limits['outer_timeout_seconds'] <= 0:
        raise ValueError('resource limits must be positive')
    if not re.fullmatch(r'[0-9,-]+', limits['cpuset_cpus']):
        raise ValueError('cpuset_cpus must be an explicit CPU list/range')
    for key in ('worker_task_slots', 'sail_pool_bytes', 'native_quota'):
        if key not in config['defaults']:
            raise ValueError(f'missing configuration key: defaults.{key}')
    validate_admission_settings(**{key: config['defaults'][key] for key in
                                  ('worker_task_slots', 'sail_pool_bytes', 'native_quota')})
    for name, dataset in config['datasets'].items():
        if not re.fullmatch(r'[a-z0-9][a-z0-9-]*', name):
            raise ValueError(f'invalid dataset name: {name}')
        if dataset['family'] not in ('sparse', 'chain') or dataset['vertices'] <= 0:
            raise ValueError(f'invalid dataset: {name}')
    for suite in config['suites']:
        if not re.fullmatch(r'[a-z0-9][a-z0-9-]*', suite['name']):
            raise ValueError('suite name must be safe in a filename')
        if suite['mode'] not in ('local', 'process-cluster') or suite['repetitions'] <= 0:
            raise ValueError('invalid suite mode or repetition count')
        if set(suite['datasets']) - config['datasets'].keys():
            raise ValueError('suite names an unknown dataset')
        if set(suite.get('engines', ENGINES)) - set(ENGINES):
            raise ValueError('suite names an unknown engine')
        if set(suite['algorithms']) - set(ALGORITHMS):
            raise ValueError('suite names an unknown algorithm')
        variants = suite.get('variants', config.get('variants', DEFAULT_VARIANTS))
        if set(variants) - set(VARIANTS):
            raise ValueError('suite names an unknown variant')
        for engine in suite.get('engines', ENGINES):
            for algorithm in suite['algorithms']:
                for variant in variants:
                    algorithm_method(engine, algorithm, variant)


def plan_cells(config):
    validate_config(config)
    rng = random.Random(config['seed'])
    result = []
    for suite in config['suites']:
        for repeat in range(1, suite['repetitions'] + 1):
            group = []
            for dataset in suite['datasets']:
                for engine in suite.get('engines', ENGINES):
                    for algorithm in suite['algorithms']:
                        for variant in suite.get('variants', config.get('variants', DEFAULT_VARIANTS)):
                            name = f"{suite['name']}-r{repeat}-{dataset}-{engine}-{algorithm}-{variant}"
                            expected = suite.get('expected_outcomes', {}).get(variant, {}).get(engine, 'passed')
                            group.append(dict(cell_id=name, suite=suite['name'], repeat=repeat,
                                dataset=dataset, engine=engine, algorithm=algorithm, variant=variant,
                                mode=suite['mode'],
                                max_iterations=suite.get('max_iterations', config['defaults']['max_iterations']),
                                expected_outcome=expected, diagnostic=bool(suite.get('diagnostic', False))))
            rng.shuffle(group)
            result.extend(group)
    if len({cell['cell_id'] for cell in result}) != len(result):
        raise ValueError('cell IDs are not unique')
    for index, cell in enumerate(result, 1):
        cell['sequence'] = index
    return result


def docker_base(config):
    return ['docker', '--context', config['docker_context']]


def container_options(config, name, image=None):
    limits = config['limits']
    return ['--name', name, '--init', '--cpus', str(limits['cpus']),
            '--cpuset-cpus', limits['cpuset_cpus'], '--memory', f"{limits['memory_gib']}g",
            '--memory-swap', f"{limits['memory_gib']}g", '--pids-limit', '1024',
            '--mount', f"type=volume,source={config['target_volume']},target=/targets",
            '--workdir', config['container_repo'], '--env', 'PYTHONUNBUFFERED=1',
            '--entrypoint', config['container_python'], image or config['image']]


def cell_command(config, cell):
    root = PurePosixPath(config['container_root'])
    defaults = config['defaults']
    command = [str(PurePosixPath(config['container_repo']) / 'examples/extensions/benchmarks/graph_cell.py'),
               '--sail-binary', config['container_sail_binary'],
               '--runtime-source-sha', config['runtime_source_sha'],
               '--native-source-sha', config['native_source_sha'],
               '--dataset', str(root / 'datasets' / cell['dataset']),
               '--output', str(root / 'cells' / cell['cell_id']),
               '--engine', cell['engine'], '--algorithm', cell['algorithm'], '--variant', cell['variant'],
               '--mode', cell['mode'],
               '--repeat', str(cell['repeat']), '--max-iterations', str(cell['max_iterations'])]
    for name in ('partitions', 'threads', 'worker_task_slots', 'sail_pool_bytes',
                 'native_quota', 'tolerance', 'damping', 'timeout', 'seed'):
        command.extend(['--' + name.replace('_', '-'), str(defaults[name])])
    return command + config.get('extra_cell_args', [])


def capture(command, timeout=60):
    try:
        result = subprocess.run(command, text=True, capture_output=True, timeout=timeout)
        return dict(returncode=result.returncode, stdout=result.stdout, stderr=result.stderr)
    except (OSError, subprocess.TimeoutExpired) as error:
        return dict(returncode=None, stdout='', stderr=repr(error))


def inspect_state(base, name):
    result = capture(base + ['inspect', name])
    if result['returncode'] != 0:
        return None
    item = json.loads(result['stdout'])[0]
    keys = ('NanoCpus', 'CpuPeriod', 'CpuQuota', 'CpusetCpus', 'Memory', 'MemorySwap', 'PidsLimit', 'Init', 'PidMode')
    return dict(id=item['Id'], image=item['Image'], state=item['State'],
                limits={key: item['HostConfig'].get(key) for key in keys})


def run_container(config, name, command, output, image, timeout, copy_paths):
    """Preserve diagnostics for failures; never leave a running cell behind."""
    output.mkdir(parents=True, exist_ok=False)
    base = docker_base(config)
    create = base + ['create'] + container_options(config, name, image) + command
    record = dict(started_utc=utc(), command=create, host_load_before=None,
                  outer_timeout=False, transport_errors=[], copied={})
    try:
        import os
        record['host_load_before'] = os.getloadavg()
    except (AttributeError, OSError):
        pass
    created = False
    try:
        result = capture(create)
        record['create'] = result
        if result['returncode'] != 0:
            record['transport_errors'].append('container creation failed')
            return record
        created = True
        with (output / 'container.log').open('w') as stream:
            try:
                completed = subprocess.run(base + ['start', '--attach', name], stdout=stream,
                    stderr=subprocess.STDOUT, timeout=timeout)
                record['attach_returncode'] = completed.returncode
            except subprocess.TimeoutExpired:
                record['outer_timeout'] = True
                record['kill'] = capture(base + ['kill', name])
    except BaseException as error:
        record['transport_errors'].append(repr(error))
        if isinstance(error, (KeyboardInterrupt, SystemExit)):
            record['operator_interrupted'] = True
    finally:
        if created:
            try:
                state = inspect_state(base, name)
                if state and state['state']['Running']:
                    record['forced_cleanup_kill'] = capture(base + ['kill', name])
                record['inspect'] = inspect_state(base, name)
                if record['inspect'] is None:
                    record['transport_errors'].append('container inspection failed')
                for label, source in copy_paths.items():
                    destination = output / label
                    destination.mkdir()
                    result = capture(base + ['cp', name + ':' + source + '/.', str(destination)], timeout=180)
                    record['copied'][label] = result
                    if result['returncode'] != 0:
                        record['transport_errors'].append(f'required artifact copy failed: {label}')
            except BaseException as error:
                record['transport_errors'].append(repr(error))
            finally:
                record['remove'] = capture(base + ['rm', '--force', name])
                if record['remove']['returncode'] != 0:
                    record['transport_errors'].append('container removal failed')
        record['finished_utc'] = utc()
        write_json(output / 'orchestration.json', record)
    return record


def classify(record, receipt, expected_sha):
    state = (record.get('inspect') or {}).get('state', {})
    events = (receipt or {}).get('cgroup_after', {}).get('memory.events') or ''
    counters = dict(line.split() for line in events.splitlines() if len(line.split()) == 2)
    # A worker can be OOM-killed while the controller survives to write its
    # receipt; Docker's final process state alone does not cover that case.
    if state.get('OOMKilled') or int(counters.get('oom_kill', '0')) > 0:
        return 'oom'
    if record.get('outer_timeout'):
        return 'outer_timeout'
    if record.get('operator_interrupted'):
        return 'interrupted'
    if record['transport_errors']:
        return 'orchestration_error'
    if record.get('receipt_read_error'):
        return 'invalid_receipt'
    if receipt is None:
        return 'missing_receipt'
    if receipt.get('harness_source_sha') != expected_sha:
        return 'source_mismatch'
    if receipt.get('outcome') == 'passed' and record.get('attach_returncode') != 0:
        return 'exit_receipt_mismatch'
    return receipt.get('outcome', 'invalid_receipt')


def preflight(config, output):
    base = docker_base(config)
    image = capture(base + ['image', 'inspect', config['image'], '--format', '{{.Id}}'])
    if image['returncode'] != 0:
        raise RuntimeError(image['stderr'])
    image = image['stdout'].strip()
    script = ('import pathlib,subprocess,sys; p=pathlib.Path(sys.argv[1]); '
              'assert subprocess.check_output(["git","-C",str(p),"rev-parse","HEAD"],text=True).strip()==sys.argv[2]; '
              'assert not subprocess.check_output(["git","-C",str(p),"status","--porcelain"],text=True).strip(); '
              'assert pathlib.Path(sys.argv[3]).is_file(); print("preflight passed")')
    command = ['-I', '-c', script, config['container_repo'], config['harness_source_sha'], config['container_sail_binary']]
    name = 'sail-' + config['run_id'] + '-preflight'
    result = run_container(config, name, command, output / ('preflight-' + str(time.time_ns())), image, 120, {})
    if result.get('attach_returncode') != 0 or result['transport_errors']:
        raise RuntimeError('preflight failed; see preserved orchestration record')
    return image


def prepare_datasets(config, output, image):
    for name, options in config['datasets'].items():
        dataset_path = str(PurePosixPath(config['container_root']) / 'datasets' / name)
        command = [str(PurePosixPath(config['container_repo']) / 'examples/extensions/benchmarks/graph_fixtures.py'),
                   '--output', dataset_path]
        for key, value in options.items():
            command.extend(['--' + key.replace('_', '-'), str(value)])
        for key in ('tolerance', 'damping'):
            command.extend(['--' + key, str(config['defaults'][key])])
        result = run_container(config, 'sail-' + config['run_id'] + '-prepare-' + name, command,
            output / 'datasets' / name, image, config['limits']['outer_timeout_seconds'], {'dataset': dataset_path})
        if result.get('attach_returncode') != 0 or result['transport_errors']:
            raise RuntimeError(f'dataset preparation failed: {name}')


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--config', required=True, type=Path)
    parser.add_argument('--dry-run', action='store_true')
    parser.add_argument('--prepare-only', action='store_true')
    parser.add_argument('--skip-prepare', action='store_true')
    parser.add_argument('--resume', action='store_true', help='skip recorded cells, including failures; requires --skip-prepare')
    parser.add_argument('--select', action='append', help='run only these exact cell IDs; order is still from the full plan')
    args = parser.parse_args()
    if args.resume and not args.skip_prepare:
        parser.error('--resume requires --skip-prepare; existing datasets must be retained')
    config = json.loads(args.config.read_text())
    cells = plan_cells(config)
    if args.select:
        unknown = set(args.select) - {cell['cell_id'] for cell in cells}
        if unknown:
            parser.error(f'unknown selected cells: {sorted(unknown)}')
        cells = [cell for cell in cells if cell['cell_id'] in args.select]
    fingerprint = configuration_fingerprint(config)
    plan = dict(configuration_sha256=fingerprint, configuration=config, cells=cells,
                randomized_order='Python random.Random(seed), shuffle independently within each suite/repetition')
    if args.dry_run:
        plan['commands'] = {cell['cell_id']: cell_command(config, cell) for cell in cells}
        print(json.dumps(plan, indent=2))
        return 0
    output = Path(config['host_output'])
    output.mkdir(parents=True, exist_ok=True)
    config_record = output / 'configuration.json'
    if config_record.exists() and json.loads(config_record.read_text()) != config:
        parser.error('output directory already belongs to a different configuration')
    write_json(config_record, config)
    write_json(output / ('plan-' + str(time.time_ns()) + '.json'), plan)
    image = preflight(config, output)
    image_record = output / 'resolved-image.json'
    if image_record.exists() and json.loads(image_record.read_text())['image_sha256'] != image:
        parser.error('Docker image changed since this matrix started')
    write_json(image_record, dict(image_sha256=image))
    if not args.skip_prepare:
        prepare_datasets(config, output, image)
    if args.prepare_only:
        return 0
    results = []
    try:
        for cell in cells:
            cell_output = output / 'cells' / cell['cell_id']
            completed = cell_output / 'summary.json'
            if args.resume and completed.exists():
                result = json.loads(completed.read_text())
                if result['configuration_sha256'] != fingerprint:
                    raise RuntimeError('resume configuration differs from recorded cell')
                results.append(result)
                continue
            record = run_container(config, 'sail-' + config['run_id'] + '-' + str(cell['sequence']),
                cell_command(config, cell), cell_output, image, config['limits']['outer_timeout_seconds'],
                {'artifacts': str(PurePosixPath(config['container_root']) / 'cells' / cell['cell_id'])})
            path = cell_output / 'artifacts/receipt.json'
            try:
                receipt = json.loads(path.read_text()) if path.exists() else None
            except (OSError, json.JSONDecodeError) as error:
                receipt = None
                record['receipt_read_error'] = repr(error)
                write_json(cell_output / 'orchestration.json', record)
            outcome = classify(record, receipt, config['harness_source_sha'])
            result = dict(**cell, outcome=outcome, configuration_sha256=fingerprint,
                          expected_outcome_observed=outcome == cell['expected_outcome'],
                          receipt_path=str(path) if path.exists() else None,
                          end_to_end_seconds=receipt.get('end_to_end_seconds') if receipt else None)
            write_json(completed, result)
            results.append(result)
            write_json(output / 'matrix-results.json', dict(recorded_utc=utc(), results=results,
                outcome_counts=dict(Counter(row['outcome'] for row in results))))
            print(json.dumps(result), flush=True)
            if record['transport_errors'] or record.get('operator_interrupted'):
                raise RuntimeError('orchestration/cleanup failed; stopped before launching another cell')
    finally:
        write_json(output / 'matrix-results.json', dict(recorded_utc=utc(), results=results,
            planned_cells=len(cells), completed_cells=len(results),
            outcome_counts=dict(Counter(row['outcome'] for row in results))))
    return 0 if all(row['expected_outcome_observed'] for row in results) else 1


if __name__ == '__main__':
    raise SystemExit(main())
