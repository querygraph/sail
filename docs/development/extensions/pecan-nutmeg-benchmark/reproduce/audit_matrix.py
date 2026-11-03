#!/usr/bin/env python3
"""Independent local audit of immutable main/fusion evidence; never contacts hosts.

Example (use the locked extension Python for NumPy/PyArrow):
  audit_matrix.py --kind main --evidence SAFE --summary SUMMARY --raw RAW \
      --require-raw --output target/pecan-benchmark/main-audit

The recorded SHA supplies only orchestration/summary replay. Mathematical raw
checks below do not import Sail, Pecan, Nutmeg or graph_fixtures algorithms.
Original outcomes remain unchanged; audit issues have their own verdict.
"""
from __future__ import annotations
import argparse
from collections import Counter, defaultdict
import csv
from datetime import datetime, timezone
import hashlib
import json
import math
from pathlib import Path, PurePosixPath
import subprocess
from statistics import median
import sys
import types

import numpy as np
import pyarrow.parquet as pq

MASK = (1 << 64) - 1
SOURCE_KEYS = ('harness_source_sha', 'runtime_source_sha', 'native_source_sha')
MEMORY_KEYS = ('rss_bytes', 'pss_bytes', 'cgroup_current_bytes')


def read(path):
    return json.loads(path.read_text())


def digest(value):
    return hashlib.sha256(json.dumps(value, sort_keys=True).encode()).hexdigest()


def hash_file(path):
    result = hashlib.sha256()
    with path.open('rb') as stream:
        for block in iter(lambda: stream.read(1 << 20), b''):
            result.update(block)
    return result.hexdigest()


def source_module(repo, sha, name, filename):
    contents = subprocess.check_output(['git', '-C', str(repo), 'show', f'{sha}:{filename}'], text=True)
    module = types.ModuleType(name)
    module.__file__ = f'{sha}:{filename}'
    sys.modules[name] = module
    exec(compile(contents, module.__file__, 'exec'), module.__dict__)
    return module


class Audit:
    def __init__(self, args):
        self.args = args
        self.issues, self.warnings = [], []
        self.checked_files = 0
        self.verified_paths = set()
        self.inventory = {}
        self.datasets = {}
        self.cells = []

    def check(self, condition, message, scope='matrix'):
        if not condition:
            self.issues.append(dict(scope=scope, message=message))
        return bool(condition)

    def warn(self, message, scope='matrix'):
        self.warnings.append(dict(scope=scope, message=message))

    def resolve(self, relative):
        path = PurePosixPath(relative)
        if path.is_absolute() or '..' in path.parts:
            raise ValueError(f'unsafe inventory path: {relative}')
        for base in [self.args.evidence, *self.args.raw]:
            candidate = base / relative
            if candidate.is_file():
                return candidate
        return None

    def verify_file(self, path, spec, scope):
        good = self.check(path.stat().st_size == spec['bytes'], 'file length differs', scope)
        good &= self.check(hash_file(path) == spec['sha256'], 'file hash differs', scope)
        self.checked_files += 1
        self.verified_paths.add(str(path.resolve()))
        return good

    def inventories(self):
        manifest = self.args.evidence / 'export-manifest.json'
        if manifest.exists():
            entries = read(manifest)['files']
            self.check(len(entries) == len({entry['path'] for entry in entries}), 'duplicate export paths')
            self.inventory = {entry['path']: entry for entry in entries}
            for relative, spec in self.inventory.items():
                path = self.resolve(relative)
                if spec['included']:
                    self.check(path is not None, 'included file missing', relative)
                if path is not None:
                    self.verify_file(path, spec, relative)
                elif not relative.endswith('.parquet'):
                    self.warn('non-Parquet payload omitted', relative)
        else:
            self.warn('no safe export manifest; verifying receipt and dataset inventories directly')
        for root in self.args.raw:
            manifest = root / 'audit-manifest.json'
            if not manifest.exists():
                self.warn('raw export has no audit-manifest.json', str(root))
                continue
            raw_manifest = read(manifest)
            entries = raw_manifest['files']
            source_results = self.args.evidence / 'matrix-results.json'
            if raw_manifest.get('source_matrix_sha256') and source_results.exists():
                self.check(hash_file(source_results) == raw_manifest['source_matrix_sha256'],
                           'raw subset was exported from different matrix results', str(root))
            self.check(len(entries) == len({entry['path'] for entry in entries}), 'duplicate raw paths', str(root))
            for spec in entries:
                relative = spec['path']
                path = root / relative
                self.check(path.is_file(), 'raw inventory file missing', relative)
                if path.is_file():
                    self.verify_file(path, spec, relative)
                original = self.inventory.get(relative)
                if original:
                    self.check(all(original[key] == spec[key] for key in ('bytes', 'sha256')),
                               'raw and safe export identities disagree', relative)

    def memory(self, path, receipt, scope):
        memory = receipt.get('memory')
        if not memory:
            self.warn('no completed sampler receipt', scope)
            return dict(samples=None, execution_samples=None, pss_samples=None)
        samples = [json.loads(line) for line in path.read_text().splitlines()]
        peaks, first, phase_counts = {}, {}, Counter()
        previous = -math.inf
        for index, sample in enumerate(samples):
            phase = sample['phase']
            phase_counts[phase] += 1
            start, end = sample['scan_started_seconds'], sample['scan_finished_seconds']
            self.check(previous <= start <= end, f'memory scan interval invalid at {index}', scope)
            previous = end
            processes = sample['processes']
            self.check(len({p['pid'] for p in processes}) == len(processes), 'duplicate process in sample', scope)
            rss = sum(p['rss_bytes'] for p in processes) if processes else None
            pss = (sum(p['pss_bytes'] for p in processes)
                   if processes and all(p['pss_bytes'] is not None for p in processes) else None)
            self.check(sample['rss_bytes'] == rss and sample['pss_bytes'] == pss,
                       f'process memory sum differs at {index}', scope)
            self.check(sample['pss_processes_available'] == sum(p['pss_bytes'] is not None for p in processes),
                       f'PSS availability count differs at {index}', scope)
            if phase == 'transition':
                continue
            first.setdefault(phase, sample)
            current = peaks.setdefault(phase, {})
            for key in MEMORY_KEYS:
                value = sample[key]
                if value is not None:
                    self.check(math.isfinite(value) and value >= 0, 'invalid memory value', scope)
                    current[key] = max(current.get(key, 0), value)
        self.check(len(samples) == memory['samples'], 'sample count differs', scope)
        self.check(dict(phase_counts) == memory['phase_sample_counts'], 'phase sample counts differ', scope)
        self.check(peaks == memory['phase_peaks'], 'recomputed phase peaks differ', scope)
        self.check(first == memory['phase_first_samples'], 'phase first samples differ', scope)
        self.check(bool(phase_counts['execute']) == memory['execution_sampled'], 'execution sampling flag differs', scope)
        self.check(memory.get('error') is None and not memory.get('thread_alive'), 'sampler failed or remains alive', scope)
        execute = [sample for sample in samples if sample['phase'] == 'execute']
        return dict(samples=len(samples), execution_samples=len(execute),
                    pss_samples=sum(sample['pss_bytes'] is not None for sample in execute),
                    maximum_execution_scan_seconds=max((s['scan_finished_seconds'] - s['scan_started_seconds']
                                                        for s in execute), default=None),
                    execution_peaks=peaks.get('execute'))

    def raw_dataset(self, name, manifest):
        if name in self.datasets:
            return self.datasets[name]
        relative = f'datasets/{name}/dataset/'
        paths = {filename: self.resolve(relative + filename) for filename in manifest['files']}
        if not all(paths.values()):
            self.datasets[name] = None
            return None
        for filename, path in paths.items():
            self.verify_file(path, manifest['files'][filename], relative + filename)
        vertices = pq.read_table(paths['vertices.parquet']).column('id').to_numpy()
        ids = np.sort(vertices)
        n = len(ids)
        self.check(np.array_equal(ids, np.arange(n, dtype=np.int64)), 'fixture IDs are not exactly 0..V-1', name)
        edges = pq.read_table(paths['edges.parquet'])
        source, target = (edges.column(column).to_numpy() for column in ('src', 'dst'))
        self.check(len(source) == manifest['counts']['edges'], 'raw input edge count differs', name)
        self.check(not len(source) or (min(source.min(), target.min()) >= 0 and max(source.max(), target.max()) < n),
                   'raw endpoint outside vertices', name)
        degree = np.bincount(source, minlength=n)
        # Independent union-find: attach the larger root to the smaller root.
        # This differs from the fixture's size-balanced implementation and
        # directly yields each component's numeric minimum after compression.
        parent = list(range(n))
        def find(node):
            while parent[node] != node:
                parent[node] = parent[parent[node]]
                node = parent[node]
            return node
        for left, right in zip(source.tolist(), target.tolist()):
            a, b = find(left), find(right)
            if a != b:
                parent[max(a, b)] = min(a, b)
        components = np.fromiter((find(i) for i in range(n)), dtype=np.int64, count=n)
        reference = pq.read_table(paths['reference.parquet']).to_pydict()
        order = np.argsort(np.asarray(reference['id'], dtype=np.int64))
        self.check(np.array_equal(np.asarray(reference['id'])[order], ids), 'reference ID coverage differs', name)
        self.check(np.array_equal(np.asarray(reference['component'])[order], components),
                   'stored WCC reference differs from independent raw-edge union-find', name)
        indegree = np.bincount(target, minlength=n)
        observed_counts = dict(vertices=n, edges=len(source), components=int(np.unique(components).size),
                               isolates=int(np.count_nonzero((degree == 0) & (indegree == 0))),
                               dangling=int(np.count_nonzero(degree == 0)),
                               self_loops=int(np.count_nonzero(source == target)))
        for key, value in observed_counts.items():
            self.check(manifest['counts'][key] == value, f'raw fixture count differs: {key}', name)
        reference_scores = np.asarray(reference['pagerank'], dtype=np.float64)[order]
        self.check(np.isfinite(reference_scores).all() and (reference_scores >= 0).all()
                   and abs(reference_scores.sum() - 1) <= 1e-10, 'stored reference has invalid scores', name)
        dataset = dict(ids=ids, source=source, target=target, degree=degree, components=components,
                       reference_scores=reference_scores, manifest=manifest)
        reference_residual = float(np.abs(self.transition(reference_scores, dataset) - reference_scores).sum())
        damping, tolerance = (manifest['pagerank'][key] for key in ('damping', 'tolerance'))
        self.check(reference_residual <= damping * tolerance + 1e-12,
                   'stored power reference does not satisfy implied fixed-point residual bound', name)
        dataset['reference_residual'] = reference_residual
        self.datasets[name] = dataset
        return dataset

    @staticmethod
    def transition(scores, data):
        n = len(scores)
        damping = data['manifest']['pagerank']['damping']
        outdegree = data['degree']
        incoming = np.bincount(data['target'], weights=scores[data['source']] / outdegree[data['source']], minlength=n)
        restart = (1 - damping + damping * scores[outdegree == 0].sum()) / n
        return restart + damping * incoming

    def raw_result(self, cell, receipt):
        scope = cell['cell_id']
        specs = receipt.get('result_files') or []
        prefix = f'cells/{scope}/artifacts/result/'
        if not specs:
            # Old controls omitted capped-native inventories. Verify discovered
            # bytes independently, while clearly retaining that provenance gap.
            files = sorted((self.args.evidence / prefix).glob('*.parquet'))
            if files:
                self.warn('raw result exists without a receipt inventory', scope)
            else:
                return dict(coverage='receipt_only', reason='no delivered result inventory')
        else:
            files = [self.resolve(prefix + spec['name']) for spec in specs]
            for spec, path in zip(specs, files):
                original = self.inventory.get(prefix + spec['name'])
                if original:
                    self.check(all(original[key] == spec[key] for key in ('bytes', 'sha256')),
                               'result receipt and export inventory disagree', scope)
                if path:
                    self.verify_file(path, spec, prefix + spec['name'])
            if not all(files):
                return dict(coverage='receipt_only', reason='result Parquet omitted from local export')
        data = self.raw_dataset(cell['dataset'], receipt['dataset'])
        if data is None:
            return dict(coverage='receipt_only', reason='input/reference Parquet omitted from local export')
        table = pq.read_table(files).to_pydict()
        ids = np.asarray(table['id'], dtype=np.int64)
        order = np.argsort(ids)
        if not self.check(np.array_equal(ids[order], data['ids']), 'raw output lacks exact unique ID coverage', scope):
            return dict(coverage='raw_invalid_ids', rows=len(ids))
        answer = dict(coverage='raw_full_vector', rows=len(ids), result_files=len(files))
        recorded = receipt.get('correctness') or {}
        if cell['algorithm'] == 'wcc':
            self.check(all(value is not None for value in table['component']), 'raw WCC contains null labels', scope)
            arbitrary = np.asarray(table['component'], dtype=str)[order]
            _, inverse = np.unique(arbitrary, return_inverse=True)
            minima = np.full(int(inverse.max()) + 1, len(ids), dtype=np.int64)
            np.minimum.at(minima, inverse, data['ids'])
            canonical = minima[inverse]
            mismatch = int(np.count_nonzero(canonical != data['components']))
            self.check(mismatch == 0, 'raw WCC differs from independent input-edge union-find', scope)
            answer.update(components=len(minima), membership_mismatches=mismatch)
        else:
            scores = np.asarray(table['score'], dtype=np.float64)[order]
            valid = np.isfinite(scores).all() and (scores >= 0).all()
            self.check(valid, 'raw ranks contain invalid or negative values', scope)
            mass = float(scores.sum())
            residual = float(np.abs(self.transition(scores, data) - scores).sum())
            error = float(np.abs(scores - data['reference_scores']).sum())
            maximum = float(np.max(np.abs(scores - data['reference_scores'])))
            d, tolerance = (receipt['arguments'][key] for key in ('damping', 'tolerance'))
            limit = ((1 if cell['variant'] == 'optimized' else d) + d) * tolerance / (1 - d) + 1e-12
            converged = set(table['converged'])
            iterations = set(table['iterations'])
            self.check(len(iterations) == 1 and all(isinstance(value, int) and
                       (0 if cell['variant'] == 'optimized' else 1) <= value <= receipt['arguments']['max_iterations']
                       for value in iterations), 'raw iteration metadata invalid or inconsistent', scope)
            self.check(converged <= {True, False}, 'raw convergence metadata invalid', scope)
            if cell['engine'] == 'nutmeg-native' or cell['variant'] == 'optimized':
                residuals = set(table['residual'])
                self.check(len(residuals) == 1 and all(value is not None and math.isfinite(value)
                           and value >= 0 for value in residuals), 'raw reported residual invalid or inconsistent', scope)
                if receipt['outcome'] == 'passed':
                    self.check(all(value <= tolerance for value in residuals), 'raw reported residual exceeds tolerance', scope)
            self.check(abs(mass - 1) <= 1e-10, 'raw scores are not normalized', scope)
            if receipt['outcome'] == 'passed':
                self.check(converged == {True}, 'raw passed output is not converged', scope)
                self.check(residual <= tolerance + 1e-12, 'raw fixed-point residual exceeds contract', scope)
                self.check(error <= limit, 'raw full-vector reference error exceeds contract', scope)
                for key, value in [('true_fixed_point_residual', residual), ('l1_error', error), ('max_error', maximum)]:
                    self.check(math.isclose(recorded[key], value, rel_tol=1e-5, abs_tol=1e-12),
                               f'recomputed {key} disagrees beyond summation slack', scope)
            elif receipt['outcome'] == 'nonconverged':
                self.check(converged == {False}, 'capped native output lost nonconvergence flag', scope)
            answer.update(rank_sum=mass, true_fixed_point_residual=residual, reference_l1_error=error,
                          maximum_absolute_error=maximum, reference_error_limit=limit,
                          stationary_error_bound=residual / (1 - d), iterations=sorted(set(table['iterations'])),
                          converged=sorted(converged))
        return answer

    def trace(self, cell, receipt):
        scope = cell['cell_id']
        status = receipt.get('native_status_after') or receipt.get('native_status_on_error') or {}
        reads = [r for r in status.get('reads', []) if r['algorithm'] == receipt.get('kernel')]
        native = reads[0] if len(reads) == 1 else {}
        diagnostics = native.get('diagnostics') or {}
        events = [e for e in receipt.get('iteration_events', []) if e['kind'] == 'iteration_end']
        result = dict(kernel_threads=diagnostics.get('kernel_threads'),
                      requested_concurrency=diagnostics.get('requested_concurrency'))
        if cell['engine'] == 'nutmeg-native' and receipt['outcome'] == 'passed':
            self.check(len(reads) == 1 and native.get('state') == 'finished' and
                       native.get('rows') == receipt['dataset']['counts']['vertices'],
                       'native result did not come from exactly one completed full read', scope)
        if diagnostics:
            self.check(1 <= diagnostics['kernel_threads'] <= diagnostics['requested_concurrency'],
                       'actual native width exceeds requested concurrency', scope)
            self.check(diagnostics['requested_concurrency'] == receipt['arguments']['threads'],
                       'native requested concurrency differs from trial', scope)
        rounds = diagnostics.get('rounds', []) if cell['engine'] == 'nutmeg-native' else events
        if cell['algorithm'] == 'wcc' and cell['variant'] in ('optimized', 'fused'):
            coefficients = splitmix_coefficients(receipt['arguments']['seed'])
            normalized = []
            previous_edges = None
            for index, item in enumerate(rounds, 1):
                a, b = next(coefficients)
                actual = (int(item.get('a_bits', item.get('coefficient_a'))) & MASK,
                          int(item.get('b_bits', item.get('coefficient_b'))) & MASK)
                self.check(actual == (a, b), f'WCC coefficient stream differs in round {index}', scope)
                before, after = item['edges_before'], item['edges_after']
                vertices = item.get('vertices_before', item.get('active_vertices'))
                self.check(item['iteration'] == index and 0 <= after <= before and vertices >= 0,
                           f'invalid contraction counters in round {index}', scope)
                if previous_edges is not None:
                    self.check(before == previous_edges, 'contraction edge generations do not connect', scope)
                previous_edges = after
                normalized.append(dict(iteration=index, vertices_before=vertices, edges_before=before,
                                       edges_after=after, a_bits=str(a), b_bits=str(b)))
            if receipt['outcome'] == 'passed':
                self.check(not normalized or normalized[-1]['edges_after'] == 0,
                           'successful contraction has remaining edges', scope)
                expected_rounds = receipt.get('algorithm_iterations', diagnostics.get('iterations'))
                self.check(expected_rounds == len(rounds), 'WCC iteration count differs from trace', scope)
            if cell['variant'] == 'fused' and diagnostics:
                self.check(diagnostics.get('initial_edge_policy') == 'raw-non-loop', 'fused native policy not recorded', scope)
            result['contractions'] = normalized
        elif cell['algorithm'] == 'pagerank' and cell['variant'] == 'optimized':
            n = receipt['dataset']['counts']['vertices']
            total_edges = receipt['dataset']['counts']['edges']
            counts = []
            for index, item in enumerate(rounds, 1):
                active = item.get('active_vertices', item.get('frontier_size'))
                self.check(item['iteration'] == index and 0 <= active <= n and
                           0 <= item['active_edges'] <= total_edges and
                           0 <= item['reactivated_vertices'] <= active,
                           f'invalid delta frontier counters at round {index}', scope)
                self.check(0 <= item['activation_threshold'] <=
                           receipt['arguments']['tolerance'] * item['activation_mass'] / (4 * n) + 1e-30,
                           'delta cutoff exceeds tolerance-scaled cap', scope)
                if 'activation_residual_l1' in item:
                    expected = min(item['activation_residual_l1'] / (2 * n),
                                   receipt['arguments']['tolerance'] * item['activation_mass'] / (4 * n))
                    self.check(item['activation_threshold'] == expected, 'native delta cutoff policy differs', scope)
                counts.append((active, item['active_edges'], item['reactivated_vertices']))
            result.update(frontier_vertices=[r[0] for r in counts], active_edges=[r[1] for r in counts],
                          reactivated_vertices=[r[2] for r in counts], frontier_messages=sum(r[1] for r in counts))
            if diagnostics:
                self.check(result['frontier_messages'] == diagnostics['frontier_edges'],
                           'native frontier message total differs', scope)
        return result

    def common_receipt(self, cell, summary, receipt, config):
        scope = cell['cell_id']
        for key in SOURCE_KEYS:
            self.check(receipt.get(key) == config[key], f'source identity differs: {key}', scope)
        self.check(receipt.get('source_dirty') == '', 'dirty or unrecorded source', scope)
        args = receipt['arguments']
        expected = {key: cell[key] for key in ('engine', 'algorithm', 'variant', 'mode', 'repeat', 'max_iterations')}
        expected.update({key: config['defaults'][key] for key in ('partitions', 'threads', 'native_quota',
                        'tolerance', 'damping', 'timeout', 'seed', 'worker_task_slots', 'sail_pool_bytes')
                         if key in config['defaults']})
        base = PurePosixPath(config['container_root'])
        expected.update(dataset=str(base / 'datasets' / cell['dataset']),
                        output=str(base / 'cells' / scope), sail_binary=config['container_sail_binary'],
                        runtime_source_sha=config['runtime_source_sha'], native_source_sha=config['native_source_sha'],
                        allow_dirty=False, allow_unisolated=False)
        for key, value in expected.items():
            self.check(args.get(key) == value, f'argument differs: {key}', scope)
        self.check(receipt['prepaid_native_quota_bytes'] == args['native_quota'], 'native admission receipt differs', scope)
        if 'worker_task_slots' in args:
            expected = dict(worker_task_slots_per_worker=args['worker_task_slots'],
                            worker_task_slots_total=args['worker_task_slots'] * (2 if cell['mode'] == 'process-cluster' else 0),
                            sail_pool_per_process_bytes=args['sail_pool_bytes'],
                            remaining_participating_df_budget_bytes=args['sail_pool_bytes'] - args['native_quota'])
            for key, value in expected.items():
                self.check(receipt.get(key) == value, f'admission field differs: {key}', scope)
        self.check(not receipt.get('cleanup_errors'), 'receipt contains cleanup errors', scope)
        if 'staging_files_after_shutdown' in receipt:
            self.check(receipt['staging_files_after_shutdown'] == [], 'post-shutdown staging files remain', scope)
        if receipt['outcome'] == 'passed':
            c = receipt['correctness']
            n = config['datasets'][cell['dataset']]['vertices']
            self.check(c['rows'] == c['unique_ids'] == n and c['null_ids'] == 0, 'recorded vertex coverage differs', scope)
            if cell['algorithm'] == 'pagerank':
                self.check(c['all_converged'] == 1 and c['true_fixed_point_residual'] <= args['tolerance'] + 1e-12
                           and c['l1_error'] <= c['l1_error_limit'], 'recorded PageRank failed validation', scope)
            else:
                self.check(c['membership_mismatches'] == 0, 'recorded WCC mismatch', scope)
        execution = receipt.get('cgroup_execution_after', {}).get('memory.peak')
        lifetime = receipt.get('cgroup_after', {}).get('memory.peak')
        if execution is not None and lifetime is not None:
            self.check(int(lifetime) >= int(execution), 'cgroup lifetime peak decreased', scope)

    def orchestration(self, cell, record, config, image):
        scope = cell['cell_id']
        inspection = record.get('inspect') or {}
        limits, state = inspection.get('limits') or {}, inspection.get('state') or {}
        if inspection:
            expected = dict(NanoCpus=int(config['limits']['cpus'] * 1e9),
                            CpusetCpus=config['limits']['cpuset_cpus'], Init=True, PidMode='', PidsLimit=1024,
                            Memory=config['limits']['memory_gib'] * 1024**3,
                            MemorySwap=config['limits']['memory_gib'] * 1024**3)
            for key, value in expected.items():
                self.check(limits.get(key) == value, f'container limit differs: {key}', scope)
            self.check(inspection['image'] == image, 'container image differs', scope)
            self.check(not state.get('Running'), 'container still running', scope)
        else:
            self.warn('container inspection missing; cause retained separately', scope)
        if record.get('create', {}).get('returncode') == 0:
            self.check(record.get('remove', {}).get('returncode') == 0, 'created container not successfully removed', scope)
        self.check(not record.get('transport_errors'), 'artifact transport/orchestration errors recorded', scope)
        for label, copied in record.get('copied', {}).items():
            self.check(copied['returncode'] == 0, f'artifact copy failed: {label}', scope)
        return dict(oom_killed=state.get('OOMKilled'), exit_code=state.get('ExitCode'),
                    outer_timeout=record.get('outer_timeout'), transport_errors=record.get('transport_errors'),
                    removed=record.get('remove', {}).get('returncode') == 0)

    def compare_traces(self):
        groups = defaultdict(list)
        for cell in self.cells:
            if cell['outcome'] == 'passed' and cell.get('trace', {}).get('contractions') is not None:
                groups[cell['dataset']].append(cell)
        results = []
        for name, cells in groups.items():
            # GF64 uses the same seed in each cell; floating PR frontiers are
            # deliberately not required to be identical across reduction orders.
            baseline = cells[0]['trace']['contractions']
            for cell in cells:
                rounds = cell['trace']['contractions']
                self.check(len(rounds) == len(baseline), 'cross-path contraction round counts differ', cell['cell_id'])
                for index, (a, b) in enumerate(zip(baseline, rounds)):
                    a, b = dict(a), dict(b)
                    if index == 0:
                        a.pop('edges_before'); b.pop('edges_before')
                    self.check(a == b, f'cross-path contraction differs in round {index + 1}', cell['cell_id'])
            results.append(dict(dataset=name, compared_cells=[c['cell_id'] for c in cells], rounds=len(baseline),
                                common_after_initial_input_count=baseline,
                                initial_counts={c['cell_id']: c['trace']['contractions'][0]['edges_before']
                                                if c['trace']['contractions'] else 0 for c in cells}))
        return results

    def run(self):
        config = read(self.args.evidence / 'configuration.json')
        sha = config['harness_source_sha']
        for module in ('runtime', 'run_matrix'):
            source_module(self.args.repo, sha, module, f'examples/extensions/benchmarks/{module}.py')
        runner = sys.modules['run_matrix']
        summarizer = source_module(self.args.repo, sha, 'audit_summary', 'examples/extensions/benchmarks/summarize.py')
        plan = runner.plan_cells(config)
        if self.args.kind != 'control':
            self.check(len(plan) == (150 if self.args.kind == 'main' else 78), 'unexpected planned-cell count')
        self.inventories()
        entries, sources, binary, native, datasets = [], set(), set(), set(), defaultdict(set)
        image = read(self.args.evidence / 'resolved-image.json')['image_sha256']
        for cell in plan:
            scope = cell['cell_id'];directory = self.args.evidence / 'cells' / scope
            sp, rp = directory / 'summary.json', directory / 'artifacts/receipt.json'
            summary = read(sp) if sp.exists() else None
            receipt = read(rp) if rp.exists() else None
            entries.append((cell, summary, receipt))
            details = dict(cell, outcome=summary['outcome'] if summary else ('incomplete_record' if receipt else 'not_run'),
                           original_receipt_outcome=receipt.get('outcome') if receipt else None)
            self.cells.append(details)
            try:
                if summary is not None:
                    self.check(summary['configuration_sha256'] == runner.configuration_fingerprint(config),
                               'configuration fingerprint differs', scope)
                    record = read(directory / 'orchestration.json')
                    self.check(runner.classify(record, receipt, sha) == summary['outcome'],
                               'original-SHA classification differs', scope)
                    details['orchestration'] = self.orchestration(cell, record, config, image)
                if receipt is None:
                    if summary:
                        self.warn('completed summary has no receipt', scope)
                    continue
                self.common_receipt(cell, summary, receipt, config)
                sources.add(tuple(receipt.get(key) for key in SOURCE_KEYS));binary.add(receipt.get('binary_sha256'))
                native.add(digest(receipt['native_package_identity']['files_sha256']))
                datasets[cell['dataset']].add(digest(receipt['dataset']['files']))
                local_manifest = self.resolve(f'datasets/{cell["dataset"]}/dataset/manifest.json')
                if local_manifest:
                    self.check(read(local_manifest) == receipt['dataset'], 'dataset manifest differs from receipt', scope)
                memory_file = directory / 'artifacts/memory-samples.jsonl'
                if memory_file.exists():
                    details['memory'] = self.memory(memory_file, receipt, scope)
                else:
                    self.warn('raw memory samples unavailable', scope)
                details.update(trace=self.trace(cell, receipt), raw=self.raw_result(cell, receipt),
                               recorded_correctness=receipt.get('correctness'),
                               cleanup_errors=receipt.get('cleanup_errors'),
                               staging_files_after_shutdown=receipt.get('staging_files_after_shutdown'),
                               error=receipt.get('error'), guest_steal_fraction=receipt.get('guest_steal_fraction'))
                if self.args.require_raw and raw_selected(cell, self.args.kind) and details['outcome'] == 'passed':
                    self.check(details['raw']['coverage'] == 'raw_full_vector', 'required raw audit coverage missing', scope)
            except Exception as error:
                self.issues.append(dict(scope=scope, message=f'audit could not complete: {type(error).__name__}: {error}'))
        for name, values in [('source triples', sources), ('Sail binaries', binary), ('native installed-file identities', native)]:
            self.check(len(values) <= 1, f'inconsistent {name} across retained receipts')
        for name, values in datasets.items():
            self.check(len(values) == 1, 'inconsistent dataset file identities', name)
        rows = summarizer.audited_rows(entries, config)
        self.check(not any(row['integrity_errors'] for row in rows), 'original summary integrity checks fail')
        if self.args.summary:
            existing = read(self.args.summary / 'summary.json')
            self.check(summarizer.aggregate(rows) == existing['groups'], 'summary aggregate replay differs')
            # Independently rebuild the plotted metrics directly from receipts,
            # instead of relying only on replaying the summarizer's own code.
            for group in existing['groups']:
                selected = [r for c, summary, r in entries if summary and summary['outcome'] == 'passed'
                            and all(c[key] == group[key] for key in summarizer.GROUP)]
                for metric in ('seconds', 'execution_rss_bytes', 'execution_pss_bytes',
                               'cgroup_peak_through_result_bytes', 'cgroup_lifetime_peak_bytes'):
                    values = []
                    for receipt in selected:
                        memory = receipt.get('memory', {}).get('phase_peaks', {}).get('execute', {})
                        value = {'seconds': receipt.get('end_to_end_seconds'),
                                 'execution_rss_bytes': memory.get('rss_bytes'),
                                 'execution_pss_bytes': memory.get('pss_bytes'),
                                 'cgroup_peak_through_result_bytes': receipt.get('cgroup_execution_after', {}).get('memory.peak'),
                                 'cgroup_lifetime_peak_bytes': receipt.get('cgroup_after', {}).get('memory.peak')}[metric]
                        if value is not None:
                            values.append(float(value))
                    expected = dict(samples=len(values), median=median(values), minimum=min(values), maximum=max(values)) if values else None
                    self.check(group['metrics'][metric] == expected, f'independent aggregate differs: {metric}', str(tuple(group[key] for key in summarizer.GROUP)))
            self.check(dict(Counter(r['outcome'] for r in rows)) == existing['outcomes'], 'summary outcome totals differ')
            with (self.args.summary / 'cells.csv').open() as stream:
                exported = list(csv.DictReader(stream))
            self.check(len(exported) == len(plan) and {r['cell_id'] for r in exported} == {c['cell_id'] for c in plan},
                       'CSV omits, duplicates or adds planned cells')
            for row, saved in zip(rows, exported):
                self.check(row['cell_id'] == saved['cell_id'] and row['outcome'] == saved['outcome'],
                           'CSV row identity or outcome differs', row['cell_id'])
                for metric in summarizer.METRICS:
                    value = None if saved[metric] == '' else float(saved[metric])
                    self.check(value == row[metric], f'CSV metric differs: {metric}', row['cell_id'])
        else:
            self.warn('no exported summary provided for aggregate replay')
        cleanup_path = self.args.evidence / 'post-stop-cleanup.json'
        candidates = [cleanup_path] if cleanup_path.exists() else sorted((self.args.evidence / 'collection-context').glob('*cleanup.json'))
        snapshots = [(path, read(path)) for path in candidates]
        snapshots = [(path, record) for path, record in snapshots
                     if record.get('harness_source_sha') == sha and record.get('run') == config['container_root']]
        self.check(len(snapshots) <= 1, 'ambiguous post-stop cleanup snapshots')
        cleanup = snapshots[0][1] if snapshots else None
        if cleanup:
            self.check(cleanup.get('remaining_staging_regular_files') == [], 'post-stop staging files remain')
            self.check(cleanup.get('completed_cell_directories') == sum(e[1] is not None for e in entries),
                       'cleanup snapshot does not cover all completed cells')
        else:
            self.warn('no independent post-stop cleanup snapshot; error-cell cleanup coverage may be limited')
        traces = self.compare_traces()
        return dict(recorded_utc=datetime.now(timezone.utc).isoformat(), kind=self.args.kind,
                    evidence=str(self.args.evidence), raw_roots=list(map(str, self.args.raw)),
                    auditor_sha256=hash_file(Path(__file__)), sources={key: config[key] for key in SOURCE_KEYS},
                    source_replayed=sha, configuration_sha256=runner.configuration_fingerprint(config),
                    planned=len(plan), outcomes=dict(Counter(c['outcome'] for c in self.cells)),
                    checked_files=self.checked_files, unique_verified_files=len(self.verified_paths),
                    image=image, binary_sha256=sorted(binary),
                    native_installed_file_identity=sorted(native), limits=config['limits'], defaults=config['defaults'],
                    raw_coverage=dict(Counter(c.get('raw', {}).get('coverage', 'no_receipt') for c in self.cells)),
                    reconstructed_samples=sum((c.get('memory') or {}).get('samples') or 0 for c in self.cells),
                    post_stop_cleanup=cleanup, cross_path_contractions=traces,
                    sample_coverage=sample_coverage(self.cells), native_widths=native_widths(self.cells),
                    steal_observations=observed_range([c.get('guest_steal_fraction') for c in self.cells]),
                    audit_outcome='failed' if self.issues else 'passed', issues=self.issues,
                    warnings=self.warnings, cells=self.cells)


def observed_range(values):
    values = [value for value in values if value is not None]
    return dict(observations=len(values), minimum=min(values), median=median(values), maximum=max(values)) if values else None


def sample_coverage(cells):
    groups = defaultdict(list)
    for cell in cells:
        groups[(cell['engine'], cell['algorithm'], cell['variant'])].append(cell)
    result = []
    for (engine, algorithm, variant), group in sorted(groups.items()):
        memory = [cell.get('memory') or {} for cell in group]
        result.append(dict(engine=engine, algorithm=algorithm, variant=variant, cells=len(group),
                           execution_samples=observed_range([m.get('execution_samples') for m in memory]),
                           pss_samples=observed_range([m.get('pss_samples') for m in memory]),
                           maximum_scan_seconds=observed_range([m.get('maximum_execution_scan_seconds') for m in memory])))
    return result


def native_widths(cells):
    groups = defaultdict(list)
    for cell in cells:
        if cell['engine'] == 'nutmeg-native':
            groups[(cell['dataset'], cell['algorithm'], cell['variant'])].append(cell)
    return [dict(dataset=dataset, algorithm=algorithm, variant=variant,
                 actual_recorded_widths=sorted({c['trace']['kernel_threads'] for c in group
                                               if c.get('trace', {}).get('kernel_threads') is not None}),
                 actual_width_recorded_cells=sum(c.get('trace', {}).get('kernel_threads') is not None for c in group),
                 cells=len(group)) for (dataset, algorithm, variant), group in sorted(groups.items())]


def splitmix_coefficients(seed):
    state = seed & MASK
    def draw():
        nonlocal state
        state = (state + 0x9e3779b97f4a7c15) & MASK
        value = ((state ^ (state >> 30)) * 0xbf58476d1ce4e5b9) & MASK
        value = ((value ^ (value >> 27)) * 0x94d049bb133111eb) & MASK
        return value ^ (value >> 31)
    while True:
        a = draw()
        while not a:
            a = draw()
        yield a, draw()


def raw_selected(cell, kind):
    if cell['suite'] != 'distributed' or cell['repeat'] != 1:
        return False
    return cell['dataset'] == 'sparse-100000' or (kind == 'main' and
            cell['dataset'] == 'sparse-1000000' and cell['engine'] == 'nutmeg-native')


def markdown(result):
    lines = [f'# Independent {result["kind"]} evidence audit', '',
             f'Recorded {result["recorded_utc"]}. Audit verdict: **{result["audit_outcome"]}**.', '',
             f'{result["planned"]} planned cells; outcomes: ' + ', '.join(f'{n} {k}' for k, n in result['outcomes'].items()) + '.', '',
             'Original trial outcomes are retained unchanged. An audit failure is a separate evidence-integrity finding, not a rewritten benchmark outcome.', '',
             '## Identity and coverage', '',
             *[f'- {key}: `{value}`.' for key, value in result['sources'].items()], '',
             f'Performed {result["checked_files"]} inventory checks across {result["unique_verified_files"]} unique files; reconstructed {result["reconstructed_samples"]} raw memory samples.',
             'Raw correctness coverage: ' + ', '.join(f'{n} {k}' for k, n in result['raw_coverage'].items()) + '.', '',
             'Raw-vector checks read actual Parquet and independently recompute PageRank fixed-point residuals/full-vector errors and WCC components from input edges. Receipt-only checks validate recorded results and hashes; they are not a new calculation from omitted graph rows.', '',
             '## Findings', '']
    lines += [f'- `{i["scope"]}`: {i["message"]}' for i in result['issues']] or ['No audit integrity defects found.']
    if result['warnings']:
        lines += ['', '## Coverage limits', ''] + [f'- `{w["scope"]}`: {w["message"]}' for w in result['warnings']]
    lines += ['', '## Every planned outcome', '', '| Sequence | Cell | Outcome | Correctness audit | Execution samples |',
              '| --- | --- | --- | --- | ---: |']
    for cell in result['cells']:
        lines.append(f'| {cell["sequence"]} | `{cell["cell_id"]}` | {cell["outcome"]} | '
                     f'{cell.get("raw", {}).get("coverage", "no receipt")} | '
                     f'{cell.get("memory", {}).get("execution_samples", "unavailable")} |')
    lines += ['', '## Sampling and contraction detail', '',
              '| Path | Algorithm | Variant | Execution sample count: min / median / max |',
              '| --- | --- | --- | --- |']
    for group in result['sample_coverage']:
        count = group['execution_samples']
        span = f'{count["minimum"]} / {count["median"]} / {count["maximum"]}' if count else 'unavailable'
        lines.append(f'| {group["engine"]} | {group["algorithm"]} | {group["variant"]} | {span} |')
    lines += ['', 'Recorded whole-VM steal fractions: `' + json.dumps(result['steal_observations']) + '`.', '']
    for group in result['cross_path_contractions']:
        lines.append(f'- `{group["dataset"]}`: {group["rounds"]} identical contraction rounds across '
                     f'{len(group["compared_cells"])} successful cells, excluding the first raw-edge input count.')
    lines += ['', 'Native advanced widths are recorded per dataset in the JSON. Native reference actual widths are not exported by these kernels and must not be inferred from requested concurrency.', '',
              'Post-stop cleanup snapshot: `' + json.dumps(result['post_stop_cleanup']) + '`.', '']
    lines += ['', 'RSS/PSS are sampled execution-phase totals; short peaks may be missed. RSS can double-count shared mappings; PSS apportions them. Cgroup peaks include cache and lifetime activity and are not interchangeable with PSS or native admission accounting. No elapsed-time or memory ranking is inferred by this audit.', '',
              'Cross-path WCC trace comparisons check coefficients, active vertices and contracted edges; only the first raw-edge input count may differ for fused plans. Floating PageRank frontier traces are recorded without requiring equality across different reduction orders.', '']
    return '\n'.join(lines)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--repo', type=Path, default=Path('/Users/alexy/src/sail-extensions-poc'))
    parser.add_argument('--kind', choices=('main', 'fusion', 'control'), required=True)
    parser.add_argument('--evidence', type=Path, required=True)
    parser.add_argument('--summary', type=Path)
    parser.add_argument('--raw', type=Path, action='append', default=[])
    parser.add_argument('--require-raw', action='store_true')
    parser.add_argument('--output', type=Path, required=True)
    args = parser.parse_args()
    result = Audit(args).run()
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.with_suffix('.json').write_text(json.dumps(result, indent=2, allow_nan=False) + '\n')
    args.output.with_suffix('.md').write_text(markdown(result))
    print(json.dumps({key: result[key] for key in ('audit_outcome', 'outcomes', 'raw_coverage', 'reconstructed_samples', 'issues')}))
    return int(bool(result['issues']))


if __name__ == '__main__':
    raise SystemExit(main())
