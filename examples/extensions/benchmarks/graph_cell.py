#!/usr/bin/env python3
"""Measure one fresh Sail trial; invoke in a private, resource-limited container."""
import argparse
from datetime import datetime, timezone
import json
import math
import os
from pathlib import Path
import platform
import resource
import signal
import sys
import tempfile
import time
import traceback

from traversal_source import MAX_DEGREE, parse_source
from measurement import Sampler, cgroup_snapshot, cpu_ticks, read_text, steal_fraction
from runtime import (algorithm_method, git, native_package_identity, package_versions, record_result_evidence,
                     server, sha256, validate_admission_settings)
from validation_outcome import effective_outcome


REPO = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(REPO / 'examples/extensions/graph-algorithms/src'))
os.environ.setdefault('SPARK_CONNECT_MODE_ENABLED', '1')

from pyspark.sql.connect import functions as F
from pyspark.sql.connect.session import SparkSession
from pyspark.sql.connect.client.retries import DefaultPolicy
from pyspark_pecan import ConvergenceError, GraphAlgorithms
from sail_nutmeg.client import Nutmeg


def utc():
    return datetime.now(timezone.utc).isoformat()


def timeout_handler(*_):
    raise TimeoutError('trial wall-clock limit exceeded')


class NonConvergedError(RuntimeError):
    pass


def validate(spark, output, dataset, algorithm, expected_rows, tolerance, damping, max_iterations, native, optimized,
             policy='reference'):
    actual = spark.read.parquet(output.as_uri())
    cardinality = actual.agg(F.count('*').alias('rows'), F.countDistinct('id').alias('unique_ids'),
                             F.sum(F.col('id').isNull().cast('long')).alias('null_ids')).first().asDict()
    assert cardinality == dict(rows=expected_rows, unique_ids=expected_rows, null_ids=0), cardinality
    if policy == 'certificate':
        return certify(spark, actual, dataset, algorithm, expected_rows, tolerance, damping, max_iterations,
                       native, optimized, cardinality)
    reference = spark.read.parquet((dataset / 'reference.parquet').as_uri())
    assert not actual.join(reference.select('id'), 'id', 'left_anti').limit(1).count()
    if algorithm == 'wcc':
        # Native labels follow canonical UTF8 row order; Pecan labels use numeric
        # order. Compare partitions after the SAME normalization for every path.
        minima = actual.groupBy('component').agg(F.min('id').alias('canonical'))
        canonical = actual.join(minima, 'component').select('id', 'canonical')
        joined = canonical.join(reference.select('id', 'component'), 'id')
        mismatches = joined.where(F.col('canonical') != F.col('component')).count()
        assert mismatches == 0, f'{mismatches} WCC membership mismatches'
        assert not actual.where(F.col('component').isNull()).limit(1).count()
        return dict(**cardinality, membership_mismatches=mismatches,
                    components=minima.count(), canonicalization='minimum numeric vertex ID per output component')
    invalid = actual.where(F.col('score').isNull() | F.isnan('score') |
                           (F.abs(F.col('score')) == F.lit(float('inf'))) | (F.col('score') < 0)).count()
    assert invalid == 0, f'{invalid} invalid PageRank scores'
    invalid_meta = actual.where(F.col('iterations').isNull() | F.col('converged').isNull() |
                                (F.col('iterations') < (0 if optimized else 1)) |
                                (F.col('iterations') > max_iterations)).count()
    assert invalid_meta == 0, f'{invalid_meta} invalid PageRank metadata rows'
    if native or optimized:
        invalid_residual = actual.where(F.col('residual').isNull() | F.isnan('residual') |
                                       (F.col('residual') < 0) |
                                       (F.col('residual') == F.lit(float('inf')))).count()
        assert invalid_residual == 0, f'{invalid_residual} invalid native residual rows'
    joined = actual.join(reference.select('id', 'pagerank'), 'id')
    checks = joined.agg(
        F.sum('score').alias('rank_sum'),
        F.sum(F.abs(F.col('score') - F.col('pagerank'))).alias('l1_error'),
        F.max(F.abs(F.col('score') - F.col('pagerank'))).alias('max_error'),
        F.min('iterations').alias('min_iterations'), F.max('iterations').alias('max_iterations'),
        F.min(F.col('converged').cast('int')).alias('all_converged'),
        F.max('residual').alias('reported_residual'),
        F.min('residual').alias('min_reported_residual'),
    ).first().asDict()
    if checks['all_converged'] != 1:
        raise NonConvergedError(f'native PageRank reached its iteration cap: {checks}')
    assert checks['min_iterations'] == checks['max_iterations'], checks
    assert checks['reported_residual'] == checks['min_reported_residual'], checks
    assert math.isclose(checks['rank_sum'], 1.0, abs_tol=1e-10), checks
    # Optimized algorithms certify the true fixed-point residual; the reference
    # stops at successive-step L1 tolerance. Sum the two stationary-error bounds.
    error_limit = ((1 if optimized else damping) + damping) * tolerance / (1 - damping) + 1e-12
    assert checks['l1_error'] <= error_limit, checks
    if checks['reported_residual'] is not None:
        assert checks['reported_residual'] <= tolerance, checks
    # Independently recompute the SAME fixed-point residual for every method,
    # including the baseline kernels whose reported residual is last-step L1.
    edges = spark.read.parquet((dataset / 'edges.parquet').as_uri())
    degrees = edges.groupBy('src').count().withColumnRenamed('count', 'degree')
    weighted = edges.join(degrees, 'src')
    incoming = weighted.join(actual, weighted.src == actual.id).select(
        weighted.dst.alias('id'), (actual.score / weighted.degree).alias('message')
    ).groupBy('id').agg(F.sum('message').alias('incoming'))
    dangling = actual.join(degrees, actual.id == degrees.src, 'left_anti').agg(F.sum('score')).first()[0] or 0.0
    combined = actual.join(incoming, 'id', 'left')
    true_residual = combined.agg(F.sum(F.abs(
        F.lit((1 - damping) / expected_rows + damping * dangling / expected_rows) +
        F.lit(damping) * F.coalesce(F.col('incoming'), F.lit(0.0)) - F.col('score')
    ))).first()[0]
    assert math.isfinite(true_residual) and true_residual <= tolerance + 1e-12, true_residual
    return dict(**cardinality, **checks, l1_error_limit=error_limit,
                true_fixed_point_residual=true_residual,
                stationary_l1_error_bound=true_residual / (1 - damping))


def execute(spark, args, manifest, receipt, sampler):
    if args.algorithm in ('bfs', 'sssp'):
        from traversal_cell import execute as traversal_execute
        return traversal_execute(spark, args, receipt, sampler)
    vertices = spark.read.parquet((args.dataset / 'vertices.parquet').as_uri())
    edges = spark.read.parquet((args.dataset / 'edges.parquet').as_uri())
    output = args.output / 'result'
    events = []
    handle = None
    nm = None
    sampler.mark('execute')
    started = time.perf_counter()
    receipt['execution_started_utc'] = utc()
    try:
        optimized = args.variant != 'reference'
        method = algorithm_method(args.engine, args.algorithm, args.variant)
        if args.engine == 'nutmeg-native':
            nm = Nutmeg(spark)
            nodes = vertices.select(F.col('id').cast('string').alias('node_id'))
            links = edges.select(F.col('src').cast('string').alias('source'),
                                 F.col('dst').cast('string').alias('target'))
            # Staging, projection and kernel are timed and sampled as separate
            # steps of the one 'execute' phase. The stage receipt carries the
            # admitted sort tiers; the status after each step carries the
            # pool's live and peak bytes; projectionStats builds the kernel's
            # projection ahead of it and reports the CSR bytes. Whether the
            # kernel then reused that projection is checked after the run.
            sampler.mark_step('stage')
            from traversal_cell import stage_order
            staged = nm.stage('benchmark', nodes, links, order=stage_order(args))
            receipt['stage_seconds'] = time.perf_counter() - started
            receipt['stage_receipt'] = staged.asDict()
            receipt['native_status_after_stage'] = nm.status()
            options = dict(concurrency=args.threads)
            if args.algorithm == 'pagerank':
                options.update(damping=args.damping, tolerance=args.tolerance,
                               maxIterations=args.max_iterations, precision='f64', orientation='outgoing')
            elif optimized:
                options.update(maxIterations=args.max_iterations, seed=args.seed)
            kernel = method
            receipt['kernel'] = kernel
            sampler.mark_step('projection')
            projection_started = time.perf_counter()
            projection_options = {key: options[key] for key in ('orientation',) if key in options}
            receipt['projection_stats'] = nm.run('benchmark', 'projectionStats', **projection_options).first().asDict()
            receipt['projection_seconds'] = time.perf_counter() - projection_started
            receipt['native_status_after_projection'] = nm.status()
            sampler.mark_step('kernel')
            kernel_started = time.perf_counter()
            frame = nm.run('benchmark', kernel, **options)
            if args.algorithm == 'pagerank':
                exported = frame.select(F.col('nodeId').cast('long').alias('id'), 'score',
                                        F.col('iterations').cast('long'), 'converged', 'residual')
            else:
                exported = frame.select(F.col('nodeId').cast('long').alias('id'),
                                        F.col('componentId').cast('string').alias('component'))
        else:
            if args.engine == 'nutmeg-datafusion':
                tables = Nutmeg(spark).tables(vertices, edges, node_id='id', source='src', target='dst')
                vertices = tables.nodes.select(F.col('node_id').alias('id'))
                edges = tables.edges.select(F.col('source').alias('src'), F.col('target').alias('dst'))
            graph = GraphAlgorithms(spark, observer=lambda event: events.append(
                dict({key: value for key, value in event.items() if key != 'run_path'},
                     elapsed_seconds=time.perf_counter() - started)))
            options = dict(max_iterations=args.max_iterations, partitions=args.partitions)
            if args.algorithm == 'pagerank':
                options.update(reset_probability=1 - args.damping, tolerance=args.tolerance,
                               method=method)
            else:
                options.update(method=method, seed=args.seed)
            receipt['kernel'] = options['method']
            sampler.mark_step('rounds')
            handle = getattr(graph, args.algorithm)(vertices, edges, **options)
            receipt['algorithm_ready_seconds'] = time.perf_counter() - started
            receipt['algorithm_iterations'] = handle.iterations
            receipt['algorithm_converged'] = handle.converged
            if args.algorithm == 'pagerank':
                exported = handle.frame.select('id', F.col('pagerank').alias('score'),
                    F.lit(handle.iterations).cast('long').alias('iterations'),
                    F.lit(handle.converged).alias('converged'),
                    F.lit(getattr(handle, 'residual', None)).cast('double').alias('residual'))
            else:
                exported = handle.frame.select('id', F.col('component').cast('string').alias('component'))
        if nm is None:
            sampler.mark_step('output')
        write_started = time.perf_counter()
        exported.write.mode('error').parquet(output.as_uri())
        receipt['end_to_end_seconds'] = time.perf_counter() - started
        receipt['output_action_seconds'] = time.perf_counter() - write_started
        if nm is not None:
            receipt['kernel_and_output_seconds'] = time.perf_counter() - kernel_started
            receipt['output_action_boundary'] = ('native kernel execution on the projection built by projectionStats, '
                                                 'plus the result write; the kernel is lazy until the write')
        else:
            receipt['output_action_boundary'] = 'export of already materialized Pecan result'
            rounds = [e['elapsed_seconds'] for e in events if e.get('kind') == 'iteration_end']
            if rounds:
                receipt['round_end_seconds'] = rounds
                receipt['first_round_seconds'] = rounds[0]
                receipt['mean_round_seconds'] = rounds[-1] / len(rounds)
        return output, handle, nm
    except BaseException:
        receipt['elapsed_until_error_seconds'] = time.perf_counter() - started
        receipt['cgroup_execution_after'] = cgroup_snapshot()
        sampler.mark('error_diagnostics')
        if nm is not None:
            try:
                signal.alarm(10)
                receipt['native_status_on_error'] = nm.status()
            except BaseException as diagnostic_error:
                receipt['native_diagnostic_error'] = repr(diagnostic_error)
            finally:
                signal.alarm(0)
        raise
    finally:
        receipt['iteration_events'] = events


def certify(spark, actual, dataset, algorithm, expected_rows, tolerance, damping, max_iterations, native, optimized, cardinality):
    """Check a ranking result without a reference vector, on inputs too large for one.

    PageRank: every vertex present once, finite nonnegative scores summing to
    one, convergence claimed by every row, and the true fixed-point L1 residual
    recomputed from the edges at or below the tolerance. WCC: every vertex
    labeled, both endpoints of every edge share a label, and every label names
    an input vertex. The label's membership in its own group is not checked.
    Numeric minimality is reported, not required, since native min-label uses
    canonical Utf8 order ("10" before "9"). Component connectivity and count
    are not verified: coarser partitions can merge disconnected components and
    still pass these partial checks.
    """
    vertices = spark.read.parquet((dataset / 'vertices.parquet').as_uri()).select('id')
    assert not vertices.join(actual.select('id'), 'id', 'left_anti').limit(1).count(), 'a vertex is missing from the result'
    edges = spark.read.parquet((dataset / 'edges.parquet').as_uri()).select('src', 'dst')
    if algorithm == 'wcc':
        assert not actual.where(F.col('component').isNull()).limit(1).count()
        labels = actual.select('id', F.col('component').cast('string').alias('label'))
        left = edges.join(labels, edges.src == labels.id).select(F.col('dst'), F.col('label').alias('src_label'))
        crossing = left.join(labels, left.dst == labels.id).where(F.col('src_label') != F.col('label')).count()
        assert crossing == 0, f'{crossing} edges cross component labels'
        minima = labels.groupBy('label').agg(F.min('id').alias('minimum'))
        foreign = minima.join(labels, minima.label.cast('long') == labels.id, 'left_anti').count()
        assert foreign == 0, f'{foreign} labels do not name input vertices'
        non_minimal = minima.where(F.col('label').cast('long') != F.col('minimum')).count()
        return dict(**cardinality, policy='certificate', crossing_edges=crossing, foreign_labels=foreign,
                    non_minimal_labels=non_minimal,
                    label_convention='numeric minimum' if non_minimal == 0 else 'not the numeric minimum; representative membership unverified',
                    components=minima.count(), component_count_verified=False,
                    verification_scope='partial_wcc_partition',
                    certificate='edge-consistent partition whose labels name input vertices; component connectivity and count unverified')
    invalid = actual.where(F.col('score').isNull() | F.isnan('score') |
                           (F.abs(F.col('score')) == F.lit(float('inf'))) | (F.col('score') < 0)).count()
    assert invalid == 0, f'{invalid} invalid PageRank scores'
    checks = actual.agg(F.sum('score').alias('rank_sum'), F.min('iterations').alias('min_iterations'),
                        F.max('iterations').alias('max_iterations'),
                        F.min(F.col('converged').cast('int')).alias('all_converged'),
                        F.max('residual').alias('reported_residual')).first().asDict()
    if checks['all_converged'] != 1:
        raise NonConvergedError(f'PageRank reached its iteration cap: {checks}')
    assert math.isclose(checks['rank_sum'], 1.0, abs_tol=1e-10), checks
    degrees = edges.groupBy('src').count().withColumnRenamed('count', 'degree')
    weighted = edges.join(degrees, 'src')
    incoming = weighted.join(actual, weighted.src == actual.id).select(
        weighted.dst.alias('id'), (actual.score / weighted.degree).alias('message')
    ).groupBy('id').agg(F.sum('message').alias('incoming'))
    dangling = actual.join(degrees, actual.id == degrees.src, 'left_anti').agg(F.sum('score')).first()[0] or 0.0
    combined = actual.join(incoming, 'id', 'left')
    true_residual = combined.agg(F.sum(F.abs(
        F.lit((1 - damping) / expected_rows + damping * dangling / expected_rows) +
        F.lit(damping) * F.coalesce(F.col('incoming'), F.lit(0.0)) - F.col('score')
    ))).first()[0]
    assert math.isfinite(true_residual) and true_residual <= tolerance + 1e-12, true_residual
    return dict(**cardinality, **checks, policy='certificate', true_fixed_point_residual=true_residual,
                stationary_l1_error_bound=true_residual / (1 - damping),
                certificate='independent fixed-point residual at or below tolerance; no reference vector')


def validate_dataset_identity(manifest, *, family=None, vertices=None, graph500_sha256=None,
                              input_sha256=None, edge_sha256=None, weight_policy=None, weight_seed=None):
    """Check matrix expectations even when input preparation was skipped."""
    if family is not None and manifest.get('family') != family:
        raise ValueError('dataset family differs from matrix configuration')
    if vertices is not None and manifest.get('counts', {}).get('vertices') != vertices:
        raise ValueError('dataset vertex count differs from matrix configuration')
    if graph500_sha256 is not None:
        if manifest.get('family') != 'graph500':
            raise ValueError('Graph500 input pin requires a Graph500 manifest')
        actual = manifest.get('canonical', {}).get('edges', {}).get('sha256')
        if actual != graph500_sha256:
            raise ValueError('Graph500 canonical edge bytes differ from matrix configuration')
    if any(value is not None for value in (input_sha256, edge_sha256, weight_policy, weight_seed)):
        if manifest.get('family') not in ('edge-list-traversal', 'snap-edge-list'):
            raise ValueError('imported traversal identity requires an edge-list-traversal or snap-edge-list manifest')
        if input_sha256 is not None and manifest.get('input', {}).get('sha256') != input_sha256:
            raise ValueError('original topology bytes differ from matrix configuration')
        if edge_sha256 is not None and manifest.get('canonical', {}).get('edges', {}).get('sha256') != edge_sha256:
            raise ValueError('canonical weighted edge bytes differ from matrix configuration')
        for key, expected in (('weight_policy', weight_policy), ('weight_seed', weight_seed)):
            if expected is not None and manifest.get('traversal', {}).get(key) != expected:
                raise ValueError(f'{key} differs from matrix configuration')


def resolve_source(requested, traversal):
    """The vertex a traversal cell starts from: the request, or the manifest's `max-degree` choice.

    A `max-degree` request is honored only by a dataset prepared with that policy, so
    the source and its recorded degree come from the same preparation; an explicit
    vertex must match the manifest, since the fixture pins the source it prepared for.
    """
    if requested == MAX_DEGREE:
        if not str(traversal.get('source_policy', '')).startswith('highest-degree'):
            raise ValueError(f'{MAX_DEGREE} source requires a dataset prepared with --source {MAX_DEGREE}')
        return int(traversal['source'])
    if traversal['source'] != requested:
        raise ValueError(f"dataset was prepared for source {traversal['source']}, not {requested}")
    return requested


def validate_dataset_files(dataset, manifest):
    """Reject added/missing input files, including partitions absent from the pin.

    Readers load entire Parquet directories. Checking only listed file hashes
    would let an additional partition silently change both execution and its
    later certificate. Input trees must therefore have exactly the pinned files.
    """
    dataset = Path(dataset)
    roots = ('vertices.parquet', 'edges.parquet', 'reference.parquet')
    expected = manifest.get('files', {})
    if not isinstance(expected, dict) or not expected:
        raise ValueError('dataset manifest must pin its input files')
    for name in expected:
        path = Path(name)
        if path.is_absolute() or '..' in path.parts or path.as_posix() != name or path.parts[0] not in roots:
            raise ValueError('dataset manifest file must remain inside its input tree')
    actual = set()
    for name in roots:
        path = dataset / name
        if name != 'reference.parquet' and not path.exists():
            raise ValueError(f'dataset is missing required input: {name}')
        for item in (path, *path.rglob('*')) if path.is_dir() else (path,):
            if item.is_symlink():
                raise ValueError('dataset input symlinks are not permitted')
            if item.is_file():
                actual.add(item.relative_to(dataset).as_posix())
    if actual != set(expected):
        raise ValueError('dataset input file inventory differs from manifest')
    for name, details in expected.items():
        if sha256(dataset / name) != details['sha256']:
            raise ValueError(f'dataset changed: {name}')


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--sail-binary', type=Path, required=True)
    parser.add_argument('--runtime-source-sha', required=True)
    parser.add_argument('--native-source-sha', help='source revision used for the installed native Nutmeg wheel')
    parser.add_argument('--dataset', type=Path, required=True)
    parser.add_argument('--expected-dataset-family')
    parser.add_argument('--expected-vertices', type=int)
    parser.add_argument('--expected-graph500-sha256')
    parser.add_argument('--expected-input-sha256')
    parser.add_argument('--expected-edge-sha256')
    parser.add_argument('--expected-weight-policy')
    parser.add_argument('--expected-weight-seed', type=int)
    parser.add_argument('--output', type=Path, required=True)
    parser.add_argument('--engine', choices=['pecan', 'nutmeg-native', 'nutmeg-datafusion', 'argentea'], required=True)
    parser.add_argument('--algorithm', choices=['pagerank', 'wcc', 'bfs', 'sssp'], required=True)
    parser.add_argument('--variant', choices=['reference', 'optimized', 'fused', 'frontier', 'delta_star', 'push_pull'], default='reference')
    parser.add_argument('--source', type=parse_source, default=0,
                        help=f'traversal source: a vertex id, or {MAX_DEGREE} as prepared by the fixture')
    parser.add_argument('--directed', action=argparse.BooleanOptionalAction, default=True)
    parser.add_argument('--delta', type=float, default=1.0)
    parser.add_argument('--traversal-validation', choices=['reference', 'certificate'], default='reference')
    parser.add_argument('--http2-keepalive-timeout', type=int, default=120,
                        help='seconds a Sail server waits for a keepalive ping answer before dropping the connection (host default 10)')
    parser.add_argument('--record-plans', action='store_true',
                        help='Pecan/Grenada traversal cells: record the physical plan of each iteration in its iteration_start event')
    parser.add_argument('--argentea-max-rounds', type=int, default=62,
                        help='Argentea BFS levels / SSSP rounds cap; the native phase budget is 2*cap+4 (at most 128)')
    parser.add_argument('--stage-order', choices=['canonical', 'asStaged'], default='canonical',
                        help='native staging order: canonical sorts every staged row after admitting the sort working space; '
                             'asStaged keeps arrival order and skips the sort (sent to the server only when not canonical)')
    parser.add_argument('--ranking-validation', choices=['reference', 'certificate'], default='reference',
                        help='compare with reference.parquet, or check a PageRank residual / partial WCC partition without one')
    parser.add_argument('--certificate-max-rounds', type=int, default=10000)
    parser.add_argument('--seed', type=int, default=42)
    parser.add_argument('--mode', choices=['local', 'process-cluster'], default='process-cluster')
    parser.add_argument('--partitions', type=int, default=8)
    parser.add_argument('--threads', type=int, default=8)
    parser.add_argument('--worker-task-slots', type=int, default=32,
                        help='concurrent asynchronous task capacity per worker, independent of CPU threads')
    parser.add_argument('--sail-pool-bytes', type=int, default=16 * 1024**3,
                        help='participating Sail memory pool per driver/worker process')
    parser.add_argument('--native-quota', type=int, default=8 * 1024**3)
    parser.add_argument('--tolerance', type=float, default=1e-8)
    parser.add_argument('--damping', type=float, default=0.85)
    parser.add_argument('--max-iterations', type=int, default=1000)
    parser.add_argument('--timeout', type=int, default=600)
    parser.add_argument('--repeat', type=int, default=0)
    parser.add_argument('--allow-unisolated', action='store_true', help='functional smoke only, no publishable measurements')
    parser.add_argument('--allow-dirty', action='store_true', help='development smoke only')
    args = parser.parse_args()
    try:
        validate_admission_settings(args.worker_task_slots, args.sail_pool_bytes, args.native_quota)
        algorithm_method(args.engine, args.algorithm, args.variant)
    except ValueError as error:
        parser.error(str(error))
    args.dataset = args.dataset.resolve()
    args.output = args.output.resolve()
    args.sail_binary = args.sail_binary.resolve()
    if not args.allow_unisolated and not (Path('/.dockerenv').exists() and Path('/proc/stat').exists()):
        parser.error('run each measured trial in a fresh Linux container with its own PID namespace')
    dirty = git(REPO, 'status', '--porcelain')
    if dirty and not args.allow_dirty:
        parser.error('benchmark source must be a clean frozen checkout')
    args.output.mkdir(parents=True, exist_ok=False)
    manifest = json.loads((args.dataset / 'manifest.json').read_text())
    validate_dataset_identity(manifest, family=args.expected_dataset_family,
                              vertices=args.expected_vertices,
                              graph500_sha256=args.expected_graph500_sha256,
                              input_sha256=args.expected_input_sha256, edge_sha256=args.expected_edge_sha256,
                              weight_policy=args.expected_weight_policy, weight_seed=args.expected_weight_seed)
    for name in (() if args.algorithm in ('bfs', 'sssp') or args.ranking_validation == 'certificate' else ('damping', 'tolerance')):
        assert manifest['pagerank'][name] == getattr(args, name), f'reference {name} differs'
    validate_dataset_files(args.dataset, manifest)
    source_request = args.source
    if args.algorithm in ('bfs', 'sssp'):
        args.source = resolve_source(args.source, manifest['traversal'])
        assert manifest['traversal']['directed'] == args.directed
    receipt = dict(started_utc=utc(), arguments={k: str(v) if isinstance(v, Path) else v for k, v in vars(args).items()},
                   source_request=source_request, source_degree=manifest.get('traversal', {}).get('source_degree'),
                   source_policy=manifest.get('traversal', {}).get('source_policy'),
                   harness_source_sha=git(REPO, 'rev-parse', 'HEAD'), source_dirty=dirty,
                   runtime_source_sha=args.runtime_source_sha, binary_sha256=sha256(args.sail_binary),
                   native_source_sha=args.native_source_sha,
                   native_package_identity=native_package_identity(),
                   packages=package_versions(), platform=platform.platform(), python=sys.version,
                   dataset=manifest, cgroup_before=cgroup_snapshot(),
                   host_load_before=read_text('/proc/loadavg'),
                   installed_extension_boundary='Experimental extension loading enabled; installed package inventory is recorded separately and does not by itself prove runtime use',
                   prepaid_native_quota_bytes=args.native_quota,
                   sail_pool_per_process_bytes=args.sail_pool_bytes,
                   remaining_participating_df_budget_bytes=args.sail_pool_bytes - args.native_quota,
                   worker_task_slots_per_worker=args.worker_task_slots,
                   worker_task_slots_total=args.worker_task_slots * (2 if args.mode == 'process-cluster' else 0),
                   boundary='input DataFrame handles to completed full result Parquet write; server startup and correctness verification excluded',
                   cache_boundary='fresh Sail state; dataset checksum reads before timing warm OS page cache; no cache flushing',
                   outcome='started', cleanup_errors=[])
    soft_nofile, hard_nofile = resource.getrlimit(resource.RLIMIT_NOFILE)
    receipt['rlimit_nofile'] = {'soft': soft_nofile, 'hard': hard_nofile}
    receipt['temporary_directory'] = tempfile.gettempdir()
    sampler = Sampler(args.output / 'memory-samples.jsonl',
                      watch={'staging': args.output / 'staging', 'temporary': tempfile.gettempdir()})
    handle = nm = spark = None
    signal.signal(signal.SIGALRM, timeout_handler)
    before_ticks = cpu_ticks()
    try:
        with sampler:
            with server(args.sail_binary, args.output, args.mode, args.partitions, args.threads,
                        args.native_quota, receipt['cleanup_errors'],
                        worker_task_slots=args.worker_task_slots,
                        sail_pool_bytes=args.sail_pool_bytes,
                        http2_keepalive_timeout=args.http2_keepalive_timeout) as (endpoint, pid):
                receipt['driver_pid'] = pid
                spark = SparkSession.builder.remote(endpoint).create()
                spark.client.set_retry_policies([DefaultPolicy(max_retries=1, initial_backoff=100, max_backoff=100, jitter=0)])
                signal.alarm(60)
                assert spark.sql('SELECT 1 AS ready').first().ready == 1
                signal.alarm(0)
                sampler.mark('baseline')
                time.sleep(3 * sampler.interval)
                receipt['cgroup_execution_before'] = cgroup_snapshot()
                signal.alarm(args.timeout)
                try:
                    result, handle, nm = execute(spark, args, manifest, receipt, sampler)
                    sampler.mark('verification')
                    signal.alarm(args.timeout)
                    receipt['cgroup_execution_after'] = cgroup_snapshot()
                    record_result_evidence(result, nm, receipt['kernel'], manifest['counts']['vertices'], receipt)
                    if nm is not None:
                        graphs = [g for g in receipt['native_status_after'].get('graphs', []) if g['name'] == 'benchmark']
                        builds = graphs[0].get('projection_builds', []) if graphs else []
                        receipt['projection_builds'] = builds
                        receipt['projection_reused_by_kernel'] = len(builds) == 1
                        receipt['projection_boundary'] = (
                            'one projection built by projectionStats and reused by the kernel' if len(builds) == 1
                            else f'{len(builds)} projections built: the kernel did not reuse the projectionStats build, '
                                 'so kernel_and_output_seconds includes a second build')
                    if args.algorithm in ('bfs', 'sssp'):
                        from traversal_cell import validate as traversal_validate
                        receipt['correctness'] = traversal_validate(spark, result, args.dataset, args.algorithm,
                            manifest['counts']['vertices'], args.source, args.directed, args.engine == 'nutmeg-native',
                            policy=args.traversal_validation, certificate_max_rounds=args.certificate_max_rounds,
                            partitions=args.partitions)
                    else:
                        receipt['correctness'] = validate(spark, result, args.dataset, args.algorithm,
                            manifest['counts']['vertices'], args.tolerance, args.damping,
                            args.max_iterations, args.engine == 'nutmeg-native', args.variant != 'reference',
                            policy=args.ranking_validation)
                    receipt['outcome'] = effective_outcome(receipt, outcome='passed')
                finally:
                    active_error = sys.exc_info()[1]
                    signal.alarm(0)
                    sampler.mark('cleanup')
                    operations = [('stop_session', spark.stop)]
                    if nm is not None:
                        operations.insert(0, ('drop_native_graph', lambda: nm.drop('benchmark')))
                    if handle is not None:
                        operations.insert(0, ('close_pecan_result', handle.close))
                    for name, operation in operations:
                        try:
                            signal.alarm(30)
                            operation()
                        except BaseException as error:
                            receipt['cleanup_errors'].append({'operation': name, 'error': repr(error)})
                        finally:
                            signal.alarm(0)
                    if receipt['cleanup_errors'] and active_error is None:
                        raise RuntimeError('cleanup failed; see cleanup_errors')
        receipt['staging_files_after_shutdown'] = [str(p.relative_to(args.output))
                                                   for p in (args.output / 'staging').rglob('*.parquet')]
        assert not receipt['staging_files_after_shutdown'], receipt['staging_files_after_shutdown']
        assert sampler.error is None, sampler.error
        assert git(REPO, 'rev-parse', 'HEAD') == receipt['harness_source_sha'], 'source HEAD moved'
        if not args.allow_dirty:
            assert not git(REPO, 'status', '--porcelain'), 'source became dirty during the trial'
    except BaseException as error:
        native_wcc_cap = (args.engine == 'nutmeg-native' and args.algorithm == 'wcc' and
                          args.variant in ('optimized', 'fused') and
                          any(f'{kernel} did not converge within maxIterations=' in str(error)
                              for kernel in ('wccRandomized', 'wccRandomizedFused')))
        receipt['outcome'] = ('timeout' if isinstance(error, TimeoutError) else
            'nonconverged' if isinstance(error, (ConvergenceError, NonConvergedError)) or native_wcc_cap else
            'mismatch' if isinstance(error, AssertionError) else 'error')
        receipt['error'] = traceback.format_exc()
    finally:
        signal.alarm(0)
        receipt.update(finished_utc=utc(), memory=sampler.receipt(), cgroup_after=cgroup_snapshot(),
                       guest_steal_fraction=steal_fraction(before_ticks, cpu_ticks()),
                       steal_scope='whole Linux VM from /proc/stat, not container or assigned cpuset',
                       host_load_after=read_text('/proc/loadavg'))
        (args.output / 'receipt.json').write_text(json.dumps(receipt, indent=2, default=str) + '\n')
        print(json.dumps({k: receipt[k] for k in ('outcome', 'harness_source_sha')}), flush=True)
    return 0 if receipt['outcome'] == 'passed' else 1


if __name__ == '__main__':
    raise SystemExit(main())
