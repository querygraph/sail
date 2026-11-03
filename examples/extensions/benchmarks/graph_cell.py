#!/usr/bin/env python3
"""Measure one fresh Sail trial; invoke in a private, resource-limited container."""
import argparse
from datetime import datetime, timezone
import json
import math
import os
from pathlib import Path
import platform
import signal
import sys
import time
import traceback

from measurement import Sampler, cgroup_snapshot, cpu_ticks, read_text, steal_fraction
from runtime import (algorithm_method, git, native_package_identity, package_versions, record_result_evidence,
                     server, sha256, validate_admission_settings)


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


def validate(spark, output, dataset, algorithm, expected_rows, tolerance, damping, max_iterations, native, optimized):
    actual = spark.read.parquet(output.as_uri())
    reference = spark.read.parquet((dataset / 'reference.parquet').as_uri())
    cardinality = actual.agg(F.count('*').alias('rows'), F.countDistinct('id').alias('unique_ids'),
                             F.sum(F.col('id').isNull().cast('long')).alias('null_ids')).first().asDict()
    assert cardinality == dict(rows=expected_rows, unique_ids=expected_rows, null_ids=0), cardinality
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
            staged = nm.stage('benchmark', nodes, links)
            receipt['stage_seconds'] = time.perf_counter() - started
            receipt['stage_receipt'] = staged.asDict()
            options = dict(concurrency=args.threads)
            if args.algorithm == 'pagerank':
                options.update(damping=args.damping, tolerance=args.tolerance,
                               maxIterations=args.max_iterations, precision='f64', orientation='outgoing')
            elif optimized:
                options.update(maxIterations=args.max_iterations, seed=args.seed)
            kernel = method
            receipt['kernel'] = kernel
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
        write_started = time.perf_counter()
        exported.write.mode('error').parquet(output.as_uri())
        receipt['end_to_end_seconds'] = time.perf_counter() - started
        receipt['output_action_seconds'] = time.perf_counter() - write_started
        receipt['output_action_boundary'] = (
            'includes native kernel/CSR execution' if nm else 'export of already materialized Pecan result')
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


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--sail-binary', type=Path, required=True)
    parser.add_argument('--runtime-source-sha', required=True)
    parser.add_argument('--native-source-sha', help='source revision used for the installed native Nutmeg wheel')
    parser.add_argument('--dataset', type=Path, required=True)
    parser.add_argument('--output', type=Path, required=True)
    parser.add_argument('--engine', choices=['pecan', 'nutmeg-native', 'nutmeg-datafusion'], required=True)
    parser.add_argument('--algorithm', choices=['pagerank', 'wcc'], required=True)
    parser.add_argument('--variant', choices=['reference', 'optimized', 'fused'], default='reference')
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
    for name in ('damping', 'tolerance'):
        assert manifest['pagerank'][name] == getattr(args, name), f'reference {name} differs'
    for name, details in manifest['files'].items():
        assert sha256(args.dataset / name) == details['sha256'], f'dataset changed: {name}'
    receipt = dict(started_utc=utc(), arguments={k: str(v) if isinstance(v, Path) else v for k, v in vars(args).items()},
                   harness_source_sha=git(REPO, 'rev-parse', 'HEAD'), source_dirty=dirty,
                   runtime_source_sha=args.runtime_source_sha, binary_sha256=sha256(args.sail_binary),
                   native_source_sha=args.native_source_sha,
                   native_package_identity=native_package_identity(),
                   packages=package_versions(), platform=platform.platform(), python=sys.version,
                   dataset=manifest, cgroup_before=cgroup_snapshot(),
                   host_load_before=read_text('/proc/loadavg'),
                   installed_extension_boundary='Sedona and Nutmeg loaded for every path; same shared-extension deployment',
                   prepaid_native_quota_bytes=args.native_quota,
                   sail_pool_per_process_bytes=args.sail_pool_bytes,
                   remaining_participating_df_budget_bytes=args.sail_pool_bytes - args.native_quota,
                   worker_task_slots_per_worker=args.worker_task_slots,
                   worker_task_slots_total=args.worker_task_slots * (2 if args.mode == 'process-cluster' else 0),
                   boundary='input DataFrame handles to completed full result Parquet write; server startup and correctness verification excluded',
                   cache_boundary='fresh Sail state; dataset checksum reads before timing warm OS page cache; no cache flushing',
                   outcome='started', cleanup_errors=[])
    sampler = Sampler(args.output / 'memory-samples.jsonl')
    handle = nm = spark = None
    signal.signal(signal.SIGALRM, timeout_handler)
    before_ticks = cpu_ticks()
    try:
        with sampler:
            with server(args.sail_binary, args.output, args.mode, args.partitions, args.threads,
                        args.native_quota, receipt['cleanup_errors'],
                        worker_task_slots=args.worker_task_slots,
                        sail_pool_bytes=args.sail_pool_bytes) as (endpoint, pid):
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
                    receipt['correctness'] = validate(spark, result, args.dataset, args.algorithm,
                        manifest['counts']['vertices'], args.tolerance, args.damping,
                        args.max_iterations, args.engine == 'nutmeg-native', args.variant != 'reference')
                    receipt['outcome'] = 'passed'
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
