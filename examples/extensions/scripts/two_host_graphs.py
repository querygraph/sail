"""Functional two-host exercise for Pecan's relational graph algorithms."""
import math
from pathlib import Path
import sys


def _pagerank_step(ids, edges, ranks, reset):
    outgoing = {node: [] for node in ids}
    for source, target in edges:
        outgoing[source].append(target)
    dangling = sum(ranks[node] for node in ids if not outgoing[node])
    updated = {node: reset / len(ids) + (1 - reset) * dangling / len(ids) for node in ids}
    for source, targets in outgoing.items():
        for target in targets:
            updated[target] += (1 - reset) * ranks[source] / len(targets)
    return updated


def _reference_pagerank(ids, edges, steps, reset):
    ranks = {node: 1 / len(ids) for node in ids}
    for _ in range(steps):
        ranks = _pagerank_step(ids, edges, ranks, reset)
    return ranks


def exercise(endpoint, worker_hosts, evidence, *, pagerank_method="power", wcc_method="min_label",
             pagerank_iterations=None, tolerance=None, wcc_iterations=None, seed=42):
    # Test the client from this exact source distribution, even if another
    # version was previously installed in the controller's Python environment.
    sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "graph-algorithms/src"))
    from pyspark.sql.connect.session import SparkSession
    from pyspark.sql.connect.client.retries import DefaultPolicy
    from pyspark_pecan import GraphAlgorithms, GraphUtils

    spark = SparkSession.builder.remote(endpoint).create()
    spark.client.set_retry_policies([
        DefaultPolicy(max_retries=1, initial_backoff=100, max_backoff=100, jitter=0)
    ])
    try:
        evidence["iteration_windows"] = windows = []
        starts = {}

        def stages():
            return [r.asDict() for r in spark.sql("""
                SELECT CAST(job_id AS BIGINT) AS job_id, CAST(stage AS BIGINT) AS stage,
                       CAST(partitions AS BIGINT) AS partitions, placement
                FROM system.execution.stages
            """).collect()]

        def observe(event):
            if event["kind"] not in {"iteration_start", "iteration_end"}:
                return
            key = event["algorithm"], event["iteration"]
            recorded = stages()
            if event["kind"] == "iteration_start":
                starts[key] = max((row["job_id"] for row in recorded), default=0)
            elif event["kind"] == "iteration_end":
                # Input snapshot and graph validation precede iteration_start.
                # System-table observation queries execute on the driver; only
                # distributed stages in this interval count as algorithm work.
                selected = [row for row in recorded if row["job_id"] > starts[key]
                            and row["placement"] == "Worker" and row["partitions"] >= 2]
                assert selected, f"no distributed algorithm stage in {key}"
                metrics = {name: value for name, value in event.items()
                           if name not in {"kind", "algorithm", "iteration", "run_path"}}
                windows.append(dict(algorithm=key[0], iteration=key[1], stages=selected, metrics=metrics))

        utils = GraphUtils(spark)
        evidence["utils"] = dict(engine=utils.engine, capabilities=sorted(utils.capabilities),
                                 lease_seconds=utils.lease_seconds)
        ids = [0, 1, 2, 3, 4, 5, 6]
        links = [(0, 1), (0, 1), (1, 2), (2, 0), (2, 3), (4, 5), (5, 4)]
        vertices = spark.createDataFrame([(v,) for v in ids], "id long").repartition(4)
        edges = spark.createDataFrame(links, "src long,dst long").repartition(4)
        graph = GraphAlgorithms(spark, observer=observe)
        if pagerank_iterations is None:
            pagerank_iterations = 1000 if pagerank_method == "delta" else 3
        if tolerance is None and pagerank_method == "delta":
            tolerance = 1e-8
        if wcc_iterations is None:
            wcc_iterations = 100 if wcc_method in ("randomized", "randomized_fused") else 10
        evidence["methods"] = dict(pagerank=pagerank_method, wcc=wcc_method,
                                   pagerank_iterations=pagerank_iterations, tolerance=tolerance,
                                   wcc_iterations=wcc_iterations, seed=seed)
        with graph.pagerank(vertices, edges, method=pagerank_method,
                            max_iterations=pagerank_iterations, tolerance=tolerance, partitions=4) as result:
            ranks = {row.id: row.pagerank for row in result.frame.collect()}
            reference = _reference_pagerank(ids, links, 1000 if pagerank_method == "delta" else result.iterations, 0.15)
            assert ranks.keys() == reference.keys()
            assert all(math.isfinite(rank) and rank >= 0 for rank in ranks.values()), ranks
            assert math.isclose(sum(ranks.values()), 1.0, rel_tol=0, abs_tol=1e-12), ranks
            evidence["pagerank"] = dict(ranks=ranks, iterations=result.iterations,
                                        converged=result.converged, algorithm=result.algorithm,
                                        method=pagerank_method)
            if pagerank_method == "delta":
                updated = _pagerank_step(ids, links, ranks, 0.15)
                residual = sum(abs(updated[node] - ranks[node]) for node in ids)
                error = sum(abs(reference[node] - ranks[node]) for node in ids)
                assert result.converged and residual <= tolerance + 2e-13, residual
                assert math.isclose(result.residual, residual, rel_tol=0, abs_tol=2e-13)
                assert error <= result.error_bound + 2e-12, (error, result.error_bound)
                evidence["pagerank"].update(true_fixed_point_residual=residual,
                                             stationary_l1_error=error, error_bound=result.error_bound)
            else:
                assert all(math.isclose(ranks[k], reference[k], rel_tol=1e-10, abs_tol=1e-12)
                           for k in reference), (ranks, reference)
        with graph.wcc(vertices, edges, method=wcc_method, max_iterations=wcc_iterations,
                       seed=seed, partitions=4) as result:
            labels = {row.id: row.component for row in result.frame.collect()}
            assert labels == {0: 0, 1: 0, 2: 0, 3: 0, 4: 4, 5: 4, 6: 6}
            evidence["wcc"] = dict(labels=labels, iterations=result.iterations,
                                   converged=result.converged, algorithm=result.algorithm,
                                   method=wcc_method)
            if wcc_method in ("randomized", "randomized_fused"):
                evidence["wcc"].update(seed=result.seed, contractions=result.contractions)
        # Each selected method must contribute iteration work. Otherwise the
        # launcher's per-method worker-placement check could pass vacuously.
        expected = {evidence["pagerank"]["algorithm"], evidence["wcc"]["algorithm"]}
        assert expected == {window["algorithm"] for window in windows}, windows
        evidence["worker_endpoints"] = [r.asDict() for r in spark.sql("""
            SELECT CAST(worker_id AS BIGINT) AS worker_id, host, CAST(port AS INT) AS port,status
            FROM system.cluster.workers
        """).collect()]
        running = [r for r in evidence["worker_endpoints"] if r["status"] == "RUNNING"]
        assert worker_hosts <= {r["host"] for r in running}
        evidence["stage_placement"] = stages()
        return evidence
    finally:
        spark.stop()
