"""Relational graph algorithms; only bounded receipts/scalar reductions collect."""

import math
import warnings

from pyspark.sql.connect import functions as F
from pyspark.sql.types import LongType

from .lifecycle import CancellationToken, GraphCancelledError
from .staging import StagingRun
from .utils import GraphUtils


class ConvergenceError(RuntimeError):
    """The iteration limit was reached before the requested stopping rule."""


def _positive_integer(value, name):
    if isinstance(value, bool) or not isinstance(value, int) or value < 1:
        raise ValueError(f"{name} must be a positive integer")


def _check_input_schema(spark, vertices, edges):
    if vertices.sparkSession is not spark or edges.sparkSession is not spark:
        raise ValueError("vertices and edges must belong to this Spark session")
    for frame, columns in ((vertices, ("id",)), (edges, ("src", "dst"))):
        if len(frame.columns) != len(set(frame.columns)):
            raise ValueError("graph inputs require unique column names")
        for column in columns:
            if column not in frame.columns or not isinstance(frame.schema[column].dataType, LongType):
                raise ValueError(f"graph column {column!r} must have BIGINT type")


def _snapshot(run, vertices, edges, edge_columns=("src", "dst")):
    _, vertices = run.materialize(vertices.select("id"), key="id")
    _, edges = run.materialize(edges.select(*edge_columns), key="src")
    run.cancellation.check()
    if vertices.where(F.col("id").isNull()).limit(1).count():
        raise ValueError("vertex IDs must not be null")
    run.cancellation.check()
    if vertices.groupBy("id").count().where(F.col("count") > 1).limit(1).count():
        raise ValueError("vertex IDs must be unique")
    run.cancellation.check()
    if edges.where(F.col("src").isNull() | F.col("dst").isNull()).limit(1).count():
        raise ValueError("edge endpoints must not be null")
    for endpoint in ("src", "dst"):
        run.cancellation.check()
        if edges.join(vertices, edges[endpoint] == vertices.id, "left_anti").limit(1).count():
            raise ValueError(f"edge {endpoint} does not reference a vertex")
    run.cancellation.check()
    return vertices, edges, vertices.count()


class GraphAlgorithms:
    """Graph algorithms over BIGINT id/src/dst tables.

    Input tables are separately materialized once before validation. This is
    stable during an algorithm, but is not an atomic snapshot across mutable
    sources. Results contain structural columns, not input properties.
    """

    def __init__(self, spark, *, observer=None, layout="shuffle"):
        """`layout="declared"` needs the Nutmeg extension loaded in Sail; see StagingRun."""
        self.spark = spark
        self.utils = GraphUtils(spark)
        self.observer = observer
        self.layout = layout
        self.nutmeg = None
        if layout == "declared":
            from sail_nutmeg.client import Nutmeg
            self.nutmeg = Nutmeg(spark)
        elif layout != "shuffle":
            raise ValueError("layout must be 'shuffle' or 'declared'")

    def _observe(self, run, algorithm, step, kind, **metrics):
        if self.observer is not None:
            self.observer({"kind": kind, "algorithm": algorithm,
                           "iteration": step, "run_path": run.path, **metrics})

    def _run(self, vertices, edges, partitions, cancellation, body, *, edge_columns=("src", "dst")):
        _positive_integer(partitions, "partitions")
        cancellation = cancellation or CancellationToken()
        cancellation.check()
        _check_input_schema(self.spark, vertices, edges)
        cancellation.attach(self.spark)
        run = None
        try:
            run = StagingRun(self.spark, self.utils, cancellation, partitions,
                             layout=self.layout, nutmeg=self.nutmeg)
            vertices, edges, size = _snapshot(run, vertices, edges, edge_columns)
            return body(run, vertices, edges, size)
        except BaseException as error:
            # Remove the query tag before issuing cleanup, so cancellation of
            # the algorithm cannot accidentally target its cleanup operation.
            cancellation.detach()
            terminal = error
            if cancellation.cancelled and not isinstance(error, GraphCancelledError):
                terminal = GraphCancelledError("graph algorithm cancelled")
            if run is not None:
                terminal.run_path = run.path
                terminal.cleanup_deferred = run.write_uncertain
                message = None
                if run.write_uncertain:
                    message = (
                        f"write completion is uncertain; cleanup deferred for {run.path}. "
                        "The server session retains ownership and attempts cleanup at teardown; "
                        "this is best effort, not a guarantee that all writers have drained."
                    )
                else:
                    try:
                        run.close()
                    except Exception as cleanup_error:
                        terminal.cleanup_deferred = True
                        message = f"graph cleanup failed; session teardown will retry: {cleanup_error}"
                if message is not None:
                    if hasattr(terminal, "add_note"):
                        terminal.add_note(message)
                    else:
                        warnings.warn(message, RuntimeWarning)
            if terminal is not error:
                raise terminal from error
            raise
        finally:
            cancellation.detach()

    def pagerank(self, vertices, edges, *, reset_probability=0.15,
                 max_iterations=20, tolerance=None, partitions=4, cancellation=None, method="power"):
        """Probability-normalized directed PageRank with uniform restart.

        Initialize rank=1/N. At each step, redistribute dangling rank uniformly,
        then set rank(v)=reset/N+(1-reset)*(incoming(v)+dangling/N). Parallel edges
        count separately; self-loops and isolated vertices are retained.

        The default method="power" performs exactly max_iterations when
        tolerance=None (converged=None), or stops at L1 rank change <= tolerance.
        method="delta" retains unsent residual and pushes a tolerance-scaled
        active frontier; inactive vertices can accumulate updates and reactivate.
        It requires a positive tolerance and certifies the normalized output's
        full fixed-point L1 residual. Its result exposes residual/error_bound.
        Set an explicit larger cap, e.g. 1000, for strict delta tolerances.
        Frontier joins can still scan the edge table. Both tolerance-controlled
        methods raise ConvergenceError at the limit. Output: id BIGINT,
        pagerank DOUBLE. Fixed-step power behavior remains unchanged.
        """
        _positive_integer(max_iterations, "max_iterations")
        if (isinstance(reset_probability, bool) or not isinstance(reset_probability, (int, float))
                or not math.isfinite(reset_probability) or not 0 < reset_probability <= 1):
            raise ValueError("reset_probability must be finite and in (0, 1]")
        if tolerance is not None and (
            isinstance(tolerance, bool) or not isinstance(tolerance, (int, float))
            or not math.isfinite(tolerance) or tolerance <= 0
        ):
            raise ValueError("tolerance must be a positive finite number")

        if method == "delta":
            if tolerance is None:
                raise ValueError("delta PageRank requires a positive tolerance")
            from .pagerank_delta import execute as execute_delta
            return execute_delta(self, vertices, edges, reset_probability=reset_probability,
                                 max_iterations=max_iterations, tolerance=tolerance,
                                 partitions=partitions, cancellation=cancellation)
        if method != "power":
            raise ValueError("PageRank method must be power or delta")

        def execute(run, vertices, edges, size):
            if not size:
                path, result = run.materialize(vertices.withColumn("pagerank", F.lit(0.0)), expected_rows=0)
                return run.finish(path, result, algorithm="pagerank", iterations=0, converged=True)
            _, weighted = run.materialize(
                edges.join(edges.groupBy("src").count().withColumnRenamed("count", "degree"), "src"),
                key="src",
            )
            # Static dangling vertices need no per-iteration anti-join.
            _, dangling = run.materialize(
                vertices.join(edges.select(F.col("src").alias("id")).distinct(), "id", "left_anti"),
                key="id",
            )
            path, rank = run.materialize(vertices.withColumn("pagerank", F.lit(1.0 / size)),
                                         expected_rows=size, key="id")
            converged = None if tolerance is None else False
            for step in range(1, max_iterations + 1):
                run.cancellation.check()
                self._observe(run, "pagerank", step, "iteration_start")
                run.cancellation.check()
                dangling_mass = rank.join(dangling, "id").agg(F.sum("pagerank")).first()[0] or 0.0
                message = weighted.join(rank, weighted.src == rank.id).select(
                    weighted.dst.alias("id"), (rank.pagerank / weighted.degree).alias("message")
                ).groupBy("id").agg(F.sum("message").alias("incoming"))
                message_path = None
                if run.declared:
                    # Materialize the aggregated messages bucketed by id: the
                    # join back to vertices then sees exact O(|V|) statistics
                    # and a co-partitioned input, instead of the O(|E|) row
                    # estimate an aggregate inherits from its input.
                    message_path, message = run.materialize(message, key="id")
                updated = vertices.join(message, "id", "left").select(
                    "id", (F.lit(reset_probability / size) + F.lit(1.0 - reset_probability) * (
                        F.coalesce(F.col("incoming"), F.lit(0.0)) + F.lit(dangling_mass / size)
                    )).alias("pagerank"),
                )
                next_path, next_rank = run.materialize(updated, expected_rows=size, key="id")
                if message_path is not None:
                    run.remove(message_path)
                if tolerance is not None:
                    run.cancellation.check()
                    before = rank.select("id", F.col("pagerank").alias("before"))
                    change = next_rank.join(before, "id").agg(
                        F.sum(F.abs(F.col("pagerank") - F.col("before")))
                    ).first()[0]
                    converged = change <= tolerance
                run.remove(path)
                path, rank = next_path, next_rank
                self._observe(run, "pagerank", step, "iteration_end")
                if converged:
                    break
            if converged is False:
                raise ConvergenceError(f"PageRank did not reach tolerance in {max_iterations} iterations")
            return run.finish(path, rank, algorithm="pagerank", iterations=step, converged=converged)

        return self._run(vertices, edges, partitions, cancellation, execute)

    def wcc(self, vertices, edges, *, max_iterations=100, partitions=4, cancellation=None,
            method="min_label", seed=42):
        """Exact weak components by propagation or seeded randomized contraction.

        Treat every edge as undirected. The default method="min_label"
        starts each vertex with its own ID and repeatedly takes the minimum of each
        vertex's own and its neighbors' labels, stopping at a fixed point.
        method="randomized" contracts using GF64 affine priorities and expands
        representative maps in reverse. method="randomized_fused" uses the same
        contraction choices with fused edge projections and min_by, omitting
        the initial canonical edge write and per-round priority tables/joins.
        Both contraction plans require axpb and an unsigned 64-bit seed
        (default 42). max_iterations limits propagation or contraction rounds,
        respectively. All methods label a component by its minimum ID;
        isolates label themselves. Reaching the cap raises ConvergenceError.
        Output: id BIGINT, component BIGINT.
        """
        _positive_integer(max_iterations, "max_iterations")

        if method in ("randomized", "randomized_fused"):
            from .wcc_randomized import execute as execute_randomized
            return execute_randomized(self, vertices, edges, max_iterations=max_iterations,
                                      partitions=partitions, cancellation=cancellation, seed=seed,
                                      fused=method == "randomized_fused")
        if method != "min_label":
            raise ValueError("WCC method must be min_label, randomized or randomized_fused")

        def execute(run, vertices, edges, size):
            _, adjacency = run.materialize(edges.unionByName(
                edges.select(F.col("dst").alias("src"), F.col("src").alias("dst"))
            ).distinct())
            path, labels = run.materialize(vertices.withColumn("component", F.col("id")), expected_rows=size)
            if not size:
                return run.finish(path, labels, algorithm="wcc-min-label", iterations=0, converged=True)
            for step in range(1, max_iterations + 1):
                run.cancellation.check()
                self._observe(run, "wcc-min-label", step, "iteration_start")
                run.cancellation.check()
                messages = adjacency.join(labels, adjacency.src == labels.id).select(
                    adjacency.dst.alias("id"), labels.component
                )
                updated = labels.unionByName(messages).groupBy("id").agg(F.min("component").alias("component"))
                next_path, next_labels = run.materialize(updated, expected_rows=size)
                before = labels.select("id", F.col("component").alias("before"))
                changed = next_labels.join(before, "id").where(F.col("component") != F.col("before")).limit(1).count()
                run.remove(path)
                path, labels = next_path, next_labels
                self._observe(run, "wcc-min-label", step, "iteration_end")
                if not changed:
                    return run.finish(path, labels, algorithm="wcc-min-label", iterations=step, converged=True)
            raise ConvergenceError(f"WCC did not reach a fixed point in {max_iterations} iterations")

        return self._run(vertices, edges, partitions, cancellation, execute)

    def bfs(self, vertices, edges, *, source, method="frontier", directed=True,
            max_iterations=1000, partitions=4, cancellation=None):
        """Single-source hop distances and a parent tree; unreachable rows are null.

        method="reference" relaxes all reached vertices each round; "frontier"
        expands only changed vertices. "push_pull" switches relational join
        orientation; it does not promise native adjacency early exit.
        The source must exist and has parent=source.
        The cap includes the final round certifying no further changes.
        """
        from .traversal import execute
        return execute(self, vertices, edges, source=source, weighted=False,
                       method=method, directed=directed, max_iterations=max_iterations,
                       partitions=partitions, cancellation=cancellation)

    def sssp(self, vertices, edges, *, source, method="frontier", directed=True,
             max_iterations=1000, partitions=4, cancellation=None, delta=1.0):
        """Single-source shortest distances for finite nonnegative DOUBLE weights.

        Edges require a `weight` column. Reference is synchronous Bellman–Ford;
        frontier relaxes only changed vertices. "delta_star" processes the lowest
        pending distance bucket with width delta, relaxing all outgoing edges;
        this differs from classical light/heavy delta-stepping. Equal-distance
        paths prefer fewer hops, then the smaller parent ID, preventing parent
        cycles on zero-weight edges. Unreachable distance/parent/hops are null.
        Finite-distance overflow raises an error rather than marking a vertex
        unreachable. Both methods require an explicit convergence certificate.
        """
        from .traversal import execute
        return execute(self, vertices, edges, source=source, weighted=True,
                       method=method, directed=directed, max_iterations=max_iterations,
                       partitions=partitions, cancellation=cancellation, delta=delta)
