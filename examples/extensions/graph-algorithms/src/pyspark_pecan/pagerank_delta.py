"""Signed-residual PageRank with an active frontier and a global certificate.

For d=1-reset and the column-stochastic transition P (uniform dangling
columns), maintain r = reset/N + d*P*x - x. Push a subset a of r:
x <- x+a; r <- r-a+d*P*a. Inactive residual is retained, not discarded.
Use threshold min(||r||_1/(2*N), tolerance*sum(x)/(4*N)). The tolerance-scaled
term retains insignificant tail corrections while larger corrections propagate;
the relative cap leaves at most half the pending norm inactive, so that norm
contracts by at least 1-reset/2 in exact arithmetic. Activity can shrink and
grow; vertices can reactivate after receiving later messages.

For m=sum(x), the residual of normalized x/m is bounded by 2*||r||_1/m.
This bound triggers a full recomputation, never substitutes for the final
certificate: ||reset/N+d*P*y-y||_1 <= tolerance. Consequently the normalized
output's L1 distance from stationary PageRank is at most tolerance/reset.
All arithmetic uses DOUBLE; distributed sums are not promised bitwise stable.

Only active sources emit edge messages. A relational join may still scan the
Parquet edge table; this client does not claim indexed adjacency or fewer
physical edge reads. Scalar frontier counts report propagated edge messages.
"""

import math

from pyspark.sql.connect import functions as F


def _observe(graph, run, step, kind, **metrics):
    if graph.observer is not None:
        graph.observer(dict(kind=kind, algorithm="pagerank-delta", iteration=step,
                            run_path=run.path, **metrics))
    run.cancellation.check()


def _statistics(run, state, size):
    run.cancellation.check()
    row = state.agg(
        F.count("*").alias("rows"), F.sum("pagerank").alias("mass"),
        F.min("pagerank").alias("minimum"),
        F.sum(F.abs(F.col("pending"))).alias("residual"),
    ).first()
    run.cancellation.check()
    if row["rows"] != size:
        raise RuntimeError("delta PageRank state changed its vertex count")
    if (not math.isfinite(row.mass) or row.mass <= 0 or
            not math.isfinite(row.residual) or not math.isfinite(row.minimum) or row.minimum < 0):
        raise ArithmeticError("delta PageRank produced invalid floating-point state")
    return row.mass, row.residual


def _incoming(edges, state, column):
    return edges.join(state, edges.src == state.id).select(
        edges.dst.alias("id"), (state[column] / state.degree).alias("message"),
    ).groupBy("id").agg(F.sum("message").alias("incoming"))


def _certificate(run, state, edges, size, damping, reset, mass):
    """Materialize normalized scores and a freshly recomputed true residual."""
    normalized = state.select(
        "id", "degree", (F.col("pagerank") / F.lit(mass)).alias("pagerank"),
        "ever_active", "active_previous",
    )
    run.cancellation.check()
    dangling = normalized.where(F.col("degree") == 0).agg(F.sum("pagerank")).first()[0] or 0.0
    run.cancellation.check()
    incoming = _incoming(edges, normalized, "pagerank")
    certified = normalized.join(incoming, "id", "left").select(
        "id", "degree", "pagerank", "ever_active", "active_previous",
        (F.lit(reset / size) + F.lit(damping) * (
            F.coalesce(F.col("incoming"), F.lit(0.0)) + F.lit(dangling / size)
        ) - F.col("pagerank")).alias("pending"),
    )
    path, certified = run.materialize(certified)
    mass, residual = _statistics(run, certified, size)
    return path, certified, mass, residual


def _finish(run, state, steps, residual, reset):
    path, result = run.materialize(state.select("id", "pagerank"))
    result = run.finish(path, result, algorithm="pagerank-delta", iterations=steps, converged=True)
    result.method = "delta"
    result.residual = residual
    result.error_bound = residual / reset
    return result


def execute(graph, vertices, edges, *, reset_probability=0.15, max_iterations=20,
            tolerance=None, partitions=4, cancellation=None):
    """Run frontier pushes; tolerance bounds the final global fixed-point L1 residual.

    Unlike fixed-K power iteration, delta execution requires a positive tolerance.
    max_iterations counts frontier pushes, excluding initialization and final
    certificates. A stationary uniform initialization therefore uses zero pushes.
    Failure to certify at the limit raises ConvergenceError and cleans the run.
    """
    from .algorithms import ConvergenceError, _positive_integer

    _positive_integer(max_iterations, "max_iterations")
    if (isinstance(reset_probability, bool) or not isinstance(reset_probability, (int, float))
            or not math.isfinite(reset_probability) or not 0 < reset_probability <= 1):
        raise ValueError("reset_probability must be finite and in (0, 1]")
    if (isinstance(tolerance, bool) or not isinstance(tolerance, (int, float))
            or not math.isfinite(tolerance) or tolerance <= 0):
        raise ValueError("delta PageRank tolerance must be a positive finite number")
    reset = float(reset_probability)
    damping = 1.0 - reset

    def body(run, vertices, edges, size):
        if not size:
            empty = vertices.withColumn("pagerank", F.lit(0.0))
            return _finish(run, empty, 0, 0.0, reset)
        degrees = edges.groupBy("src").count().select(
            F.col("src").alias("id"), F.col("count").alias("degree"),
        )
        path, state = run.materialize(vertices.join(degrees, "id", "left").select(
            "id", F.coalesce(F.col("degree"), F.lit(0)).alias("degree"),
            F.lit(1.0 / size).alias("pagerank"),
            F.lit(False).alias("ever_active"), F.lit(False).alias("active_previous"),
        ))
        next_path, state, mass, residual = _certificate(
            run, state, edges, size, damping, reset, 1.0,
        )
        run.remove(path)
        path = next_path
        _observe(graph, run, 0, "certificate", residual=residual,
                 error_bound=residual / reset)
        if residual <= tolerance:
            return _finish(run, state, 0, residual, reset)

        for step in range(1, max_iterations + 1):
            _observe(graph, run, step, "iteration_start")
            activation_mass = mass
            threshold = min(residual / (2.0 * size), tolerance * activation_mass / (4.0 * size))
            active_path, active = run.materialize(state.where(
                F.abs(F.col("pending")) > F.lit(threshold),
            ).select("id", "degree", F.col("pending").alias("push"),
                     "ever_active", "active_previous"))
            run.cancellation.check()
            activity = active.agg(
                F.count("*").alias("vertices"), F.sum("degree").alias("edges"),
                F.sum(F.when(F.col("degree") == 0, F.col("push")).otherwise(0.0)).alias("dangling"),
                F.sum((F.col("ever_active") & ~F.col("active_previous")).cast("long")).alias("reactivated"),
            ).first()
            run.cancellation.check()
            if activity.vertices == 0:
                raise ArithmeticError("positive PageRank residual produced an empty frontier")
            incoming = _incoming(edges, active, "push")
            selected = active.select("id", "push")
            next_state = state.join(selected, "id", "left").join(incoming, "id", "left")
            push = F.coalesce(F.col("push"), F.lit(0.0))
            next_state = next_state.select(
                "id", "degree", (F.col("pagerank") + push).alias("pagerank"),
                (F.col("ever_active") | F.col("push").isNotNull()).alias("ever_active"),
                F.col("push").isNotNull().alias("active_previous"),
                (F.col("pending") - push + F.lit(damping) * (
                    F.coalesce(F.col("incoming"), F.lit(0.0)) + F.lit((activity.dangling or 0.0) / size)
                )).alias("pending"),
            )
            next_path, next_state = run.materialize(next_state)
            mass, residual = _statistics(run, next_state, size)
            run.remove(active_path)
            run.remove(path)
            path, state = next_path, next_state
            bound = 2.0 * residual / mass
            _observe(graph, run, step, "iteration_end", frontier_size=activity.vertices,
                     active_edges=activity.edges or 0, reactivated_vertices=activity.reactivated or 0,
                     residual=residual, normalized_residual_bound=bound,
                     activation_threshold=threshold, activation_mass=activation_mass)
            if bound <= tolerance or step == max_iterations:
                next_path, certified, mass, residual = _certificate(
                    run, state, edges, size, damping, reset, mass,
                )
                run.remove(path)
                path, state = next_path, certified
                _observe(graph, run, step, "certificate", residual=residual,
                         error_bound=residual / reset)
                if residual <= tolerance:
                    return _finish(run, state, step, residual, reset)
                # A failed certificate rebases state to the normalized scores
                # and recomputed residual before any further frontier pushes.
        raise ConvergenceError(
            f"delta PageRank did not reach global residual tolerance in {max_iterations} iterations "
            f"(residual={residual}, tolerance={tolerance})"
        )

    return graph._run(vertices, edges, partitions, cancellation, body)
