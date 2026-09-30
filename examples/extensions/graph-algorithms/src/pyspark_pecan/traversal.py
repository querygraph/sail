"""Table-based single-source traversal with bounded client-side reductions."""
from pyspark.sql.connect import functions as F
from pyspark.sql.types import DoubleType

from .algorithms import ConvergenceError, _positive_integer
from .traversal_relaxation import weighted_relaxation, materialize_weighted_relaxation


def execute(graph, vertices, edges, *, source, weighted, method, directed,
            max_iterations, partitions, cancellation, delta=1.0):
    _positive_integer(max_iterations, "max_iterations")
    if isinstance(source, bool) or not isinstance(source, int) or not -(2**63) <= source < 2**63:
        raise ValueError("source must be a BIGINT integer")
    if not isinstance(directed, bool):
        raise ValueError("directed must be boolean")
    if method not in (("reference", "frontier", "delta_star") if weighted else ("reference", "frontier", "push_pull")):
        raise ValueError("unsupported traversal method")
    if weighted and ("weight" not in edges.columns or
                     not isinstance(edges.schema["weight"].dataType, DoubleType)):
        raise ValueError("SSSP requires a DOUBLE weight column")
    algorithm = ("sssp" if weighted else "bfs") + "-" + method

    def body(run, vertices, edges, size):
        if not vertices.where(F.col("id") == source).limit(1).count():
            raise ValueError("source must reference an existing vertex")
        if weighted:
            if edges.where(F.col("weight").isNull() | F.isnan("weight") |
                           (F.col("weight") < 0) | (F.col("weight") == float("inf"))).limit(1).count():
                raise ValueError("weights must be finite, non-null and nonnegative")
        else:
            edges = edges.withColumn("weight", F.lit(1.0))
        if not directed:
            edges = edges.unionByName(edges.select(
                F.col("dst").alias("src"), F.col("src").alias("dst"), "weight"))
        _, adjacency = run.materialize(edges)
        if method == "push_pull":
            from .traversal_bfs import execute as bfs
            return bfs(graph, run, vertices, adjacency, size, source, max_iterations)
        if method == "delta_star":
            from .traversal_stepping import execute as stepping
            return stepping(graph, run, vertices, adjacency, size, source, delta, max_iterations)
        initial = vertices.where(F.col("id") == source).select(
            "id", F.lit(0.0).alias("distance"), F.lit(0).cast("long").alias("hops"),
            F.col("id").alias("parent"))
        path, reached = run.materialize(initial, expected_rows=1)
        frontier_path, frontier = path, reached
        for step in range(1, max_iterations + 1):
            run.cancellation.check()
            active = reached if method == "reference" else frontier
            # The frontier is the left input on purpose: in cluster mode the expansion is
            # a partitioned hash join whose build side is the left input, so the small
            # side must be written first or every worker builds a table over its share of
            # the adjacency (the recorded plans of the 2026-09-29 capacity cells).
            candidates = active.join(adjacency, active.id == adjacency.src).select(
                adjacency.dst.alias("id"),
                (active.distance + adjacency.weight).alias("distance"),
                (active.hops + 1).alias("hops"), active.id.alias("parent"))
            # Lexicographic minimization makes parent choice deterministic and
            # gives every parent a strictly smaller hop count, even at weight 0.
            updated = (weighted_relaxation(reached, candidates) if weighted else
                       reached.unionByName(candidates).groupBy("id").agg(
                           F.min(F.struct("distance", "hops", "parent")).alias("best")
                       ).select("id", "best.*"))
            graph._observe(run, algorithm, step, "iteration_start", plan_of=updated)
            next_path, next_reached = (materialize_weighted_relaxation(run, updated)
                                       if weighted else run.materialize(updated))
            if not weighted and next_reached.where(F.col("distance") == float("inf")).limit(1).count():
                raise OverflowError("shortest-path distance overflow")
            before = reached.select("id", F.struct("distance", "hops", "parent").alias("before"))
            changed = next_reached.join(before, "id", "left").where(
                F.col("before").isNull() |
                (F.struct("distance", "hops", "parent") != F.col("before"))
            ).select("id", "distance", "hops", "parent")
            change_path, next_frontier = run.materialize(changed)
            count = next_frontier.count()
            for obsolete in {path, frontier_path}:
                run.remove(obsolete)
            path, reached = next_path, next_reached
            frontier_path, frontier = change_path, next_frontier
            graph._observe(run, algorithm, step, "iteration_end", active_vertices=count)
            if not count:
                result_path, result = run.materialize(
                    vertices.join(reached, "id", "left"), expected_rows=size)
                return run.finish(result_path, result, algorithm=algorithm,
                                  iterations=step, converged=True)
        raise ConvergenceError(f"{algorithm} did not converge in {max_iterations} iterations")

    return graph._run(vertices, edges, partitions, cancellation, body,
                      edge_columns=("src", "dst", "weight") if weighted else ("src", "dst"))
