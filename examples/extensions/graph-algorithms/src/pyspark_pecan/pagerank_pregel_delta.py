"""Delta PageRank as GraphX's dynamic vertex program, in the form of graphframes-rs.

The reference is GraphX `PageRank.runUntilConvergence` as graphframes-rs
implements it over its Pregel engine (`src/algorithm/centrality/pagerank.rs`).
A vertex carries its out-degree, its accumulated rank and the rank it gained
in the last step, its delta. Both start at the reset probability. In a step
every vertex whose delta exceeds the tolerance sends delta/out_degree along
its out-edges, and every vertex then sets

    delta    = (1 - reset) * (sum of received messages, 0 when none)
    pagerank = pagerank + delta

In the first step all vertices send. There is no dangling term, no residual
and no certificate; the state is the only relation a step writes, so a step
is one job. Ranks are on GraphX's scale, where a vertex starts at `reset` and
the tolerance compares to a single vertex's gain; `normalize=True` divides by
the total at the end, as graphframes-rs always does.

A fixed budget runs exactly `max_iterations` steps and never counts anything
(graphframes-rs with `max_iter > 0`). With `vote_to_halt` a step also counts
the vertices still above the tolerance and the run stops when there are none
(GraphX, and graphframes-rs with `max_iter = 0`); reaching the cap first
raises ConvergenceError.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from pyspark.sql.connect import functions as F

from ._contracts import ConvergenceError, first_row

if TYPE_CHECKING:
    from pyspark.sql import DataFrame

    from .algorithms import GraphAlgorithms
    from .lifecycle import CancellationToken, GraphResult
    from .staging import StagingRun
    from .types import PageRankOptions

ALGORITHM = "pagerank-pregel-delta"


def step(state: DataFrame, edges: DataFrame, damping: float, tolerance: float | None) -> DataFrame:
    """The state after one superstep; `tolerance=None` lets every vertex send (the first step)."""
    senders = state if tolerance is None else state.where(F.col("delta") > F.lit(tolerance))
    incoming = edges.join(senders, edges.src == senders.id).select(
        edges.dst.alias("id"), (senders.delta / senders.degree).alias("message"),
    ).groupBy("id").agg(F.sum("message").alias("incoming"))
    gained = F.lit(damping) * F.coalesce(F.col("incoming"), F.lit(0.0))
    return state.join(incoming, "id", "left").select(
        "id", "degree", (F.col("pagerank") + gained).alias("pagerank"), gained.alias("delta"))


def execute(graph: GraphAlgorithms, vertices: DataFrame, edges: DataFrame, *, options: PageRankOptions,
            cancellation: CancellationToken | None) -> GraphResult:
    if options.tolerance is None:
        raise ValueError("pregel_delta PageRank requires a positive tolerance")
    tolerance: float = options.tolerance
    reset = options.reset_probability
    damping = 1.0 - reset

    def body(run: StagingRun, vertices: DataFrame, edges: DataFrame, size: int | None) -> GraphResult:
        degrees = edges.groupBy("src").count().select(F.col("src").alias("id"), F.col("count").alias("degree"))
        path, state = run.materialize(vertices.join(degrees, "id", "left").select(
            "id", F.coalesce(F.col("degree"), F.lit(0)).alias("degree"),
            F.lit(reset).alias("pagerank"), F.lit(reset).alias("delta"),
        ))
        steps = 0
        converged: bool | None = False if options.vote_to_halt else None
        while steps < options.max_iterations and not converged:
            steps += 1
            run.cancellation.check()
            graph._observe(run, ALGORITHM, steps, "iteration_start")
            next_path, next_state = run.materialize(step(state, edges, damping, None if steps == 1 else tolerance))
            run.remove(path)
            path, state = next_path, next_state
            if options.vote_to_halt:
                run.cancellation.check()
                active: int = state.where(F.col("delta") > F.lit(tolerance)).count()
                converged = active == 0
                graph._observe(run, ALGORITHM, steps, "iteration_end", frontier_size=active)
            else:
                graph._observe(run, ALGORITHM, steps, "iteration_end")
        if converged is False:
            raise ConvergenceError(
                f"pregel_delta PageRank still had active vertices after {options.max_iterations} iterations")
        ranks = state.select("id", "pagerank")
        if options.normalize:
            run.cancellation.check()
            # The total is null for a graph without vertices; there is then nothing to divide.
            total: float | None = first_row(state.agg(F.sum("pagerank")))[0]
            if total:
                ranks = state.select("id", (F.col("pagerank") / F.lit(total)).alias("pagerank"))
        result_path, result = run.materialize(ranks)
        handle = run.finish(result_path, result, algorithm=ALGORITHM, iterations=steps, converged=converged)
        handle.method = options.method
        return handle

    return graph._run(vertices, edges, options.partitions, cancellation, body, count_vertices=False)
