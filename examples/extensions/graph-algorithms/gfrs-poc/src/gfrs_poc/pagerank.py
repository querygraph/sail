"""Minimal PySpark port of ``graphframes-rs/src/algorithm/centrality/pagerank.rs``.

Incremental (GraphX-style) PageRank over the Pregel engine, with a decreasing
active frontier:

* every vertex carries ``pagerank`` and ``pagerank_delta`` next to ``out_degree``;
* the message a source sends is its *delta* split over its out-edges
  (``delta / out_degree``), aggregated with ``sum``;
* ``pagerank += alpha * msg`` and ``pagerank_delta = alpha * msg``, where
  ``alpha = 1 - reset_prob`` is kept explicitly: it cannot be folded into the
  final normalization because the graph may have sinks;
* only sources whose new delta is still above ``tol`` participate; with
  ``skip_dest_state`` the participation filter runs before the join, so the
  active frontier shrinks every iteration;
* ``max_iter <= 0`` runs in convergence mode: a voting column stops the loop as
  soon as no vertex is active. ``max_iter > 0`` runs a fixed budget instead;
* the final ranks are normalized to sum to 1.
"""

from __future__ import annotations

import logging

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from .pregel import (
    EDGE_DST,
    EDGE_SRC,
    VERTEX_ID,
    MessageDirection,
    Pregel,
    pregel_default_msg,
    pregel_src,
)

logger = logging.getLogger("gfrs_poc")

#: Column name for pagerank in the PageRank algorithm.
PAGERANK = "pagerank"

#: Per-iteration pagerank delta; an internal state column, not part of the output.
PAGERANK_DELTA = "pagerank_delta"


def _out_degrees(edges: DataFrame, vertices: DataFrame) -> DataFrame:
    """Out-degree per vertex, keeping zero-out-degree vertices (sinks) at 0.

    Dropping sinks would silently remove them from the result and lose the rank
    mass they should accumulate.
    """
    degrees = (
        edges.groupBy(EDGE_SRC)
        .agg(F.count(F.col(EDGE_DST)).alias("out_degree"))
        .select(F.col(EDGE_SRC).alias(VERTEX_ID), F.col("out_degree"))
    )
    return (
        vertices.select(VERTEX_ID)
        .join(degrees, how="left", on=VERTEX_ID)
        .select(F.col(VERTEX_ID), F.coalesce(F.col("out_degree"), F.lit(0)).alias("out_degree"))
    )


def pagerank(
    edges: DataFrame,
    vertices: DataFrame,
    tol: float = 1e-5,
    max_iter: int = 0,
    reset_prob: float = 0.15,
    checkpoint_dir: str = "_gfrs_poc_checkpoints",
    num_partitions: int | None = None,
    info: dict | None = None,
) -> DataFrame:
    """Run delta PageRank and return a DataFrame with ``id`` and normalized ``pagerank``.

    Mirrors ``PageRankBuilder`` defaults: uniform seeding with ``reset_prob``,
    damping ``1 - reset_prob``, tolerance-based active frontier. ``max_iter <= 0``
    (default) converges via vertex voting; ``max_iter > 0`` caps the iterations
    without voting. When ``info`` is given it is filled with ``iterations`` and
    ``active_counts`` for observability.
    """
    alpha = 1.0 - reset_prob

    graph_vertices = _out_degrees(edges, vertices)

    # Each vertex sends its *delta* split over its out-edges; the additive constant
    # does not matter because the result is normalized at the end, but a non-zero
    # initial delta is needed to bootstrap. Seeding both columns with reset_prob
    # reproduces the GraphX dynamic PageRank bootstrap.
    new_delta = F.lit(alpha) * F.coalesce(pregel_default_msg(), F.lit(0.0))

    builder = (
        Pregel(
            edges,
            graph_vertices,
            checkpoint_dir=f"{checkpoint_dir.rstrip('/')}/inner_checkpoint",
            num_partitions=num_partitions,
        )
        .add_vertex_column(PAGERANK, F.lit(reset_prob), F.col(PAGERANK) + new_delta)
        .add_vertex_column(PAGERANK_DELTA, F.lit(reset_prob), new_delta)
        .add_vertex_column("out_degree", F.col("out_degree"), F.col("out_degree"))
        .add_message(
            pregel_src(PAGERANK_DELTA) / pregel_src("out_degree"),
            MessageDirection.SRC_TO_DST,
        )
        .add_aggregate_expr(F.sum(pregel_default_msg()))
        # A vertex participates while its (new) delta is still above tol. A
        # separate column from the voting flag: participation prunes message
        # generation every iteration, voting only decides when to stop.
        .with_participation_column("participates", F.lit(True), new_delta > F.lit(tol))
        .skip_dest_state()
    )

    if max_iter > 0:
        # Fixed iteration budget: keep the participation filter (cheap tail) but
        # do not vote for early termination.
        builder = builder.max_iterations(max_iter)
    else:
        # Convergence mode: stop once no vertex is still active.
        builder = builder.with_vertex_voting("active", new_delta > F.lit(tol))

    result = builder.run()

    if info is not None:
        info["iterations"] = builder.iterations
        info["active_counts"] = builder.active_counts

    total = result.agg(F.sum(PAGERANK).alias("pagerank_sum")).first()["pagerank_sum"]
    if total is None or total == 0:
        raise ArithmeticError("pagerank sum is zero; the graph produced no rank mass")
    return result.select(F.col(VERTEX_ID), (F.col(PAGERANK) / F.lit(total)).alias(PAGERANK))
