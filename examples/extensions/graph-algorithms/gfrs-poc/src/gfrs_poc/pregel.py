"""Minimal PySpark port of ``graphframes-rs/src/algorithm/pregel.rs``.

Same concept, same plan shape, same column names:

* vertex state and edges live in parquet checkpoints, re-read every iteration;
* edges are checkpointed once with ``__pregel_msg_edge_*`` column names;
* triplets are ``src-state JOIN edges`` (``skip_dest_state`` pushes the
  participation filter *before* the join -- the GraphX-style truncation; with the
  destination state the triplets keep the ``src_part OR dst_part`` post-join
  filter);
* messages are ``struct(target_id, payload)`` projections over the triplets,
  unioned by name for multiple messages, then aggregated by ``id``;
* aggregated messages are checkpointed (spill) and left-joined onto the state;
* update expressions produce the next state, checkpointed under ``state-<i>``;
* vertex voting stops the loop when no vertex is active anymore.

Differences forced by the PySpark API: no purge/eviction (see ``checkpointer.py``)
and no declared pre-partitioning, so the server-side join strategy is controlled
by the Sail ``optimizer.prefer_hash_join`` setting instead of a scoped session.
"""

from __future__ import annotations

import logging
from enum import Enum
from uuid import uuid4

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F

from .checkpointer import ParquetCheckpointer

logger = logging.getLogger("gfrs_poc")

VERTEX_ID = "id"
EDGE_SRC = "src"
EDGE_DST = "dst"

#: Message column names used in the Pregel algorithm (same as the Rust engine).
PREGEL_MSG = "__pregel_msg"
PREGEL_MSG_SRC = "__pregel_msg_src"
PREGEL_MSG_DST = "__pregel_msg_dst"
PREGEL_MSG_EDGE = "__pregel_msg_edge"


class MessageDirection(Enum):
    """Direction of message passing in Pregel."""

    SRC_TO_DST = "src-to-dst"
    DST_TO_SRC = "dst-to-src"
    BIDIRECTIONAL = "bidirectional"


def pregel_src(col_name: str) -> Column:
    return F.col(f"{PREGEL_MSG_SRC}_{col_name}")


def pregel_dst(col_name: str) -> Column:
    return F.col(f"{PREGEL_MSG_DST}_{col_name}")


def pregel_edge(col_name: str) -> Column:
    return F.col(f"{PREGEL_MSG_EDGE}_{col_name}")


def pregel_msg(msg_name: str) -> Column:
    return F.col(f"{PREGEL_MSG}_{msg_name}")


def pregel_default_msg() -> Column:
    return pregel_msg("msg")


class _VertexColumn:
    def __init__(self, name: str, init_expr: Column, update_expr: Column):
        self.name = name
        self.init_expr = init_expr
        self.update_expr = update_expr


class _Message:
    def __init__(self, name: str, expr: Column, direction: MessageDirection):
        self.name = name
        self.expr = expr
        self.direction = direction


class Pregel:
    """Builder-style Pregel engine; ``run`` executes the loop and returns the
    final vertex state (``id`` + the declared vertex columns) as a DataFrame.

    The returned frame reads the last ``state-<i>`` checkpoint, so consuming it
    does not recompute the loop.
    """

    def __init__(
        self,
        edges: DataFrame,
        vertices: DataFrame,
        checkpoint_dir: str = "_gfrs_poc_checkpoints",
        num_partitions: int | None = None,
    ):
        self.edges = edges
        self.vertices = vertices
        self.spark = vertices.sparkSession
        #: iteration budget; ``None`` means unlimited (field named differently from
        #: the ``max_iterations`` builder method on purpose: in Python an instance
        #: attribute would shadow the method).
        self._max_iterations: int | None = None
        self.use_vertex_voting = False
        self.activity_column: str | None = None
        self.voting_condition: Column | None = None
        self.vertex_columns: list[_VertexColumn] = []
        self.edge_columns = [EDGE_SRC, EDGE_DST]
        self.participation_column: _VertexColumn | None = None
        self.messages: list[_Message] = []
        self.aggregate_exprs: list[Column] = []
        self.use_dest_state = True
        self.checkpoint_dir = checkpoint_dir
        if num_partitions is None:
            num_partitions = int(self.spark.conf.get("spark.sql.shuffle.partitions", "200"))
        self.num_partitions = num_partitions
        #: Filled by ``run``: number of executed iterations and per-iteration
        #: active-vertex counts (only with vertex voting).
        self.iterations = 0
        self.active_counts: list[int] = []

    # ------------------------------------------------------------------ builder

    def max_iterations(self, max_iterations: int) -> Pregel:
        self._max_iterations = max_iterations
        return self

    def skip_dest_state(self) -> Pregel:
        self.use_dest_state = False
        return self

    def with_vertex_voting(self, activity_column: str, voting_condition: Column) -> Pregel:
        self.use_vertex_voting = True
        self.activity_column = activity_column
        self.voting_condition = voting_condition
        return self

    def add_vertex_column(self, name: str, init_expr: Column, update_expr: Column) -> Pregel:
        self.vertex_columns.append(_VertexColumn(name, init_expr, update_expr))
        return self

    def with_participation_column(
        self, column: str, initial_expr: Column, update_condition: Column
    ) -> Pregel:
        """A boolean column that decides whether a vertex still sends messages."""
        self.participation_column = _VertexColumn(column, initial_expr, update_condition)
        return self

    def add_message(self, expr: Column, direction: MessageDirection) -> Pregel:
        self.messages.append(_Message("msg", expr, direction))
        return self

    def add_named_message(self, name: str, expr: Column, direction: MessageDirection) -> Pregel:
        self.messages.append(_Message(name, expr, direction))
        return self

    def add_aggregate_expr(self, expr: Column) -> Pregel:
        self.aggregate_exprs.append(expr.alias(f"{PREGEL_MSG}_msg"))
        return self

    def add_named_aggregate_expr(self, name: str, expr: Column) -> Pregel:
        self.aggregate_exprs.append(expr.alias(f"{PREGEL_MSG}_{name}"))
        return self

    # --------------------------------------------------------------------- run

    def run(self) -> DataFrame:
        if not self.messages:
            raise ValueError("No messages defined for Pregel algorithm")
        if not self.aggregate_exprs and len(self.messages) > 1:
            raise ValueError("Aggregate expression is required when multiple messages are defined")

        run_id = uuid4()
        logger.info("start pregel with ID %s", run_id)
        run_dir = f"{self.checkpoint_dir.rstrip('/')}/{run_id}"
        edges_checkpointer = ParquetCheckpointer(self.spark, run_dir, self.num_partitions)
        state_checkpointer = ParquetCheckpointer(self.spark, run_dir, self.num_partitions)

        # Initialize vertices with the initial expressions.
        current_vertices = self.vertices
        for column in self.vertex_columns:
            current_vertices = current_vertices.withColumn(column.name, column.init_expr)

        max_iterations = self._max_iterations if self._max_iterations is not None else 2**63 - 1

        # Message payloads as struct(target_id, <name> = expr), one select per
        # message direction, unioned by name afterwards.
        message_structs: list[tuple[str, Column]] = []
        for message in self.messages:
            if message.direction == MessageDirection.SRC_TO_DST:
                target = pregel_edge(EDGE_DST)
            else:  # DST_TO_SRC and the src half of BIDIRECTIONAL
                target = pregel_edge(EDGE_SRC)
            message_structs.append(
                (message.name, F.struct(target.alias(VERTEX_ID), message.expr.alias(message.name)))
            )
            if message.direction == MessageDirection.BIDIRECTIONAL:
                message_structs.append(
                    (
                        message.name,
                        F.struct(
                            pregel_edge(EDGE_DST).alias(VERTEX_ID),
                            message.expr.alias(message.name),
                        ),
                    )
                )

        update_columns = [
            column.update_expr.alias(column.name) for column in self.vertex_columns
        ]

        if self.use_vertex_voting:
            activity_column = self.activity_column
            assert activity_column is not None
            current_vertices = current_vertices.withColumn(activity_column, F.lit(True))
            voting_condition = (
                self.voting_condition if self.voting_condition is not None else F.lit(True)
            )
            update_columns.append(voting_condition.alias(activity_column))

        if self.participation_column is not None:
            participation_column = self.participation_column
            current_vertices = current_vertices.withColumn(
                participation_column.name, participation_column.init_expr
            )
            update_columns.append(
                participation_column.update_expr.alias(participation_column.name)
            )
        update_columns.append(F.col(VERTEX_ID).alias(VERTEX_ID))

        # Prepare and offload edges to disk.
        edges_df = self.edges.select(
            *[F.col(name).alias(f"{PREGEL_MSG_EDGE}_{name}") for name in self.edge_columns]
        )
        edges_struct = edges_checkpointer.push(
            "edges", edges_df, key=f"{PREGEL_MSG_EDGE}_{EDGE_SRC}"
        )

        # Offload the prepared state to disk.
        current_vertices = state_checkpointer.push("state-0", current_vertices, key=VERTEX_ID)

        participation_name = (
            self.participation_column.name if self.participation_column is not None else None
        )

        iteration = 0
        self.iterations = 0
        self.active_counts = []
        while iteration < max_iterations:
            iteration += 1
            field_names = [field.name for field in current_vertices.schema.fields]
            src_projection = [
                F.col(name).alias(f"{PREGEL_MSG_SRC}_{name}") for name in field_names
            ]

            # When the destination state is not needed we can push the participation
            # filter *before* the join: only participating sources ever emit a
            # message, so dropping inactive sources up front shrinks the join input.
            # With the destination state we must keep both sides and defer to the
            # post-join OR filter below.
            if not self.use_dest_state:
                src_state = current_vertices
                if participation_name is not None:
                    src_state = src_state.filter(F.col(participation_name))
                src_vertices = src_state.select(src_projection)
            else:
                src_vertices = current_vertices.select(src_projection)

            triplets = src_vertices.join(
                edges_struct,
                how="inner",
                on=(pregel_src(VERTEX_ID) == pregel_edge(EDGE_SRC)),
            )

            if self.use_dest_state:
                dst_vertices = current_vertices.select(
                    *[F.col(name).alias(f"{PREGEL_MSG_DST}_{name}") for name in field_names]
                )
                triplets = triplets.join(
                    dst_vertices,
                    how="inner",
                    on=(F.col(f"{PREGEL_MSG_DST}_{VERTEX_ID}") == pregel_edge(EDGE_DST)),
                )
                # Drop triplets where *both* endpoints are inactive; keep a triplet
                # as long as the source or the destination still participates.
                if participation_name is not None:
                    triplets = triplets.filter(
                        pregel_src(participation_name) | pregel_dst(participation_name)
                    )

            messages_df: DataFrame | None = None
            for name, message_struct in message_structs:
                part = triplets.select(
                    message_struct.getField(VERTEX_ID).alias(VERTEX_ID),
                    message_struct.getField(name).alias(f"{PREGEL_MSG}_{name}"),
                )
                messages_df = part if messages_df is None else messages_df.unionByName(part)

            # Spill the aggregation to disk to reduce the memory peak.
            if self.aggregate_exprs:
                messages_df = messages_df.groupBy(VERTEX_ID).agg(*self.aggregate_exprs)
            aggregated = state_checkpointer.push(
                f"aggregated-messages-{iteration}", messages_df
            )

            vertices_with_messages = current_vertices.join(
                aggregated.withColumnRenamed(VERTEX_ID, "am_vid"),
                how="left",
                on=(F.col(VERTEX_ID) == F.col("am_vid")),
            ).drop("am_vid")

            new_vertices = vertices_with_messages.select(*update_columns)
            current_vertices = state_checkpointer.push(
                f"state-{iteration}", new_vertices, key=VERTEX_ID
            )

            if self.use_vertex_voting:
                active_count = current_vertices.filter(
                    F.col(self.activity_column)
                ).count()
                self.active_counts.append(active_count)
                logger.info(
                    "iteration %d, %d vertices are participating in the loop",
                    iteration,
                    active_count,
                )
                if active_count == 0:
                    break
            else:
                logger.info("iteration %d / %d completed", iteration, max_iterations)

        self.iterations = iteration

        # Drop the debug columns: keep id + the declared vertex columns.
        required = [F.col(VERTEX_ID)] + [F.col(column.name) for column in self.vertex_columns]
        return current_vertices.select(required)
