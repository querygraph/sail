"""gfrs-poc: PySpark port of the graphframes-rs Pregel engine and delta PageRank.

This is a proof of concept that re-implements, in 100% pure PySpark, the
DataFusion engine from ``graphframes-rs`` (see the per-module docstrings for the
exact source files and the kept/dropped optimizations).
"""

from .pagerank import PAGERANK, PAGERANK_DELTA, pagerank
from .pregel import (
    EDGE_DST,
    EDGE_SRC,
    PREGEL_MSG,
    PREGEL_MSG_DST,
    PREGEL_MSG_EDGE,
    PREGEL_MSG_SRC,
    VERTEX_ID,
    MessageDirection,
    Pregel,
    pregel_default_msg,
    pregel_dst,
    pregel_edge,
    pregel_msg,
    pregel_src,
)

__all__ = [
    "EDGE_DST",
    "EDGE_SRC",
    "PAGERANK",
    "PAGERANK_DELTA",
    "PREGEL_MSG",
    "PREGEL_MSG_DST",
    "PREGEL_MSG_EDGE",
    "PREGEL_MSG_SRC",
    "VERTEX_ID",
    "MessageDirection",
    "Pregel",
    "pagerank",
    "pregel_default_msg",
    "pregel_dst",
    "pregel_edge",
    "pregel_msg",
    "pregel_src",
]
