"""Deterministic graph inputs and independent, unmeasured reference answers.

Run with the extension requirements.lock environment; no Spark or Sail process is
used. ``prepare(path, vertices=10000)`` writes immutable Parquet inputs, a full
reference, and a manifest. Reusing a nonempty output directory is rejected.

Sparse fixtures have contiguous components of at most ``block_size`` vertices,
5% isolated vertices, and approximately 5% dangling vertices within each block.
A random recursive directed tree connects each block. Additional edges select
sources with Zipf-like weights and destinations uniformly; a self-loop and a
parallel edge are deliberately retained in each nonsingleton block. The degree
argument targets E/V including isolates. Chain fixtures are a separate,
high-diameter case, not an alternative spelling of the sparse workload.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import math
from dataclasses import dataclass
from pathlib import Path

import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq


@dataclass(frozen=True)
class PageRankReference:
    scores: np.ndarray
    iterations: int
    residual: float


def generate(vertices: int, *, family: str = "sparse", seed: int = 20260927,
             degree: float = 8, block_size: int = 1024) -> tuple[np.ndarray, np.ndarray]:
    """Return int64 source/destination arrays over vertex IDs 0..vertices-1.

    Degree and seed do not change a chain. Sparse generation uses NumPy PCG64
    explicitly; the manifest also records the NumPy version and file hashes.
    """
    if isinstance(vertices, bool) or not isinstance(vertices, int) or vertices < 1:
        raise ValueError("vertices must be a positive integer")
    if family not in {"sparse", "chain"}:
        raise ValueError("family must be sparse or chain")
    if not math.isfinite(degree) or degree < 0:
        raise ValueError("degree must be finite and nonnegative")
    if isinstance(block_size, bool) or not isinstance(block_size, int) or block_size < 2:
        raise ValueError("block_size must be an integer >= 2")
    if isinstance(seed, bool) or not isinstance(seed, int) or seed < 0:
        raise ValueError("seed must be a nonnegative integer")
    if family == "chain":
        return np.arange(vertices - 1, dtype=np.int64), np.arange(1, vertices, dtype=np.int64)

    rng = np.random.Generator(np.random.PCG64(seed))
    active = vertices - vertices // 20
    total_edges = round(vertices * degree)
    sources = np.empty(total_edges, dtype=np.int64)
    targets = np.empty(total_edges, dtype=np.int64)
    for start in range(0, active, block_size):
        size = min(block_size, active - start)
        left, right = total_edges * start // active, total_edges * (start + size) // active
        count = right - left
        required = size + 1 if size > 1 else 1
        if count < required:
            raise ValueError("degree is too small to retain each block's tree, loop, and duplicate")
        dangling = max(1, size // 20) if size > 1 else 0
        core = size - dangling
        tree_dst = np.arange(1, size, dtype=np.int64)
        tree_src = rng.integers(0, np.minimum(tree_dst, core), dtype=np.int64)
        # No dangling vertex is an edge source, including in the tree.
        src = np.empty(count, dtype=np.int64)
        dst = np.empty(count, dtype=np.int64)
        src[:size - 1], dst[:size - 1] = tree_src, tree_dst
        src[size - 1], dst[size - 1] = 0, 0
        if size > 1:
            src[size], dst[size] = tree_src[0], tree_dst[0]
        weights = np.arange(1, core + 1, dtype=np.float64) ** -0.75
        weights /= weights.sum()
        src[required:] = rng.choice(core, size=count - required, p=weights)
        dst[required:] = rng.integers(0, size, size=count - required)
        order = rng.permutation(count)
        sources[left:right], targets[left:right] = src[order] + start, dst[order] + start
    return sources, targets


def _check_edges(vertices: int, source: np.ndarray, target: np.ndarray) -> None:
    if vertices < 0 or source.ndim != 1 or target.ndim != 1 or source.shape != target.shape:
        raise ValueError("expected equal one-dimensional edge arrays and a nonnegative vertex count")
    if source.dtype.kind not in "iu" or target.dtype.kind not in "iu":
        raise ValueError("edge endpoints must be integers")
    if source.size and (source.min() < 0 or target.min() < 0
                        or source.max() >= vertices or target.max() >= vertices):
        raise ValueError("edge endpoint is outside 0..vertices-1")


def pagerank_reference(vertices: int, source: np.ndarray, target: np.ndarray, *,
                       damping: float = 0.85, tolerance: float = 1e-8,
                       max_iterations: int = 1000) -> PageRankReference:
    """Uniform PageRank, including dangling redistribution and all edge rows.

    The residual is the L1 difference between the final two iterates, matching
    the reference power methods. Delta methods use a true fixed-point residual;
    the trial validator recomputes that common certificate for every method.
    Exhaustion is an error.
    This NumPy recurrence is independent of Sail, DataFusion, and Nutmeg.
    """
    _check_edges(vertices, source, target)
    if not math.isfinite(damping) or not 0 <= damping < 1:
        raise ValueError("damping must be finite and in [0, 1)")
    if not math.isfinite(tolerance) or tolerance <= 0:
        raise ValueError("tolerance must be finite and positive")
    if isinstance(max_iterations, bool) or not isinstance(max_iterations, int) or max_iterations < 1:
        raise ValueError("max_iterations must be a positive integer")
    if vertices == 0:
        return PageRankReference(np.empty(0, dtype=np.float64), 0, 0.0)
    degree = np.bincount(source, minlength=vertices)
    dangling = degree == 0
    # Scale once per vertex, then gather by source; parallel edges remain
    # separate contributions in bincount and in the outdegree denominator.
    divisor = np.maximum(degree, 1)
    scores = np.full(vertices, 1 / vertices, dtype=np.float64)
    for iteration in range(1, max_iterations + 1):
        incoming = np.bincount(target, weights=(scores / divisor)[source], minlength=vertices)
        base = (1 - damping + damping * scores[dangling].sum()) / vertices
        updated = base + damping * incoming
        residual = float(np.abs(updated - scores).sum())
        scores = updated
        if residual <= tolerance:
            return PageRankReference(scores, iteration, residual)
    raise RuntimeError(f"reference PageRank did not converge in {max_iterations} iterations; L1={residual}")


def wcc_reference(vertices: int, source: np.ndarray, target: np.ndarray) -> np.ndarray:
    """Independent union-find over every edge; labels are minimum numeric IDs."""
    _check_edges(vertices, source, target)
    parent = list(range(vertices))
    sizes = [1] * vertices

    def find(node: int) -> int:
        while parent[node] != node:
            parent[node] = parent[parent[node]]
            node = parent[node]
        return node

    for source_id, target_id in zip(source, target):
        left, right = find(int(source_id)), find(int(target_id))
        if left != right:
            if sizes[left] < sizes[right]:
                left, right = right, left
            parent[right] = left
            sizes[left] += sizes[right]
    roots = np.fromiter((find(node) for node in range(vertices)), dtype=np.int64, count=vertices)
    minimum = np.full(vertices, vertices, dtype=np.int64)
    np.minimum.at(minimum, roots, np.arange(vertices, dtype=np.int64))
    return minimum[roots]


def _file_details(path: Path) -> dict:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(block)
    return {"sha256": digest.hexdigest(), "bytes": path.stat().st_size}


def prepare(output: str | Path, *, vertices: int, family: str = "sparse", seed: int = 20260927,
            degree: float = 8, tolerance: float = 1e-8, damping: float = 0.85,
            max_iterations: int = 1000, block_size: int = 1024) -> dict:
    """Write a fixture and return its JSON-serializable manifest.

    Files are vertices.parquet(id:int64), edges.parquet(src:int64,dst:int64),
    reference.parquet(id:int64,pagerank:float64,component:int64), manifest.json.
    The manifest is written last. An incomplete directory must be removed or a
    new path chosen explicitly; this function never overwrites existing data.
    """
    output = Path(output)
    if output.exists() and (not output.is_dir() or any(output.iterdir())):
        raise FileExistsError(f"fixture output must be absent or empty: {output}")
    source, target = generate(vertices, family=family, seed=seed, degree=degree, block_size=block_size)
    ranks = pagerank_reference(vertices, source, target, damping=damping, tolerance=tolerance,
                               max_iterations=max_iterations)
    components = wcc_reference(vertices, source, target)
    ids = np.arange(vertices, dtype=np.int64)
    outdegree = np.bincount(source, minlength=vertices)
    indegree = np.bincount(target, minlength=vertices)
    counts = {
        "vertices": vertices, "edges": int(source.size),
        "isolates": int(np.count_nonzero((outdegree == 0) & (indegree == 0))),
        "dangling": int(np.count_nonzero(outdegree == 0)),
        "components": int(np.unique(components).size),
        "self_loops": int(np.count_nonzero(source == target)),
        "mean_outdegree": float(source.size / vertices),
    }
    output.mkdir(parents=True, exist_ok=True)
    tables = {
        "vertices.parquet": pa.table({"id": ids}),
        "edges.parquet": pa.table({"src": source, "dst": target}),
        "reference.parquet": pa.table({"id": ids, "pagerank": ranks.scores, "component": components}),
    }
    for name, table in tables.items():
        pq.write_table(table, output / name, compression="zstd", row_group_size=128 * 1024)
    manifest = {
        "format_version": 1,
        "generator": {"numpy": np.__version__, "pyarrow": pa.__version__, "rng": "PCG64"},
        "family": family, "seed": seed, "counts": counts,
        "parameters": {"degree": degree, "block_size": block_size,
                       "degree_applies": family == "sparse", "seed_applies": family == "sparse"},
        "semantics": {"ids": "consecutive int64, 0..V-1", "parallel_edges": "retained",
                      "self_loops": "retained", "wcc_labels": "minimum numeric vertex ID",
                      "chain_direction": "i -> i+1" if family == "chain" else None,
                      "chain_undirected_diameter": vertices - 1 if family == "chain" else None},
        "pagerank": {"damping": damping, "tolerance": tolerance, "max_iterations": max_iterations,
                     "iterations": ranks.iterations, "residual": ranks.residual, "converged": True,
                     "residual_kind": "L1 difference between successive iterates",
                     "initialization": "uniform", "restart": "uniform", "dangling": "uniform",
                     "precision": "float64", "score_sum": float(ranks.scores.sum())},
        "files": {name: _file_details(output / name) for name in tables},
    }
    (output / "manifest.json").write_text(json.dumps(manifest, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    return manifest


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", required=True, type=Path)
    parser.add_argument("--vertices", required=True, type=int)
    parser.add_argument("--family", choices=["sparse", "chain"], default="sparse")
    parser.add_argument("--seed", type=int, default=20260927)
    parser.add_argument("--degree", type=float, default=8)
    parser.add_argument("--tolerance", type=float, default=1e-8)
    parser.add_argument("--damping", type=float, default=0.85)
    parser.add_argument("--max-iterations", type=int, default=1000)
    parser.add_argument("--block-size", type=int, default=1024)
    manifest = prepare(**vars(parser.parse_args()))
    print(json.dumps(manifest, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
