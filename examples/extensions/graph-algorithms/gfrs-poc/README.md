# gfrs-poc: graphframes-rs Pregel engine in pure PySpark

A minimal re-implementation of the [graphframes-rs](../../../../graphframes-rs)
Pregel engine and its delta PageRank, written in 100% pure PySpark (Spark Connect
client code, no custom JVM/JNI/Scala). It is a proof of concept used by the
[`sem_benchmark`](../../../benchmarks/sem_benchmark) harness to compare against
the native Nutmeg kernels on Sail.

| Rust source | Python source | What is kept |
| --- | --- | --- |
| `src/algorithm/pregel.rs` | `src/gfrs_poc/pregel.py` | Builder API, message structs + union-by-name, participation column, vertex voting, `skip_dest_state`, column prefixing, per-iteration parquet checkpoints |
| `src/memory/parquet_checkpointer.rs` | `src/gfrs_poc/checkpointer.py` | write + read-back checkpoints, hash-repartitioned and sorted writes (`push_pre_sorted`) |
| `src/algorithm/centrality/pagerank.rs` | `src/gfrs_poc/pagerank.py` | GraphX-style deltas, `delta / out_degree` messages, decreasing active frontier, voting convergence, final normalization |

## Kept optimizations

- **Parquet checkpointing of the state every iteration** (`state-<i>`): the lazy
  lineage stays a short "read parquet -> transform" plan and the working set
  lives on disk, not in memory.
- **Edges checkpointed once** before the loop, with the `__pregel_msg_edge_*`
  column names used by every later join.
- **Aggregated messages checkpointed** (`aggregated-messages-<i>`) to cut the
  memory peak of the aggregation.
- **Pre-sorted checkpoint layout**: state and edges are hash-repartitioned into
  `num_partitions` parts and sorted by the join key within partitions before the
  write (the pure-PySpark equivalent of the Rust `push_pre_sorted`; PySpark
  cannot *declare* the layout back to the optimizer, so the server-side join
  strategy is controlled by the Sail `optimizer.prefer_hash_join` setting).
- **GraphX-style truncation**: with `skip_dest_state`, the participation filter
  is applied to the source side *before* the join, so the join input shrinks as
  the active frontier decreases.

## Differences from the Rust version (POC scope)

- **No eviction/purge**: the PySpark Connect API does not expose FS list/delete,
  so checkpoints are never removed. Every run writes under
  `<checkpoint_dir>/<run-uuid>/`; wipe that directory manually (or keep
  `--work-dir` on a large NVM disk).
- **No scoped session**: the Rust engine flips `datafusion.optimizer.prefer_hash_join`
  per algorithm; here the flag is a server-level Sail setting
  (`SAIL_OPTIMIZER__PREFER_HASH_JOIN`, see the benchmark's `--use-smj`).
- Source vertices only (no personalized PageRank yet).

## Usage

```python
from pyspark.sql import functions as F
from gfrs_poc import pagerank, Pregel, MessageDirection, pregel_src, pregel_default_msg

result = pagerank(edges, vertices, tol=1e-5, max_iter=0,
                  checkpoint_dir="/mnt/nvm/work/gfrs", num_partitions=8)
result.show()  # id, pagerank (normalized to sum 1)

# The generic engine, e.g. in-degree by counting messages:
in_degree = (
    Pregel(edges, vertices, checkpoint_dir="/mnt/nvm/work/gfrs", num_partitions=8)
    .add_vertex_column("in_degree", F.lit(0),
                       F.col("in_degree") + F.coalesce(pregel_default_msg(), F.lit(0)))
    .add_message(F.lit(1), MessageDirection.SRC_TO_DST)
    .add_aggregate_expr(F.sum(pregel_default_msg()))
    .skip_dest_state()
    .max_iterations(1)
    .run()
)
```

Vertices must expose an `id` column and edges `src` / `dst` columns (int64),
matching the graphframes-rs defaults.
