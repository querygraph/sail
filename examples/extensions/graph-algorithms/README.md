# Pecan: relational graph algorithms

The [benchmark report](../../../docs/development/extensions/pecan-nutmeg-benchmark.md)
compares reference, advanced and fused methods through Pecan, Banda and Grenada.

Pecan is a pure Python client that runs graph algorithms through ordinary Spark Connect
queries. Joins, aggregations and Parquet writes run in Sail/DataFusion; the
client advances iterations and receives only scalar reductions and filesystem
receipts. It does not build a local graph representation.

PageRank offers reference power iteration and an active-frontier delta method.
WCC offers reference minimum-label propagation and seeded randomized contraction,
including an optional fused representative plan.
The traversal branch adds BFS and nonnegative weighted SSSP with reference,
frontier, and explicit advanced methods; follow the
[BFS/SSSP tutorial](../benchmarks/TRAVERSAL-TUTORIAL.md) for that branch and its
local/process-worker checks.
The reference methods remain the defaults. This is a new API, not a GraphFrames
wire or behavioral compatibility layer.

For a fresh build and executable review, follow the [testing tutorial](TESTING.md).
It covers local execution, separate worker processes, two hosts, expected
algorithm answers and cleanup checks.

The [benchmark guide](../benchmarks/README.md) defines comparisons retaining all
reference and optimized algorithms. **Nutmeg Banda** runs native kernels over a
staged driver-resident graph. **Nutmeg Grenada** exposes Nutmeg graph tables to
Pecan's relational controller; it shares these implementations rather than
providing an independent third algorithm. Results distinguish both entry path
and method, with explicit timing and memory boundaries.

## Install and run

The distribution is `pyspark-pecan`; import it as `pyspark_pecan`.
The existing `examples/extensions/graph-algorithms` source directory and tutorial
URLs are unchanged. Old `pyspark_graph_algorithms` imports, including its
submodules, remain compatibility aliases supplied by Pecan. The wire protocol
remains `gf.utils.v1`.

Clone the implementation branch:

```bash
git clone --branch work/extensions-datafusion-graphs https://github.com/querygraph/sail.git
cd sail
```

Follow the source tutorial's [prerequisites](../TUTORIAL.md#3-prepare-a-build-machine)
and [build instructions](../TUTORIAL.md#4-build-and-install), keeping this branch
checked out. The tutorial's older `sail-extensions-1` tag does not contain these
algorithms or their host utils service. Use this branch's binary and its pinned
Python environment: PySpark Connect 4.0.1 and protobuf 7.36.2. A released Sail
wheel does not contain the new service either.

```bash
uv pip uninstall --python .venv/bin/python pyspark-graph-algorithms
uv pip install --python .venv/bin/python --no-deps ./examples/extensions/graph-algorithms

# Set these on the Sail server, in addition to its usual embedded-Python setup.
export SAIL_EXPERIMENTAL_EXTENSIONS=1
mkdir -p /absolute/path/to/graph-staging
export SAIL_GRAPH_UTILS_ROOT=file:///absolute/path/to/graph-staging
target/extensions-poc/host/debug/sail spark server --ip 127.0.0.1 --port 50051
```

The uninstall removes the former distribution when upgrading an existing review
environment; Pecan itself supplies the compatibility imports. Fresh builds
already install Pecan through `examples/extensions/scripts/build.sh`.

For multiple hosts, configure a shared object store or filesystem visible under
the same URI to every worker. A driver's private local directory is insufficient.
The service uses Sail's existing object-store registry and credentials.
The local root must already exist and be exclusively managed by Sail. Symlinks
inside owned runs are rejected; concurrent external filesystem modification is
outside this storage contract.
The [two-host recipe and validation record](../../../docs/development/extensions/portable-graph-validation.md#reproduction)
show how to supply shared storage configuration to the launcher.

In another terminal:

```bash
.venv/bin/python examples/extensions/graph-algorithms/examples/run.py \
  --remote sc://localhost:50051 --partitions 4
```

## API

```python
from pyspark.sql.connect.session import SparkSession
from pyspark_pecan import GraphAlgorithms

spark = SparkSession.builder.remote("sc://localhost:50051").create()
vertices = spark.createDataFrame([(0,), (1,), (2,), (9,)], "id long")
edges = spark.createDataFrame([(0, 1), (1, 2), (2, 0)], "src long, dst long")
graph = GraphAlgorithms(spark)

with graph.pagerank(vertices, edges, max_iterations=20) as result:
    result.frame.show()                 # id, pagerank
    # Optional persistent export, owned and cleaned up by the caller:
    # result.write_parquet("s3://my-bucket/results/pagerank")

with graph.wcc(vertices, edges) as result:
    result.frame.show()                 # id, component
    assert result.converged
```

Select the optimized methods explicitly. These calls use the same input and
result ownership rules:

```python
with graph.pagerank(vertices, edges, method="delta", tolerance=1e-8,
                    max_iterations=1000) as result:
    result.frame.show()
    print(result.residual, result.error_bound)

with graph.wcc(vertices, edges, method="randomized", seed=42,
               max_iterations=100) as result:
    result.frame.show()
# Same contraction choices, with fewer intermediate writes and joins:
with graph.wcc(vertices, edges, method="randomized_fused", seed=42,
               max_iterations=100) as result:
    result.frame.show()
spark.stop()
```

### Valid graph contract

Pecan assumes a valid graph and never spends a job checking it. The caller
guarantees, and Pecan does not verify:

- vertex `id` is BIGINT, unique and non-null;
- edge `src` and `dst` are BIGINT, non-null, and name existing vertices;
- for shortest paths, `weight` is DOUBLE, finite and non-negative, and
  distance sums stay finite;
- for traversals, the source is a vertex;
- `distance / delta` fits the engine's floor for delta-star.

The only input check is the schema (column names and BIGINT/DOUBLE types),
which requires no execution job (planning RPCs may still occur). A graph that breaks the contract produces an undefined
result, with no promised error diagnosis. Properties are ignored; results contain the structural
columns shown above. Duplicate edges and self-loops are permitted. Isolated
vertices are preserved. Arguments (iteration caps, tolerances, seeds,
methods, sources) are validated once by the Pydantic option models in
`pyspark_pecan.types`, so a bad argument fails before any server access.
The client separately snapshots the two input relations into Parquet; this is
not an atomic snapshot across mutable input sources.
There is no optional algorithm validation mode. A separate explicit validation
utility, if desired by a caller, must run before the algorithm and outside its
timer; benchmark certificates remain independent post-execution checks.

Reference/frontier BFS and SSSP, DeltaStar, and WCC omit an eager vertex count.
PageRank retains N for normalization and push/pull BFS for direction switching.
Minimum-label WCC uses a limit-one empty probe to preserve its zero-iteration
empty result; randomized WCC keeps its contraction counts and metrics. These
are algorithmic actions, not full-data validity audits. Traversals seed a lazy
one-row range with exact BIGINT literals instead of filtering the vertex table.
No checkpoint write issues an additional expected-row count audit.

The relational Grenada adapter shares this Pecan implementation and contract.
Argentea clients also omit Python input-audit jobs, while retaining counts
required by native request metadata, native protocol/resource guards and result
diagnostics. Validation-policy changes must be disclosed when comparing new
runs with historical runs; existing measurements are not changed retroactively.

The package is fully typed: every definition carries type hints, observer
events are `IterationEvent` models (`event.as_dict()` gives the flat record
shape), and contraction rounds are `ContractionStep` models. Package gates run
`mypy` and `ruff` on `src/pyspark_pecan`.

| Method | Stopping rule | Defaults and limits |
|---|---|---|
| `pagerank(method="power")` | Fixed number of steps when `tolerance=None`; otherwise successive-rank L1 change <= tolerance | Default method; reset 0.15, 20 steps, no tolerance |
| `pagerank(method="delta")` | Normalize output and certify full fixed-point L1 residual <= tolerance | Requires a positive tolerance; set `max_iterations=1000` for strict convergence runs |
| `pagerank(method="pregel")` | Exactly `max_iterations` steps | The Pregel paper's and GraphX's static form: no dangling term, one job per step; `normalize=True` divides by the total |
| `pagerank(method="pregel_delta")` | Exactly `max_iterations` steps; with `vote_to_halt=True`, no vertex's delta exceeds the tolerance | GraphX's dynamic form as in graphframes-rs: only vertices whose last gain exceeds the tolerance send, one job per step, no dangling term and no certificate. Requires a tolerance on GraphX's scale (a vertex starts at the reset probability); `normalize=True` divides by the total |
| `pregel()` | Exactly `max_iterations` steps, or a vote to halt | A Pregel program in the form of graphframes-rs's builder: vertex columns, messages, an aggregate, optional participation and vote. `pagerank(method="pregel_delta")` is such a program |
| `wcc(method="min_label")` | No label changes | Default method; at most 100 propagation rounds |
| `wcc(method="randomized")` | No edges remain after contraction, then reverse expansion | Seed 42; at most 100 contraction rounds |
| `wcc(method="randomized_fused")` | Same as `randomized` | An alias kept for older configurations |

Each PageRank step is
`reset / N + (1 - reset) * (incoming_probability + dangling_probability / N)`.
Both PageRank methods start uniformly, count duplicate edges separately and
redistribute dangling probability uniformly. They return `id: bigint` and
`pagerank: double`, with total probability approximately one. `reset_probability`
must lie in `(0, 1]`. Fixed-step power iteration returns `converged=None`.

Delta retains accumulated, unsent signed residual and can reactivate vertices
after they become inactive. For `N` vertices, current score mass `m` and total
pending residual norm `R`, a vertex is active when its absolute pending residual
exceeds `min(R / (2*N), tolerance*m / (4*N))`. The tolerance-scaled term permits
small contributions to accumulate; the relative cap preserves progress. No
inactive residual is discarded. For normalized output `y` and the PageRank operator
`T`, its final certificate is `||T(y) - y||_1 <= tolerance`; this implies stationary
L1 error at most `tolerance / reset_probability` in exact arithmetic. The measured
DOUBLE certificate is `result.residual`, and `result.error_bound` is that residual
divided by reset. This differs from power's successive-iterate stopping rule.
`result.iterations` counts frontier pushes, excluding full certificate passes;
uniform-stationary input can require zero pushes. The API's iteration default is
still 20, so specify a larger cap with a strict delta tolerance.

Only active sources emit delta edge messages. Sail's relational join may still
scan the full Parquet edge table; fewer propagated messages do not establish
fewer physical edge reads or faster execution. Fewer rounds can also involve more
propagated edge messages; the benchmark records both work counts and elapsed time.

All WCC methods treat edges as undirected and return `id: bigint` and
`component: bigint`, the minimum vertex ID in each component. Isolates retain
their own IDs; duplicate edges and self-loops do not alter membership. Minimum-label
propagation can take component diameter + 1 rounds, including the no-change check.
Randomized contraction uses reproducible GF64 affine priorities, retains original
vertex identities, and expands representative mappings in reverse. Its seed is
an unsigned 64-bit integer; changing it can change the contraction work, not the
required component answer. It requires the `axpb` capability and `gf_axpb` scalar
function; missing capability fails explicitly, with no hash fallback.

`randomized_fused` follows the fused edge-projection approach in
[Sem's graphframes-rs change](https://github.com/SemyonSinchenko/graphframes-rs/commit/10715e28d9f7c450e74881bcd4acce8dc99a250f).
Forward and reverse edge projections carry each neighbor's GF64 priority into
one grouped `min_by`/`min` aggregation; comparing with the vertex's own priority
selects the closed-neighborhood representative. `min_by` keeps the original
neighbor ID, so the existing coefficient stream, representative maps and labels
are unchanged. Priorities are unique for distinct IDs when the multiplier is
nonzero; duplicate edges therefore give equal-priority ties only for the same ID.

The fused plan skips the initial canonical edge write and per-round priority
table/write and its two representative-selection joins. It starts with already
snapshotted non-loop edge rows, including duplicates and both orientations, then
canonicalizes/deduplicates after each contraction. Its first `edges_before`
metric counts those raw rows; later contraction traces match `randomized` for
the same graph and seed. Endpoint relabeling joins and reverse mapping remain.
This is a separate opt-in plan, not a replacement or a measured speedup claim;
the edge projections can still scan their full inputs and recompute neighbor
priorities per edge row instead of once per active vertex. Grenada shares this
Pecan method through its graph-table adapter.

Empty graphs return empty results with zero iterations. All WCC methods and
tolerance-controlled PageRank raise `ConvergenceError` when their cap is exhausted.

## Staging, cancellation and ownership

Algorithms materialize Parquet generations and check schema and vertex counts.
Power, delta and minimum-label propagation release obsolete iteration state.
Randomized WCC retains representative maps until its reverse expansion consumes
them, while releasing superseded edge tables. The requested partition count is
not equated with the number of output files. At completion only the result
generation remains in the run directory.

`GraphResult` owns that directory. `close()` or leaving its context removes it
and invalidates its DataFrame. `touch()` keeps the owning server session active;
results do not survive session expiration or `spark.stop()`. `write_parquet()`
exports to an independently owned path before closing the result. Do not keep
the returned DataFrame beyond its result context unless you have exported it.
Choose an export destination outside the owned staging run.

Validation failures, convergence failures and cancellation between completed
writes remove the run immediately. A failed or interrupted write RPC has an
uncertain outcome: its remote writers may still be stopping, so the client
**does not delete that run immediately**. The exception exposes
`cleanup_deferred=True` and `run_path`; cancellation raises
`GraphCancelledError` with the original engine error as its cause. Other
failures retain their original exception type.

Sail retains ownership of uncertain runs and attempts cleanup at session
shutdown or expiration. This also covers a client that dies or cannot reach
the server. Teardown cleanup is best effort: it is not a universal barrier
proving every nested writer or remote storage operation has finished, and
persistent storage failures can require administrative cleanup. Close the
session when finished; keeping it alive also retains deferred runs. This first
protocol has no independent run TTL
(`lease_seconds=0`). It does not delete an active run on a separate timer.

```python
from pyspark_pecan import CancellationToken

token = CancellationToken()
# A UI or another thread can call token.cancel(). It interrupts only queries
# tagged by this algorithm; the controller checks cancellation between actions.
with graph.pagerank(vertices, edges, cancellation=token) as result:
    result.frame.show()
```

Cancellation is cooperative. `InterruptTag` reaches operations already
registered on the server; a cancellation racing the registration of a new
operation is best effort, not an atomic guarantee. Untagged operations and
schema-analysis RPCs are outside that interruption mechanism.

For an optional progress callback, construct
`GraphAlgorithms(spark, observer=callback)`. It receives dictionaries containing
`kind`, `algorithm`, `iteration`, and `run_path`. Iterations emit `iteration_start`
and `iteration_end`; delta also emits `certificate`. Delta end events include
`frontier_size`, `active_edges`, `reactivated_vertices`, maintained `residual` and
`normalized_residual_bound`. Certificate events report the recomputed global
residual. Randomized WCC end events include `active_vertices`, `edges_before`,
`edges_after` and the round's affine coefficients. Frontier sizes need not shrink
monotonically. Exceptions raised by the callback abort the run and trigger cleanup.

## Required server contract

The canonical [protobuf schema](../../../crates/sail-session/proto/gf/utils/v1/utils.proto) specifies
`gf.utils.v1.Request`, carried by a zero-input `Relation.extension` with type URL
`type.googleapis.com/gf.utils.v1.Request`. The operations are `Ping`, `Mkdir`,
`Exists`, bounded `Ls`, and `Rm`. Their responses are typed Arrow columns, not
serialized protobuf receipt blobs. Every request is executed eagerly by
collecting its bounded receipt.

The checked-in Python messages were generated with `protoc 36.1` (Python
gencode 7.36.1). To regenerate after changing the canonical schema, run
`python examples/extensions/graph-algorithms/generate_proto.py` from the repo.

The client requires protocol version 1 and capabilities `fs` and
`owned_runs_v1`. `Mkdir` accepts a client-generated request UUID and returns an
opaque run token. Subsequent filesystem operations require that token and the
same session. A path under the root is not sufficient authority; root deletion
is prohibited. Retrying allocation is idempotent; retrying removal is safe.
Power/delta PageRank and minimum-label WCC require no native function capability.
Randomized WCC additionally requires `axpb` and executes the host's `gf_axpb`
scalar on every worker that evaluates its priority expression.
The fused plan also uses the engine's ordinary `min_by` aggregate; Sail provides
it on this branch.
Other graph algorithms are not exposed by this initial API.
The host accepts at most 8 KiB per request, lists at most 1,000 entries plus its
summary row, and retains at most 1,024 run identities per session (including
released runs for retry safety). Use a new session after reaching that limit.

The client uses the same protocol for any compatible Spark Connect engine;
cross-engine portability requires an engine's utils implementation and semantic
tests. This Sail implementation alone does not establish support on Spark or
Snowpark. No Python UDF, JVM, RDD, `cache`, `persist`, or Spark checkpoint API is
used by these algorithms.

## Test

```bash
PYTHONPATH=examples/extensions/graph-algorithms/src \
  SAIL_GRAPH_TEST_REMOTE=sc://localhost:50051 \
  .venv/bin/python -m pytest -q examples/extensions/graph-algorithms/tests
```

The small correctness fixtures deliberately collect their final answers for
comparison. Algorithm implementations collect only scalar values, never graph
rows. Local, process-worker and two-host execution run the same client.
