# Pecan: portable graph algorithms through Spark Connect

Status: implemented on `work/extensions-datafusion-graphs`. The
[initial exact-revision validation record](portable-graph-validation.md) covers
the original power PageRank and minimum-label WCC delivery. The
[current API tutorial](../../../examples/extensions/graph-algorithms/README.md)
also describes the subsequent delta/frontier and randomized-contraction methods,
including an explicit fused WCC plan. The
[benchmark report](pecan-nutmeg-benchmark.md) records their time/memory measurements
and exact-revision qualification; the
[all-path tutorial](../../../examples/extensions/benchmarks/TUTORIAL.md) runs all fifteen combinations.

Pecan, the first distributed graph algorithm implementation, is a pure PySpark client
with a small Sail utilities service. This adopts Semyon Sinchenko's
`pyspark-graph-algorithms` project draft and develops the client-loop alternative
from the [graphframes-rs plan](graphframes-rs-plan.md). The compiled-in
server-side algorithm controller remains a later option, not a prerequisite.

Pecan is distributed as `pyspark-pecan` and imported as `pyspark_pecan`.
The former `pyspark_graph_algorithms` import paths remain compatibility aliases;
the `gf.utils.v1` protocol and existing source-directory/document URLs are unchanged.

## Architecture and scope

```text
Python algorithm loop
  -> ordinary Spark Connect joins / aggregates / Parquet writes
  -> Sail planning, distributed stages and worker memory/spill
  -> Parquet stage read by the next iteration

Python lifecycle helper
  -> gf.utils.v1.Request relation -> eager typed receipt
  -> driver-owned storage operations using Sail's object-store registry
```

The Python process controls iterations and collects only bounded control results.
Graph rows remain in engine relations. The original power PageRank and exact
minimum-label WCC remain the defaults. Explicit `method="delta"` PageRank adds
signed residual pushes, inactive-residual retention, reactivation and a final
global fixed-point certificate. Explicit `method="randomized"` WCC adds seeded
closed-neighborhood contraction and reverse representative expansion using
native `gf_axpb`, without a prime-field fallback. `method="randomized_fused"`
uses forward/reverse projections with `min_by`, removing the priority-table joins
and deferring initial canonicalization. It preserves representative choices but
computes priorities per edge row; the original plan remains a measured control.
This adapts [graphframes-rs PR 56](https://github.com/SemyonSinchenko/graphframes-rs/pull/56/files)
without adding Sail host APIs. These are separate methods;
the earlier validation record does not qualify the later algorithms.

PageRank defines reset probability, dangling-node redistribution, normalization,
convergence norm and iteration-limit behavior in its client API. WCC treats edges
as undirected for connectivity and chooses the minimum original integer ID as
component label. Both preserve isolated vertices and reject invalid IDs or
endpoints. Duplicate-edge and self-loop semantics must be documented and tested.
Algorithms beyond the implemented set are explicitly unsupported.

## Relationship to the existing extensions

| Surface | Role |
| --- | --- |
| Sedona wheel | Native spatial scalar functions, including worker execution. |
| Nutmeg graph-table helpers | Degrees, triplets and bounded walks using ordinary Sail plans, with no CSR. |
| Nutmeg native wheel | Explicit graph staging and driver-native kernels; host-funded quota and shared CSR snapshots. |
| Pecan client and graph utils | Iterative relational algorithms, distributed through ordinary queries, with engine-owned storage utilities. |

The portable client is a new graph API implementation, not another CSR engine.
The benchmark exposes it through Nutmeg's graph-table API as **Nutmeg Grenada**;
this adapts tables into the same Pecan controller. **Nutmeg Banda** names the native
path. Its existing Grust kernels remain available, alongside explicit local
`pagerankDelta`, `wccRandomized` and `wccRandomizedFused` additions with
corresponding algorithm contracts. Banda already selects from both edge endpoints
in one loop; its fused variant only defers the initial kernel sort/deduplication.
No path silently falls back to another. Relational execution avoids a separate native topology but
still creates shuffle buffers, spill files and successive Parquet generations.
No single-copy or total-RSS guarantee is made.

## Sail integration boundary

Reuse the existing zero-input relation extension and function-by-name paths.
Filesystem mutations execute when their receipt is consumed, never during
planning or schema analysis. Use one canonical protobuf schema and explicitly
defined Arrow receipt columns. A protobuf message definition alone is not the
DataFrame result schema.

The first branch implementation uses compiled-in utils registration under an
explicit opt-in. This is a deliberate packaging deviation from the draft's wheel:
the current wheel binding exposes neither the host object-store registry nor
host configuration, and a single manifest cannot combine driver-only relations
with worker scalar functions in cluster mode. Compiled-in registration preserves
host ownership without inventing a new FFI resource API.

A later wheel version can split filesystem and scalar registrations into two
entry points, but still needs a narrow host storage capability. Do not expose
Rust SessionContext or ObjectStore trait-object layouts across the native ABI.
Custom aggregate loading is outside the first PageRank/WCC milestone: their
reductions use builtin sum/min.

[Upstream PR #2670](https://github.com/lakehq/sail/pull/2670), reviewed at
`8f8ba9d8`, provides a related Python storage bridge through Sail's host registry.
Its private proxies are scoped to Python datasource callbacks, not Connect
extension callbacks or graph-run lifetimes. Reuse the common `sail-object-store`
resolution layer and align URI handling with that work. The utils service adds
run ownership, bounded receipts and teardown above this storage foundation; it
must not construct competing Python storage clients. Integrating the callback
bridge itself is unnecessary for the initial Rust host utility implementation.

## Storage and lifecycle contract

Use a server-configured trusted root and Sail's existing object-store registry;
the client supplies neither credentials nor an unrestricted filesystem root.
The initial configuration is `SAIL_GRAPH_UTILS_ROOT`; this does not imply that
Spark's `spark.checkpoint.dir` is already wired into Sail's plugin bindings.
For multi-host execution, the driver and every worker must reach the same store.
For `file://`, precreate a dedicated directory. Sail canonicalizes its root and
rejects existing symlinks in run paths before storage operations. Administrators
must keep the directory exclusively managed by Sail; this check does not defend
against concurrent hostile filesystem changes.

Allocate a server-owned namespace per run, with retry identity and a run token.
Scope deletion to that run, forbid root deletion and cross-run access, normalize
URIs and reject traversal. Define bounded listing behavior explicitly.
Checkpoint storage and operator spill are separate storage consumers.

The client materializes input relations and each iteration, then reads the
materialized result. Publish a new stage only after a successful write. Delete
an older stage only when no remaining relation depends on it. A result handle
retains its final stage until closed or copied to caller-owned output.
Separately materializing vertices and edges does not create an atomic snapshot
across arbitrary changing sources; callers must supply stable inputs.

Use finally/context cleanup after completed writes, including validation errors
and cancellation between iterations. An interrupted or failed write leaves its
namespace owned by the session: the exception exposes `run_path` and
`cleanup_deferred = True`. A Connect interrupt acknowledgment does not guarantee
that every worker writer has terminated, so immediate deletion would race those
writers. On session close or idle expiry, request job shutdown, then attempt
cleanup with bounded retries. Teardown cleanup is best effort: there is no
universal writer-drain barrier or crash-persistent orphan catalog in this POC.
Persistent storage failures and abrupt server termination require administrator
cleanup after confirming the writers have stopped.

Runs belong to the host session incarnation. There is no independent per-run TTL in v1:
`lease_seconds = 0` denotes session ownership. A killed client therefore leaves
files until session teardown. Validate observed orphan cleanup with an explicit
short session-expiry fixture and cancellation during an actual write. These
tests do not establish a general termination barrier. A new session cannot
adopt an old session’s run token. Root/run ownership and commit state are
correctness requirements, not optional cleanup polish.

Do not infer committed file count from `repartition(n)`. Validate schema, rows
and algorithm invariants; list files only for bounded diagnostics and cleanup.

## Delivery sequence

1. Freeze the versioned utils request and receipt contract, capability names,
   ownership rules and graph algorithm semantics.
2. Implement Sail driver storage handlers and worker scalar registration, with
   pure planning, explicit configuration and boundary tests.
3. Implement the portable Python client, stage ownership and PageRank/WCC;
   add runnable examples and exact small-graph reference tests.
4. Qualify local and process-worker execution, failures, cancellation and orphan
   cleanup. Run Linux gates on Morrobay and native macOS checks.
5. Run a genuine Capitola–Morrobay graph workload with shared storage and retain
   evidence that algorithm joins and aggregations execute on both hosts.
6. Publish concise installation/API documentation and exact-revision validation.

A Scala/Spark utils implementation and cross-engine semantic qualification are
subsequent milestones. Common PySpark APIs are the portability target, not proof
that every Spark Connect implementation already supports the client. Pin the
client environment and disclose the engines/modes actually tested.

## Acceptance criteria

- No graph rows are collected into the Python control loop.
- Schema analysis and EXPLAIN do not allocate/delete a run or execute algorithms.
- PageRank and WCC match independently specified small-graph results, including
  sinks, isolates, loops, duplicates, disconnected and empty graphs.
- Invalid graph data/options and missing capabilities fail explicitly. Reaching
  an iteration limit is not reported as convergence.
- Driver filesystem operations reuse host storage; worker functions resolve after
  plan serialization in fresh workers. `gf_axpb` is bit-exact with GF(2^64).
- Run roots cannot escape configured storage or delete another run. Retries,
  partial writes, explicit close, exceptions and client termination are covered.
- Distributed tests exercise algorithm stages, not only input preparation.
- Existing Sedona and Nutmeg local/distributed behavior remains covered.

## Deferred server-side option

The [graphframes-rs integration plan](graphframes-rs-plan.md) retains the design
for reusing its Rust algorithm implementation inside Sail. Revisit it when lazy
query composition or server-owned iteration justifies the extra execution
interface, preparation hook and child-job lifecycle. The portable client provides
an independent semantic reference, but a min-label WCC result is not evidence
that randomized contraction uses the same intermediate algorithm. Pecan's current
randomized method and Banda's local kernel share seeded contraction semantics;
neither imports graphframes-rs's Rust implementation.
