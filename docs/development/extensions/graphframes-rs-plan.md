# Running graphframes-rs inside Sail

**Status: deferred server-side option.** The first implementation now follows
the [Pecan PySpark client plan](portable-graph-plan.md), using ordinary Sail
queries and a small storage/function utils service. The server controller and
Rust library integration described below remain proposed, not implemented.

Implement PageRank and weakly connected components (WCC) over Sail tables using
`graphframes-rs`. Execute their DataFusion joins and aggregations inside Sail,
first locally and then across Sail workers. Keep graph data as relations;
this path requires no CSR conversion.

Review scope: `querygraph/sail`, branch `work/extensions-datafusion-graphs`,
starting at `088a1d540`; graphframes-rs source pinned to
[`b4da56da`](https://github.com/SemyonSinchenko/graphframes-rs/tree/b4da56dabe20bba8e29563e06acc5179b2113ce3).
This proposal extends the existing [graph-table work](datafusion-graph-plan.md).
The native Nutmeg path remains a separate execution choice.

## What can be reused

The library already builds graph algorithms from DataFusion DataFrames.
[PageRank](https://github.com/SemyonSinchenko/graphframes-rs/blob/b4da56dabe20bba8e29563e06acc5179b2113ce3/src/algorithm/centrality/pagerank.rs)
uses its Pregel engine;
[WCC](https://github.com/SemyonSinchenko/graphframes-rs/blob/b4da56dabe20bba8e29563e06acc5179b2113ce3/src/algorithm/connectivity/connected_components.rs)
uses randomized contraction followed by backward propagation. Both materialize
intermediate state and write their final results to Parquet. Their `run` methods
return iteration counts, rather than result DataFrames.

The library's Cargo requirement is `datafusion = "55.0.1"`; its reviewed lockfile
resolves 55.1.0, matching the Sail branch. Compile with one resolved DataFusion
version and verify Arrow dependencies before treating this as compatibility.
Disable the library's default CLI feature when embedding it.

The current Sail native relation interface supplies physical input plans and
places native graph execution on the driver. Simply wrapping graphframes-rs in
that interface would not distribute its iterations. The library also executes
physical partitions directly in its specialized checkpoint writer.

## Alternative: a client-controlled loop

A Python client can issue each iteration as ordinary Spark Connect joins and
aggregations, with materialization between iterations. Sail already distributes
these relational operations. This avoids a new host adapter and provides an
independent way to establish graph semantics and exercise worker execution.
The [portable client implementation](../../../examples/extensions/graph-algorithms/README.md)
now supplies the loop, convergence checks and owned Parquet staging. Its
[integration plan](portable-graph-plan.md) records the lifecycle limits and
qualification requirements. Its original minimum-label WCC remains available;
an explicit randomized-contraction method now adds the same algorithm family.
The client implementation does not import this library's Rust code.

| Choice | Benefit | Cost |
| --- | --- | --- |
| Client-controlled loop | No new Sail execution interface; each iteration is a separate observable request. | Client connectivity and round trips are part of execution; algorithm and checkpoint logic live in the client; results are materialized before composition. |
| Server-controlled library | Reuses graphframes-rs; one lazy relation can compose inside a query; iteration control does not depend on client round trips. | Requires parent/child job observability, cancellation, checkpoint lifetime and host integration. SQL exposure would need separate syntax/registration work. |

Build a small client-loop reference first, starting with PageRank and a simple
WCC, using verified materialization support. Treat it as a useful deliverable if
server integration is deferred. Continue with the server route when reusing the
Rust library and composing a lazy algorithm result justify the additional host
work; do not claim a performance advantage before measuring it.

## Proposed execution model

```text
Connect relation request: algorithm + options + vertices + edges
                            |
                   Sail graph adapter
                            |
             driver iteration controller
                            |
          Sail planning and JobService / JobRunner
                            |
       local execution OR distributed worker stages
         joins -> aggregations -> checkpoint writes
                            |
           committed checkpoint + scalar status
                            |
                next iteration / final scan
```

The controller handles iteration state and convergence. Graph rows remain in
worker stages and checkpoint storage; only bounded control results return to the
controller. A local run uses the same interface with Sail's local job runner.

Begin with an optional, compiled-in `sail-graphframes` adapter. Keep the algorithm
library independent of Sail. This avoids inventing a native plugin ABI while the
execution interface is still being established. A separately installed wheel is
a later packaging decision; do not pass Rust `SessionContext` objects through
the existing FFI boundary.

## Controller placement: prerequisite design

Run the controller in the server's request/operation orchestration layer, outside
any worker task and outside the scheduler actor's message handler. It submits
child jobs asynchronously through `JobRunner`; it never waits while holding a
worker-slot permit, scheduler lock or a parent execution admission permit that
its children need.

The proposed hook is an execution-preparation phase before the outer query is
submitted to `JobRunner`. Keep algorithm relations as logical placeholders during
analysis. During execution, prepare them in dependency order, run their child
jobs, replace them with committed checkpoint scans, and then physically plan and
submit the outer query. No controller node reaches worker-plan serialization.
Deduplicate preparation by operation/node identity; do not reuse results across
independent actions without an explicit cache contract.

This hook does not exist yet. The current Connect
[`handle_execute_plan`](../../../crates/sail-spark-connect/src/service/plan_executor.rs)
resolves the plan and starts its job before registering the response executor.
The new operation must be registered and cancellable *before* preparation starts;
put the reusable preparation logic below the Connect transport. Ensure schema
analysis never invokes it. Track child jobs under the parent operation with
algorithm, iteration, job ID, progress and attributed failures. Reattachment
must observe the same operation rather than launch another controller.

Before integrating either algorithm, demonstrate a fake two-step controller with
one available worker slot, concurrent ordinary work, cancellation during child
execution, and cleanup. Audit any job-admission semaphore as well as task slots.
This is a prerequisite design gate, not a claim that deadlock freedom is already
implemented or proven. If this hook is unsuitable, retain the client-loop route
while revising the host design.

## Library interface to extract

Introduce a small execution interface in graphframes-rs, retaining a standalone
DataFusion implementation. These are proposed responsibilities, not existing API
names or final Rust signatures:

| Responsibility | Contract |
| --- | --- |
| Execute a control query | Run a logical plan and return a bounded scalar result, such as active-vertex or remaining-edge count. |
| Materialize | Execute a relation and return a committed checkpoint handle with its schema. |
| Read checkpoint | Produce a relation over a live checkpoint; advertise only verified partitioning and ordering. |
| Release checkpoint | Release an ownership handle; delete storage after the last dependent operation or result releases it. |
| Cancel | Propagate query cancellation to active jobs, writers and the iteration loop. |

The Sail implementation plans these operations with the host session and submits
them through its existing
[`JobRunner`](../../../crates/sail-common-datafusion/src/session/job.rs).
Route all execution sites used by PageRank, Pregel, WCC and checkpoint handling
through the interface, including counts, final normalization, cache operations
and output writes. Replacing only `collect()` would leave bypasses.

At the pinned revision, the direct production execution calls are:

| File | Calls to redirect |
| --- | --- |
| `algorithm/pregel.rs` | One `count`, one `write_parquet` |
| `algorithm/centrality/pagerank.rs` | One `cache`, one `write_parquet` |
| `algorithm/connectivity/connected_components.rs` | Two `count`, one `write_parquet` |
| `memory/parquet_checkpointer.rs` | One `write_parquet` |
| `memory/hash_partitioned.rs` | One physical `execute` call, inside a partition loop |

These nine call sites exclude tests and Rust iterator `collect` calls. They are
an audit starting point, not an estimate of patch size: checkpoint reads/deletes,
metadata listing, session construction and indirect execution also need review.

Extract this interface before integrating algorithms into Sail. Request upstream
agreement, but do not make delivery depend on its timing: maintain the minimal
patch in a pinned graphframes-rs fork if necessary, preserve the standalone API,
and submit the patch for upstream review separately. If the interface itself is
rejected, keep the client route available. A wrapper around unchanged file-writing
`run` methods can demonstrate local execution, but does not meet the distributed
host-execution contract.

Preserve the host planner, function registry, object stores and session services.
The library's current
[`scoped_ctx`](https://github.com/SemyonSinchenko/graphframes-rs/blob/b4da56dabe20bba8e29563e06acc5179b2113ce3/src/utils/options.rs)
shares the runtime but constructs fresh session state. Sharing the runtime alone
is insufficient to preserve Sail's execution behavior.

## Sail integration and client contract

Reuse the Connect relation envelope with exactly two input relations and a
versioned algorithm/options payload. Add a host-side handler that retains resolved
logical inputs, before the existing native path lowers and gathers them.
Keep that handler separate from the physical-plan wheel interface.

The adapter should provide a lazy result relation: analysis validates options and
returns the schema without starting jobs or writing files. At execution, the
preparation controller described above produces a committed checkpoint, which becomes a normal scan usable by subsequent Sail operators.

Initial contract:

- Vertices have unique, non-null `Int64 id`; edges have `Int64 src` and `dst`
  referencing existing vertices. Preserve isolated vertices. Validate data at
  execution, not during schema analysis.
- PageRank returns `id, pagerank`; WCC returns `id, component`. Define duplicate
  edge and self-loop behavior explicitly and retain the library's documented
  options only where verified.
- Reject unsupported algorithms and options explicitly. Do not silently route
  them to the native Nutmeg backend.
- Expose iteration limits/deadlines and defined non-convergence behavior. Review
  reset probability, tolerance, personalization and normalization before claiming
  GraphFrames API equivalence.
- Start with integer IDs. String-ID support can later reuse the library's ingest
  mapping while preserving original IDs in results; it needs its own distributed
  execution and lifetime review.

## Memory and checkpoint ownership

Use Sail's existing runtime and memory pool for local execution; workers use
their configured process resources. Do not create a second independently budgeted
runtime for the graph library. Sharing a process alone does not prevent competing
memory pools from overcommitting it. DataFusion accounting also does not bound
all allocations or total RSS.

Assign each execution a unique storage namespace. Keep checkpoints and final
results alive through explicit ownership handles. Publish an iteration only after
all its writes succeed; failed attempts must not appear as committed input to the
next iteration. Cancellation must stop child jobs and clean abandoned files.
Final result files survive until their consumers release them.

Before the distributed stage, provision a shared object store accessible from
the driver and every worker, with consistent credentials and isolated execution
prefixes. One-host development can use local paths; independent host-local paths
do not satisfy this contract. Initially read plain Parquet checkpoints and let Sail repartition as necessary. Defer the library's
specialized hash-partitioned/sorted checkpoint optimization until its physical
properties, serialization and file ownership are proven across workers. Bound
checkpoint storage separately from operator spill storage.

## Implementation stages

| Stage | Changes | Completion condition |
| --- | --- | --- |
| 0. Prerequisites and reference | Implement a small client-loop reference; settle controller placement with the fake-controller gate; agree the library interface or pin its fork. | Materialization and semantics are explicit; controller children can progress with one worker slot and cancellation reaches them. |
| 1. Library execution interface | Extract execution and checkpoint operations; preserve standalone behavior and host session state. | The audited execution paths use the interface, including output and cleanup. |
| 2. Local integration | Pin dependencies; add the optional adapter and lazy request/result contract. | PageRank and WCC run over Sail input tables, compose with a subsequent query, and perform no work during analysis. |
| 3. Distributed stages | Submit algorithm jobs through Sail; implement shared-store checkpoint commit/lifetime and parent/child cancellation. | Iteration joins, aggregations and writes run on workers without gathering the graph to the driver. WCC scalar expressions round-trip through the plan codec and resolve in fresh worker sessions. |
| 4. Qualification and examples | Verify semantics, constrained resources and failure handling; add local and distributed examples. | Linux and two-host runs demonstrate correct results and actual algorithm-stage execution on both hosts. |

Keep changes reviewable in separate PRs: the library execution interface, Sail's
optional graph adapter, distributed orchestration/checkpoints, and client examples.
Prefer reusing existing Sail job and checkpoint services; identify missing
capabilities before proposing new generic infrastructure. WCC's `finite_axpb`
scalar expression must be registered in fresh worker sessions and survive the
plan codec; having its code in the binary is insufficient. PageRank and WCC use
builtin `sum` and `min` aggregates, so custom aggregate registration is not a
prerequisite. A later wheel API for other algorithms may need AggregateUDF
registration, serialization and compatibility work; that is outside this scope.

Validation should cover empty graphs, sinks, isolates, disconnected components,
loops, duplicate edges and invalid endpoints. Check PageRank numerical semantics
against an independently specified reference, and WCC membership plus the chosen
component-label convention. Include spill pressure, concurrent sessions,
cancellation, worker failure and partial checkpoint writes. Record non-convergence
and failures explicitly. Distributed floating-point reductions need justified
tolerances; do not assume bitwise equality across partition counts.

## Relationship to Nutmeg's direct DataFusion path

Both approaches keep graph data in tables and execute relational operations
through Sail's DataFusion engine. The proposed integration extends that approach
to iterative algorithms; it does not replace the existing graph-table helpers.

| | Existing Nutmeg graph-table path | Proposed graphframes-rs path |
| --- | --- | --- |
| Operations | Degrees, triplets and fixed-length walks | Initially PageRank and WCC |
| Implementation | Python helpers generate ordinary Spark Connect plans | A Rust library constructs DataFusion operations, executed through the Sail adapter |
| Iteration control | No convergence loop; bounded walks expand into a fixed sequence of joins | A controller submits successive jobs until convergence or an explicit limit |
| Intermediate state | Ordinary query execution | Explicit checkpoints between iterations |
| Sail integration | Uses existing relational execution | Requires host-controlled iteration, checkpoint ownership and cancellation |

The existing [Nutmeg graph-table helpers](../../../examples/extensions/nutmeg/python/sail_nutmeg/graph.py)
construct no CSR. Nutmeg's current `run(..., "pagerank")` and `run(..., "wcc")`
use its staged native graph path instead; those algorithms are not implemented
by the direct DataFusion helpers today.

This is initially a proposed compiled-in adapter, not a third independently
installed extension beside Sedona and Nutmeg. Separating its implementation does
not require a third user-facing graph API. One option is to expose graphframes-rs
behind Nutmeg's graph-table API with an explicit execution-backend choice,
retaining the distinction between relational algorithms and staged native
kernels. That API choice remains subject to review; it is not implemented and
must not silently change existing Nutmeg calls or imply identical algorithm
semantics. Independently installed plugin packaging is a later decision.

## Choosing a graph path and its semantics

Use relational helpers for degrees, triplets and bounded walks. Use relational
algorithms for iterative PageRank/WCC over tables, including distributed stages
and out-of-core state. Use native Nutmeg kernels when explicitly choosing a
staged graph and that backend's supported algorithms and resource constraints.
The client must identify the backend; there is no automatic fallback.

Shared algorithm names must document their semantics. Establish a common test
contract where parameters describe the same mathematics, including dangling
vertices, normalization, duplicates and component labels. Do not silently modify
one implementation to make unlike defaults appear equivalent. Initially expose
backend-specific behavior explicitly; claim API compatibility only for verified
option combinations. WCC's initial labeling policy is the minimum original
integer ID in each component. Distributed PageRank permits justified numerical
tolerances across partition counts. No cross-backend bitwise guarantee is made.

## Decisions requested in review

1. Is an execution/checkpoint interface acceptable upstream in graphframes-rs,
   with its standalone DataFusion implementation retained?
2. Is a compiled-in optional Sail adapter the right first integration boundary,
   before designing an independently installed plugin interface?
3. Which PageRank options and WCC labeling convention should the initial client
   guarantee? Should these be exposed through Nutmeg's graph-table API with an
   explicit backend, a library-native API, or a GraphFrames subset?
4. Can the first distributed implementation use ordinary Parquet checkpoints,
   deferring partition-preserving checkpoint optimization?
5. Is the proposed pre-submission preparation phase acceptable, including early
   operation registration, child-job tracking and checkpoint scan replacement?

The first deliverable is two executable examples over Sail tables—PageRank and
WCC—with the same request shape in local and distributed mode, accompanied by
short build/run instructions. Other algorithms remain explicitly unsupported
until their execution paths and semantics are reviewed.

## Review disposition

The [Opus review](graphframes-rs-plan-review.md) motivated the client-loop
comparison, prerequisite sequencing, named fork fallback and explicit controller
placement above. Its execution-site count is corrected by the source inventory
in this revision. A client loop is an alternative requiring validation, not a
zero-cost implementation of graphframes-rs. The review's original text is retained
unchanged; this amendment does not constitute implementation or runtime evidence.
