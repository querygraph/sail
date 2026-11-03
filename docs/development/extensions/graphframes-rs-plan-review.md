# Review of the graphframes-rs integration plan

Reviewed [`graphframes-rs-plan.md`](graphframes-rs-plan.md) at `9cb9627fa`,
against graphframes-rs pinned at `b4da56da` (read from source) and this branch
at `088a1d540`. Nothing here was built or run; this is a review of the proposal.

## Verdict

The diagnosis is right and the sequencing is right. Two things need to change
before work starts: the plan must argue against the alternative that already
works, and it mis-ranks its own critical path.

## What it gets right

**The central diagnosis.** "Simply wrapping graphframes-rs in that interface
would not distribute its iterations" is exactly the problem. The existing native
relation handler returns an opaque provider inside a driver-placed region, so a
library whose entire value is that its plans are ordinary relational plans would
be executed in the one place that throws that value away.

**Compiled-in first.** Starting with an optional `sail-graphframes` adapter
rather than inventing a plan-contributing plugin ABI is the correct order. A
`LogicalPlan` does not cross `datafusion-ffi`, so the alternative is a new ABI
designed before anyone knows what the execution interface needs to be.

**Session state, not just the runtime.** Catching that the library's `scoped_ctx`
shares a runtime while constructing fresh session state — and that this is
insufficient — is the same class of defect as substituting a default
`RuntimeEnv` for a foreign plan, and it would otherwise have been found late.

**Lazy results and explicit refusals.** Analysis validates options and returns a
schema without starting jobs; unsupported algorithms are rejected rather than
silently routed to the native backend. Both are the right defaults, and the
second avoids a trap: two graph backends answering the same verb differently.

**Honest about floating point.** "Do not assume bitwise equality across
partition counts" is correct for distributed reductions, and it is the first
statement in this project that a graph result is *not* bit-reproducible.

## The plan does not evaluate the alternative that works today

A client-side Pregel loop — each iteration a join and an aggregation issued as
ordinary DataFrame operations over Spark Connect — distributes across Sail
workers **now**, with no Sail change, no adapter and no upstream change to
graphframes-rs. It is what GraphFrames itself does on Spark, and this branch
already demonstrates the shape with its relational graph-table helpers.

Server-side orchestration may still be the better destination: the loop stays
next to the data, the result composes as a relation in a larger query, it is
reachable from SQL, and there is no per-iteration client round trip. Those are
real advantages. But the plan asserts the destination without comparing it to
the thing that costs nothing, and a reviewer will ask. State the comparison,
including the two places where the server-side choice is *worse*:

- **Observability.** A client loop makes each iteration a visible job. Server-side,
  one Connect request hides N jobs; progress, cancellation and failure attribution
  all become the adapter's problem rather than the platform's.
- **Blast radius.** A client loop needs nothing from Sail. The server-side route
  needs a job-submitting controller, checkpoint lifetime, worker function
  registration and codec support — the plan's own stage 3.

If the client route is cheap enough to try first, it also produces the strongest
possible argument for the server-side one: a working distributed consumer.

## The critical path is mis-ranked

Stage 2 — extracting an execution interface in graphframes-rs — is listed as
"can be developed together with stage 1". It is the precondition for everything.
The library's API is side-effecting, not plan-producing: `PageRankBuilder::run`
returns `Result<usize>`, an iteration count, and writes its results with
`write_parquet`; the same is true of WCC. Inverting that — returning relations
and letting the host own materialization — is the integration.

Two consequences the plan should state:

1. **It depends on an external maintainer accepting an API inversion.** Decision 1
   asks whether the interface is acceptable upstream. There is no stated fallback
   if the answer is no or slow. Name one: a fork, a coarser adapter that accepts
   the file-writing behaviour and treats checkpoints as host-owned storage, or a
   vendored copy with the interface applied locally.
2. **The execution sites are few and countable**, which is good news worth
   putting in the plan: eleven `collect`/`write_parquet`/`cache` sites across
   Pregel, PageRank and WCC. That is the size of the change, and it makes the
   "replacing only `collect()` would leave bypasses" warning concrete.

## The controller deadlock is named but not solved

"The controller must not hold a worker task slot while waiting for jobs that
require those workers" is the single most likely way stage 3 fails, and the plan
defers it: "determine the precise execution-preparation hook in the first
implementation stage." That is the wrong stage to discover it in. The question —
where the controller runs, and whether it occupies a slot in the pool whose jobs
it is waiting on — should be answered before stage 1 is written, because the
answer may change the adapter's shape.

## Verified details that sharpen the plan

- **WCC needs no custom aggregate.** Its contraction uses DataFusion's builtin
  `min`; PageRank uses builtin `sum`. The custom expressions in the library
  (`hll`, `linalg`, `kmeans_step`) belong to other algorithms.
- **The aggregate gap therefore does not bite yet, but it exists.** This branch's
  package loader registers `ScalarUDF` only — there is no `AggregateUDF` path — so
  a *wheel-packaged* graphframes-rs would hit a wall the moment an algorithm
  needs a custom aggregate. Compiled-in registration sidesteps it for now, which
  is one more argument for the chosen order, and mirroring the scalar path with
  `FFI_AggregateUDF` is the small change that removes it later.
- **Worker-side resolution is the real function problem**, not availability. With
  a compiled-in adapter the code is in the worker binary, but the function must
  be registered in the worker session and survive the plan codec. The plan lists
  this in stage 3; it deserves its own completion condition.
- **Two hosts means shared object storage.** "Storage accessible to all workers"
  is true but understated: the one-host modes can use local paths and the
  two-host run cannot. Make an object store a stated precondition of stage 3
  rather than a discovery during it.

## Two policy questions the plan should answer, not leave implicit

**Two PageRanks in one product.** The native path returns bit-identical scores at
any worker count; this path will not. They may also differ on dangling-mass
handling, normalization and the WCC component-label convention. A user will
compare them and file a bug. Decide now whether the two backends must agree on
semantics — and document where they cannot, as the plan already does for floats.

**Three graph paths.** After this, a user choosing how to compute a graph result
has relational helpers, relational algorithms and native CSR kernels. The plan
should say in one paragraph which to reach for, or the choice becomes folklore.

## Recommendation

Adopt the plan with three amendments: add the comparison against the client-side
loop and say why the server-side route earns its cost; promote the library
execution interface to the critical path with a named fallback if upstream
declines; and answer the controller-placement question before stage 1 rather than
inside it. The staged structure, the completion conditions and the validation
list are otherwise sound, and the first deliverable — two algorithms, the same
request shape locally and distributed, with build and run instructions — is the
right thing to aim at.
