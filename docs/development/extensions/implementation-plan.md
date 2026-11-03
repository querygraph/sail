# Sedona and Nutmeg native extension proof of concept

The follow-up branch `work/extensions-datafusion-graphs` implements the
[graph-table and memory-admission plan](datafusion-graph-plan.md). It supersedes
this original plan's separate native/host budget design and adds the review's
geometry, projection-cache and wheel-packaging corrections.

## Objective and completion contract

Build two separately compiled native Python packages, discovered by one Sail
server on branch `work/extensions-sedona-nutmeg`. Exercise Apache SedonaDB's
actual spatial kernels through Spark function names and Nutmeg's actual Grust
algorithms through Spark Connect `Relation.extension`. A joint example must
stage a graph derived from spatial SQL and return composable algorithm results.
All claimed successes require a runnable test and a receipt at an exact commit.

The initial delivery was local-mode; the resumed goal adds distributed execution
under the contract at the end of this plan. Both include ordinary Sail spatial joins
using imported predicates. It does not claim the optimized Sedona SpatialJoinExec
hook, geometry UDT interchange, or a stable native ABI.
Those are distinct milestones below, not properties inferred from local success.

## Review baseline

Reviewed Grust `docs/proposals/sail-extension-api.md`, fifth revision, Grust
`7fc0514` (the requested proposal-v5). Sail implementation baseline is upstream
`a85d912d72ae03a6d97b6a3fd151f5752da636c6`. Nutmeg source baseline is
`f267b03659dd536981f98420944f911b667632b7`. SedonaDB source is pinned in the
extension package; its upstream DataFusion 54 / Arrow 58 closure needs a small
explicit port to the Sail build tuple, DataFusion 55.1.0 / Arrow 59.3.0.

### Findings that change the implementation

1. **A provider scan does not invoke insert_into.** Proposal §§7.2–7.3 omit
   staging execution. StageGraph has two inputs (§10.2), not one. Implement a
   lazy mutation provider which consumes both inputs only when executed, builds
   replacement state privately, then publishes atomically. Analysis is read-only.
2. **A bare factory function does not require global session state.** It can
   return a stateful factory; create receives session information. Bind a new
   extension object and new Nutmeg registry per session, with a host-generated
   incarnation. Only loaded code/module ownership may be process-wide.
3. **Preserve existing function precedence.** The catalog precedes Sail built-ins.
   Reject extension collisions after case folding, including aliases, before
   installing any components; do not change existing user-defined precedence.
4. **The relation handler is a new interface.** The local PoC uses Python method
   dispatch plus named DataFusion FFI capsules rather than declaring an untested
   C manifest layout stable. Verify ordinary Python metadata before dereferencing
   any capsule. Pin the exact DataFusion/Arrow tuple and reject mismatches.
5. **Only relations are needed.** Keep expression and command extension requests
   explicitly unsupported. Implement the envelope and input-free bare Any route.
6. **Preserve the host runtime.** Foreign parents must never run Sail children
   using the FFI-reconstructed default runtime. Capture the real Sail TaskContext
   and Tokio handle in a host adapter; execute all partitions through an explicit
   coalesce. Treat each returned native physical region as an opaque host leaf:
   inputs are optimized before export, and outer result composition remains
   optimizable. DataFusion 55.1 foreign child replacement loses host runtime
   metadata for newly inserted host nodes. Keep the extension's own graph/kernel
   memory budget distinct from Sail's budget.
7. **Column names are a host contract.** Restore visible names on inputs before
   handing them to a handler; use resolve_table_provider_with_rename for results.
8. **Local execution is not cluster evidence.** The initial implementation
   rejected cluster modes. The distributed milestone replaces that rejection
   with worker package identity checks, scalar codecs and driver-stage placement,
   and requires separate-process execution evidence.

## Implementation sequence and ownership

| Step | Files / behavior | Exit evidence |
| --- | --- | --- |
| 1. Reproducibility | Standalone packages under examples/extensions; source pins, licenses, port patches, lockfiles, build script | Separate wheels; no Sail crate linked into plugins |
| 2. Discovery | sail-session extension loader; pysail.extensions; version checks; collision checks; per-session bind | malformed metadata, version/name/type URL collision tests; no partial session installation |
| 3. Sedona scalars | Sedona functions + GEOS kernel registry exported as FFI_ScalarUDF capsules | constructors, text, distance/area, predicates, nulls, aliases via SQL and Connect |
| 4. Protocol | plain-data spec Extension; bounded SailExtensionRequest; exact URL registry; query-root inputs | unknown URL, unsupported envelope version/expressions, input arity, byte/depth limit tests |
| 5. Host input execution | ordinary input resolution and optimization; visible schema names; retained host TaskContext/Tokio handle; opaque returned native physical region | uneven four-partition inputs including empty partitions; runtime identity and optimizer-boundary tests |
| 6. Nutmeg state | vendored attributed core with session registry, atomic whole-graph replace, immutable read snapshot | two sessions use same name; invalid overwrite preserves prior graph; revision tests |
| 7. Nutmeg verbs | lazy stage, streaming run, lazy drop; client eagerly consumes mutation receipts | schema/explain do not mutate; counts, names, PageRank and exact component/degree results |
| 8. Joint proof | one Sail binary and both installed packages; spatial SQL creates graph inputs | expected rows through Spark Connect and downstream DataFrame composition |
| 9. Verification | focused Rust checks and end-to-end Python matrix on detached worktree with own target | named commit, exact commands/build identities, retained pass/refusal/error receipts |

## Package protocol (experimental API 1)

Each entry-point factory returns an object with `manifest()` and
`bind(session_incarnation)`. Metadata includes name/version, api_version,
datafusion_version, arrow_version, placement, and relation_types with type_url,
accepts_bare and input-count bounds. A bound instance exports scalar_udfs() and,
when claimed, plan_relation(type_url, payload_bytes, input_capsules).

Scalar objects use __datafusion_scalar_udf__ and capsule name
`datafusion_scalar_udf`; planned inputs use `datafusion_execution_plan`; returned
providers use `datafusion_table_provider`. Only DataFusion FFI objects cross
native library boundaries. Modules stay loaded while any native code can run.
Package trust remains the same as importing a native Python package.

The wire envelope is `type.googleapis.com/sail.extension.v1.SailExtensionRequest`:
fields payload_type_url=1, payload=2, repeated spark.connect.Plan inputs=3,
input_expressions=4 (rejected in this PoC), envelope_version=5 (must equal 1).
Nutmeg uses a versioned JSON payload inside its Any for this prototype; the
transport and field numbering remain compatible with a generated protobuf client.

Mutation helpers collect their receipts immediately. Reusing a single physical
mutation plan is guarded; re-planning a new request is a new operation. No retry
or exactly-once claim spans client reconnects. Reads bind to an immutable graph
revision. Drop removes the session's name without invalidating existing reads.

## Acceptance matrix

- Correct scalar outputs and spatial joins, with duplicates, nulls, empty input,
  reordered columns and residual predicates where the baseline supports them.
- Nutmeg stage has two inputs, consumes every partition, retains isolates and
  duplicate edges, and reports signed Spark-compatible counts and revision.
- Native algorithm output preserves declared column names and composes with
  select/filter/order/join; PageRank tolerances are stated and exact algorithms
  compare exact rows.
- Two sessions with the same graph name remain independent. Analysis has no
  effects. Failed stage is atomic. Repeated/drop requests have stated semantics.
- Dropping a result stream during bounded-output backpressure cancels its
  producer; already-held Arrow data remains valid until its consumer releases
  it. Fresh projection construction and synchronous mutation publication have
  the cancellation limits recorded in [the implementation review](implementation-review.md);
  this PoC does not claim a universal cancellation-latency bound.
- Missing/invalid package, build mismatch, duplicate name/type URL, unknown URL,
  malformed/oversized envelope, unsupported input mode, and cluster mode fail
  explicitly. Unsupported is not counted as a successful execution mode.
- Same-session spatial-to-graph example runs with both native packages installed.

## Later milestones required to qualify the entire proposal

**Optimized spatial join.** Add a Sail-owned physical rule at a fixed documented
position, with a narrow inner-join predicate grammar. Carry the actual selected
function identity, JoinFilter schema and side/index mapping, both child schemas,
residual condition, output schema, build side and input requirements. Refuse
unsupported joins without changing their meaning. Compare SpatialJoinExec against
the baseline over nulls, empty/unequal partitions, duplicate matches, reversed
arguments, projected columns and residuals. Qualify runtime and spill budgets.

**Distributed execution.** Add an owner-discriminated physical codec envelope,
worker package discovery, exact build/config descriptors, centralized placement
and stage boundaries. Preserve declared requirements after every child rewrite.
Nutmeg residency and stage retries need explicit operation/attempt identity,
commit/acknowledgement failure semantics and revision pinning. Run both local
cluster and genuinely separate worker processes; report these independently.

**Broader API.** Aggregate/window exports need resolver construction and modifier
semantics tests. Command results, expression handlers, generic catalogs/formats,
GeoParquet, geometry client UDT, configuration snapshots, object-store/runtime
bridges, ABI evolution and unload remain separately qualified work.

## Evidence policy

Gate a detached commit with its own CARGO_TARGET_DIR and CARGO_INCREMENTAL=0;
check disk before builds. A receipt records Sail SHA, extension source SHAs,
lockfile/wheel hashes, Python/client/toolchain versions, execution mode and actual
partition counts. Commit only after gates succeed in the same shell && chain.
This is a branch proof of concept, not a Grust crate release or benchmark.

## Distributed execution milestone

The user resumed the goal for both extensions in distributed execution. The
completion contract includes `local-cluster` and independently launched Sail
worker processes, with both native wheels installed. A scalar-only code path or
cluster admission without actual execution is insufficient evidence.

1. Sedona scalars use an owner/version-discriminated descriptor in Sail's physical
   codec. Identity hashes installed package files and canonical immutable metadata,
   in addition to validating the DataFusion/Arrow/API tuple. Workers discover their
   own wheels before decoding; aliases use the same case normalization as the
   driver. Missing packages and same-version different code must fail explicitly.
2. Nutmeg remains **driver-resident**, as proposal §§10 and 15 require. The host
   exposes gathered input children to the distributed stage planner and wraps the
   native region with a driver-placement adapter. Workers execute input queries;
   driver native kernels consume the prepared shuffle streams. Results pass through
   worker stages and can participate in further distributed queries.
3. Driver plan descriptors resolve only within the originating session and live
   query. They reference the original native plan, preserving the bound graph
   revision and mutation attempt through encode, decode and child replacement.
   Weak registry entries must not retain abandoned snapshots indefinitely.
4. Regions containing driver-native operations disable automatic retries. A lost
   acknowledgement is explicitly indeterminate. Fault injection must demonstrate
   one mutation and no replay, while an invalid overwrite preserves prior state.
5. Verify multi-partition and empty inputs, session isolation, ordinary spatial
   joins, spatial-to-graph composition, snapshot lifetime, early stop/cancellation,
   package mismatch and placement/codec invariants. Qualify worker-process evidence
   independently of the in-process local-cluster run. Preserve local regressions.

The process launcher is enabled with `SAIL_EXPERIMENTAL_PROCESS_WORKERS=1` in
`local-cluster` mode. This uses the same Sail worker CLI and RPC/task protocol as
external workers. Kubernetes orchestration requires a deployment environment and
is not inferred from a same-host multi-process test.

Plan recorded: 2026-09-26T00:09:26+00:00
