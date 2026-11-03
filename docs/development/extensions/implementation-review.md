# Implementation review and verification history

Reviewed at: 2026-09-26T00:52:49+00:00

The opening review and initial gate history describe the local delivery.
The distributed continuation is recorded separately below.

This is a source review of the current proof-of-concept working tree. It does
not identify an uncommitted tree as a tested commit. The implementation baseline
is Sail `a85d912d72ae03a6d97b6a3fd151f5752da636c6`; native dependency versions are
DataFusion 55.1.0, Arrow 59.3.0 and published Grust 0.23.0. The final gate receipt
must pin the resulting Sail commit and actual wheel/build identities separately.

Scope: the local relation protocol, host input adapter, Python capsule loader,
Nutmeg session registry, mutation executor, scoped changes to the vendored graph
core, and native provider/stream ownership. This is not a qualification of the
later distributed or optimized spatial-join designs.

## Findings and status

| Finding | Status in source | Evidence and required qualification |
| --- | --- | --- |
| A scan does not call `insert_into`; two graph inputs need an explicit execution path | Implemented | `MutationTable::scan` constructs a lazy `MutationExec`; `Mutation::run` consumes both inputs before atomic publication. Planning and scanning do not call the staging transaction. |
| Foreign execution reconstructs a default task context | Implemented for host inputs | `HostInputExec` captures the host context and ignores the incoming foreign context. It coalesces all partitions and validates replacement arity/schema. This does not supply Sail's runtime to the extension's own kernel. |
| A nested `Any` resets a naive protobuf recursion budget | Corrected; 10 focused development tests pass | The raw wire validator follows Sail envelopes inside `Any.value` iteratively before decoding any embedded `Plan`. Conversion uses stack growth; bounded generated decoding has its own known stack. Ordinary extension payload bytes remain opaque. |
| First-use output-schema discovery ran a kernel during user-plan analysis | Corrected; native development tests pass | `prepare_output_schemas` now runs during binding using a separate 16 MiB schema store. Session algorithms use cache-only schema lookup and refuse missing keys rather than probe during planning. |
| Empty inputs bypassed structural-column and mapping checks | Corrected; native development tests pass | `MutationTable::stage` validates each input schema using an empty batch before creating the mutation provider. Empty valid graphs remain supported. |
| Exported provider's task-context provider died when the constructor returned | Corrected; native fixture and real server matrix pass | `FFI_TableProvider` retains a weak task-context provider reference. Native `OwnedProvider` and `OwnedExec` wrappers retain a strong owner beyond the constructor. The constructor-scope-drop fixture and corrected real server matrix pass. Earlier combined runs exposed the separate host-runtime failure below. |
| Host optimization introduced unguarded nodes inside a foreign graph | Corrected; optimizer regression and 24-case combined development matrix pass | Capturing a handle inside `HostInputExec` alone passed three host tests but the real server still failed. The host inserted new repartition nodes above that wrapper and between native parents; FFI child replacement exported them without a host runtime. `NativeTableProvider` now returns an opaque `NativeRelationExec` leaf, preventing the outer host optimizer from entering the mixed-library region. |
| Known failed mutation and in-progress/indeterminate mutation share one state | Conservative limitation | The first error leaves `Attempt::Running`, so replay is refused. This prevents duplicate mutation but does not retain a distinct terminal failure reason. Do not describe every such refusal as evidence that a commit occurred. |

Source entry points:

- [Host registry and input wrapper](../../../crates/sail-common-datafusion/src/connect_extension.rs),
  [input resolver](../../../crates/sail-plan/src/resolver/query/extension.rs),
  [raw wire validator](../../../crates/sail-spark-connect/src/proto/extension/wire.rs).
- [Package loader](../../../crates/sail-session/src/extensions/mod.rs),
  [metadata validation](../../../crates/sail-session/src/extensions/manifest.rs),
  [native region boundary](../../../crates/sail-session/src/extensions/plan.rs).
- [Session registry and graph transaction](../../../examples/extensions/vendor/nutmeg-graph/src/session.rs),
  [schema setup](../../../examples/extensions/vendor/nutmeg-graph/src/prepared_schema.rs),
  [graph core](../../../examples/extensions/vendor/nutmeg-graph/src/lib.rs).
- [Native provider boundary](../../../examples/extensions/nutmeg/src/lib.rs),
  [retained context owner](../../../examples/extensions/nutmeg/src/context.rs),
  [mutation execution](../../../examples/extensions/nutmeg/src/mutation.rs),
  [native tests](../../../examples/extensions/nutmeg/src/tests.rs).

## Atomicity, snapshots and ownership

`GraphStaging::finish` prepares nodes and edges in a private `Entry`. Only after
both parts succeed does it take the graph-map write lock, read the previous
revision, increment once, and replace the map entry. A preparation error drops
the private entry and reservations without replacing the prior graph. Concurrent
whole-graph replacements therefore publish complete pairs in commit order;
there is no append contract in this prototype.

A session read clones the selected entry into a private snapshot store sharing
the session memory pool. Later overwrite or drop replaces/removes the public
name while the snapshot retains the old rows, revision and their reservations.
The native mutation provider retains a cloned `SessionRegistry`; algorithm
providers/plans retain the snapshot. Their state does not depend on the Python
bound object's continued existence.

The loader validates Python metadata before dereferencing a named native
capsule, clones the FFI object while the capsule is live, and retains package
code for the process lifetime. Scalar wrappers also retain the exporting Python
object and bound session. These checks assume a trusted native package honestly
implements its declared capsule layout; they are not a sandbox or a proof of
arbitrary pointer validity.

Grust 0.23.0's `arrow_output::finish` uses
`grust_arrow::retain_array_owner` to attach a memory-reservation owner to Arrow
buffers. Nutmeg's result batch cloning, schema conformance and projection share
those arrays, and Arrow FFI retains their release ownership. This supports
consumer-held batches outliving stream cancellation. A positive memory charge
while a batch alone remains held is stronger accounting evidence than merely
observing zero after everything is dropped.

## Local physical optimization boundary

The observed pre-fix Spark Connect explain plan (`/tmp/sail-extension-pre-fix-plan.log`)
contained `FFI NutmegContextOwnerExec -> host RepartitionExec(4) -> FFI NutmegMutationExec`
and, for each input, `host RepartitionExec(4) -> HostInputExec(1) -> CoalescePartitionsExec`.
The first panic stack identified the host repartition stream spawning a task on a
plugin worker without the host Tokio reactor. This directly demonstrates why a
runtime guard only inside the original input wrapper was insufficient.

The local PoC therefore treats the physical region returned by a native provider
as an opaque host leaf. Input plans have already been resolved, named and
optimized by Sail before export. The package must satisfy its own internal input
requirements; Nutmeg consumes all input partitions explicitly. The outer host
optimizer can still optimize operators composed above the native result, but
cannot insert or replace nodes inside that native region. The wrapper preserves
the native output schema and plan properties and retains the provider owner.

This is an explicit qualification boundary relative to the full proposal:
cross-boundary physical rewrites, internal host plan visualization/telemetry and
distributed codecs are not implemented. They require a stronger runtime and
physical-requirements contract, rather than inferring those properties from the
DataFusion 55.1 capsule interface.

## Cancellation and resource boundaries

The bounded-output test has a non-timing-based reason to reach backpressure:
five output batches cannot all complete after one consumer batch when the
channel holds only two. Dropping that stream cancels its child query, closes
the channel and allows the producer to exit. The watchdog tests eventual exit;
it does not establish a universal cancellation-latency bound.

Fresh projection construction is a separate limitation. `Store::run_kernel`
checks the child query before calling `Store::projection`, but the projection
is constructed using the pool context. Cancelling the child or exhausting its
per-read deadline/work budget does not interrupt that build. The cancellation
signal is observed again when subsequent query work runs. This inherited path
must not be represented as bounded cancellation throughout projection building.

Mutation finalization also has a cancellation boundary: after the last input
await, canonicalization and publication are synchronous. An interrupt can race
that region and its acknowledgement. A cancelled/unacknowledged stage must be
treated as potentially committed; callers should inspect state or issue an
explicit new operation. The local physical-plan attempt guard does not provide
an operation identity or exactly-once behavior across newly planned requests,
client reconnects or process restarts.

The session graph budget accounts retained staged rows, canonical copies,
projections and native kernel reservations. It is not an end-to-end RSS cap.
Incoming Sail batches/transfer buffers belong to the host boundary, and inherited
normalization calls check availability before casting but admit the resulting
retained buffers after normalization. Some schema-unification/sort-key sizes are
also measured after allocation. These boundaries do not invalidate the retained
buffer ownership argument, but they prevent claiming every temporary allocation
is reserved before allocation or that the graph budget alone bounds total RSS.

## Development evidence versus final gates

The initial focused development run passed the two host registry/input tests and
two loader metadata tests. Eight protocol tests passed before the nested-`Any`
test aborted with a stack overflow. That failure was retained and prompted the
raw global traversal/stack-growth correction. The corrected focused run
(`cargo test -p sail-spark-connect proto::extension::tests --lib`) passed all
10 tests, including 1,000 nested envelopes and accepted near-limit nesting on
a 128 KiB initial stack. Its development log is
`/tmp/sail-extension-protocol-fixed-tests.log`. A stopped or aborted suite is not
a passing gate; the corrected development run still does not name a final commit.

The first real combined server test also exposed the weak task-context-provider
lifetime error. The same-library capsule test had kept its local strong owner
alive and therefore did not establish constructor-scope lifetime safety. The
separate-library test is necessary evidence, not a redundant smoke test.

After the native owner correction, five native tests, 44 vendored graph tests
and three Python client tests passed in development. The subsequent combined
server run reached host input execution, then aborted while polling an FFI
stream without a host Tokio reactor. DataFusion 55.1 foreign child replacement
can re-export host children without the handle supplied on their initial
export. Capturing the runtime inside `HostInputExec` protects that node independently
of FFI wrapper metadata, but the next combined run still failed on newly
inserted host repartition nodes outside its guard. The corrected focused host run
(`cargo test -p sail-common-datafusion connect_extension::tests --lib`) passed
all three tests; its log is `/tmp/sail-extension-host-runtime-tests.log`.
The native region regression uses the real `ForeignExecutionPlan` adapter and
`EnsureRequirements` optimizer. Its exposed control acquires host repartitioning;
the opaque version retains the output properties, blocks internal insertion and
executes the expected rows. All three session extension tests passed in the
corrected development run (`/tmp/sail-extension-native-region-tests-3.log`).
The initial fixture stayed below the optimizer's statistics threshold and failed
the control assertion; reducing its batch-size threshold made the fixture reach
the required path without weakening the assertion. Both failed combined runs
remain development evidence; the initial three host tests were insufficient
to qualify the mixed-library optimizer path.

The corrected native region then passed the real spatial-to-graph joint test and
Nutmeg stage, PageRank, degree, WCC, composition and drop. The first complete
re-test stopped on a speculative missing-graph diagnostic regex after the
operation correctly refused the missing graph. The fixture now asserts the
actual graph-specific diagnostic. The next combined Spark Connect matrix
passed all 24 cases with both separately built native packages installed
(`/tmp/sail-extension-e2e-combined-6.log`). This is development evidence against
the working tree; it is not the final detached-commit gate.

Final acceptance requires a detached checkout at a named commit with its own
target directory, the corrected Rust protocol/ownership/schema tests, independently
built native wheels, and the combined Spark Connect test on the actual server.
The receipt must retain failures and their corrections, distinguish development
checks from the detached verdict, and state the exact limits above. Local-cluster,
separate-process workers and optimized Sedona joins remain unqualified here.

## First detached candidate

Recorded at: 2026-09-26T01:07:42+00:00

Candidate `003b8bd1e4572244feb7a920e3798480301d61d4` built both wheels
and the host, then passed 16 host extension tests, clippy, four Sedona
native tests and five Nutmeg native tests. Its gate stopped on rustfmt
for the new `prepared_schema.rs` helper; it was not promoted to the branch
and is not a passing verdict. The correction only expands one error macro
to rustfmt's required layout. Formatting checks now precede test execution,
and the three host packages run the same extension modules in one Cargo
invocation. The failed log is `/tmp/sail-extension-detached-gate-003b8bd1.log`.
The final receipt must come from a complete gate at the corrected commit.

## Second detached candidate

Recorded at: 2026-09-26T01:24:27+00:00

Candidate `abb2cd6d61dc861a38a2ffbed807d1589cf0bf0e` built both wheels
and the host, passed formatting, 16 host tests, clippy, four Sedona native
tests, five Nutmeg native tests, 44 graph-core tests, 11 release cancellation
runs (one idle and ten with every logical CPU saturated), and three Python
client tests. Its end-to-end suite finished with 23 passes and one failure:
PySpark 4.0.1 raised `cannot schedule new futures after shutdown` during
release of a query response. This candidate was not promoted. Its full log
is `/tmp/sail-extension-detached-gate-abb2cd6d.log`.

A deterministic client-only control reproduced the failure: cyclic collection
of a stale `SparkSession` calls `client.close()` a second time, shutting down
PySpark's class-global response-release executor while a different session
uses it. The same frozen binary and environment passed all 24 cases with
cyclic GC disabled (`/tmp/sail-extension-detached-gc-control.log`); the failing
test also passed alone. Those controls identify a harness/client lifecycle
problem, not a graph-state mismatch. The graph-state assertion remains intact.
The correction retains test sessions until suite teardown, allowing explicit
`stop()` and ordinary Connect reattachment throughout the tests while preventing
stale destructors from closing an active session's executor. A deterministic
forced-GC regression accompanies the fixture. A new complete detached gate is
required after this correction.

The retention fixture passed its focused forced-GC check and the complete
25-case development matrix (the original 24 plus that regression), with
normal garbage collection and Connect reattachment enabled. Logs are
`/tmp/sail-extension-lifecycle-retention-focused.log` and
`/tmp/sail-extension-lifecycle-retention-matrix.log`. These remain development
controls until the new detached gate finishes.

## Distributed scalar milestone

Commit `c3766a47b` added scalar descriptors and worker loading, but had no actual
worker-execution test. A test invocation filtering on `native_scalar` ran zero
tests. The existing 16 extension tests did not exercise that codec. Its package
identity contained only name/version despite a comment claiming content and
configuration hashes; worker aliases also lacked the driver's normalization.
It continued rejecting Nutmeg in cluster mode, so it did not complete the resumed
goal. The commit was pushed without a detached-commit gate and is retained as
incomplete development history, not a distributed qualification verdict.

The proposal explicitly places Nutmeg on the driver. Distributed execution needs
worker-produced input stages and driver-resident graph operations, not migration
of graph state to workers. The current implementation follows that contract:
declared inputs are visible to Sail's stage planner while the native region stays
opaque to outer physical optimization. Session-scoped descriptors retain the same
native snapshot/attempt through task decoding; task-local shuffle children replace
typed placeholders only at execution. Regions containing native driver operations
do not retry. Actual in-process cluster and separate-process worker evidence is
required before this continuation can be considered complete.

## Distributed implementation and development checks

Recorded at: 2026-09-26T02:42:21.705Z

The distributed path now loads immutable scalar packages independently on workers.
Descriptors include a SHA-256 of installed package files and canonical manifest
metadata. Same-version different content is incompatible. Aliases are normalized
identically on the driver and workers. Native geometry expressions use a focused
physical-expression codec that preserves the complete Arrow return field.
Metadata-bearing literals have their own codec, because constant folding can
turn a geometry function into a literal before stage serialization.

Nutmeg providers are planned once. A session-owned driver binding freezes the
native plan and its read snapshot or mutation state. Task decoding resolves that
binding rather than planning a new request. Unique typed placeholders are replaced
with prepared host shuffle inputs at execution, under the real task/runtime
context. Placement is classified centrally for both stage barriers and preserved
roots. Native regions permit one attempt even when ordinary tasks allow three.
Query cleanup releases native plans/providers retained by archived job graphs;
already materialized streams retain their own ownership until dropped.

Development checks passed 52 common-DataFusion, 121 execution and 28 session
tests, including a commit-then-error fixture which exhausts the native region
after one attempt while its ordinary-task control remains retryable. This is
scheduler fault injection, not a claim of a real network partition after a
Nutmeg commit. The separate-process integration suite passed 29 tests, including
two distinct worker PIDs, independently loaded Sedona content identities, absent
and same-version changed worker packages, driver placement with eight worker input
partitions, joint spatial-to-graph execution, isolation and early result limits.
These development runs do not certify a final commit.

Retained development failures: the first cluster Sedona query lost geometry
metadata in the standard scalar-expression codec; the first process shuffle
fixture then exposed metadata loss for constant-folded literals. Both required
the codecs above. Rust test compilation also exposed incorrect fixture helper
names and readonly options construction; corrected fixtures compile and pass.
The first complete Rust run passed common-DataFusion then failed to locate
libpython; the control with the verification script's library path passed.
System-table counters required explicit signed casts for PySpark, and the WKT
fixture now compares exact coordinates rather than an assumed spacing style.
Logs are retained with the delivery artifacts.

The final gate must rerun the local, in-process cluster and separate-process
matrix at an immutable candidate. Separate processes run on this macOS host;
multi-host networking and Kubernetes deployment remain unqualified. Nutmeg graph
residency stays on the driver, and optimized Sedona joins remain a separate goal.

### First distributed candidate

Recorded at: 2026-09-26T02:46:10.267Z

Candidate `691e721521aeb5f9000e97febc38f69bf1b63626` failed detached
clippy because the new metadata round-trip test used `unwrap()`, forbidden by
the repository lint. The test now returns an explicit error if the decoded
expression is not a literal; its value and metadata assertions are unchanged.
The failed candidate is not promoted. Log:
`/tmp/sail-distributed-gate-clippy-691e72152.log`.
