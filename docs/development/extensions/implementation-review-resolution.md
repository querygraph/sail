# Extension implementation review and resolution

Original review: `2026-09-26T04:11:42.485Z`, branch
`work/extensions-sedona-nutmeg`, commit
`18d7f6aa019846e01a10a6e426a76bc4e7e0b347` in `querygraph/sail`.
Follow-up branch: `work/extensions-datafusion-graphs`.

This record preserves the review's findings and maps them to the follow-up
implementation. A correction being implemented is not a platform gate verdict.
The [follow-up plan](datafusion-graph-plan.md) owns final candidate receipts,
platform envelopes and delivery outcomes. The earlier
[implementation review](implementation-review.md) remains a historical record.

The central architectural finding remains: DataFusion can execute relational
graph queries directly inside Sail. Nutmeg's native kernels already run inside
the Sail driver process; CSR is their adjacency representation, not an external
service. The defects concerned resource admission, duplicated projections,
expression metadata and deployment/lifecycle qualification.

## Findings and corrections

### R1 — P1: distributed WKB composition lost geometry metadata

At the reviewed commit, dynamic
`ST_AsText(ST_GeomFromWKB(ST_AsBinary(ST_Point(CAST(id AS DOUBLE), 2.0))))`
returned points locally but failed in separate-process cluster mode with
`st_astext(binary): No kernel matching arguments`. The native expression codec
preserved complete return fields for imported `OwnedScalar` functions but omitted
the retained Sail builtin `ST_GeomFromWKB`.

The [native expression codec](../../../crates/sail-execution/src/proto/native_expr.rs)
now preserves metadata-bearing scalar return fields. The
[composition regressions](../../../examples/extensions/tests/test_geometry_composition.py)
exercise dynamic WKB and geography conversion before and after shuffles. Final
qualification requires their passing results in each claimed execution mode.

### R2 — P1: graph normalization allocated before admission

The original staging path checked input bytes without reserving them, converted
the arrays, and only then admitted their retained output. One million unique
UInt32 node IDs with no label/properties reproduced the expansion:

| Original probe measurement | Bytes |
| --- | ---: |
| Budget checked | 5,242,880 |
| Input passing the check | 4,000,000 |
| Normalized Arrow buffers | 16,587,072 |
| Live Rust allocation requests above the input baseline | 16,587,980 |
| Peak Rust allocation requests above the input baseline | 32,587,352 |

This was an isolated reproduction of the Arrow 59.3.0 normalization calls, not
RSS or a full server staging measurement. Source ordering established that the
allocations preceded admission. Canonicalization also contained post-allocation
admission paths; the original probe did not measure their peaks separately.

[Staging and canonicalization](../../../examples/extensions/vendor/nutmeg-graph/src/lib.rs)
now reserve [conservative bounds](../../../examples/extensions/vendor/nutmeg-graph/src/admission.rs)
before conversion, schema filling, sort-key encoding and sorted copies. Bounds
include builder growth, empty casts, nested encoded conversions and repeated
view values whose logical expansion exceeds their physical buffers. Excess is
returned after retained buffers are measured. Unknown unsupported structural
casts fail rather than relying on an arbitrary fixed string-width assumption.

The [isolated allocator regression](../../../examples/extensions/vendor/nutmeg-graph/tests/admission_before_allocation.rs)
checks that the rejected million-row input allocates less than 256 KiB of
temporary metadata/error overhead, and that accepted normalization plus
canonicalization stay below their admission peak. The core suite also retains
atomic refusal, streamed-write and concurrent-reader accounting assertions.
This establishes the tested buffer/workspace bounds, not a cap on every Rust
allocation or total process RSS.

### R3 — P2: independent readers duplicated CSR projections

Each original algorithm provider cloned an entry into a private store. Arrow
payloads remained shared, but CSR built afterward was cached only in that private
entry. Later independently planned readers rebuilt topology and repeated work.
This finding followed from source ownership, not a timing comparison.

[SessionRegistry::algorithm](../../../examples/extensions/vendor/nutmeg-graph/src/session.rs)
now shares the published entry and its synchronized projection cache. Overwrite
replaces the entry; already-planned readers retain the old entry. The cache key is
the shared entry plus projection options. Displayed name/revision strings alone
are not unique identity: revision numbering restarts after drop/recreate.

[Graph table tests](../../../examples/extensions/vendor/nutmeg-graph/src/graph_tables/tests.rs)
verify concurrent independently planned readers share one cached projection,
old snapshots survive overwrite/drop, and admission/host leases remain held until
the final consumer buffer drops. There is no automatic cache eviction policy.

### R4 — P2: ordinary geometry expressions lost metadata locally too

At the reviewed commit, `CASE`, `coalesce` and `element_at(array(...), 1)` feeding
`ST_AsText` failed as binary in both local and separate-process modes. Direct
native nesting and `first(geometry)` controls passed. R1 alone could not fix this.

[Logical expression propagation](../../../crates/sail-plan/src/resolver/expression/geometry.rs),
[shared geometry field handling](../../../crates/sail-common-datafusion/src/geometry.rs)
and array handling now preserve compatible geometry fields. The composition
suite covers nulls, array indexing and shuffles, and rejects incompatible binary
or geometry/geography mixtures. This is scoped expression support, not a claim
that every future expression automatically propagates extension metadata.

Candidate `d7143e099` failed the full host gate on macOS and Linux because
DataFusion's scalar-list conversion dropped the list item's metadata. A separate
host test and live-server control also exposed `coalesce(geometry, NULL)` failing
Binary/Null coercion. These failures are retained in the candidate's gate logs;
they are not successful platform verdicts. The correction restores the actual
list descriptor after scalar conversion and types untyped NULL operands only
after establishing compatible geometry metadata. The original failing assertions
remain, with scalar broadcast, array input and live literal/dynamic regressions.
Ordinary binary coercion is outside this geometry-specific correction.

### R5 — P2: the delivered macOS wheel omitted GEOS

The original README claimed bundling, but the inspected wheel contained no GEOS
dylib and linked `/opt/homebrew/opt/geos/lib/libgeos_c.1.dylib`. Installed-wheel
identity therefore omitted that dependency's bytes. Same-host success did not
establish portable deployment or equal native dependency closure across hosts.

The [build](../../../examples/extensions/scripts/build.sh) now explicitly repairs
macOS wheels with delocate, retains Linux repair, and runs a
[wheel dependency check](../../../examples/extensions/scripts/check_wheel.py)
before installation. Bundled wheel members participate in package content
identity. Packaging qualification still belongs to the actual repaired artifacts
and platform receipts; source inspection alone cannot establish portability.

### R6 — P2: lifecycle and resource acceptance was incomplete

The earlier foreign-plan cancellation test meaningfully observed termination and
retained-output ownership. Its saturated release repetitions remain evidence
for that fixture. The old Spark LIMIT check only established returned rows and
continued usability. The scheduler mutation-counter fixture did not execute a
real Nutmeg transaction or lose a real network acknowledgement.

The follow-up adds these explicit checks:

- [Spark resource tests](../../../examples/extensions/tests/test_resources.py)
  inspect native read states after LIMIT, iterator close and tagged interruption;
  observe final host-lease release after session stop, idle expiry and graceful
  driver shutdown; and test competing session quotas.
- A real graph is published before a downstream expression fails while consuming
  its receipt. The same suite checks revision one and no automatic driver-region
  retry. This is post-publication receipt failure, not injected network packet
  loss or a cross-request exactly-once guarantee.
- [Foreign-input resource tests](../../../crates/sail-session/src/extensions/resource_tests.rs)
  exercise actual foreign-plan adaptation while preserving the host input's
  memory pressure and disabled/exhausted spill policies. They do not add spilling
  for native graph allocations.
- The [native FFI tests](../../../examples/extensions/nutmeg/src/tests.rs) and
  graph-buffer lifetime tests retain output while upstream owners disappear.

These checks exposed three further lifetime defects. Spark interruption removed
the operation identity, so a reattaching client received `operation not found`.
The [executor](../../../crates/sail-spark-connect/src/executor.rs) now retains a
terminal interruption without retaining its stream or buffers, and a concurrent
pause cannot restore the cancelled context. The
[executor tests](../../../crates/sail-spark-connect/src/executor/tests.rs) hold the
exact pause state through scheduler polling, without a timing window. Explicit
[session cleanup](../../../crates/sail-common-datafusion/src/session/lifecycle.rs)
also drains protocol-owned executors on deletion, expiry and shutdown and rejects
plans that finish after teardown.

Next, releasing the last `Py<BoundExtension>` outside the GIL deferred its decref
until another Python entry. The controlled development probe
`target/extensions-datafusion-development/probe_deferred_owner.py` stopped a
running APSP query and observed its 128 MiB host reservation still held; the next
session's first request released that reservation before admitting its own.
`probe-deferred-owner.log` and `probe-deferred-owner-audit.jsonl` retain both
observations. The [Python owner](../../../crates/sail-session/src/extensions/python_owner.rs)
now acquires the GIL for final destruction; its unit test drops the last owner on
a separate thread and requires release without another Python request.

Finally, graceful server shutdown could finish before the detached native
producer released its last buffer. The
[session-local native resource tracker](../../../crates/sail-common-datafusion/src/native_resource.rs)
owns no leases and waits for their final release after session contexts are
cleared. The [Spark server](../../../crates/sail-spark-connect/src/entrypoint.rs)
stops session resources before gRPC drains active responses. Teardown tests first
observe a running APSP kernel that has emitted batches, then require exactly one
durable last-owner release with host pool reservation zero. Session stop and
expiry additionally require a fresh session to reacquire the quota. A stop
acknowledgement or client connection failure alone does not satisfy these checks.
Deletion and expiry cleanup also use a
[dedicated task set](../../../crates/sail-session/src/session_manager/cleanup.rs)
that shutdown drains even after the session is marked deleted. Its deterministic
test holds the last native owner on a separate thread and establishes that
shutdown remains pending until both release and cleanup complete; the combined
session-stop-then-shutdown integration case is supplementary coverage, not the
proof that a race window was held.

Development runs in `resources-local-7.log`, `resources-actor-2.log` and
`resources-process-2.log` each passed all seven resource tests in local, actor-cluster
and separate-process cluster modes respectively; these logs are under
`target/extensions-datafusion-development`. Exact immutable
candidate outcomes must still come from the gate receipts. Query interruption
still cannot preempt synchronous initial CSR
construction or staging's final canonical sort. Abrupt process death,
arbitrary network failures and all possible cancellation interleavings are not
qualified by graceful teardown or one controlled receipt failure.

## Direct graph relations and native boundaries

The original review ran the delivered binary with two separate worker processes
and `SAIL_EXPERIMENTAL_EXTENSIONS=0`. On nodes 0–3 and edges `(0,1), (0,1),
(1,2), (2,0)`, normal SQL returned out-degrees `2,1,1,0`, two-hop multiplicities
`(0,2):2`, `(1,0):1`, `(2,1):2`, and six closed three-hop walks. EXPLAIN showed
joins, aggregations and repartitioning. This was small-fixture functional evidence,
not a performance benchmark or a count of deduplicated triangles.

The new [GraphTables client](../../../examples/extensions/nutmeg/python/sail_nutmeg/graph.py)
packages those relational patterns as ordinary Spark Connect plans:

```python
from sail_nutmeg import Nutmeg

nm = Nutmeg(spark)
graph = nm.tables("nodes", "edges").validate()
graph.degrees().show()
graph.triplets().show()
graph.walks(2).show()  # one row per walk; duplicate edges retain multiplicity

nm.stage("g", graph.nodes, graph.edges)  # explicit driver-resident capture
nm.nodes("g").show()                   # Arrow relation scan; no CSR
nm.run("g", "pagerank").show()         # explicit native CSR kernel
```

`GraphTables` preserves ordinary field/ID types and lazy table consistency.
Native staged scans expose normalized rows, pin at provider resolution, and
start with one driver partition. Two separately resolved scans do not promise
a common revision across concurrent overwrite; the Rust snapshot API can pin
both together. Their downstream relational operators may execute on workers.
Native staged state and kernels themselves remain on the driver.

Finite joins, filters, properties and motifs fit existing DataFusion operators.
Distributed iterative algorithms still need partitioned state, round exchanges,
convergence, recovery and reduction semantics. Sail's recursive CTE resolver
remains unimplemented. Nothing here turns native PageRank/WCC into distributed
iterative execution or silently replaces every native algorithm with joins.

## What the memory bridge guarantees

The original independent 256 MiB native session pool had no shared Sail parent.
The follow-up [resource lease](../../../crates/sail-common-datafusion/src/native_resource.rs)
reserves the configured native cap up front from the host's actual DataFusion
pool. Grust subdivides that prepaid quota. A versioned callback ABI carries an
opaque lease, not a Rust trait-object layout across separately compiled libraries.
Plans, producers, snapshots and exported Arrow buffers retain ownership.

Matching enabled pool configurations share process admission; different worker
processes have their own pools. A finite configured pool enforces participating
reservations; an unbounded pool stays unbounded. Native quotas are non-spillable,
reserve idle capacity and have no dynamic lending. Graph rows, CSR, kernel scratch
and output can coexist. The fixed schema-probe store, runtime/transport overhead,
Rust metadata and nonparticipating allocations need separate headroom.

Sem's ownership concern is valid as an integration concern, but does not imply
an out-of-process Nutmeg service. Rust/Arrow ownership and FFI release callbacks
handle shared buffers explicitly. Likewise, sharing a JVM heap does not place
all user structures inside Spark's managed execution/storage region; see
[Spark memory management](https://spark.apache.org/docs/4.0.1/tuning.html#memory-management-overview).
Neither model makes admission or spilling automatic for every extension object.

## Evidence provenance and remaining acceptance

The original full review and probes remain in the ignored local artifact tree
`target/extensions-distributed-poc/review/`: `REVIEW.md`,
`normalization-allocation.{md,rs}`, `wkb-observed.txt`,
`sail-ultra-geometry-review.jsonl`, `sedona-native-dependencies.txt`, and
`direct_graph_sql.json`. These are local evidence paths, not files distributed
with this documentation. This tracked record preserves their findings and scope.

The prior detached gate at the reviewed commit reported 235 host tests, clippy,
4 Sedona native tests, 5 Nutmeg native tests, 44 graph-core tests, 3 Python client
tests and 11 release cancellation runs. Integration outcomes were local 28 passed
plus one cluster-only skip, actor cluster 29 passed, and process cluster 29 passed
on macOS arm64. Those results remain valid only for their tested commit and paths;
they did not cover the counterexamples above.

During follow-up development, the final vendor graph source passed 55 unit tests
and one isolated allocator test, with formatting/diff checks passing. Its local
log is `target/extensions-datafusion-development/graph-core.log`. This was a
development checkout run, not an immutable candidate verdict. Final platform
matrices, saturated concurrency results and deployment evidence must name the
exact candidate and retain failures as well as successful outcomes. Optimized
Sedona spatial joins, client geometry UDT collection, distributed native graph
residency/iteration and general orchestrator qualification remain separate work.
