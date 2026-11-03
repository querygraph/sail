# Graph tables and resource-governed native extensions

Plan recorded: 2026-09-26T04:31:06.357Z

Repository: `querygraph/sail`; branch: `work/extensions-datafusion-graphs`.
Baseline: `18d7f6aa019846e01a10a6e426a76bc4e7e0b347`.

## Delivery contract

Keep graph data in ordinary partitioned Sail tables when an operation can use
relational operators. Provide graph helpers that produce normal Spark Connect
plans, with no second DataFusion SessionContext and no implicit CSR construction.
Native graph staging and algorithms remain available as an explicit choice.
Their memory allowance must participate in host admission, and independent reads
of one staged revision must share derived topology.

The original PoC branch and receipts remain unchanged. This branch addresses the
subsequent implementation review; passing the old test matrix is insufficient.
All implementation commits will be pushed to `querygraph/sail` after combined
verification. Changes are in Sail and its example extensions, not Grust crates.

## Work and acceptance evidence

| Work | Evidence required before delivery |
| --- | --- |
| Direct graph relations | Degrees including isolates, duplicate edges, loops, triplets and fixed-length walks run in separate-worker Sail with extensions disabled; EXPLAIN contains normal joins/aggregations and no native driver region |
| Explicit staged scans | Nodes/edges expose normalized Arrow fields without building CSR; empty property schemas survive; old snapshots remain valid after replacement/drop |
| Shared CSR revision cache | Independently planned concurrent reads construct one projection per revision/options key; replacement selects a new revision |
| Admission before allocation | Expanding IDs and missing labels are refused before normalized arrays allocate; sort/canonicalization scratch is reserved; refusal preserves existing graph |
| Host/native memory | Configured native quota and participating DataFusion operators contend for the same process pool; lease crosses a versioned callback ABI and survives every plan/output owner |
| Geometry | Native/builtin WKB composition, CASE/coalesce and array extraction preserve compatible fields locally and across workers; incompatible binary/geography combinations are rejected |
| Packaging | GEOS libraries are included in repaired wheels, relative dependencies resolve, and packaged bytes enter worker content identity |
| Failure and cancellation | Observe actual kernel state and admission after client interrupt/early stop; exercise real graph publication followed by failure before client receipt; preserve retained-output lifetime tests |
| Platform gates | Full extension build/test matrix on macOS arm64 and Linux x86_64 in Colima on morrobay; record each exact commit, toolchain, resource envelope and outcome independently |

## API and semantics

`Nutmeg(spark).tables(nodes, edges)` accepts ordinary DataFrames or table names.
It returns `GraphTables` with `nodes`, `edges`, `validate()`, `out_degrees()`,
`in_degrees()`, `degrees()`, `triplets()`, `walks(hops)` and `closed_walks(hops)`.
IDs retain their types; all three endpoint columns must have the same type.
Unique non-null node IDs and existing non-null edge endpoints define a valid
graph. Validation is explicit and executes queries. Duplicate edges and loops
remain distinct. Walks may revisit vertices/edges; their counts are not counts
of distinct simple paths or deduplicated triangles.

Table/query references follow normal Sail execution-time consistency. They are
not advertised as immutable snapshots. `stage(name, nodes, edges)` explicitly
captures a driver-resident native revision. `nodes(name)` and `edges(name)` scan
that staged representation; `run` explicitly chooses a native kernel.

```python
from sail_nutmeg import Nutmeg

nm = Nutmeg(spark)
graph = nm.tables("nodes", "edges").validate()
graph.degrees().show()       # ordinary Sail joins/aggregations on workers
graph.closed_walks(3).count()  # multiplicity includes starting points and edges

nm.stage("snapshot", graph.nodes, graph.edges)
nm.nodes("snapshot").filter("node_id = 'a'").show()  # driver Arrow scan; no CSR
nm.run("snapshot", "pagerank").show()                # explicit driver kernel
```

Staged scans expose normalized `property.*`/`present.*` fields and UTF8 IDs;
they do not preserve all original field types or derive unstaged nodes from
edge endpoints. Each provider pins its revision when resolved. Separately
resolved nodes/edges providers do not promise a common revision across a
concurrent overwrite; the Rust `SessionRegistry::snapshot` API can pin both.
The cache belongs to the shared published entry plus projection options.
Displayed revision numbers restart after drop/recreate and are not durable or
globally unique snapshot identifiers.

## Memory design

The first bridge reserves the native session cap up front from the host pool,
then lets the native graph budget subdivide that prepaid quota. This is coarse
admission: idle native capacity is reserved rather than dynamically lent to SQL.
The lease is non-spillable. Refusal must be explicit, and the charge remains until
the final native state, producer or Arrow output owner releases it.

The ABI contains version/size, byte count, opaque owner and retain/release
callbacks. No Rust trait object or allocator layout is interpreted by the other
library. A tiny dependency-free ABI crate is shared by the separately compiled
host and extension; native wheels do not depend on Sail engine crates.

With experimental extensions enabled, matching configured DataFusion pool kinds
and limits share a process pool across sessions/runtimes. Separate worker
processes have separate pools. An unbounded configuration remains unbounded;
configure a finite Greedy/Fair pool to enforce admission. This is not a universal
RSS cap: nonparticipating allocations and transport/runtime overhead need headroom.
Native accounting covers Arrow buffers/builders and admitted graph workspace,
not every Rust metadata allocation. Conservative bounds include transient growth,
so a write can be refused even when its eventual retained arrays alone would fit.
The per-library 16 MiB schema-probe store is outside user session quotas. The
bridge adds neither dynamic quota lending nor automatic CSR eviction.

## Boundaries

Relational graph queries execute on Sail workers. Native CSR kernels and staged
Arrow graphs remain on the driver; their scans have one driver partition even
when downstream operators redistribute the output. Initial CSR construction
and staging's final canonical sort are synchronous store operations that query
interruption cannot preempt. A streaming-kernel cancellation result does not
qualify those regions. Distributed iterative graph algorithms need
their own partitioned-state, convergence, exchange and fault-recovery design.
This change does not silently substitute joins for every native algorithm.

Optimized Sedona spatial joins, client geometry UDT collection and multi-host
orchestrator deployment remain separate milestones. Client-driven cancellation,
session shutdown and spill behavior must be reported at the exact boundaries
actually tested; a cancellation request alone is not termination evidence.
The [review and resolution record](implementation-review-resolution.md) preserves
the original findings, implementation references and unresolved acceptance scope.

## Gate procedure

Build an immutable candidate in a detached worktree with its own target directory
and `CARGO_INCREMENTAL=0`. Preserve failed experiments and exact verdicts. Run
the same extension matrix on macOS and Linux, including local, actor-cluster and
separate-process modes. Morrobay is an Intel iMac Pro with 36 logical CPUs and
128 GiB physical RAM. Record the Linux guest's actual memory and CPU visibility,
container limits, image identity and free disk; a Colima configuration value is
not evidence that the guest received that memory. The initial VZ development
attempt exposed only 3 GiB despite a 96 GiB setting and hit OOM. Preserve that
failure. The replacement QEMU profile now exposes approximately 78.53 GiB and
36 CPUs; its build container is capped at 72 GiB with no swap. The
[Linux environment record](linux-environment.md) contains the measurements,
image identity, setup commands and restoration procedure.
Run the two-host harness with one worker on Capitola and one on Morrobay, using
identical Intel executable/wheel bytes; Rosetta on Capitola is functional
qualification, not a native-performance comparison.
Do not publish performance measurements from this shared host as dedicated-host
timings. A branch update/push is conditional on the final candidate gates.
