# Vendored Nutmeg graph integration

Source: https://github.com/querygraph/nutmeg

Revision: `f267b03659dd536981f98420944f911b667632b7`.

Only `crates/nutmeg-graph/src` is included as implementation, with the upstream
MIT and Apache licenses and the upstream README as a catalog-test fixture. The standalone manifest reproduces its dependencies with the Sail
host's exact DataFusion 55.2.0 and Arrow 59.2.0 pins. The original catalog remains
a source dependency backed by the unchanged Grust 0.24.1 algorithms.

Local changes add `SessionRegistry` with a separate graph store and memory pool,
atomic two-part replacement, stable graph snapshots for read providers, and
session-specific read accounting. Existing process-global APIs remain available
for the upstream test suite. The extension exclusively uses session APIs.

Schema probes now use a separate fixed 16 MiB setup store. `prepare_output_schemas`
warms the finite catalog before user planning; session read constructors use
cache-only schema lookup. The one upstream test that constructs `AlgorithmTable`
directly is bound to the setup store explicitly.

The DataFusion graph follow-up shares each committed session revision and its
projection cache across independently planned readers. `GraphSnapshot` exposes
the staged normalized node and edge columns as ordinary Arrow table providers,
without constructing CSR. Empty staged schemas remain available. Snapshot
buffers retain their row admission until the final consumer releases them.
The cache belongs to the shared published entry and projection-options key;
name/revision strings are diagnostics, and revision numbering restarts after
drop/recreate. These providers scan one driver-resident partition.

`SessionRegistry::new_with_owner` accepts an opaque host admission lease; stores,
background readers, and exported algorithm/graph buffers retain it. This allows
the Sail bridge to reserve the whole native session budget from Sail's pool
while the existing Grust pool accounts for graph allocations within that quota.
It is conservative admission, not allocator-level RSS accounting or native
spill support. Standalone `SessionRegistry::new` keeps its independent budget.

Normalization and canonicalization reserve conservative buffer/build bounds
before converting IDs, filling missing columns, encoding sort keys, or copying
sorted rows. They shrink excess admission after measuring the retained buffers.
An isolated allocator regression fixture checks that rejecting an expanding
integer-ID batch does not allocate the normalized buffers first, and that the
accepted fixture's transient allocation peak fits its admission peak. Bounds
cover Arrow buffers/builders and admitted graph workspace, not every Rust
metadata allocation. They include empty casts, nested encoded casts and repeated
view values, which can expand beyond their physical backing buffers.

Query cancellation remains cooperative within kernels. Initial CSR construction
and the final canonical staging sort are synchronous store operations and cannot
be preempted by query interruption. No native spill or automatic cache eviction
is introduced.

The local optimized-algorithm follow-up adds distinct `pagerankDelta` and
`wccRandomized` kernels under `src/optimized/`; the original `pagerank` and `wcc`
names keep their Grust implementations. These additions use public projection
edges and the existing query admission, work, cancellation and output ownership
paths. Rayon is now an explicit dependency for per-read worker pools, bounded
by configured concurrency. PageRank uses fixed input partitions for floating
reductions; randomized WCC uses SplitMix64/GF64 priorities, stable original-ID
representatives, contraction histories and reverse expansion. Both have static
output schemas, so session binding does not execute them against Grust's
string-ID probe graph. Bounded diagnostic records expose the work performed.

See [OPTIMIZED_ALGORITHMS.md](OPTIMIZED_ALGORITHMS.md) for exact options,
normalization/certification rules, algorithm attribution, serial phases and
accounting boundaries. These kernels were written locally; no Grust repository,
published crate, or registry-cache source was modified. The upstream README
fixture stays unchanged and its catalog test covers the reference catalog;
separate tests cover the documented local additions.

The `wccRandomizedFused` follow-up applies the initial-edge preparation idea
from graphframes-rs PR 56, pinned at
`10715e28d9f7c450e74881bcd4acce8dc99a250f`, to the local native contraction
implementation. Nutmeg already handles both endpoints in one pass; this
variant omits the kernel's initial sort/deduplication and keeps the original
`wccRandomized` and Grust `wcc` methods available. It shares subsequent rounds,
admission and output ownership with the existing kernel. No upstream source
was copied or modified; the precise scope and tradeoffs are documented in
[OPTIMIZED_ALGORITHMS.md](OPTIMIZED_ALGORITHMS.md).
