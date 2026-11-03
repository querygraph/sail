# Experimental Nutmeg kernels

These local additions are `pagerankDelta`, `wccRandomized`, and
`wccRandomizedFused`. The unchanged
Grust 0.23.0 `pagerank` and `wcc` kernels remain available as reference variants.
No Grust registry source or published crate has been modified.

These kernels use the existing Nutmeg projection, read lifecycle, work limits,
cancellation checks, buffer reservations, bounded output channel, and host
memory lease. The public Grust projection exposes dense edge endpoints but not
its CSR adjacency, so delta PageRank builds an additional, query-owned outgoing
index. Its construction and memory are part of the query. The new kernels
accept only unweighted outgoing projections; node/relationship filters remain
projection options. Unsupported options are rejected during validation.

## `pagerankDelta`

Options: `damping=0.85`, `tolerance=1e-8`, `maxIterations=1000`,
`precision="f64"`, plus normal read limits and `concurrency`. Results retain
`nodeId`, `score`, `iterations`, `converged`, `residual`.

Let `T(x)=(1-d)/N+dPx`, where dangling columns of `P` are uniform, parallel
edges contribute individually, and self-loops are retained. Start uniformly,
maintain `r=T(x)-x`, and let `R=||r||1` and `m=sum(x)`. Select the frontier where
`abs(r_i)>min(R/(2N), tolerance*m/(4N))`.
For its selected residual vector `a`, update `x+=a` and `r=r-a+dPa`, retaining
inactive residuals so vertices can reactivate. Only active source adjacency is
traversed between certificates. Dangling pushes require a uniform vertex update.

The tolerance-scaled cutoff lets sufficiently small local corrections become
inactive as the solution settles. The relative cap bounds the inactive L1
residual by `R/2`, so the next total residual is at most `((1+d)/2)*R` in exact
arithmetic. If every vertex becomes inactive, then
`2R/m<=tolerance/2`, within the cheap certificate bound. Frontiers may grow again
through reactivation; monotonic shrinking is not promised. Diagnostics record
the cutoff policy, activation mass, residual, threshold, and active edge count.

Normalize `y=x/sum(x)` and recompute the full residual `||T(y)-y||1` before
claiming convergence. The cheap certificate trigger is `2||r||1/sum(x)<=tol`;
a failed certificate rebases the maintained state. Initialization and the final
capped iteration are also certified. A stationary graph may take zero pushes.
At exhaustion, normalized output is returned with `converged=false`.

Here `residual` means the **normalized fixed-point L1 residual**, unlike the
reference kernel's successive-iterate difference. Its stationary L1 error is
bounded by `residual/(1-d)` in exact arithmetic. Diagnostics identify this
meaning explicitly; benchmark validation recomputes it independently.

A per-read Rayon pool respects the requested concurrency. Sources are split
into fixed blocks of 4096, independent of pool width. Each block produces an
admitted sparse vector of target contributions, sorts/reduces it, and merges in
fixed block order. Results are deterministic across worker counts for identical
inputs. Global vertex updates, final certificates, and block merging are serial.
Admission includes worst-case contribution storage, not only observed frontier
size. OS thread stacks and read-log metadata are outside buffer accounting.

## `wccRandomized`

Algorithm background: Bögeholz, Brand, and Todor,
[In-database connected component analysis](https://arxiv.org/abs/1802.09478),
and [Sem's pinned graphframes-rs implementation](https://github.com/SemyonSinchenko/graphframes-rs/blob/b4da56dabe20bba8e29563e06acc5179b2113ce3/src/algorithm/connectivity/connected_components.rs).
This implementation is written locally. It retains original IDs as representatives
instead of using affine priority values as new IDs; reverse expansion therefore
uses stored mappings and needs no accumulated affine transformation.

Options: `seed=42` (an exact unsigned 64-bit JSON integer; signed int64 bit
patterns are also accepted for compatibility),
`maxIterations=1000`, plus normal read limits and `concurrency`. Results retain
`nodeId` and `componentId`, both strings. IDs must parse uniquely as BIGINT;
labels are the minimum **numeric** original ID in each weak component.

Each round draws a nonzero `a` and then `b` from SplitMix64 and computes the
signed priority `gf_axpb(a,id,b)` over GF(2^64), modulo
`x^64+x^4+x^3+x+1`. Each active vertex chooses the minimum-priority vertex in
its closed neighborhood. Representatives retain original IDs. This is one-hop
contraction: parent chains are not shortcut inside a round. Relabel both edge
endpoints, drop self-loops, deduplicate undirected edges, and repeat. Save each
mapping and back-propagate through the history, preserving terminal components
and isolates; normalize each final component to its numeric minimum.

This is a separate randomized contraction implementation, not the reference
union-find kernel under a new name. GF64 priorities and edge relabeling use the
per-read worker pool. Neighborhood choices, sorting, and back-propagation are
serial. Exhaustion raises an explicit nonconvergence error. Supported iteration
limits are 1..10000 for these algorithms.

## `wccRandomizedFused`

This optional variant shares the same contraction implementation, options,
static schema, BIGINT identity contract, seed sequence, labels, admission and
cancellation behavior as `wccRandomized`. The original method and reference
`wcc` remain available.

The motivation is [graphframes-rs PR 56](https://github.com/SemyonSinchenko/graphframes-rs/pull/56),
pinned at [`10715e28`](https://github.com/SemyonSinchenko/graphframes-rs/blob/10715e28d9f7c450e74881bcd4acce8dc99a250f/src/algorithm/connectivity/connected_components.rs).
That relational change combines symmetric neighbor contributions with the
representative aggregation and removes upfront edge preparation. Native Nutmeg
already considers both endpoints in one choice pass, without making a reverse
edge copy. Its corresponding change therefore omits only the kernel's initial
undirected canonicalization, sort and deduplication. It filters self-loops and
visits raw directed edges during the first round. Repeated and reverse edges
cannot change a neighborhood minimum. After relabeling, the existing canonical
sort/deduplication produces the same contracted graph as `wccRandomized`, so all
later rounds match for the same seed.

Diagnostics mark `initial_edge_policy="raw-non-loop"`; the first `edges_before`
counts raw non-loop edges and may exceed the original variant's count. Every
other contraction trace field agrees. Omitting an initial sort can reduce
preprocessing, while duplicate-heavy inputs can increase first-round visits.
No elapsed-time or peak-memory improvement is assumed. The conservative scratch
reservation remains based on the full input edge count. Native graph staging
and projection construction are unchanged; this variant does not bypass their
sorting, allocations, or driver-local execution boundary.

`Nutmeg.status().reads[].diagnostics` reports actual/requested worker counts,
frontier or contraction sizes per round, seed bits, iteration count, and final
convergence evidence. These bounded read-log records are bookkeeping, outside
data-buffer accounting. Output buffers retain their admitted reservations
through stream delivery and Arrow FFI ownership.
