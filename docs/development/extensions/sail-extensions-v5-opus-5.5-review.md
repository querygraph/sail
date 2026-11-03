# Review of the extension design review, and of the parallel Grust proposal

A second-opinion review, written 2026-09-26 by a Claude Code session that had
been developing a competing design document in another repository and did not
know this proof of concept existed. That is the first finding: two overlapping
asks for the same maintainer discussion were being prepared independently, and
only one should be sent.

## What was reviewed, and what was not

Read in full: [`design-review.md`](design-review.md), which describes
implementation commit `de8e670989edb8ed5343764c52d0d002b6b6cd63` on
`querygraph/sail` branch `work/extensions-datafusion-graphs`, against upstream
baseline `a85d912d72ae03a6d97b6a3fd151f5752da636c6`. Read for orientation:
[`implementation-plan.md`](implementation-plan.md),
[`implementation-review.md`](implementation-review.md),
[`implementation-review-resolution.md`](implementation-review-resolution.md),
[`datafusion-graph-plan.md`](datafusion-graph-plan.md),
[`linux-environment.md`](linux-environment.md), and the evidence index at
`target/extensions-datafusion-final/evidence-index.json`, whose recorded status
is `all_gates_passed_and_published` at that commit.

Not done here: the gates were not re-run, the 30,210-line diff was not read, the
wheels were not audited, and the review's central architectural claim — that no
Nutmeg payload type and no Sedona dependency enters a Sail engine crate — was
taken as stated rather than verified against the source. That claim is the one a
maintainer will check first, so it is worth verifying before submission rather
than after.

The other document under discussion is `docs/proposals/sail-extension-api.md`
in `querygraph/grust`, branch `work/proposal-v5`, commit `7fc0514` — a fifth
revision written on 2026-09-25, restructured around the Spark Connect protocol
after two research notes established how the JVM mechanism works and where Sail
rejects extension messages. Its fourth revision carried an external architecture
review (`sail-extension-api-astra-review.md`, committed at `aa3903e`) whose
eleven findings shaped its execution-contract sections.

## The main finding: the proposal is behind the implementation

The Grust-side fifth revision presents as design decisions, and in two cases as
open problems, things this proof of concept has already built and gated.

| Fifth revision | State in the proof of concept |
| --- | --- |
| "Route on the type URL" | Built, and bounded: 8 MiB envelope, 1 MiB payload, 16 inputs, 512-byte type URLs, nesting depth 64, validated before children are planned |
| "A Sail envelope that makes nested plans first-class" | Built as ordinary Connect input plans; registered input-free handlers may opt into a bare `Any` |
| "A handler returns an `FFI_TableProvider`" | Built: a provider produced from resolved physical inputs |
| "The output-field-names contract" | Built: input field names and result naming follow Sail resolution |
| "Commands versus relations" | Decided: stage, algorithm, scan, status and drop are all relation extensions |
| **"Runtime fidelity — the sharpest open issue"** | **Solved**: the host-owned input adapter retains the real Sail task context and Tokio runtime instead of a reconstructed default `RuntimeEnv` |
| "Placement covering functions as well as nodes" | Built: `DriverExtensionExec`, owner/plan handles a worker cannot decode, and a native expression codec carrying ownership identity and metadata-bearing return fields |
| "Mutation under retry: at-most-once in v1" | Built: one attempt for a region containing driver-native execution, with unacknowledged mutation reported as indeterminate |
| "Cluster mode is where a working plugin breaks" | Superseded for scalars: worker loading, installed-package identity and expression serialization, exercised on separate-process workers |
| Native memory accounting | Absent from the proposal entirely; the proof of concept has a dependency-free lease ABI, a host-funded quota drawn from the actual DataFusion pool, and admission before expansion |

Two consequences. First, the execution contracts that the external review
demanded — physical input requirements, runtime fidelity, ownership identity,
placement covering functions, retry semantics, teardown — are not open questions
here; most are implemented, and the remainder are stated as bounded policies.
Second, sending both documents to the discussion would present the maintainers
with two framings of one request, which is the failure already flagged about an
earlier over-long description.

**Recommendation: one ask, built on this review's spine** — its six decisions to
request and its eight-slice upstream sequence. What is worth carrying over from
the proposal is the constraint analysis against the maintainers' own quoted
statements, and the citation-level map of where Sail rejects and resolves
extension messages.

## What this review does well, and should not be edited away

- **It refuses the whole branch as one pull request**, and says so in bold. The
  eight-slice sequence, each slice naming one invariant, its smallest meaningful
  regression and what depends on it, is the correct shape for this maintainer.
- **Its evidence section states what the evidence does not establish** — network
  acknowledgement-loss tolerance, abrupt process recovery, Kubernetes operation,
  optimized spatial joins, full RSS accounting, portable ABI across Sail
  releases, distributed native graph iteration, performance. A claims section is
  only as credible as its exclusions.
- **Failed candidates are retained as evidence.** `d7143e099` failed scalar-list
  geometry metadata and geometry/NULL coercion; the fix preserved the assertions
  and corrected the subject rather than the test.
- **Boundaries travel with every claim**: 128 registered Sedona functions is
  explicitly not semantic qualification of each; the two-host run qualifies
  cross-host execution and explicitly not mixed-architecture compatibility or
  performance; native results feeding distributed SQL is explicitly not a
  distributed CSR kernel.
- **The pool-sharing policy is flagged for maintainer agreement** rather than
  slipped in, and the relational graph path works with extensions disabled,
  which is a strong independent argument.

## Six concerns, in the order I would address them

1. **Exact wheel rebuilds collide head-on with the maintainer's stated reason
   for wanting an FFI.** From discussion #2001: "we won't need to recompile the
   extension on every Sail version or Rust version change. This allows Sail and
   the extension to release under different schedule." The initial policy here
   is the opposite. Calling it "initial policy, not a permanent promise" is
   honest but not enough; the ask should confront this in its opening, with
   either a supported-build matrix or an explicit experimental status and a
   named path to stability.
2. **Change the process-wide pool registry before upstreaming slice 6, rather
   than shipping it and discussing it.** Sharing pools by matching kind and
   configured limit treats equal configuration as equal tenancy. The review
   already says this needs agreement; a multi-tenant accounting defect would
   discredit the whole resource contract, so prefer an explicitly owned resource
   domain with pool injection.
3. **Lead with slices 1 and 2.** Lifecycle correctness and geometry field
   preservation are useful to Sail with no extension API at all, are not
   conditional on the flag, and landing them first earns standing while reducing
   what the contentious slices must carry.
4. **Package the evidence before review.** The receipts, logs and artifact
   hashes exist under `target/extensions-datafusion-final/` with an index, but
   they are gitignored and absent from a clone. A redacted, checksummed bundle
   attached to the review is the difference between a narrative and something a
   maintainer can audit.
5. **Isolate the conversion-depth guard and its `stacker` dependency.** The
   review already recommends treating it as its own design decision; it touches
   ordinary conversion and will draw review attention disproportionate to its
   size.
6. **Note the emulation in the two-host run.** Capitola ran x86-64 under
   Rosetta; Morrobay is native Intel. The review discloses this correctly, but a
   maintainer may discount it, and no second native x86-64 host is currently
   available.

## What remains genuinely open after this work

The review's own six questions stand, and are the right ones to ask. To them I
would add only that the *first* question — whether an opt-in, exact-build Python
bootstrap is acceptable for an initial experiment — is really the wheel-rebuild
question above, and deserves the strongest answer the evidence can support,
because a maintainer who declines it declines the packaging model that everything
else assumes.

---

## Amendment, 2026-09-26: what the response addressed

Three commits answered this review: `bf97367dd` (explicit admission domains),
`d9a29bda3` (compatibility qualification and published evidence) and `fc4c32b21`
(the design review restructured as a standalone technical document). The
[follow-up record](review-follow-up.md) states what was accepted and what was
declined. Each claim below was checked against the repository rather than taken
from that record.

**Resolved, verified.**

- *Concern 2, the pool registry.* `MemoryResourceDomain`
  (`crates/sail-session/src/runtime/memory.rs`) replaces the process-global map
  keyed by pool kind and configured limit. Constructing a domain creates a pool;
  cloning shares that pool; two domains with identical configuration do not share
  admission, and there is a test for exactly that. The session manager owns one
  domain and injects it into sessions and in-process workers, with
  `SessionManagerOptions::with_resource_domain` for embedders who want sharing to
  be deliberate. This was the concern most likely to produce a quiet
  multi-tenant defect, and configuration equality is no longer identity.
- *Concern 4, the evidence.* `evidence/extension-review-evidence.tar.gz` is in
  the tree with a sidecar checksum that verifies (`shasum -a 256 -c`: OK), 1,049
  members, `original/` and `follow-up/` trees, a `manifest.json` carrying original
  and redacted hashes, and `SHA256SUMS` for the extracted contents. Binary and
  wheel bytes are excluded with their hashes retained; home paths, review
  hostnames and private addresses are redacted, and the exporter fails on
  recognizable keys. A maintainer can now audit rather than trust a narrative.
- *The two-documents problem.* [`maintainer-request.md`](maintainer-request.md)
  is a single ask of about fifty lines, and it demotes proposal v5 explicitly to
  "prior constraint research, not a second ask". It leads with the maintainers'
  own independence requirement rather than burying it.
- *Concerns 3 and 5, extraction order.* Lifecycle and field-metadata corrections
  remain the first review slices, and the conversion-depth guard with its
  `stacker` change is called out as a separately reviewed prerequisite rather
  than hidden inside the loader patch.

**Improved, with the limit named rather than removed.**

- *Concern 1, the wheel-rebuild collision with the release-independence
  requirement.* The response separates three statements that were previously one:
  what the loader accepts (API 1, DataFusion 55.1.0, Arrow 59.3.0 — not Sail SHA
  or compiler identity), what artifacts are qualified, and what is promised
  (nothing open-ended). Unchanged installed wheels were then run against two host
  revisions on both platforms — 776 integration passes across twelve cells — and
  the result is recorded in a machine-readable
  [matrix](compatibility-matrix.json). The posture is right: reuse demonstrated
  within a measured matrix, with compatibility-version negotiation named as the
  path to more.

  Two things keep this from closing. The interval is narrow: `de8e67098` and
  `bf97367dd` differ by one commit confined to `sail-session`, which does not
  touch the FFI boundary, so the test could not plausibly have failed. It would
  be worth far more against a revision that moves the FFI-adjacent surface — an
  upstream `main` advance, or a DataFusion patch bump. And both platforms used
  Rust 1.97.1, so compiler coupling — the other half of "every Sail version or
  Rust version change" — is disclosed and unqualified.

**Newly observed.**

- The maintainer request asks three decisions; the design review lists six open
  design choices. The one that falls outside the ask and still modifies Sail core
  is **field semantics**: slice 2 changes geometry metadata handling in the host.
  A maintainer answering three questions may not notice they have implicitly
  accepted a core change. Either raise it to the ask or say in the request that
  the other choices are documented and deferred.
- The follow-up **refuses to inherit the earlier verdict**: the resource-domain
  change was gated at its own narrower scope (35 session tests, strict clippy,
  workspace formatting, executable build) and the 651-test platform gates and the
  two-host run remain scoped to `de8e67098`. That is the discipline that makes
  the rest of the evidence worth reading, and it should survive editing.
- Failed candidates continue to be retained rather than deleted: a missing
  `PYTHONHOME` in the gate wrapper, `8cf7e2b60` failing strict clippy on an
  `expect` in a new test, an interrupted Linux transfer replaced by a verified
  re-export. Assertions were not weakened to pass.

**Still unverified here.** The architectural claim that no domain payload type
enters a Sail engine crate was not independently checked in either round, and the
follow-up has sharpened it honestly — geometry semantics and the generic host
extension machinery *do* modify core. That distinction is worth stating in the
request itself, because it is the first thing a maintainer will test the claim
against.

---

## The wheel question, stated in full

Extensions ship as Python wheels containing natively compiled Rust — SedonaDB's
`ST_*` functions, Nutmeg's kernels — that talk to Sail across DataFusion's FFI.
The question is: **once you build that wheel, how long does it keep working as
Sail releases?**

It matters because it is the maintainers' own stated reason for wanting an FFI at
all. From [discussion #2001](https://github.com/lakehq/sail/discussions/2001#discussioncomment-17083578):

> since DataFusion has an FFI, we won't use Rust trait as the API… so that we
> won't need to recompile the extension on every Sail version or Rust version
> change. This allows Sail and the extension to release under different schedule.

The proof of concept's initial policy was exact wheel rebuilds per supported host
build — the opposite of that. And the underlying difficulty is real: Rust has no
stable ABI, so two independently compiled libraries can only exchange `#[repr(C)]`
layouts and Arrow's C data interface. DataFusion's FFI `version()` reports only
the major, nothing enforces more, and a mismatch can misbehave rather than fail
loudly. The pins are exact — API 1, DataFusion 55.1.0, Arrow 59.3.0, and in
practice one compiler.

### What the response did

Three useful moves, and one of them is conceptually the best thing in it:

1. **Split one muddled claim into three.** What the loader *accepts* (API,
   DataFusion and Arrow versions — explicitly not Sail SHA, not compiler
   identity), what artifacts are *qualified* (a named matrix of host binary hash,
   wheel bytes, Python and platform), and what is *promised* (nothing
   open-ended). Previously these ran together, which is how "we rebuild wheels"
   and "we support reuse" could both sound true.
2. **Measured it.** Unchanged installed wheels run against two host revisions on
   both platforms, three execution modes each — 776 integration passes across
   twelve cells, with wheel and binary hashes checked before and after, a
   machine-readable [`compatibility-matrix.json`](compatibility-matrix.json) and
   a reproducible runner.
3. **Named the path**: expand the matrix with unchanged-wheel tests across
   supported Sail revisions, then explicit compatibility-version negotiation,
   rather than relaxing version checks speculatively. And a sharp observation —
   *"Bootstrap through Python can remain while the compatibility policy evolves;
   it is not itself what forces Sail-commit coupling."* The Python entry point was
   never the problem; the ABI is.

### Has it been solved? No — and mostly it is not ours to solve

What changed is the epistemic status: an unstated liability became a bounded,
measured, reproducible claim with a route forward. That is the right engineering
move and it is what should be in front of a maintainer. But the demonstration
does not yet support much weight:

- **The interval is trivial.** `de8e67098` and `bf97367dd` differ by one commit
  confined to `sail-session` that never touches the FFI boundary, changes no
  dependency version and leaves the lease ABI untouched. A wheel could not
  plausibly have broken across it. The test proves the harness works, not that
  reuse holds.
- **One compiler.** Rust 1.97.1 on both platforms, so the "or Rust version
  change" half of the requirement is untested.
- **No dependency movement.** No DataFusion or Arrow patch bump was crossed,
  which is the churn that actually happens.

Three cheap tests would move this from posture to evidence: an unchanged wheel
against upstream `main` advanced by a real interval (there are already thirty-odd
commits past the baseline); the same across a DataFusion patch bump; and the same
across a rustc minor bump. Each is hours, not weeks, and the third answers the
stated requirement's wording directly.

And the honest limit underneath: durable reuse comes from *narrowing the surface*,
not from testing it. Everything crossing must be C-layout or the Arrow C data
interface — if any Rust type crosses, no matrix saves you. The lease ABI crate is
the right shape for this (version, size, byte count, opaque owner, callbacks, no
`Arc` or trait objects). Whether the rest holds depends on `datafusion-ffi`, whose
own stabilisation is an open upstream issue. So the most Sail can promise now is a
negotiated compatibility version plus a refusal on mismatch — which is precisely
what the follow-up proposes as the next step.

The framing for the maintainers is therefore: we cannot promise an ABI range yet,
here is exactly what we tested, here is what would extend it, and here is the
negotiation we would build instead of loosening checks.
