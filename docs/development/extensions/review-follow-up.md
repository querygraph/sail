# Review follow-up: admission ownership and compatibility qualification

This document implements the accepted recommendations from the
[Opus review](sail-extensions-v5-opus-5.5-review.md), with the qualifications below.
The [design review](design-review.md) remains the consolidated architecture and
upstream sequence. Proposal v5 is a requirements/research source, not a second
competing maintainer request. This follow-up does not authorize or claim an
upstream submission.

## Explicit resource domains

Implementation candidate: `bf97367dd231de2535ad9b04b415fd2619bc4696`.
The host change is confined to eight files in `sail-session`; it introduces no
new crate, wire field, native ABI change or domain-library dependency.

`MemoryResourceDomain` replaces the process-global map keyed by pool kind/limit.
Constructing a domain creates a pool; cloning it shares that exact pool. Identical
configuration in two independently created domains does not share admission.

With experimental extensions enabled, the standard session manager creates one
domain and passes it explicitly in `ServerSessionInfo` and `SessionJobRunnerInfo`.
Server runtime environments and in-process worker runtime environments use that
domain. An independently started worker factory owns a separate domain. With the
flag off and no explicitly injected domain, runtime environments retain separate
pools. Configuration equality is never used as identity.

The manager is the default admission boundary, not a security/tenant boundary:
users of one manager still compete for its finite pool. An embedder can explicitly
supply a domain with `SessionManagerOptions::with_resource_domain`; reusing a
clone across managers deliberately shares admission. A custom session factory or
runtime mutator remains responsible for honoring that contract. No process RSS
limit, per-user fairness, native spilling or dynamic quota lending is added.

Tests cover Greedy/Fair contention, isolation with identical settings, lease
survival after the domain owner drops, and actual runtime-factory pool injection.
Existing multi-session resource and actor/process integration tests exercise the
standard factory wiring.

## Three distinct compatibility statements

1. **Loader acceptance:** the manifest requires API 1, DataFusion 55.1.0 and Arrow
   59.3.0, then validates placement, names and declared relations. It does not
   compare Sail source SHA or Rust compiler version. Workers additionally require
   the matching installed package identity for that distributed job.
2. **Qualified artifacts:** a supported combination must identify the host binary,
   wheel bytes, Python/platform and tests. An unchanged wheel exercised against
   two host revisions demonstrates reuse across those revisions only.
3. **Compatibility promise:** no open-ended ABI range is currently promised.
   Neither manifest agreement nor content identity proves binary safety by itself.
   Rejecting a declared version mismatch is a validation test, not an experiment
   that deliberately invokes an incompatible native capsule.

The reproducible runner is
[`check_compatibility.py`](../../../examples/extensions/scripts/check_compatibility.py).
It requires at least two host source revisions, verifies binary checksums, checks
installed wheel payloads against the supplied wheel archives, and runs the full
extension integration suite in local, actor-cluster and process-cluster modes.
It checks wheel and binary bytes again afterward. It does not build or install
packages. Host source provenance comes from independently retained build receipts;
a binary hash alone cannot prove source provenance.

The path toward release independence is a published matrix expanded by unchanged-
wheel tests across supported Sail revisions, followed by explicit API/FFI version
negotiation for any broader range. A future changed DataFusion/Arrow boundary or
compiler/platform combination requires its own evidence. Mixed host versions within a single cluster and cross-compiler wheel reuse are
not established by separate homogeneous-cluster runs. Bootstrap through Python
can remain while the compatibility policy evolves; it is not itself what forces
Sail-commit coupling.

## Qualifications retained from the response

- Runtime fidelity is demonstrated for host child execution via its retained task
  context/runtime and memory/spill tests. It is not a universal foreign-service
  bridge. Gathered single-partition inputs do not implement arbitrary sorted or
  co-partitioned foreign input requirements.
- Driver-only scalar exports are rejected; general driver-scalar placement is not
  implemented. Driver-only relation placement and worker-capable scalars are.
- Proposal v5 discusses independent native budgets and resource risks. The new
  contribution is the concrete host-funded lease/admission implementation.
- Rosetta remains disclosed in the original cross-host qualification. That run
  proves cross-host execution with matched x86_64 artifacts, not native mixed-
  architecture interoperability or performance.
- Domain libraries and payload implementations remain outside Sail engine crates.
  Geometry semantics and generic host extension machinery still modify core.

## Auditable evidence distribution

[`package_review_evidence.py`](../../../examples/extensions/scripts/package_review_evidence.py)
exports text receipts, logs and retained failed candidates. It excludes executable
and wheel bytes, preserves their hashes in existing receipts, redacts home-account
paths, the two review hostnames/SSH accounts and private non-loopback IPv4 addresses,
and fails on recognizable private keys or credential tokens. Manual review is
still required before publication; this is not a universal secret detector.

Every exported record has both original and redacted SHA-256 values in the manifest.
`SHA256SUMS` checks the extracted bundle, and a sidecar checks the archive itself.
Original evidence is never rewritten. Historical absolute paths are provenance,
not promised portable links; the manifest lists actual bundle members. The original
`de8e67098` verdict remains scoped to that commit. New results must identify their
own source revision and must not inherit the old Linux/two-host verdicts.

## Upstream extraction order

Retain independent lifecycle and geometry corrections as the first review slices.
Extract the generic Connect conversion-depth guard and `stacker` change as a
separate prerequisite within the bounded-relation series, with ordinary-parser
regressions. Do not hide that policy inside the extension loader PR. This follow-up
changes resource ownership and provides qualification/review artifacts; it does
not claim those upstream PRs have been extracted, opened or accepted.

## Qualified results and download

Both platforms passed the targeted resource-domain gate at
`bf97367dd231de2535ad9b04b415fd2619bc4696`: 35 session library tests, strict
all-target session clippy, workspace formatting and the Sail executable build.
The qualification also runs unchanged installed wheels against the original
`de8e67098` binary and that revised binary:

| Platform | Host source | Local | Actor cluster | Process cluster |
| --- | --- | --- | --- | --- |
| mac-arm64 | `de8e67098` | 64 / 1 expected skip | 65 | 65 |
| mac-arm64 | `bf97367dd` | 64 / 1 expected skip | 65 | 65 |
| linux-x86_64 | `de8e67098` | 64 / 1 expected skip | 65 | 65 |
| linux-x86_64 | `bf97367dd` | 64 / 1 expected skip | 65 | 65 |

This is 776 integration passes and four expected worker-only skips across twelve
cells. API/DataFusion/Arrow pins, native package source and the lease ABI are
unchanged. Rust 1.97.1 is used on both platforms; these are not cross-compiler tests.
Seven Python harness tests passed, including refusal of changed installed wheel
bytes and redaction/hash retention with failed-evidence fixtures.

The [machine-readable matrix](compatibility-matrix.json) names both host binary
hashes and the two platform-specific wheel pairs. Each pair was reused unchanged
within its platform; no wheel is claimed portable between these architectures.
The original full 651-test platform gates and two-host run remain historical
`de8e67098` evidence, not fresh qualifications of the resource-domain change.

Download the [review evidence bundle](evidence/extension-review-evidence.tar.gz)
and [archive checksum](evidence/extension-review-evidence.tar.gz.sha256). After
extracting into an empty directory, run `shasum -a 256 -c SHA256SUMS` there.
`manifest.json` lists exported records, original and redacted hashes, and omitted
binary files. The `original/` directory preserves previous gates and rejected
candidates; `follow-up/` carries the new gates, compatibility logs, harness hashes,
environments and cleanup records. Hostnames and account paths are redacted.

The first macOS attempt exposed missing `PYTHONHOME` in the gate wrapper; the
corrected environment reran it. Candidate `8cf7e2b60` then failed strict clippy
on an `expect` in a new test. Candidate `bf97367dd` propagates that error and
passed. Both failed logs remain in the bundle. Assertions were not weakened.

The first Linux evidence transfer was interrupted by premature container shutdown;
a verified text-only re-export replaced it. Ten binary AppleDouble resource-fork
sidecars from the macOS tar transfer are recorded separately by magic and hash.
All ten actual Python fixture hashes and the runner hash match across platforms.
These metadata files were not collected by pytest and do not change the test matrix.

The follow-up Linux guest exposed 24 CPUs and 67,414,478,848 bytes of RAM, with a
56 GiB no-swap container limit. This differs from the original gate envelope and
is recorded separately. The task container and profile were stopped after export;
the existing default Colima profile remained running. No performance inference
is made from these functional runs.
