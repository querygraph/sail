# Extension wheel compatibility under independent host changes

This experiment separates compiler changes, upstream Sail changes, a change to
DataFusion's FFI implementation, and a whole-engine dependency downgrade. It
retains the original platform-specific Sedona and Nutmeg wheels, built with Rust
1.97.1. No wheel was rebuilt or its manifest edited to make a test pass.

The strongest new result is executable reuse across Rust 1.97.1 and 1.98.1.
The result is an artifact matrix, not an ABI range or a release-support promise.
The production branch keeps its dependency pins and strict manifest checks.
The `sail-extensions-1` tag remains unchanged.

## Experiment design and provenance

The source starting point is `287930474c6af2523223621cddc9a42c44ef0fdd`.
Its host code and lockfile match `bf97367dd`; intervening commits change review
and tutorial material. This makes the compiler experiment independent of a host
code change. The old host control is the preserved, qualified `bf97367dd`
executable on each platform. New builds use detached worktrees, separate target
directories, locked dependencies and `CARGO_INCREMENTAL=0`.

| Case | Source | Deliberate change |
| --- | --- | --- |
| Compiler | `287930474` | Build Sail with Rust 1.98.1; wheels remain Rust 1.97.1 artifacts |
| Upstream advance | `77c43834c` | Merge upstream `48e90326e` into the starting point; compile with Rust 1.97.1 |
| FFI diagnostic | `1378bbcb3` | Only `datafusion-ffi` moves from 55.1.0 to 55.0.0; engine and Arrow remain 55.1.0/59.3.0 |
| Whole-engine downgrade | `fa34dd697` | All 37 DataFusion lockfile packages move to 55.0.0; host manifest checks change to that exact engine version |

Upstream was fetched before choosing a candidate. It had **two**, not thirty-odd,
commits beyond the original baseline `a85d912d7`: Delta schema/column-mapping
corrections and Iceberg Windows path/timestamp corrections. The merge touches 17
files and includes an array-lambda helper change. It is a real upstream advance,
but does not establish compatibility across a long release interval.

The registry query found no DataFusion 55.1.1 or later release: 55.1.0 was latest
for both `datafusion` and `datafusion-ffi`. The FFI-only reverse-minor experiment
therefore tests available dependency movement without inventing a patch release.
Its lockfile changes exactly one package. A complete 55.0.0 downgrade is recorded
separately and also includes resolver changes to Windows dependency edges.
The available-version response is retained with a generated timestamp.

Rust 1.98.1 was selected as the newer minor line's patch release. The
[official release note](https://blog.rust-lang.org/2026/09/03/Rust-1.98.1/)
describes the vtable code-generation correction relative to 1.98.0. This
experiment does not qualify 1.98.0, all compiler flags, release/LTO builds, or
other allocators.

## Execution results

| Platform / host case | Local | Actor cluster | Process cluster |
| --- | --- | --- | --- |
| macOS-arm64 / baseline-rust-1.97.1 | 64 passed, 1 skip | 65 passed | 65 passed |
| macOS-arm64 / compiler-rust-1.98.1 | 64 passed, 1 skip | 65 passed | 65 passed |
| Linux-x86_64 / baseline | 64 passed, 1 skip | 65 passed | 65 passed |
| Linux-x86_64 / upstream | 64 passed, 1 skip | 65 passed | 65 passed |
| Linux-x86_64 / compiler | 64 passed, 1 skip | 65 passed | 65 passed |
| Linux-x86_64 / ffi-diagnostic | 64 passed, 1 skip | 65 passed | 65 passed |

These six rows contribute 1,164 integration passes and six expected skips.
The two mixed-compiler process runs below add 130 passes, for **1,294 integration
passes and six expected skips** in this round. The diagnostic FFI row is reported
separately from production qualification even though its assertions pass.

Every normal matrix row runs the existing 65-test integration suite in local,
actor-cluster and separate-process-cluster modes. Local mode has one expected
worker-only skip. Tests cover real wheel binding, spatial expressions and
geometry shuffles, relational graph plans, native staging/algorithms/scans/drop,
resource admission, protocol rejection and lifecycle behavior. Registering a
function alone is not counted as execution qualification.

The macOS Rust 1.98.1 candidate also passes 35 session library tests and the
resource-lease ownership test. Seven Python harness tests pass. These targeted
checks do not repeat the original 651-test platform gates or strict clippy.

The runner hashes wheel archives, verifies every installed non-dist-info payload
against its archive before and after the run, and checks host executable hashes.
Native extensions remain separately compiled shared libraries. Mac and Linux
use their own original wheel pair; no cross-platform wheel reuse is claimed.
The [expanded machine-readable matrix](abi-compatibility-matrix.json) records
case labels, compiler versions, source commits, binary hashes and outcomes.

On macOS arm64, two additional full process-cluster runs use a Rust 1.97.1 driver
with Rust 1.98.1 workers, then the reverse. Both pass all 65 tests. The trusted
worker-command override selects the opposite binary; both sides use identical
installed wheel content. These are mixed-compiler processes on one machine,
not mixed engine versions, mixed architectures, or a new two-host verdict.

### Whole-engine downgrade: compilation refusal

The complete DataFusion 55.0.0 candidate fails to compile in
`crates/sail-execution/src/proto/native_expr.rs` because
`CastExpr::has_explicit_metadata` is unavailable in that release. The method is
needed by the host's geometry-field codec. The test stops there: no executable
or runtime compatibility result is claimed. Removing the metadata guard just
to obtain a passing build would change the behavior being tested.

This is a **source API dependency**, not evidence that a wheel crossed an
incompatible ABI. Conversely, the FFI-only diagnostic keeps that engine API
unchanged and can reach real execution. Keeping those outcomes separate is the
point of the two candidates.

## Refusal and resource-boundary probes

The new [boundary runner](../../../examples/extensions/scripts/check_abi_boundaries.py)
executes each probe in a fresh process group with a timeout. It retains logs,
exit codes, wheel/binary hashes and the runner/fixture fingerprints.

Eight manifest probes advertise older/newer API versions, DataFusion 55.0.0,
hypothetical 55.1.1 and 56.0.0, and Arrow 59.2.0, hypothetical 59.3.1 and 60.0.0.
They are metadata fixtures, not wheels compiled against those versions. Every
case must fail before binding or inspecting native capsules. The existing
54.1.0 mismatch test remains in the full integration suite.

Nine lease probes call the unchanged Nutmeg wheel using a real, aligned
C-layout lease allocated by Python `ctypes`, with live retain/release callbacks:

- A valid lease produces exactly one retain and one final release.
- Versions 0 and 2, and sizes shorter/longer than the expected record, fail with
  an ABI mismatch before callbacks run.
- Wrong quota and null owner fail with a quota mismatch before callbacks run.
- An eight-byte header immediately before an inaccessible guard page is rejected
  for a wrong version or short declared size. Any read into the versioned tail
  would fault the isolated child. This checks the readable-header-only contract.

Each platform passes eight manifest probes against the new-compiler host and
nine direct-wheel lease probes. The ctypes owner is an ordinary integer, not a
Rust `Arc`; the wheel must treat it as opaque and use only its callbacks.
The lease record measures 40 bytes on these platforms. These tests supply readable, aligned headers; they do not pass
unreadable header pointers, call fabricated function addresses, unload a live library,
or establish safety against malicious native code. Trusted native plugins can
still corrupt their process.

## What the boundary actually contains

The examined DataFusion 55.1.0 boundary uses C-layout callback records, Arrow C
array/schema wrappers, Stabby string/vector containers and async-ffi future/poll
wrappers. It is more than the Arrow C data interface alone. The relevant
`Arc<dyn ...>`, Tokio handles and native stream objects live in producer-owned
private data behind opaque pointers. Release callbacks execute in the owning
library. Library-marker checks distinguish the same-library optimization from
the foreign conversion path.

A small compiled probe records sizes, alignments and selected public offsets for
`FFI_ScalarUDF`, `FFI_TableProvider`, `FFI_ExecutionPlan`,
`FFI_RecordBatchStream`, `FFI_TaskContextProvider` and `FFI_TaskContext`.
The measured layouts match between the macOS arm64 Rust 1.98.1 build and all
4 examined Linux library variants across Rust 1.97.1/1.98.1 and FFI
55.0.0/55.1.0. Multiple baseline feature variants were measured separately.
For example, scalar UDF, table provider and execution-plan records measure
144, 232 and 120 bytes respectively, each aligned to eight bytes.
The probe compiles against host dependency archives; it does not introspect the
installed wheels. Matching these measurements rules out those particular layout differences; it
cannot establish callback semantics, every nested layout, all generic
instantiations, or future ABI stability. The execution suite supplies a separate
behavioral check. Neither is a formal memory-safety audit.

The FFI-only candidate deliberately changes the host's FFI crate while keeping
its engine version. The loader currently compares declared API/engine/Arrow
versions, not a separately negotiated FFI-layout identifier. Thus its acceptance
is not itself proof of this combination's safety. The production lockfile still
pins engine and FFI to the same release. A future compatibility contract should
name the actual boundary version and supported dependency combinations instead
of treating the engine's version string as a complete ABI fingerprint.

## Evidence and reproduction

The [redacted evidence bundle](evidence/extension-abi-evidence.tar.gz) includes
build logs, exact commands, receipts, source patches, layout probes and retained
failures. Its [checksum](evidence/extension-abi-evidence.tar.gz.sha256) protects
the archive; extracted `SHA256SUMS` protects its members. Binary and wheel bytes
are omitted, with their identities retained. Original wheel-build provenance
remains in the [earlier evidence bundle](evidence/extension-review-evidence.tar.gz).
Prior evidence is not overwritten.

The experimental source commits are published as separate branches:
`work/extensions-abi-upstream`, `work/extensions-abi-ffi-55.0` and
`work/extensions-abi-df-55.0`. They are test inputs, not proposed production
upgrades or downgrades. Rebuild a candidate in a detached worktree with its own
target and recorded compiler; retain the same already-built wheels.

```bash
# Install the same wheel pair once into a dedicated Python 3.12 environment.
# hosts.json records at least two distinct source revisions and binary hashes.
.venv/bin/python examples/extensions/scripts/check_compatibility.py \
  --hosts /path/to/hosts.json \
  --wheel /path/to/sail_sedona_extension.whl \
  --wheel /path/to/sail_nutmeg.whl \
  --output /path/to/new-compatibility-evidence

.venv/bin/python examples/extensions/scripts/check_abi_boundaries.py \
  --sail-binary /path/to/sail \
  --wheel /path/to/sail_sedona_extension.whl \
  --wheel /path/to/sail_nutmeg.whl \
  --output /path/to/new-boundary-evidence
```

Use the actual platform-tagged wheel filenames. Build logs must establish source
and toolchain provenance; a binary checksum alone does not establish either.
The mixed-worker runner and layout probe, including their commands, are in the
bundle. They are diagnostic recipes rather than additions to Sail's runtime.

The first boundary run failed because its relative fixture path was resolved
from the server's temporary working directory. Converting it to an absolute path
made the same assertions pass; that failed receipt and script are retained.
The downgrade's lock resolution initially tried to lower `datafusion-doc` while
`datafusion-macros` still required its newer version; dependency-order correction
resolved it. Initial layout-probe setup errors and the whole-engine compile
failure are retained as well. The first new-compiler Rust unit wrapper omitted
`PYTHONHOME`, so embedded Python could not import `encodings`; the same compiled
tests pass with the documented environment. Both logs are retained. No assertion
was weakened.

The Linux runs use Morrobay’s separate Colima/QEMU profile: 24 visible CPUs,
67,414,470,656 bytes of measured guest RAM and a 56 GiB container limit with no
swap. Recorded cgroup OOM counters remain zero. These are functional results on
shared review hosts; no performance conclusion follows from elapsed times.
Test binaries and receipts are preserved before removing this round’s temporary
build caches and worktrees. The task container and VM are stopped afterward;
the pre-existing default Colima profile remains running.

## Consequences for the design

Compiler identity need not be part of the loader's acceptance key for the tested
combinations. These runs provide direct evidence for that conclusion beyond a
same-compiler host-code refactor. They do not justify removing exact dependency
checks or promising a version range.

The defensible next step remains a small, explicit compatibility negotiation
contract, with independently built wheel artifacts in CI. Treat successful
execution, refusal before binding, build failure, unavailable releases and
untested combinations as distinct outcomes. Include callback ownership and
lifecycle regressions, not only layout fingerprints or scalar smoke tests.
No engine code or production dependency is changed by this evidence update.
