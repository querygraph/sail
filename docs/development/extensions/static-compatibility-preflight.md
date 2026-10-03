# Static compatibility checks before extension import

This change lets Sail reject an extension's declared build incompatibility before
loading its Python entry point. It closes the import-order gap in the reviewed
prototype, while retaining the existing binding and DataFusion FFI paths.

This candidate implementation lives on the local `work/extensions-static-preflight`
branch, based on `bd8ce9ae8839477e2c08a0475ab7900b115c5366`. It proposes a stricter
loading contract for review, including deliberate rejection of old wheels without
static metadata. The decision book and its captured source
remain a historical review of that base; this document describes the separate
implementation change. Validation is recorded below and must be read independently
of the historical prototype's results.

## What changes for Alexy's decision

Previously the [session and worker loaders](../../../crates/sail-session/src/extensions/mod.rs)
had to load an entry point and call `manifest()` to discover its compatibility
declarations:

```text
entry-point import → optional factory → manifest() → version check → bind → FFI
```

Sedona deferred its native import until `bind()`, but that was a package convention.
Another package could import its native module at the top level, before Sail saw
the manifest. Rejecting that manifest later could not undo the import.

The new order has two passes, on both driver and worker:

```text
all discovered entries → read their static records → validate their declarations
then: import → factory → manifest() → agreement + validation → bind → FFI
```

The boundary covers **all entry points discovered by that loader invocation**. If
the last entry has an incompatible static declaration, an earlier compatible
entry is not loaded first. This is stronger than putting a static check immediately
before each individual import.

It gives you a concrete guarantee to consider: a static compatibility failure
stops discovery before Sail calls any discovered extension entry point's `load()`.
It does not establish that matching declarations prove ABI compatibility.
The shared [preflight implementation](../../../crates/sail-session/src/extensions/preflight.rs)
supplies the checked entries to both loader paths; it is not a separate Python
import helper.

## The static file and its owner

Each installed distribution that advertises a `pysail.extensions` entry point
must record its compatibility file among `Distribution.files`. Sail reads the
file through that owning distribution's `locate_file()` method. It does not import
the package, call `find_spec()`, or fall back to the dynamic manifest to find it.

For entry-point module `m`, replace dots with slashes. Exactly one of these
recorded paths must exist:

| Entry-point layout | Recorded compatibility path |
| --- | --- |
| Package, such as `sail_sedona:extension` | `sail_sedona/sail-extension.json` |
| Module, such as `my_extension:factory` | `my_extension.sail-extension.json` |

Nested module names follow the same rule: `company.adapter:factory` uses
`company/adapter/sail-extension.json` or `company/adapter.sail-extension.json`.
The part after the colon names the object loaded later; it does not change the
static-file location. Missing file records, missing files and two candidate paths
are errors rather than reasons to guess a path or import the package.

The Sedona document has this shape:

```json
{
  "schema_version": 1,
  "extensions": [
    {
      "entry_point": "sedona",
      "name": "sedona",
      "version": "0.1.0",
      "api_version": 1,
      "datafusion_version": "55.1.0",
      "arrow_version": "59.3.0"
    }
  ]
}
```

`entry_point` selects the registered name in `pysail.extensions`, not the wheel's
distribution name or its import module. A file can describe more than one entry
point, but each mapping must be unambiguous. The host rejects missing or malformed
metadata, unsupported schema versions, duplicate mappings and incompatible API,
DataFusion or Arrow declarations. There is no import-based compatibility fallback.

## What remains dynamic

Once all static checks pass, the host follows its existing import, factory and
`manifest()` path. First these five fields must agree with the selected static
record:

- `name`
- `version`
- `api_version`
- `datafusion_version`
- `arrow_version`

A disagreement fails before binding or host interpretation of FFI capsules. The
entry-point import and `manifest()` have already executed by this second check;
the static pass does not move dynamic validation ahead of import.
After agreement, the host still applies the [dynamic manifest validation](../../../crates/sail-session/src/extensions/manifest.rs).
Static and dynamic declarations share the same `validate_build()` version rules.

Placement, relation declarations and runtime resource configuration remain on the
dynamic path. In particular, Nutmeg's `memory_bytes` can still come from the
environment. The static document is not a snapshot of session settings or a new
memory-admission protocol. `bind()`, `bind_with_resources()` and the capsule
interfaces keep their existing responsibilities.

The existing [package fingerprint](../../../crates/sail-session/src/extensions/package_identity.py)
includes recorded package-file bytes and the dynamic manifest. Because the static
JSON is a recorded package file, its contents participate in that identity too.
Preflight and worker identity answer different questions: whether declarations
are admitted before import, and whether the worker resolves the expected package
identity during execution.

## Migrating a package

Existing wheels without static metadata now fail the preflight. There is no
silent legacy mode. An extension author should:

1. Add `sail-extension.json` at the path determined by the entry-point module.
2. Match the entry-point name and the five immutable fields in `manifest()`.
3. Include the JSON in the built wheel and its installed file record. A file
   present only in a source checkout is insufficient.
4. Rebuild and install the wheel on the driver and every applicable worker.
5. Check both rejection before import and successful binding of the new wheel.

Sedona and Nutmeg are migrated together with the loader. Their packaging and
dynamic bootstrap definitions are the examples to follow:

- [Sedona packaging](../../../examples/extensions/sedona/pyproject.toml) and
  [bootstrap](../../../examples/extensions/sedona/python/sail_sedona/__init__.py).
- [Nutmeg packaging](../../../examples/extensions/nutmeg/pyproject.toml) and
  [bootstrap](../../../examples/extensions/nutmeg/python/sail_nutmeg/__init__.py).

Both examples can retain their deferred native imports. The new host check makes
static compatibility rejection independent of that convention; it does not require
moving imports earlier.

## Scope and remaining trust

Installed extensions remain trusted native code. Static metadata is a declaration,
not a signature, sandbox or proof that a library obeys its declared ABI. A package
can make a false declaration, import another package, or misbehave after passing
preflight. Reading metadata from the owning distribution does not authenticate
the provider that Python's import resolution will load. This change does not make
Python import resolution or mutable installed
files tamper-proof. It does not distribute wheels, qualify compatibility ranges,
change function registration, or alter relation execution and resource ownership.

The guarantee belongs to a loader invocation, not to the complete lifetime of a
Python process: an extension might already have been imported for another reason.
Describe it as **static declaration checks before extension loading**, rather than
"no native code has run."

## Validation

These results cover the candidate branch over
`bd8ce9ae8839477e2c08a0475ab7900b115c5366`, on macOS ARM64 with Python 3.12.8 and
Rust 1.97.1. They do not reuse the historical prototype's test results.

| Check | Result |
| --- | --- |
| Fresh host build, `cargo build --locked -p sail-cli` | Passed; produced `target/debug/sail`. |
| Formatting, `cargo +nightly fmt -- --check` | Passed. |
| Rust extension tests, `cargo test --locked -p sail-session --lib extensions::` | 22 passed, 0 failed, 27 filtered out; 0.92 seconds. |
| Sedona and Nutmeg native wheels | Built with the dev profile, repaired and installed. |
| Metadata packaging tests on both repaired wheels | All 3 passed. |
| Installed distribution file records | Both static files present; inspected without importing extension modules. |
| Sedona native dependency check | Bundled GEOS check passed. |
| Native positive cases with separate process workers | 4 passed using the fresh host and both rebuilt, installed wheels. |
| Extension protocol tests | 35 passed. |
| Separate-worker rejection before import | 1 passed. |
| Missing or changed worker package identity | 2 passed. |

The build used this environment:

```sh
PYO3_PYTHON=/Users/alexy/src/sail-extensions-poc/.venv/bin/python \
CARGO_BUILD_JOBS=4 CARGO_INCREMENTAL=0 CARGO_PROFILE_DEV_DEBUG=0 \
cargo build --locked -p sail-cli
```

The Rust suite required the embedded Python runtime's library and standard-library
paths:

```sh
PYO3_PYTHON=/Users/alexy/src/sail-extensions-poc/.venv/bin/python \
PYTHONHOME=/Users/alexy/.local/share/uv/python/cpython-3.12.8-macos-aarch64-none \
DYLD_LIBRARY_PATH=/Users/alexy/.local/share/uv/python/cpython-3.12.8-macos-aarch64-none/lib \
CARGO_BUILD_JOBS=4 CARGO_INCREMENTAL=0 CARGO_PROFILE_DEV_DEBUG=0 \
cargo test --locked -p sail-session --lib extensions::
```

The successful local log is
`target/static-preflight-evidence/rust-tests-python-runtime.log`. Initial attempts
without the Python library path and then `PYTHONHOME` failed before the suite
completed; correcting the environment required no source change.

Both wheels were built from the migrated package sources using:

```sh
.venv/bin/python -m maturin build \
  --manifest-path examples/extensions/sedona/Cargo.toml --locked --profile dev \
  --interpreter .venv/bin/python --out target/static-preflight-wheels/raw-sedona \
  --auditwheel repair
.venv/bin/python -m maturin build \
  --manifest-path examples/extensions/nutmeg/Cargo.toml --locked --profile dev \
  --interpreter .venv/bin/python --out target/static-preflight-wheels/raw-nutmeg \
  --auditwheel repair
```

The macOS wheel repair and final packaging check used:

```sh
.venv/bin/python -m delocate.cmd.delocate_wheel -v \
  -w target/static-preflight-wheels/repaired \
  target/static-preflight-wheels/raw-sedona/sail_sedona_extension-0.1.0-cp312-cp312-macosx_11_0_arm64.whl
.venv/bin/python -m delocate.cmd.delocate_wheel -v \
  -w target/static-preflight-wheels/repaired \
  target/static-preflight-wheels/raw-nutmeg/sail_nutmeg-0.1.0-cp312-cp312-macosx_11_0_arm64.whl
.venv/bin/python examples/extensions/scripts/tests/test_extension_metadata.py \
  --wheel target/static-preflight-wheels/repaired/sail_sedona_extension-0.1.0-cp312-cp312-macosx_26_0_arm64.whl \
  --wheel target/static-preflight-wheels/repaired/sail_nutmeg-0.1.0-cp312-cp312-macosx_11_0_arm64.whl -v
```

After native-library repair, the wheel SHA-256 values were:

- Nutmeg: `74420f5fac22006a19d194212f2df8b6fc6db4fe7accd6c1fd956f0d80b25668`.
- Sedona: `317c325d210645d8cdb1fa0fc83d592e796292d7a89af14efa1a49b03dbc5678`.

The local receipt is
`target/static-preflight-evidence/native-wheel-build-receipt.json`; related files
are `wheel-metadata-tests.log`, `installed-wheel-metadata.json` and
`sedona-native-dependencies.json` in the same directory. These logs and receipts
are ignored build artifacts in the validation checkout, not repository attachments.

The native positive run exercised Sedona scalar SQL and geometry metadata across
workers and shuffle, plus Nutmeg schema, algorithms, worker inputs and driver
barrier. From the repository root, its command was:

```sh
.venv/bin/python -B -m pytest \
  examples/extensions/tests/test_sedona.py::test_geometry_metadata_survives_worker_expressions_and_shuffle \
  examples/extensions/tests/test_sedona.py::test_scalar_sql_uses_native_sedona \
  examples/extensions/tests/test_nutmeg.py::test_all_partitions_schema_and_algorithms \
  examples/extensions/tests/test_distributed.py::test_nutmeg_stage_has_worker_inputs_and_a_driver_barrier \
  --sail-binary target/debug/sail --execution-mode process-cluster \
  -q -p no:cacheprovider \
  --basetemp target/static-preflight-evidence/native-positive-tmp \
  --junitxml target/static-preflight-evidence/native-positive.xml
```

It exited successfully with all four cases passing. The exact absolute-path
invocation is in `target/static-preflight-evidence/native-positive-receipt.json`;
the same directory contains `native-positive.log` and `native-positive.xml`.

The protocol run passed 35 cases, including static schema and file-location
failures before import, an incompatible later entry preventing earlier imports,
nested module lookup without importing its parent, immutable-field disagreement
before binding, and dynamic `memory_bytes` values. Its command was:

```sh
.venv/bin/python -B -m pytest examples/extensions/tests/test_protocol.py \
  -q -p no:cacheprovider --sail-binary target/debug/sail \
  --basetemp target/static-preflight-evidence/protocol-final-tmp \
  --junitxml target/static-preflight-evidence/protocol-final.xml
```

The exact invocation and successful result are in the local
`target/static-preflight-evidence/protocol-final-run.json` and
`protocol-final.log`.

The separate-worker rejection case passed with an empty import/factory/manifest/
bind event log and the expected static mismatch in worker logs. It launched
distinct worker processes and checked that a later incompatible entry prevented
an earlier compatible entry from loading. Two existing worker identity cases
also passed, rejecting a missing package and changed package contents:

```sh
.venv/bin/python -B -m pytest \
  examples/extensions/tests/test_distributed.py::test_process_workers_reject_static_mismatch_before_import \
  -q -p no:cacheprovider --execution-mode process-cluster \
  --sail-binary target/debug/sail \
  --basetemp target/static-preflight-evidence/worker-preimport-tmp \
  --junitxml target/static-preflight-evidence/worker-preimport.xml
.venv/bin/python -B -m pytest \
  examples/extensions/tests/test_distributed.py::test_separate_worker_rejects_unavailable_or_changed_package \
  -q -p no:cacheprovider --execution-mode process-cluster \
  --sail-binary target/debug/sail \
  --basetemp target/static-preflight-evidence/worker-identity-tmp \
  --junitxml target/static-preflight-evidence/worker-identity.xml
```

Their local receipts are `worker-preimport-run.json` and `worker-identity-run.json`
under `target/static-preflight-evidence`, with corresponding `.log` and `.xml`
files. Both invocations exited successfully. These are correctness checks on one
platform and build profile, not performance measurements or compatibility-range
qualification.

Parser tests, loader-order tests, wheel-content checks and a native runtime
exercise establish different things; none substitutes for the others.

The relevant fixtures live with the [Rust preflight](../../../crates/sail-session/src/extensions/preflight.rs),
the [metadata packaging checks](../../../examples/extensions/scripts/tests/test_extension_metadata.py),
the [protocol tests](../../../examples/extensions/tests/test_protocol.py), and the
[distributed tests](../../../examples/extensions/tests/test_distributed.py).
