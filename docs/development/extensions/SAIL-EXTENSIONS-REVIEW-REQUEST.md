# Sail Extensions: review request

Version **1.0.0** · **2026-10-03**

Start all Sail extension reviews here. This is the shared entry point for the
Sedona scalar path, stateful relation extensions, and the separate compatibility
preflight candidate. The [canonical review request](https://github.com/querygraph/sail/blob/sail-extensions/docs/development/extensions/SAIL-EXTENSIONS-REVIEW-REQUEST.md)
tracks the current request; [version 1.0.0](SAIL-EXTENSIONS-REVIEW-REQUEST.v1.0.0.md)
preserves this text. Record the document version and source commit with feedback.

## The request

We built an experimental extension mechanism for Sail, the Rust Spark Connect
engine, and used it to call native SedonaDB/GEOS scalar functions from independent
Python wheels through DataFusion FFI. The spatial implementation stays outside
Sail's engine crates. Sail provides registration, planning, metadata transport
and worker execution. Selected spatial expressions and geometry metadata have
been exercised across a shuffle; exporting 128 functions does not establish
exhaustive coverage of that catalog. Indexed Sedona spatial joins and a complete
Spark Sedona implementation are outside this prototype.

We would value Sedona-side feedback on the scalar boundary and feedback from
other extension authors on the additional stateful contracts. The aim is to
decide which guarantees are useful, which constraints are acceptable, and what
evidence is still missing. Adoption and any future upstream scope remain open.

## Choose a review route

| Route | Read and run | Questions to examine |
| --- | --- | --- |
| Sedona and distributed scalars | Read [Sedona scope](design-review.md#sedona-reuse-native-functions-without-importing-a-spatial-engine), then the [tutorial](../../../examples/extensions/TUTORIAL.md). Complete prerequisites and **step 4: build**, **step 5: server**, **step 6: spatial SQL**; use **step 8: workers** for process execution. | Registration, name/alias collisions and compatibility; worker package identity; geometry metadata; callback and library lifetime. Which compositions and failures should be contractual? |
| Stateful native relations | Read the [execution architecture](design-review.md#execution-architecture), [input adapter](design-review.md#4-a-host-owned-input-adapter), [driver placement and retry](design-review.md#5-explicit-driver-placement-and-conservative-retry-behavior), and [native ownership](design-review.md#6-host-funded-native-ownership). Run tutorial **step 7: Nutmeg**, then **step 8: workers**. | Bounded relation dispatch, input ownership, memory admission, driver-local state and mutation receipts. Is the conservative one-attempt policy appropriate? |
| Compatibility preflight | Read the candidate summary below and its [implementation and validation record](https://github.com/querygraph/sail/blob/work/extensions-static-preflight/docs/development/extensions/static-compatibility-preflight.md). | Is rejecting old wheels without static metadata worth the stronger loading order? What migration and compatibility policy should accompany it? |

Bounded relation dispatch, driver placement and mutation retry rules belong to
the stateful path. They are not requirements for every scalar extension.
Nutmeg's native graph state and algorithms remain on the driver; distributed
input and output do not establish distributed native graph execution.

## Keep the two implementations distinct

| Review target | Runtime source | Status |
| --- | --- | --- |
| Published combined prototype | `bd8ce9ae8839477e2c08a0475ab7900b115c5366` | The `sail-extensions` branch carries this runtime plus review-document updates. Its historical gates keep their own source revisions. |
| Static compatibility preflight | `ae32aee3521540849ad55e9efa6f6733169a7013` | Separate candidate on `work/extensions-static-preflight`, based on the prototype. Its implementation and tests do not change the prototype's runtime. |

The prototype loads an entry point, optionally calls a factory, and calls
`manifest()` before validating versions. Sedona delays its native import until
binding, but that is a package convention. The host's existing validation
precedes binding and capsule interpretation, not arbitrary package import.

The candidate first collects **all discovered entry points**, reads static JSON
from their owning distributions' recorded files, and validates their declarations
on both driver and worker. Any static failure stops that loader invocation
before any discovered entry point's `load()`. It then imports, constructs the
extension and checks that the dynamic manifest agrees on name, version, API,
DataFusion and Arrow versions before binding. Runtime options such as Nutmeg's
`memory_bytes` remain dynamic; binding and FFI contracts are unchanged.

Missing, malformed, ambiguous or incompatible static metadata fails closed.
**Old wheels without it must be rebuilt and reinstalled**, including on workers;
there is no import fallback. Sedona and Nutmeg have migrated example wheels.
Extensions remain trusted native code. Declarations do not prove ABI safety,
authenticate Python's imported provider or sandbox its behavior. Earlier imports
elsewhere in the process are outside this loader-invocation guarantee.

The candidate's fresh host and rebuilt wheels passed **22 Rust extension tests,
42 integration cases and 3 packaging tests on macOS ARM64**, with Python 3.12.8
and Rust 1.97.1. The integration cases comprise 35 protocol checks, four native
positive cases, one worker pre-import rejection and two worker identity cases.
Packaging checks covered both repaired wheels; installed file records and
Sedona's bundled GEOS dependencies were checked separately. This is candidate
evidence on one platform, not a Linux run, exhaustive Sedona qualification or a
performance claim. Commands and local receipt boundaries are in its validation
record; ignored local logs are not presented as downloadable repository evidence.

## Check out the target you mean to review

For the published prototype:

```sh
git clone --branch sail-extensions https://github.com/querygraph/sail.git sail-extension-review
cd sail-extension-review
git rev-parse HEAD
```

For the separately adoptable preflight candidate, use another checkout:

```sh
git clone --branch work/extensions-static-preflight https://github.com/querygraph/sail.git sail-preflight-review
cd sail-preflight-review
git rev-parse HEAD
```

Then follow the tutorial from the checkout you selected. **Skip its clone
commands when already in that checkout**, including inside a prepared Linux
environment; do not switch a candidate review back to the prototype. Use each
checkout's own build and environment. Branch heads can gain documentation
commits, so record `HEAD` and the runtime source above. The older
`sail-extensions-1` tag remains a historical snapshot.

## Useful feedback

Please name the document version, source commit, platform and review route.
Distinguish what you inspected from commands you ran; include the executable and
wheel identities with runtime results. A concrete counterexample, an alternative
contract, or a missing acceptance test is especially useful. Reply in the existing
review conversation or use [the Sail fork's issues](https://github.com/querygraph/sail/issues).

The [design review](design-review.md) supplies the wider architecture and
historical evidence. Its review units describe possible boundaries; separately
extracted and validated upstream changes have not been produced. This request
does not presume a deadline or an adoption decision.
