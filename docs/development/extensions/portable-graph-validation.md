# Portable graph algorithms: implementation and validation

This is the historical qualification of the original two methods. Later
delta/frontier PageRank, randomized WCC and fused WCC are documented and
qualified separately in the [current benchmark report](pecan-nutmeg-benchmark.md).

The implementation is on `work/extensions-datafusion-graphs` in
`querygraph/sail`. The executable code reviewed here is commit
`70b0d1cab2cab945d4dbaf6842ee0e38c8aa1822`. The earlier
`sail-extensions-1` tag is unchanged and does not contain this addition.

The [step-by-step testing tutorial](../../../examples/extensions/graph-algorithms/TESTING.md)
covers a fresh source build, local and distributed tests, an interactive example
with expected answers, and a two-host run using shared storage.

## What runs where

The pure Python `pyspark_graph_algorithms` client implements PageRank and weakly
connected components with ordinary Spark Connect relations. Sail/DataFusion
executes their joins, aggregates and Parquet writes. Python advances iterations
and receives scalar convergence results and bounded storage receipts.

PageRank redistributes dangling probability and preserves parallel edges. WCC
uses exact minimum-label propagation, with the minimum original BIGINT ID as
component label. It is not graphframes-rs's randomized contraction algorithm.
Both preserve isolates and reject invalid endpoints. Their precise semantics
and runnable examples are in the [client guide](../../../examples/extensions/graph-algorithms/README.md).

## Sail changes

The host implementation stays inside an opt-in `sail-session` module, with four
integration points:

- Generate the `gf.utils.v1.Request` contract and register its zero-input
  relation through the existing extension registry.
- Resolve staging storage through Sail's existing object-store registry and
  credentials. The driver owns run allocation, bounded listing and deletion.
- Register `gf_version` and bit-exact `gf_axpb` through the existing scalar
  ownership and worker serialization paths.
- Clean up session-owned runs during session teardown, with bounded retries.

The only Spark Connect server edit replaces full request debug dumps with
request identifiers, preventing opaque extension payloads from exposing run
tokens. No graph-specific scheduler, aggregate ABI, DataFusion fork or CSR
conversion was added. Existing Sedona and Nutmeg implementations remain separate.

[PR #2670](https://github.com/lakehq/sail/pull/2670) addresses the same underlying
host-storage boundary for Python datasource callbacks. This implementation uses
the shared Rust storage-resolution layer; it adds Connect receipts and graph-run
ownership above that layer. It does not need a second Python storage client or
the callback bridge itself.

## Validation

| Host / mode | Extension integration | Portable graph package |
| --- | --- | --- |
| Capitola macOS / local | 69 passed, 1 topology skip | 33 passed |
| Capitola macOS / process workers | 70 passed | 33 passed |
| Morrobay Linux / local | 69 passed, 1 topology skip | 33 passed |
| Morrobay Linux / in-process cluster | 70 passed | 33 passed |
| Morrobay Linux / process workers | 70 passed | 33 passed |

Native macOS and Linux each passed 47 `sail-session` and 37
`sail-spark-connect` library tests, plus Clippy with warnings denied. The
launcher suite passed its 9 tests on macOS and Linux.

Both two-host exercises passed: existing Sedona/Nutmeg behavior, and the new
graph algorithms. PageRank completed 72 iteration tasks and WCC completed 66,
with successful tasks from both physical hosts in each algorithm. Each ran
three iterations and matched its expected answers. All supervised Sail
processes exited. Post-run inspection found no files in 10 owned staging roots
on Capitola, no files in 15 on Linux, and no objects in the shared S3 run prefix.

The [evidence archive](evidence/portable-graph-evidence.tar.gz) and
[archive checksum](evidence/portable-graph-evidence.tar.gz.sha256) contain
receipts, exact binary/package identities, commands, successful gates and
retained failures. The archive includes checksums for every exported record.
Home-directory names and private host addresses are redacted; credentials and
executable bytes are excluded.

Recorded UTC: 2026-09-27T17:44:20.527320+00:00

Tests compare small-graph results against independent PageRank calculations and
exact component labels. Cases include empty graphs, isolated vertices, sinks,
self-loops, duplicate edges, convergence limits and invalid graph data. Storage
tests cover pure analysis, idempotent allocation/release, cross-session and
cross-run refusal, bounded listings, symlink rejection, transient cleanup
failures, active-write cancellation and killed-client expiry.

The two-host graph check matches successful worker task records to jobs issued
inside algorithm iteration windows. Input preparation alone does not qualify.
The same executable and native wheel identities are checked on both hosts.
These are functional checks, not performance measurements; Capitola uses the
macOS x86_64 executable under Rosetta.

The debug-log regression was also run against the pre-fix candidate and failed
on the actual token leak. Earlier candidate failures are retained in the
evidence alongside successful gates; their verdicts do not apply to later code.

## Lifecycle limits

Normal completion, validation errors and cancellation between completed writes
delete staging immediately. An interrupted or failed write leaves its namespace
owned by the session; the exception exposes `run_path` and `cleanup_deferred`.
An interrupt acknowledgment is not a writer-termination barrier. Teardown
requests job shutdown and makes up to three cleanup attempts, but detached late
writers, persistent store failures and abrupt server termination can require
administrator cleanup after writers have stopped. There is no durable orphan
catalog or independent per-run TTL.

The local staging root must be precreated and exclusively managed by Sail.
Existing symlinks are refused; concurrent hostile filesystem modification is
outside the contract. Multi-host operation requires storage reachable by every
worker under the same URI. Spark/Snowpark portability, randomized contraction,
additional algorithms and native aggregates remain future work.

## Reproduction

Use the [client installation guide](../../../examples/extensions/graph-algorithms/README.md)
and retain this implementation branch when following the source build tutorial.
`examples/extensions/scripts/verify.sh` now includes the portable package tests
in local, local-cluster and process-cluster modes. For a focused run:

```bash
.venv/bin/python examples/extensions/scripts/test_graph_algorithms.py \
  --sail-binary target/extensions-poc/host/debug/sail \
  --execution-mode process-cluster --output /tmp/graph-check-new
```

The output directory must not already exist. For two physical hosts, follow
the [two-host setup](../../../examples/extensions/TUTORIAL.md#10-run-across-two-hosts)
using matching source/binaries from this branch. Add an `environment_file` entry
to the driver and each worker target in the launcher JSON. Each value is an
absolute path on that target's host to a private JSON file, for example:

```json
{
  "SAIL_GRAPH_UTILS_ROOT": "s3://review-bucket/graph-staging",
  "AWS_DEFAULT_REGION": "us-east-1",
  "AWS_ACCESS_KEY_ID": "YOUR_ACCESS_KEY",
  "AWS_SECRET_ACCESS_KEY": "YOUR_SECRET_KEY"
}
```

Use an existing bucket. For an S3-compatible endpoint, also provide
`AWS_ENDPOINT`; HTTP endpoints additionally require `AWS_ALLOW_HTTP="true"`.
All targets must resolve the root to the same objects. Protect these files and
keep them out of source control; launcher receipts contain their paths, not
their contents. Then run:

```bash
.venv/bin/python examples/extensions/scripts/two_host.py \
  --config two-host.json --exercise portable-graphs --output /tmp/two-host-graph-check
```

The [implementation plan](portable-graph-plan.md) records the accepted differences
from the project draft and the deferred server-side graphframes-rs option.
