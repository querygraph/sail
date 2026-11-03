# Benchmark evidence and reproduction

Read [the report](../pecan-nutmeg-benchmark.md) for the execution and measurement
boundaries, and [the tutorial](../../../../examples/extensions/benchmarks/TUTORIAL.md)
for building and running every method. Run the commands below from the repository
root, using Python 3.12 and a full clone containing the measured commits.

## Files

| Directory | Contents |
| --- | --- |
| `primary/` | 150 planned cells: reference and advanced methods; 148 passes and two expected convergence caps. |
| `fusion/` | 78 planned WCC cells: original randomized and fused randomized plans, with controls built from the same native source. |
| `constrained/` | Original resource-limited attempt: 16 passes, four admission errors, 130 not run. |
| `figures/` | Generated PNG/SVG figures, compact tables, ratios and chain diagnostics. |
| `audit/` | Independent audit findings; the report distinguishes raw-vector checks from receipt-only coverage. |
| `proof/` | Compressed original diagnostics, logs, inventories, memory samples, failure records and qualification receipts. |
| `reproduce/` | Figure renderer, independent auditor, corruption controls and delivery-integrity check. |

Each result directory contains `summary.json`, `cells.csv` and `tables.md`.
Published CSV sidecars use LF line endings; original CSV bytes remain in the
proof archive. Every planned cell remains present, including failures and cells never run.
Only successful trials enter successful-run aggregates. The chain diagnostic
has one repetition; sparse groups have three. The complete tables preserve
RSS, PSS, cgroup peaks, native accounting and VM-wide steal alongside time.

The proof archive preserves original bytes. Its `manifest.json` records every
included file's origin, size and SHA256. Parquet, wheels and native libraries
are omitted; their hashes remain in the inventories. The public archive cannot
recalculate the omitted result vectors: that part of the recorded independent
audit used the separately retained raw artifacts. A new harness run generates
new full outputs and validates each against independent references.

## Verify and unpack the evidence

This writes only below `target/pecan-public-proof` and checks every archived
file before using it. Use a fresh destination.

```bash
python3 - <<'PY'
import hashlib, json, tarfile
from pathlib import Path
base = Path('docs/development/extensions/pecan-nutmeg-benchmark/proof')
archive = base / 'evidence.tar.gz'
bundle = json.loads((base / 'bundle.json').read_text())
assert hashlib.sha256(archive.read_bytes()).hexdigest() == bundle['archive_sha256']
manifest = json.loads((base / 'manifest.json').read_text())
out = Path('target/pecan-public-proof')
out.mkdir(parents=True, exist_ok=False)
with tarfile.open(archive, 'r:gz') as tar:
    assert tar.extractfile('manifest.json').read() == (base / 'manifest.json').read_bytes()
    for entry in manifest['included']:
        data = tar.extractfile(entry['path']).read()
        assert len(data) == entry['bytes']
        assert hashlib.sha256(data).hexdigest() == entry['sha256'], entry['path']
    tar.extractall(out, filter='data')
print('Verified', len(manifest['included']), 'files')
PY
```

The archive is organized into `primary/`, `fusion/`,
`constrained-first-attempt/`, `qualification/` and `development-controls/`.
Historical local paths in original receipts are provenance, not installation
instructions. `qualification/two-host-fusion/` preserves the first task-slot
refusal, corrected test, independent storage scan and final shutdown verdict.
An attribution note corrects a scanner's static source label without changing
the original receipt.

## Rebuild tables and audit recorded measurements

These commands do not run Sail. The auditor obtains the matching historical
summarizer from Git, checks receipts and inventories, reconstructs sampled
memory peaks and replays the summary. Without raw Parquet it labels output
correctness checks as `receipt_only`; it does not substitute that coverage for
the original raw-vector audit.

```bash
python3 -m venv target/pecan-evidence-venv
target/pecan-evidence-venv/bin/pip install numpy==2.5.3 pyarrow==21.0.0 matplotlib==3.11.2
export PECAN_EVIDENCE_PYTHON="$PWD/target/pecan-evidence-venv/bin/python"
export PECAN_EVIDENCE_ROOT="$PWD/target/pecan-public-proof"
export PECAN_REPORT_ROOT="$PWD/docs/development/extensions/pecan-nutmeg-benchmark"
"$PECAN_EVIDENCE_PYTHON" "$PECAN_REPORT_ROOT/reproduce/audit_matrix.py" \
  --repo "$PWD" --kind main \
  --evidence "$PECAN_EVIDENCE_ROOT/primary/export-matrix-3c1fd28c" \
  --summary "$PECAN_REPORT_ROOT/primary" \
  --output target/pecan-public-primary-audit
"$PECAN_EVIDENCE_PYTHON" "$PECAN_REPORT_ROOT/reproduce/audit_matrix.py" \
  --repo "$PWD" --kind fusion \
  --evidence "$PECAN_EVIDENCE_ROOT/fusion/export-fusion-matrix-3fd69787" \
  --summary "$PECAN_REPORT_ROOT/fusion" \
  --output target/pecan-public-fusion-audit
PECAN_AUDIT_PILOTS="$PECAN_EVIDENCE_ROOT/qualification/linux/pecan-pilots-a6567b51" \
  "$PECAN_EVIDENCE_PYTHON" "$PECAN_REPORT_ROOT/reproduce/test_audit_matrix.py"
"$PECAN_EVIDENCE_PYTHON" "$PECAN_REPORT_ROOT/reproduce/render_report.py" \
  --primary "$PECAN_REPORT_ROOT/primary/summary.json" \
  --fusion "$PECAN_REPORT_ROOT/fusion/summary.json" \
  --output target/pecan-rebuilt-figures
```

To add independent vector checks from retained output files, pass `--raw PATH`
and `--require-raw`. The selected raw layout is `datasets/<name>/dataset/` and
`cells/<cell-id>/artifacts/result/`, matching the original inventories. Primary
raw selection is the twelve 100k first-repetition distributed results plus four
Banda 1m results; fusion selection is all six 100k first-repetition results.
Missing required raw files cause the audit to fail. Hash checks detect altered
files, and the corruption controls test wrong output IDs, component membership,
rank vectors, coefficient streams and process-memory totals.

## Run new measurements

Follow tutorial section 7. Use `matrix.example.json` for the twelve original
path/method combinations and `fusion-matrix.example.json` for the matched WCC
comparison. Build the current source consistently, record the actual source
SHAs and artifact hashes, and use new run/output names. Run capacity pilots
before the timed matrix, stop other task workloads, and retain every cell.
The source table in the report identifies the historical runs; rebuilding a
current revision is a new experiment, not a replacement for their evidence.
