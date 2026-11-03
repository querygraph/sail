#!/usr/bin/env python3
"""Record immutable identities after the verification script has passed."""
import argparse
from datetime import datetime, timezone
import hashlib
from importlib import metadata
import json
import os
from pathlib import Path
import platform
import re
import runpy
import shutil
import subprocess
import sys

parser = argparse.ArgumentParser()
parser.add_argument("--target", type=Path, required=True)
parser.add_argument("--output", type=Path, required=True)
args = parser.parse_args()
repo = Path(__file__).resolve().parents[3]


def sha256(path):
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(8 * 1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def command(*argv):
    return subprocess.check_output(argv, cwd=repo, text=True).strip()


def native_files(distribution):
    package = metadata.distribution(distribution)
    return {str(path): sha256(Path(package.locate_file(path)))
            for path in package.files or [] if str(path).endswith((".so", ".dylib", ".pyd"))}


def resource_envelope():
    # On Linux this is guest-visible memory, not the physical Mac's RAM.
    visible_memory = os.sysconf("SC_PHYS_PAGES") * os.sysconf("SC_PAGE_SIZE")
    cgroup = {}
    for name in ("memory.max", "memory.peak", "cpu.max", "cpuset.cpus.effective"):
        path = Path("/sys/fs/cgroup") / name
        if path.is_file():
            cgroup[name] = path.read_text().strip()
    disk = shutil.disk_usage(args.target)
    return {
        "os_visible_memory_bytes": visible_memory,
        "cpu_affinity_count": len(os.sched_getaffinity(0)) if hasattr(os, "sched_getaffinity") else None,
        "cgroup_v2": cgroup,
        "target_disk_total_bytes": disk.total,
        "target_disk_free_bytes_after_gate": disk.free,
        "container_image": os.environ.get("SAIL_GATE_IMAGE_ID"),
        "virtual_machine_profile": os.environ.get("SAIL_GATE_VM_PROFILE"),
    }


identity = runpy.run_path(str(repo / "crates/sail-session/src/extensions/package_identity.py"))["identity"]
identities = {}
for entry in metadata.entry_points(group="pysail.extensions"):
    factory = entry.load()
    if callable(factory):
        factory = factory()
    manifest = factory.manifest()
    identities[manifest["name"]] = identity(entry, manifest)
process_logs = list((args.target / "pytest-process-cluster").rglob("server.log"))
processes = []
for path in process_logs:
    content = path.read_text()
    for worker, driver in re.findall(r"extension process worker \d+: pid=Some\((\d+)\), driver_pid=(\d+)", content):
        processes.append({"log": str(path.relative_to(args.target)), "worker_pid": int(worker), "driver_pid": int(driver)})
if not processes or not all(row["worker_pid"] != row["driver_pid"] for row in processes):
    raise RuntimeError("missing separate-process worker evidence")

receipt = {
    "timestamp_utc": datetime.now(timezone.utc).isoformat(timespec="seconds"),
    "host": platform.node(),
    "platform": platform.platform(),
    "machine": platform.machine(),
    "logical_cpus": os.cpu_count(),
    "cargo_build_jobs": os.environ.get("CARGO_BUILD_JOBS"),
    "resource_envelope": resource_envelope(),
    "sail_commit": command("git", "rev-parse", "HEAD"),
    "rust": command("rustc", "--version"),
    "rustfmt": command("rustup", "run", os.environ.get("SAIL_EXTENSION_FMT_TOOLCHAIN", "nightly-2026-05-28"), "rustfmt", "--version"),
    "python": sys.version,
    "modes": ["local", "local-cluster", "process-cluster"],
    "native_plugins": "separate installed wheels; driver-resident Nutmeg, independently loaded worker Sedona",
    "package_identities": identities,
    "worker_processes": processes,
    "extension_sources": {
        "sedonadb_commit": command("git", "-C", "examples/extensions/sedona/.deps/sedona-db", "rev-parse", "HEAD"),
        "sedonadb_patch_sha256": sha256(repo / "examples/extensions/sedona/patches/datafusion-55.patch"),
        "nutmeg_upstream_commit": "f267b03659dd536981f98420944f911b667632b7",
        "nutmeg_local_changes": "vendored source belongs to sail_commit",
    },
    "input_partition_fixture": {"nodes": 4, "edges": 4, "rows_each": 3, "includes_empty_partition": True},
    "host_binary_sha256": sha256(args.target / "host/debug/sail"),
    "source_locks": {str(path.relative_to(repo)): sha256(path) for path in
                     [repo / "Cargo.lock", repo / "examples/extensions/sedona/Cargo.lock",
                      repo / "examples/extensions/nutmeg/Cargo.lock",
                      repo / "examples/extensions/vendor/nutmeg-graph/Cargo.lock",
                      repo / "examples/extensions/requirements.lock"]},
    "wheels": {path.name: sha256(path) for path in sorted((args.target / "wheels").glob("*.whl"))},
    "python_distributions": {name: metadata.version(name) for name in
                             ["pyspark", "pyarrow", "apache-sedona", "sail-sedona-extension", "sail-nutmeg"]},
    "installed_native_files": {name: native_files(name) for name in
                               ["sail-sedona-extension", "sail-nutmeg"]},
    "cancellation_stress": json.loads((args.target / "cancellation-stress.json").read_text()),
    "snapshot_stress": json.loads((args.target / "snapshot-stress.json").read_text()),
    "sedona_native_dependencies": json.loads((args.target / "sedona-native-dependencies.json").read_text()),
    "outcome": "passed local and distributed native extension gates",
    "boundaries": {
        "distributed_extensions": "this gate covers actor and separate-process workers on one host; two-host evidence is recorded separately; Kubernetes is not qualified",
        "graph_tables": "ordinary DataFusion joins/aggregations execute on workers without native staging or CSR; staged Arrow scans/native kernels remain driver-resident",
        "memory_admission": "native session quotas reserve from the shared same-config host pool with experimental extensions enabled; quotas are nonspillable, and unbounded configuration remains unbounded; not an RSS cap",
        "sedona_indexed_join": "not implemented; baseline spatial SQL correctness tested",
        "geometry_client_udt": "not qualified; results collected as scalars/text",
        "kernel_cancellation": "native FFI retained-output lifetime and client-driven kernel/accounting tests; fresh projection build and final canonical sort remain synchronous",
        "mutation_replay": "driver-native regions have one attempt; acknowledgement loss is indeterminate; a new request is a new operation",
    },
}
args.output.parent.mkdir(parents=True, exist_ok=True)
args.output.write_text(json.dumps(receipt, indent=2) + "\n")
print(f"Evidence: {args.output}")
