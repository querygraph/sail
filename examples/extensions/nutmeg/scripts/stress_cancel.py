#!/usr/bin/env python3
"""Build and stress the release cancellation fixture with every logical CPU busy.

On macOS set PYO3_PYTHON and DYLD_LIBRARY_PATH as for normal native tests.
Use CARGO_TARGET_DIR or --target-dir to isolate build artifacts. Passing
--test-binary skips compilation and is reported explicitly as a prebuilt run.
"""
import argparse
import datetime
import hashlib
import json
import os
import socket
import subprocess
from pathlib import Path


def revision(directory):
    return subprocess.check_output(["git", "-C", str(directory), "rev-parse", "HEAD"], text=True).strip()


def source_identity(manifest):
    digest = hashlib.sha256()
    for directory in (manifest.parent, manifest.parent.parent / "vendor" / "nutmeg-graph"):
        for path in sorted(directory.rglob("*")):
            if not path.is_file() or any(part in {"target", "dist", "__pycache__", ".pytest_cache"} for part in path.parts) or path.suffix in {".so", ".pyc"}:
                continue
            if path.suffix not in {".rs", ".toml", ".lock", ".py", ".md"}:
                continue
            digest.update(str(path.relative_to(manifest.parent.parent)).encode())
            digest.update(path.read_bytes())
    return digest.hexdigest()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--manifest-path", type=Path, default=Path(__file__).resolve().parents[1] / "Cargo.toml")
    parser.add_argument("--target-dir", type=Path)
    parser.add_argument("--test-binary")
    parser.add_argument("--jobs", type=int, default=4)
    parser.add_argument("--runs", type=int, default=10)
    parser.add_argument("--test-name", default="tests::dropped_blocked_stream_cancels_and_keeps_exported_batch_valid")
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    if args.runs < 1 or args.jobs < 1:
        parser.error("--runs and --jobs must be positive")
    manifest = args.manifest_path.resolve()
    before = revision(manifest.parent)
    before_source = source_identity(manifest)
    report = {
        "started_at": datetime.datetime.now(datetime.timezone.utc).isoformat(),
        "host": socket.gethostname(), "logical_cpus": os.cpu_count(),
        "head_before": before, "source_before": before_source,
        "build_profile": "release", "prebuilt_binary": bool(args.test_binary),
        "runs": [],
    }
    args.output.parent.mkdir(parents=True, exist_ok=True)
    load = []
    try:
        binary = args.test_binary
        if not binary:
            command = ["cargo", "test", "--release", "--lib", "--no-run", "--locked", "--manifest-path", str(manifest), "--message-format=json-render-diagnostics"]
            env = os.environ.copy()
            env["CARGO_INCREMENTAL"] = "0"
            env["CARGO_BUILD_JOBS"] = str(args.jobs)
            if args.target_dir:
                env["CARGO_TARGET_DIR"] = str(args.target_dir.resolve())
            report["build_command"] = command
            report["cargo_target_dir"] = env.get("CARGO_TARGET_DIR")
            built = subprocess.run(command, env=env, text=True, capture_output=True)
            build_log = args.output.with_suffix(".build.log")
            build_log.write_text(built.stdout + "\n" + built.stderr)
            report["build_log"] = str(build_log)
            report["build_exit_code"] = built.returncode
            if built.returncode:
                raise RuntimeError(f"release build failed; see {build_log}")
            for line in built.stdout.splitlines():
                try:
                    event = json.loads(line)
                except ValueError:
                    continue
                if event.get("reason") == "compiler-artifact" and event.get("profile", {}).get("test") and event.get("executable"):
                    binary = event["executable"]
            if not binary:
                raise RuntimeError("cargo did not report a test executable")
        if "release" not in Path(binary).parts:
            raise RuntimeError("expected a release-profile test executable")
        command = [binary, args.test_name, "--exact", "--nocapture"]
        report["command"] = command

        def run(mode, index):
            try:
                result = subprocess.run(command, text=True, capture_output=True, timeout=90)
            except subprocess.TimeoutExpired as error:
                report["runs"].append({"mode": mode, "index": index, "outcome": "timeout", "message": str(error)})
                raise
            report["runs"].append({"mode": mode, "index": index, "exit_code": result.returncode, "stdout": result.stdout, "stderr": result.stderr})
            print(mode, index, result.returncode, flush=True)
            if result.returncode:
                raise RuntimeError(result.stdout + result.stderr)
            if "1 passed" not in result.stdout or "0 failed" not in result.stdout:
                raise RuntimeError("stress filter did not execute exactly one passing test")

        run("unsaturated", 0)
        for _ in range(os.cpu_count() or 1):
            load.append(subprocess.Popen(["yes"], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL))
        for index in range(args.runs):
            run("one_yes_per_logical_cpu", index)
    finally:
        for child in load:
            child.terminate()
        for child in load:
            child.wait(timeout=10)
        report["finished_at"] = datetime.datetime.now(datetime.timezone.utc).isoformat()
        report["head_after"] = revision(manifest.parent)
        report["source_after"] = source_identity(manifest)
        report["unchanged"] = before == report["head_after"] and before_source == report["source_after"]
        args.output.write_text(json.dumps(report, indent=2) + "\n")
    if not report["unchanged"]:
        raise RuntimeError("HEAD or source changed during stress run; receipt is not a verdict")


if __name__ == "__main__":
    main()
