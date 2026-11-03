#!/usr/bin/env python3
"""Foreground worker launcher for SAIL_EXPERIMENTAL_WORKER_COMMAND.

Only trusted startup JSON specifies executable paths and SSH hosts. Remote
stdin carries heartbeats for the worker lifetime; launcher death closes that
lease and a broken network expires it on the remote host.
"""
import argparse
import json
import os
from pathlib import Path
import shlex
import signal
import subprocess
import sys
import threading


def supervised_command(target):
    helper = str(Path(target["repo"]) / "examples/extensions/scripts/two_host_remote.py")
    argv = [target["python"], helper]
    if target.get("ssh"):
        return ["ssh", "-T", "-o", "BatchMode=yes", "-o", "ConnectTimeout=10",
                target["ssh"], shlex.join(["env", "-u", "PYTHONHOME", "-u", "PYTHONPATH",
                                          "-u", "DYLD_LIBRARY_PATH", *argv])]
    return argv


def clean_python_environment():
    env = dict(os.environ)
    for key in ("PYTHONHOME", "PYTHONPATH", "DYLD_LIBRARY_PATH", "LD_LIBRARY_PATH"):
        env.pop(key, None)
    return env


def launch(target, argv, environment, stdout=None):
    process = subprocess.Popen(supervised_command(target), stdin=subprocess.PIPE,
                               stdout=stdout, stderr=subprocess.STDOUT,
                               env=clean_python_environment())
    request = dict(version=1, argv=argv, environment=environment, cwd=target["repo"])
    if "environment_file" in target:
        # Keep credentials on their host: the wire request and receipts contain
        # only a trusted configuration path, never the file's contents.
        request["environment_file"] = target["environment_file"]
    try:
        process.stdin.write(json.dumps(request).encode() + b"\n")
        process.stdin.flush()
    except BaseException:
        stop(process)
        raise
    lease_stop = threading.Event()

    def heartbeat():
        while not lease_stop.wait(2):
            try:
                process.stdin.write(b"\n")
                process.stdin.flush()
            except (OSError, ValueError):
                return

    process.sail_lease_stop = lease_stop
    process.sail_lease_thread = threading.Thread(target=heartbeat, daemon=True)
    process.sail_lease_thread.start()
    return process


def stop(process):
    # Even SIGKILL of this launcher closes the pipe; the remote supervisor then
    # terminates its worker group. On the normal path wait for its acknowledgement.
    if hasattr(process, "sail_lease_stop"):
        process.sail_lease_stop.set()
        process.sail_lease_thread.join(timeout=1)
    if process.stdin is not None:
        process.stdin.close()
    try:
        process.wait(timeout=15)
    except subprocess.TimeoutExpired:
        process.terminate()
        try:
            process.wait(timeout=5)
        except subprocess.TimeoutExpired:
            process.kill()
            process.wait(timeout=5)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--targets-json", required=True)
    args = parser.parse_args()
    targets = json.loads(args.targets_json)
    if not isinstance(targets, list) or not targets:
        raise ValueError("at least one worker target is required")
    worker = int(os.environ["SAIL_CLUSTER__WORKER_ID"])
    target = targets[worker % len(targets)]
    env = {key: value for key, value in os.environ.items()
           if key.startswith(("SAIL_CLUSTER__", "SAIL_EXECUTION__"))
           or key in ("SAIL_EXPERIMENTAL_EXTENSIONS", "RUST_LOG")}
    env.update(SAIL_CLUSTER__WORKER_LISTEN_HOST="0.0.0.0",
               SAIL_CLUSTER__WORKER_EXTERNAL_HOST=target["advertise"],
               SAIL_CLUSTER__WORKER_LISTEN_PORT=str(target.get("port", 0)),
               SAIL_CLUSTER__WORKER_EXTERNAL_PORT=str(target.get("port", 0)))
    process = launch(target, [target["sail"], "worker"], env)
    try:
        return process.wait()
    finally:
        stop(process)


def interrupted(_number, _frame):
    raise KeyboardInterrupt


if __name__ == "__main__":
    signal.signal(signal.SIGTERM, interrupted)
    try:
        sys.exit(main())
    except KeyboardInterrupt:
        sys.exit(130)
