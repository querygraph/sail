#!/usr/bin/env python3
"""Supervise one trusted Sail command with a bounded stdin heartbeat lease.

Invoked locally or through SSH by two_host.py / two_host_worker.py. The first
stdin line is versioned JSON, never shell source; remaining stdin is a lease.
"""
import json
import os
from pathlib import Path
import platform
import select
import signal
import subprocess
import sys
import sysconfig
import time

MAX_REQUEST = 128 * 1024


def storage_environment(filename):
    """Read host-local object-store settings without admitting process controls."""
    if not isinstance(filename, str) or "\0" in filename or not Path(filename).is_absolute():
        raise ValueError("environment_file must be an absolute path")
    with Path(filename).open("rb") as stream:
        data = stream.read(MAX_REQUEST + 1)
    if len(data) > MAX_REQUEST:
        raise ValueError("environment_file is too large")
    configured = json.loads(data)
    if not isinstance(configured, dict) or not all(
        isinstance(key, str) and isinstance(value, str)
        and "=" not in key and "\0" not in key and "\0" not in value
        and (key.startswith(("AWS_", "AZURE_", "GOOGLE_"))
             or key == "SAIL_GRAPH_UTILS_ROOT")
        for key, value in configured.items()
    ):
        raise ValueError("environment_file may contain only object-store settings and SAIL_GRAPH_UTILS_ROOT")
    return configured


def event(name, **fields):
    print(json.dumps(dict(event=name, hostname=platform.node(), **fields)), flush=True)


def group_exists(pgid):
    try:
        os.killpg(pgid, 0)
        return True
    except ProcessLookupError:
        return False
    except PermissionError:
        # macOS can return EPERM for a group whose leader has just exited.
        # Check the process table: permission failure alone proves no absence.
        rows = subprocess.check_output(["ps", "-axo", "pid=,pgid="], text=True, timeout=5)
        return any(line.split()[1] == str(pgid) for line in rows.splitlines()
                   if len(line.split()) == 2)


def terminate(process):
    if process is None:
        return
    try:
        # Sail handles SIGINT for graceful server shutdown. The whole isolated
        # group includes launchers, so even a crashed driver cannot orphan them.
        os.killpg(process.pid, signal.SIGINT)
    except ProcessLookupError:
        process.wait()
        return
    deadline = time.monotonic() + 5
    while time.monotonic() < deadline:
        process.poll()  # Reap the leader even while its descendants are closing.
        if not group_exists(process.pid):
            return
        time.sleep(0.05)
    try:
        os.killpg(process.pid, signal.SIGKILL)
    except ProcessLookupError:
        pass
    process.wait(timeout=5)


def main():
    line = sys.stdin.buffer.readline(MAX_REQUEST + 1)
    if len(line) > MAX_REQUEST:
        raise ValueError("launcher request too large")
    request = json.loads(line)
    if request.get("version") != 1:
        raise ValueError("unsupported launcher protocol version")
    argv = request["argv"]
    if not isinstance(argv, list) or not argv or not all(
        isinstance(arg, str) and "\0" not in arg for arg in argv
    ) or not argv[0]:
        raise ValueError("argv must be a nonempty array of literal strings")
    configured = request.get("environment", {})
    if not isinstance(configured, dict) or not all(
        isinstance(key, str) and isinstance(value, str) and "=" not in key
        and "\0" not in key and "\0" not in value
        for key, value in configured.items()
    ):
        raise ValueError("invalid worker environment")
    lease_seconds = request.get("lease_seconds", 15)
    if not isinstance(lease_seconds, (int, float)) or not 1 <= lease_seconds <= 300:
        raise ValueError("lease_seconds must be between 1 and 300")
    env = dict(os.environ)
    if "environment_file" in request:
        env.update(storage_environment(request["environment_file"]))
    env.update(configured)
    env.pop("SAIL_INTERNAL__RUN_PYTHON", None)
    # Derive these from THIS host/interpreter, never the driver's paths.
    env.update(PYTHONHOME=sys.base_prefix,
               PYTHONPATH=sysconfig.get_paths()["purelib"])
    library = sysconfig.get_config_var("LIBDIR") or ""
    if sys.platform == "darwin":
        env["DYLD_LIBRARY_PATH"] = library
    else:
        env["LD_LIBRARY_PATH"] = library
    process = None
    try:
        process = subprocess.Popen(argv, cwd=request.get("cwd"), env=env,
                                   stdin=subprocess.DEVNULL, start_new_session=True)
        event("sail_remote_started", pid=process.pid, architecture=platform.machine(),
              worker_id=env.get("SAIL_CLUSTER__WORKER_ID"), argv=argv)
        last_heartbeat = time.monotonic()
        while process.poll() is None:
            readable, _, _ = select.select([sys.stdin.buffer], [], [], 0.2)
            if readable:
                if not os.read(sys.stdin.fileno(), 4096):
                    event("sail_remote_lease_closed", pid=process.pid)
                    break
                last_heartbeat = time.monotonic()
            if time.monotonic() - last_heartbeat >= lease_seconds:
                event("sail_remote_lease_expired", pid=process.pid)
                break
        return process.poll() or 0
    finally:
        terminate(process)
        if process is not None:
            event("sail_remote_stopped", pid=process.pid, returncode=process.returncode)


def interrupted(_number, _frame):
    raise KeyboardInterrupt


if __name__ == "__main__":
    signal.signal(signal.SIGTERM, interrupted)
    try:
        sys.exit(main())
    except KeyboardInterrupt:
        sys.exit(130)
