"""Fresh Sail server per benchmark run.

One helper per run: start ``sail spark server`` with the fair memory pool, NVM
temporary files and (optionally) the SMJ join preference plus the Nutmeg
extension environment, wait for the Spark Connect port, and shut the process
group down afterwards.

The environment mirrors the Sail configuration reference
(`docs/guide/configuration`): every option is an env var
``SAIL_<SECTION>__<KEY>`` with a TOML-style value.
"""

from __future__ import annotations

import contextlib
import os
import re
import signal
import socket
import subprocess
import sys
import sysconfig
import time
from pathlib import Path

_UNITS = {"": 1, "K": 1024, "M": 1024**2, "G": 1024**3, "T": 1024**4}


def parse_memory(value: str) -> int:
    """Parse "24G" / "512M" / "268435456" into bytes."""
    match = re.fullmatch(r"\s*(\d+(?:\.\d+)?)\s*([KMGT]?)I?B?\s*", value, re.IGNORECASE)
    if not match:
        raise ValueError(f"invalid memory value: {value!r} (expected e.g. 24G or 512M)")
    number, unit = match.groups()
    return int(float(number) * _UNITS[unit.upper()])


class SailServer:
    """One Sail Spark Connect server in local mode."""

    def __init__(
        self,
        binary: Path,
        workdir: Path,
        *,
        max_memory: str,
        use_smj: bool,
        partitions: int,
        threads: int,
        log_path: Path,
        load_extensions: bool,
        nutmeg_memory: str = "8G",
        port: int = 0,
    ):
        self.binary = Path(binary).resolve()
        self.workdir = Path(workdir).resolve()
        self.log_path = log_path
        self.port = port
        self.process: subprocess.Popen | None = None
        self.endpoint: str | None = None
        spill_dir = self.workdir / "spill"
        spill_dir.mkdir(parents=True, exist_ok=True)

        env = dict(os.environ)
        # Keep worker-related knobs from leaking into local mode.
        for name in (
            "SAIL_INTERNAL__RUN_PYTHON",
            "SAIL_EXPERIMENTAL_WORKER_COMMAND",
            "SAIL_EXPERIMENTAL_WORKER_PYTHONPATH",
        ):
            env.pop(name, None)
        env.update(
            SAIL_MODE="local",
            # runtime.memory_pool.type = "fair"
            SAIL_RUNTIME__MEMORY_POOL__TYPE="fair",
            # runtime.memory_pool.fair.max_size, passed from main.py --max-memory
            SAIL_RUNTIME__MEMORY_POOL__FAIR__MAX_SIZE=str(parse_memory(max_memory)),
            # runtime.temporary_files.paths, passed from main.py --work-dir (fast NVM)
            SAIL_RUNTIME__TEMPORARY_FILES__PATHS=f'["{spill_dir}"]',
            SAIL_EXECUTION__DEFAULT_PARALLELISM=str(partitions),
            TOKIO_WORKER_THREADS=str(threads),
            RAYON_NUM_THREADS=str(threads),
            RUST_LOG="info",
        )
        # optimizer.prefer_hash_join = false when --use-smj is passed; the Sail
        # default (hash join preferred) is kept otherwise.
        if use_smj:
            env["SAIL_OPTIMIZER__PREFER_HASH_JOIN"] = "false"
        # The sail binary embeds Python (pyo3); it must see the same venv this
        # script runs from, so the interpreter env is always set.
        libdir = sysconfig.get_config_var("LIBDIR") or ""
        env.update(
            PYTHONHOME=sys.base_prefix,
            PYTHONPATH=sysconfig.get_paths()["purelib"],
            LD_LIBRARY_PATH=libdir,
            DYLD_LIBRARY_PATH=libdir,
        )
        if load_extensions:
            env.update(
                SAIL_EXPERIMENTAL_EXTENSIONS="1",
                SAIL_NUTMEG_MEMORY_BYTES=str(parse_memory(nutmeg_memory)),
            )
        self.env = env

    def start(self, timeout: float = 120.0) -> str:
        if self.port == 0:
            with socket.socket() as listener:
                listener.bind(("127.0.0.1", 0))
                self.port = listener.getsockname()[1]
        with self.log_path.open("w") as log:
            self.process = subprocess.Popen(
                [
                    str(self.binary),
                    "spark",
                    "server",
                    "--ip",
                    "127.0.0.1",
                    "--port",
                    str(self.port),
                ],
                env=self.env,
                cwd=str(self.workdir),
                stdout=log,
                stderr=subprocess.STDOUT,
                start_new_session=True,
            )
        deadline = time.monotonic() + timeout
        while True:
            if self.process.poll() is not None:
                raise RuntimeError(f"Sail exited during startup; see {self.log_path}")
            try:
                with socket.create_connection(("127.0.0.1", self.port), timeout=0.2):
                    break
            except OSError:
                if time.monotonic() >= deadline:
                    self.stop()
                    raise TimeoutError(f"Sail startup exceeded {timeout}s; see {self.log_path}")
                time.sleep(0.05)
        self.endpoint = f"sc://127.0.0.1:{self.port}"
        return self.endpoint

    def stop(self) -> None:
        if self.process is None:
            return
        try:
            os.killpg(self.process.pid, signal.SIGINT)
            self.process.wait(timeout=15)
        except (ProcessLookupError, subprocess.TimeoutExpired):
            with contextlib.suppress(ProcessLookupError, subprocess.TimeoutExpired):
                os.killpg(self.process.pid, signal.SIGKILL)
                self.process.wait(timeout=5)
        self.process = None
