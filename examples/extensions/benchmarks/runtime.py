"""Fresh benchmark server; each trial must run in its own container."""
import contextlib
import hashlib
from importlib.metadata import version
import importlib.util
import os
from pathlib import Path
import signal
import socket
import subprocess
import sys
import sysconfig
import time


def sha256(path):
    digest = hashlib.sha256()
    with Path(path).open('rb') as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b''):
            digest.update(chunk)
    return digest.hexdigest()


def git(repo, *args):
    return subprocess.check_output(['git', '-C', str(repo), *args], text=True).strip()


def package_versions():
    result = {}
    for name in ('pyspark', 'numpy', 'pyarrow', 'protobuf', 'sail-nutmeg',
                 'sail-sedona-extension', 'pyspark-pecan'):
        try:
            result[name] = version(name)
        except Exception:
            result[name] = 'not installed (source import may be used)'
    return result


def algorithm_method(engine, algorithm, variant):
    """Resolve explicit methods; a fused PageRank is not an available algorithm."""
    if engine not in ('pecan', 'nutmeg-native', 'nutmeg-datafusion'):
        raise ValueError(f'unknown graph engine: {engine}')
    methods = {
        'pagerank': {'reference': ('power', 'pagerank'),
                     'optimized': ('delta', 'pagerankDelta')},
        'wcc': {'reference': ('min_label', 'wcc'),
                'optimized': ('randomized', 'wccRandomized'),
                'fused': ('randomized_fused', 'wccRandomizedFused')},
    }
    try:
        return methods[algorithm][variant][engine == 'nutmeg-native']
    except KeyError as error:
        raise ValueError(f'unsupported graph method: {algorithm}/{variant}') from error


def native_package_identity():
    """Installed Python/native bytes, independent of unchanged version labels."""
    spec = importlib.util.find_spec('sail_nutmeg')
    root = Path(spec.origin).parent
    files = {str(path.relative_to(root)): sha256(path) for path in sorted(root.rglob('*'))
             if path.is_file() and path.suffix in {'.py', '.so', '.dylib', '.dll', '.pyd'}}
    return {'root': str(root), 'files_sha256': files}


def record_result_evidence(result, native, kernel, expected_rows, receipt):
    """Retain delivered output and the one native read before convergence checks."""
    receipt['result_files'] = [{'name': path.name, 'bytes': path.stat().st_size, 'sha256': sha256(path)}
                               for path in sorted(result.rglob('*.parquet'))]
    if native is not None:
        receipt['native_status_after'] = native.status()
        reads = [read for read in receipt['native_status_after']['reads']
                 if read['algorithm'] == kernel and read['graph'] == 'benchmark']
        assert len(reads) == 1 and reads[0]['state'] == 'finished' and reads[0]['rows'] == expected_rows, reads


def group_exists(pgid):
    try:
        os.killpg(pgid, 0)
        return True
    except ProcessLookupError:
        return False
    except PermissionError:
        # Some macOS smoke runs returned EPERM after the leader exited. Do not
        # infer absence from EPERM: require an independent process-table check.
        rows = subprocess.check_output(['ps', '-axo', 'pid=,pgid='], text=True, timeout=5)
        if any(line.split()[1] == str(pgid) for line in rows.splitlines() if len(line.split()) == 2):
            # Shutdown can leave a transient process-table entry after the
            # group stops accepting signals. Keep waiting within the existing
            # deadline; a persistent group still fails cleanup after SIGKILL.
            return True
        return False


def stop_group(process):
    """Stop descendants even when their original driver has already exited."""
    try:
        os.killpg(process.pid, signal.SIGINT)
    except ProcessLookupError:
        process.wait()
        return
    deadline = time.monotonic() + 15
    while time.monotonic() < deadline:
        process.poll()
        if not group_exists(process.pid):
            return
        time.sleep(0.05)
    try:
        os.killpg(process.pid, signal.SIGKILL)
    except ProcessLookupError:
        pass
    process.wait(timeout=5)
    deadline = time.monotonic() + 5
    while time.monotonic() < deadline:
        if not group_exists(process.pid):
            return
        time.sleep(0.05)
    raise RuntimeError('Sail process group remains after SIGKILL; use a container init to reap descendants')


def validate_admission_settings(worker_task_slots, sail_pool_bytes, native_quota):
    for name, value in (('worker_task_slots', worker_task_slots),
                        ('sail_pool_bytes', sail_pool_bytes), ('native_quota', native_quota)):
        if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
            raise ValueError(f'{name} must be a positive integer')
    if native_quota >= sail_pool_bytes:
        raise ValueError('native_quota must leave positive participating memory in sail_pool_bytes')


@contextlib.contextmanager
def server(binary, output, mode, partitions, threads, native_quota, cleanup_errors,
           *, worker_task_slots, sail_pool_bytes):
    validate_admission_settings(worker_task_slots, sail_pool_bytes, native_quota)
    staging = output / 'staging'
    staging.mkdir()
    with socket.socket() as listener:
        listener.bind(('127.0.0.1', 0))
        port = listener.getsockname()[1]
    env = dict(os.environ)
    for name in ('SAIL_INTERNAL__RUN_PYTHON', 'SAIL_EXPERIMENTAL_WORKER_COMMAND',
                 'SAIL_EXPERIMENTAL_WORKER_PYTHONPATH'):
        env.pop(name, None)
    env.update(
        PYTHONHOME=sys.base_prefix, PYTHONPATH=sysconfig.get_paths()['purelib'],
        LD_LIBRARY_PATH=sysconfig.get_config_var('LIBDIR') or '',
        DYLD_LIBRARY_PATH=sysconfig.get_config_var('LIBDIR') or '',
        SAIL_EXPERIMENTAL_EXTENSIONS='1',
        SAIL_MODE='local-cluster' if mode == 'process-cluster' else 'local',
        SAIL_EXPERIMENTAL_PROCESS_WORKERS='1' if mode == 'process-cluster' else '0',
        SAIL_CLUSTER__WORKER_INITIAL_COUNT='2', SAIL_CLUSTER__WORKER_MAX_COUNT='2',
        SAIL_CLUSTER__WORKER_TASK_SLOTS=str(worker_task_slots),
        SAIL_CLUSTER__TASK_MAX_ATTEMPTS='1',
        SAIL_EXECUTION__DEFAULT_PARALLELISM=str(partitions),
        SAIL_RUNTIME__MEMORY_POOL__TYPE='greedy',
        SAIL_RUNTIME__MEMORY_POOL__GREEDY__MAX_SIZE=str(sail_pool_bytes),
        SAIL_NUTMEG_MEMORY_BYTES=str(native_quota),
        SAIL_GRAPH_UTILS_ROOT=staging.as_uri(),
        TOKIO_WORKER_THREADS=str(threads), RAYON_NUM_THREADS=str(threads),
        RUST_LOG='info',
    )
    with (output / 'server.log').open('w') as log:
        process = subprocess.Popen([str(binary), 'spark', 'server', '--ip', '127.0.0.1', '--port', str(port)],
                                   env=env, cwd=output, stdout=log, stderr=subprocess.STDOUT,
                                   start_new_session=True)
        try:
            deadline = time.monotonic() + 120
            while True:
                if process.poll() is not None:
                    raise RuntimeError('Sail exited during startup; see server.log')
                try:
                    with socket.create_connection(('127.0.0.1', port), timeout=0.2):
                        break
                except OSError:
                    if time.monotonic() >= deadline:
                        raise TimeoutError('Sail startup exceeded 120s')
                    time.sleep(0.05)
            yield f'sc://127.0.0.1:{port}', process.pid
        finally:
            active_error = sys.exc_info()[1]
            try:
                stop_group(process)
            except BaseException as error:
                cleanup_errors.append({'operation': 'stop_server_group', 'error': repr(error)})
                if active_error is None:
                    raise
