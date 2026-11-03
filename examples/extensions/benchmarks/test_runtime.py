"""Regression checks for shutdown races observed in process-worker pilots."""
import signal
from contextlib import nullcontext
from types import SimpleNamespace

import pytest

import runtime


def test_explicit_fused_wcc_is_distinct_and_never_a_pagerank_alias():
    assert runtime.algorithm_method('pecan', 'wcc', 'fused') == 'randomized_fused'
    assert runtime.algorithm_method('nutmeg-datafusion', 'wcc', 'fused') == 'randomized_fused'
    assert runtime.algorithm_method('nutmeg-native', 'wcc', 'fused') == 'wccRandomizedFused'
    assert runtime.algorithm_method('nutmeg-native', 'wcc', 'optimized') == 'wccRandomized'
    assert runtime.algorithm_method('pecan', 'wcc', 'optimized') == 'randomized'
    for engine in ('pecan', 'nutmeg-native', 'nutmeg-datafusion'):
        with pytest.raises(ValueError, match='unsupported graph method: pagerank/fused'):
            runtime.algorithm_method(engine, 'pagerank', 'fused')


def test_permission_denied_with_member_keeps_waiting(monkeypatch):
    def denied(*_):
        raise PermissionError(1, 'Operation not permitted')
    monkeypatch.setattr(runtime.os, 'killpg', denied)
    monkeypatch.setattr(runtime.subprocess, 'check_output', lambda *a, **k: ' 123 77\n')
    assert runtime.group_exists(77)


def test_permission_denied_requires_independent_absence(monkeypatch):
    def denied(*_):
        raise PermissionError(1, 'Operation not permitted')
    monkeypatch.setattr(runtime.os, 'killpg', denied)
    monkeypatch.setattr(runtime.subprocess, 'check_output', lambda *a, **k: ' 123 78\n')
    assert not runtime.group_exists(77)


def test_exited_driver_does_not_skip_descendant_shutdown(monkeypatch):
    calls = []
    monkeypatch.setattr(runtime.os, 'killpg', lambda *args: calls.append(args))
    clock = iter([0, 16, 16, 17])
    monkeypatch.setattr(runtime.time, 'monotonic', lambda: next(clock))
    monkeypatch.setattr(runtime, 'group_exists', lambda _: False)
    process = SimpleNamespace(pid=77, poll=lambda: 0, wait=lambda **kwargs: 0)
    runtime.stop_group(process)
    assert calls == [(77, signal.SIGINT), (77, signal.SIGKILL)]


def test_server_delivers_independent_task_and_memory_admission_settings(tmp_path, monkeypatch):
    launched = []
    monkeypatch.setenv('SAIL_CLUSTER__WORKER_TASK_SLOTS', '1')
    monkeypatch.setenv('SAIL_RUNTIME__MEMORY_POOL__GREEDY__MAX_SIZE', '1')
    monkeypatch.setenv('SAIL_NUTMEG_MEMORY_BYTES', '1')
    def launch(*args, **kwargs):
        launched.append(kwargs['env'])
        return SimpleNamespace(pid=77, poll=lambda: None)
    monkeypatch.setattr(runtime.subprocess, 'Popen', launch)
    monkeypatch.setattr(runtime.socket, 'create_connection', lambda *args, **kwargs: nullcontext())
    stopped = []
    monkeypatch.setattr(runtime, 'stop_group', lambda process: stopped.append(process.pid))
    with runtime.server(tmp_path / 'sail', tmp_path, 'process-cluster', 8, 2, 3 * 1024**3, [],
                        worker_task_slots=24, sail_pool_bytes=9 * 1024**3):
        env, = launched
        assert env['SAIL_CLUSTER__WORKER_TASK_SLOTS'] == '24'
        assert env['SAIL_CLUSTER__WORKER_INITIAL_COUNT'] == env['SAIL_CLUSTER__WORKER_MAX_COUNT'] == '2'
        assert env['TOKIO_WORKER_THREADS'] == env['RAYON_NUM_THREADS'] == '2'
        assert int(env['SAIL_RUNTIME__MEMORY_POOL__GREEDY__MAX_SIZE']) == 9 * 1024**3
        assert int(env['SAIL_NUTMEG_MEMORY_BYTES']) == 3 * 1024**3
    assert stopped == [77]


@pytest.mark.parametrize('slots,pool,quota', [(0, 10, 4), (True, 10, 4), (2, 0, 4), (2, 10, 0), (2, 10, 10), (2, 10, 11)])
def test_invalid_admission_is_rejected_before_creating_server_files(tmp_path, slots, pool, quota):
    with pytest.raises(ValueError):
        with runtime.server(tmp_path / 'sail', tmp_path, 'process-cluster', 8, 2, quota, [],
                            worker_task_slots=slots, sail_pool_bytes=pool):
            pytest.fail('invalid admission reached server startup')
    assert list(tmp_path.iterdir()) == []


def test_completed_nonconverged_native_read_keeps_status_and_output_evidence(tmp_path):
    path = tmp_path / 'part.parquet'
    path.write_bytes(b'completed output bytes')
    read = dict(algorithm='pagerankDelta', graph='benchmark', state='finished', rows=128,
                diagnostics={'iterations': 1, 'converged': False, 'residual': 0.1})
    calls = []
    def status():
        calls.append('status')
        return {'reads': [read]}
    receipt = {}
    runtime.record_result_evidence(tmp_path, SimpleNamespace(status=status), 'pagerankDelta', 128, receipt)
    assert calls == ['status']
    assert receipt['native_status_after']['reads'] == [read]
    assert receipt['result_files'] == [{'name': path.name, 'bytes': path.stat().st_size, 'sha256': runtime.sha256(path)}]


def test_duplicate_native_execution_cannot_pass_evidence_check(tmp_path):
    read = dict(algorithm='pagerankDelta', graph='benchmark', state='finished', rows=128)
    with pytest.raises(AssertionError):
        runtime.record_result_evidence(tmp_path, SimpleNamespace(status=lambda: {'reads': [read, read]}),
                                       'pagerankDelta', 128, {})
