"""Selected capacity must reach the driver and both physical worker launchers."""
import json
from pathlib import Path
from types import SimpleNamespace
import sys

import pytest

SCRIPTS = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(SCRIPTS))
import two_host
import two_host_worker


@pytest.mark.parametrize('value', ['0', '-1', '1.5', 'invalid'])
def test_invalid_capacity_is_rejected_before_config_or_process_access(value, monkeypatch, capsys):
    monkeypatch.setattr(sys, 'argv', ['two_host.py', '--config', '/nonexistent-config.json',
                                     '--output', '/unused-output', '--worker-task-slots', value])
    with pytest.raises(SystemExit) as error:
        two_host.main()
    assert error.value.code == 2
    assert '--worker-task-slots: must be a positive integer' in capsys.readouterr().err


@pytest.mark.parametrize('option,expected', [(None, '2'), ('4', '4')])
def test_capacity_is_recorded_and_forwarded_to_driver_and_each_worker(option, expected, tmp_path, monkeypatch):
    driver = dict(repo=str(SCRIPTS.parents[2]), python=sys.executable, sail='/unused-sail',
                  advertise='192.0.2.1', connect_port=50051, gateway_port=50052)
    workers = [dict(driver, advertise=host, port=port)
               for host, port in [('192.0.2.1', 50161), ('192.0.2.2', 50162)]]
    config = tmp_path / 'config.json'
    config.write_text(json.dumps(dict(driver=driver, workers=workers)))
    output = tmp_path / 'output'
    argv = ['two_host.py', '--config', str(config), '--output', str(output)]
    if option is not None:
        argv += ['--worker-task-slots', option]
    monkeypatch.setattr(sys, 'argv', argv)
    monkeypatch.setattr(two_host, 'inventory', lambda target: dict(
        source_commit='same-source', source_dirty=False, host=target['advertise'],
        binary_sha256='same-binary', packages=dict(sedona='same-sedona', nutmeg='same-nutmeg')))
    captured = {}

    class DriverLaunchObserved(Exception):
        pass

    def capture_driver(target, command, environment, stdout):
        captured.update(target=target, command=command, environment=environment)
        raise DriverLaunchObserved()

    monkeypatch.setattr(two_host, 'launch', capture_driver)
    with pytest.raises(DriverLaunchObserved):
        two_host.main()
    assert captured['environment']['SAIL_CLUSTER__WORKER_TASK_SLOTS'] == expected
    assert captured['environment']['SAIL_CLUSTER__WORKER_INITIAL_COUNT'] == '2'
    receipt = json.loads((output / 'receipt.json').read_text())
    assert receipt['worker_task_slots'] == int(expected)

    # Exercise the real worker launcher main(), including its environment
    # allowlist. Stop immediately at the process-launch boundary, not SSH.
    launcher = json.loads(captured['environment']['SAIL_EXPERIMENTAL_WORKER_COMMAND'])
    for name, value in captured['environment'].items():
        monkeypatch.setenv(name, value)
    monkeypatch.setattr(two_host_worker, 'stop', lambda process: None)
    worker_requests = []

    def capture_worker(target, command, environment):
        worker_requests.append((target, command, environment))
        return SimpleNamespace(wait=lambda: 0)

    monkeypatch.setattr(two_host_worker, 'launch', capture_worker)
    for worker_id in (1, 2):
        monkeypatch.setenv('SAIL_CLUSTER__WORKER_ID', str(worker_id))
        monkeypatch.setattr(sys, 'argv', launcher[1:])
        assert two_host_worker.main() == 0
    assert {request[0]['advertise'] for request in worker_requests} == {'192.0.2.1', '192.0.2.2'}
    assert all(request[2]['SAIL_CLUSTER__WORKER_TASK_SLOTS'] == expected
               for request in worker_requests)
