"""Shutdown errors must not turn into successful two-host qualifications."""

import json
from pathlib import Path
from types import SimpleNamespace
import sys
from unittest.mock import Mock

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import two_host
import two_host_remote


@pytest.mark.parametrize("rows, expected", [
    (" 10 10\n 20 20\n", False),
    (" 10 10\n 456 123\n", True),
])
def test_permission_error_uses_process_group_membership(monkeypatch, rows, expected):
    monkeypatch.setattr(two_host_remote.os, "killpg", Mock(side_effect=PermissionError()))
    process_table = Mock(return_value=rows)
    monkeypatch.setattr(two_host_remote.subprocess, "check_output", process_table)
    assert two_host_remote.group_exists(123) is expected
    process_table.assert_called_once_with(["ps", "-axo", "pid=,pgid="], text=True, timeout=5)


def test_transient_permission_failure_does_not_abort_or_hide_live_descendants(monkeypatch):
    signals = []

    def killpg(pgid, sig):
        signals.append((pgid, sig))
        if sig == 0:
            raise PermissionError()

    monkeypatch.setattr(two_host_remote.os, "killpg", killpg)
    monkeypatch.setattr(two_host_remote.subprocess, "check_output", Mock(side_effect=[
        "456 123\n", "789 789\n",
    ]))
    monkeypatch.setattr(two_host_remote.time, "monotonic", Mock(side_effect=[0, 1, 2]))
    sleep = Mock()
    monkeypatch.setattr(two_host_remote.time, "sleep", sleep)
    process = SimpleNamespace(pid=123, poll=Mock(), wait=Mock())
    two_host_remote.terminate(process)
    assert signals == [(123, two_host_remote.signal.SIGINT), (123, 0), (123, 0)]
    assert process.poll.call_count == 2
    sleep.assert_called_once_with(0.05)


def test_failed_process_table_probe_does_not_claim_group_absent(monkeypatch):
    monkeypatch.setattr(two_host_remote.os, "killpg", Mock(side_effect=PermissionError()))
    monkeypatch.setattr(two_host_remote.subprocess, "check_output", Mock(side_effect=OSError("ps unavailable")))
    with pytest.raises(OSError, match="ps unavailable"):
        two_host_remote.group_exists(123)


def test_nonzero_supervisor_exit_fails_even_when_algorithm_and_pid_checks_pass(tmp_path, monkeypatch):
    config = tmp_path / "config.json"
    config.write_text(json.dumps({
        "driver": {"python": sys.executable, "repo": str(tmp_path), "sail": "sail",
                   "advertise": "host-a", "gateway_port": 50152, "connect_port": 50151},
        "workers": [{"advertise": "host-a"}, {"advertise": "host-b"}],
    }))
    output = tmp_path / "result"
    monkeypatch.setattr(sys, "argv", ["two_host.py", "--config", str(config), "--output", str(output)])
    inventory = dict(source_commit="fixed", source_dirty="", binary_sha256="same",
                     packages={"sedona": "same", "nutmeg": "same"})
    monkeypatch.setattr(two_host, "inventory", lambda target: dict(inventory, host=target["advertise"]))
    monkeypatch.setattr(two_host, "launch", Mock(return_value=SimpleNamespace(returncode=1)))
    monkeypatch.setattr(two_host, "wait_server", Mock())
    monkeypatch.setattr(two_host, "stop", Mock())

    def exercise(_endpoint, _hosts, evidence):
        evidence["worker_endpoints"] = [dict(worker_id=1, host="host-a"), dict(worker_id=2, host="host-b")]

    monkeypatch.setattr(two_host, "exercise", exercise)
    monkeypatch.setattr(two_host, "completed_worker_tasks", Mock(return_value=[dict(worker_id=1), dict(worker_id=2)]))
    cleanup = Mock(return_value={"host-a": [dict(pid=123, alive=False)], "host-b": [dict(pid=456, alive=False)]})
    monkeypatch.setattr(two_host, "process_cleanup", cleanup)
    with pytest.raises(AssertionError, match="driver supervisor failed during shutdown: 1"):
        two_host.main()
    cleanup.assert_called_once()
    receipt = json.loads((output / "receipt.json").read_text())
    assert receipt["outcome"] == "failed"
    assert receipt["driver_supervisor_returncode"] == 1
    assert receipt["process_cleanup"] == cleanup.return_value
