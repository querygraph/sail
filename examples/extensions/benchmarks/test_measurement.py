"""Deterministic measurement regressions; no Linux host or live sampler is needed."""

import json
from pathlib import Path

import pytest

import measurement


STATUS = """Name:\tsail
Umask:\t0022
State:\tS (sleeping)
Tgid:\t123
Pid:\t123
VmSize:\t16384 kB
VmRSS:\t4096 kB
Threads:\t8
"""

ROLLUP = """00400000-7ffffffff000 ---p 00000000 00:00 0 [rollup]
Rss:                4096 kB
Pss:                3072 kB
Pss_Dirty:          2048 kB
Shared_Clean:       1024 kB
Private_Dirty:      2048 kB
Swap:                 0 kB
"""


@pytest.fixture
def proc_root(tmp_path, monkeypatch):
    root = tmp_path / "proc"
    root.mkdir()
    (root / "stat").write_text("cpu  100 0 20 200 0 0 0 5 10 0\n")
    cgroup = tmp_path / "cgroup"
    cgroup.mkdir()
    (cgroup / "memory.current").write_text("8192\n")

    def path(value):
        candidate = Path(value)
        if candidate == Path("/proc") or Path("/proc") in candidate.parents:
            return root / candidate.relative_to("/proc")
        return candidate

    monkeypatch.setattr(measurement, "Path", path)
    monkeypatch.setattr(measurement, "CGROUP", cgroup)
    return root


@pytest.mark.parametrize("text, expected", [
    (STATUS, {"VmRSS": 4096, "VmSize": 16384, "Threads": 8}),
    (ROLLUP, {"Rss": 4096, "Pss": 3072, "Swap": 0}),
])
def test_linux_counter_headers_and_text_fields_are_not_numbers(tmp_path, text, expected):
    path = tmp_path / "counters"
    path.write_text(text)
    actual = measurement.counters(path)
    assert {key: actual[key] for key in expected} == expected
    assert "Name" not in actual
    assert "State" not in actual
    assert "00400000-7ffffffff000" not in actual


def test_process_memory_converts_linux_kibibytes(proc_root):
    process = proc_root / "123"
    process.mkdir()
    (process / "status").write_text(STATUS)
    (process / "smaps_rollup").write_text(ROLLUP)
    (process / "comm").write_text("sail\n")
    assert measurement.process_memory() == [{
        "pid": 123, "name": "sail", "rss_bytes": 4096 * 1024,
        "pss_bytes": 3072 * 1024,
    }]


@pytest.mark.parametrize("failure", ["missing", "unreadable", "missing_counter"])
def test_unavailable_process_pss_is_unknown_not_zero(proc_root, monkeypatch, failure):
    process = proc_root / "123"
    process.mkdir()
    (process / "status").write_text(STATUS)
    (process / "comm").write_text("sail\n")
    rollup = process / "smaps_rollup"
    if failure == "missing_counter":
        rollup.write_text("Rss: 4096 kB\n")
    elif failure == "unreadable":
        rollup.write_text(ROLLUP)
        original = measurement.read_text
        # Permission-dependent chmod tests behave differently when run as root.
        monkeypatch.setattr(measurement, "read_text", lambda path:
                            None if Path(path) == rollup else original(path))
    row, = measurement.process_memory()
    assert row["rss_bytes"] == 4096 * 1024
    assert row["pss_bytes"] is None


def process(pid, rss, pss):
    return {"pid": pid, "name": "sail", "rss_bytes": rss, "pss_bytes": pss}


def sample_synchronously(sampler, monkeypatch, scans):
    """Run finite scans on this thread, with explicit callbacks for transitions.

    Setting stop during the last scan lets that scan finish and avoids sleeps;
    no assertion depends on thread scheduling or a wall-clock interval.
    """
    scans = list(scans)

    def read_processes():
        scan = scans.pop(0)
        if callable(scan):
            scan = scan()
        if not scans:
            sampler.stop.set()
        return scan

    monkeypatch.setattr(measurement, "process_memory", read_processes)
    sampler._run()
    assert sampler.error is None
    assert not scans
    return [json.loads(line) for line in sampler.output.read_text().splitlines()]


@pytest.mark.parametrize("processes, rss, pss, available", [
    ([process(1, 4096, 3072), process(2, 2048, 1024)], 6144, 4096, 2),
    ([process(1, 4096, 3072), process(2, 2048, None)], 6144, None, 1),
    ([], None, None, 0),
])
def test_sample_aggregate_does_not_treat_missing_memory_as_zero(
    proc_root, tmp_path, monkeypatch, processes, rss, pss, available,
):
    sampler = measurement.Sampler(tmp_path / "memory.jsonl", interval=0)
    sampler.mark("execute")
    row, = sample_synchronously(sampler, monkeypatch, [processes])
    assert row["rss_bytes"] == rss
    assert row["pss_bytes"] == pss
    assert row["pss_processes_available"] == available
    assert row["cgroup_current_bytes"] == 8192
    peak = sampler.receipt()["phase_peaks"]["execute"]
    assert peak.get("pss_bytes") == pss
    if pss is None:
        assert "pss_bytes" not in peak


def test_transition_scan_is_retained_but_excluded_from_execution_peak(
    proc_root, tmp_path, monkeypatch,
):
    sampler = measurement.Sampler(tmp_path / "memory.jsonl", interval=0)
    sampler.mark("execute")

    def crossing_scan():
        # Returning to the same name still crosses phase boundaries. Generation
        # tracking must reject it even though the final phase string is equal.
        sampler.mark("validate")
        sampler.mark("execute")
        return [process(1, 99999, 88888)]

    rows = sample_synchronously(sampler, monkeypatch, [
        crossing_scan, [process(1, 4096, 3072)],
    ])
    assert [row["phase"] for row in rows] == ["transition", "execute"]
    assert all(row["scan_started_seconds"] <= row["scan_finished_seconds"] for row in rows)
    receipt = sampler.receipt()
    assert receipt["samples"] == 2
    assert receipt["phase_sample_counts"] == {"transition": 1, "execute": 1}
    assert receipt["phase_peaks"]["execute"]["rss_bytes"] == 4096
    assert receipt["phase_peaks"]["execute"]["pss_bytes"] == 3072
    assert "transition" not in receipt["phase_peaks"]
    assert receipt["phase_first_samples"]["execute"] == rows[1]
    assert receipt["execution_sampled"] is True


def test_phase_counts_and_peaks_cover_all_complete_scans(proc_root, tmp_path, monkeypatch):
    sampler = measurement.Sampler(tmp_path / "memory.jsonl", interval=0)
    sampler.mark("execute")
    rows = sample_synchronously(sampler, monkeypatch, [
        [process(1, 100, 90)], [process(1, 200, 70)], [process(1, 150, 80)],
    ])
    receipt = sampler.receipt()
    assert receipt["samples"] == len(rows) == 3
    assert receipt["phase_sample_counts"] == {"execute": 3}
    assert receipt["phase_peaks"]["execute"] == {
        "rss_bytes": 200, "pss_bytes": 90, "cgroup_current_bytes": 8192,
    }
    assert receipt["phase_first_samples"]["execute"] == rows[0]


def test_unsampled_execution_is_explicit_and_has_no_fabricated_peak(
    proc_root, tmp_path, monkeypatch,
):
    sampler = measurement.Sampler(tmp_path / "memory.jsonl", interval=0)
    sampler.mark("execute")
    sampler.mark("validate")
    sample_synchronously(sampler, monkeypatch, [[process(1, 4096, 3072)]])
    receipt = sampler.receipt()
    assert receipt["execution_sampled"] is False
    assert receipt["phase_sample_counts"].get("execute", 0) == 0
    assert "execute" not in receipt["phase_peaks"]
    assert "execute" not in receipt["phase_first_samples"]
    assert receipt["phase_sample_counts"] == {"validate": 1}
