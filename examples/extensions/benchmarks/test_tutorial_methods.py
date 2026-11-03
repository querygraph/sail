"""A tutorial keeps ordinary failures, while operator cancellation stops it."""
from collections import Counter
import json
from pathlib import Path
import sys
from types import SimpleNamespace

import pytest

import tutorial_methods


def setup_tutorial(tmp_path, monkeypatch, launch, *options):
    binary = tmp_path / 'sail'
    binary.write_bytes(b'test-only executable placeholder; subprocess is mocked')
    output = tmp_path / 'tutorial'
    monkeypatch.setattr(sys, 'argv', ['tutorial_methods.py', '--sail-binary', str(binary),
                                    '--runtime-source-sha', 'a' * 40, '--native-source-sha', 'b' * 40,
                                    '--output', str(output), *options])
    monkeypatch.setattr(tutorial_methods, 'prepare', lambda *args, **kwargs: {'counts': {'vertices': 128}})
    monkeypatch.setattr(tutorial_methods.subprocess, 'run', launch)
    return output


def receipt(command, value):
    output = Path(command[command.index('--output') + 1])
    output.mkdir()
    (output / 'receipt.json').write_text(value if isinstance(value, str) else json.dumps(value))


def test_all_fifteen_cases_retained_across_launch_receipt_and_child_failures(tmp_path, monkeypatch):
    commands = []
    def launch(command, **kwargs):
        commands.append(command)
        index = len(commands)
        if index == 1:
            raise OSError('child executable unavailable')
        if index == 2:
            receipt(command, '{partial JSON')
        elif index != 5:
            receipt(command, {'outcome': 'error' if index == 4 else 'passed',
                              'error': 'retained algorithm error' if index == 4 else None,
                              'end_to_end_seconds': 1.0})
        return SimpleNamespace(returncode=7 if index == 3 else 1 if index in (4, 5) else 0)
    output = setup_tutorial(tmp_path, monkeypatch, launch)
    assert tutorial_methods.main() == 1
    summary = json.loads((output / 'tutorial-summary.json').read_text())
    assert summary['outcome'] == 'failed'
    assert summary['planned_cells'] == len(summary['cells']) == len(commands) == 15
    assert len({(row['engine'], row['algorithm'], row['variant']) for row in summary['cells']}) == 15
    assert Counter(row['outcome'] for row in summary['cells']) == {
        'orchestration_error': 3, 'error': 1, 'incomplete_record': 1, 'passed': 10}
    assert all('--allow-unisolated' in command for command in commands)
    launch_row, invalid_row, exit_row, algorithm_row, missing_row = summary['cells'][:5]
    assert launch_row['returncode'] is None and 'child executable unavailable' in launch_row['error']
    assert 'JSONDecodeError' in invalid_row['error']
    assert exit_row['returncode'] == 7 and exit_row['original_receipt_outcome'] == 'passed'
    assert 'disagrees' in exit_row['error']
    assert algorithm_row['original_receipt_outcome'] == 'error'
    assert algorithm_row['error'] == 'retained algorithm error'
    assert missing_row['returncode'] == 1 and missing_row['original_receipt_outcome'] is None


@pytest.mark.parametrize('value', [[], {'outcome': 'started'},
                                  {'outcome': 'passed', 'end_to_end_seconds': 'not numeric'},
                                  {'outcome': 'passed', 'memory': []}])
def test_invalid_receipt_shape_is_retained_without_skipping_later_cases(tmp_path, monkeypatch, value):
    calls = []
    def launch(command, **kwargs):
        calls.append(command)
        receipt(command, value if len(calls) == 1 else {'outcome': 'passed'})
        return SimpleNamespace(returncode=0)
    output = setup_tutorial(tmp_path, monkeypatch, launch)
    assert tutorial_methods.main() == 1
    rows = json.loads((output / 'tutorial-summary.json').read_text())['cells']
    assert len(rows) == 15
    assert rows[0]['outcome'] == 'orchestration_error'
    assert all(row['outcome'] == 'passed' for row in rows[1:])


@pytest.mark.parametrize('exception', [KeyboardInterrupt, lambda: SystemExit(7)])
def test_operator_cancellation_persists_partial_summary_and_stops(tmp_path, monkeypatch, exception):
    calls = []
    def launch(command, **kwargs):
        calls.append(command)
        if len(calls) == 2:
            raise exception()
        receipt(command, {'outcome': 'passed'})
        return SimpleNamespace(returncode=0)
    output = setup_tutorial(tmp_path, monkeypatch, launch)
    with pytest.raises((KeyboardInterrupt, SystemExit)):
        tutorial_methods.main()
    summary = json.loads((output / 'tutorial-summary.json').read_text())
    assert summary['outcome'] == 'interrupted' and summary['planned_cells'] == 15
    assert len(summary['cells']) == len(calls) == 2
    assert [row['outcome'] for row in summary['cells']] == ['passed', 'interrupted']
    assert summary['cells'][-1]['error']


def test_single_method_selection_retains_requested_case(tmp_path, monkeypatch):
    def launch(command, **kwargs):
        receipt(command, {'outcome': 'passed'})
        return SimpleNamespace(returncode=0)
    output = setup_tutorial(tmp_path, monkeypatch, launch, '--engine', 'nutmeg-native',
                            '--algorithm', 'wcc', '--variant', 'optimized')
    assert tutorial_methods.main() == 0
    summary = json.loads((output / 'tutorial-summary.json').read_text())
    assert summary['planned_cells'] == 1
    row, = summary['cells']
    assert (row['engine'], row['algorithm'], row['variant']) == ('nutmeg-native', 'wcc', 'optimized')


def test_fused_selection_runs_only_wcc_and_rejects_explicit_pagerank(tmp_path, monkeypatch):
    def launch(command, **kwargs):
        receipt(command, {'outcome': 'passed'})
        return SimpleNamespace(returncode=0)
    output = setup_tutorial(tmp_path, monkeypatch, launch, '--variant', 'fused')
    assert tutorial_methods.main() == 0
    rows = json.loads((output / 'tutorial-summary.json').read_text())['cells']
    assert len(rows) == 3
    assert all(row['algorithm'] == 'wcc' and row['variant'] == 'fused' for row in rows)
    setup_tutorial(tmp_path, monkeypatch, launch, '--variant', 'fused', '--algorithm', 'pagerank')
    with pytest.raises(SystemExit) as error:
        tutorial_methods.main()
    assert error.value.code == 2
