"""Retain partial evidence while excluding it from exact-pass aggregates."""
from copy import deepcopy
import json
from types import SimpleNamespace

import pytest

from summarize import aggregate, audited_rows
from run_matrix import configuration_fingerprint, plan_cells
from test_summarize import records
import tutorial_methods


def wcc_records():
    return records('wcc')


def partial(receipt):
    receipt['arguments']['ranking_validation'] = 'certificate'
    receipt['correctness'] = dict(policy='certificate', component_count_verified=False,
                                  verification_scope='partial_wcc_partition')


def test_legacy_partial_pass_cannot_improve_exact_pass_metrics_or_rewrite_evidence():
    configuration, entries = wcc_records()
    partial(entries[0][2])
    for entry, seconds in zip(entries, [.01, 5., 7.]):
        entry[2]['end_to_end_seconds'] = seconds
    original = deepcopy(entries)
    rows = audited_rows(entries, configuration)
    assert entries == original
    assert [row['outcome'] for row in rows] == ['partially_verified', 'passed', 'passed']
    assert rows[0]['original_outcome'] == rows[0]['receipt_outcome'] == 'passed'
    assert rows[0]['component_count_verified'] is False
    assert rows[0]['verification_scope'] == 'partial_wcc_partition'
    assert rows[0]['dataset_files_identity'] == rows[1]['dataset_files_identity']
    assert rows[0]['native_installed_files_identity'] == rows[1]['native_installed_files_identity']
    group, = aggregate(rows)
    assert group['outcomes'] == {'partially_verified': 1, 'passed': 2}
    assert group['metrics']['seconds'] == dict(samples=2, median=6., minimum=5., maximum=7.)


def test_explicit_partial_only_group_retains_counts_and_raw_metrics():
    configuration, entries = wcc_records()
    for cell, summary, receipt in entries:
        partial(receipt)
        summary['outcome'] = receipt['outcome'] = 'partially_verified'
        cell['expected_outcome'] = summary['expected_outcome'] = 'partially_verified'
    rows = audited_rows(entries, configuration)
    assert all(row['seconds'] == 5 and row['expected_outcome_observed'] for row in rows)
    group, = aggregate(rows)
    assert group['passed'] == 0 and group['outcomes'] == {'partially_verified': 3}
    assert all(metric is None for metric in group['metrics'].values())


@pytest.mark.parametrize('explicit', [None, 'passed', 'partially_verified'])
def test_historical_matrix_is_replanned_without_erasing_explicit_expectations(explicit):
    configuration, entries = wcc_records()
    configuration['extra_cell_args'] = ['--ranking-validation=certificate']
    if explicit is not None:
        suite = next(s for s in configuration['suites'] if s['name'] == entries[0][0]['suite'])
        suite.setdefault('expected_outcomes', {}).setdefault('optimized', {})['nutmeg-native'] = explicit
    for _, summary, receipt in entries:
        partial(receipt)
        # A historical default (or explicit passed) was recorded as passed.
        # An explicit partial expectation must not be silently grandfathered.
        summary['configuration_sha256'] = configuration_fingerprint(configuration)
    new_cells = {cell['cell_id']: cell for cell in plan_cells(configuration)}
    entries = [(new_cells[cell['cell_id']], summary, receipt) for cell, summary, receipt in entries]
    original = deepcopy(entries)
    rows = audited_rows(entries, configuration)
    assert entries == original
    assert all(row['original_expected_outcome'] == 'passed' for row in rows)
    assert all(row['original_outcome'] == row['receipt_outcome'] == 'passed' for row in rows)
    if explicit == 'partially_verified':
        assert all(row['outcome'] == 'integrity_error' for row in rows)
    else:
        assert all(row['outcome'] == 'partially_verified' for row in rows)
        assert all(row['expected_outcome_observed'] is (explicit is None) for row in rows)
        assert aggregate(rows)[0]['passed'] == 0


@pytest.mark.parametrize('binary', ['invalid', '1' * 64])
def test_legacy_scope_downgrade_does_not_bypass_integrity_or_cross_cell_identity(binary):
    configuration, entries = wcc_records()
    partial(entries[0][2])
    entries[0][2]['binary_sha256'] = binary
    rows = audited_rows(entries, configuration)
    assert rows[0]['outcome'] == 'integrity_error'
    assert rows[0]['original_outcome'] == rows[0]['receipt_outcome'] == 'passed'
    assert rows[0]['integrity_errors']
    if binary != 'invalid':
        assert all(row['outcome'] == 'integrity_error' for row in rows)


@pytest.mark.parametrize('reported,exit_status,expected', [
    ('partially_verified', 1, 'partially_verified'),
    ('passed', 0, 'partially_verified'),
    ('passed', 7, 'orchestration_error'),
])
def test_tutorial_keeps_partial_and_original_outcomes(tmp_path, monkeypatch, reported, exit_status, expected):
    receipt = dict(outcome=reported, arguments={'algorithm': 'wcc'}, end_to_end_seconds=1.)
    partial(receipt)
    path = tmp_path / 'receipt.json'
    path.write_text(json.dumps(receipt))
    original = path.read_bytes()
    monkeypatch.setattr(tutorial_methods.subprocess, 'run', lambda *args, **kwargs: SimpleNamespace(returncode=exit_status))
    row = tutorial_methods.run_case(['placeholder'], tmp_path / 'cell.log', path, False)
    assert row['outcome'] == expected
    assert row['original_receipt_outcome'] == reported
    assert row['end_to_end_seconds'] == 1.
    assert path.read_bytes() == original
