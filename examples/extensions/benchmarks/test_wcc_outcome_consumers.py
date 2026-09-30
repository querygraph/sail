"""Retain partial evidence while excluding it from exact-pass aggregates."""
from copy import deepcopy
import json
from types import SimpleNamespace

import pytest

from summarize import aggregate, audited_rows
from run_matrix import configuration_fingerprint, plan_cells
from test_summarize import records
import tutorial_methods
from test_certificate_identity import graph500, imported


def wcc_records():
    return records('wcc')


def partial(receipt):
    receipt['arguments']['ranking_validation'] = 'certificate'
    receipt['correctness'] = dict(policy='certificate', component_count_verified=False,
                                  verification_scope='partial_wcc_partition')


def certificate_records(reported='passed'):
    configuration, entries = wcc_records()
    configuration['extra_cell_args'] = ['--ranking-validation=certificate']
    planned = {cell['cell_id']: cell for cell in plan_cells(configuration)}
    for cell, summary, receipt in entries:
        partial(receipt)
        summary['configuration_sha256'] = configuration_fingerprint(configuration)
        summary['outcome'] = receipt['outcome'] = reported
        if reported == 'partially_verified':
            summary.update(planned[cell['cell_id']])
    return configuration, [(planned[cell['cell_id']], summary, receipt) for cell, summary, receipt in entries]


def test_legacy_partial_pass_cannot_improve_exact_pass_metrics_or_rewrite_evidence():
    configuration, entries = certificate_records()
    for entry, seconds in zip(entries, [.01, 5., 7.]):
        entry[2]['end_to_end_seconds'] = seconds
    original = deepcopy(entries)
    rows = audited_rows(entries, configuration)
    assert entries == original
    assert {row['outcome'] for row in rows} == {'partially_verified'}
    assert rows[0]['original_outcome'] == rows[0]['receipt_outcome'] == 'passed'
    assert rows[0]['component_count_verified'] is False
    assert rows[0]['verification_scope'] == 'partial_wcc_partition'
    assert rows[0]['dataset_files_identity'] == rows[1]['dataset_files_identity']
    assert rows[0]['native_installed_files_identity'] == rows[1]['native_installed_files_identity']
    group, = aggregate(rows)
    assert group['outcomes'] == {'partially_verified': 3}
    assert group['passed'] == 0 and group['metrics']['seconds'] is None
    # Aggregation itself must also exclude a faster partial observation from
    # any pass-only statistic; these synthetic rows isolate that filter.
    mixed = deepcopy(rows)
    mixed[1]['outcome'] = mixed[2]['outcome'] = 'passed'
    assert aggregate(mixed)[0]['metrics']['seconds'] == dict(samples=2, median=6., minimum=5., maximum=7.)


def test_explicit_partial_only_group_retains_counts_and_raw_metrics():
    configuration, entries = certificate_records('partially_verified')
    rows = audited_rows(entries, configuration)
    assert all(row['seconds'] == 5 and row['expected_outcome_observed'] for row in rows)
    group, = aggregate(rows)
    assert group['passed'] == 0 and group['outcomes'] == {'partially_verified': 3}
    assert all(metric is None for metric in group['metrics'].values())


def test_reclassified_legacy_receipt_can_back_a_new_partial_summary():
    configuration, entries = certificate_records('partially_verified')
    for _, _, receipt in entries:
        receipt['outcome'] = 'passed'
    original = deepcopy(entries)
    rows = audited_rows(entries, configuration)
    assert entries == original
    assert all(row['outcome'] == row['original_outcome'] == 'partially_verified' for row in rows)
    assert all(row['receipt_outcome'] == 'passed' and not row['integrity_errors'] for row in rows)
    assert aggregate(rows)[0]['passed'] == 0


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
    configuration, entries = certificate_records()
    entries[0][2]['binary_sha256'] = binary
    rows = audited_rows(entries, configuration)
    assert rows[0]['outcome'] == 'integrity_error'
    assert rows[0]['original_outcome'] == rows[0]['receipt_outcome'] == 'passed'
    assert rows[0]['integrity_errors']
    if binary != 'invalid':
        assert all(row['outcome'] == 'integrity_error' for row in rows)


@pytest.mark.parametrize('corruption', ['source', 'binary', 'dataset', 'native', 'receipt_outcome', 'validation_policy'])
def test_new_partial_receipts_receive_the_same_identity_audit(corruption):
    configuration, entries = certificate_records('partially_verified')
    receipt = entries[0][2]
    if corruption == 'source':
        receipt['runtime_source_sha'] = '9' * 40
    elif corruption == 'binary':
        receipt['binary_sha256'] = '9' * 64
    elif corruption == 'dataset':
        receipt['dataset']['files']['edges.parquet']['sha256'] = '9' * 64
    elif corruption == 'native':
        receipt['native_package_identity']['files_sha256']['extension.so'] = '9' * 64
    elif corruption == 'receipt_outcome':
        receipt['outcome'] = 'mismatch'
    else:
        receipt['arguments']['ranking_validation'] = 'reference'
    original = deepcopy(entries)
    rows = audited_rows(entries, configuration)
    assert entries == original
    assert rows[0]['outcome'] == 'integrity_error'
    assert rows[0]['original_outcome'] == 'partially_verified'
    assert rows[0]['receipt_outcome'] == receipt['outcome']
    assert aggregate(rows)[0]['passed'] == 0
    if corruption in ('binary', 'dataset', 'native'):
        assert all(row['outcome'] == 'integrity_error' for row in rows)


@pytest.mark.parametrize('reported', ['passed', 'partially_verified'])
@pytest.mark.parametrize('marker', ['missing', None, 0, 'false', True])
def test_malformed_partial_scope_metadata_is_an_integrity_error(reported, marker):
    configuration, entries = certificate_records(reported)
    if marker == 'missing':
        del entries[0][2]['correctness']['component_count_verified']
    else:
        entries[0][2]['correctness']['component_count_verified'] = marker
    row = audited_rows(entries, configuration)[0]
    assert row['outcome'] == 'integrity_error'
    assert 'partial WCC component count marker must be boolean false' in row['integrity_errors']


def test_receipt_cannot_change_a_planned_reference_protocol_to_certificate():
    configuration, entries = wcc_records()
    partial(entries[0][2])
    row = audited_rows(entries, configuration)[0]
    assert row['outcome'] == 'integrity_error'
    assert 'receipt argument differs: ranking_validation' in row['integrity_errors']


@pytest.mark.parametrize('field', ['arguments', 'correctness'])
@pytest.mark.parametrize('value', [None, ['malformed'], 'malformed'])
def test_nonobject_completed_metadata_is_retained_as_an_integrity_error(field, value):
    configuration, entries = certificate_records('partially_verified')
    entries[0][2][field] = value
    original = deepcopy(entries)
    row = audited_rows(entries, configuration)[0]
    assert entries == original
    assert row['outcome'] == 'integrity_error'
    assert f'completed receipt {field} must be a JSON object' in row['integrity_errors']


@pytest.mark.parametrize('kind', ['imported', 'graph500'])
def test_partitioned_no_reference_certificates_are_audited_without_becoming_passes(imported, kind):
    manifest, options = imported if kind == 'imported' else graph500()
    configuration, entries = certificate_records('partially_verified')
    dataset = entries[0][0]['dataset']
    configuration['datasets'][dataset] = options
    planned = {cell['cell_id']: cell for cell in plan_cells(configuration)}
    for cell, summary, receipt in entries:
        summary.update(planned[cell['cell_id']], configuration_sha256=configuration_fingerprint(configuration))
        receipt['dataset'] = deepcopy(manifest)
    entries = [(planned[cell['cell_id']], summary, receipt) for cell, summary, receipt in entries]
    rows = audited_rows(entries, configuration)
    assert all(row['outcome'] == 'partially_verified' and not row['integrity_errors'] for row in rows)
    assert all(row['dataset_files_identity'] == rows[0]['dataset_files_identity'] for row in rows)
    assert aggregate(rows)[0]['passed'] == 0
    first = next(iter(entries[0][2]['dataset']['files']))
    entries[0][2]['dataset']['files'][first]['sha256'] = '9' * 64
    assert all(row['outcome'] == 'integrity_error' for row in audited_rows(entries, configuration))


@pytest.mark.parametrize('reported,exit_status,expected', [
    ('partially_verified', 1, 'partially_verified'),
    ('passed', 0, 'partially_verified'),
    ('passed', 7, 'orchestration_error'),
    ('partially_verified', 0, 'orchestration_error'),
    ('partially_verified', 7, 'orchestration_error'),
    ('partially_verified', 137, 'orchestration_error'),
    ('partially_verified', None, 'orchestration_error'),
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
