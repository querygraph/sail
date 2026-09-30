"""Partial validation cannot become an exact pass or replace a failure."""
from copy import deepcopy
import json
from pathlib import Path
import sys

import pytest

from run_matrix import classify, plan_cells
import run_matrix
from test_imported_snap import config
from validation_outcome import effective_outcome


def certificate_receipt(outcome='passed'):
    return dict(outcome=outcome, harness_source_sha='a' * 40,
                arguments={'algorithm': 'wcc'},
                correctness={'policy': 'certificate', 'component_count_verified': False})


def test_partial_scope_is_derived_without_mutating_original_evidence():
    receipt = certificate_receipt()
    original = deepcopy(receipt)
    assert effective_outcome(receipt) == 'partially_verified'
    assert effective_outcome(receipt, outcome='passed') == 'partially_verified'
    assert receipt == original


@pytest.mark.parametrize('outcome', ['mismatch', 'error', 'timeout', 'nonconverged', 'partially_verified'])
def test_existing_outcomes_are_preserved(outcome):
    assert effective_outcome(certificate_receipt(outcome)) == outcome


def test_reference_wcc_pagerank_and_independent_certificates_remain_passes():
    receipt = certificate_receipt()
    receipt['correctness']['policy'] = 'reference'
    assert effective_outcome(receipt) == 'passed'
    receipt = certificate_receipt()
    receipt['arguments']['algorithm'] = 'pagerank'
    assert effective_outcome(receipt) == 'passed'
    receipt = certificate_receipt()
    receipt['correctness']['component_count_verified'] = True
    assert effective_outcome(receipt) == 'passed'


def test_classifier_keeps_partial_scope_and_failure_precedence():
    record = dict(transport_errors=[], attach_returncode=0, inspect={'state': {'OOMKilled': False}})
    receipt = certificate_receipt()
    assert classify(record, receipt, 'a' * 40) == 'partially_verified'
    record['attach_returncode'] = 7
    assert classify(record, receipt, 'a' * 40) == 'exit_receipt_mismatch'
    receipt['outcome'] = 'partially_verified'
    record['attach_returncode'] = 1
    assert classify(record, receipt, 'a' * 40) == 'partially_verified'
    record['outer_timeout'] = True
    assert classify(record, receipt, 'a' * 40) == 'outer_timeout'
    record['inspect']['state']['OOMKilled'] = True
    assert classify(record, receipt, 'a' * 40) == 'oom'


def test_matrix_defaults_distinguish_certificate_wcc_from_other_algorithms():
    configuration = config('certificate')
    cells = plan_cells(configuration)
    assert {c['expected_outcome'] for c in cells if c['algorithm'] == 'wcc'} == {'partially_verified'}
    assert {c['expected_outcome'] for c in cells if c['algorithm'] != 'wcc'} == {'passed'}


@pytest.mark.parametrize('extra', [['--ranking-validation', 'reference'], ['--ranking-validation=reference']])
def test_matrix_defaults_use_final_validation_flags(extra):
    configuration = config('certificate')
    configuration['extra_cell_args'] = extra
    assert {c['expected_outcome'] for c in plan_cells(configuration)} == {'passed'}


def test_reference_fixtures_remain_exact_unless_certificate_is_requested():
    configuration = json.loads(Path(__file__).with_name('matrix.example.json').read_text())
    uncapped = [c for c in plan_cells(configuration) if c['expected_outcome'] != 'nonconverged']
    assert {c['expected_outcome'] for c in uncapped} == {'passed'}
    # This control exercises defaults. The example's cap suite also contains
    # explicit passed expectations, whose precedence is tested separately.
    for suite in configuration['suites']:
        suite.pop('expected_outcomes', None)
    configuration['extra_cell_args'] = ['--ranking-validation=certificate']
    cells = [c for c in plan_cells(configuration) if c['expected_outcome'] != 'nonconverged']
    assert {c['expected_outcome'] for c in cells if c['algorithm'] == 'wcc'} == {'partially_verified'}
    assert {c['expected_outcome'] for c in cells if c['algorithm'] == 'pagerank'} == {'passed'}


@pytest.mark.parametrize('expected', ['passed', 'nonconverged'])
def test_matrix_explicit_expectation_overrides_are_preserved(expected):
    configuration = config('certificate')
    configuration['suites'][0]['expected_outcomes'] = {'reference': {'pecan': expected}}
    cells = plan_cells(configuration)
    assert {c['expected_outcome'] for c in cells if c['engine'] == 'pecan'} == {expected}
    assert {c['expected_outcome'] for c in cells if c['engine'] != 'pecan' and c['algorithm'] == 'wcc'} == {'partially_verified'}


@pytest.mark.parametrize('explicit', [False, True])
def test_resume_restates_legacy_partial_pass_without_rewriting_cell_evidence(tmp_path, monkeypatch, explicit):
    configuration = config('certificate')
    configuration['host_output'] = str(tmp_path / 'matrix')
    configuration['suites'][0].update(engines=['pecan'], algorithms=['wcc'])
    if explicit:
        configuration['suites'][0]['expected_outcomes'] = {'reference': {'pecan': 'passed'}}
    cell, = plan_cells(configuration)
    directory = Path(configuration['host_output']) / 'cells' / cell['cell_id']
    (directory / 'artifacts').mkdir(parents=True)
    legacy = dict(cell, expected_outcome='passed', outcome='passed', expected_outcome_observed=True,
                  configuration_sha256=run_matrix.configuration_fingerprint(configuration))
    summary_path, receipt_path = directory / 'summary.json', directory / 'artifacts/receipt.json'
    summary_path.write_text(json.dumps(legacy))
    receipt_path.write_text(json.dumps(certificate_receipt()))
    original_summary, original_receipt = summary_path.read_bytes(), receipt_path.read_bytes()
    configuration_path = tmp_path / 'configuration.json'
    configuration_path.write_text(json.dumps(configuration))
    monkeypatch.setattr(sys, 'argv', ['run_matrix.py', '--config', str(configuration_path), '--resume', '--skip-prepare'])
    monkeypatch.setattr(run_matrix, 'preflight', lambda *args: configuration['image'])
    assert run_matrix.main() == (1 if explicit else 0)
    result, = json.loads((Path(configuration['host_output']) / 'matrix-results.json').read_text())['results']
    assert result['outcome'] == 'partially_verified'
    assert result['original_outcome'] == result['original_expected_outcome'] == 'passed'
    assert result['expected_outcome_observed'] is (not explicit)
    assert summary_path.read_bytes() == original_summary
    assert receipt_path.read_bytes() == original_receipt
