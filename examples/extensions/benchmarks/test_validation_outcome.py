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
                arguments={'algorithm': 'wcc', 'ranking_validation': 'certificate'},
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


def test_reference_wcc_and_pagerank_remain_passes():
    receipt = certificate_receipt()
    receipt['correctness']['policy'] = 'reference'
    receipt['arguments']['ranking_validation'] = 'reference'
    assert effective_outcome(receipt) == 'passed'
    receipt = certificate_receipt()
    receipt['arguments']['algorithm'] = 'pagerank'
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


@pytest.mark.parametrize('exit_status', [None, 0, 7, 137])
def test_partial_exit_must_be_one(exit_status):
    record = dict(transport_errors=[], attach_returncode=exit_status, inspect={'state': {'OOMKilled': False}})
    assert classify(record, certificate_receipt('partially_verified'), 'a' * 40) == 'exit_receipt_mismatch'
    record['outer_timeout'] = True
    assert classify(record, certificate_receipt('partially_verified'), 'a' * 40) == 'outer_timeout'
    record['inspect']['state']['OOMKilled'] = True
    assert classify(record, certificate_receipt('partially_verified'), 'a' * 40) == 'oom'


@pytest.mark.parametrize('marker', ['missing', None, 0, 'false', True])
def test_malformed_component_count_marker_cannot_restore_exact_pass(marker):
    receipt = certificate_receipt()
    if marker == 'missing':
        del receipt['correctness']['component_count_verified']
    else:
        receipt['correctness']['component_count_verified'] = marker
    assert effective_outcome(receipt) == 'invalid_receipt'
    record = dict(transport_errors=[], attach_returncode=0, inspect={'state': {'OOMKilled': False}})
    assert classify(record, receipt, 'a' * 40) == 'invalid_receipt'


@pytest.mark.parametrize('field', ['arguments', 'correctness'])
@pytest.mark.parametrize('value', [['malformed'], 'malformed', None])
def test_nonobject_completed_metadata_is_invalid_without_masking_failures(field, value):
    receipt = certificate_receipt()
    receipt[field] = value
    record = dict(transport_errors=[], attach_returncode=0, inspect={'state': {'OOMKilled': False}})
    assert effective_outcome(receipt) == 'invalid_receipt'
    assert classify(record, receipt, 'a' * 40) == 'invalid_receipt'
    receipt['outcome'] = 'mismatch'
    assert effective_outcome(receipt) == classify(record, receipt, 'a' * 40) == 'mismatch'


@pytest.mark.parametrize('field', ['arguments', 'correctness', 'receipt'])
@pytest.mark.parametrize('value', [['malformed'], 'malformed'])
def test_fresh_matrix_retains_malformed_receipt_without_crashing(tmp_path, monkeypatch, field, value):
    configuration = config('certificate')
    configuration['host_output'] = str(tmp_path / 'matrix')
    configuration['suites'][0].update(engines=['pecan'], algorithms=['wcc'])
    receipt = certificate_receipt('partially_verified')
    receipt['harness_source_sha'] = configuration['harness_source_sha']
    if field == 'receipt':
        receipt = value
    else:
        receipt[field] = value

    def run_container(_config, _name, _command, output, *_args):
        (output / 'artifacts').mkdir(parents=True)
        (output / 'artifacts/receipt.json').write_text(json.dumps(receipt))
        return dict(transport_errors=[], attach_returncode=1)

    configuration_path = tmp_path / 'configuration.json'
    configuration_path.write_text(json.dumps(configuration))
    monkeypatch.setattr(sys, 'argv', ['run_matrix.py', '--config', str(configuration_path), '--skip-prepare'])
    monkeypatch.setattr(run_matrix, 'preflight', lambda *args: configuration['image'])
    monkeypatch.setattr(run_matrix, 'run_container', run_container)
    assert run_matrix.main() == 1
    result, = json.loads((Path(configuration['host_output']) / 'matrix-results.json').read_text())['results']
    assert result['outcome'] == 'invalid_receipt'
    assert not result['expected_outcome_observed']
    assert json.loads(Path(result['receipt_path']).read_text()) == receipt


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
@pytest.mark.parametrize('evidence', ['valid', 'missing_receipt', 'malformed_receipt', 'missing_exit', 'unexpected_exit'])
def test_resume_restates_legacy_partial_pass_without_rewriting_cell_evidence(tmp_path, monkeypatch, explicit, evidence):
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
    receipt = certificate_receipt()
    receipt['harness_source_sha'] = configuration['harness_source_sha']
    if evidence != 'missing_receipt':
        receipt_path.write_text('{malformed' if evidence == 'malformed_receipt' else json.dumps(receipt))
    if evidence != 'missing_exit':
        (directory / 'orchestration.json').write_text(json.dumps(dict(transport_errors=[],
            attach_returncode=7 if evidence == 'unexpected_exit' else 0)))
    original_summary = summary_path.read_bytes()
    original_receipt = receipt_path.read_bytes() if receipt_path.exists() else None
    configuration_path = tmp_path / 'configuration.json'
    configuration_path.write_text(json.dumps(configuration))
    monkeypatch.setattr(sys, 'argv', ['run_matrix.py', '--config', str(configuration_path), '--resume', '--skip-prepare'])
    monkeypatch.setattr(run_matrix, 'preflight', lambda *args: configuration['image'])
    assert run_matrix.main() == (0 if not explicit and evidence == 'valid' else 1)
    result, = json.loads((Path(configuration['host_output']) / 'matrix-results.json').read_text())['results']
    assert result['outcome'] == dict(valid='partially_verified', missing_receipt='missing_receipt',
        malformed_receipt='invalid_receipt', missing_exit='exit_receipt_mismatch', unexpected_exit='exit_receipt_mismatch')[evidence]
    assert result['original_outcome'] == result['original_expected_outcome'] == 'passed'
    assert result['expected_outcome_observed'] is (not explicit and evidence == 'valid')
    assert summary_path.read_bytes() == original_summary
    assert (receipt_path.read_bytes() if receipt_path.exists() else None) == original_receipt
