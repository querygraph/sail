"""Separate a completed partial WCC check from independently verified results."""
import argparse
import json


PARTIALLY_VERIFIED = 'partially_verified'


def is_partial_wcc_certificate(receipt):
    correctness = receipt.get('correctness')
    arguments = receipt.get('arguments')
    correctness = correctness if isinstance(correctness, dict) else {}
    arguments = arguments if isinstance(arguments, dict) else {}
    return (correctness.get('verification_scope') == 'partial_wcc_partition' or
            arguments.get('algorithm') == 'wcc' and
            (arguments.get('ranking_validation') == 'certificate' or correctness.get('policy') == 'certificate'))


def partial_certificate_errors(receipt):
    """No current WCC certificate proves connectivity, even if its marker claims it."""
    errors = [f'completed receipt {name} must be a JSON object' for name in ('arguments', 'correctness')
              if name in receipt and not isinstance(receipt[name], dict)]
    if errors:
        return errors
    if not is_partial_wcc_certificate(receipt) and receipt.get('outcome') != PARTIALLY_VERIFIED:
        return []
    arguments, correctness = receipt.get('arguments') or {}, receipt.get('correctness') or {}
    errors = []
    for key, expected in [('algorithm', 'wcc'), ('ranking_validation', 'certificate')]:
        if arguments.get(key) != expected:
            errors.append(f'partial certificate argument differs: {key}')
    if correctness.get('policy') != 'certificate':
        errors.append('partial WCC certificate policy is missing or contradictory')
    if correctness.get('component_count_verified') is not False:
        errors.append('partial WCC component count marker must be boolean false')
    if correctness.get('verification_scope') not in (None, 'partial_wcc_partition'):
        errors.append('partial WCC verification scope is contradictory')
    return errors


def effective_outcome(receipt, *, outcome=None):
    """Derive scope without changing an original receipt or masking a failure."""
    reported = receipt.get('outcome', 'invalid_receipt') if outcome is None else outcome
    if reported in ('passed', PARTIALLY_VERIFIED) and partial_certificate_errors(receipt):
        return 'invalid_receipt'
    if reported == 'passed' and is_partial_wcc_certificate(receipt):
        return PARTIALLY_VERIFIED
    return reported


def completed_exit_status(outcome):
    """graph_cell exits zero for an exact pass and one for completed partial checks."""
    return {'passed': 0, PARTIALLY_VERIFIED: 1}.get(outcome)


def validation_arguments(command):
    parser = argparse.ArgumentParser(add_help=False, allow_abbrev=False)
    parser.add_argument('--algorithm')
    parser.add_argument('--ranking-validation', default='reference')
    arguments, _ = parser.parse_known_args(command)
    return arguments


def expected_validation_outcome(command):
    """Read the effective flags, including any final extra-cell overrides."""
    arguments = validation_arguments(command)
    return (PARTIALLY_VERIFIED if arguments.algorithm == 'wcc' and
            arguments.ranking_validation == 'certificate' else 'passed')


def resumed_outcome(result, observed, expected):
    """Restate a saved summary in memory; retain its original outcome labels."""
    return dict(result, original_outcome=result.get('original_outcome', result['outcome']),
                original_expected_outcome=result.get('original_expected_outcome', result.get('expected_outcome')),
                outcome=observed, expected_outcome=expected,
                expected_outcome_observed=expected == observed)


def resumed_evidence(directory):
    """Missing or malformed retained evidence cannot substantiate a saved pass."""
    receipt_path, record_path = directory / 'artifacts/receipt.json', directory / 'orchestration.json'
    record, receipt = dict(transport_errors=[]), None
    try:
        if record_path.exists():
            record = json.loads(record_path.read_text())
        if receipt_path.exists():
            receipt = json.loads(receipt_path.read_text())
        if not isinstance(record, dict) or (receipt is not None and not isinstance(receipt, dict)):
            raise ValueError('resumed evidence must contain JSON objects')
        record.setdefault('transport_errors', [])
    except (OSError, ValueError) as error:
        record = dict(transport_errors=[], receipt_read_error=repr(error))
        receipt = None
    return record, receipt
