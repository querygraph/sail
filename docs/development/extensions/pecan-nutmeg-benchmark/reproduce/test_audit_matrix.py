"""Corruption controls for the local evidence auditor; never modify source evidence."""
import copy
import os
import json
from pathlib import Path
import tempfile
import types
import unittest

import pyarrow as pa
import pyarrow.parquet as pq

import audit_matrix as audit

ROOT = Path(__file__).resolve().parents[5]
PILOTS = Path(os.environ.get('PECAN_AUDIT_PILOTS',
    str(ROOT / 'target/extensions-pecan-benchmark/evidence/pecan-pilots-a6567b51')))


def instance(path):
    return audit.Audit(types.SimpleNamespace(evidence=path, raw=[], repo=ROOT))


class AuditCorruptionControls(unittest.TestCase):
    def test_full_width_coefficient_golden_vectors(self):
        self.assertEqual(next(audit.splitmix_coefficients(0)),
                         (0xe220a8397b1dcdaf, 0x6e789e6aa1b965f4))
        self.assertEqual(next(audit.splitmix_coefficients(-1)),
                         next(audit.splitmix_coefficients((1 << 64) - 1)))

    def test_coefficient_corruption_is_not_hidden_by_unchanged_counters(self):
        paths = PILOTS.glob('cells/*nutmeg-native-wcc-optimized/artifacts/receipt.json')
        receipt = next(audit.read(path) for path in paths if audit.read(path)['outcome'] == 'passed')
        receipt = copy.deepcopy(receipt)
        receipt['native_status_after']['reads'][0]['diagnostics']['rounds'][0]['a_bits'] = '1'
        checker = instance(PILOTS)
        checker.trace(dict(cell_id='corrupt', algorithm='wcc', variant='optimized', engine='nutmeg-native'), receipt)
        self.assertTrue(any('coefficient stream' in error['message'] for error in checker.issues))

    def test_wrong_process_total_is_detected_before_peak_replay(self):
        path = next(PILOTS.glob('cells/*/artifacts/receipt.json'))
        receipt = audit.read(path)
        lines = [json.loads(line) for line in (path.parent / 'memory-samples.jsonl').read_text().splitlines()]
        lines[0]['rss_bytes'] += 1
        with tempfile.TemporaryDirectory() as temporary:
            sample = Path(temporary) / 'samples.jsonl'
            sample.write_text(''.join(json.dumps(row) + '\n' for row in lines))
            checker = instance(PILOTS)
            checker.memory(sample, receipt, 'corrupt')
        self.assertTrue(any('process memory sum' in error['message'] for error in checker.issues))

    def raw_fixture(self, base, components, ids=None):
        dataset = base / 'datasets/fixture/dataset'
        result = base / 'cells/test/artifacts/result'
        dataset.mkdir(parents=True); result.mkdir(parents=True)
        inputs = {'vertices.parquet': pa.table({'id': [0, 1, 2, 3]}),
                  'edges.parquet': pa.table({'src': [0, 1, 2, 3], 'dst': [1, 0, 2, 3]}),
                  'reference.parquet': pa.table({'id': [0, 1, 2, 3], 'pagerank': [.25] * 4,
                                                'component': [0, 0, 2, 3]})}
        for name, table in inputs.items():
            pq.write_table(table, dataset / name)
        manifest = dict(counts=dict(vertices=4, edges=4, components=3, isolates=0, dangling=0, self_loops=2),
                        pagerank=dict(damping=.85, tolerance=1e-8),
                        files={name: dict(bytes=(dataset / name).stat().st_size,
                                          sha256=audit.hash_file(dataset / name)) for name in inputs})
        output = result / 'part.parquet'
        pq.write_table(pa.table({'id': ids or [0, 1, 2, 3], 'component': components}), output)
        receipt = dict(dataset=manifest, outcome='passed',
                       result_files=[dict(name=output.name, bytes=output.stat().st_size,
                                          sha256=audit.hash_file(output))])
        cell = dict(cell_id='test', dataset='fixture', algorithm='wcc')
        return cell, receipt

    def test_raw_partition_mismatch_detected_even_when_all_file_hashes_match(self):
        with tempfile.TemporaryDirectory() as temporary:
            base = Path(temporary)
            cell, receipt = self.raw_fixture(base, ['all'] * 4)
            checker = instance(base)
            result = checker.raw_result(cell, receipt)
        self.assertEqual(result['membership_mismatches'], 2)
        self.assertTrue(any('raw WCC differs' in error['message'] for error in checker.issues))

    def test_equivalent_arbitrary_component_names_are_canonicalized(self):
        with tempfile.TemporaryDirectory() as temporary:
            base = Path(temporary)
            cell, receipt = self.raw_fixture(base, ['z', 'z', 'a', 'b'])
            checker = instance(base)
            result = checker.raw_result(cell, receipt)
        self.assertEqual(result['membership_mismatches'], 0)
        self.assertEqual(checker.issues, [])

    def test_wrong_normalized_rank_vector_is_detected_with_matching_hashes(self):
        with tempfile.TemporaryDirectory() as temporary:
            base = Path(temporary)
            cell, receipt = self.raw_fixture(base, ['z', 'z', 'a', 'b'])
            output = base / 'cells/test/artifacts/result/part.parquet'
            pq.write_table(pa.table(dict(id=[0, 1, 2, 3], score=[.4, .1, .25, .25],
                                        iterations=[1] * 4, converged=[True] * 4)), output)
            receipt['result_files'] = [dict(name=output.name, bytes=output.stat().st_size,
                                            sha256=audit.hash_file(output))]
            receipt['arguments'] = dict(damping=.85, tolerance=1e-8, max_iterations=1000)
            receipt['correctness'] = dict(true_fixed_point_residual=0., l1_error=0., max_error=0.)
            cell.update(algorithm='pagerank', engine='pecan', variant='reference')
            checker = instance(base)
            result = checker.raw_result(cell, receipt)
        self.assertGreater(result['true_fixed_point_residual'], .1)
        self.assertTrue(any('fixed-point residual exceeds' in error['message'] for error in checker.issues))

    def test_duplicate_output_id_is_detected_even_at_correct_row_count(self):
        with tempfile.TemporaryDirectory() as temporary:
            base = Path(temporary)
            cell, receipt = self.raw_fixture(base, ['z', 'z', 'a', 'b'], [0, 0, 2, 3])
            checker = instance(base)
            result = checker.raw_result(cell, receipt)
        self.assertEqual(result['coverage'], 'raw_invalid_ids')
        self.assertTrue(checker.issues)


if __name__ == '__main__':
    unittest.main(verbosity=2)
