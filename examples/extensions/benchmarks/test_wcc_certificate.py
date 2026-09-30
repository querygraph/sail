"""Exercise WCC partial and reference checks on an actual configured Sail server."""
import os

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from graph_cell import validate
from validation_outcome import effective_outcome


@pytest.fixture(scope='module')
def spark():
    endpoint = os.environ.get('SAIL_GRAPH_TEST_REMOTE')
    if not endpoint:
        pytest.skip('set SAIL_GRAPH_TEST_REMOTE for actual WCC certificate checks')
    from pyspark.sql.connect.session import SparkSession
    session = SparkSession.builder.remote(endpoint).create()
    yield session
    session.stop()


def fixture(tmp_path, labels):
    dataset, output = tmp_path / 'dataset', tmp_path / 'result'
    dataset.mkdir()
    output.mkdir()
    pq.write_table(pa.table({'id': [0, 1, 2, 3]}), dataset / 'vertices.parquet')
    pq.write_table(pa.table({'src': [0, 2], 'dst': [1, 3]}), dataset / 'edges.parquet')
    pq.write_table(pa.table({'id': [0, 1, 2, 3], 'component': [0, 0, 2, 2]}), dataset / 'reference.parquet')
    pq.write_table(pa.table({'id': [0, 1, 2, 3], 'component': pa.array(labels, type=pa.string())}), output / 'part.parquet')
    return dataset, output


def check(spark, tmp_path, labels, policy):
    dataset, output = fixture(tmp_path, labels)
    return validate(spark, output, dataset, 'wcc', 4, 1e-8, .85, 100, False, False, policy=policy)


@pytest.mark.parametrize('labels', [['0', '0', '0', '0'], ['0', '0', '2', '2'], ['2', '2', '0', '0']])
def test_edge_consistency_only_is_partial_even_for_a_correct_partition(spark, tmp_path, labels):
    correctness = check(spark, tmp_path, labels, 'certificate')
    assert correctness['component_count_verified'] is False
    assert correctness['verification_scope'] == 'partial_wcc_partition'
    assert correctness['crossing_edges'] == correctness['foreign_labels'] == 0
    assert effective_outcome({'arguments': {'algorithm': 'wcc', 'ranking_validation': 'certificate'},
                              'correctness': correctness}, outcome='passed') == 'partially_verified'


def test_reference_rejects_merged_components(spark, tmp_path):
    with pytest.raises(AssertionError, match='WCC membership mismatches'):
        check(spark, tmp_path, ['0', '0', '0', '0'], 'reference')


def test_reference_normalizes_valid_representative_conventions(spark, tmp_path):
    correctness = check(spark, tmp_path, ['1', '1', '3', '3'], 'reference')
    assert correctness['membership_mismatches'] == 0
    assert correctness['components'] == 2
    assert effective_outcome({'arguments': {'algorithm': 'wcc'}, 'correctness': correctness}, outcome='passed') == 'passed'


def test_partial_check_still_rejects_split_connected_components(spark, tmp_path):
    with pytest.raises(AssertionError, match='edges cross component labels'):
        check(spark, tmp_path, ['0', '1', '2', '3'], 'certificate')
