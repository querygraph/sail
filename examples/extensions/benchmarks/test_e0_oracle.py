"""Negative physical-output controls and independent algorithm expectations."""

from __future__ import annotations

import json
from dataclasses import asdict, replace
from pathlib import Path

import e0_oracle
import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from e0_cell import Arguments, Receipt
from e0_oracle import (
    MAX_HOPS,
    FilePin,
    Graph,
    OracleReceipt,
    bounded_distances,
    complete_distances,
    pagerank,
    qualify,
    qualify_official_sssp,
)


def graph() -> Graph:
    return Graph(
        np.array([-9, 0, 2, 4, 8], dtype=np.int64),
        np.array([0, 1, 1, 2, 2], dtype=np.int64),
        np.array([1, 2, 2, 1, 3], dtype=np.int64),
        np.array([1.0, 2.0, 0.0, 0.0, 0.5], dtype=np.float64),
    )


def arguments() -> Arguments:
    return Arguments(
        "sc://127.0.0.1:1",
        Path("vertices"),
        Path("edges"),
        Path("out"),
        "sssp",
        -9,
        (-9,),
        1,
        10,
        0.01,
        0.15,
        False,
        True,
        False,
        True,
        False,
    )


def oracle() -> OracleReceipt:
    return OracleReceipt("2026-10-03T00:00:00+00:00", "running", FilePin("receipt", 0, "unused"), [], [], "sssp")


def producer(args: Arguments | None = None, *, iterations: int = 4) -> Receipt:
    return Receipt(
        "2026-10-03T00:00:00+00:00",
        "completed_unvalidated",
        args or arguments(),
        "out",
        iterations=iterations,
        converged=None if args is not None and not args.vote_to_halt else True,
        session_stopped=True,
    )


def output() -> pa.Table:
    return pa.table(
        {
            "id": pa.array([-9, 0, 2, 4, 8], type=pa.int64()),
            "distance": pa.array([0.0, 1.0, 1.0, 1.5, None], type=pa.float64()),
        }
    )


def test_independent_heap_and_bounded_all_edge_recurrence() -> None:
    np.testing.assert_array_equal(complete_distances(graph(), -9), [0.0, 1.0, 1.0, 1.5, np.inf])
    np.testing.assert_array_equal(bounded_distances(graph(), -9, 1), [0.0, 1.0, np.inf, np.inf, np.inf])
    np.testing.assert_array_equal(complete_distances(graph(), 4, reverse=True, weighted=False), [3, 2, 1, 0, np.inf])


def test_full_output_domain_order_independence_and_null_unreachable() -> None:
    receipt = oracle()
    qualify(producer(), graph(), output().take(pa.array([4, 2, 0, 3, 1])), receipt)
    assert receipt.result_rows == 5 and receipt.mismatch_rows == 0


@pytest.mark.parametrize(
    "distances",
    [[0.0, 1.0, 2.0, 1.5, None], [0.0, None, 1.0, 1.5, None], [0.0, 1.0, 1.0, 1.5, 0.0], [0.0, 1.0, np.inf, 1.5, None]],
)
def test_wrong_distance_or_null_classification_is_rejected(distances: list[float | None]) -> None:
    table = output().set_column(1, "distance", pa.array(distances, type=pa.float64()))
    with pytest.raises(ValueError):
        qualify(producer(), graph(), table, oracle())


@pytest.mark.parametrize("ids", [[-9, 0, 2, 4, 4], [-9, 0, 2, 4, 99]])
def test_duplicate_missing_or_foreign_id_is_rejected(ids: list[int]) -> None:
    table = output().set_column(0, "id", pa.array(ids, type=pa.int64()))
    with pytest.raises(ValueError, match="every original vertex exactly once"):
        qualify(producer(), graph(), table, oracle())


def test_wrong_physical_output_type_is_rejected() -> None:
    table = output().set_column(1, "distance", pa.array([0.0, 1.0, 1.0, 1.5, None], type=pa.float32()))
    with pytest.raises(ValueError, match="Float64"):
        qualify(producer(), graph(), table, oracle())


def test_fixed_budget_is_not_mistaken_for_complete_shortest_paths() -> None:
    args = replace(arguments(), vote_to_halt=False, iterations=1)
    table = output().set_column(1, "distance", pa.array([0.0, 1.0, None, None, None], type=pa.float64()))
    qualify(producer(args, iterations=1), graph(), table, oracle())
    with pytest.raises(ValueError):
        qualify(producer(args, iterations=1), graph(), output(), oracle())


def test_landmark_int32_max_and_direction_are_checked() -> None:
    args = replace(arguments(), program="landmarks", landmarks=(4,), reversed_edges=True)
    table = pa.table(
        {"id": pa.array([-9, 0, 2, 4, 8], type=pa.int64()), "dist_4": pa.array([3, 2, 1, 0, MAX_HOPS], type=pa.int32())}
    )
    qualify(producer(args), graph(), table, oracle())
    with pytest.raises(ValueError):
        qualify(producer(replace(args, reversed_edges=False)), graph(), table, oracle())
    with pytest.raises(ValueError, match="Int32"):
        qualify(producer(args), graph(), table.set_column(1, "dist_4", table["dist_4"].cast(pa.int64())), oracle())


def test_graphx_sink_recurrence_and_normalization() -> None:
    sink = Graph(
        np.array([1, 2, 3, 4], dtype=np.int64),
        np.array([0, 0, 1, 2], dtype=np.int64),
        np.array([1, 2, 3, 3], dtype=np.int64),
        np.ones(4, dtype=np.float64),
    )
    args = replace(arguments(), program="pagerank", input_weights=False)
    ranks, converged = pagerank(sink, args, 3)
    np.testing.assert_allclose(ranks, [0.15, 0.21375, 0.21375, 0.513375], rtol=0, atol=1e-15)
    assert converged
    normalized, _ = pagerank(sink, replace(args, normalized=True), 3)
    assert normalized.sum() == pytest.approx(1.0)


def prepare_cell(tmp_path: Path) -> Path:
    cell = tmp_path / "cell"
    cell.mkdir()
    (cell / "result").mkdir()
    original = graph()
    vertices, edges = tmp_path / "vertices.parquet", tmp_path / "edges.parquet"
    pq.write_table(pa.table({"id": pa.array(original.ids, type=pa.int64())}), vertices)
    pq.write_table(
        pa.table(
            {
                "source": pa.array(original.ids[original.source], type=pa.int64()),
                "target": pa.array(original.ids[original.target], type=pa.int64()),
                "weight": pa.array(original.weights, type=pa.float64()),
            }
        ),
        edges,
    )
    pq.write_table(output(), cell / "result" / "part-0.parquet")
    args = replace(arguments(), vertices=vertices, edges=edges, output=cell)
    (cell / "receipt.json").write_text(json.dumps(asdict(producer(args)), default=str))
    return cell


def test_complete_disk_oracle_binds_every_original_and_result(tmp_path: Path) -> None:
    cell = prepare_cell(tmp_path)
    receipt = tmp_path / "oracle.json"
    e0_oracle.execute(cell, receipt)
    saved = json.loads(receipt.read_text())
    assert saved["status"] == "passed_full_physical_oracle"
    assert len(saved["before"]) == 4 and saved["before"] == saved["after"]
    assert saved["result_rows"] == 5 and saved["mismatch_rows"] == 0


def test_mutated_original_during_qualification_keeps_failed_receipt(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    cell = prepare_cell(tmp_path)
    original_qualify = e0_oracle.qualify

    def change_source(producer: Receipt, graph: Graph, table: pa.Table, receipt: OracleReceipt) -> None:
        original_qualify(producer, graph, table, receipt)
        pq.write_table(pa.table({"id": pa.array([-9, 0, 2, 4, 8, 99], type=pa.int64())}), producer.arguments.vertices)

    monkeypatch.setattr(e0_oracle, "qualify", change_source)
    receipt = tmp_path / "failed-oracle.json"
    with pytest.raises(ValueError, match="changed during qualification"):
        e0_oracle.execute(cell, receipt)
    assert json.loads(receipt.read_text())["status"] == "error"


def test_full_official_reference_and_unreachable_sentinel(tmp_path: Path) -> None:
    reference = tmp_path / "official-SSSP"
    reference.write_text("-9 0.0\n0 1.0\n2 1.0\n4 1.5\n8 1.7976931348623157e308\n")
    receipt = oracle()
    qualify_official_sssp(output(), graph(), arguments(), reference, receipt)
    assert receipt.official_sssp_passed
    reference.write_text("-9 0.0\n0 1.0\n2 9.0\n4 1.5\n8 1.7976931348623157e308\n")
    with pytest.raises(ValueError, match="official SSSP reference"):
        qualify_official_sssp(output(), graph(), arguments(), reference, oracle())


def test_undirected_input_expansion_keeps_actual_weights(tmp_path: Path) -> None:
    cell = prepare_cell(tmp_path)
    args = replace(
        arguments(),
        vertices=tmp_path / "vertices.parquet",
        edges=tmp_path / "edges.parquet",
        output=cell,
        undirected=True,
    )
    expanded = e0_oracle.read_graph(args)
    assert len(expanded.source) == len(expanded.weights) == 10
    np.testing.assert_array_equal(complete_distances(expanded, 4), [1.5, 0.5, 0.5, 0.0, np.inf])
