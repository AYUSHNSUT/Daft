from __future__ import annotations

import importlib.metadata
import json
import math

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from examples.vortex_eval import fixtures


def test_boundary_values_and_independent_query_oracle():
    table = fixtures.canonical_table(rows=8)
    assert table.column("signed").to_pylist()[:2] == [-(2**63), 2**63 - 1]
    assert table.column("unsigned").to_pylist()[:3] == [0, 2**64 - 1, 2**63]
    measures = table.column("measure").to_pylist()
    assert math.copysign(1, measures[1]) == -1
    assert math.isnan(measures[3])
    assert measures[4:6] == [math.inf, -math.inf]
    assert table.column("payload")[2].as_py() == b"\x00\xff"
    assert fixtures.expected_results(table) == {
        "rows": 8,
        "count_value_nonnull": 6,
        "sum_value": 159,
        "filtered_id": [4, 5, 7],
        "null_value_id": [0, 6],
        "flag_counts": {"false": 3, "true": 3, "null": 2},
    }


def test_seeded_fixtures_are_repeatable_but_seed_matters(tmp_path):
    first = fixtures.generate_fixtures(tmp_path / "first", rows=31, seed=7, mode="parquet-only")
    second = fixtures.generate_fixtures(tmp_path / "second", rows=31, seed=7, mode="parquet-only")
    assert first == second
    assert fixtures.semantic_checksum(fixtures.canonical_table(rows=31, seed=8)) != first["semantic_checksum"]
    assert json.loads((tmp_path / "first/manifest.json").read_text()) == first


@pytest.mark.parametrize("rows", [0, 1, 7, 8, 9, 257])
def test_layouts_cover_identical_rows_and_preserve_empty_schema(tmp_path, rows):
    manifest = fixtures.generate_fixtures(tmp_path, rows=rows, mode="parquet-only")
    assert manifest["mode"] == "parquet-only"
    assert manifest["versions"]["vortex-data"] is None
    assert all(not layout["vortex"] for layout in manifest["layouts"].values())
    for layout_name, layout in manifest["layouts"].items():
        parts = [pq.ParquetFile(tmp_path / path).read() for path in layout["parquet"]]
        actual = pa.concat_tables(parts)
        assert len(actual) == (0 if layout_name == "empty" else rows)
        assert actual.schema == fixtures.canonical_table(rows=0).schema
        assert fixtures.semantic_checksum(actual) == layout["semantic_checksum"]
        assert fixtures.expected_results(actual) == layout["expected_results"]
        assert actual.column("id").to_pylist() == list(range(len(actual)))
    assert pq.ParquetFile(tmp_path / manifest["layouts"]["multi"]["parquet"][0]).metadata.num_rows == 0


def test_checksum_ignores_chunk_layout_but_detects_data_order_and_type_changes():
    table = fixtures.canonical_table(rows=17)
    chunks = pa.concat_tables([table.slice(0, 1), table.slice(1, 7), table.slice(8)])
    assert fixtures.semantic_checksum(table) == fixtures.semantic_checksum(chunks)
    assert fixtures.semantic_checksum(table) != fixtures.semantic_checksum(table.take(list(reversed(range(17)))))
    changed = table.set_column(table.schema.get_field_index("value"), "value", pa.array([1] * 17, type=pa.int64()))
    assert fixtures.semantic_checksum(table) != fixtures.semantic_checksum(changed)
    changed_type = table.set_column(table.schema.get_field_index("value"), "value", table["value"].cast(pa.int32()))
    assert fixtures.semantic_checksum(table) != fixtures.semantic_checksum(changed_type)


def test_default_mode_fails_before_writing_when_vortex_is_unavailable(tmp_path, monkeypatch):
    def missing(_name):
        raise importlib.metadata.PackageNotFoundError("vortex-data")

    monkeypatch.setattr(fixtures.importlib.metadata, "version", missing)
    output = tmp_path / "fixtures"
    with pytest.raises(RuntimeError, match=r"vortex-data==0\.88\.0.*parquet-only"):
        fixtures.generate_fixtures(output)
    assert not output.exists()


def test_refuses_wrong_vortex_version_before_writing(tmp_path, monkeypatch):
    monkeypatch.setattr(fixtures.importlib.metadata, "version", lambda _name: "0.87.0")
    with pytest.raises(RuntimeError, match="refusing an unpinned fixture writer"):
        fixtures.generate_fixtures(tmp_path)
    assert not list(tmp_path.iterdir())


def test_existing_output_is_not_overwritten(tmp_path):
    existing = tmp_path / "keep.txt"
    existing.write_text("user data")
    with pytest.raises(FileExistsError, match="absent or empty"):
        fixtures.generate_fixtures(tmp_path, mode="parquet-only")
    assert existing.read_text() == "user data"
    assert list(tmp_path.iterdir()) == [existing]


def test_roundtrip_corruption_does_not_publish_completion_manifest(tmp_path, monkeypatch):
    real_read = fixtures.pq.ParquetFile

    class CorruptRead:
        def __init__(self, path):
            self.reader = real_read(path)

        def read(self):
            return self.reader.read().slice(0, 0)

    monkeypatch.setattr(fixtures.pq, "ParquetFile", CorruptRead)
    with pytest.raises(RuntimeError, match="round-trip changed"):
        fixtures.generate_fixtures(tmp_path, rows=8, mode="parquet-only")
    assert not (tmp_path / "manifest.json").exists()


@pytest.mark.parametrize("rows", [-1, True, 1.5])
def test_invalid_row_count_is_rejected(rows):
    with pytest.raises(ValueError, match="non-negative integer"):
        fixtures.canonical_table(rows=rows)


def test_vortex_roundtrip_when_pinned_dependency_is_available(tmp_path):
    try:
        version = importlib.metadata.version("vortex-data")
    except importlib.metadata.PackageNotFoundError:
        pytest.skip("vortex-data==0.88.0 is unavailable; Python Vortex compatibility is unverified")
    if version != fixtures.VORTEX_VERSION:
        pytest.skip(f"Need vortex-data=={fixtures.VORTEX_VERSION}; installed version is {version}")
    manifest = fixtures.generate_fixtures(tmp_path, rows=17)
    assert manifest["mode"] == "both"
    assert len(manifest["files"]) == 12
    for layout in manifest["layouts"].values():
        assert len(layout["vortex"]) == len(layout["parquet"])
