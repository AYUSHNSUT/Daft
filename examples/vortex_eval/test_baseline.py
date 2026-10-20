from __future__ import annotations

import json
from pathlib import Path

import pyarrow as pa
import pytest
from baseline import _check_result, load_manifest, run_baseline
from fixtures import canonical_table, expected_results, generate_fixtures, semantic_checksum


def test_native_parquet_baseline_checks_all_layouts_and_queries(tmp_path: Path) -> None:
    root = tmp_path / "fixtures"
    generate_fixtures(root, rows=17, mode="parquet-only")
    output = tmp_path / "baseline.json"
    report = run_baseline(root / "manifest.json", output, repeats=1, batch_size=13)
    assert report["status"] == "passed", report["cases"]
    assert len(report["cases"]) == 9
    assert report["native_vortex_integration"] is False
    assert report["reader_settings"] == {
        "batch_size_rows": None,
        "batch_size_policy": "Daft native Parquet reader defaults",
    }
    assert all(len(case["samples_ms"]) == 1 for case in report["cases"])
    for case in report["cases"]:
        plan = (output.parent / case["plan"]).read_text()
        assert "Optimized Logical Plan" in plan
        assert "Physical Plan" in plan
    assert json.loads(output.read_text())["status"] == "passed"


@pytest.mark.parametrize("settings", [{"enable_projection_pushdown": False}, {"enable_filter_pushdown": False}])
def test_physical_pushdown_switches_cannot_mislabel_other_readers(tmp_path: Path, settings: dict) -> None:
    with pytest.raises(ValueError, match="require source=vortex-native"):
        run_baseline(tmp_path / "manifest.json", tmp_path / "report.json", **settings)


@pytest.mark.parametrize("projection,filters", [(False, False), (True, False), (False, True), (True, True)])
def test_native_vortex_baseline_checks_and_records_optimization_settings(
    tmp_path: Path, projection: bool, filters: bool
) -> None:
    import daft

    if not hasattr(daft.daft, "_vortex_scan_diagnostics"):
        pytest.skip("requires the Phase 2 native Vortex extension")
    pytest.importorskip("vortex", reason="fixture generation requires the optional vortex-data package")
    generate_fixtures(tmp_path, rows=17, mode="both")
    report = run_baseline(
        tmp_path / "manifest.json",
        tmp_path / "report.json",
        source="vortex-native",
        repeats=1,
        batch_size=7,
        enable_projection_pushdown=projection,
        enable_filter_pushdown=filters,
    )
    assert report["status"] == "passed", report["cases"]
    assert len(report["cases"]) == 9
    assert report["reader_settings"]["enable_projection_pushdown"] is projection
    assert report["reader_settings"]["enable_filter_pushdown"] is filters


def test_changed_fixture_is_rejected_before_running_a_query(tmp_path: Path) -> None:
    generate_fixtures(tmp_path, rows=9, mode="parquet-only")
    path = tmp_path / "single" / "data.parquet"
    path.write_bytes(path.read_bytes() + b"changed")
    with pytest.raises(ValueError, match="Fixture bytes changed"):
        load_manifest(tmp_path / "manifest.json")


def test_parquet_only_fixture_cannot_report_vortex_success(tmp_path: Path) -> None:
    generate_fixtures(tmp_path, mode="parquet-only")
    with pytest.raises(ValueError, match="no vortex files"):
        run_baseline(tmp_path / "manifest.json", tmp_path / "baseline.json", source="vortex-reference")


def test_baseline_does_not_overwrite_its_fixture_manifest(tmp_path: Path) -> None:
    generate_fixtures(tmp_path, rows=1, mode="parquet-only")
    path = tmp_path / "manifest.json"
    original = path.read_bytes()
    with pytest.raises(ValueError, match="must not overwrite"):
        run_baseline(path, path)
    assert path.read_bytes() == original


def test_scan_validation_rejects_numeric_type_changes_without_value_changes() -> None:
    canonical = canonical_table(rows=8)
    actual = canonical.set_column(
        canonical.schema.get_field_index("value"), "value", canonical["value"].cast(pa.int32())
    )
    with pytest.raises(AssertionError, match="type changed for value.*expected int64, found int32"):
        _check_result("scan_all", actual, canonical, expected_results(canonical))


@pytest.mark.parametrize(
    "column,dtype",
    [
        ("text", pa.large_string()),
        ("text", pa.string_view()),
        ("payload", pa.large_binary()),
        ("payload", pa.binary_view()),
    ],
)
def test_scan_validation_accepts_string_and_binary_representations(column: str, dtype: pa.DataType) -> None:
    canonical = canonical_table(rows=8)
    actual = canonical.set_column(canonical.schema.get_field_index(column), column, canonical[column].cast(dtype))
    assert _check_result("scan_all", actual, canonical, expected_results(canonical)) == semantic_checksum(canonical)


@pytest.mark.parametrize("missing_layouts", [("single", "multi", "empty"), ("multi",)])
def test_missing_layouts_cannot_produce_a_passing_report(tmp_path: Path, missing_layouts: tuple[str, ...]) -> None:
    root = tmp_path / "fixtures"
    manifest = generate_fixtures(root, rows=9, mode="parquet-only")
    for layout in missing_layouts:
        del manifest["layouts"][layout]
    path = root / "manifest.json"
    path.write_text(json.dumps(manifest))
    output = tmp_path / "baseline.json"
    with pytest.raises(ValueError, match="exactly the single, multi, and empty layouts"):
        run_baseline(path, output, repeats=1)
    assert not output.exists()


@pytest.mark.parametrize("relative", ["unlisted.parquet", "../outside.parquet"])
def test_executed_layout_paths_must_have_validated_file_records(tmp_path: Path, relative: str) -> None:
    manifest = generate_fixtures(tmp_path, rows=9, mode="parquet-only")
    manifest["layouts"]["single"]["parquet"] = [relative]
    path = tmp_path / "manifest.json"
    path.write_text(json.dumps(manifest))
    with pytest.raises(ValueError, match="Layout references an unlisted fixture"):
        load_manifest(path)


@pytest.mark.parametrize("field,replacement", [("layout", "multi"), ("format", "vortex")])
def test_layout_references_must_match_record_metadata(tmp_path: Path, field: str, replacement: str) -> None:
    manifest = generate_fixtures(tmp_path, rows=9, mode="parquet-only")
    manifest["files"][0][field] = replacement
    path = tmp_path / "manifest.json"
    path.write_text(json.dumps(manifest))
    with pytest.raises(ValueError, match="Layout/format does not match fixture record"):
        load_manifest(path)
