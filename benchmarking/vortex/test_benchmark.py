"""Small functional checks; these are not performance benchmarks."""

from __future__ import annotations

import json

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from benchmarking.vortex.fixtures import CASES, SCHEMA, batches, generate, load_manifest, oracle
from benchmarking.vortex.run import (
    CONFIGS,
    check_output,
    file_read_telemetry,
    io_settings,
    query,
    redacted,
    run,
    schedule,
    source_paths,
    summarize,
)


@pytest.fixture(scope="module")
def fixture_dir(tmp_path_factory):
    pytest.importorskip("vortex")
    path = tmp_path_factory.mktemp("vortex-benchmark")
    generate(path, rows=257, batch_rows=31, files=3)
    return path


def test_batch_size_does_not_change_values_and_shuffle_preserves_rows():
    sorted_table = pa.Table.from_batches(list(batches(257, 42, "sorted", 0, 257, 19)))
    shuffled = pa.Table.from_batches(list(batches(257, 42, "shuffled", 0, 257, 37)))
    assert sorted_table.equals(shuffled.sort_by("id"))
    assert shuffled.column("id").to_pylist() != sorted_table.column("id").to_pylist()
    assert max(len(b) for b in batches(257, 42, "shuffled", 0, 257, 19)) <= 19
    changed = pa.Table.from_batches(list(batches(257, 43, "sorted", 0, 257, 31)))
    assert not changed.column("text").equals(sorted_table.column("text"))


def test_oracle_handles_nulls_and_exact_selectivity():
    result = oracle(34, 42)
    present = [i - 5000 for i in range(34) if i not in {0, 17}]
    assert result["scan_narrow"] == [
        {"rows": 34, "sum_id": sum(range(34)), "count_value": 32, "sum_value": sum(present)}
    ]
    assert result["range_0"] == [{"rows": 0, "sum_id": None, "count_value": 0, "sum_value": None}]
    assert result["range_0.5"][0]["rows"] == 17
    assert result["eq_0.5"] == result["range_0.5"]
    assert oracle(1, 42)["scan_full"][0]["sum_flag"] is None


def test_generated_formats_have_identical_values(fixture_dir):
    import vortex

    manifest = load_manifest(fixture_dir / "manifest.json")
    for dataset in manifest["datasets"].values():
        parquet = pa.concat_tables([pq.read_table(fixture_dir / p) for p in dataset["paths"]["parquet"]])
        arrays = []
        for path in dataset["paths"]["vortex"]:
            with vortex.open(str(fixture_dir / path)).to_arrow(schema=SCHEMA) as reader:
                arrays.append(reader.read_all())
        native = pa.concat_tables(arrays).cast(SCHEMA)
        assert native.equals(parquet)
        assert len(native) == 257


@pytest.mark.parametrize("mutation", ["oracle", "coverage", "escape", "unreferenced"])
def test_manifest_rejects_false_provenance(fixture_dir, mutation):
    path = fixture_dir / f"invalid-{mutation}.json"
    manifest = json.loads((fixture_dir / "manifest.json").read_text())
    if mutation == "oracle":
        manifest["oracle"]["scan_narrow"][0]["sum_value"] += 1
    elif mutation == "coverage":
        manifest["files"][0]["start"] = 1
    elif mutation == "escape":
        manifest["files"][0]["path"] = "../outside.parquet"
    else:
        manifest["datasets"].pop("sorted/single")
    path.write_text(json.dumps(manifest))
    with pytest.raises(ValueError):
        load_manifest(path)


def test_manifest_rejects_file_mutation(tmp_path):
    pytest.importorskip("vortex")
    manifest = generate(tmp_path, rows=3, orders=["sorted"], layouts=["single"])
    with (tmp_path / manifest["files"][0]["path"]).open("ab") as target:
        target.write(b"changed")
    with pytest.raises(ValueError, match="Fixture bytes changed"):
        load_manifest(tmp_path / "manifest.json")


def test_schedule_interleaves_complete_rounds_reproducibly():
    first = schedule(["scan_full", "join"], ["sorted/single"], CONFIGS, 1, 5, 42)
    assert first == schedule(["scan_full", "join"], ["sorted/single"], CONFIGS, 1, 5, 42)
    assert first != schedule(["scan_full", "join"], ["sorted/single"], CONFIGS, 1, 5, 43)
    for iteration in range(-1, 5):
        block = [r for r in first if r["iteration"] == iteration]
        assert len(block) == 10
        assert {r["config"] for r in block} == set(CONFIGS)
        assert all(r["warmup"] == (iteration < 0) for r in block)


def test_report_retains_failures_and_regressions():
    common = {
        "dataset": "sorted/single",
        "case": "scan_full",
        "warmup": False,
        "status": "passed",
        "cpu_seconds": 1,
        "peak_rss_bytes": 100,
        "output_sha256": "same",
        "source_read_bytes": 99,
    }
    rows = summarize(
        [
            {**common, "config": "parquet", "wall_seconds": 1},
            {**common, "config": "vortex_11", "wall_seconds": 2},
            {**common, "config": "vortex_00", "status": "failed", "error": "failure"},
        ]
    )
    assert next(r for r in rows if r["config"] == "vortex_11")["parquet_time_over_this_time"] == 0.5
    assert next(r for r in rows if r["config"] == "vortex_00")["status"] == "failed"
    with pytest.raises(AssertionError, match="Query output differs"):
        check_output([{"rows": 2}], [{"rows": 1}])


@pytest.mark.parametrize("reported_bytes", [0, 4096])
def test_local_parquet_file_bytes_are_unavailable_even_when_a_counter_exists(reported_bytes):
    metrics = [{"category": "Source", "type": "ScanTask", "stats": [("bytes.read", {"value": reported_bytes})]}]
    result = file_read_telemetry(metrics, config="parquet", storage_root=None)
    assert result["source_read_bytes"] is None
    assert result["source_read_bytes_coverage"] == "unavailable"
    assert "Local Parquet" in result["source_read_bytes_unavailable_reason"]
    assert result["raw_file_source_read_bytes"] == reported_bytes


@pytest.mark.parametrize(
    "config,storage_root",
    [("vortex_11", None), ("vortex_11", "s3://bucket"), ("parquet", "s3://bucket"), ("parquet", "https://host/prefix")],
)
def test_file_bytes_exclude_join_in_memory_source_and_preserve_instrumented_paths(config, storage_root):
    metrics = [
        {"category": "Source", "type": "ScanTask", "stats": [("bytes.read", {"value": 1024})]},
        {"category": "Source", "type": "ScanTask", "stats": [("bytes.read", {"value": 512})]},
        {"category": "Source", "type": "InMemoryScan", "stats": [("bytes.read", {"value": 2048})]},
        {"category": "BlockingSink", "type": "ScanTask", "stats": [("bytes.read", {"value": 8192})]},
    ]
    result = file_read_telemetry(metrics, config=config, storage_root=storage_root)
    assert result["source_read_bytes"] == 1536
    assert result["raw_file_source_read_bytes"] == 1536
    assert result["raw_non_file_source_read_bytes"] == 2048
    assert result["source_read_bytes_coverage"] == "executed_file_sources"
    assert result["source_read_bytes_unavailable_reason"] is None


def test_missing_file_counter_is_unavailable_while_instrumented_zero_is_valid():
    metrics = [{"category": "Source", "type": "InMemoryScan", "stats": [("bytes.read", {"value": 2048})]}]
    missing = file_read_telemetry(metrics, config="vortex_11", storage_root=None)
    assert missing["source_read_bytes"] is None
    assert missing["raw_file_source_read_bytes"] is None
    metrics.append({"category": "Source", "type": "ScanTask", "stats": [("bytes.read", {"value": 0})]})
    assert file_read_telemetry(metrics, config="vortex_11", storage_root=None)["source_read_bytes"] == 0


def test_remote_paths_and_secret_redaction(fixture_dir, monkeypatch):
    manifest = load_manifest(fixture_dir / "manifest.json")
    dataset = manifest["datasets"]["sorted/single"]
    assert source_paths(fixture_dir / "manifest.json", dataset, "parquet", "s3://bucket/prefix")[0].startswith(
        "s3://bucket/prefix/sorted/single/"
    )
    with pytest.raises(ValueError, match="plain"):
        source_paths(fixture_dir / "manifest.json", dataset, "parquet", "https://user:secret@host/path")
    monkeypatch.setenv("DAFT_VORTEX_BENCH_IO_JSON", json.dumps({"s3": {"access_key": "secret-value"}}))
    assert redacted("failure secret-value") == "failure <redacted>"


def test_rejected_remote_credentials_are_never_written(fixture_dir, tmp_path):
    target = tmp_path / "must-not-exist"
    with pytest.raises(ValueError, match="plain"):
        run(
            fixture_dir / "manifest.json",
            target,
            cases=["scan_narrow"],
            rounds=1,
            warmups=0,
            allow_dev=True,
            storage_root="https://:secret@host/prefix",
        )
    assert not target.exists()


@pytest.mark.parametrize(
    "endpoint",
    [
        "https://user:sentinel@host/prefix",
        "https://host/prefix?token=sentinel",
        "https://host/prefix#sentinel",
        "https://user:sentinel＠host/prefix",
    ],
)
def test_unsafe_s3_endpoint_is_rejected_before_artifacts(fixture_dir, tmp_path, monkeypatch, endpoint):
    target = tmp_path / "must-not-exist"
    monkeypatch.setenv("DAFT_VORTEX_BENCH_IO_JSON", json.dumps({"s3": {"endpoint_url": endpoint}}))
    with pytest.raises(ValueError, match="plain HTTP") as error:
        run(fixture_dir / "manifest.json", target, rounds=1, warmups=0, allow_dev=True)
    assert "sentinel" not in str(error.value)
    assert not target.exists()


def test_safe_s3_endpoint_preserves_connection_settings(monkeypatch):
    settings = {"s3": {"endpoint_url": "http://127.0.0.1:9000", "key_id": "test-key", "access_key": "test-secret"}}
    monkeypatch.setenv("DAFT_VORTEX_BENCH_IO_JSON", json.dumps(settings))
    assert io_settings() == settings


def test_performance_trial_count_cannot_be_silently_weakened(tmp_path):
    with pytest.raises(ValueError, match=">=5 rounds"):
        run(tmp_path / "missing.json", tmp_path / "report", rounds=1, warmups=0)


@pytest.mark.parametrize("size", ["inf", "nan", "1e308"])
def test_cli_rejects_nonfinite_or_excessive_sizes(size, tmp_path, monkeypatch, capsys):
    from benchmarking.vortex.__main__ import main

    target = tmp_path / "fixtures"
    monkeypatch.setattr("sys.argv", ["vortex", "generate", "--output", str(target), "--logical-gib", size])
    with pytest.raises(SystemExit) as error:
        main()
    assert error.value.code == 1
    assert "finite and positive" in capsys.readouterr().err
    assert not target.exists()


@pytest.mark.parametrize("config", CONFIGS)
def test_native_queries_match_independent_oracle(fixture_dir, config):
    import daft

    if config != "parquet" and not hasattr(daft.daft, "read_vortex_schema"):
        pytest.skip("requires native Vortex feature")
    manifest = load_manifest(fixture_dir / "manifest.json")
    for case in CASES:
        spec = {
            "manifest": str(fixture_dir / "manifest.json"),
            "dataset": "shuffled/multi",
            "config": config,
            "case": case,
            "settings": {"batch_size": 31, "max_concurrent_requests": 8, "max_in_flight_bytes": 67108864},
        }
        actual = query(spec, manifest).to_arrow().to_pylist()
        check_output(actual, manifest["oracle"][case])


def test_isolated_cli_report(fixture_dir, tmp_path):
    import daft

    if not hasattr(daft.daft, "read_vortex_schema"):
        pytest.skip("requires native Vortex feature")
    report = run(
        fixture_dir / "manifest.json",
        tmp_path / "report",
        cases=["scan_full"],
        orders=["sorted"],
        layouts=["multi"],
        rounds=1,
        warmups=0,
        allow_dev=True,
    )
    assert report["status"] == "passed"
    assert len(report["trials"]) == 5
    assert {r["output_sha256"] for r in report["trials"]} == {report["trials"][0]["output_sha256"]}
    assert all(r["peak_rss_bytes"] > 0 and r["read_requests"] is None for r in report["trials"])
    parquet = next(trial for trial in report["trials"] if trial["config"] == "parquet")
    assert parquet["source_read_bytes"] is None
    assert parquet["source_read_bytes_coverage"] == "unavailable"
    assert all(trial["source_read_bytes"] > 0 for trial in report["trials"] if trial["config"] != "parquet")
    assert (tmp_path / "report" / "report.md").is_file()
