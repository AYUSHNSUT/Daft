"""Scheduling, measurement scope and diagnostics tests; no benchmark queries run."""

from __future__ import annotations

import copy
import json
import subprocess
from datetime import timedelta
from pathlib import Path
from types import SimpleNamespace

import pytest

from benchmarking.vortex import metadata_eval as evaluation
from benchmarking.vortex.fixtures import SCHEMA


@pytest.mark.parametrize("concurrency", [[True, 8], [0, 8], [1, 65], [1, 1], [8], []])
def test_invalid_concurrency_cannot_change_the_baseline(concurrency):
    with pytest.raises(ValueError, match="concurrency"):
        evaluation.validate_options(concurrency, ["scan_narrow"], ["query"], 20, 1, False, 60)


@pytest.mark.parametrize("timeout", [float("nan"), float("inf"), 0, -1, True])
def test_timeout_must_be_finite_and_positive(timeout):
    with pytest.raises(ValueError, match="timeout"):
        evaluation.validate_options([1, 8], ["scan_narrow"], ["query"], 20, 1, False, timeout)


def test_performance_minimums_and_separate_metadata_jobs():
    with pytest.raises(ValueError, match="20 measured"):
        evaluation.validate_options([1, 8], ["scan_narrow"], ["query"], 5, 1, False, 60)
    with pytest.raises(ValueError, match="warmup"):
        evaluation.validate_options([1, 8], ["scan_narrow"], ["query"], 20, 0, False, 60)
    jobs = evaluation.schedule(["scan_narrow", "join"], ["sorted/multi"], [1, 8], ["query", "metadata"], 1, 3, 42)
    assert jobs == evaluation.schedule(
        ["scan_narrow", "join"], ["sorted/multi"], [1, 8], ["query", "metadata"], 1, 3, 42
    )
    assert jobs != evaluation.schedule(
        ["scan_narrow", "join"], ["sorted/multi"], [1, 8], ["query", "metadata"], 1, 3, 43
    )
    for iteration in range(-1, 3):
        block = [job for job in jobs if job["iteration"] == iteration]
        assert len(block) == 8
        assert {(job["mode"], job["case"], job["reader"], job["metadata_concurrency"]) for job in block} == {
            (mode, case, "vortex", concurrency)
            for mode, case in [("query", "scan_narrow"), ("query", "join"), ("metadata", "footer_metadata")]
            for concurrency in (1, 8)
        } | {("query", case, "parquet", None) for case in ("scan_narrow", "join")}
        assert all(job["warmup"] == (iteration < 0) for job in block)
    metadata_jobs = evaluation.schedule(["scan_narrow"], ["sorted/multi"], [1, 8], ["metadata"], 0, 1, 42)
    assert len(metadata_jobs) == 2
    assert all(job["reader"] == "vortex" and job["mode"] == "metadata" for job in metadata_jobs)


def diagnostics():
    return {
        "num_files": 2,
        "max_concurrent_files": 8,
        "elapsed_seconds": 0.5,
        "head_requests": 2,
        "get_requests": 2,
        "list_requests": 0,
        "bytes_read": 100,
    }


def test_metadata_validation_checks_order_schema_counts_and_finite_counters():
    entries = [("schema", 3), ("schema", 7)]
    evaluation.validate_metadata(entries, diagnostics(), [3, 7], "schema", 8)
    with pytest.raises(AssertionError, match="ordering"):
        evaluation.validate_metadata(entries[::-1], diagnostics(), [3, 7], "schema", 8)
    with pytest.raises(AssertionError, match="schema"):
        evaluation.validate_metadata(entries, diagnostics(), [3, 7], "different", 8)
    for key, value in [
        ("num_files", 1),
        ("max_concurrent_files", 9),
        ("elapsed_seconds", float("nan")),
        ("bytes_read", -1),
        ("get_requests", 1.5),
    ]:
        broken = {**diagnostics(), key: value}
        with pytest.raises(AssertionError):
            evaluation.validate_metadata(entries, broken, [3, 7], "schema", 8)


def sample(mode, concurrency, iteration, wall, reader="vortex"):
    return {
        "mode": mode,
        "case": "scan_narrow" if mode == "query" else "footer_metadata",
        "dataset": "sorted/multi",
        "reader": reader,
        "metadata_concurrency": concurrency,
        "iteration": iteration,
        "warmup": iteration < 0,
        "status": "passed",
        "wall_seconds": wall,
        "cpu_seconds": 0.1,
        "peak_rss_bytes": 1000,
        "output_sha256": mode,
        "source_read_bytes": 500 if mode == "query" else None,
        "metadata_diagnostics": diagnostics() if mode == "metadata" else None,
    }


def test_reports_do_not_mix_metadata_latency_with_query_speedups():
    trials = [
        sample(mode, concurrency, iteration, (10 if mode == "query" else 2) / concurrency)
        for mode in ("query", "metadata")
        for concurrency in (1, 8)
        for iteration in (-1, 0, 1)
    ]
    trials += [sample("query", None, iteration, 5, "parquet") for iteration in (-1, 0, 1)]
    trials[0]["wall_seconds"] = 9999
    summary = evaluation.summarize(trials)
    query = next(row for row in summary if row["mode"] == "query" and row["metadata_concurrency"] == 8)
    metadata = next(row for row in summary if row["mode"] == "metadata" and row["metadata_concurrency"] == 8)
    assert query["metrics"]["wall_seconds"]["median"] == 1.25
    assert query["concurrency_1_time_over_this_time"] == 8
    assert query["parquet_time_over_this_time"] == 4
    assert "metadata_diagnostics" not in query
    assert metadata["metrics"]["source_read_bytes"]["available_samples"] == 0
    assert metadata["metadata_diagnostics"]["get_requests"]["median"] == 2
    assert "parquet_time_over_this_time" not in metadata
    assert "not query speedups" in evaluation.markdown({"status": "passed", "purpose": "test", "summary": summary})
    broken = copy.deepcopy(trials)
    broken[-1]["output_sha256"] = "changed"
    with pytest.raises(AssertionError, match="digests differ"):
        evaluation.summarize(broken)
    broken = copy.deepcopy(trials)
    for trial in broken:
        if trial["reader"] == "parquet":
            trial["output_sha256"] = "wrong-but-repeatable"
    with pytest.raises(AssertionError, match="different outputs"):
        evaluation.summarize(broken)


def manifest():
    return {
        "generator": {"rows": 10},
        "datasets": {
            "sorted/multi": {
                "order": "sorted",
                "layout": "multi",
                "paths": {
                    "vortex": ["first.vortex", "second.vortex"],
                    "parquet": ["first.parquet", "second.parquet"],
                },
            }
        },
        "files": [{"path": "first.vortex", "rows": 3}, {"path": "second.vortex", "rows": 7}],
        "oracle": {"scan_narrow": [{"rows": 10}]},
    }


def test_invalid_endpoint_cannot_create_artifacts(tmp_path, monkeypatch):
    monkeypatch.setattr(evaluation, "load_manifest", lambda _: manifest())
    monkeypatch.setenv("DAFT_VORTEX_BENCH_IO_JSON", json.dumps({"s3": {"endpoint_url": "https://user:secret@host"}}))
    target = tmp_path / "results"
    with pytest.raises(ValueError, match="plain HTTP"):
        evaluation.run(tmp_path / "manifest.json", target, rounds=1, warmups=0, allow_dev=True)
    assert not target.exists()


def test_orchestrator_retains_failures_and_continues_other_modes(tmp_path, monkeypatch):
    path = tmp_path / "manifest.json"
    path.write_text("{}")
    monkeypatch.setattr(evaluation, "load_manifest", lambda _: manifest())
    monkeypatch.setattr(evaluation, "require_api", lambda: None)
    monkeypatch.setattr(evaluation, "environment", lambda: {"build_type": "dev"})
    seen = []

    def child(command, **kwargs):
        spec = json.loads(Path(command[command.index("--worker-spec") + 1]).read_text())
        seen.append(spec)
        assert kwargs["env"]["DAFT_ANALYTICS_ENABLED"] == "0"
        if spec["mode"] == "query" and spec["metadata_concurrency"] == 8:
            return subprocess.CompletedProcess(command, 1, stderr="controlled failure")
        value = sample(spec["mode"], spec["metadata_concurrency"], spec["iteration"], 1, spec["reader"])
        Path(command[command.index("--output") + 1]).write_text(json.dumps(value))
        return subprocess.CompletedProcess(command, 0, stderr="")

    monkeypatch.setattr(evaluation.subprocess, "run", child)
    target = tmp_path / "report"
    result = evaluation.run(path, target, cases=["scan_narrow"], rounds=1, warmups=0, allow_dev=True)
    assert len(seen) == 5
    assert result["status"] == "failed"
    assert len(result["trials"]) == 5
    assert sum(trial["status"] == "failed" for trial in result["trials"]) == 1
    assert json.loads((target / "report.json").read_text())["status"] == "failed"
    assert (target / "report.md").is_file()


@pytest.mark.parametrize("reader_name", ["vortex", "parquet"])
def test_query_preserves_reader_controls_and_normal_parquet_defaults(tmp_path, monkeypatch, reader_name):
    import daft

    received = {}

    def reader(paths, **kwargs):
        received.update(kwargs)
        assert paths == [str(tmp_path / f"first.{reader_name}"), str(tmp_path / f"second.{reader_name}")]
        raise RuntimeError("stop before building or running the query")

    monkeypatch.setattr(daft, f"read_{reader_name}", reader)
    monkeypatch.setattr(evaluation, "io_config", lambda: None)
    spec = {
        "manifest": str(tmp_path / "manifest.json"),
        "dataset": "sorted/multi",
        "case": "scan_narrow",
        "reader": reader_name,
        "metadata_concurrency": 1,
    }
    with pytest.raises(RuntimeError, match="stop before"):
        evaluation.query(spec, manifest())
    if reader_name == "vortex":
        assert received["metadata_concurrency"] == 1
        assert received["max_concurrent_requests"] == 8
        assert received["max_in_flight_bytes"] == 67108864
        assert received["enable_projection_pushdown"] is received["enable_filter_pushdown"] is True
    else:
        assert received == {"io_config": None}


@pytest.mark.parametrize("reader_name", ["vortex", "parquet"])
def test_query_worker_times_source_construction_through_collection(tmp_path, monkeypatch, reader_name):
    import daft

    events = []
    path = tmp_path / "manifest.json"
    path.write_text(json.dumps(manifest()))

    class Frame:
        def agg(self, *args):
            return self

        def collect(self):
            events.append("collect")
            return self

        def to_arrow(self):
            return SimpleNamespace(to_pylist=lambda: [{"rows": 10}])

        @property
        def metrics(self):
            return SimpleNamespace(
                to_pylist=lambda: [
                    {
                        "category": "Source",
                        "type": "ScanTask",
                        "duration": timedelta(),
                        "stats": [["bytes.read", {"value": 123}]],
                    }
                ]
            )

        def explain(self, **kwargs):
            kwargs["file"].write("diagnostic plan")

    def build_query(*_):
        events.append("construct_source")
        return Frame()

    def clock():
        events.append("wall_clock")
        return len(events) * 1000

    monkeypatch.setattr(evaluation, "require_api", lambda: None)
    monkeypatch.setattr(daft, "set_runner_native", lambda: None)
    monkeypatch.setattr(daft, "from_pydict", lambda _: Frame())
    monkeypatch.setattr(evaluation, "query", build_query)
    monkeypatch.setattr(evaluation.time, "perf_counter_ns", clock)
    result = evaluation.worker(
        {
            "manifest": str(path),
            "dataset": "sorted/multi",
            "case": "scan_narrow",
            "mode": "query",
            "reader": reader_name,
            "metadata_concurrency": 8,
            "allow_dev": True,
        }
    )
    assert events[:5] == ["collect", "wall_clock", "construct_source", "collect", "wall_clock"]
    assert result["source_read_bytes"] == (123 if reader_name == "vortex" else None)
    assert result["metadata_diagnostics"] is None
    assert result["output"] == [{"rows": 10}]


def test_metadata_worker_uses_bulk_api_and_checks_ordered_counts(tmp_path, monkeypatch):
    import daft
    from daft.schema import Schema

    path = tmp_path / "manifest.json"
    path.write_text(json.dumps(manifest()))
    arrow = SCHEMA.with_metadata(None)
    fields = [field.with_metadata({b"daft.vortex.nullable": b"true"}) for field in arrow]
    import pyarrow as pa

    schema = Schema.from_pyarrow_schema(pa.schema(fields))._schema
    received = {}

    def bulk(paths, **kwargs):
        received.update(kwargs)
        assert paths == [str(tmp_path / "first.vortex"), str(tmp_path / "second.vortex")]
        return [(schema, 3), (schema, 7)], diagnostics()

    warmup = SimpleNamespace(agg=lambda *_: SimpleNamespace(collect=lambda: None))
    monkeypatch.setattr(daft, "from_pydict", lambda _: warmup)
    monkeypatch.setattr(daft, "set_runner_native", lambda: None)
    monkeypatch.setattr(daft.daft, "read_vortex_metadata_bulk", bulk, raising=False)
    monkeypatch.setattr(evaluation, "io_config", lambda: None)
    result = evaluation.worker(
        {
            "manifest": str(path),
            "dataset": "sorted/multi",
            "case": "footer_metadata",
            "mode": "metadata",
            "reader": "vortex",
            "metadata_concurrency": 8,
            "allow_dev": True,
        }
    )
    assert received == {
        "io_config": None,
        "max_concurrent_files": 8,
        "max_concurrent_requests": 8,
        "max_in_flight_bytes": 67108864,
    }
    assert result["output"] == [{"rows": 3}, {"rows": 7}]
    assert result["metadata_diagnostics"]["bytes_read"] == 100
    assert "source_read_bytes" not in result
