from __future__ import annotations

import asyncio
import sys
from contextlib import nullcontext
from pathlib import Path
from types import SimpleNamespace

import pyarrow as pa
import pytest

import daft
from daft import daft as native
from daft.io import _vortex, _vortex_diagnostics
from daft.io.pushdowns import Pushdowns
from daft.io.source import DataSourceTask
from daft.pickle import cloudpickle
from daft.schema import Schema


@pytest.mark.parametrize("enabled", [False, True])
@pytest.mark.parametrize(
    "name,value",
    [
        ("read_coalesce_gap_bytes", -1),
        ("read_coalesce_gap_bytes", True),
        ("read_coalesce_gap_bytes", 1.5),
        ("read_coalesce_gap_bytes", sys.maxsize + 1),
        ("read_coalesce_max_bytes", 0),
        ("read_coalesce_max_bytes", False),
        ("read_coalesce_max_bytes", "8"),
        ("read_coalesce_max_bytes", sys.maxsize + 1),
        ("discovery_concurrency", 0),
        ("discovery_concurrency", 65),
        ("discovery_concurrency", True),
        ("discovery_concurrency", None),
    ],
)
def test_vortex_read_controls_validate_before_io_even_when_disabled(name, value, enabled):
    with pytest.raises(ValueError, match=name):
        daft.read_vortex("unused.vortex", enable_read_coalescing=enabled, **{name: value})


@pytest.mark.parametrize("value", [0, 1, None, "false"])
def test_vortex_coalescing_switch_requires_bool_before_io(value):
    with pytest.raises(TypeError, match="enable_read_coalescing"):
        daft.read_vortex("unused.vortex", enable_read_coalescing=value)


@pytest.mark.parametrize("concurrency", [1, 3])
def test_discovery_windows_preserve_mixed_input_order_and_deduplicate(tmp_path, monkeypatch, concurrency):
    local = str(tmp_path / "local.vortex")
    Path(local).touch()
    remote = [f"s3://bucket/{index}.vortex" for index in range(7)]
    paths = [*remote[:2], local, *remote[2:], remote[0]]
    calls = []

    def bulk(inputs, **kwargs):
        assert len(inputs) <= concurrency
        calls.append(list(inputs))
        return [{"files": [{"path": path, "size": 1}]} for path in inputs], None

    monkeypatch.setattr(native, "_io_glob_bulk", bulk, raising=False)
    monkeypatch.setattr(_vortex, "glob_path_with_stats", lambda *args, **kwargs: SimpleNamespace(file_paths=[local]))
    assert _vortex._resolve_vortex_paths(paths, None, concurrency) == [*remote[:2], local, *remote[2:]]
    expected = [remote[:2][start : start + concurrency] for start in range(0, 2, concurrency)]
    suffix = [*remote[2:], remote[0]]
    expected += [suffix[start : start + concurrency] for start in range(0, len(suffix), concurrency)]
    assert calls == expected


def test_discovery_directory_fallback_error_precedes_later_parallel_error(monkeypatch):
    first, later = "s3://bucket/directory", "s3://bucket/later.vortex"
    calls = []

    def bulk(inputs, **kwargs):
        calls.append(list(inputs))
        if inputs == [first, later]:
            return [
                {"files": [{"path": first + "/child.vortex", "size": 1}]},
                {"error": RuntimeError("later discovery failed")},
            ], None
        assert inputs == [first + "/**/*.vortex"]
        return [{"error": FileNotFoundError("earlier fallback failed")}], None

    monkeypatch.setattr(native, "_io_glob_bulk", bulk, raising=False)
    with pytest.raises(FileNotFoundError, match="earlier fallback failed") as failure:
        _vortex._resolve_vortex_paths([first, later], None, 2)
    assert "later discovery failed" not in str(failure.value)
    assert calls == [[first, later], [first + "/**/*.vortex"]]


@pytest.mark.parametrize("global_failure", [False, True])
def test_parallel_discovery_errors_redact_signed_urls(monkeypatch, global_failure):
    path = "https://sentinel-user:sentinel-password@host/data.vortex?token=sentinel-query"

    def bulk(inputs, **kwargs):
        error = OSError(f"failed: {inputs[0]} {path}")
        if global_failure:
            raise error
        return [{"error": error}], None

    monkeypatch.setattr(native, "_io_glob_bulk", bulk, raising=False)
    with pytest.raises(OSError, match="Vortex discovery") as failure:
        _vortex._resolve_vortex_paths([path], None)
    for secret in ("sentinel-user", "sentinel-password", "sentinel-query"):
        assert secret not in str(failure.value)


@pytest.mark.parametrize("active", [False, True])
def test_discovery_metrics_are_opt_in(monkeypatch, active):
    path = "https://host/data.vortex?fixture=1"
    capture_token = object() if active else None
    observed = []
    recorded = []

    def bulk(inputs, **kwargs):
        observed.append(kwargs["collect_metrics"])
        return [{"files": [{"path": path, "size": 1}]}], {"num_inputs": 1} if active else None

    monkeypatch.setattr(native, "_io_glob_bulk", bulk, raising=False)
    monkeypatch.setattr(_vortex_diagnostics, "capture_token", lambda: capture_token)
    monkeypatch.setattr(_vortex_diagnostics, "stage", lambda _: nullcontext())
    monkeypatch.setattr(
        _vortex_diagnostics, "record_discovery", lambda metrics, *, token: recorded.append((metrics, token))
    )
    assert _vortex._resolve_vortex_paths([path], None) == [path]
    assert observed == [active]
    assert recorded == ([({"num_inputs": 1}, capture_token)] if active else [])


def test_discovery_metrics_stay_with_original_capture(monkeypatch):
    path = "https://host/data.vortex"
    completed = []
    monkeypatch.setattr(_vortex_diagnostics, "_capture", None)
    monkeypatch.setattr(native, "_vortex_diagnostics_start", lambda: None, raising=False)
    monkeypatch.setattr(native, "_vortex_diagnostics_finish", lambda: {"events": []}, raising=False)

    def bulk(inputs, **kwargs):
        assert kwargs["collect_metrics"] is True
        # A native call can release the GIL while another thread finishes one
        # capture and starts the next. Its late counters belong to neither report.
        completed.append(_vortex_diagnostics.finish())
        _vortex_diagnostics.start()
        return [{"files": [{"path": path, "size": 1}]}], {"num_inputs": 1, "head_requests": 7}

    monkeypatch.setattr(native, "_io_glob_bulk", bulk, raising=False)
    _vortex_diagnostics.start()
    try:
        assert _vortex._resolve_vortex_paths([path], None) == [path]
        completed.append(_vortex_diagnostics.finish())
    finally:
        if _vortex_diagnostics.active():
            _vortex_diagnostics.finish()
    for capture in completed:
        discovery = capture["python"]["discovery"]
        assert discovery["head_requests"] == discovery["bulk_calls"] == discovery["inputs"] == 0


def test_native_bulk_discovery_retains_per_input_outcomes_without_vortex_feature(tmp_path):
    first = tmp_path / "first.bin"
    second = tmp_path / "second.bin"
    first.write_bytes(b"first")
    second.write_bytes(b"second")
    outcomes, metrics = native._io_glob_bulk(
        [str(first), str(tmp_path / "missing.bin"), str(second)], collect_metrics=True
    )
    assert outcomes[0]["files"][0]["path"].endswith("first.bin")
    assert isinstance(outcomes[1]["error"], FileNotFoundError)
    assert outcomes[2]["files"][0]["path"].endswith("second.bin")
    assert metrics["num_inputs"] == 3
    assert metrics["head_requests"] == metrics["get_requests"] == metrics["list_requests"] == 0
    assert native._io_glob_bulk([], collect_metrics=False) == ([], None)


@pytest.mark.parametrize("value", [0, -1, 65, True, False, 1.5, "8", None])
def test_native_discovery_concurrency_validates_before_io(value):
    with pytest.raises((TypeError, ValueError, OverflowError)) as failure:
        native._io_glob_bulk(["unused-missing-discovery.vortex"], max_concurrent_inputs=value)
    assert "unused-missing-discovery.vortex" not in str(failure.value)


@pytest.mark.parametrize("name,value", [("read_coalesce_gap_bytes", -1), ("read_coalesce_max_bytes", 0)])
def test_native_config_rejects_invalid_coalescing_bounds_without_reader_feature(name, value):
    with pytest.raises((TypeError, ValueError, OverflowError)):
        native.VortexSourceConfig(enable_read_coalescing=False, **{name: value})


@pytest.mark.parametrize("pickle_source", [False, True])
def test_source_preserves_coalescing_controls_through_schema_metadata_tasks_and_pickle(
    tmp_path, monkeypatch, pickle_source
):
    path = str(tmp_path / "source.vortex")
    Path(path).touch()
    schema = Schema.from_pyarrow_schema(
        pa.schema([pa.field("id", pa.int64(), metadata={b"daft.vortex.nullable": b"true"})])
    )
    observed = {}

    def infer(path, **options):
        observed["schema"] = options
        return schema._schema

    def metadata(paths, **options):
        observed["metadata"] = options
        return [(schema._schema, 3)], None

    monkeypatch.setattr(native, "read_vortex_schema", infer, raising=False)
    monkeypatch.setattr(native, "read_vortex_metadata_bulk", metadata, raising=False)
    monkeypatch.setattr(native, "VortexSourceConfig", lambda **kwargs: SimpleNamespace(**kwargs))
    monkeypatch.setattr(DataSourceTask, "vortex", staticmethod(lambda **kwargs: kwargs))
    source = _vortex.VortexSource(
        [path],
        None,
        2,
        enable_read_coalescing=False,
        read_coalesce_gap_bytes=0,
        read_coalesce_max_bytes=8192,
        max_in_flight_bytes=1024,
        discovery_concurrency=3,
    )
    if pickle_source:
        source = cloudpickle.loads(cloudpickle.dumps(source))

    async def tasks():
        return [task async for task in source.get_tasks(Pushdowns())]

    task = asyncio.run(tasks())[0]
    assert source._discovery_concurrency == 3
    for options in (observed["schema"], observed["metadata"], vars(task["vortex_config"])):
        assert options["enable_read_coalescing"] is False
        assert options["read_coalesce_gap_bytes"] == 0
        assert options["read_coalesce_max_bytes"] == 8192
        assert options["max_in_flight_bytes"] == 1024
