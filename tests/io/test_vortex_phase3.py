"""Socket-independent configuration, transport, and local I/O checks.

Real HTTP/S3/Ray service acceptance is separate under integration/io/vortex.
Plan serialization here exercises the descriptors used by Flotilla workers;
it does not establish that a Ray cluster started or executed remotely.
"""

from __future__ import annotations

import asyncio
from pathlib import Path
from types import SimpleNamespace

import numpy as np
import pyarrow as pa
import pytest

import daft
from daft import daft as _native
from daft.context import get_context
from daft.execution.native_executor import NativeExecutor
from daft.io import _vortex
from daft.io.pushdowns import Pushdowns
from daft.io.source import DataSourceTask
from daft.pickle import cloudpickle
from daft.schema import Schema


@pytest.fixture
def vortex_writer():
    if not hasattr(_native, "read_vortex_schema"):
        pytest.skip("requires a Daft build with Rust features python,vortex")
    return pytest.importorskip("vortex", reason="optional vortex-data package is needed only to write test fixtures")


@pytest.mark.parametrize("name", ["max_concurrent_requests", "max_in_flight_bytes"])
@pytest.mark.parametrize("value", [0, -1, True, False, 1.5, "8", None, np.int64(8)])
def test_vortex_request_budgets_validate_before_io(name: str, value) -> None:
    with pytest.raises(ValueError, match=f"{name} must be a positive integer"):
        daft.read_vortex("unused.vortex", **{name: value})


@pytest.mark.parametrize("name", ["max_concurrent_requests", "max_in_flight_bytes"])
def test_vortex_request_budget_upper_bounds_validate_before_io(name: str) -> None:
    with pytest.raises(ValueError, match=f"{name} must be at most"):
        daft.read_vortex("unused.vortex", **{name: 2**128})


@pytest.mark.parametrize("value", [0, -1, 65, True, False, 1.5, "8", None, np.int64(8)])
def test_vortex_metadata_concurrency_validates_before_io(value) -> None:
    with pytest.raises(ValueError, match="metadata_concurrency"):
        daft.read_vortex("unused.vortex", metadata_concurrency=value)


@pytest.mark.parametrize("name", ["max_concurrent_requests", "max_in_flight_bytes"])
@pytest.mark.parametrize("value", [0, -1, True, 1.5, "8", None, np.int64(8)])
def test_vortex_native_config_budget_validation_without_reader_feature(name: str, value) -> None:
    valid = _native.VortexSourceConfig(max_concurrent_requests=2, max_in_flight_bytes=1024)
    assert valid.max_concurrent_requests == 2
    assert valid.max_in_flight_bytes == 1024
    with pytest.raises((TypeError, ValueError, OverflowError)):
        _native.VortexSourceConfig(**{name: value})


def test_vortex_native_config_budget_pickle_without_reader_feature() -> None:
    config = _native.VortexSourceConfig(
        batch_size=5,
        enable_projection_pushdown=False,
        enable_filter_pushdown=True,
        max_concurrent_requests=3,
        max_in_flight_bytes=2 * 1024 * 1024,
    )
    restored = cloudpickle.loads(cloudpickle.dumps(config))
    assert restored.batch_size == 5
    assert restored.enable_projection_pushdown is False
    assert restored.enable_filter_pushdown is True
    assert restored.max_concurrent_requests == 3
    assert restored.max_in_flight_bytes == 2 * 1024 * 1024


@pytest.mark.parametrize("name", ["max_concurrent_requests", "max_in_flight_bytes"])
@pytest.mark.parametrize(
    "value,error_type,reason",
    [
        (0, ValueError, "positive integers"),
        (-1, OverflowError, "negative int"),
        (True, TypeError, "not bool"),
        (False, TypeError, "not bool"),
        (1.5, TypeError, "instance of 'int'"),
        ("8", TypeError, "instance of 'int'"),
        (None, TypeError, "instance of 'int'"),
        (np.int64(8), TypeError, "instance of 'int'"),
        (2**128, OverflowError, "too big"),
    ],
)
def test_vortex_native_schema_budget_validation_precedes_io(
    tmp_path: Path, vortex_writer, name: str, value, error_type, reason: str
) -> None:
    if not hasattr(_native, "read_vortex_schema"):
        pytest.skip("requires a Daft build with Rust features python,vortex")
    # Guard the positive ABI first. An old extension rejecting the new keyword
    # arguments must not count as successful negative-type validation.
    table = pa.table({"id": [1]})
    valid_path = str(tmp_path / "valid.vortex")
    vortex_writer.io.write(table, valid_path)
    inferred = _native.read_vortex_schema(valid_path, max_concurrent_requests=1, max_in_flight_bytes=1024 * 1024)
    assert Schema._from_pyschema(inferred) == Schema.from_pyarrow_schema(table.schema)

    path = str(tmp_path / "phase3-missing.vortex")
    with pytest.raises(error_type, match=reason) as invalid:
        _native.read_vortex_schema(path, **{name: value})
    assert invalid.value.__class__ is error_type
    assert "phase3-missing.vortex" not in str(invalid.value)
    assert "object not found" not in str(invalid.value)


@pytest.mark.parametrize(
    "path",
    [
        "s3://bucket/path/data.vortex",
        "s3://bucket/prefix",
        "s3://bucket/**/*.vortex",
        "http://localhost/data",
        "https://host/a%20b.vortex?token=dev",
    ],
)
def test_vortex_remote_paths_remain_remote_without_io(path: str) -> None:
    assert _vortex._input_paths(path) == [path]


@pytest.mark.parametrize(
    "path",
    [
        "gs://bucket/data.vortex",
        "az://bucket/data.vortex",
        "ftp://host/data.vortex",
        "http:///data.vortex",
        "s3:///data.vortex",
        "https://host",
        "https://host/directory/",
        "https://host/*.vortex",
        "https://host/data.vortex#",
        "s3://bucket/data.vortex?query=1",
    ],
)
def test_vortex_unsupported_remote_paths_are_explicit_errors(path: str) -> None:
    with pytest.raises(ValueError, match="(?i)(support|exact|host|fragment|query)"):
        daft.read_vortex(path)


@pytest.mark.parametrize("scheme", ["gs", "s3", "https"])
def test_vortex_invalid_url_diagnostics_redact_credentials(scheme: str) -> None:
    path = f"{scheme}://sentinel-user:sentinel-password@host/data.vortex?token=sentinel-query#sentinel-fragment"
    with pytest.raises(ValueError) as failure:
        _vortex._input_paths(path)
    for secret in ["sentinel-user", "sentinel-password", "sentinel-query", "sentinel-fragment"]:
        assert secret not in str(failure.value)


def test_vortex_invalid_unicode_authority_redacts_credentials_before_io() -> None:
    path = "https://sentinel-user:sentinel-password@host\uff0fextra/data.vortex?token=sentinel-query"
    with pytest.raises(ValueError, match="Invalid Vortex URL") as failure:
        daft.read_vortex(path)
    for secret in ["sentinel-user", "sentinel-password", "sentinel-query"]:
        assert secret not in str(failure.value)


@pytest.mark.parametrize("backend_error", [False, True])
def test_vortex_missing_http_diagnostics_redact_credentials(
    monkeypatch: pytest.MonkeyPatch, backend_error: bool
) -> None:
    path = "https://sentinel-user:sentinel-password@host/missing.vortex?token=sentinel-query"

    def glob(discovery_path, **_kwargs):
        if backend_error:
            raise FileNotFoundError(f"404 not found: {path}")
        return SimpleNamespace(file_paths=[])

    monkeypatch.setattr(_vortex, "glob_path_with_stats", glob)
    with pytest.raises(FileNotFoundError) as failure:
        _vortex._resolve_vortex_paths([path], None)
    for secret in ["sentinel-user", "sentinel-password", "sentinel-query"]:
        assert secret not in str(failure.value)


@pytest.mark.parametrize(
    "path",
    [
        "gs://sentinel-user:sentinel-password@bucket/data?token=sentinel-query",
        "https://sentinel-user:sentinel-password@host:invalid/data?token=sentinel-query",
    ],
)
@pytest.mark.parametrize("operation", ["task", "schema", "metadata", "bulk_metadata"])
def test_vortex_native_task_path_errors_redact_credentials(path: str, operation: str) -> None:
    if not hasattr(_native, "read_vortex_schema"):
        pytest.skip("requires a Daft build with Rust features python,vortex")
    schema = Schema.from_pydict({"id": daft.DataType.int64()})
    with pytest.raises(Exception) as failure:
        if operation == "task":
            DataSourceTask.vortex(path, schema)
        elif operation == "schema":
            _native.read_vortex_schema(path)
        elif operation == "metadata":
            _native.read_vortex_metadata(path)
        else:
            _native.read_vortex_metadata_bulk([path], max_concurrent_files=1)
    assert "Vortex" in str(failure.value)
    for secret in ["sentinel-user", "sentinel-password", "sentinel-query"]:
        assert secret not in str(failure.value)


def test_vortex_remote_listing_deduplicates_overlaps_and_escapes_http_query(monkeypatch: pytest.MonkeyPatch) -> None:
    first = "s3://bucket/first.vortex"
    second = "s3://bucket/nested/second.vortex"
    url = "https://host/data.vortex?token=dev[1]"
    io_config = daft.io.IOConfig()
    calls = []

    def glob(path, file_format, io_config):
        calls.append((path, io_config))
        if path == "s3://bucket":
            files = [first, second]
        elif path == "s3://bucket/**/*.vortex":
            files = [first, second]
        elif path.startswith("https:"):
            files = [url]
        else:
            files = [path]
        return SimpleNamespace(file_paths=files)

    monkeypatch.setattr(_vortex, "glob_path_with_stats", glob)
    assert _vortex._resolve_vortex_paths([first, "s3://bucket", url, url], io_config) == [first, second, url]
    assert all(config is io_config for _, config in calls)
    assert ("s3://bucket/**/*.vortex", io_config) in calls
    assert ("https://host/data.vortex\\?token=dev\\[1\\]", io_config) in calls


@pytest.mark.parametrize(
    "children", [["https://host/child.vortex"], ["https://host/one.vortex", "https://host/two.vortex"]]
)
def test_vortex_http_discovery_rejects_html_child_urls(monkeypatch: pytest.MonkeyPatch, children) -> None:
    path = "https://sentinel-user:sentinel-password@host/data.vortex?token=sentinel-query"
    calls = []

    def glob(discovery_path, **_kwargs):
        calls.append(discovery_path)
        return SimpleNamespace(file_paths=children)

    monkeypatch.setattr(_vortex, "glob_path_with_stats", glob)
    with pytest.raises(ValueError, match="exact original file URL") as failure:
        _vortex._resolve_vortex_paths([path], None)
    assert len(calls) == 1
    for secret in ["sentinel-user", "sentinel-password", "sentinel-query"]:
        assert secret not in str(failure.value)


def test_vortex_http_discovery_preserves_ipv6_authority(monkeypatch: pytest.MonkeyPatch) -> None:
    path = "http://[::1]/data.vortex?fixture=1"
    calls = []

    def glob(discovery_path, **_kwargs):
        calls.append(discovery_path)
        return SimpleNamespace(file_paths=[path])

    monkeypatch.setattr(_vortex, "glob_path_with_stats", glob)
    assert _vortex._resolve_vortex_paths([path], None) == [path]
    assert calls == ["http://[::1]/data.vortex\\?fixture=1"]


def test_vortex_io_config_and_budgets_reach_discovery_schema_and_tasks(monkeypatch: pytest.MonkeyPatch) -> None:
    path = "s3://bucket/data.vortex"
    io_config = daft.io.IOConfig(s3=daft.io.S3Config(endpoint_url="http://127.0.0.1:9000", anonymous=True))
    scan_schema = Schema.from_pyarrow_schema(
        pa.schema([pa.field("id", pa.int64(), metadata={b"daft.vortex.nullable": b"true"})])
    )
    observed = {}

    def glob(path, file_format, io_config):
        observed["listing_config"] = io_config
        return SimpleNamespace(file_paths=[path])

    def infer(path, **kwargs):
        observed["schema_path"] = path
        observed["schema_options"] = kwargs
        return scan_schema._schema

    def metadata(paths, **kwargs):
        observed["metadata_paths"] = paths
        observed["metadata_options"] = kwargs
        return [(scan_schema._schema, 17)], {}

    def task(**kwargs):
        observed["task"] = kwargs
        return object()

    monkeypatch.setattr(_vortex, "glob_path_with_stats", glob)
    monkeypatch.setattr(_native, "read_vortex_schema", infer, raising=False)
    monkeypatch.setattr(_native, "read_vortex_metadata_bulk", metadata, raising=False)
    monkeypatch.setattr(_native, "VortexSourceConfig", lambda **kwargs: SimpleNamespace(**kwargs))
    monkeypatch.setattr(DataSourceTask, "vortex", staticmethod(task))
    source = _vortex.VortexSource(
        [path], io_config, 3, metadata_concurrency=3, max_concurrent_requests=2, max_in_flight_bytes=4096
    )

    async def tasks():
        return [task async for task in source.get_tasks(Pushdowns(columns=["id"]))]

    assert len(asyncio.run(tasks())) == 1
    assert observed["listing_config"] is io_config
    assert observed["schema_path"] == path
    assert observed["schema_options"] == {
        "io_config": io_config,
        "max_concurrent_requests": 2,
        "max_in_flight_bytes": 4096,
    }
    assert observed["metadata_paths"] == [path]
    assert observed["metadata_options"] == {**observed["schema_options"], "max_concurrent_files": 3}
    task = observed["task"]
    assert task["path"] == path
    assert task["schema"] is source._scan_schema
    assert task["num_rows"] == 17
    assert task["storage_config"].io_config.__getstate__() == io_config.__getstate__()
    assert task["vortex_config"].max_concurrent_requests == 2
    assert task["vortex_config"].max_in_flight_bytes == 4096
    assert b"daft.vortex.nullable" in source._scan_schema.to_pyarrow_schema().field("id").metadata
    assert b"daft.vortex.nullable" not in (source.schema.to_pyarrow_schema().field("id").metadata or {})


def _local_plan(frame):
    builder = frame._builder.optimize(get_context().daft_execution_config)
    return _native.LocalPhysicalPlan.from_logical_plan_builder(builder._builder, {})


def _execute_plan(plan, inputs):
    iterator = NativeExecutor().run(plan, inputs, get_context(), None)
    try:
        return [row for result in iterator for row in result.micropartition().to_arrow().to_pylist()]
    finally:
        iterator.close()


def test_vortex_flotilla_plan_transport_reopens_local_file(tmp_path: Path, vortex_writer) -> None:
    path = tmp_path / "reopened.vortex"
    vortex_writer.io.write(pa.table({"id": range(6), "value": range(6)}), str(path))
    source = _vortex.VortexSource(
        [str(path)], daft.io.IOConfig(), 2, max_concurrent_requests=1, max_in_flight_bytes=1024 * 1024
    )
    source = cloudpickle.loads(cloudpickle.dumps(source))
    frame = source.read().where(daft.col("value") >= 3).select("id")
    plan, inputs = cloudpickle.loads(cloudpickle.dumps(_local_plan(frame)))
    del frame, source
    # A serialized descriptor must reopen its file on the executing worker.
    # Change only data (same schema), after planning and releasing descriptors.
    path.unlink()
    vortex_writer.io.write(pa.table({"id": range(10, 16), "value": range(6)}), str(path))
    assert sorted(_execute_plan(plan, inputs), key=lambda row: row["id"]) == [{"id": 13}, {"id": 14}, {"id": 15}]


def test_vortex_flotilla_plan_transport_keeps_request_budget(
    tmp_path: Path, vortex_writer, monkeypatch: pytest.MonkeyPatch
) -> None:
    path = tmp_path / "budget.vortex"
    vortex_writer.io.write(pa.table({"id": range(6)}), str(path))
    # Infer and plan with an adequate footer budget, then attach a deliberately
    # tiny execution budget to the task. A lost config would silently read the file.
    source = _vortex.VortexSource([str(path)], None, 2)
    original_task = DataSourceTask.vortex

    def task_with_tiny_budget(**kwargs):
        config = kwargs["vortex_config"]
        kwargs["vortex_config"] = _native.VortexSourceConfig(
            batch_size=config.batch_size,
            enable_projection_pushdown=config.enable_projection_pushdown,
            enable_filter_pushdown=config.enable_filter_pushdown,
            max_concurrent_requests=config.max_concurrent_requests,
            max_in_flight_bytes=1,
        )
        return original_task(**kwargs)

    monkeypatch.setattr(DataSourceTask, "vortex", staticmethod(task_with_tiny_budget))
    plan, inputs = cloudpickle.loads(cloudpickle.dumps(_local_plan(source.read())))
    with pytest.raises(Exception, match="(?i)(budget|in.flight|exceed)"):
        _execute_plan(plan, inputs)


def _bytes_read(frame) -> int:
    frame.collect()
    assert frame.metrics is not None
    values = [
        descriptor["value"]
        for row in frame.metrics.to_pylist()
        for name, descriptor in row["stats"]
        if name == "bytes.read"
    ]
    assert values, frame.metrics.to_pylist()
    return sum(int(value) for value in values)


def test_vortex_execution_byte_metrics_are_positive_and_query_local(tmp_path: Path, vortex_writer) -> None:
    paths = []
    for index in range(2):
        path = tmp_path / f"{index}.vortex"
        vortex_writer.io.write(
            pa.table({"id": range(31), "payload": [f"row-{row}:" + "x" * 100 for row in range(31)]}), str(path)
        )
        paths.append(str(path))
    first = _bytes_read(daft.read_vortex(paths[0]))
    repeated = _bytes_read(daft.read_vortex(paths[0]))
    both = _bytes_read(daft.read_vortex(paths))
    after_multiple = _bytes_read(daft.read_vortex(paths[0]))
    assert first > 0
    assert repeated == first
    assert both == first + _bytes_read(daft.read_vortex(paths[1]))
    assert after_multiple == first
