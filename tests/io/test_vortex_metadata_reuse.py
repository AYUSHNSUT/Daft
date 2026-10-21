from __future__ import annotations

import asyncio
from types import SimpleNamespace

import pyarrow as pa
import pytest

import daft
from daft import daft as native
from daft.io import _vortex
from daft.io.pushdowns import Pushdowns
from daft.io.source import DataSourceTask
from daft.pickle import cloudpickle
from daft.schema import Schema


def _source(monkeypatch, paths, *, enabled=True):
    schema = Schema.from_pyarrow_schema(pa.schema([pa.field("id", pa.int64())]))
    monkeypatch.setattr(_vortex, "_resolve_vortex_paths", lambda *args: paths)
    monkeypatch.setattr(native, "read_vortex_schema", lambda *args, **kwargs: schema._schema, raising=False)
    monkeypatch.setattr(native, "VortexSourceConfig", lambda **kwargs: SimpleNamespace(**kwargs))
    monkeypatch.setattr(DataSourceTask, "vortex", staticmethod(lambda **kwargs: kwargs))
    return _vortex.VortexSource(paths, None, 3, metadata_concurrency=1, enable_metadata_reuse=enabled)


def _tasks(source):
    async def collect():
        return [task async for task in source.get_tasks(Pushdowns())]

    return asyncio.run(collect())


@pytest.mark.parametrize("value", [0, 1, None, "false", []])
def test_metadata_reuse_is_strict_boolean_before_io(value):
    with pytest.raises(TypeError, match="enable_metadata_reuse"):
        daft.read_vortex("unused.vortex", enable_metadata_reuse=value)


def test_snapshot_budget_and_counts_reset_for_each_materialization(monkeypatch):
    source = _source(monkeypatch, ["s3://bucket/first", "s3://bucket/second"])
    monkeypatch.setattr(_vortex, "_MAX_METADATA_SNAPSHOT_BYTES", 4)
    captured = []

    def cached(paths, **kwargs):
        captured.append(kwargs["max_snapshot_bytes"])
        return [(source._scan_schema._schema, len(captured), b"abcd")], {}

    ordinary = []

    def fresh(paths, **kwargs):
        ordinary.extend(paths)
        return [(source._scan_schema._schema, 9)], {}

    monkeypatch.setattr(native, "_read_vortex_metadata_bulk_cached", cached, raising=False)
    monkeypatch.setattr(native, "read_vortex_metadata_bulk", fresh, raising=False)
    for count in (1, 2):
        tasks = _tasks(source)
        assert [task["num_rows"] for task in tasks] == [count, 9]
        assert tasks[0]["vortex_config"].metadata_snapshot == b"abcd"
        assert tasks[1]["vortex_config"].metadata_snapshot is None
    assert captured == [4, 4]
    assert ordinary == ["s3://bucket/second"] * 2
    assert not any("snapshot" in name for name in vars(source))


@pytest.mark.parametrize(
    "enabled,path", [(False, "s3://bucket/file"), (True, "/local/file"), (True, "https://host/file")]
)
def test_disabled_and_unsupported_sources_keep_fresh_metadata_path(monkeypatch, enabled, path):
    source = _source(monkeypatch, [path], enabled=enabled)

    def unexpected(*args, **kwargs):
        raise AssertionError("Unsupported path attempted metadata reuse")

    monkeypatch.setattr(native, "_read_vortex_metadata_bulk_cached", unexpected, raising=False)
    monkeypatch.setattr(
        native,
        "read_vortex_metadata_bulk",
        lambda *args, **kwargs: ([(source._scan_schema._schema, 7)], {}),
        raising=False,
    )
    task = _tasks(source)[0]
    assert task["num_rows"] == 7
    assert task["vortex_config"].metadata_snapshot is None


def test_oversized_snapshot_is_bypassed_without_losing_fresh_rows(monkeypatch):
    source = _source(monkeypatch, ["s3://bucket/file"])
    monkeypatch.setattr(_vortex, "_MAX_METADATA_SNAPSHOT_BYTES", 4)
    monkeypatch.setattr(
        native,
        "_read_vortex_metadata_bulk_cached",
        lambda *args, **kwargs: ([(source._scan_schema._schema, 8, b"12345")], {}),
        raising=False,
    )
    task = _tasks(source)[0]
    assert task["num_rows"] == 8
    assert task["vortex_config"].metadata_snapshot is None


@pytest.mark.skipif(
    not hasattr(native, "_read_vortex_metadata_bulk_cached"), reason="requires metadata-reuse native build"
)
@pytest.mark.parametrize("value", [-1, True, False, 64 * 1024 * 1024 + 1, 1.5, "4", None])
def test_native_snapshot_budget_validates_before_open(value):
    with pytest.raises((TypeError, ValueError, OverflowError)):
        native._read_vortex_metadata_bulk_cached(["unused.vortex"], max_snapshot_bytes=value)


@pytest.mark.skipif(
    not hasattr(native, "_read_vortex_metadata_bulk_cached"), reason="requires metadata-reuse native build"
)
def test_native_snapshot_config_pickle_preserves_bytes_without_debug_contents():
    raw = b"sentinel-snapshot-secret"
    config = native.VortexSourceConfig(metadata_snapshot=raw)
    restored = cloudpickle.loads(cloudpickle.dumps(config))
    assert bytes(restored.metadata_snapshot) == raw
    assert raw.decode() not in repr(restored)
    metadata, stats = native._read_vortex_metadata_bulk_cached([], max_snapshot_bytes=0)
    assert metadata == [] and stats["head_requests"] == stats["get_requests"] == 0


@pytest.mark.skipif(
    not hasattr(native, "_read_vortex_metadata_bulk_cached"), reason="requires metadata-reuse native build"
)
@pytest.mark.parametrize("budget", [0, 64 * 1024 * 1024])
def test_native_local_metadata_fallback_and_public_reader(tmp_path, budget):
    import vortex

    path = tmp_path / "local.vortex"
    table = pa.table({"id": [1, 2, 3]})
    vortex.io.write(table, str(path))
    cached, _ = native._read_vortex_metadata_bulk_cached([str(path)], max_snapshot_bytes=budget)
    fresh, _ = native.read_vortex_metadata_bulk([str(path)])
    assert [(schema, rows) for schema, rows, _ in cached] == fresh
    assert [snapshot for _, _, snapshot in cached] == [None]
    assert daft.read_vortex(path, enable_metadata_reuse=True).to_pydict() == table.to_pydict()
