"""Lightweight planning shares session setup while keeping metadata and scans fresh."""

from __future__ import annotations

import asyncio
from pathlib import Path

import pyarrow as pa
import pytest

import daft
from daft import daft as native
from daft.io import _vortex
from daft.io import _vortex_diagnostics as diagnostics
from daft.io.pushdowns import Pushdowns
from daft.io.source import DataSourceTask
from daft.pickle import cloudpickle


@pytest.fixture
def writer():
    if not hasattr(native, "_read_vortex_row_counts_bulk"):
        pytest.skip("requires Phase 7 row-count planning native build")
    return pytest.importorskip("vortex")


@pytest.mark.parametrize("value", [None, 0, 1, "true", []])
def test_row_count_planning_requires_boolean_before_io(value):
    with pytest.raises(TypeError, match="enable_row_count_planning must be a bool"):
        daft.read_vortex("missing.vortex", enable_row_count_planning=value)


@pytest.mark.parametrize("cached", [False, True])
@pytest.mark.parametrize("concurrency", [1, 2, 64])
def test_row_count_bulk_preserves_order_duplicates_empty_inputs_and_io(tmp_path, writer, cached, concurrency):
    paths = []
    for index, rows in enumerate([7, 0, 3]):
        path = tmp_path / f"{index}.vortex"
        writer.io.write(pa.table({"id": pa.array(range(rows), pa.int64())}), str(path))
        paths.append(str(path))
    paths.append(paths[0])
    api = native._read_vortex_row_counts_bulk_cached if cached else native._read_vortex_row_counts_bulk
    baseline, baseline_stats = native.read_vortex_metadata_bulk(paths, max_concurrent_files=concurrency)
    values, stats = api(paths, max_concurrent_files=concurrency)
    assert values == [(7, None), (0, None), (3, None), (7, None)] if cached else values == [7, 0, 3, 7]
    assert [rows for _, rows in baseline] == [7, 0, 3, 7]
    for counter in ["head_requests", "get_requests", "list_requests", "bytes_read"]:
        assert stats[counter] == baseline_stats[counter]
    empty, stats = api([], max_concurrent_files=concurrency)
    assert empty == []
    assert all(stats[counter] == 0 for counter in ["head_requests", "get_requests", "list_requests", "bytes_read"])


@pytest.mark.parametrize("pickle_source", [False, True])
def test_row_count_source_refreshes_after_replacement(tmp_path, writer, monkeypatch, pickle_source):
    path = tmp_path / "replace.vortex"
    writer.io.write(pa.table({"id": [1, 2, 3]}), str(path))
    source = _vortex.VortexSource([str(path)], None, 2, enable_row_count_planning=True)
    if pickle_source:
        source = cloudpickle.loads(cloudpickle.dumps(source))
    counts = []
    original = DataSourceTask.vortex

    def record(**kwargs):
        counts.append(kwargs["num_rows"])
        return original(**kwargs)

    monkeypatch.setattr(DataSourceTask, "vortex", staticmethod(record))

    async def tasks():
        return [task async for task in source.get_tasks(Pushdowns())]

    assert len(asyncio.run(tasks())) == 1
    path.unlink()
    writer.io.write(pa.table({"id": list(range(17))}), str(path))
    assert len(asyncio.run(tasks())) == 1
    assert counts == [3, 17]
    assert source.read().count_rows() == 17


@pytest.mark.parametrize("mismatch", ["type", "nullable", "nested"])
def test_row_count_planning_keeps_full_execution_schema_validation(tmp_path, writer, mismatch):
    first, second = tmp_path / "first.vortex", tmp_path / "second.vortex"
    writer.io.write(pa.table({"id": [1], "unused": ["a"]}), str(first))
    if mismatch == "nested":
        table = pa.table({"id": [2], "unused": [[1]]})
    elif mismatch == "type":
        table = pa.table({"id": [2], "unused": [4]})
    else:
        table = pa.table(
            {"id": [2], "unused": ["b"]},
            schema=pa.schema([pa.field("id", pa.int64()), pa.field("unused", pa.string(), nullable=False)]),
        )
    writer.io.write(table, str(second))
    with pytest.raises(Exception, match="(?i)(schema|nullability|flat primitive)"):
        daft.read_vortex([str(first), str(second)], enable_row_count_planning=True).select("id").collect()


def test_row_count_planning_profiles_bulk_session_reuse_and_fresh_materializations(tmp_path: Path, writer):
    paths = []
    for index in range(3):
        path = tmp_path / f"profile-{index}.vortex"
        writer.io.write(pa.table({"id": [index * 2, index * 2 + 1]}), str(path))
        paths.append(str(path))
    for enabled in (False, True):
        source = _vortex.VortexSource(paths, None, 2, enable_row_count_planning=enabled)
        for _ in range(2):
            diagnostics.start()
            try:
                result = source.read().to_pydict()
            finally:
                capture = diagnostics.finish()
            assert sorted(result["id"]) == list(range(6))
            events = capture["native"]["events"]
            converted = [
                event
                for event in events
                if event["phase"] == "planning_metadata" and event["operation"] == "schema_conversion"
            ]
            assert len(converted) == (0 if enabled else 3)
            planning_sessions = [
                event
                for event in events
                if event["phase"] == "planning_metadata" and event["operation"] == "session_creation"
            ]
            assert len(planning_sessions) == (1 if enabled else 3)
            assert all((event["file_id"] is None) is enabled for event in planning_sessions)
            for operation in ("session_creation", "schema_validation"):
                assert (
                    sum(event["phase"] == "execution_open" and event["operation"] == operation for event in events) == 3
                )
            assert any(
                event["phase"] == "execution" and event["operation"] == "batch_wait_decode" and event["poll_count"] > 0
                for event in events
            )
            assert capture["native"]["unfinished_events"] == capture["native"]["dropped_events"] == 0


@pytest.mark.parametrize("cached", [False, True])
def test_row_count_bulk_reinitializes_one_session_per_call_and_none_for_empty_input(tmp_path, writer, cached):
    paths = []
    for index in range(3):
        path = tmp_path / f"{index}.vortex"
        writer.io.write(pa.table({"id": [index]}), str(path))
        paths.append(str(path))
    api = native._read_vortex_row_counts_bulk_cached if cached else native._read_vortex_row_counts_bulk
    for requested in (paths, paths, []):
        diagnostics.start()
        try:
            values, _ = api(requested)
        finally:
            capture = diagnostics.finish()
        assert len(values) == len(requested)
        sessions = [event for event in capture["native"]["events"] if event["operation"] == "session_creation"]
        assert len(sessions) == (1 if requested else 0)
        assert all(event["phase"] == "planning_metadata" and event["file_id"] is None for event in sessions)
        assert capture["native"]["unfinished_events"] == capture["native"]["dropped_events"] == 0
