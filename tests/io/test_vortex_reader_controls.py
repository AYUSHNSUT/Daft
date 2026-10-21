from __future__ import annotations

import itertools

import pyarrow as pa
import pytest

import daft
from daft import col
from daft.io import _vortex
from daft.io import _vortex_diagnostics as diagnostics
from daft.pickle import cloudpickle

FLAGS = ("enable_large_offsets", "enable_execution_session_reuse", "enable_file_pruning", "enable_layout_splitting")


@pytest.fixture
def files(tmp_path):
    if not hasattr(daft.daft.VortexSourceConfig, "enable_large_offsets"):
        pytest.skip("requires rebuilt experimental reader controls")
    writer = pytest.importorskip("vortex")
    tables, paths = [], []
    text = [None, "", "汉字", "café", "🙂", "e\u0301", "x" * 4097]
    data = [None, b"", b"\x00\xff", b"abc", b"\x80", b"z" * 513, b"last"]
    for index in range(3):
        table = pa.table({"id": list(range(index * 7, index * 7 + 7)), "text": text, "data": data})
        path = tmp_path / f"{index}.vortex"
        writer.io.write(table, str(path))
        paths.append(str(path))
        tables.append(table)
    return paths, pa.concat_tables(tables), writer


@pytest.mark.parametrize("values", itertools.product((False, True), repeat=4))
@pytest.mark.parametrize("limit", [0, 1, 8, 99])
def test_reader_controls_preserve_full_values_projection_filter_and_limit(files, values, limit):
    paths, table, _ = files
    options = dict(zip(FLAGS, values))
    query = daft.read_vortex(paths, batch_size=3, enable_row_count_planning=True, **options)
    actual = query.where(col("id") >= 5).select("data", "id", "text").limit(limit).to_pydict()
    expected = table.filter(pa.compute.greater_equal(table["id"], 5)).select(["data", "id", "text"]).slice(0, limit)
    assert actual == expected.to_pydict()


@pytest.mark.parametrize("flag", FLAGS)
@pytest.mark.parametrize("value", [0, 1, "true", None])
def test_reader_controls_reject_non_bool_before_io(flag, value):
    with pytest.raises(TypeError, match=flag):
        daft.read_vortex("missing.vortex", **{flag: value})


def test_reader_control_configuration_roundtrip():
    config = daft.daft.VortexSourceConfig(**dict.fromkeys(FLAGS, True))
    decoded = cloudpickle.loads(cloudpickle.dumps(config))
    assert all(getattr(decoded, flag) is True for flag in FLAGS)


@pytest.mark.parametrize("enabled", [False, True])
def test_execution_sessions_are_fresh_per_materialization_and_schema_validated_per_file(files, enabled):
    paths, table, _ = files
    source = _vortex.VortexSource(paths, None, 3, enable_execution_session_reuse=enabled)
    for _ in range(2):
        diagnostics.start()
        try:
            actual = source.read().to_pydict()
        finally:
            capture = diagnostics.finish()
        assert actual == table.to_pydict()
        events = capture["native"]["events"]
        sessions = [e for e in events if e["phase"] == "execution_open" and e["operation"] == "session_creation"]
        assert len(sessions) == (1 if enabled else len(paths))
        assert sum(e["phase"] == "execution_open" and e["operation"] == "schema_validation" for e in events) == len(
            paths
        )
        assert any(e["operation"] == "sdk_cpu_task" for e in events)
        assert capture["native"]["dropped_events"] == capture["native"]["unfinished_events"] == 0


def test_pruned_file_still_checks_projected_away_schema(files):
    paths, _, writer = files
    writer.io.write(pa.table({"id": [100], "text": [123], "data": [b"bad"]}), paths[-1])
    with pytest.raises(Exception, match="(?i)(schema|column|type)"):
        daft.read_vortex(paths, enable_file_pruning=True, enable_row_count_planning=True).where(col("id") < 1).select(
            "id"
        ).collect()


def test_pruning_preserves_null_fallback_and_no_matches(files):
    paths, _, _ = files
    for predicate in [col("id") < -1, col("text").is_null(), (col("id") < 2) | (col("id") > 17)]:
        baseline = daft.read_vortex(paths).where(predicate).to_pydict()
        assert daft.read_vortex(paths, enable_file_pruning=True).where(predicate).to_pydict() == baseline


def test_whole_file_pruning_avoids_scan_preparation_after_validation(files):
    paths, _, _ = files
    diagnostics.start()
    try:
        assert daft.read_vortex(paths, enable_file_pruning=True).where(col("id") < -1).to_pydict()["id"] == []
    finally:
        capture = diagnostics.finish()
    events = capture["native"]["events"]
    assert sum(e["operation"] == "file_pruning" for e in events) == 3
    assert sum(e["phase"] == "execution_open" and e["operation"] == "schema_validation" for e in events) == 3
    assert not any(e["operation"] == "stream_preparation" for e in events)


def test_layout_splits_keep_batch_row_bound_and_order(files):
    paths, table, _ = files
    partitions = list(daft.read_vortex(paths, batch_size=3, enable_layout_splitting=True).iter_partitions())
    # Engine morsels may combine reader batches; validate reader bounds in native tests.
    assert pa.concat_tables([part.to_arrow() for part in partitions]).to_pydict() == table.to_pydict()
