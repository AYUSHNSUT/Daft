from __future__ import annotations

import asyncio
import builtins
from collections.abc import AsyncIterator
from pathlib import Path

import pyarrow as pa
import pytest

import daft
from daft import daft as _native
from daft.io import _vortex
from daft.io.pushdowns import Pushdowns
from daft.io.source import DataSource, DataSourceTask
from daft.pickle import cloudpickle
from daft.schema import Schema
from tests.conftest import get_tests_daft_runner_name
from tests.utils import explain_to_text


@pytest.fixture
def vortex_writer():
    if not hasattr(_native, "read_vortex_schema"):
        pytest.skip("requires a Daft build with Rust features python,vortex")
    assert hasattr(_native, "read_vortex_metadata"), "rebuild the native extension with Vortex metadata support"
    return pytest.importorskip("vortex", reason="optional vortex-data package is needed only to write test fixtures")


def _write(path: Path, table: pa.Table, writer) -> str:
    writer.io.write(table, str(path))
    return str(path)


@pytest.mark.parametrize("rows", [0, 9])
@pytest.mark.parametrize("zero_fields", [False, True])
def test_vortex_footer_metadata_preserves_schema_and_cardinality(
    tmp_path: Path, vortex_writer, rows: int, zero_fields: bool
) -> None:
    table = pa.table({"id": pa.array(range(rows), pa.int64())})
    if zero_fields:
        table = table.select([])
    path = _write(tmp_path / "metadata.vortex", table, vortex_writer)

    schema, num_rows = _native.read_vortex_metadata(path)

    assert num_rows == rows
    assert Schema._from_pyschema(schema) == Schema._from_pyschema(_native.read_vortex_schema(path))
    assert daft.read_vortex(path, batch_size=2).count_rows() == rows
    assert daft.read_vortex(path, batch_size=2).select(daft.lit(1).alias("constant")).to_pydict() == {
        "constant": [1] * rows
    }


@pytest.mark.skipif(get_tests_daft_runner_name() != "native", reason="checks native hash-join planning and metrics")
@pytest.mark.parametrize("fact_on_left", [False, True])
def test_vortex_join_builds_the_small_dimension(tmp_path: Path, vortex_writer, fact_on_left: bool) -> None:
    rows = 4096
    dimension_rows = 16
    path = _write(
        tmp_path / "fact.vortex",
        pa.table({"id": range(rows), "key": [index % dimension_rows for index in range(rows)]}),
        vortex_writer,
    )
    fact = daft.read_vortex(path, batch_size=127)
    dimension = daft.from_pydict({"key": list(range(dimension_rows)), "weight": list(range(1, dimension_rows + 1))})
    left, right = (fact, dimension) if fact_on_left else (dimension, fact)
    query = left.join(right, on="key", strategy="hash").select("id", "weight").sort("id")

    plan = explain_to_text(query, only_physical_plan=True)
    assert f"Build on left: {str(not fact_on_left).lower()}" in plan, plan
    assert query.to_pydict() == {
        "id": list(range(rows)),
        "weight": [index % dimension_rows + 1 for index in range(rows)],
    }
    assert query.metrics is not None
    join_metrics = [row for row in query.metrics.to_pylist() if row["type"] == "HashJoin"]
    assert len(join_metrics) == 1
    # The small-build path can omit the build-row counter. Probe rows, together
    # with the physical plan, verify that the fact table streams through the join.
    probe_rows = [descriptor["value"] for name, descriptor in join_metrics[0]["stats"] if name == "probe rows in"]
    assert probe_rows == [rows], join_metrics


def test_vortex_multifile_statistics_preserve_filter_limit_and_empty_semantics(tmp_path: Path, vortex_writer) -> None:
    schema = pa.schema([pa.field("id", pa.int64())])
    empty = _write(tmp_path / "empty.vortex", pa.Table.from_batches([], schema=schema), vortex_writer)
    first = _write(tmp_path / "first.vortex", pa.table({"id": range(4)}), vortex_writer)
    second = _write(tmp_path / "second.vortex", pa.table({"id": range(4, 9)}), vortex_writer)
    paths = [empty, first, second, first]

    assert daft.read_vortex(paths, batch_size=2).count_rows() == 9
    assert daft.read_vortex(paths, batch_size=2).sort("id").to_pydict() == {"id": list(range(9))}
    assert daft.read_vortex(paths, batch_size=2).where(daft.col("id") >= 6).count_rows() == 3
    assert daft.read_vortex(paths, batch_size=2).where(daft.col("id") > 100).count_rows() == 0
    assert daft.read_vortex(paths, batch_size=2).where(daft.col("id") >= 6).limit(2).to_pydict() == {"id": [6, 7]}
    assert daft.read_vortex(paths, batch_size=2).limit(2).where(daft.col("id") >= 6).to_pydict() == {"id": []}
    assert daft.read_vortex(paths, batch_size=2).limit(0).count_rows() == 0
    assert daft.read_vortex(paths, batch_size=2).limit(20).count_rows() == 9


@pytest.mark.parametrize("pickle_source", [False, True])
def test_vortex_task_row_counts_refresh_when_a_source_is_planned_again(
    tmp_path: Path, vortex_writer, monkeypatch: pytest.MonkeyPatch, pickle_source: bool
) -> None:
    path = tmp_path / "replace.vortex"
    _write(path, pa.table({"id": range(3)}), vortex_writer)
    source = _vortex.VortexSource([str(path)], None, 2)
    if pickle_source:
        source = cloudpickle.loads(cloudpickle.dumps(source))
    observed = []
    original_task = DataSourceTask.vortex

    def record_task(**kwargs):
        observed.append(kwargs["num_rows"])
        return original_task(**kwargs)

    monkeypatch.setattr(DataSourceTask, "vortex", staticmethod(record_task))

    async def tasks():
        return [task async for task in source.get_tasks(Pushdowns())]

    assert len(asyncio.run(tasks())) == 1
    path.unlink()
    _write(path, pa.table({"id": range(17)}), vortex_writer)
    assert len(asyncio.run(tasks())) == 1
    assert observed == [3, 17]
    assert source.read().count_rows() == 17


@pytest.mark.parametrize("rows", [0, 9])
@pytest.mark.parametrize("known_count", [False, True])
def test_vortex_tasks_allow_optional_exact_row_counts(
    tmp_path: Path, vortex_writer, rows: int, known_count: bool
) -> None:
    path = _write(tmp_path / "task.vortex", pa.table({"id": pa.array(range(rows), pa.int64())}), vortex_writer)
    schema = Schema._from_pyschema(_native.read_vortex_schema(path))

    class NativeSource(DataSource):
        @property
        def name(self) -> str:
            return "VortexStatisticsTest"

        @property
        def schema(self) -> Schema:
            return schema

        async def get_tasks(self, pushdowns: Pushdowns) -> AsyncIterator[DataSourceTask]:
            yield DataSourceTask.vortex(path, schema, pushdowns=pushdowns, num_rows=rows if known_count else None)

    assert NativeSource().read().to_pydict() == {"id": list(range(rows))}
    assert NativeSource().read().count_rows() == rows


def test_vortex_task_rejects_negative_row_counts(tmp_path: Path, vortex_writer) -> None:
    path = _write(tmp_path / "negative.vortex", pa.table({"id": [1]}), vortex_writer)
    schema = Schema._from_pyschema(_native.read_vortex_schema(path))
    with pytest.raises(ValueError, match="(?i)(non.negative|negative|num_rows|row.count)"):
        DataSourceTask.vortex(path, schema, num_rows=-1)


def test_vortex_metadata_and_native_read_do_not_import_python_vortex(
    tmp_path: Path, vortex_writer, monkeypatch: pytest.MonkeyPatch
) -> None:
    path = _write(tmp_path / "native.vortex", pa.table({"id": range(9)}), vortex_writer)
    original_import = builtins.__import__

    def reject_vortex(name, *args, **kwargs):
        if name == "vortex" or name.startswith("vortex."):
            pytest.fail("Native Vortex metadata and reads must not import the optional Python writer")
        return original_import(name, *args, **kwargs)

    monkeypatch.setattr(builtins, "__import__", reject_vortex)
    _, num_rows = _native.read_vortex_metadata(path)
    assert num_rows == 9
    assert daft.read_vortex(path).where(daft.col("id") >= 6).to_pydict() == {"id": [6, 7, 8]}
