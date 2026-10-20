"""Native local task execution and source/config descriptor serialization.

Python pickling of native task wrappers is outside this local reader phase.
Pickled sources create fresh native tasks after restoration; Rust ScanTask
serialization is checked separately by the native tests.
"""

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator
from pathlib import Path

import pyarrow as pa
import pytest

import daft
from daft import daft as _native
from daft.io.pushdowns import Pushdowns
from daft.io.source import DataSource, DataSourceTask
from daft.pickle import cloudpickle
from daft.schema import Schema


@pytest.fixture
def native_file(tmp_path: Path) -> tuple[str, Schema]:
    if not hasattr(_native, "read_vortex_schema"):
        pytest.skip("requires a Daft build with Rust features python,vortex")
    vx = pytest.importorskip("vortex", reason="optional vortex-data package is needed only to write test fixtures")
    path = str(tmp_path / "task.vortex")
    vx.io.write(pa.table({"id": range(9), "value": [None, 1, 2, 3, 4, 5, 6, 7, 8]}), path)
    return path, Schema._from_pyschema(_native.read_vortex_schema(path))


class _NativeVortexSource(DataSource):
    def __init__(self, path: str, schema: Schema) -> None:
        self._path = path
        self._schema = schema

    @property
    def name(self) -> str:
        return "DirectNativeVortexTest"

    @property
    def schema(self) -> Schema:
        return self._schema

    async def get_tasks(self, pushdowns: Pushdowns) -> AsyncIterator[DataSourceTask]:
        config = _native.VortexSourceConfig(batch_size=2)
        yield DataSourceTask.vortex(self._path, self._schema, vortex_config=config, pushdowns=pushdowns)


def test_vortex_task_disabled_build_is_actionable(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delattr(_native, "read_vortex_schema", raising=False)
    with pytest.raises(ImportError, match="python,vortex"):
        DataSourceTask.vortex("unused.vortex", Schema.from_pydict({"id": daft.DataType.int64()}))


def test_vortex_task_native_wrapper_and_schema(native_file: tuple[str, Schema]) -> None:
    path, schema = native_file
    task = DataSourceTask.vortex(path, schema, vortex_config=_native.VortexSourceConfig(batch_size=2))
    assert isinstance(task, DataSourceTask)
    assert task.schema == schema
    assert b"daft.vortex.nullable" in task.schema.to_pyarrow_schema().field("id").metadata

    async def cannot_read_python() -> None:
        with pytest.raises(NotImplementedError, match="Rust engine"):
            await anext(task.read())

    asyncio.run(cannot_read_python())


@pytest.mark.parametrize("pickle_source", [False, True])
def test_vortex_source_creates_native_tasks_after_optional_pickle(
    native_file: tuple[str, Schema], pickle_source: bool
) -> None:
    path, schema = native_file
    source = _NativeVortexSource(path, schema)
    if pickle_source:
        source = cloudpickle.loads(cloudpickle.dumps(source))
    frame = source.read()
    assert frame.where(daft.col("value") >= 6).select("id").limit(2).to_pydict() == {"id": [6, 7]}
    assert frame.limit(2).where(daft.col("value") >= 6).select("id").to_pydict() == {"id": []}
    assert frame.where(daft.col("value").is_null()).select("id").to_pydict() == {"id": [0]}
    assert frame.count_rows() == 9
    assert frame.select(daft.lit(1).alias("constant")).to_pydict() == {"constant": [1] * 9}


def test_vortex_task_requires_native_schema_metadata(native_file: tuple[str, Schema]) -> None:
    path, _ = native_file
    manual = Schema.from_pydict({"id": daft.DataType.int64(), "value": daft.DataType.int64()})
    with pytest.raises(Exception, match="nullability"):
        _NativeVortexSource(path, manual).read().select("id").collect()


def test_vortex_task_rejects_unsupported_aggregation_pushdown(native_file: tuple[str, Schema]) -> None:
    path, schema = native_file
    with pytest.raises(NotImplementedError, match="aggregation"):
        DataSourceTask.vortex(path, schema, pushdowns=Pushdowns(aggregation=daft.col("id").count()))


def test_vortex_config_pickle(native_file: tuple[str, Schema]) -> None:
    config = cloudpickle.loads(cloudpickle.dumps(_native.VortexSourceConfig(batch_size=7)))
    assert config.batch_size == 7
