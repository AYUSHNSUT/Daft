"""Focused checks for the optional, non-public Python Vortex reference.

Real-format tests require vortex-data==0.88.0. They skip explicitly when that
optional distribution is unavailable; skipped cases are not scan coverage.
"""

from __future__ import annotations

import asyncio
import threading
from pathlib import Path
from types import ModuleType, SimpleNamespace

import pyarrow as pa
import pytest

import daft
from daft.io.pushdowns import Pushdowns
from examples.vortex_eval import reference


@pytest.fixture
def vx():
    return pytest.importorskip("vortex", reason="optional real-format tests require vortex-data==0.88.0")


@pytest.fixture
def vortex_file(tmp_path: Path, vx) -> Path:
    path = tmp_path / "sample data.vortex"
    table = pa.table(
        {
            "id": pa.array([0, 1, 2, 3, 4], type=pa.int64()),
            "score": pa.array([1.5, None, -2.0, 3.0, 0.0], type=pa.float64()),
            "keep": pa.array([True, False, True, None, True], type=pa.bool_()),
            "name": pa.array(["zero", None, "two", "three", ""], type=pa.string()),
            "payload": pa.array([b"a", None, b"c", b"", b"e"], type=pa.binary()),
        }
    )
    vx.io.write(vx.array(table), str(path))
    return path


@pytest.mark.parametrize("batch_size", [0, -1, True, 1.5])
def test_invalid_batch_size(batch_size) -> None:
    with pytest.raises(ValueError, match="positive integer"):
        reference.read_vortex_reference("unused.vortex", batch_size=batch_size)


def test_explicit_local_paths_only(tmp_path: Path) -> None:
    with pytest.raises(ValueError, match="At least one"):
        reference.read_vortex_reference([])
    with pytest.raises(ValueError, match="empty Vortex filepath"):
        reference.read_vortex_reference("")
    with pytest.raises(ValueError, match="Only explicit local"):
        reference.read_vortex_reference("s3://bucket/file.vortex")
    with pytest.raises(ValueError, match="explicit local file"):
        reference.read_vortex_reference(tmp_path)
    with pytest.raises(FileNotFoundError):
        reference.read_vortex_reference(tmp_path / "missing.vortex")


def test_resolved_paths_deduplicate_in_input_order(tmp_path: Path) -> None:
    first, second = tmp_path / "first.vortex", tmp_path / "second.vortex"
    first.touch()
    second.touch()
    assert reference._resolve_paths([second, first, f"{tmp_path}/./second.vortex", first]) == [
        str(second.resolve()),
        str(first.resolve()),
    ]


def test_optional_dependency_message(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    path = tmp_path / "placeholder.vortex"
    path.touch()

    def unavailable(name: str):
        raise ModuleNotFoundError("No module named 'vortex'", name="vortex")

    monkeypatch.setattr(reference, "import_module", unavailable)
    with pytest.raises(ImportError, match="vortex-data==0.88.0"):
        reference.read_vortex_reference(path)


def test_exact_optional_dependency_version(monkeypatch: pytest.MonkeyPatch) -> None:
    module = ModuleType("vortex")
    monkeypatch.setattr(reference, "import_module", lambda name: module)
    monkeypatch.setattr(reference, "version", lambda name: "0.87.0")
    with pytest.raises(ImportError, match="requires vortex-data==0.88.0, but found 0.87.0"):
        reference._require_vortex()
    monkeypatch.setattr(reference, "version", lambda name: "0.88.0")
    assert reference._require_vortex() is module


def test_cancellation_waits_before_closing_generator(monkeypatch: pytest.MonkeyPatch) -> None:
    # A running to_thread(next) cannot be cancelled. Exercise its lifetime
    # independently of the optional file format so it cannot race close().
    entered = threading.Event()
    release = threading.Event()
    closed = threading.Event()

    def blocked(*args):
        try:
            entered.set()
            if not release.wait(timeout=5):
                raise TimeoutError("Test did not release the batch reader")
            yield reference.RecordBatch.from_pydict({"id": [1]})
        finally:
            closed.set()

    monkeypatch.setattr(reference, "_iter_vortex_batches", blocked)
    schema = pa.schema([("id", pa.int64())])
    task = reference._VortexReferenceTask("unused.vortex", schema, schema, 1)

    async def inspect() -> None:
        batches = task.read()
        pending = asyncio.create_task(anext(batches))
        try:
            assert await asyncio.to_thread(entered.wait, 5)
            pending.cancel()
            await asyncio.sleep(0)
            assert not closed.is_set()
            release.set()
            with pytest.raises(asyncio.CancelledError):
                await pending
            assert closed.is_set()
        finally:
            release.set()
            await asyncio.gather(pending, return_exceptions=True)
            await batches.aclose()

    asyncio.run(inspect())


@pytest.mark.parametrize("failure", ["iteration", "conversion"])
def test_scan_errors_include_path_and_close_reader(failure: str, monkeypatch: pytest.MonkeyPatch) -> None:
    schema = pa.schema([("id", pa.int64())])
    batch = pa.record_batch([[1]], schema=schema)

    class FailingReader:
        closed = False

        def __iter__(self):
            if failure == "iteration":
                raise RuntimeError("controlled iteration failure")
            yield batch

        def close(self):
            self.closed = True

    reader = FailingReader()
    file = SimpleNamespace(
        dtype=SimpleNamespace(to_arrow_schema=lambda: schema),
        to_arrow=lambda **kwargs: reader,
    )
    monkeypatch.setattr(reference, "_open_vortex", lambda path: file)
    if failure == "conversion":

        def fail_conversion(*args):
            raise RuntimeError("controlled conversion failure")

        monkeypatch.setattr(reference.RecordBatch, "from_arrow_record_batches", fail_conversion)
    with pytest.raises(OSError, match=f"broken.vortex.*controlled {failure} failure"):
        list(reference._iter_vortex_batches("/fixtures/broken.vortex", schema, schema, 1))
    assert reader.closed


def test_values_and_native_string_types(vortex_file: Path) -> None:
    frame = reference.read_vortex_reference(vortex_file, batch_size=2)
    assert frame.schema()["name"].dtype == daft.DataType.string()
    assert frame.schema()["payload"].dtype == daft.DataType.binary()
    assert frame.sort("id").to_pydict() == {
        "id": [0, 1, 2, 3, 4],
        "score": [1.5, None, -2.0, 3.0, 0.0],
        "keep": [True, False, True, None, True],
        "name": ["zero", None, "two", "three", ""],
        "payload": [b"a", None, b"c", b"", b"e"],
    }


def test_residual_filter_projection_and_limit(vortex_file: Path) -> None:
    frame = reference.read_vortex_reference(vortex_file, batch_size=2)
    assert frame.where(daft.col("keep")).select("id").sort("id").to_pydict() == {"id": [0, 2, 4]}
    assert frame.where(daft.col("keep")).select("id").limit(2).to_pydict() == {"id": [0, 2]}
    # Applying LIMIT first is observably different: only row 0 survives the
    # predicate among the first two input rows. It must not move after FILTER.
    assert frame.limit(2).where(daft.col("keep")).select("id").to_pydict() == {"id": [0]}
    assert reference.read_vortex_reference(vortex_file).where(daft.col("name").is_null()).count_rows() == 1
    assert reference.read_vortex_reference(vortex_file).limit(2).count_rows() == 2


def test_duplicate_explicit_files_are_read_once(vortex_file: Path) -> None:
    result = reference.read_vortex_reference([vortex_file, vortex_file], batch_size=2)
    assert result.select("id").sort("id").to_pydict() == {"id": [0, 1, 2, 3, 4]}


def test_projected_tasks_keep_residual_columns(vortex_file: Path) -> None:
    source = reference._VortexReferenceSource([str(vortex_file)], batch_size=2)

    async def inspect() -> None:
        tasks = [task async for task in source.get_tasks(Pushdowns(columns=["id"], filters=daft.col("keep"), limit=1))]
        assert len(tasks) == 1
        assert tasks[0].projected_schema.names == ["id", "keep"]
        batches = [batch async for batch in tasks[0].read()]
        assert sum(len(batch) for batch in batches) == 5  # Daft's bridge applies the pushed filter/limit
        assert all(len(batch) <= 2 for batch in batches)

    asyncio.run(inspect())


def test_zero_column_cardinality(vortex_file: Path) -> None:
    source = reference._VortexReferenceSource([str(vortex_file)], batch_size=2)

    async def inspect() -> None:
        tasks = [task async for task in source.get_tasks(Pushdowns(columns=[]))]
        batches = [batch async for batch in tasks[0].read()]
        assert [len(batch) for batch in batches] == [2, 2, 1]
        assert all(len(batch.schema()) == 0 for batch in batches)

    asyncio.run(inspect())
    assert reference.read_vortex_reference(vortex_file).count_rows() == 5


def test_empty_file(tmp_path: Path, vx) -> None:
    path = tmp_path / "empty.vortex"
    vx.io.write(vx.array(pa.table({"id": pa.array([], type=pa.int64())})), str(path))
    frame = reference.read_vortex_reference(path, batch_size=2)
    assert frame.schema()["id"].dtype == daft.DataType.int64()
    assert frame.count_rows() == 0
    assert reference.read_vortex_reference(path).to_pydict() == {"id": []}


def test_schema_mismatch_even_when_projecting_away_difference(vortex_file: Path, tmp_path: Path, vx) -> None:
    other = tmp_path / "different.vortex"
    vx.io.write(vx.array(pa.table({"id": pa.array([9], type=pa.int64())})), str(other))
    frame = reference.read_vortex_reference([vortex_file, other]).select("id")
    with pytest.raises(Exception, match="Vortex schema mismatch"):
        frame.collect()


def test_nested_types_are_explicitly_unsupported(tmp_path: Path, vx) -> None:
    path = tmp_path / "nested.vortex"
    vx.io.write(vx.array(pa.table({"values": [[1, 2], [3]]})), str(path))
    with pytest.raises(NotImplementedError, match="unsupported type"):
        reference.read_vortex_reference(path)


def test_corrupt_file_is_contextualized(tmp_path: Path, vx) -> None:
    path = tmp_path / "corrupt.vortex"
    path.write_bytes(b"not a vortex file")
    with pytest.raises(OSError, match="corrupt.vortex"):
        reference.read_vortex_reference(path)


def test_closing_task_closes_batch_generator(vortex_file: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    original = reference._iter_vortex_batches
    closed = []

    def observed(*args):
        try:
            yield from original(*args)
        finally:
            closed.append(True)

    monkeypatch.setattr(reference, "_iter_vortex_batches", observed)
    source = reference._VortexReferenceSource([str(vortex_file)], batch_size=2)

    async def inspect() -> None:
        tasks = [task async for task in source.get_tasks(Pushdowns())]
        batches = tasks[0].read()
        assert len(await anext(batches)) <= 2
        await batches.aclose()

    asyncio.run(inspect())
    assert closed == [True]
