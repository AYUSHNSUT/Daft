"""Consumer ownership checks through real native Vortex queries.

These exercise public DataFrame iterators and the existing executor's plan-count
introspection. Closing a consumer calls the current finish path, which can wait
for execution. Registry cleanup and retained buffers do not prove prompt OS-read
cancellation, bounded prefetch, or a fixed memory limit. Controlled reader-poll
and pending-read cancellation tests live in the Rust reader suite.
"""

from __future__ import annotations

import gc
import weakref
from pathlib import Path

import pyarrow as pa
import pytest

import daft
from daft import daft as _native


@pytest.fixture
def vortex_input(tmp_path: Path):
    if not hasattr(_native, "read_vortex_schema"):
        pytest.skip("requires a Daft build with Rust features python,vortex")
    if daft.get_or_infer_runner_type() != "native":
        pytest.skip("experimental Vortex lifecycle checks require the native runner")
    writer = pytest.importorskip("vortex", reason="optional vortex-data package is needed only to write test fixtures")
    rows = 217
    table = pa.table(
        {
            "id": range(rows),
            "text": [None if index % 7 == 0 else f"row-{index}: 汉字 {'x' * (index % 17)}" for index in range(rows)],
            "payload": [None if index % 11 == 0 else bytes([index % 256]) * (index % 19) for index in range(rows)],
        }
    )
    path = tmp_path / "lifecycle.vortex"
    writer.io.write(table, str(path))
    # Fixed small batches ensure the consumer can abandon unconsumed output.
    # They do not constrain the native runner's result queue or Vortex prefetch.
    with daft.execution_config_ctx(default_morsel_size=7, enable_dynamic_batching=False, maintain_order=True):
        yield path, table


def _executor():
    # This existing diagnostic observes registry cleanup, not OS I/O completion.
    return daft.get_or_create_runner().native_executor._executor


def _snapshot_buffers(batch: pa.RecordBatch):
    return [
        (buffer.address, buffer.to_pybytes())
        for array in batch.columns
        for buffer in array.buffers()
        if buffer is not None
    ]


def _assert_retained_batch(batch: pa.RecordBatch, expected: dict, buffers) -> None:
    assert batch.to_pydict() == expected
    # Re-enumerate through the retained batch: snapshots contain no independent
    # Arrow buffer owners that could keep otherwise orphaned memory alive.
    assert _snapshot_buffers(batch) == buffers


@pytest.mark.parametrize("consumer", ["partitions", "arrow"])
@pytest.mark.parametrize("release", ["close", "drop"])
def test_vortex_early_consumer_release_preserves_retained_buffers(vortex_input, consumer: str, release: str) -> None:
    path, table = vortex_input
    executor = _executor()
    baseline = executor.active_plan_count()
    frame = daft.read_vortex(path, batch_size=7)
    iterator = frame.iter_partitions() if consumer == "partitions" else frame.to_arrow_iter()
    first = next(iterator)
    batch = first.to_arrow().to_batches()[0] if consumer == "partitions" else first
    assert 0 < batch.num_rows < table.num_rows
    expected = table.slice(0, batch.num_rows).to_pydict()
    buffers = _snapshot_buffers(batch)
    _assert_retained_batch(batch, expected, buffers)
    assert executor.active_plan_count() == baseline + 1

    if release == "close":
        iterator.close()
        with pytest.raises(StopIteration):
            next(iterator)
    else:
        reference = weakref.ref(iterator)
        del iterator
        gc.collect()
        assert reference() is None
    del first, frame
    gc.collect()
    assert executor.active_plan_count() == baseline
    _assert_retained_batch(batch, expected, buffers)

    # Fresh queries verify reuse and retained ownership. Their success alone
    # would not establish that the abandoned query was cancelled.
    for _ in range(2):
        assert daft.read_vortex(path, batch_size=7).to_pydict() == table.to_pydict()
        assert executor.active_plan_count() == baseline
        _assert_retained_batch(batch, expected, buffers)


def test_vortex_consumer_exception_preserved_after_previous_output(vortex_input) -> None:
    path, table = vortex_input
    executor = _executor()
    baseline = executor.active_plan_count()
    iterator = daft.read_vortex(path, batch_size=7).to_arrow_iter()
    batch = next(iterator)
    expected = table.slice(0, batch.num_rows).to_pydict()
    buffers = _snapshot_buffers(batch)

    class ConsumerFailure(Exception):
        pass

    # This is a consumer-side exception, not a simulated native read error.
    # It must propagate while the iterator's real execution finish path runs.
    try:
        with pytest.raises(ConsumerFailure, match="consumer stopped"):
            iterator.throw(ConsumerFailure("consumer stopped"))
    finally:
        iterator.close()
    assert executor.active_plan_count() == baseline
    _assert_retained_batch(batch, expected, buffers)
    assert daft.read_vortex(path, batch_size=7).count_rows() == table.num_rows
    assert executor.active_plan_count() == baseline


def test_vortex_eof_preserves_earlier_batches_while_consuming_later_batches(vortex_input) -> None:
    path, table = vortex_input
    executor = _executor()
    baseline = executor.active_plan_count()
    iterator = daft.read_vortex(path, batch_size=7).to_arrow_iter()
    first = next(iterator)
    expected = table.slice(0, first.num_rows).to_pydict()
    buffers = _snapshot_buffers(first)
    batches = [first]
    try:
        for batch in iterator:
            _assert_retained_batch(first, expected, buffers)
            batches.append(batch)
    finally:
        iterator.close()
    assert len(batches) > 1
    assert pa.Table.from_batches(batches).to_pydict() == table.to_pydict()
    assert executor.active_plan_count() == baseline
    _assert_retained_batch(first, expected, buffers)


def test_vortex_later_query_error_keeps_earlier_output_valid(vortex_input, tmp_path: Path) -> None:
    path, table = vortex_input
    executor = _executor()
    baseline = executor.active_plan_count()
    earlier = daft.read_vortex(path, batch_size=7).to_arrow_iter()
    batch = next(earlier)
    expected = table.slice(0, batch.num_rows).to_pydict()
    buffers = _snapshot_buffers(batch)
    earlier.close()
    assert executor.active_plan_count() == baseline

    corrupt = tmp_path / "corrupt-after-earlier-output.vortex"
    corrupt.write_bytes(b"not a Vortex file")
    # Planning uses the valid first file; execution discovers the bad footer in
    # the second. We make no assumption about whether this failing query emits
    # any output before the error: parallel file execution may report it early.
    failed = daft.read_vortex([path, corrupt], batch_size=7).iter_partitions()
    try:
        with pytest.raises(Exception, match="corrupt-after-earlier-output.vortex"):
            list(failed)
    finally:
        failed.close()
    assert executor.active_plan_count() == baseline
    _assert_retained_batch(batch, expected, buffers)
    assert daft.read_vortex(path, batch_size=7).to_pydict() == table.to_pydict()
    assert executor.active_plan_count() == baseline
