"""Shared native result buffering and explicit iterator ownership."""

from __future__ import annotations

import asyncio
import os
import threading
from types import SimpleNamespace

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

import daft
from daft.context import execution_config_ctx, get_context
from daft.daft import LocalPhysicalPlan
from daft.daft import NativeExecutor as RustNativeExecutor
from daft.event_loop import BackgroundEventLoop
from daft.execution.native_executor import NativeExecutor
from daft.recordbatch import MicroPartition
from daft.runners import get_or_create_runner

pytestmark = pytest.mark.skipif(os.environ.get("DAFT_RUNNER") != "native", reason="Native executor lifecycle")


@pytest.fixture
def background_loop():
    loop = asyncio.new_event_loop()
    thread = threading.Thread(target=loop.run_forever, daemon=True)
    thread.start()
    try:
        yield BackgroundEventLoop.from_existing_loop(loop)
    finally:
        loop.call_soon_threadsafe(loop.stop)
        thread.join(timeout=5)
        assert not thread.is_alive()
        loop.close()


def test_interrupted_background_wait_cancels_coroutine_and_releases_owner(monkeypatch, background_loop):
    started = threading.Event()
    released = threading.Event()
    submit = asyncio.run_coroutine_threadsafe

    async def blocked_operation():
        try:
            started.set()
            await asyncio.Future()
        finally:
            released.set()

    class InterruptedWait:
        def __init__(self, submitted):
            self.submitted = submitted

        def result(self):
            assert started.wait(timeout=5)
            raise KeyboardInterrupt

        def cancel(self):
            return self.submitted.cancel()

    monkeypatch.setattr(asyncio, "run_coroutine_threadsafe", lambda future, loop: InterruptedWait(submit(future, loop)))
    with pytest.raises(KeyboardInterrupt):
        background_loop.run(blocked_operation())
    assert released.wait(timeout=5), "the interrupted coroutine must run its cleanup"


@pytest.mark.parametrize("termination", ["eof", "close", "error"])
def test_python_bridge_finishes_eof_and_closes_abandoned_results(monkeypatch, background_loop, termination):
    import daft.execution.native_executor as module

    partition = MicroPartition.from_arrow(pa.table({"id": [1]}))._micropartition
    stats = SimpleNamespace(query_plan="retained execution stats")

    class Receiver:
        def __init__(self):
            self.next_calls = 0
            self.finished = 0
            self.closed = 0

        def __aiter__(self):
            return self

        async def __anext__(self):
            self.next_calls += 1
            if self.next_calls == 1:
                return partition
            if termination == "error":
                raise RuntimeError("producer failed")
            return None

        async def try_finish(self):
            self.finished += 1
            return stats

        async def aclose(self):
            self.closed += 1

    receiver = Receiver()
    observed = {}

    async def run(*args, **kwargs):
        observed.update(kwargs)
        return receiver

    executor = NativeExecutor()
    executor._executor = SimpleNamespace(run=run)
    monkeypatch.setattr(module, "get_or_init_event_loop", lambda: background_loop)
    iterator = executor.run(None, {}, get_context(), None, results_buffer_size=3)
    assert next(iterator).micropartition().to_arrow().to_pydict() == {"id": [1]}
    assert observed["results_buffer_size"] == 3
    if termination == "close":
        iterator.close()
    elif termination == "error":
        with pytest.raises(RuntimeError, match="producer failed"):
            next(iterator)
    else:
        with pytest.raises(StopIteration) as stopped:
            next(iterator)
        assert stopped.value.value == ("retained execution stats", stats)
    assert receiver.finished == int(termination == "eof")
    assert receiver.closed == int(termination != "eof")
    iterator.close()


@pytest.fixture
def parquet_plan(tmp_path):
    path = tmp_path / "input.parquet"
    pq.write_table(pa.table({"id": list(range(32))}), path)
    frame = daft.read_parquet(str(path)).into_batches(1)
    builder = frame._builder.optimize(get_context().daft_execution_config)
    plan, inputs = LocalPhysicalPlan.from_logical_plan_builder(builder._builder, {})
    return path, frame, plan, inputs


@pytest.mark.parametrize("capacity", [None, 1, 3])
def test_native_result_queue_preserves_rows_and_eof_stats(parquet_plan, capacity):
    _, _, plan, inputs = parquet_plan
    executor = NativeExecutor()
    iterator = executor.run(plan, inputs, get_context(), None, results_buffer_size=capacity)
    rows = []
    while True:
        try:
            rows.extend(next(iterator).micropartition().to_arrow().column("id").to_pylist())
        except StopIteration as stopped:
            _, stats = stopped.value
            break
    assert rows == list(range(32))
    assert stats is not None
    assert executor._executor.active_plan_count() == 0


@pytest.mark.parametrize("iterator_name", ["iter_rows", "to_arrow_iter", "iter_partitions"])
@pytest.mark.parametrize("capacity", [None, 1, 3])
def test_public_iterator_close_releases_native_plan(parquet_plan, iterator_name, capacity, monkeypatch):
    _, frame, _, _ = parquet_plan
    wrapper = get_or_create_runner().native_executor
    executor = wrapper._executor
    run = wrapper.run
    observed = []

    def record_capacity(*args, **kwargs):
        observed.append(kwargs["results_buffer_size"])
        return run(*args, **kwargs)

    monkeypatch.setattr(wrapper, "run", record_capacity)
    previous = executor.active_plan_count()
    iterator = getattr(frame, iterator_name)(results_buffer_size=capacity)
    first = next(iterator)
    iterator.close()
    assert observed == [capacity]
    assert executor.active_plan_count() == previous
    # A retained result remains valid even after its producer is closed.
    if iterator_name == "iter_rows":
        assert first == {"id": 0}
    elif iterator_name == "to_arrow_iter":
        assert first.to_pydict() == {"id": [0]}
    else:
        assert first.to_arrow().to_pydict() == {"id": [0]}
    assert frame.limit(1).to_pydict() == {"id": [0]}


@pytest.mark.parametrize("capacity", [0, -1, True, 1.5, "1", 2**63, 2**100])
def test_native_rejects_invalid_result_capacity_before_start(parquet_plan, capacity):
    _, _, plan, inputs = parquet_plan
    executor = RustNativeExecutor(False, "")

    async def run():
        with pytest.raises((TypeError, ValueError), match="results_buffer_size"):
            await executor.run(plan, get_context()._ctx, 0, inputs, results_buffer_size=capacity)

    asyncio.run(run())
    assert executor.active_plan_count() == 0


def test_native_pending_next_can_close_twice(parquet_plan):
    _, _, plan, _ = parquet_plan
    executor = RustNativeExecutor(False, "")

    async def run():
        # The source receives no input, so a pending next cannot reach EOF.
        receiver = await executor.run(plan, get_context()._ctx, 0, {}, results_buffer_size=1)
        pending = receiver.__anext__()
        await asyncio.wait_for(receiver.aclose(), timeout=10)
        assert await asyncio.wait_for(pending, timeout=10) is None
        await receiver.aclose()
        assert await receiver.__anext__() is None

    asyncio.run(run())
    assert executor.active_plan_count() == 0


def test_native_execution_error_reaches_consumer_and_cleans_plan(parquet_plan):
    path, _, plan, inputs = parquet_plan
    path.unlink()
    for maintain_order in [True, False]:
        with execution_config_ctx(maintain_order=maintain_order):
            executor = NativeExecutor()
            iterator = executor.run(plan, inputs, get_context(), None, results_buffer_size=1)
            with pytest.raises(Exception, match=r"(?i)(not found|no such file|unable to open)"):
                list(iterator)
            assert executor._executor.active_plan_count() == 0


def test_runner_consumer_error_closes_owned_results(parquet_plan, monkeypatch):
    from daft.runners.native_runner import LocalPartitionSet

    _, frame, _, _ = parquet_plan
    runner = get_or_create_runner()
    executor = runner.native_executor._executor
    previous = executor.active_plan_count()

    def fail_to_store(*args):
        raise RuntimeError("consumer cannot retain result")

    monkeypatch.setattr(LocalPartitionSet, "set_partition", fail_to_store)
    with pytest.raises(RuntimeError, match="consumer cannot retain result"):
        runner.run(frame._builder)
    assert executor.active_plan_count() == previous


def test_native_shared_plan_close_does_not_cancel_other_input(parquet_plan):
    _, _, plan, inputs = parquet_plan
    executor = RustNativeExecutor(False, "")
    context = {"plan_fingerprint": "7237"}

    async def run():
        abandoned = await executor.run(plan, get_context()._ctx, 0, inputs, context, results_buffer_size=1)
        retained = await executor.run(plan, get_context()._ctx, 1, inputs, context, results_buffer_size=1)
        await abandoned.aclose()
        assert executor.active_plan_count() == 1
        with pytest.raises(ValueError, match="abandoned.*cannot be reused"):
            await executor.run(plan, get_context()._ctx, 0, inputs, context, results_buffer_size=1)
        rows = []
        while (part := await asyncio.wait_for(retained.__anext__(), timeout=10)) is not None:
            rows.extend(MicroPartition._from_pymicropartition(part).to_arrow().column("id").to_pylist())
        assert rows == list(range(32))
        assert await retained.try_finish() is not None
        await retained.aclose()

    asyncio.run(run())
    assert executor.active_plan_count() == 0
