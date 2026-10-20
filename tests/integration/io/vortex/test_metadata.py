"""Real HTTP acceptance for bounded, ordered Vortex footer discovery."""

from __future__ import annotations

import re
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

import pyarrow as pa
import pytest

import daft
from daft import daft as _native

pytestmark = pytest.mark.integration


class _MetadataRequests:
    def __init__(self) -> None:
        self.condition = threading.Condition()
        self.stopping = threading.Event()
        self.handlers = 0
        self.pending = {}
        self.peak = 0
        self.calls = []
        self.first_paths = set()
        self.parallelism = 1
        self.reorder = False
        self.completed_gets = []
        self.barrier_timed_out = False
        self.hold_failure_sibling = False
        self.sibling_get_entered = False
        self.release_sibling_get = False

    def reset(self, parallelism: int, *, reorder: bool = False) -> None:
        assert self.wait_idle(), "previous metadata HTTP requests did not finish"
        with self.condition:
            self.pending.clear()
            self.peak = 0
            self.calls.clear()
            self.first_paths.clear()
            self.parallelism = parallelism
            self.reorder = reorder
            self.completed_gets.clear()
            self.barrier_timed_out = False
            self.hold_failure_sibling = False
            self.sibling_get_entered = False
            self.release_sibling_get = False

    def begin(self, method: str, path: str, authorized: bool, status: int) -> None:
        with self.condition:
            self.handlers += 1
            self.pending[path] = self.pending.get(path, 0) + 1
            self.peak = max(self.peak, len(self.pending))
            self.calls.append({"method": method, "path": path, "authorized": authorized, "status": status})
            self.first_paths.add(path)
            self.condition.notify_all()
            ready = self.condition.wait_for(
                lambda: len(self.first_paths) >= self.parallelism or self.stopping.is_set(), timeout=3
            )
            self.barrier_timed_out |= not ready
            if self.hold_failure_sibling and method == "GET":
                if path == "/0.vortex":
                    self.sibling_get_entered = True
                    self.condition.notify_all()
                    ready = self.condition.wait_for(
                        lambda: self.release_sibling_get or self.stopping.is_set(), timeout=3
                    )
                    self.barrier_timed_out |= not ready
                elif path == "/corrupt.vortex":
                    ready = self.condition.wait_for(
                        lambda: self.sibling_get_entered or self.stopping.is_set(), timeout=3
                    )
                    self.barrier_timed_out |= not ready
            # Hold the first file's GET until a later file completes. This
            # proves ordered results despite completion order, without a speed threshold.
            if self.reorder and method == "GET" and path == "/0.vortex":
                ready = self.condition.wait_for(lambda: bool(self.completed_gets) or self.stopping.is_set(), timeout=3)
                self.barrier_timed_out |= not ready
        self.stopping.wait(0.01)

    def responding(self, path: str) -> None:
        # Count requests waiting for a response. Decrement before sending it,
        # so a client that immediately opens its next file cannot race this bookkeeping.
        with self.condition:
            self.pending[path] -= 1
            if self.pending[path] == 0:
                del self.pending[path]
            self.condition.notify_all()

    def finish(self, method: str, path: str, complete: bool) -> None:
        with self.condition:
            if method == "GET" and complete:
                self.completed_gets.append(path)
            self.handlers -= 1
            self.condition.notify_all()

    def wait_idle(self) -> bool:
        with self.condition:
            return self.condition.wait_for(lambda: self.handlers == 0, timeout=5)


@pytest.fixture
def metadata_http(vortex_service_file, vortex_http_token, tmp_path):
    # The shared fixture establishes the feature and optional writer prerequisites.
    assert hasattr(_native, "read_vortex_metadata_bulk"), "rebuild with bulk Vortex metadata support"
    writer = pytest.importorskip("vortex")
    counts = [13, 0, 4, 2]
    payloads = {}
    expected_ids = []
    for index, rows in enumerate(counts):
        values = list(range(index * 100, index * 100 + rows))
        expected_ids.extend(values)
        path = tmp_path / f"{index}.vortex"
        writer.io.write(pa.table({"id": pa.array(values, pa.int64())}), str(path))
        payloads[f"/{index}.vortex"] = path.read_bytes()
    payloads["/corrupt.vortex"] = b"not a Vortex file"
    state = _MetadataRequests()

    class Handler(BaseHTTPRequestHandler):
        protocol_version = "HTTP/1.1"

        def log_message(self, *_args):
            pass

        def _serve(self, head: bool):
            target = self.path.split("?", 1)[0]
            authorized = self.headers.get("Authorization") == f"Bearer {vortex_http_token}"
            payload = payloads.get(target, b"")
            start, end = 0, len(payload) - 1
            status = 200
            if not authorized:
                status, payload = 401, b""
            elif target not in payloads:
                status, payload = 404, b""
            elif self.headers.get("Range") is not None:
                match = re.fullmatch(r"bytes=(\d*)-(\d*)", self.headers["Range"])
                if match is None or not any(match.groups()):
                    status, payload = 416, b""
                else:
                    first, last = match.groups()
                    if first:
                        start = int(first)
                        end = min(int(last), end) if last else end
                    else:
                        start = max(0, len(payload) - int(last))
                    if start > end:
                        status, payload = 416, b""
                    else:
                        status, payload = 206, payload[start : end + 1]
            state.begin(self.command, target, authorized, status)
            state.responding(target)
            complete = False
            try:
                self.send_response(status)
                self.send_header("Content-Type", "application/octet-stream")
                self.send_header("Accept-Ranges", "bytes")
                self.send_header("Content-Length", str(len(payload)))
                if status == 206:
                    self.send_header("Content-Range", f"bytes {start}-{end}/{len(payloads[target])}")
                self.end_headers()
                if not head and payload:
                    self.wfile.write(payload)
                    self.wfile.flush()
                complete = status == 206
            except (BrokenPipeError, ConnectionResetError):
                # A failed sibling footer may cancel this request's client.
                pass
            finally:
                state.finish(self.command, target, complete)

        def do_HEAD(self):
            self._serve(True)

        def do_GET(self):
            self._serve(False)

    server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    server.daemon_threads = True
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    base = f"http://127.0.0.1:{server.server_port}"
    config = daft.io.IOConfig(
        http=daft.io.HTTPConfig(
            bearer_token=vortex_http_token,
            num_tries=1,
            connect_timeout_ms=5000,
            read_timeout_ms=5000,
        )
    )
    try:
        yield [f"{base}/{index}.vortex" for index in range(len(counts))], counts, expected_ids, config, state
    finally:
        state.stopping.set()
        with state.condition:
            state.condition.notify_all()
        server.shutdown()
        server.server_close()
        thread.join(timeout=5)
        assert not thread.is_alive()
        assert state.wait_idle(), "metadata HTTP handler remained active after service cleanup"
        assert not state.pending


@pytest.mark.parametrize("concurrency", [1, 3])
def test_vortex_http_bulk_metadata_is_bounded_and_preserves_order(metadata_http, concurrency: int) -> None:
    urls, counts, expected_ids, config, state = metadata_http
    state.reset(concurrency, reorder=concurrency > 1)

    metadata, metrics = _native.read_vortex_metadata_bulk(
        urls,
        io_config=config,
        max_concurrent_files=concurrency,
        max_concurrent_requests=1,
        max_in_flight_bytes=1024 * 1024,
    )

    assert state.wait_idle()
    assert not state.barrier_timed_out
    assert [rows for _, rows in metadata] == counts
    assert state.peak == concurrency
    if concurrency > 1:
        assert state.completed_gets[0] != "/0.vortex"
    assert all(request["authorized"] for request in state.calls)
    assert metrics["num_files"] == len(urls)
    assert metrics["max_concurrent_files"] == concurrency
    assert metrics["head_requests"] == sum(request["method"] == "HEAD" for request in state.calls)
    assert metrics["get_requests"] == sum(request["method"] == "GET" for request in state.calls)
    assert metrics["list_requests"] == 0
    assert metrics["bytes_read"] > 0

    state.reset(1)
    query = daft.read_vortex(urls, io_config=config, metadata_concurrency=concurrency).sort("id")
    assert query.to_pydict() == {"id": sorted(expected_ids)}
    assert state.wait_idle()


def test_vortex_http_bulk_metadata_failure_releases_pending_requests(metadata_http) -> None:
    urls, counts, _, config, state = metadata_http
    corrupt = urls[0].replace("0.vortex", "corrupt.vortex") + "?token=sentinel-metadata-query"
    state.reset(2)
    state.hold_failure_sibling = True

    try:
        with pytest.raises(Exception) as failure:
            _native.read_vortex_metadata_bulk(
                [corrupt, *urls], io_config=config, max_concurrent_files=2, max_concurrent_requests=1
            )
        # The corrupt file cannot respond until its valid sibling is waiting.
        # Failure must return while that sibling's response is still withheld.
        assert state.sibling_get_entered
        assert not state.release_sibling_get
        assert not state.barrier_timed_out
    finally:
        with state.condition:
            state.release_sibling_get = True
            state.condition.notify_all()

    assert "sentinel-metadata-query" not in str(failure.value)
    assert state.wait_idle(), "failed bulk metadata left an HTTP handler running"
    assert not state.pending
    assert not state.barrier_timed_out
    assert state.peak <= 2
    state.reset(2)
    metadata, _ = _native.read_vortex_metadata_bulk(
        urls, io_config=config, max_concurrent_files=2, max_concurrent_requests=1
    )
    assert [rows for _, rows in metadata] == counts
    assert state.wait_idle()
    assert not state.pending
    assert not state.barrier_timed_out
