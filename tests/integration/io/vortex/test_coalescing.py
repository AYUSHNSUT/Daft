"""Controlled response delays validate request shape, without timing assertions."""

from __future__ import annotations

import hashlib
import json
import re
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

import pyarrow as pa
import pytest

import daft
from daft import daft as native
from daft.io import _vortex_diagnostics as diagnostics

pytestmark = pytest.mark.integration

_BYTE_BUDGET = 512 * 1024
_MERGE_MAXIMUM = 4 * 1024 * 1024
_QUERY_SECRET = "sentinel-coalescing-query-secret"
_BEARER_TOKEN = "sentinel-coalescing-bearer-token"


@pytest.fixture(scope="module")
def coalescing_file(tmp_path_factory):
    if not hasattr(native, "_vortex_diagnostics_start"):
        pytest.skip("requires a Daft build with Rust features python,vortex")
    writer = pytest.importorskip("vortex", reason="optional vortex-data package writes service fixtures")
    rows = 2048
    columns = {"id": list(range(rows)), "value": [index % 17 for index in range(rows)]}
    for column in range(8):
        # Distinct incompressible payloads keep real column segments outside the
        # initial 65535-byte footer read and below the per-request byte budget.
        columns[f"payload_{column}"] = [hashlib.shake_256(f"{column}:{row}".encode()).digest(64) for row in range(rows)]
    table = pa.table(columns)
    path = tmp_path_factory.mktemp("vortex-coalescing") / "columns.vortex"
    writer.io.write(table, str(path))
    payload = path.read_bytes()
    assert len(payload) > 4 * 65535, "fixture must exercise data reads beyond the initial footer buffer"
    expected = {name: [value for row, value in enumerate(values) if row % 17 >= 9] for name, values in columns.items()}
    return payload, expected


@pytest.fixture
def coalescing_http(coalescing_file, request):
    payload, expected = coalescing_file
    response_delay_ms = request.param
    lock = threading.Lock()
    state = {"pending_headers": 0, "peak_pending_headers": 0}
    calls = []

    class Handler(BaseHTTPRequestHandler):
        def log_message(self, *_args):
            pass

        def _serve(self, head):
            authorized = self.headers.get("Authorization") == f"Bearer {_BEARER_TOKEN}"
            range_header = self.headers.get("Range")
            start, end = 0, len(payload) - 1
            status = 200 if authorized else 401
            if authorized and range_header is not None:
                match = re.fullmatch(r"bytes=(\d+)-(\d+)", range_header)
                if match is None:
                    status = 416
                else:
                    start, end = map(int, match.groups())
                    status = 206 if 0 <= start <= end < len(payload) else 416
            body = payload[start : end + 1] if status in {200, 206} else b""
            with lock:
                if not head:
                    state["pending_headers"] += 1
                    state["peak_pending_headers"] = max(state["peak_pending_headers"], state["pending_headers"])
                calls.append(
                    {
                        "method": self.command,
                        "range": range_header,
                        "start": start,
                        "length": len(body),
                        "status": status,
                        "authorized": authorized,
                        "query_preserved": self.path.endswith(f"?token={_QUERY_SECRET}"),
                    }
                )
            if response_delay_ms:
                time.sleep(response_delay_ms / 1000)
            # Finish this accounting before releasing headers to the client:
            # a completed response cannot race its next request's bookkeeping.
            with lock:
                if not head:
                    state["pending_headers"] -= 1
            self.send_response(status)
            self.send_header("Content-Length", str(len(body)))
            self.send_header("Accept-Ranges", "bytes")
            if status == 206:
                self.send_header("Content-Range", f"bytes {start}-{end}/{len(payload)}")
            self.end_headers()
            if not head:
                self.wfile.write(body)

        def do_HEAD(self):
            self._serve(True)

        def do_GET(self):
            self._serve(False)

    # Startup errors fail acceptance. The default HTTP/1.0 response closes each
    # connection, making teardown independent of cached client connections.
    server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    server.daemon_threads = True
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    config = daft.io.IOConfig(
        http=daft.io.HTTPConfig(bearer_token=_BEARER_TOKEN, num_tries=1, connect_timeout_ms=2000, read_timeout_ms=5000)
    )
    url = f"http://127.0.0.1:{server.server_port}/columns.vortex?token={_QUERY_SECRET}"
    try:
        yield url, config, calls, state, lock, expected, len(payload)
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=5)
        assert not thread.is_alive()
        assert state["pending_headers"] == 0


@pytest.mark.parametrize("coalescing_http", [0, 5, 20], indirect=True, ids=["delay-0ms", "delay-5ms", "delay-20ms"])
@pytest.mark.parametrize("max_requests", [1, 3])
def test_vortex_delayed_http_coalescing_preserves_rows_and_bounds_real_ranges(coalescing_http, max_requests):
    url, config, calls, state, lock, expected, size = coalescing_http
    data_ranges = {}
    for enabled in (False, True):
        with lock:
            calls.clear()
            state["peak_pending_headers"] = 0
        diagnostics.start()
        try:
            result = (
                daft.read_vortex(
                    url,
                    io_config=config,
                    batch_size=512,
                    enable_read_coalescing=enabled,
                    read_coalesce_gap_bytes=65536,
                    read_coalesce_max_bytes=_MERGE_MAXIMUM,
                    max_concurrent_requests=max_requests,
                    max_in_flight_bytes=_BYTE_BUDGET,
                )
                .where(daft.col("value") >= 9)
                .to_pydict()
            )
        finally:
            capture = diagnostics.finish()
        assert result == expected
        native_capture = capture["native"]
        assert native_capture["file_count"] == 1
        assert native_capture["dropped_events"] == native_capture["unfinished_events"] == 0
        assert capture["python"]["dropped_events"] == 0
        events = native_capture["events"]
        ranges = [event for event in events if event["operation"] == "range"]
        data = [event for event in ranges if event["phase"] == "data"]
        assert data, "the fixture must read data beyond the footer buffer"
        assert not any(event["error"] or event["cancelled"] for event in ranges)
        assert all(event["returned_bytes"] == event["requested_bytes"] for event in ranges)
        assert all(0 < event["requested_bytes"] <= _BYTE_BUDGET for event in ranges)
        assert all(event["offset"] + event["requested_bytes"] <= size for event in ranges)
        data_ranges[enabled] = [(event["offset"], event["requested_bytes"]) for event in data]
        with lock:
            observed = list(calls)
            peak = state["peak_pending_headers"]
        gets = [call for call in observed if call["method"] == "GET"]
        assert gets and len(gets) == len(ranges)
        assert all(call["authorized"] and call["query_preserved"] for call in observed)
        assert all(call["status"] == 206 and call["range"] is not None for call in gets)
        assert all(0 < call["length"] <= _BYTE_BUDGET for call in gets)
        assert sorted((call["start"], call["length"]) for call in gets) == sorted(
            (event["offset"], event["returned_bytes"]) for event in ranges
        )
        assert 1 <= peak <= max_requests
        serialized = json.dumps(capture)
        assert _QUERY_SECRET not in serialized
        assert _BEARER_TOKEN not in serialized
        assert "127.0.0.1" not in serialized
    # The pinned file driver admits at most max_requests logical ranges into a
    # batch, so a limit of one leaves no opportunity to merge across requests.
    if max_requests == 1:
        assert sorted(data_ranges[True]) == sorted(data_ranges[False])
    else:
        # Scheduling can submit singleton batches or nonadjacent columns even
        # with several requests in flight. Preserve coverage without requiring
        # a merge opportunity in every HTTP run. The native file-driver test
        # supplies a deterministic batch and proves the actual GET reduction.
        assert len(data_ranges[True]) <= len(data_ranges[False])
        assert all(
            any(start <= old_start and old_start + old_length <= start + length for start, length in data_ranges[True])
            for old_start, old_length in data_ranges[False]
        )
