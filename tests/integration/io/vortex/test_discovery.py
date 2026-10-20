"""Authenticated HTTP discovery concurrency without decoding file payloads."""

from __future__ import annotations

import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

import pytest

import daft
from daft import daft as native
from daft.io import _vortex

pytestmark = pytest.mark.integration


@pytest.fixture
def discovery_http(vortex_http_token):
    condition = threading.Condition()
    state = {
        "active": 0,
        "peak": 0,
        "expected": 1,
        "arrivals": set(),
        "later_done": False,
        "timeout": False,
        "stop": False,
        "get_requests": 0,
    }
    calls = []

    class Handler(BaseHTTPRequestHandler):
        def log_message(self, *_args):
            pass

        def do_HEAD(self):
            target = self.path.split("?", 1)[0]
            authorized = self.headers.get("Authorization") == f"Bearer {vortex_http_token}"
            with condition:
                state["active"] += 1
                state["peak"] = max(state["peak"], state["active"])
                state["arrivals"].add(target)
                calls.append((self.path, authorized))
                condition.notify_all()
                ready = condition.wait_for(
                    lambda: len(state["arrivals"]) >= state["expected"] or state["stop"], timeout=3
                )
                state["timeout"] |= not ready
                if state["expected"] > 1 and target == "/0.bin":
                    ready = condition.wait_for(lambda: state["later_done"] or state["stop"], timeout=3)
                    state["timeout"] |= not ready
                # Pending response accounting ends before sending headers so the
                # client's next input cannot race the bookkeeping for this one.
                state["active"] -= 1
            self.send_response(200 if authorized else 401)
            self.send_header("Content-Length", "17" if authorized else "0")
            self.end_headers()
            if target != "/0.bin":
                with condition:
                    state["later_done"] = True
                    condition.notify_all()

        def do_GET(self):
            with condition:
                state["get_requests"] += 1
            self.send_response(500)
            self.send_header("Content-Length", "0")
            self.end_headers()

    server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    server.daemon_threads = True
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    urls = [f"http://127.0.0.1:{server.server_port}/{index}.bin?token=sentinel-query" for index in range(5)]
    config = daft.io.IOConfig(http=daft.io.HTTPConfig(bearer_token=vortex_http_token, num_tries=1))
    try:
        yield urls, config, state, calls
    finally:
        with condition:
            state["stop"] = True
            condition.notify_all()
        server.shutdown()
        server.server_close()
        thread.join(timeout=5)
        assert not thread.is_alive()
        assert state["active"] == 0


@pytest.mark.parametrize("concurrency", [1, 3])
def test_vortex_http_discovery_is_bounded_ordered_and_uses_config(discovery_http, concurrency):
    urls, config, state, calls = discovery_http
    state["expected"] = concurrency
    escaped = [_vortex._discovery_path(url) for url in urls]
    outcomes, metrics = native._io_glob_bulk(
        escaped, io_config=config, max_concurrent_inputs=concurrency, collect_metrics=True
    )
    assert [outcome["files"][0]["path"] for outcome in outcomes] == urls
    assert state["peak"] == concurrency
    assert not state["timeout"]
    assert state["get_requests"] == 0
    assert all(authorized for _, authorized in calls)
    assert metrics["head_requests"] == len(urls)
    assert metrics["get_requests"] == metrics["list_requests"] == metrics["bytes_read"] == 0
    assert all("?token=sentinel-query" in path for path, _ in calls)


def test_vortex_http_discovery_wrong_credentials_are_rejected_and_redacted(discovery_http):
    urls, _, state, calls = discovery_http
    wrong = daft.io.IOConfig(http=daft.io.HTTPConfig(bearer_token="wrong-discovery-secret", num_tries=1))
    with pytest.raises(OSError) as failure:
        _vortex._resolve_vortex_paths(urls[:1], wrong)
    assert "sentinel-query" not in str(failure.value)
    assert "wrong-discovery-secret" not in str(failure.value)
    assert calls and not any(authorized for _, authorized in calls)
    assert state["get_requests"] == 0
