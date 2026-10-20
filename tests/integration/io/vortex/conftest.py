"""Local service fixtures. Service startup failures are errors, never skips."""

from __future__ import annotations

import os
import re
import socket
import threading
import uuid
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

import pyarrow as pa
import pytest

import daft
from daft import daft as _native


@pytest.fixture
def vortex_service_file(tmp_path):
    if not hasattr(_native, "read_vortex_schema"):
        pytest.skip("requires a Daft build with Rust features python,vortex")
    writer = pytest.importorskip("vortex", reason="optional vortex-data package writes service fixtures")
    table = pa.table(
        {
            "id": range(31),
            "value": [index % 7 for index in range(31)],
            "payload": [f"{index}:" + "x" * 1024 for index in range(31)],
        }
    )
    path = tmp_path / "data.vortex"
    writer.io.write(table, str(path))
    return path, table


@pytest.fixture
def vortex_http_token():
    return "vortex-local-test-token"


@pytest.fixture
def vortex_http(vortex_service_file, vortex_http_token):
    path, table = vortex_service_file
    token = vortex_http_token
    payloads = {
        "/data.vortex": path.read_bytes(),
        "/truncate.vortex": path.read_bytes(),
        "/ignored-range.vortex": path.read_bytes(),
        "/wrong-range.vortex": path.read_bytes(),
        "/corrupt.vortex": b"not a Vortex file",
    }
    requests = []
    lock = threading.Lock()
    truncated = False

    class Handler(BaseHTTPRequestHandler):
        protocol_version = "HTTP/1.1"

        def log_message(self, *_args):
            pass

        def _serve(self, head):
            nonlocal truncated
            target = self.path.split("?", 1)[0]
            authorized = self.headers.get("Authorization") == f"Bearer {token}"
            range_header = self.headers.get("Range")
            status = 200
            payload = payloads.get(target, b"")
            start, end = 0, len(payload) - 1
            if not authorized:
                status, payload = 401, b""
            elif target not in payloads:
                status, payload = 404, b""
            elif range_header is not None and target != "/ignored-range.vortex":
                match = re.fullmatch(r"bytes=(\d*)-(\d*)", range_header)
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
            with lock:
                truncate = (
                    target == "/truncate.vortex" and not head and status == 206 and not truncated and len(payload) > 1
                )
                if truncate:
                    truncated = True
                sent = max(1, len(payload) // 2) if truncate else len(payload)
                requests.append(
                    {
                        "method": self.command,
                        "path": self.path,
                        "range": range_header,
                        "status": status,
                        "authorized": authorized,
                        "truncated": truncate,
                        "resume_at": start + sent,
                        "end": end,
                    }
                )
            self.send_response(status)
            self.send_header("Content-Type", "application/octet-stream")
            self.send_header("Accept-Ranges", "bytes")
            self.send_header("Content-Length", str(len(payload)))
            if status == 206:
                offset = (-1 if start else 1) if target == "/wrong-range.vortex" else 0
                self.send_header("Content-Range", f"bytes {start + offset}-{end + offset}/{len(payloads[target])}")
            self.end_headers()
            if not head and payload:
                self.wfile.write(payload[:sent])
                self.wfile.flush()
                if truncate:
                    # Advertise the full requested body, then terminate it
                    # early. The I/O client's resumable stream must request
                    # the remaining interval; no timing sleep is involved.
                    self.close_connection = True
                    self.connection.shutdown(socket.SHUT_WR)

        def do_HEAD(self):
            self._serve(True)

        def do_GET(self):
            self._serve(False)

    # Do not catch EPERM and skip: a selected service acceptance run must fail
    # clearly when its host does not permit a loopback listener.
    server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    server.daemon_threads = True
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    config = daft.io.IOConfig(
        http=daft.io.HTTPConfig(
            bearer_token=token, num_tries=2, retry_initial_backoff_ms=1, connect_timeout_ms=2000, read_timeout_ms=2000
        )
    )
    try:
        yield f"http://127.0.0.1:{server.server_port}/data.vortex", config, requests, table
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=5)
        assert not thread.is_alive()


@pytest.fixture
def vortex_minio(vortex_service_file):
    endpoint = os.environ.get("VORTEX_MINIO_ENDPOINT")
    if not endpoint:
        pytest.fail("MinIO acceptance requires VORTEX_MINIO_ENDPOINT; use examples/vortex_eval/run_phase3_services.py")
    boto3 = pytest.importorskip("boto3", reason="boto3 provisions the temporary MinIO acceptance bucket")
    from botocore.config import Config

    key = os.environ.get("VORTEX_MINIO_ACCESS_KEY", "vortexdev")
    secret = os.environ.get("VORTEX_MINIO_SECRET_KEY", "vortexdev-secret")
    region = os.environ.get("VORTEX_MINIO_REGION", "us-east-1")
    client = boto3.client(
        "s3",
        endpoint_url=endpoint,
        aws_access_key_id=key,
        aws_secret_access_key=secret,
        region_name=region,
        config=Config(s3={"addressing_style": "path"}, retries={"max_attempts": 1}),
    )
    bucket = f"vortex-acceptance-{uuid.uuid4().hex[:16]}"
    client.create_bucket(Bucket=bucket)
    path, table = vortex_service_file
    config = daft.io.IOConfig(
        s3=daft.io.S3Config(
            endpoint_url=endpoint,
            key_id=key,
            access_key=secret,
            region_name=region,
            use_ssl=endpoint.startswith("https://"),
            force_virtual_addressing=False,
            num_tries=1,
        )
    )
    try:
        for name in ["prefix/explicit.bin", "prefix/nested/data.vortex"]:
            client.put_object(Bucket=bucket, Key=name, Body=path.read_bytes())
        yield f"s3://{bucket}", config, table
    finally:
        # Delete only the test's unique bucket and objects, including on failure.
        objects = client.list_objects_v2(Bucket=bucket).get("Contents", [])
        if objects:
            client.delete_objects(Bucket=bucket, Delete={"Objects": [{"Key": item["Key"]} for item in objects]})
        client.delete_bucket(Bucket=bucket)
