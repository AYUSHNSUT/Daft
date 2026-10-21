"""Validated, task-scoped S3 opening-range reuse against real local MinIO."""

from __future__ import annotations

import asyncio
import hashlib
import re
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

import pytest

import daft
from daft import daft as native
from daft.io import _vortex
from daft.io import _vortex_diagnostics as diagnostics
from daft.io.pushdowns import Pushdowns

pytestmark = pytest.mark.integration


def _client(config):
    import boto3
    from botocore.config import Config

    s3 = config.s3
    return boto3.client(
        "s3",
        endpoint_url=s3.endpoint_url,
        aws_access_key_id=s3.key_id,
        aws_secret_access_key=s3.access_key,
        region_name=s3.region_name,
        config=Config(s3={"addressing_style": "path"}, retries={"max_attempts": 1}),
    )


@pytest.mark.parametrize("enabled", [False, True])
def test_minio_reuses_only_validated_opening_ranges(vortex_minio, enabled):
    base, config, table = vortex_minio
    diagnostics.start()
    try:
        result = (
            daft.read_vortex(
                base + "/prefix/explicit.bin", io_config=config, enable_metadata_reuse=enabled, batch_size=3
            )
            .where(daft.col("value") >= 4)
            .select("id")
            .to_pydict()
        )
    finally:
        capture = diagnostics.finish()
    assert result == {"id": [row["id"] for row in table.to_pylist() if row["value"] >= 4]}
    events = capture["native"]["events"]
    opening = [event for event in events if event["phase"] == "execution_open"]
    assert any(event["operation"] == "head" for event in opening), "always validate fresh object metadata"
    hits = [event for event in opening if event["operation"] == "cached_range"]
    assert bool(hits) is enabled
    if enabled:
        assert not any(event["operation"] == "range" for event in opening), "do not refetch opening bytes"
        assert all(event["requested_bytes"] == event["returned_bytes"] > 0 for event in hits)
    assert not any(event["error"] or event["cancelled"] for event in events)
    assert capture["native"]["unfinished_events"] == capture["native"]["dropped_events"] == 0


def test_minio_same_size_replacement_after_planning_is_rejected(vortex_minio, vortex_service_file, monkeypatch):
    base, config, _ = vortex_minio
    path, _ = vortex_service_file
    payload = path.read_bytes()
    changed = payload[:-1] + bytes([payload[-1] ^ 1])
    client = _client(config)
    original = native._read_vortex_metadata_bulk_cached
    snapshots = []

    def replace_after_planning(*args, **kwargs):
        metadata = original(*args, **kwargs)
        snapshots.extend(snapshot for _, _, snapshot in metadata[0] if snapshot is not None)
        client.put_object(Bucket=base.removeprefix("s3://"), Key="prefix/explicit.bin", Body=changed)
        return metadata

    monkeypatch.setattr(native, "_read_vortex_metadata_bulk_cached", replace_after_planning)
    with pytest.raises(Exception, match="(?i)object changed"):
        daft.read_vortex(base + "/prefix/explicit.bin", io_config=config, enable_metadata_reuse=True).select(
            "id"
        ).collect()
    assert snapshots, "the task must actually carry a validated snapshot"


def test_minio_reused_source_reads_fresh_footer_counts_each_time(vortex_minio, tmp_path, monkeypatch):
    import vortex

    base, config, table = vortex_minio
    source = _vortex.VortexSource([base + "/prefix/explicit.bin"], config, 3, enable_metadata_reuse=True)
    original = native._read_vortex_metadata_bulk_cached
    counts = []

    def record(*args, **kwargs):
        result = original(*args, **kwargs)
        counts.extend(rows for _, rows, _ in result[0])
        assert all(snapshot is not None for _, _, snapshot in result[0])
        return result

    monkeypatch.setattr(native, "_read_vortex_metadata_bulk_cached", record)

    async def materialize():
        return [task async for task in source.get_tasks(Pushdowns())]

    assert len(asyncio.run(materialize())) == 1
    replacement = table.slice(0, 7)
    path = tmp_path / "replacement.vortex"
    vortex.io.write(replacement, str(path))
    _client(config).put_object(Bucket=base.removeprefix("s3://"), Key="prefix/explicit.bin", Body=path.read_bytes())
    assert len(asyncio.run(materialize())) == 1
    assert counts == [len(table), len(replacement)]


def test_s3_replacement_after_validation_rejects_uncached_data(tmp_path, monkeypatch):
    """Force the HEAD/GET race through the real SDK and Vortex adapter."""
    import pyarrow as pa
    import vortex

    path = tmp_path / "large.vortex"
    table = pa.table(
        {
            "id": range(8192),
            "payload": [hashlib.sha512(str(index).encode()).hexdigest() for index in range(8192)],
        }
    )
    vortex.io.write(table, str(path))
    payload = path.read_bytes()
    assert len(payload) > 2 * 65535, "require data outside the opening buffer"
    state = {"armed": False, "changed": False}
    rejected = []

    class Handler(BaseHTTPRequestHandler):
        protocol_version = "HTTP/1.1"

        def log_message(self, *_args):
            pass

        def do_HEAD(self):
            self.send_response(200)
            self.send_header("Content-Length", str(len(payload)))
            self.send_header("ETag", '"stable"')
            # The fresh HEAD still identifies the planned object. Replace it
            # before its next uncached GET, without relying on a sleep.
            if state["armed"]:
                state["changed"] = True
            self.end_headers()

        def do_GET(self):
            condition = self.headers.get("If-Match")
            if state["changed"] and condition is not None:
                rejected.append(condition)
                self.send_response(412)
                self.send_header("Content-Length", "0")
                self.end_headers()
                return
            match = re.fullmatch(r"bytes=(\d+)-(\d+)", self.headers.get("Range", ""))
            assert match is not None
            start, end = map(int, match.groups())
            body = payload[start : end + 1]
            self.send_response(206)
            self.send_header("Content-Length", str(len(body)))
            self.send_header("Content-Range", f"bytes {start}-{end}/{len(payload)}")
            self.send_header("ETag", '"changed"' if state["changed"] else '"stable"')
            self.end_headers()
            self.wfile.write(body)

    server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    server.daemon_threads = True
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    original = native._read_vortex_metadata_bulk_cached

    def arm_after_planning(*args, **kwargs):
        result = original(*args, **kwargs)
        assert result[0][0][2] is not None
        state["armed"] = True
        return result

    monkeypatch.setattr(native, "_read_vortex_metadata_bulk_cached", arm_after_planning)
    config = daft.io.IOConfig(
        s3=daft.io.S3Config(
            endpoint_url=f"http://127.0.0.1:{server.server_port}",
            region_name="us-east-1",
            anonymous=True,
            use_ssl=False,
            force_virtual_addressing=False,
            num_tries=1,
        )
    )
    try:
        diagnostics.start()
        try:
            with pytest.raises(Exception, match="(?i)object changed"):
                daft.read_vortex(
                    "s3://metadata-race/large.vortex", io_config=config, enable_metadata_reuse=True
                ).select("payload").collect()
        finally:
            capture = diagnostics.finish()
        assert rejected and all(tag == '"stable"' for tag in rejected)
        events = capture["native"]["events"]
        assert any(event["phase"] == "execution_open" and event["operation"] == "cached_range" for event in events)
        assert any(event["phase"] == "data" and event["operation"] == "range" and event["error"] for event in events)
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=5)
        assert not thread.is_alive()
