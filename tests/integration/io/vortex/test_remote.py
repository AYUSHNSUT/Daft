"""Explicit service acceptance: HTTP Range, authenticated MinIO, and Ray.

Run through run_phase3_services.py so absent services or skipped prerequisites
cannot be mistaken for passing acceptance. Test data and credentials are local
development fixtures; no public bucket is needed.
"""

from __future__ import annotations

import io
import json
import os
import subprocess
import sys

import pytest

import daft
from daft.io import _vortex
from daft.pickle import cloudpickle

pytestmark = pytest.mark.integration


@pytest.mark.parametrize("projection,filtering", [(False, False), (True, True)])
def test_vortex_http_range_matches_independent_rows(vortex_http, projection: bool, filtering: bool) -> None:
    url, config, requests, table = vortex_http
    frame = daft.read_vortex(
        url,
        io_config=config,
        batch_size=3,
        enable_projection_pushdown=projection,
        enable_filter_pushdown=filtering,
        max_concurrent_requests=2,
    )
    planning_requests = len(requests)
    expected = [row["id"] for row in table.to_pylist() if row["value"] >= 4]
    assert frame.where(daft.col("value") >= 4).select("id").to_pydict() == {"id": expected}
    assert len(requests) > planning_requests
    assert all(request["authorized"] for request in requests)
    get_requests = [request for request in requests if request["method"] == "GET"]
    assert get_requests
    assert all(request["range"] is not None and request["status"] == 206 for request in get_requests)


def test_vortex_http_source_pickle_and_query_url_preserve_config(vortex_http) -> None:
    url, config, requests, table = vortex_http
    source = _vortex.VortexSource([url + "?fixture=1"], config, 3, max_concurrent_requests=2)
    restored = cloudpickle.loads(cloudpickle.dumps(source))
    assert restored._io_config.__getstate__() == config.__getstate__()
    assert restored.read().select("id").to_pydict() == {"id": table["id"].to_pylist()}
    assert all(request["authorized"] for request in requests)
    assert any("?fixture=1" in request["path"] for request in requests)


def test_vortex_signed_http_explain_redacts_query(vortex_http) -> None:
    url, config, requests, _ = vortex_http
    signed = url + "?token=sentinel-explain-secret"
    frame = daft.read_vortex(signed, io_config=config).select("id")
    output = io.StringIO()
    frame.explain(show_all=True, file=output)
    assert "sentinel-explain-secret" not in output.getvalue()
    assert frame.count_rows() == 31
    assert any("token=sentinel-explain-secret" in request["path"] for request in requests)


def test_vortex_http_rejects_wrong_credentials_and_corrupt_input(vortex_http) -> None:
    url, config, _, _ = vortex_http
    wrong = daft.io.IOConfig(http=daft.io.HTTPConfig(bearer_token="wrong-local-test-token", num_tries=1))
    with pytest.raises(Exception, match="(?i)(401|unauthorized|forbidden)"):
        daft.read_vortex(url, io_config=wrong).collect()
    with pytest.raises(Exception, match="corrupt.vortex"):
        daft.read_vortex(url.replace("data.vortex", "corrupt.vortex"), io_config=config).collect()


def test_vortex_http_missing_signed_url_redacts_query(vortex_http) -> None:
    url, config, requests, _ = vortex_http
    missing = url.replace("data.vortex", "missing.vortex") + "?token=sentinel-query-secret"
    with pytest.raises(Exception) as failure:
        daft.read_vortex(missing, io_config=config).collect()
    assert "sentinel-query-secret" not in str(failure.value)
    # Discovery must send the original query, even though errors redact it.
    assert any(request["path"].endswith("?token=sentinel-query-secret") for request in requests)


def test_vortex_http_resumes_truncated_range_body(vortex_http) -> None:
    url, config, requests, table = vortex_http
    url = url.replace("data.vortex", "truncate.vortex")
    assert daft.read_vortex(url, io_config=config).select("id").to_pydict() == {"id": table["id"].to_pylist()}
    truncated = [request for request in requests if request["truncated"]]
    assert len(truncated) == 1
    first = truncated[0]
    resumed_range = f"bytes={first['resume_at']}-{first['end']}"
    assert any(request["range"] == resumed_range and not request["truncated"] for request in requests)


@pytest.mark.parametrize("filename,status", [("ignored-range.vortex", 200), ("wrong-range.vortex", 206)])
def test_vortex_http_rejects_invalid_range_responses(vortex_http, filename: str, status: int) -> None:
    url, config, requests, _ = vortex_http
    url = url.replace("data.vortex", filename)
    with pytest.raises(Exception):
        daft.read_vortex(url, io_config=config).collect()
    ranged = [request for request in requests if request["method"] == "GET" and request["range"]]
    assert ranged
    assert all(request["status"] == status for request in ranged)


def test_vortex_minio_exact_directory_glob_and_pickle(vortex_minio) -> None:
    base, config, table = vortex_minio
    expected = {"id": table["id"].to_pylist()}
    assert daft.read_vortex(base + "/prefix/explicit.bin", io_config=config).select("id").to_pydict() == expected
    for path in [base + "/prefix", base + "/prefix/**/*.vortex"]:
        assert daft.read_vortex(path, io_config=config).select("id").to_pydict() == expected
    source = _vortex.VortexSource(
        [base + "/prefix/explicit.bin", base + "/prefix", base + "/prefix/**/*.vortex"], config, 3
    )
    restored = cloudpickle.loads(cloudpickle.dumps(source))
    assert restored._io_config.__getstate__() == config.__getstate__()
    assert restored.read().select("id").sort("id").to_pydict() == {"id": sorted(expected["id"] * 2)}


def test_vortex_minio_credentials_are_required(vortex_minio) -> None:
    base, config, _ = vortex_minio
    wrong = daft.io.IOConfig(s3=config.s3.replace(access_key="wrong-local-test-secret", num_tries=1))
    with pytest.raises(Exception, match="(?i)(403|forbidden|signature|access.denied|credential)"):
        daft.read_vortex(base + "/prefix/explicit.bin", io_config=wrong).collect()


_RAY_QUERY = r"""
import json
import os
import sys
import ray
import daft

settings = json.load(sys.stdin)
ray.init(num_cpus=2, include_dashboard=False, object_store_memory=100 * 1024 * 1024)
try:
    daft.set_runner_ray(noop_if_initialized=True)
    backend = settings.get("backend")
    if backend == "http":
        config = daft.io.IOConfig(http=daft.io.HTTPConfig(**settings["config"]))
    elif backend == "s3":
        config = daft.io.IOConfig(s3=daft.io.S3Config(**settings["config"]))
    else:
        config = daft.io.IOConfig()

    @daft.func(return_dtype=daft.DataType.int64())
    def worker_pid(value: int) -> int:
        return os.getpid()

    frame = daft.read_vortex(settings["path"], io_config=config, batch_size=3, max_concurrent_requests=2)
    result = frame.where(daft.col("value") >= 4).select("id", worker_pid(daft.col("id")).alias("worker_pid")).sort("id").to_pydict()
    assert result["id"] == settings["expected"], result
    assert result["worker_pid"] and all(pid != os.getpid() for pid in result["worker_pid"]), result
    print("VORTEX_RAY_ACCEPTANCE_OK")
finally:
    ray.shutdown()
"""


def _ray_query(path, table, backend=None, config=None):
    pytest.importorskip("ray", reason="Ray executes the worker acceptance query")
    settings = {
        "path": str(path),
        "backend": backend,
        "config": config,
        "expected": sorted(row["id"] for row in table.to_pylist() if row["value"] >= 4),
    }
    environment = {**os.environ, "DAFT_RUNNER": "ray", "DAFT_PROGRESS_BAR": "0"}
    # Use stdin rather than argv for configured credentials. They are never
    # printed; any failure still includes the child process's diagnostics.
    result = subprocess.run(
        [sys.executable, "-c", _RAY_QUERY],
        input=json.dumps(settings),
        text=True,
        capture_output=True,
        env=environment,
        timeout=180,
    )
    assert result.returncode == 0, result.stdout + result.stderr
    assert "VORTEX_RAY_ACCEPTANCE_OK" in result.stdout


def test_vortex_ray_local_matches_native(vortex_service_file) -> None:
    path, table = vortex_service_file
    expected = sorted(row["id"] for row in table.to_pylist() if row["value"] >= 4)
    assert daft.read_vortex(path).where(daft.col("value") >= 4).select("id").sort("id").to_pydict() == {"id": expected}
    _ray_query(path, table)


def test_vortex_ray_http_reopens_with_explicit_io_config(vortex_http) -> None:
    url, config, _, table = vortex_http
    _ray_query(url, table, "http", {"bearer_token": config.http.bearer_token, "num_tries": 1})


def test_vortex_ray_minio_reopens_with_explicit_io_config(vortex_minio) -> None:
    base, config, table = vortex_minio
    s3 = config.s3
    _ray_query(
        base + "/prefix/explicit.bin",
        table,
        "s3",
        {
            "endpoint_url": s3.endpoint_url,
            "key_id": s3.key_id,
            "access_key": s3.access_key,
            "region_name": s3.region_name,
            "use_ssl": s3.use_ssl,
            "force_virtual_addressing": False,
            "num_tries": 1,
        },
    )
