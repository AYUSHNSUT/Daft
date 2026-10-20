"""Service helper ownership and failure cleanup without sockets or Docker."""

from __future__ import annotations

import json
import subprocess
from types import SimpleNamespace

import pytest

from examples.vortex_eval import run_phase4_services


@pytest.mark.parametrize("outcome", ["success", "query_failure", "upload_failure", "cleanup_failure"])
def test_remote_benchmark_cleans_its_bucket_and_container(monkeypatch, tmp_path, outcome):
    manifest_path = tmp_path / "manifest.json"
    manifest_path.write_text("{}")
    output = tmp_path / "results"
    secret = "local-test-secret-value"
    events = []
    bucket_names = []
    manifest = {"datasets": {"sorted/multi": {"paths": {"parquet": ["data.parquet"], "vortex": ["data.vortex"]}}}}
    monkeypatch.setattr(run_phase4_services, "load_manifest", lambda path: manifest)
    monkeypatch.delenv("VORTEX_MINIO_ENDPOINT", raising=False)

    def start(image, environment):
        assert image == "local-minio:phase4"
        environment.update(
            VORTEX_MINIO_ENDPOINT="http://127.0.0.1:9000",
            VORTEX_MINIO_ACCESS_KEY="developer",
            VORTEX_MINIO_SECRET_KEY=secret,
        )
        return "owned-container"

    def create_bucket(**kwargs):
        bucket_names.append(kwargs["Bucket"])
        events.append("create")

    def upload_file(*args):
        events.append("upload")
        if outcome == "upload_failure":
            raise RuntimeError(f"Upload failed with credential {secret}")

    def delete_objects(**kwargs):
        assert kwargs["Bucket"] == bucket_names[0]
        assert kwargs["Delete"]["Objects"] == [{"Key": "data.parquet"}]
        events.append("delete_objects")

    def delete_bucket(**kwargs):
        assert kwargs["Bucket"] == bucket_names[0]
        events.append("delete_bucket")

    client = SimpleNamespace(
        create_bucket=create_bucket,
        upload_file=upload_file,
        get_paginator=lambda name: SimpleNamespace(
            paginate=lambda **kwargs: [{"Contents": [{"Key": "data.parquet"}]}, {}]
        ),
        delete_objects=delete_objects,
        delete_bucket=delete_bucket,
    )
    import boto3

    monkeypatch.setattr(boto3, "client", lambda *args, **kwargs: client)
    monkeypatch.setattr(run_phase4_services, "_start_minio", start)

    def run(command, **kwargs):
        if command[:2] == ["docker", "rm"]:
            assert command[-1] == "owned-container"
            events.append("remove_container")
            return subprocess.CompletedProcess(
                command, 1 if outcome == "cleanup_failure" else 0, stdout="", stderr=f"Failed removal {secret}"
            )
        assert command[:4] == [run_phase4_services.sys.executable, "-m", "benchmarking.vortex", "run"]
        assert command[command.index("--storage-root") + 1] == f"s3://{bucket_names[0]}"
        config = json.loads(kwargs["env"]["DAFT_VORTEX_BENCH_IO_JSON"])
        assert config["s3"]["access_key"] == secret
        assert kwargs["env"]["DAFT_ANALYTICS_ENABLED"] == "0"
        assert all(secret not in str(argument) for argument in command)
        events.append("query")
        output.mkdir()
        (output / "report.json").write_text("{}")
        return subprocess.CompletedProcess(command, 3 if outcome == "query_failure" else 0)

    monkeypatch.setattr(run_phase4_services.subprocess, "run", run)
    arguments = [
        "--manifest",
        str(manifest_path),
        "--output",
        str(output),
        "--docker-image",
        "local-minio:phase4",
        "--allow-dev",
        "--rounds",
        "1",
        "--warmups",
        "0",
    ]
    if outcome in {"upload_failure", "cleanup_failure"}:
        with pytest.raises(RuntimeError, match="Upload failed|container cleanup failed") as failure:
            run_phase4_services.main(arguments)
        assert secret not in str(failure.value)
        if outcome == "upload_failure":
            assert "query" not in events
    else:
        assert run_phase4_services.main(arguments) == (3 if outcome == "query_failure" else 0)
    assert events[-3:] == ["delete_objects", "delete_bucket", "remove_container"]
    assert len(bucket_names) == 1 and bucket_names[0].startswith("vortex-phase4-")


@pytest.mark.parametrize(
    "endpoint",
    [
        "http://user:secret@localhost:9000",
        "http://localhost:9000?token=secret",
        "http://localhost:9000/#secret",
        "s3://bucket",
    ],
)
def test_service_endpoint_rejects_embedded_credentials_without_echo(endpoint):
    with pytest.raises(ValueError) as failure:
        run_phase4_services._validate_endpoint(endpoint)
    assert "secret" not in str(failure.value)
