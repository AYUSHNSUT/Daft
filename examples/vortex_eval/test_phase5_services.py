"""Metadata service-helper ownership and failure cleanup without live services."""

from __future__ import annotations

import json
import subprocess
from types import SimpleNamespace

import pytest

from examples.vortex_eval import run_phase5_services


@pytest.fixture
def service_lifecycle(monkeypatch, tmp_path):
    manifest_path = tmp_path / "manifest.json"
    manifest_path.write_text("{}")
    output = tmp_path / "results"
    secret = "phase5-local-test-secret-value"
    events = []
    bucket_names = []
    outcome = {"value": "success"}
    manifest = {"datasets": {"sorted/multi": {"paths": {"parquet": ["data.parquet"], "vortex": ["data.vortex"]}}}}
    monkeypatch.setattr(run_phase5_services, "load_manifest", lambda path: manifest)
    monkeypatch.setattr(run_phase5_services, "_require_environment", lambda allow_dev: None)
    monkeypatch.setattr(run_phase5_services, "_health_snapshot", lambda _: {"status": 200, "elapsed_seconds": 0.01})
    monkeypatch.delenv("VORTEX_MINIO_ENDPOINT", raising=False)

    def start(image, environment):
        assert image == "local-minio:phase5"
        events.append("start_container")
        environment.update(
            VORTEX_MINIO_ENDPOINT="http://127.0.0.1:9000",
            VORTEX_MINIO_ACCESS_KEY="phase5-developer",
            VORTEX_MINIO_SECRET_KEY=secret,
        )
        return "owned-phase5-container"

    def create_bucket(**kwargs):
        bucket_names.append(kwargs["Bucket"])
        events.append("create_bucket")
        if outcome["value"] == "create_failure":
            raise RuntimeError(f"Create failed with credential {secret}")

    def upload_file(local, bucket, relative):
        assert bucket == bucket_names[0]
        assert local == str(manifest_path.parent / relative)
        events.append(f"upload:{relative}")
        if outcome["value"] == "upload_failure":
            raise RuntimeError(f"Upload failed with credential {secret}")

    def delete_objects(**kwargs):
        assert kwargs["Bucket"] == bucket_names[0]
        assert kwargs["Delete"]["Objects"] == [{"Key": "data.parquet"}, {"Key": "data.vortex"}]
        events.append("delete_objects")

    def delete_bucket(**kwargs):
        assert kwargs["Bucket"] == bucket_names[0]
        events.append("delete_bucket")
        if outcome["value"] == "bucket_cleanup_failure":
            raise RuntimeError(f"Bucket deletion failed with credential {secret}")

    client = SimpleNamespace(
        create_bucket=create_bucket,
        upload_file=upload_file,
        get_paginator=lambda name: SimpleNamespace(
            paginate=lambda **kwargs: [{"Contents": [{"Key": "data.parquet"}, {"Key": "data.vortex"}]}, {}]
        ),
        delete_objects=delete_objects,
        delete_bucket=delete_bucket,
    )
    import boto3

    monkeypatch.setattr(boto3, "client", lambda *args, **kwargs: client)
    monkeypatch.setattr(run_phase5_services, "_start_minio", start)

    def run(command, **kwargs):
        if command[:2] in (["docker", "version"], ["docker", "inspect"], ["docker", "logs"]):
            assert kwargs["timeout"] == 10
            if command[1] == "logs":
                assert "--timestamps" in command
                assert command[command.index("--tail") + 1] == "2000"
            if command[1] == "inspect":
                assert ".Config.Env" not in command[command.index("--format") + 1]
                assert command[-1] == "owned-phase5-container"
            events.append(f"diagnostic:{command[1]}")
            if outcome["value"] == "diagnostic_failure":
                raise subprocess.TimeoutExpired(command, 10, stderr=secret)
            return subprocess.CompletedProcess(command, 0, stdout=f"diagnostic {secret}", stderr=f"warning {secret}")
        if command[:2] == ["docker", "rm"]:
            assert command[-1] == "owned-phase5-container"
            events.append("remove_container")
            return subprocess.CompletedProcess(
                command,
                1 if outcome["value"] == "container_cleanup_failure" else 0,
                stdout="",
                stderr=f"Failed removal {secret}",
            )
        assert command[:3] == [run_phase5_services.sys.executable, "-m", "benchmarking.vortex.metadata_eval"]
        assert command[command.index("--storage-root") + 1] == f"s3://{bucket_names[0]}"
        assert command[command.index("--orders") + 1] == "sorted"
        assert command[command.index("--layouts") + 1] == "multi"
        assert command[command.index("--concurrency") + 1 : command.index("--rounds")] == ["1", "8"]
        assert command[command.index("--modes") + 1 : command.index("--concurrency")] == ["query", "metadata"]
        assert "--allow-dev" in command
        config = json.loads(kwargs["env"]["DAFT_VORTEX_BENCH_IO_JSON"])
        assert config["s3"]["access_key"] == secret
        assert kwargs["env"]["DAFT_ANALYTICS_ENABLED"] == "0"
        assert kwargs["env"]["DAFT_RUNNER"] == "native"
        assert all(secret not in str(argument) for argument in command)
        events.append("evaluate")
        output.mkdir()
        if outcome["value"] != "missing_report":
            report = {"status": "failed" if outcome["value"] == "failed_report" else "passed"}
            report["experiment"] = "metadata_concurrency"
            (output / "report.json").write_text(json.dumps(report))
        return subprocess.CompletedProcess(command, 3 if outcome["value"] == "query_failure" else 0)

    monkeypatch.setattr(run_phase5_services.subprocess, "run", run)
    arguments = [
        "--manifest",
        str(manifest_path),
        "--output",
        str(output),
        "--docker-image",
        "local-minio:phase5",
        "--allow-dev",
        "--rounds",
        "1",
        "--warmups",
        "0",
    ]
    return SimpleNamespace(
        arguments=arguments,
        output=output,
        outcome=outcome,
        events=events,
        bucket_names=bucket_names,
        secret=secret,
    )


@pytest.mark.parametrize(
    "outcome",
    [
        "success",
        "query_failure",
        "upload_failure",
        "bucket_cleanup_failure",
        "container_cleanup_failure",
        "missing_report",
        "failed_report",
    ],
)
def test_metadata_evaluation_cleans_only_its_owned_bucket_and_container(service_lifecycle, outcome):
    state = service_lifecycle
    state.outcome["value"] = outcome
    if outcome in {"success", "query_failure"}:
        assert run_phase5_services.main(state.arguments) == (3 if outcome == "query_failure" else 0)
    else:
        with pytest.raises(RuntimeError) as failure:
            run_phase5_services.main(state.arguments)
        assert state.secret not in str(failure.value)
    if outcome == "upload_failure":
        assert "evaluate" not in state.events
    else:
        assert "upload:data.parquet" in state.events
        assert "upload:data.vortex" in state.events
    assert state.events[-3:] == ["delete_objects", "delete_bucket", "remove_container"]
    assert len(state.bucket_names) == 1 and state.bucket_names[0].startswith("vortex-phase5-")


def test_failed_bucket_creation_does_not_delete_a_bucket_it_did_not_create(service_lifecycle):
    state = service_lifecycle
    state.outcome["value"] = "create_failure"
    with pytest.raises(RuntimeError, match="Create failed") as failure:
        run_phase5_services.main(state.arguments)
    assert state.secret not in str(failure.value)
    assert state.events[:2] == ["start_container", "create_bucket"]
    assert state.events[-1] == "remove_container"
    assert "delete_bucket" not in state.events


def test_existing_endpoint_is_used_without_starting_or_removing_a_container(monkeypatch, service_lifecycle):
    state = service_lifecycle
    monkeypatch.setenv("VORTEX_MINIO_ENDPOINT", "http://127.0.0.1:9000")
    monkeypatch.setenv("VORTEX_MINIO_ACCESS_KEY", "phase5-developer")
    monkeypatch.setenv("VORTEX_MINIO_SECRET_KEY", state.secret)
    assert run_phase5_services.main(state.arguments) == 0
    assert "start_container" not in state.events
    assert "remove_container" not in state.events
    assert not any(event.startswith("diagnostic:") for event in state.events)
    assert state.events[-2:] == ["delete_objects", "delete_bucket"]


def test_nonempty_output_is_rejected_before_service_contact(service_lifecycle):
    state = service_lifecycle
    state.output.mkdir()
    (state.output / "keep.txt").write_text("existing result")
    with pytest.raises(FileExistsError, match="absent or empty"):
        run_phase5_services.main(state.arguments)
    assert state.events == []
    assert (state.output / "keep.txt").read_text() == "existing result"


@pytest.mark.parametrize("outcome", ["success", "query_failure", "diagnostic_failure"])
def test_diagnostics_are_redacted_outside_evaluation_and_before_cleanup(service_lifecycle, outcome):
    state = service_lifecycle
    state.outcome["value"] = outcome
    assert run_phase5_services.main(state.arguments) == (3 if outcome == "query_failure" else 0)
    text = (state.output / "service-diagnostics.json").read_text()
    assert state.secret not in text
    diagnostic = json.loads(text)
    assert diagnostic["evaluation_returncode"] == (3 if outcome == "query_failure" else 0)
    for key in ("before_evaluation", "before_cleanup"):
        snapshot = diagnostic[key]
        assert snapshot["owns_container"] is True
        assert snapshot["health"]["status"] == 200
        if outcome == "diagnostic_failure":
            assert "error" in snapshot["container_state"]
        else:
            assert "<redacted>" in text
    first = state.events.index("diagnostic:inspect")
    evaluate = state.events.index("evaluate")
    last = len(state.events) - 1 - state.events[::-1].index("diagnostic:inspect")
    cleanup = state.events.index("delete_objects")
    assert first < evaluate < last < cleanup


def test_health_failure_is_recorded_without_exposing_credentials(monkeypatch):
    def failure(_):
        raise RuntimeError("endpoint failed with private-credential")

    monkeypatch.setattr(run_phase5_services, "_health_snapshot", failure)
    result = run_phase5_services._service_snapshot(
        {"VORTEX_MINIO_ENDPOINT": "http://127.0.0.1:9000", "VORTEX_MINIO_SECRET_KEY": "private-credential"},
        None,
    )
    assert result["health"] == {"error": "endpoint failed with <redacted>"}
    assert result["owns_container"] is False


@pytest.mark.parametrize("options", [["--concurrency", "8"], ["--concurrency", "1", "65"], ["--rounds", "0"]])
def test_invalid_comparison_options_are_rejected_before_service_contact(service_lifecycle, options):
    state = service_lifecycle
    with pytest.raises(SystemExit) as failure:
        run_phase5_services.main([*state.arguments, *options])
    assert failure.value.code == 2
    assert state.events == []
