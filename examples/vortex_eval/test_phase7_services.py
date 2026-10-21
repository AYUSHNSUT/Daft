"""The profiling helper owns only its temporary bucket/container and preserves failures."""

from __future__ import annotations

import json
import subprocess
from types import SimpleNamespace

import boto3
import pytest

from examples.vortex_eval import run_phase7_services as services


@pytest.fixture
def lifecycle(monkeypatch, tmp_path):
    events, commands = [], []
    secret = "phase7-private-test-secret"
    state = SimpleNamespace(code=0, valid_report=True)
    manifest = tmp_path / "manifest.json"
    manifest.write_text("{}")
    output = tmp_path / "out"
    monkeypatch.delenv("VORTEX_MINIO_ENDPOINT", raising=False)
    monkeypatch.setattr(services, "_require_environment", lambda _: None)
    monkeypatch.setattr(
        services,
        "load_manifest",
        lambda _: {"datasets": {"sorted/multi": {"paths": {"parquet": ["a.parquet"], "vortex": ["a.vortex"]}}}},
    )

    def start(image, env):
        events.append("start")
        env.update(
            VORTEX_MINIO_ENDPOINT="http://127.0.0.1:9000",
            VORTEX_MINIO_ACCESS_KEY="test",
            VORTEX_MINIO_SECRET_KEY=secret,
        )
        return "owned-container"

    def run(command, **kwargs):
        commands.append(command)
        if command[:2] == ["docker", "rm"]:
            assert command[-1] == "owned-container"
            events.append("remove")
            return subprocess.CompletedProcess(command, 0, stdout="", stderr="")
        assert command[:3] == [services.sys.executable, "-m", "benchmarking.vortex.phase7"]
        assert "--diagnostic-rounds" in command
        events.append("evaluate")
        output.mkdir()
        (output / "report.json").write_text(
            json.dumps(
                {
                    "experiment": "phase7_paired_diagnosis" if state.valid_report else "wrong",
                    "status": "passed" if state.code == 0 else "failed",
                }
            )
        )
        return subprocess.CompletedProcess(command, state.code)

    monkeypatch.setattr(services, "_start_minio", start)
    monkeypatch.setattr(services, "_cleanup_bucket", lambda *_: events.append("delete"))
    monkeypatch.setattr(services, "_service_snapshot", lambda *_: {"test_snapshot": True})
    monkeypatch.setattr(services.subprocess, "run", run)
    monkeypatch.setattr(
        boto3,
        "client",
        lambda *_, **kw: SimpleNamespace(
            create_bucket=lambda **_: events.append("create"), upload_file=lambda *_: events.append("upload")
        ),
    )
    return SimpleNamespace(
        args=["--manifest", str(manifest), "--output", str(output), "--docker-image", "local"],
        events=events,
        commands=commands,
        output=output,
        state=state,
    )


@pytest.mark.parametrize("code", [0, 1])
def test_cleanup_and_exit_status(lifecycle, code):
    lifecycle.state.code = code
    assert services.main(lifecycle.args) == code
    assert lifecycle.events[-2:] == ["delete", "remove"]
    snapshot = json.loads((lifecycle.output / "service-diagnostics.json").read_text())
    assert snapshot["evaluation_returncode"] == code


def test_report_mismatch_still_cleans_up(lifecycle):
    lifecycle.state.valid_report = False
    with pytest.raises(RuntimeError, match="report did not pass"):
        services.main(lifecycle.args)
    assert lifecycle.events[-2:] == ["delete", "remove"]


@pytest.mark.parametrize("extra", [["--rounds", "19"], ["--diagnostic-rounds", "0"], ["--datasets", "missing"]])
def test_invalid_matrix_does_not_contact_services(lifecycle, extra):
    with pytest.raises(SystemExit):
        services.main([*lifecycle.args, *extra])
    assert lifecycle.events == []


def test_old_results_are_preserved(lifecycle):
    lifecycle.output.mkdir()
    saved = lifecycle.output / "failed.json"
    saved.write_text("preserve")
    with pytest.raises(FileExistsError):
        services.main(lifecycle.args)
    assert lifecycle.events == [] and saved.read_text() == "preserve"
