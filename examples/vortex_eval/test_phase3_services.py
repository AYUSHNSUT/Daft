"""Socket-independent diagnostics for failed service startup."""

from __future__ import annotations

import subprocess

import pytest

from examples.vortex_eval import run_phase3_services


@pytest.mark.parametrize(
    "exit_code,detail",
    [(125, "pull access denied for daft-vortex-minio"), (126, "permission denied connecting to Docker socket")],
)
def test_minio_startup_preserves_docker_diagnostics_without_credentials(monkeypatch, exit_code, detail):
    credentials = []

    def failed_run(command, **kwargs):
        assert command[:2] == ["docker", "run"]
        environment = kwargs["env"]
        credentials.extend([environment["MINIO_ROOT_USER"], environment["MINIO_ROOT_PASSWORD"]])
        return subprocess.CompletedProcess(command, exit_code, stdout="", stderr=detail + " " + " ".join(credentials))

    monkeypatch.setattr(run_phase3_services.subprocess, "run", failed_run)
    with pytest.raises(RuntimeError) as failure:
        run_phase3_services._start_minio("daft-vortex-minio:phase3", {})
    message = str(failure.value)
    assert "daft-vortex-minio:phase3" in message
    assert f"docker run exited {exit_code}" in message
    assert detail in message
    assert all(credential not in message for credential in credentials)
