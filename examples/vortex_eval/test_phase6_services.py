"""Terminal orchestration gates and temporary-service ownership without services."""

from __future__ import annotations

import io
import json
import subprocess
from pathlib import Path
from types import SimpleNamespace

import pytest

from examples.vortex_eval import run_phase6_acceptance as acceptance
from examples.vortex_eval import run_phase6_services as services


@pytest.fixture
def lifecycle(monkeypatch, tmp_path):
    path = tmp_path / "manifest.json"
    path.write_text("{}")
    events, commands = [], []
    outcome = {"value": "passed"}
    secret = "private-phase6-developer-secret"
    monkeypatch.delenv("VORTEX_MINIO_ENDPOINT", raising=False)
    monkeypatch.setattr(
        services,
        "load_manifest",
        lambda _: {"datasets": {"sorted/multi": {"paths": {"parquet": ["a.parquet"], "vortex": ["a.vortex"]}}}},
    )
    monkeypatch.setattr(services, "_require_environment", lambda _: None)

    def start(image, env):
        events.append("start")
        env.update(
            VORTEX_MINIO_ENDPOINT="http://127.0.0.1:9000",
            VORTEX_MINIO_ACCESS_KEY="developer",
            VORTEX_MINIO_SECRET_KEY=secret,
        )
        return "owned-phase6-container"

    def create(**kwargs):
        assert kwargs["Bucket"].startswith("vortex-phase6-")
        events.append("create")
        if outcome["value"] == "create_failure":
            raise RuntimeError(secret)

    def upload(*args):
        events.append("upload")
        if outcome["value"] == "upload_failure":
            raise RuntimeError(secret)

    def snapshot(env, container):
        events.append("snapshot")
        return {"owns_container": container is not None}

    def run(command, **kwargs):
        commands.append(command)
        if command[:2] == ["docker", "rm"]:
            assert command[-1] == "owned-phase6-container"
            events.append("remove")
            return subprocess.CompletedProcess(command, 0, stdout="", stderr="")
        assert command[:3] == [services.sys.executable, "-m", "benchmarking.vortex.remote_eval"]
        events.append("evaluate")
        output = Path(command[command.index("--output") + 1])
        output.mkdir()
        result = {
            "status": "failed" if outcome["value"] == "gate_failure" else "passed",
            "experiment": "remote_read_policy",
            "performance_gate": {"status": "passed"},
            "settings": {"execution_mode": command[command.index("--execution-mode") + 1]},
        }
        if outcome["value"] == "mode_mismatch":
            result["settings"]["execution_mode"] = "fresh-process"
        (output / "report.json").write_text(json.dumps(result))
        return subprocess.CompletedProcess(command, 1 if outcome["value"] == "gate_failure" else 0)

    import boto3

    monkeypatch.setattr(
        boto3, "client", lambda *args, **kwargs: SimpleNamespace(create_bucket=create, upload_file=upload)
    )
    monkeypatch.setattr(services, "_cleanup_bucket", lambda *_: events.append("delete"))
    monkeypatch.setattr(services, "_start_minio", start)
    monkeypatch.setattr(services, "_service_snapshot", snapshot)
    monkeypatch.setattr(services.subprocess, "run", run)
    args = [
        "--manifest",
        str(path),
        "--output",
        str(tmp_path / "out"),
        "--docker-image",
        "local-minio",
        "--stage",
        "final",
        "--diagnostics",
    ]
    return SimpleNamespace(
        args=args, events=events, commands=commands, outcome=outcome, secret=secret, output=tmp_path / "out"
    )


@pytest.mark.parametrize("outcome", ["passed", "gate_failure", "upload_failure", "create_failure"])
def test_service_ownership_cleanup_and_failed_gate_are_preserved(lifecycle, outcome):
    s = lifecycle
    s.outcome["value"] = outcome
    if outcome.endswith("_failure") and outcome != "gate_failure":
        with pytest.raises(RuntimeError) as failure:
            services.main(s.args)
        assert s.secret not in str(failure.value)
    else:
        assert services.main(s.args) == (1 if outcome == "gate_failure" else 0)
    assert s.events[-1] == "remove"
    assert ("delete" in s.events) is (outcome != "create_failure")
    assert ("evaluate" in s.events) is (outcome in {"passed", "gate_failure"})
    if "evaluate" in s.events:
        assert s.events.index("snapshot") < s.events.index("evaluate")
        command = next(command for command in s.commands if "benchmarking.vortex.remote_eval" in command)
        assert command[command.index("--rounds") + 1] == "50"
        assert command[command.index("--warmups") + 1] == "3"
        assert "--diagnostics" in command
    assert (s.output / "service-diagnostics.json").is_file()


def test_external_endpoint_does_not_transfer_container_ownership(lifecycle, monkeypatch):
    monkeypatch.setenv("VORTEX_MINIO_ENDPOINT", "http://127.0.0.1:9000")
    monkeypatch.setenv("VORTEX_MINIO_ACCESS_KEY", "developer")
    monkeypatch.setenv("VORTEX_MINIO_SECRET_KEY", lifecycle.secret)
    assert services.main(lifecycle.args) == 0
    assert "start" not in lifecycle.events and "remove" not in lifecycle.events
    assert "delete" in lifecycle.events


@pytest.mark.parametrize("execution_mode", ["fresh-process", "persistent-process"])
def test_service_forwards_execution_mode(lifecycle, execution_mode):
    assert services.main([*lifecycle.args, "--execution-mode", execution_mode]) == 0
    command = next(command for command in lifecycle.commands if "benchmarking.vortex.remote_eval" in command)
    assert command[command.index("--execution-mode") + 1] == execution_mode


def test_service_rejects_selection_from_another_execution_mode_before_contact(lifecycle, tmp_path, monkeypatch):
    selection = tmp_path / "selected-policy.json"
    selection.write_text(json.dumps({"execution_mode": "fresh-process"}))

    def load(path, allow_dev=False, *, execution_mode=None):
        assert execution_mode == "persistent-process"
        if json.loads(path.read_text())["execution_mode"] != execution_mode:
            raise ValueError("Frozen selection execution mode differs")

    monkeypatch.setattr(services, "load_selection", load)
    with pytest.raises(ValueError, match="execution mode"):
        services.main([*lifecycle.args, "--selection", str(selection), "--execution-mode", "persistent-process"])
    assert lifecycle.events == []
    assert not lifecycle.output.exists()


def test_service_rejects_report_from_another_execution_mode_and_cleans_up(lifecycle):
    lifecycle.outcome["value"] = "mode_mismatch"
    with pytest.raises(RuntimeError, match="execution mode"):
        services.main([*lifecycle.args, "--execution-mode", "persistent-process"])
    assert lifecycle.events[-2:] == ["delete", "remove"]


@pytest.mark.parametrize("extra", [["--rounds", "49"], ["--warmups", "1"], ["--cases", "join"]])
def test_invalid_final_matrix_never_contacts_service(lifecycle, extra):
    with pytest.raises(SystemExit) as failure:
        services.main([*lifecycle.args, *extra])
    assert failure.value.code == 2 and lifecycle.events == []


def test_nonempty_output_is_never_overwritten(lifecycle):
    lifecycle.output.mkdir()
    (lifecycle.output / "old.txt").write_text("preserve failed run")
    with pytest.raises(FileExistsError):
        services.main(lifecycle.args)
    assert lifecycle.events == []
    assert (lifecycle.output / "old.txt").read_text() == "preserve failed run"


@pytest.mark.parametrize(
    "skip,zero_skips,expected", [(False, True, "passed"), (True, True, "failed"), (True, False, "passed")]
)
def test_acceptance_zero_skip_gate_uses_actual_junit(tmp_path, monkeypatch, skip, zero_skips, expected):
    xml = tmp_path / "suite.xml"
    xml.write_text(
        '<testsuites><testsuite><testcase name="one">'
        + ('<skipped message="missing service"/>' if skip else "")
        + "</testcase></testsuite></testsuites>"
    )

    class Process:
        stdout = io.StringIO("private-secret\n")

        def __enter__(self):
            return self

        def __exit__(self, *_):
            pass

        def wait(self):
            return 0

    monkeypatch.setattr(acceptance.subprocess, "Popen", lambda *args, **kwargs: Process())
    result = acceptance.run_gate(
        "service", ["pytest"], tmp_path, {"VORTEX_MINIO_SECRET_KEY": "private-secret"}, junit=xml, zero_skips=zero_skips
    )
    assert result["status"] == expected
    assert result["counts"]["tests"] == 1 and result["counts"]["skipped"] == int(skip)
    assert "private-secret" not in (tmp_path / "service.log").read_text()


@pytest.mark.parametrize("stage", ["tune", "final", "guard"])
@pytest.mark.parametrize("execution_mode", ["fresh-process", "persistent-process"])
def test_acceptance_rejects_mismatched_report_mode(tmp_path, monkeypatch, stage, execution_mode):
    def gate(name, command, output, env, **kwargs):
        assert command[command.index("--execution-mode") + 1] == execution_mode
        directory = output / {"tuning": "tuning", "remote": "remote-report", "local-guard": "local-guard"}[name]
        directory.mkdir()
        (directory / "report.json").write_text(
            json.dumps(
                {
                    "status": "passed",
                    "correctness_status": "passed",
                    "diagnostics_status": "passed",
                    "performance_gate": {"status": "passed"},
                    "settings": {
                        "execution_mode": "persistent-process" if execution_mode == "fresh-process" else "fresh-process"
                    },
                }
            )
        )
        return {"status": "passed", "returncode": 0}

    monkeypatch.setattr(acceptance, "run_gate", gate)
    result = acceptance.run_evaluation_gate(
        stage, tmp_path / "manifest.json", tmp_path, {}, "off", execution_mode=execution_mode
    )
    assert result["status"] == "failed"
    assert "execution mode" in result["validation_error"]


def test_acceptance_rejects_selection_from_another_mode_before_service_contact(tmp_path, monkeypatch):
    manifest = tmp_path / "manifest.json"
    manifest.write_text("{}")
    selection = tmp_path / "selection.json"
    selection.write_text(json.dumps({"execution_mode": "fresh-process"}))
    output = tmp_path / "acceptance"
    provenance = {
        "git_head": "head",
        "tracked_changes": False,
        "build_type": "release",
        "build_command": "build",
        "native_binaries_sha256": {"native.so": "sha"},
    }
    events = []
    monkeypatch.setattr(acceptance, "load_manifest", lambda _: {})
    monkeypatch.setattr(acceptance, "require_api", lambda: None)
    monkeypatch.setattr(acceptance, "_require_environment", lambda: None)
    monkeypatch.setattr(acceptance, "benchmark_environment", lambda: provenance)
    monkeypatch.setattr(acceptance, "_start_minio", lambda *_: events.append("start"))

    def load(path, *, execution_mode=None):
        assert execution_mode == "persistent-process"
        if json.loads(path.read_text())["execution_mode"] != execution_mode:
            raise ValueError("Frozen selection execution mode differs")

    monkeypatch.setattr(acceptance, "load_selection", load)
    with pytest.raises(ValueError, match="execution mode"):
        acceptance.main(
            [
                "--manifest",
                str(manifest),
                "--output",
                str(output),
                "--selection",
                str(selection),
                "--execution-mode",
                "persistent-process",
            ]
        )
    assert events == [] and not output.exists()


@pytest.mark.parametrize("report_state", ["performance_miss", "truncated", "missing", "wrong_type"])
def test_acceptance_keeps_all_checks_after_performance_miss_and_runs_timing_first(tmp_path, monkeypatch, report_state):
    path = tmp_path / "manifest.json"
    path.write_text("{}")
    provenance = {
        "git_head": "head",
        "tracked_changes": False,
        "build_type": "release",
        "build_command": "build",
        "native_binaries_sha256": {"native.so": "sha"},
        "cargo_lock_sha256": "lock",
        "harness_sha256": {},
    }
    monkeypatch.setattr(acceptance, "load_manifest", lambda _: {})
    monkeypatch.setattr(acceptance, "require_api", lambda: None)
    monkeypatch.setattr(acceptance, "_require_environment", lambda: None)
    monkeypatch.setattr(acceptance, "benchmark_environment", lambda: provenance)
    monkeypatch.setenv("VORTEX_MINIO_ENDPOINT", "http://127.0.0.1:9000")
    monkeypatch.setattr(acceptance, "_service_snapshot", lambda *_: {})
    calls = []

    def gate(name, command, output, env, **kwargs):
        calls.append((name, command, env, kwargs))
        if name == "remote":
            report = output / "remote-report" / "report.json"
            report.parent.mkdir()
            if report_state == "missing":
                return {"status": "failed", "returncode": 1}
            report.write_text(
                json.dumps(
                    {
                        "status": "failed",
                        "correctness_status": "passed",
                        "performance_gate": {"status": "failed"},
                        "diagnostics_status": "passed",
                        "settings": {"execution_mode": "fresh-process"},
                    }
                )
            )
            if report_state == "truncated":
                report.write_text('{"status": "running",')
            elif report_state == "wrong_type":
                report.write_text("[]")
        return {"status": "failed" if name == "remote" else "passed", "returncode": 1 if name == "remote" else 0}

    monkeypatch.setattr(acceptance, "run_gate", gate)
    output = tmp_path / "acceptance"
    assert (
        acceptance.main(
            [
                "--manifest",
                str(path),
                "--output",
                str(output),
                "--expected-source",
                "head",
                "--expected-native-sha",
                "sha",
            ]
        )
        == 1
    )
    assert [row[0] for row in calls] == ["remote", "services", "orc-mcap", "focused", "s3-pooling", "workspace-rust"]
    assert calls[1][3]["zero_skips"] is True
    assert calls[-1][1][-2:] == ["--workspace", "--no-fail-fast"]
    assert calls[-1][2]["CARGO_BUILD_JOBS"] == "1"
    assert "RUST_DAFT_PKG_BUILD_TYPE" not in calls[-1][2]
    result = json.loads((output / "acceptance.json").read_text())
    assert result["status"] == "failed" and len(result["gates"]) == 6
    if report_state != "performance_miss":
        assert "validation_error" in result["gates"]["remote"]


@pytest.mark.parametrize(
    "scenario",
    [
        "passed",
        "tuning_failed",
        "missing_selection",
        "malformed_selection",
        "source_changed",
        "native_changed",
        "final_failed",
        "guard_failed",
        "guard_malformed",
    ],
)
@pytest.mark.parametrize("execution_mode", ["fresh-process", "persistent-process"])
def test_acceptance_tunes_then_freezes_one_policy_or_blocks_final_and_continues(
    tmp_path, monkeypatch, scenario, execution_mode
):
    manifest = tmp_path / "manifest.json"
    manifest.write_text("{}")
    local_manifest = tmp_path / "local-manifest.json"
    local_manifest.write_text('{"local": true}')
    output = tmp_path / "acceptance"
    provenance = {
        "git_head": "head",
        "tracked_changes": False,
        "build_type": "release",
        "build_command": "build",
        "native_binaries_sha256": {"native.so": "sha"},
        "cargo_lock_sha256": "lock",
        "harness_sha256": {},
    }
    selected = {
        "policy": "16k_1m",
        "source_revision": "head",
        "native_binaries_sha256": {"native.so": "sha"},
        "manifest_sha256": acceptance.file_hash(manifest),
        "tuning_report_sha256": "tuning-report-hash",
        "execution_mode": execution_mode,
    }
    commands, service_events, environment_calls = [], [], []
    monkeypatch.delenv("VORTEX_MINIO_ENDPOINT", raising=False)
    monkeypatch.setattr(
        acceptance,
        "load_manifest",
        lambda _: {
            "datasets": {f"{order}/{layout}": {} for order in ("sorted", "shuffled") for layout in ("single", "multi")}
        },
    )
    monkeypatch.setattr(acceptance, "require_api", lambda: None)
    monkeypatch.setattr(acceptance, "_require_environment", lambda: None)

    # Full selection validation is independently tested against the seeded grid.
    # Here the disk read preserves missing/malformed selection failure modes.
    def load(path, *, execution_mode=None):
        value = json.loads(path.read_text())
        assert value["execution_mode"] == execution_mode
        return value

    monkeypatch.setattr(acceptance, "load_selection", load)

    def environment():
        current = dict(provenance)
        if environment_calls:
            if scenario == "source_changed":
                current["git_head"] = "different-source"
            elif scenario == "native_changed":
                current["native_binaries_sha256"] = {"native.so": "different-binary"}
        environment_calls.append(current)
        return current

    def start(image, env):
        service_events.append(("start", image))
        env.update(VORTEX_MINIO_ENDPOINT="http://127.0.0.1:9000")
        return "owned-acceptance-container"

    def cleanup(command, **kwargs):
        service_events.append(("cleanup", command))
        assert command == ["docker", "rm", "--force", "owned-acceptance-container"]
        return subprocess.CompletedProcess(command, 0, stdout="", stderr="")

    def gate(name, command, destination, env, **kwargs):
        commands.append((name, command))
        assert env["VORTEX_MINIO_ENDPOINT"] == "http://127.0.0.1:9000"
        failed = (name == "tuning" and scenario == "tuning_failed") or (name == "remote" and scenario == "final_failed")
        guard_gate_failed = name == "local-guard" and scenario == "guard_failed"
        if name in {"tuning", "remote", "local-guard"}:
            directory = destination / ("remote-report" if name == "remote" else name)
            directory.mkdir()
            (directory / "report.json").write_text(
                json.dumps(
                    {
                        "status": "failed" if failed else "passed",
                        "correctness_status": "passed",
                        "diagnostics_status": "passed",
                        "performance_gate": {"status": "failed" if failed or guard_gate_failed else "passed"},
                        "settings": {"execution_mode": execution_mode},
                    }
                )
            )
            if name == "tuning" and scenario != "missing_selection":
                (directory / "selected-policy.json").write_text(
                    "{" if scenario == "malformed_selection" else json.dumps(selected)
                )
            if name == "local-guard" and scenario == "guard_malformed":
                (directory / "report.json").write_text("{")
        return {"status": "failed" if failed else "passed", "returncode": int(failed)}

    monkeypatch.setattr(acceptance, "benchmark_environment", environment)
    monkeypatch.setattr(acceptance, "_start_minio", start)
    monkeypatch.setattr(acceptance, "_service_snapshot", lambda *_: {})
    monkeypatch.setattr(acceptance.subprocess, "run", cleanup)
    monkeypatch.setattr(acceptance, "run_gate", gate)
    assert acceptance.main(
        [
            "--manifest",
            str(manifest),
            "--local-manifest",
            str(local_manifest),
            "--output",
            str(output),
            "--tune",
            "--execution-mode",
            execution_mode,
            "--expected-source",
            "head",
            "--expected-native-sha",
            "sha",
        ]
    ) == (0 if scenario == "passed" else 1)
    final_executed = scenario in {"passed", "final_failed", "guard_failed", "guard_malformed"}
    assert [name for name, _ in commands] == [
        "tuning",
        *(["remote", "local-guard"] if final_executed else []),
        "services",
        "orc-mcap",
        "focused",
        "s3-pooling",
        "workspace-rust",
    ]
    tuning_command = commands[0][1]
    assert tuning_command[tuning_command.index("--stage") + 1] == "tune"
    assert tuning_command[tuning_command.index("--rounds") + 1] == "20"
    assert tuning_command[tuning_command.index("--warmups") + 1] == "1"
    assert "--diagnostics" in tuning_command
    for name, command in commands:
        if name in {"tuning", "remote", "local-guard"}:
            assert command[command.index("--execution-mode") + 1] == execution_mode
    report = json.loads((output / "acceptance.json").read_text())
    assert report["tuning_requested"] is True and len(report["gates"]) == 8
    assert report["execution_mode"] == execution_mode
    assert report["local_manifest_sha256"] == acceptance.file_hash(local_manifest)
    assert report["status"] == ("passed" if scenario == "passed" else "failed")
    if final_executed:
        final_command = commands[1][1]
        assert final_command[final_command.index("--stage") + 1] == "final"
        assert final_command[final_command.index("--rounds") + 1] == "50"
        assert final_command[final_command.index("--warmups") + 1] == "3"
        assert final_command[final_command.index("--candidate-policy") + 1] == "16k_1m"
        assert final_command[final_command.index("--selection") + 1] == str(output / "tuning" / "selected-policy.json")
        assert report["frozen_selection"]["sha256"] == acceptance.file_hash(output / "tuning" / "selected-policy.json")
        guard_command = commands[2][1]
        assert guard_command[:3] == [acceptance.sys.executable, "-m", "benchmarking.vortex.remote_eval"]
        assert guard_command[guard_command.index("--stage") + 1] == "guard"
        assert guard_command[guard_command.index("--rounds") + 1] == "20"
        assert guard_command[guard_command.index("--warmups") + 1] == "1"
        assert guard_command[guard_command.index("--orders") + 1 : guard_command.index("--orders") + 3] == [
            "sorted",
            "shuffled",
        ]
        assert guard_command[guard_command.index("--layouts") + 1 : guard_command.index("--layouts") + 3] == [
            "single",
            "multi",
        ]
        assert guard_command[guard_command.index("--candidate-policy") + 1] == "16k_1m"
        assert guard_command[guard_command.index("--selection") + 1] == str(output / "tuning" / "selected-policy.json")
        assert guard_command[guard_command.index("--manifest") + 1] == str(local_manifest)
        assert "--diagnostics" in guard_command and "--storage-root" not in guard_command
        assert report["gates"]["local-guard"]["report_sha256"] == acceptance.file_hash(
            output / "local-guard" / "report.json"
        )
        assert report["gates"]["local-guard"]["status"] == ("failed" if scenario.startswith("guard_") else "passed")
    else:
        assert report["gates"]["remote"]["status"] == "blocked"
        assert report["gates"]["remote"]["executed"] is False
        assert report["gates"]["remote"]["returncode"] is None
        assert not (output / "remote-report").exists()
        assert not (output / "remote-exit.txt").exists()
        assert report["gates"]["local-guard"]["status"] == "blocked"
        assert report["gates"]["local-guard"]["executed"] is False
        assert not (output / "local-guard").exists()
    assert [event[0] for event in service_events] == ["start", "cleanup"]


def test_acceptance_rejects_conflicting_selection_before_preflight(tmp_path, monkeypatch):
    monkeypatch.setattr(acceptance, "load_manifest", lambda _: pytest.fail("Conflicting selection reached preflight"))
    with pytest.raises(SystemExit) as failure:
        acceptance.main(
            [
                "--manifest",
                str(tmp_path / "manifest.json"),
                "--output",
                str(tmp_path / "output"),
                "--tune",
                "--selection",
                str(tmp_path / "selected-policy.json"),
            ]
        )
    assert failure.value.code == 2


def test_acceptance_local_guard_requires_complete_corpus_before_service_contact(tmp_path, monkeypatch):
    monkeypatch.setattr(acceptance, "load_manifest", lambda _: {"datasets": {"sorted/multi": {}}})
    monkeypatch.setattr(
        acceptance, "require_api", lambda: pytest.fail("Incomplete local corpus reached API/service preflight")
    )
    with pytest.raises(SystemExit) as failure:
        acceptance.main(
            [
                "--manifest",
                str(tmp_path / "remote.json"),
                "--local-manifest",
                str(tmp_path / "local.json"),
                "--output",
                str(tmp_path / "out"),
            ]
        )
    assert failure.value.code == 2


def test_local_guard_explicit_preset_does_not_require_tuning_selection(tmp_path, monkeypatch):
    def gate(name, command, output, environment):
        assert name == "local-guard"
        assert "--selection" not in command
        assert command[command.index("--candidate-policy") + 1] == "off"
        directory = output / "local-guard"
        directory.mkdir()
        (directory / "report.json").write_text(
            json.dumps(
                {
                    "status": "passed",
                    "correctness_status": "passed",
                    "diagnostics_status": "passed",
                    "performance_gate": {"status": "passed"},
                    "settings": {"execution_mode": "fresh-process"},
                }
            )
        )
        return {"status": "passed", "returncode": 0}

    monkeypatch.setattr(acceptance, "run_gate", gate)
    result = acceptance.run_evaluation_gate("guard", tmp_path / "manifest.json", tmp_path, {}, "off")
    assert result["status"] == "passed" and "report_sha256" in result


@pytest.mark.parametrize(
    "scenario", ["startup", "runtime", "cleanup_exception", "cleanup_nonzero", "runtime_and_cleanup", "snapshot"]
)
def test_unexpected_acceptance_failures_are_persisted_and_owned_cleanup_preserves_primary_error(
    tmp_path, monkeypatch, scenario
):
    manifest = tmp_path / "manifest.json"
    manifest.write_text("{}")
    output = tmp_path / "acceptance"
    provenance = {
        "git_head": "head",
        "tracked_changes": False,
        "build_type": "release",
        "build_command": "build",
        "native_binaries_sha256": {"native.so": "sha"},
        "cargo_lock_sha256": "lock",
        "harness_sha256": {},
    }
    secret, events = "private-acceptance-generated-secret", []
    original = ValueError(f"{scenario} failed with {secret}")
    monkeypatch.delenv("VORTEX_MINIO_ENDPOINT", raising=False)
    monkeypatch.setattr(acceptance, "load_manifest", lambda _: {})
    monkeypatch.setattr(acceptance, "require_api", lambda: None)
    monkeypatch.setattr(acceptance, "_require_environment", lambda: None)
    monkeypatch.setattr(acceptance, "benchmark_environment", lambda: provenance)

    def start(image, environment):
        events.append("start")
        environment.update(VORTEX_MINIO_ENDPOINT="http://127.0.0.1:9000", VORTEX_MINIO_SECRET_KEY=secret)
        if scenario == "startup":
            # The existing startup helper cleans its container before raising:
            # it has not transferred ownership to the acceptance orchestrator.
            events.append("startup_cleanup")
            raise original
        return "owned-container"

    def evaluate(*args, **kwargs):
        assert kwargs["execution_mode"] == "fresh-process"
        events.append("evaluate")
        if scenario in {"runtime", "runtime_and_cleanup"}:
            raise original
        return {"status": "passed", "returncode": 0}

    def cleanup(command, **kwargs):
        events.append("cleanup")
        assert command == ["docker", "rm", "--force", "owned-container"]
        if scenario in {"cleanup_exception", "runtime_and_cleanup"}:
            raise OSError(f"cleanup failed with {secret}")
        return subprocess.CompletedProcess(
            command, 1 if scenario == "cleanup_nonzero" else 0, stdout="", stderr=f"cleanup failed with {secret}"
        )

    snapshots = []

    def snapshot(*args):
        snapshots.append(None)
        if scenario == "snapshot" and len(snapshots) == 3:
            raise original
        return {}

    monkeypatch.setattr(acceptance, "_start_minio", start)
    monkeypatch.setattr(acceptance, "_service_snapshot", snapshot)
    monkeypatch.setattr(acceptance, "run_evaluation_gate", evaluate)
    monkeypatch.setattr(acceptance, "run_gate", lambda *args, **kwargs: {"status": "passed", "returncode": 0})
    monkeypatch.setattr(acceptance.subprocess, "run", cleanup)
    args = ["--manifest", str(manifest), "--output", str(output)]
    if scenario in {"startup", "runtime", "runtime_and_cleanup"}:
        with pytest.raises(RuntimeError) as failure:
            acceptance.main(args)
        assert failure.value.__cause__ is original
        assert secret not in str(failure.value)
    else:
        assert acceptance.main(args) == 1
    raw = (output / "acceptance.json").read_text()
    report = json.loads(raw)
    assert report["status"] == "failed" and secret not in raw
    assert report["failure"] and report["exception_type"]
    if scenario == "startup":
        assert events == ["start", "startup_cleanup"]
        assert report["gates"] == {}
    else:
        assert events == ["start", "evaluate", "cleanup"]
    if scenario in {"startup", "runtime", "runtime_and_cleanup", "snapshot"}:
        assert report["exception_type"] == "ValueError"
    if scenario == "runtime_and_cleanup":
        assert report["cleanup_exception_type"] == "OSError"
        assert report["failure"].startswith("runtime_and_cleanup")
    if scenario == "cleanup_nonzero":
        assert report["container_cleanup_returncode"] == 1
    if scenario == "snapshot":
        assert report["service_snapshot_exception_type"] == "ValueError"
