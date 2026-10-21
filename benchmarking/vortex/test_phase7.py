from __future__ import annotations

import copy
import json
from types import SimpleNamespace

import pytest

from benchmarking.vortex import phase7
from benchmarking.vortex.test_profile import capture


def test_pairs_are_adjacent_balanced_and_seeded_across_cases_and_layouts():
    pairs = phase7.comparisons(phase7.EXPERIMENTS)
    args = (["scan_narrow", "eq_0.01"], ["sorted/multi", "shuffled/single"], pairs, 1, 20)
    jobs = phase7.schedule(*args, 71)
    assert jobs == phase7.schedule(*args, 71)
    assert jobs != phase7.schedule(*args, 72)
    assert len(jobs) == 21 * 2 * 2 * len(pairs) * 2
    for index in range(0, len(jobs), 2):
        first, second = jobs[index : index + 2]
        assert first["pair_id"] == second["pair_id"]
        assert first["pair_position"] == 0 and second["pair_position"] == 1
        assert {first["pair_member"], second["pair_member"]} == {"A", "B"}
        assert (first["dataset"], first["case"], first["comparison"], first["iteration"]) == (
            second["dataset"],
            second["case"],
            second["comparison"],
            second["iteration"],
        )
    for comparison in pairs:
        orders = [
            job["pair_order"]
            for job in jobs
            if not job["warmup"] and job["pair_position"] == 0 and job["comparison"] == comparison
        ]
        assert orders.count("AB") == orders.count("BA") == 40


def test_identical_control_has_equal_payloads_and_optimization_isolated():
    configs = phase7.configurations()
    assert phase7.comparisons(["row-count"]) == {"lightweight_planning": ("baseline", "row_count")}
    assert phase7.fingerprint(configs["baseline"]) == phase7.fingerprint(configs["baseline_copy"])
    assert configs["baseline"] is not configs["baseline_copy"]
    candidate = copy.deepcopy(configs["row_count"])
    assert candidate["options"].pop("enable_row_count_planning") is True
    baseline = copy.deepcopy(configs["baseline"])
    assert baseline["options"].pop("enable_row_count_planning") is False
    assert candidate == baseline
    for cap in (1, 2, 4):
        modified = copy.deepcopy(configs[f"requests_{cap}"])
        assert modified["options"]["max_concurrent_requests"] == cap
        modified["options"]["max_concurrent_requests"] = 8
        assert modified == configs["baseline"]


def test_pair_summary_retains_failures_and_keeps_paired_ratios_distinct():
    pairs = {"test": ("baseline", "row_count")}
    trials = phase7.schedule(["scan_narrow"], ["sorted/multi"], pairs, 0, 2, 3)
    for trial in trials:
        trial.update(status="passed", output_sha256="same", wall_seconds=1 if trial["pair_member"] == "A" else 2)
    row = phase7.summarize(trials, ["scan_narrow"], ["sorted/multi"], pairs, 2)[0]
    assert row["status"] == "passed"
    assert row["paired_B_over_A"]["median"] == row["B_over_A_ratio_of_medians"] == 2
    assert row["measured_AB_pairs"] == row["measured_BA_pairs"] == 1
    trials[0]["status"] = "failed"
    row = phase7.summarize(trials, ["scan_narrow"], ["sorted/multi"], pairs, 2)[0]
    assert row["status"] == "failed" and row["complete_pairs"] == 1
    assert row["failed_pair_iterations"] == [0]
    assert len(trials) == 4


def test_pair_output_mismatch_is_retained_as_failed_measurement():
    pairs = {"test": ("baseline", "row_count")}
    trials = phase7.schedule(["scan_narrow"], ["sorted/multi"], pairs, 0, 2, 3)
    for trial in trials:
        trial.update(status="passed", output_sha256="same", wall_seconds=1)
    trials[0]["output_sha256"] = "changed"
    row = phase7.summarize(trials, ["scan_narrow"], ["sorted/multi"], pairs, 2)[0]
    assert row["status"] == "failed" and row["complete_pairs"] == 1
    assert row["failed_pair_iterations"] == row["output_mismatch_pair_iterations"] == [0]


@pytest.mark.parametrize(
    "kwargs",
    [
        {"rounds": 19},
        {"rounds": 21},
        {"warmups": 0},
        {"diagnostic_rounds": 2},
        {"timeout": float("nan")},
        {"rounds": True},
    ],
)
def test_release_diagnosis_requires_balanced_minimum_measurements(kwargs):
    options = {
        "cases": ["scan_narrow"],
        "datasets": ["sorted/multi"],
        "experiments": ["aa"],
        "rounds": 20,
        "warmups": 1,
        "diagnostic_rounds": 3,
        "timeout": 1,
        "allow_dev": False,
    }
    options.update(kwargs)
    with pytest.raises(ValueError):
        phase7.validate(**options)


@pytest.fixture
def fake_run(tmp_path, monkeypatch):
    manifest = tmp_path / "manifest.json"
    manifest.write_text(json.dumps({"datasets": {"sorted/multi": {}}, "oracle": {}}))
    monkeypatch.setattr(phase7, "load_manifest", lambda path: json.loads(path.read_text()))
    monkeypatch.setattr(phase7, "source_paths", lambda *_: [])
    monkeypatch.setattr(phase7, "io_settings", lambda: None)
    monkeypatch.setattr(phase7, "require_api", lambda: None)
    provenance = {
        "build_type": "release",
        "build_command": "attested",
        "native_binaries_sha256": {"native": "hash"},
        "git_head": "commit",
        "tracked_changes": False,
    }
    monkeypatch.setattr(phase7, "environment", lambda: provenance.copy())
    requests, sessions = [], []
    fail = {"enabled": False}

    class Session:
        def __init__(self, *_):
            self.process = SimpleNamespace(pid=1234, returncode=None)
            self.failure = None
            self.started_at_utc = "start"
            self.completed_at_utc = None
            sessions.append(self)

        def __enter__(self):
            return self

        def execute(self, spec_path, result_path, timeout):
            spec = json.loads(spec_path.read_text())
            requests.append(spec)
            if fail["enabled"]:
                self.failure = "failed request; no restart"
                raise RuntimeError(self.failure)
            result = {"status": "passed", "wall_seconds": 1, "output_sha256": "verified", "worker_pid": 1234}
            if spec["diagnostic"]:
                result.update(diagnostics=capture(), diagnostics_complete=True)
            result_path.write_text(json.dumps(result))
            return result

        def __exit__(self, *_):
            self.process.returncode = 1 if self.failure else 0
            self.completed_at_utc = "end"

    monkeypatch.setattr(phase7, "PersistentWorker", Session)
    return SimpleNamespace(
        manifest=manifest, output=tmp_path / "output", requests=requests, sessions=sessions, fail=fail
    )


def test_full_harness_persists_one_process_and_runs_diagnostics_after_trials(fake_run):
    state = fake_run
    result = phase7.run(state.manifest, state.output, cases=["scan_narrow"], experiments=["aa"], rounds=20)
    assert result["status"] == "passed"
    assert result["performance_gate"]["status"] == "not_evaluated"
    assert len(state.sessions) == 1
    assert len(result["trials"]) == 42 and len(result["diagnostics"]) == 6
    assert all(not request["diagnostic"] for request in state.requests[:42])
    assert all(request["diagnostic"] for request in state.requests[42:])
    assert all(request["execution_mode"] == "persistent-process" for request in state.requests)
    assert (state.output / "profiles/00000/timeline.json").exists()
    assert result["worker_session"]["returncode"] == 0
    assert json.loads((state.output / "report.json").read_text()) == result
    with pytest.raises(FileExistsError):
        phase7.run(state.manifest, state.output, cases=["scan_narrow"], experiments=["aa"])


def test_failed_session_retains_all_scheduled_failures_without_restart(fake_run):
    state = fake_run
    state.fail["enabled"] = True
    result = phase7.run(
        state.manifest,
        state.output,
        cases=["scan_narrow"],
        experiments=["aa"],
        rounds=2,
        warmups=0,
        diagnostic_rounds=1,
        allow_dev=True,
    )
    assert result["status"] == "failed"
    assert len(state.sessions) == 1
    assert len(result["trials"]) == 4 and len(result["diagnostics"]) == 2
    assert all(trial["status"] == "failed" for trial in result["trials"])
    assert result["summary"][0]["failed_pair_iterations"] == [0, 1]


def test_changed_native_identity_invalidates_completed_measurements(fake_run, monkeypatch):
    original = phase7.environment()
    changed = {**original, "native_binaries_sha256": {"native": "changed"}}
    states = iter([original, changed])
    monkeypatch.setattr(phase7, "environment", lambda: next(states))
    result = phase7.run(
        fake_run.manifest,
        fake_run.output,
        cases=["scan_narrow"],
        experiments=["aa"],
        rounds=2,
        warmups=0,
        diagnostic_rounds=1,
        allow_dev=True,
    )
    assert result["status"] == "failed" and not result["provenance_unchanged"]
    assert all(row["status"] == "passed" for row in result["summary"])


def test_changed_fixture_inventory_invalidates_completed_measurements(fake_run, monkeypatch):
    original = phase7.load_manifest
    validations = 0

    def validate_manifest(path):
        nonlocal validations
        validations += 1
        if validations == 2:
            raise ValueError("Fixture bytes changed: sorted/multi/part.vortex")
        return original(path)

    monkeypatch.setattr(phase7, "load_manifest", validate_manifest)
    result = phase7.run(
        fake_run.manifest,
        fake_run.output,
        cases=["scan_narrow"],
        experiments=["aa"],
        rounds=2,
        warmups=0,
        diagnostic_rounds=1,
        allow_dev=True,
    )
    assert validations == 2
    assert result["status"] == "failed" and not result["provenance_unchanged"]
    checks = result["artifact_checks_after"]
    assert checks["manifest"]["unchanged"] and not checks["local_fixture_inventory"]["unchanged"]
    assert "Fixture bytes changed" in checks["local_fixture_inventory"]["error"]
    assert json.loads((fake_run.output / "report.json").read_text()) == result


@pytest.mark.parametrize("remove", [False, True])
def test_changed_or_missing_frozen_settings_invalidates_report(fake_run, monkeypatch, remove):
    original = phase7.environment
    calls = 0

    def environment():
        nonlocal calls
        calls += 1
        if calls == 2:
            frozen = fake_run.output / "frozen-experiment.json"
            if remove:
                frozen.unlink()
            else:
                frozen.write_text("changed")
        return original()

    monkeypatch.setattr(phase7, "environment", environment)
    result = phase7.run(
        fake_run.manifest,
        fake_run.output,
        cases=["scan_narrow"],
        experiments=["aa"],
        rounds=2,
        warmups=0,
        diagnostic_rounds=1,
        allow_dev=True,
    )
    assert result["status"] == "failed" and not result["provenance_unchanged"]
    assert not result["artifact_checks_after"]["frozen_experiment"]["unchanged"]
    assert json.loads((fake_run.output / "report.json").read_text()) == result


def test_all_experiments_share_session_and_keep_every_comparison(fake_run):
    state = fake_run
    result = phase7.run(
        state.manifest, state.output, cases=["scan_narrow"], rounds=2, warmups=0, diagnostic_rounds=1, allow_dev=True
    )
    assert result["status"] == "passed"
    assert len(result["summary"]) == 7
    assert len(result["trials"]) == 28
    assert len(result["diagnostics"]) == 7
    assert len(state.sessions) == 1
    assert (
        result["settings"]["configuration_sha256"]["baseline"]
        == result["settings"]["configuration_sha256"]["baseline_copy"]
    )
