"""Persistent scheduling and failed-session reporting without services."""

from __future__ import annotations

import json
from types import SimpleNamespace

import pytest

from benchmarking.vortex import remote_eval as evaluation
from benchmarking.vortex.run import schedule
from benchmarking.vortex.test_remote_eval import manifest, sample


@pytest.fixture
def persistent_run(tmp_path, monkeypatch):
    path = tmp_path / "manifest.json"
    path.write_text(json.dumps(manifest()))
    output = tmp_path / "report"
    secret = "private-persistent-schedule-secret"
    monkeypatch.setenv("DAFT_VORTEX_BENCH_IO_JSON", json.dumps({"s3": {"access_key": secret}}))
    monkeypatch.setattr(evaluation, "load_manifest", lambda _: manifest())
    monkeypatch.setattr(evaluation, "require_api", lambda: None)
    monkeypatch.setattr(
        evaluation,
        "environment",
        lambda: {
            "git_head": "frozen-source",
            "tracked_changes": False,
            "build_type": "release",
            "build_command": "release-build",
            "native_binaries_sha256": {"native.so": "frozen-native"},
            "cargo_lock_sha256": "frozen-lock",
            "harness_sha256": {},
        },
    )
    sessions, events = [], []
    failure = {"enabled": False}

    class Session:
        def __init__(self, directory, environment):
            assert environment["DAFT_ANALYTICS_ENABLED"] == "0"
            self.process = SimpleNamespace(pid=12345, returncode=None)
            self.failure = None
            self.started_at_utc = "2026-10-21T10:00:00+00:00"
            self.completed_at_utc = None
            self.log_path = directory / "persistent-worker.log"
            self.log_path.write_text("")
            self.requests = []
            self.executed = []
            sessions.append(self)

        def __enter__(self):
            events.append("enter")
            return self

        def execute(self, spec_path, result_path, timeout):
            assert timeout == 60
            spec = json.loads(spec_path.read_text())
            self.requests.append(spec)
            if self.failure is not None:
                raise RuntimeError(f"Persistent worker unavailable after prior failure: {self.failure}")
            events.append("diagnostic" if spec["diagnostic"] else "performance")
            self.executed.append(spec)
            if failure["enabled"] and not spec["warmup"] and not spec["diagnostic"]:
                self.failure = f"Query failed with {secret}"
                self.process.returncode = 1
                raise RuntimeError(self.failure)
            wall = 999 if spec["warmup"] or spec["diagnostic"] else 2 if spec["config"] == "parquet" else 1
            result = sample(spec["config"], spec["case"], spec["iteration"], wall)
            result.update(
                diagnostic=spec["diagnostic"],
                diagnostics_complete=True,
                execution_mode="persistent-process",
                worker_pid=self.process.pid,
                peak_rss_bytes=None,
                process_peak_rss_bytes=2048,
            )
            result_path.write_text(json.dumps(result))
            return result

        def __exit__(self, *_):
            events.append("exit")
            self.completed_at_utc = "2026-10-21T10:01:00+00:00"
            if self.process.returncode is None:
                self.process.returncode = 0

    monkeypatch.setattr(evaluation, "PersistentWorker", Session)
    return SimpleNamespace(
        manifest=path, output=output, sessions=sessions, events=events, failure=failure, secret=secret
    )


def run(state, stage):
    return evaluation.run(
        state.manifest,
        state.output,
        stage=stage,
        candidate_policy="off",
        rounds=50 if stage == "final" else 20,
        warmups=3 if stage == "final" else 1,
        seed=173,
        storage_root="s3://owned-temporary-bucket",
        timeout=60,
        diagnostics=True,
        execution_mode="persistent-process",
    )


def assert_schedule(state, report, stage):
    assert len(state.sessions) == 1
    session = state.sessions[0]
    configs = evaluation.configurations(stage, "off")
    settings = report["settings"]
    jobs = schedule(
        evaluation.QUERY_CASES, ["sorted/multi"], list(configs), settings["warmups"], settings["rounds"], 173
    )
    diagnostic_jobs = schedule(evaluation.QUERY_CASES, ["sorted/multi"], list(configs), 0, 1, 173)
    assert len(session.requests) == len(jobs) + len(diagnostic_jobs)
    for index, (spec, expected) in enumerate(zip(session.requests, jobs + diagnostic_jobs)):
        assert {key: spec[key] for key in expected} == expected
        assert spec["diagnostic"] is (index >= len(jobs))
        assert spec["execution_mode"] == "persistent-process"
        assert spec["configuration"] == configs[spec["config"]]
    assert len(report["trials"]) == len(jobs)
    assert len(report["diagnostics"]) == len(diagnostic_jobs)
    assert state.events[0] == "enter" and state.events[-1] == "exit"
    return jobs


def test_persistent_rounds_share_one_session_and_diagnostics_follow_performance(persistent_run):
    state = persistent_run
    report = run(state, "final")
    jobs = assert_schedule(state, report, "final")
    assert state.events == ["enter", *(["performance"] * len(jobs)), *(["diagnostic"] * 15), "exit"]
    assert report["status"] == report["correctness_status"] == report["performance_gate"]["status"] == "passed"
    assert report["diagnostics_status"] == "passed"
    assert report["settings"]["execution_mode"] == "persistent-process"
    assert report["worker_session"]["pid"] == 12345
    assert report["worker_session"]["returncode"] == 0
    assert report["worker_session"]["failure"] is None
    assert report["worker_session"]["started_at_utc"] < report["worker_session"]["completed_at_utc"]
    assert report["worker_session"]["process_peak_rss_bytes"] == 2048
    assert {trial["worker_pid"] for trial in report["trials"] + report["diagnostics"]} == {12345}
    for row in report["summary"]:
        assert row["metrics"]["wall_seconds"]["median"] == (2 if row["config"] == "parquet" else 1)
        assert row["metrics"]["peak_rss_bytes"]["median"] is None
    disk_report = json.loads((state.output / "report.json").read_text())
    assert disk_report == report


@pytest.mark.parametrize("stage", ["tune", "final"])
def test_failed_session_retains_remaining_trials_without_restart_or_selection(persistent_run, stage):
    state = persistent_run
    state.failure["enabled"] = True
    report = run(state, stage)
    jobs = assert_schedule(state, report, stage)
    warmups = [job for job in jobs if job["warmup"]]
    session = state.sessions[0]
    assert len(session.executed) == len(warmups) + 1
    assert session.failure is not None and session.process.returncode == 1
    assert all(trial["status"] == "passed" for trial in report["trials"] if trial["warmup"])
    assert all(trial["status"] == "failed" for trial in report["trials"] if not trial["warmup"])
    assert all(trial["status"] == "failed" for trial in report["diagnostics"])
    assert report["status"] == report["correctness_status"] == report["diagnostics_status"] == "failed"
    assert report["worker_session"]["returncode"] == 1
    assert report["worker_session"]["failure"] is not None
    assert report["performance_gate"]["status"] == ("failed" if stage == "final" else "not_evaluated")
    assert not (state.output / "selected-policy.json").exists()
    for path in state.output.rglob("*"):
        if path.is_file():
            assert state.secret not in path.read_text()
    assert "<redacted>" in (state.output / "report.json").read_text()
