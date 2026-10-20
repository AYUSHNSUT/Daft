"""Policy gates, independent output validation and diagnostic scope without timings."""

from __future__ import annotations

import copy
import json
import subprocess
from contextlib import contextmanager
from datetime import timedelta
from pathlib import Path
from types import SimpleNamespace

import pytest

from benchmarking.vortex import remote_eval as evaluation
from benchmarking.vortex.run import schedule


def manifest():
    return {
        "generator": {"rows": 10},
        "datasets": {
            "sorted/multi": {
                "order": "sorted",
                "layout": "multi",
                "paths": {"parquet": ["data.parquet"], "vortex": ["data.vortex"]},
            }
        },
        "oracle": {case: [{"rows": 10}] for case in evaluation.QUERY_CASES},
    }


def sample(config, case, iteration, wall, *, status="passed"):
    return {
        "dataset": "sorted/multi",
        "case": case,
        "config": config,
        "iteration": iteration,
        "warmup": iteration < 0,
        "diagnostic": False,
        "status": status,
        "wall_seconds": wall,
        "cpu_seconds": 0.1,
        "peak_rss_bytes": 1024,
        "source_read_bytes": 100,
        "output_sha256": "verified-oracle-digest",
    }


@pytest.mark.parametrize(
    "stage,rounds,warmups",
    [("tune", 19, 1), ("tune", 20, 0), ("final", 49, 3), ("final", 51, 3), ("final", 50, 2), ("final", True, 3)],
)
def test_frozen_performance_minimums(stage, rounds, warmups):
    with pytest.raises(ValueError):
        evaluation.validate_options(stage, "64k_4m", evaluation.QUERY_CASES, rounds, warmups, False, 60)


@pytest.mark.parametrize("timeout", [True, 0, -1, float("nan"), float("inf")])
def test_finite_positive_timeout(timeout):
    with pytest.raises(ValueError, match="timeout"):
        evaluation.validate_options("final", "off", evaluation.QUERY_CASES, 50, 3, False, timeout)


def test_no_per_query_configuration_selection_and_seeded_complete_rounds():
    with pytest.raises(ValueError, match="all five"):
        evaluation.validate_options("final", "64k_4m", ["scan_narrow"], 50, 3, False, 60)
    configs = evaluation.configurations("tune", "64k_4m")
    assert len(configs) == 9
    assert {
        (
            entry["options"]["enable_read_coalescing"],
            entry["options"]["read_coalesce_gap_bytes"],
            entry["options"]["read_coalesce_max_bytes"],
            entry["options"]["discovery_concurrency"],
        )
        for entry in configs.values()
        if entry["reader"] == "vortex"
    } == {
        (enabled, gap, maximum, cap)
        for enabled, gap, maximum in (
            (False, 65536, 4194304),
            (True, 16384, 1048576),
            (True, 65536, 4194304),
            (True, 262144, 4194304),
        )
        for cap in (1, 8)
    }
    jobs = schedule(evaluation.QUERY_CASES, ["sorted/multi"], list(configs), 1, 20, 42)
    assert len(jobs) == 945
    assert jobs == schedule(evaluation.QUERY_CASES, ["sorted/multi"], list(configs), 1, 20, 42)
    assert jobs != schedule(evaluation.QUERY_CASES, ["sorted/multi"], list(configs), 1, 20, 43)
    for iteration in range(-1, 20):
        block = [job for job in jobs if job["iteration"] == iteration]
        assert {(job["case"], job["config"]) for job in block} == {
            (case, config) for case in evaluation.QUERY_CASES for config in configs
        }
    final = evaluation.configurations("final", "16k_1m")
    assert list(final) == ["parquet", "phase5", "candidate"]
    assert final["phase5"]["options"]["enable_read_coalescing"] is False
    assert final["phase5"]["options"]["discovery_concurrency"] == 1
    assert final["candidate"]["options"]["discovery_concurrency"] == 8
    assert len(schedule(evaluation.QUERY_CASES, ["sorted/multi"], list(final), 3, 50, 42)) == 795


def test_final_gate_rejects_one_regression_equal_median_or_missing_sample():
    configs = evaluation.configurations("final", "64k_4m")
    trials = [
        sample(config, case, iteration, 10 if config == "parquet" else 9)
        for case in evaluation.QUERY_CASES
        for config in configs
        for iteration in (-1, 0, 1)
    ]
    trials[0]["wall_seconds"] = 99999
    summary = evaluation.summarize(trials, ["sorted/multi"], evaluation.QUERY_CASES, configs, 2)
    assert (
        evaluation.performance_gate(summary, stage="final", allow_dev=False, all_trials_passed=True)["status"]
        == "passed"
    )
    for value in (10, 11):
        changed = copy.deepcopy(trials)
        for trial in changed:
            if trial["case"] == "join" and trial["config"] == "candidate":
                trial["wall_seconds"] = value
        broken = evaluation.summarize(changed, ["sorted/multi"], evaluation.QUERY_CASES, configs, 2)
        gate = evaluation.performance_gate(broken, stage="final", allow_dev=False, all_trials_passed=True)
        assert gate["status"] == "failed" and gate["misses"][0]["case"] == "join"
    missing = evaluation.summarize(trials[:-1], ["sorted/multi"], evaluation.QUERY_CASES, configs, 2)
    assert (
        evaluation.performance_gate(missing, stage="final", allow_dev=False, all_trials_passed=True)["status"]
        == "failed"
    )
    assert (
        evaluation.performance_gate(summary, stage="final", allow_dev=False, all_trials_passed=False)["status"]
        == "failed"
    )
    assert (
        evaluation.performance_gate(summary, stage="final", allow_dev=True, all_trials_passed=True)["status"]
        == "not_evaluated"
    )


@pytest.mark.parametrize("candidate,expected", [(10, "passed"), (10.5, "passed"), (10.51, "failed")])
def test_local_guard_compares_phase5_with_five_percent_tolerance(candidate, expected):
    configs = evaluation.configurations("guard", "64k_4m")
    trials = [
        sample(config, case, 0, {"parquet": 1, "phase5": 10, "candidate": candidate}[config])
        for case in evaluation.QUERY_CASES
        for config in configs
    ]
    summary = evaluation.summarize(trials, ["sorted/multi"], evaluation.QUERY_CASES, configs, 1)
    gate = evaluation.performance_gate(summary, stage="guard", allow_dev=False, all_trials_passed=True)
    assert gate["status"] == expected
    assert (
        evaluation.performance_gate(summary, stage="final", allow_dev=False, all_trials_passed=True)["status"]
        == "failed"
    )
    assert (
        evaluation.performance_gate(summary, stage="guard", allow_dev=True, all_trials_passed=True)["status"]
        == "not_evaluated"
    )


def test_guard_has_local_storage_and_repetition_requirements(tmp_path):
    with pytest.raises(ValueError, match="at least 20"):
        evaluation.validate_options("guard", "off", evaluation.QUERY_CASES, 19, 1, False, 60)
    with pytest.raises(ValueError, match="without a storage root"):
        evaluation.run(tmp_path / "manifest.json", tmp_path / "out", stage="guard", storage_root="s3://bucket")
    assert not (tmp_path / "out").exists()


def test_ranking_uses_worst_case_before_average_and_has_stable_ties():
    configs = evaluation.configurations("tune", "64k_4m")
    summary = []
    for config in configs:
        for index, case in enumerate(evaluation.QUERY_CASES):
            wall = 1 if config == "parquet" else 0.9
            if config == "16k_1m_discovery8":
                wall = 1.1 if index == 0 else 0.1
            summary += [sample(config, case, 0, wall)]
    rows = evaluation.summarize(summary, ["sorted/multi"], evaluation.QUERY_CASES, configs, 1)
    ranked = evaluation.rank_policies(rows, configs)
    assert ranked[-1]["config"] == "16k_1m_discovery8"
    assert [row["config"] for row in ranked[:-1]] == sorted(row["config"] for row in ranked[:-1])
    broken = copy.deepcopy(summary)
    broken[-1]["output_sha256"] = "repeatable-but-wrong-output"
    with pytest.raises(AssertionError, match="different outputs"):
        evaluation.summarize(broken, ["sorted/multi"], evaluation.QUERY_CASES, configs, 1)


@pytest.mark.parametrize("diagnostic", [False, True])
@pytest.mark.parametrize("outcome", ["success", "wrong_oracle", "execution_error"])
def test_worker_oracle_and_capture_scope(tmp_path, monkeypatch, diagnostic, outcome):
    import daft
    from daft.io import _vortex_diagnostics

    path = tmp_path / "manifest.json"
    path.write_text(json.dumps(manifest()))
    events = []

    class Frame:
        def collect(self):
            events.append("collect")
            if outcome == "execution_error":
                raise RuntimeError("controlled query failure")

        def to_arrow(self):
            events.append("oracle")
            return SimpleNamespace(to_pylist=lambda: [{"rows": 11 if outcome == "wrong_oracle" else 10}])

        @property
        def metrics(self):
            return SimpleNamespace(
                to_pylist=lambda: [
                    {
                        "category": "Source",
                        "type": "ScanTask",
                        "duration": timedelta(),
                        "stats": [["bytes.read", {"value": 123}]],
                    }
                ]
            )

        def explain(self, **kwargs):
            events.append("explain")
            kwargs["file"].write("saved plan")

    @contextmanager
    def stage(name):
        events.append(name + "_start")
        yield
        events.append(name + "_end")

    def finish():
        events.append("finish")
        return {"native": {"unfinished_events": 0, "dropped_events": 0}, "python": {"dropped_events": 0}}

    def build(*_):
        events.append("query")
        return Frame()

    monkeypatch.setattr(evaluation, "require_api", lambda: None)
    monkeypatch.setattr(daft, "set_runner_native", lambda: None)
    monkeypatch.setattr(
        daft,
        "from_pydict",
        lambda _: SimpleNamespace(agg=lambda *_: SimpleNamespace(collect=lambda: events.append("warmup_runtime"))),
    )
    monkeypatch.setattr(evaluation, "query", build)
    monkeypatch.setattr(_vortex_diagnostics, "start", lambda: events.append("start"))
    monkeypatch.setattr(_vortex_diagnostics, "finish", finish)
    monkeypatch.setattr(_vortex_diagnostics, "stage", stage)
    spec = {
        "manifest": str(path),
        "dataset": "sorted/multi",
        "case": "scan_narrow",
        "allow_dev": True,
        "configuration": evaluation.configurations("final", "64k_4m")["candidate"],
        "diagnostic": diagnostic,
    }
    if outcome == "wrong_oracle":
        with pytest.raises(AssertionError, match="Query output differs"):
            evaluation.worker(spec)
    elif outcome == "execution_error" and not diagnostic:
        with pytest.raises(RuntimeError, match="controlled"):
            evaluation.worker(spec)
    else:
        result = evaluation.worker(spec)
        assert result["status"] == ("failed" if outcome == "execution_error" else "passed")
        assert ("wall_seconds" in result) is not diagnostic
        if diagnostic and outcome == "success":
            assert result["diagnostics_complete"] is True
    if diagnostic:
        assert events.index("warmup_runtime") < events.index("start") < events.index("query") < events.index("finish")
        assert events.index("source_creation_start") < events.index("query") < events.index("source_creation_end")
        assert events.index("source_creation_end") < events.index("collection_start")
        assert events.count("source_creation_start") == 1
        if "oracle" in events:
            assert events.index("finish") < events.index("oracle")
        if "explain" in events:
            assert events.index("finish") < events.index("explain")
    else:
        assert "start" not in events and "finish" not in events


@pytest.mark.parametrize("failure", ["exit", "timeout", "incomplete_diagnostic"])
@pytest.mark.parametrize("stage", ["final", "tune"])
def test_failures_are_retained_without_retry_and_diagnostics_are_separate(tmp_path, monkeypatch, failure, stage):
    path = tmp_path / "manifest.json"
    path.write_text("{}")
    monkeypatch.setattr(evaluation, "load_manifest", lambda _: manifest())
    monkeypatch.setattr(evaluation, "require_api", lambda: None)
    monkeypatch.setattr(evaluation, "environment", lambda: {"build_type": "dev"})
    secret = "private-test-credential"
    monkeypatch.setenv("DAFT_VORTEX_BENCH_IO_JSON", json.dumps({"s3": {"access_key": secret}}))
    calls = []
    failing_config = "candidate" if stage == "final" else "64k_4m_discovery8"

    def child(command, **kwargs):
        spec = json.loads(Path(command[command.index("--worker-spec") + 1]).read_text())
        calls.append(spec)
        assert kwargs["env"]["DAFT_ANALYTICS_ENABLED"] == "0"
        if spec["config"] == failing_config and not spec["diagnostic"] and failure != "incomplete_diagnostic":
            if failure == "timeout":
                raise subprocess.TimeoutExpired(command, 60, stderr=secret.encode())
            return subprocess.CompletedProcess(command, 1, stderr=secret)
        result = sample(spec["config"], spec["case"], spec["iteration"], 1)
        result.update(diagnostic=spec["diagnostic"], diagnostics_complete=failure != "incomplete_diagnostic")
        Path(command[command.index("--output") + 1]).write_text(json.dumps(result))
        return subprocess.CompletedProcess(command, 0, stderr=secret)

    monkeypatch.setattr(evaluation.subprocess, "run", child)
    target = tmp_path / "out"
    report = evaluation.run(
        path, target, stage=stage, cases=["scan_narrow"], rounds=1, warmups=0, allow_dev=True, diagnostics=True
    )
    count = 3 if stage == "final" else 9
    assert len(calls) == count * 2
    assert not any(row["diagnostic"] for row in calls[:count])
    assert all(row["diagnostic"] for row in calls[count:])
    assert report["status"] == "failed" and len(report["trials"]) == count and len(report["diagnostics"]) == count
    assert not (target / "selected-policy.json").exists()
    if failure == "incomplete_diagnostic":
        assert report["correctness_status"] == "passed" and report["diagnostics_status"] == "failed"
    assert report["performance_gate"]["status"] == "not_evaluated"
    assert all(secret not in p.read_text() for p in target.rglob("*") if p.is_file())
    assert all(row["trials"] == 1 for row in report["summary"])


def test_unsafe_endpoints_fail_before_output(tmp_path, monkeypatch):
    monkeypatch.setattr(evaluation, "load_manifest", lambda _: manifest())
    monkeypatch.setenv("DAFT_VORTEX_BENCH_IO_JSON", json.dumps({"s3": {"endpoint_url": "http://user:secret@host"}}))
    output = tmp_path / "out"
    with pytest.raises(ValueError, match="plain HTTP"):
        evaluation.run(tmp_path / "manifest.json", output, rounds=1, warmups=0, allow_dev=True)
    assert not output.exists()


def tuning_selection(tmp_path):
    configs = evaluation.configurations("tune", "64k_4m")
    settings = {
        "stage": "tune",
        "cases": list(evaluation.QUERY_CASES),
        "datasets": ["sorted/multi"],
        "rounds": 20,
        "warmups": 1,
        "seed": 42,
        "configurations": configs,
    }
    jobs = schedule(settings["cases"], settings["datasets"], list(configs), 1, 20, 42)
    trials = [
        sample(
            job["config"],
            job["case"],
            job["iteration"],
            10 if job["config"] == "parquet" else 8 if job["config"] == "64k_4m_discovery8" else 9,
        )
        for job in jobs
    ]
    summary = evaluation.summarize(trials, settings["datasets"], settings["cases"], configs, 20)
    provenance = {
        "git_head": "source-a",
        "native_binaries_sha256": {"native.so": "binary-a"},
        "build_type": "release",
        "tracked_changes": False,
    }
    report = {
        "experiment": "remote_read_policy",
        "purpose": "same-build tune remote policy comparison",
        "status": "passed",
        "correctness_status": "passed",
        "provenance_unchanged": True,
        "settings": settings,
        "trials": trials,
        "summary": summary,
        "ranking": evaluation.rank_policies(summary, configs),
        "environment": provenance,
        "storage_root": "s3://temporary-bucket",
        "manifest_sha256": "remote-fixtures",
    }
    selected = {
        "selection_version": 1,
        "policy": "64k_4m",
        "config": "64k_4m_discovery8",
        "configuration": configs["64k_4m_discovery8"],
        "discovery_concurrency": 8,
        "metadata_concurrency": 8,
        "criterion": evaluation.RANKING,
        "development_only": False,
        "source_revision": provenance["git_head"],
        "native_binaries_sha256": provenance["native_binaries_sha256"],
        "manifest_sha256": report["manifest_sha256"],
    }
    report_path, selection_path = tmp_path / "report.json", tmp_path / "selected-policy.json"

    def save():
        report_path.write_text(json.dumps(report, sort_keys=True))
        selected["tuning_report_sha256"] = evaluation.file_hash(report_path)
        selection_path.write_text(json.dumps(selected, sort_keys=True))

    save()
    return report, selected, provenance, selection_path, save


def test_selection_binds_report_winner_and_current_build_with_guard_manifest_exception(tmp_path):
    _, selected, provenance, path, _ = tuning_selection(tmp_path)
    assert evaluation.load_selection(path) == selected
    evaluation.validate_selection_identity(selected, provenance, "remote-fixtures", "final")
    evaluation.validate_selection_identity(selected, provenance, "different-local-fixtures", "guard")
    with pytest.raises(ValueError, match="manifest"):
        evaluation.validate_selection_identity(selected, provenance, "different-fixtures", "final")
    for changed in (
        {**provenance, "git_head": "source-b"},
        {**provenance, "native_binaries_sha256": {"native.so": "binary-b"}},
    ):
        with pytest.raises(ValueError, match="source/native"):
            evaluation.validate_selection_identity(selected, changed, "remote-fixtures", "final")
        with pytest.raises(ValueError, match="source/native"):
            evaluation.validate_selection_identity(selected, changed, "local-fixtures", "guard")


@pytest.mark.parametrize(
    "mutation",
    [
        "hash",
        "status",
        "correctness",
        "stage",
        "cases",
        "rounds",
        "warmups",
        "controls",
        "winner",
        "ranking",
        "trial",
        "source",
        "native",
        "manifest",
        "development_purpose",
    ],
)
def test_selection_rejects_inconsistent_or_incomplete_tuning_evidence(tmp_path, mutation):
    report, selected, _, path, save = tuning_selection(tmp_path)
    if mutation == "hash":
        (tmp_path / "report.json").write_text((tmp_path / "report.json").read_text() + " ")
    else:
        if mutation in {"status", "correctness"}:
            report["status" if mutation == "status" else "correctness_status"] = "failed"
        elif mutation in {"stage", "cases", "rounds", "warmups"}:
            report["settings"][mutation] = {"stage": "guard", "cases": ["join"], "rounds": 19, "warmups": 0}[mutation]
        elif mutation == "controls":
            report["settings"]["configurations"]["64k_4m_discovery8"]["options"]["read_coalesce_gap_bytes"] = 1
        elif mutation == "winner":
            selected.update(
                policy="off",
                config="off_discovery8",
                configuration=report["settings"]["configurations"]["off_discovery8"],
            )
        elif mutation == "ranking":
            report["ranking"] = report["ranking"][::-1]
        elif mutation == "trial":
            report["trials"][0]["status"] = "failed"
        elif mutation == "source":
            selected["source_revision"] = "different-source"
        elif mutation == "native":
            selected["native_binaries_sha256"] = {"native.so": "different-binary"}
        elif mutation == "development_purpose":
            report["purpose"] = "development correctness smoke; no performance claim"
        else:
            selected["manifest_sha256"] = "different-manifest"
        save()
    with pytest.raises(ValueError):
        evaluation.load_selection(path)


def test_development_selection_fails_before_production_output(tmp_path):
    output = tmp_path / "out"
    selected = tmp_path / "selected.json"
    selected.write_text(
        json.dumps(
            {
                "selection_version": 1,
                "discovery_concurrency": 8,
                "metadata_concurrency": 8,
                "policy": "off",
                "development_only": True,
            }
        )
    )
    with pytest.raises(ValueError, match="production tuning"):
        evaluation.run(tmp_path / "manifest.json", output, stage="final", selection=selected)
    assert not output.exists()
