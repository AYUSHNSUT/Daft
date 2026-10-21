from __future__ import annotations

import copy
import json
from pathlib import Path

import pytest

from benchmarking.vortex import phase7, phase8
from benchmarking.vortex.test_phase7 import fake_run as fake_run  # noqa: PLC0414


def test_independent_candidates_change_only_one_control():
    configs = phase8.configurations()
    assert configs["baseline"] == configs["baseline_copy"]
    for name, flag in zip(("offsets", "sessions", "pruning", "splits"), phase8.FLAGS):
        candidate = phase8.validate_configuration(configs[name])
        assert candidate["options"].pop(flag) is True
        baseline = copy.deepcopy(configs["baseline"])
        assert baseline["options"].pop(flag) is False
        assert candidate == baseline


@pytest.mark.parametrize("mutation", ["missing", "unknown", "int_bool", "negative", "policy"])
def test_candidate_freeze_rejects_implicit_or_inconsistent_options(mutation):
    config = phase8.configurations()["combined"]
    if mutation == "missing":
        del config["options"]["enable_large_offsets"]
    elif mutation == "unknown":
        config["options"]["unimplemented"] = True
    elif mutation == "int_bool":
        config["options"]["max_concurrent_requests"] = True
    elif mutation == "negative":
        config["options"]["batch_size"] = -1
    else:
        config["options"]["enable_read_coalescing"] = True
    with pytest.raises(ValueError):
        phase8.validate_configuration(config)


def test_independent_sessions_keep_seeds_options_and_trial_order(fake_run, monkeypatch):
    monkeypatch.setattr(phase8, "require_api", lambda: None)
    result = phase8.run(
        fake_run.manifest,
        fake_run.output,
        cases=["scan_narrow"],
        experiments=["aa", "offsets"],
        datasets=["sorted/multi"],
        sessions=2,
        rounds=2,
        warmups=0,
        diagnostic_rounds=1,
        allow_dev=True,
    )
    assert result["status"] == "passed"
    assert len(fake_run.sessions) == 2
    reports = [json.loads(Path(entry["report"]).read_text()) for entry in result["sessions"]]
    assert reports[0]["settings"]["seed"] != reports[1]["settings"]["seed"]
    assert all(report["settings"]["configurations"]["offsets"]["options"]["enable_large_offsets"] for report in reports)
    assert all(len(report["trials"]) == 8 for report in reports)
    assert all(report["order_diagnostics"] for report in reports)


@pytest.mark.parametrize(
    "stage,ratio,expected",
    [
        ("final", 0.99, "passed"),
        ("final", 1, "failed"),
        ("final", 1.01, "failed"),
        ("guard", 1.05, "passed"),
        ("guard", 1.051, "failed"),
    ],
)
def test_original_performance_thresholds_are_preserved(stage, ratio, expected):
    comparison = "parquet_vs_candidate" if stage == "final" else "baseline_vs_candidate"
    report = {
        "status": "passed",
        "settings": {"development_only": False, "cases": list(phase8.QUERY_CASES), "datasets": ["sorted/multi"]},
        "summary": [
            {
                "dataset": "sorted/multi",
                "case": case,
                "comparison": comparison,
                "status": "passed",
                "B_over_A_ratio_of_medians": ratio,
            }
            for case in phase8.QUERY_CASES
        ],
    }
    assert phase8.performance_gate(report, stage)["status"] == expected
    report["summary"].pop()
    assert phase8.performance_gate(report, stage)["status"] == "failed"


def release_report():
    configs = phase8.configurations()
    pairs = phase8.comparisons(["combined", "parquet"])
    cases, datasets, rounds = list(phase8.QUERY_CASES), ["sorted/multi"], 20
    trials = phase7.schedule(cases, datasets, pairs, 1, rounds, 11)
    for trial in trials:
        trial.update(
            status="passed",
            wall_seconds=1,
            output_sha256="oracle",
            configuration=configs[trial["config"]],
            execution_mode="persistent-process",
        )
    return {
        "experiment": "phase8_paired_diagnosis",
        "status": "passed",
        "provenance_unchanged": True,
        "environment": {"git_head": "source", "native_binaries_sha256": {"native": "binary"}},
        "storage_root": "s3://local",
        "manifest_sha256": "fixture",
        "settings": {
            "configurations": configs,
            "comparisons": pairs,
            "development_only": False,
            "cases": cases,
            "datasets": datasets,
            "rounds": rounds,
            "warmups": 1,
            "seed": 11,
            "diagnostic_rounds": 3,
            "configuration_sha256": {name: phase7.fingerprint(config) for name, config in configs.items()},
        },
        "trials": trials,
        "summary": phase7.summarize(trials, cases, datasets, pairs, rounds),
    }


def test_frozen_candidate_loads_exact_options_and_rejects_modified_evidence(tmp_path):
    report = release_report()
    path, selected = tmp_path / "report.json", tmp_path / "selection.json"
    path.write_text(json.dumps(report))
    phase8.freeze(path, "combined", selected)
    provenance = report["environment"]
    assert (
        phase8.load_selection(selected, stage="guard", manifest_path=path, provenance=provenance)
        == report["settings"]["configurations"]["combined"]
    )
    with pytest.raises(ValueError, match="source/native"):
        phase8.load_selection(selected, stage="guard", manifest_path=path, provenance={**provenance, "git_head": "new"})
    report["trials"].pop()
    path.write_text(json.dumps(report))
    with pytest.raises(ValueError, match="changed"):
        phase8.load_selection(selected, stage="guard", manifest_path=path, provenance=provenance)
    with pytest.raises(ValueError, match="schedule"):
        phase8.freeze(path, "combined", tmp_path / "invalid.json")
