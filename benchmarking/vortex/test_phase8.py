from __future__ import annotations

import copy
import json
from pathlib import Path

import pytest

from benchmarking.vortex import phase7, phase8
from benchmarking.vortex.test_phase7 import fake_run as fake_run  # noqa: PLC0414
from benchmarking.vortex.test_profile import capture


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


@pytest.mark.parametrize("cases", [["scan_narrow"], list(phase8.QUERY_CASES)])
def test_cli_accepts_forwarded_remote_cases(fake_run, monkeypatch, cases):
    monkeypatch.setattr(phase8, "require_api", lambda: None)
    assert (
        phase8.main(
            [
                "--manifest",
                str(fake_run.manifest),
                "--output",
                str(fake_run.output),
                "--storage-root",
                "s3://local-test",
                "--cases",
                *cases,
                "--datasets",
                "sorted/multi",
                "--experiments",
                "aa",
                "--sessions",
                "1",
                "--rounds",
                "2",
                "--warmups",
                "0",
                "--diagnostic-rounds",
                "1",
                "--allow-dev",
            ]
        )
        == 0
    )
    report = json.loads((fake_run.output / "session-01/report.json").read_text())
    assert report["settings"]["cases"] == cases
    assert {trial["case"] for trial in report["trials"]} == set(cases)
    assert len(report["trials"]) == len(cases) * 4


def test_cli_rejects_unknown_case(tmp_path):
    with pytest.raises(SystemExit) as error:
        phase8.main(["--output", str(tmp_path / "report"), "--cases", "unknown"])
    assert error.value.code == 2


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
    pairs = phase8.comparisons(["aa", "sessions", "combined", "parquet"])
    used = {name for pair in pairs.values() for name in pair}
    configs = {name: config for name, config in phase8.configurations().items() if name in used}
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
    diagnostics = phase8.remote_eval.base_schedule(cases, datasets, list(configs), 0, 3, 11)
    for trial in diagnostics:
        trial.update(
            status="passed",
            output_sha256="oracle",
            configuration=configs[trial["config"]],
            execution_mode="persistent-process",
            diagnostic=True,
            diagnostics_complete=True,
            profile_complete=True,
            diagnostics=capture(),
        )
    provenance = {
        "git_head": "source",
        "native_binaries_sha256": {"native": "binary"},
        "build_type": "release",
        "build_command": "attested",
        "tracked_changes": False,
    }
    return {
        "experiment": "phase8_paired_diagnosis",
        "status": "passed",
        "provenance_unchanged": True,
        "environment": provenance,
        "environment_after": copy.deepcopy(provenance),
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
        "diagnostics": diagnostics,
        "summary": phase7.summarize(trials, cases, datasets, pairs, rounds),
    }


def test_diagnosis_retains_comparison_order_after_sorted_json_roundtrip():
    report = release_report()
    original_order = list(report["settings"]["comparisons"])
    saved = json.loads(json.dumps(report, sort_keys=True))
    assert list(saved["settings"]["comparisons"]) != original_order
    phase8.validate_diagnosis(saved)


def test_diagnosis_rejects_duplicate_comparison_order():
    report = release_report()
    report["summary"][1] = copy.deepcopy(report["summary"][0])
    with pytest.raises(ValueError, match="exactly once"):
        phase8.validate_diagnosis(report)


def test_diagnosis_rejects_summary_order_that_differs_from_raw_schedule():
    report = release_report()
    report["summary"][:2] = reversed(report["summary"][:2])
    with pytest.raises(ValueError, match="schedule"):
        phase8.validate_diagnosis(report)


@pytest.mark.parametrize("mutation", ["unmeasured", "missing_hash", "changed_hash"])
def test_freeze_rejects_unmeasured_or_unidentified_configuration(tmp_path, mutation):
    report = release_report()
    if mutation == "unmeasured":
        report["settings"]["configurations"]["offsets"] = phase8.configurations()["offsets"]
        report["settings"]["configuration_sha256"]["offsets"] = phase7.fingerprint(phase8.configurations()["offsets"])
    elif mutation == "missing_hash":
        del report["settings"]["configuration_sha256"]["combined"]
    else:
        report["settings"]["configuration_sha256"]["baseline"] = "changed"
    path, selected = tmp_path / "report.json", tmp_path / "selection.json"
    path.write_text(json.dumps(report, sort_keys=True))
    with pytest.raises(ValueError, match="configurations"):
        phase8.freeze(path, "offsets" if mutation == "unmeasured" else "combined", selected)
    assert not selected.exists()


@pytest.mark.parametrize("mutation", ["failed", "missing_time", "nonfinite_time", "output"])
def test_diagnosis_rejects_invalid_warmup_even_when_measured_pairs_pass(mutation):
    report = release_report()
    warmup = next(trial for trial in report["trials"] if trial["warmup"])
    if mutation == "failed":
        warmup["status"] = "failed"
    elif mutation == "missing_time":
        del warmup["wall_seconds"]
    elif mutation == "nonfinite_time":
        warmup["wall_seconds"] = float("nan")
    else:
        warmup["output_sha256"] = "different"
    with pytest.raises(ValueError, match="schedule|outputs"):
        phase8.validate_diagnosis(report)


@pytest.mark.parametrize(
    "mutation",
    [
        "missing",
        "duplicate",
        "reordered",
        "failed",
        "configuration",
        "capture_incomplete",
        "profile_incomplete",
        "output",
    ],
)
def test_diagnosis_rejects_incomplete_or_mismatched_diagnostic_evidence(mutation):
    report = release_report()
    trial = report["diagnostics"][0]
    if mutation == "missing":
        report["diagnostics"].pop()
    elif mutation == "duplicate":
        report["diagnostics"][1] = copy.deepcopy(trial)
    elif mutation == "reordered":
        report["diagnostics"][:2] = reversed(report["diagnostics"][:2])
    elif mutation == "failed":
        trial["status"] = "failed"
    elif mutation == "configuration":
        trial["configuration"] = {"reader": "wrong"}
    elif mutation == "capture_incomplete":
        trial["diagnostics_complete"] = False
    elif mutation == "profile_incomplete":
        trial["profile_complete"] = False
    else:
        trial["output_sha256"] = "different"
    with pytest.raises(ValueError, match="diagnostic schedule|outputs"):
        phase8.validate_diagnosis(report)


@pytest.mark.parametrize(
    "origin,field", [("python", "dropped_events"), ("native", "dropped_events"), ("native", "unfinished_events")]
)
def test_diagnosis_recomputes_capture_completeness_instead_of_trusting_flags(origin, field):
    report = release_report()
    report["diagnostics"][0]["diagnostics"][origin][field] = 1
    with pytest.raises(ValueError, match="complete raw captures"):
        phase8.validate_diagnosis(report)


@pytest.mark.parametrize("mutation", ["debug", "dirty", "unattested", "changed_native"])
def test_diagnosis_requires_unchanged_attested_release_provenance(mutation):
    report = release_report()
    if mutation == "debug":
        report["environment"]["build_type"] = "dev"
        report["environment_after"]["build_type"] = "dev"
    elif mutation == "dirty":
        report["environment"]["tracked_changes"] = True
        report["environment_after"]["tracked_changes"] = True
    elif mutation == "unattested":
        report["environment"]["build_command"] = None
        report["environment_after"]["build_command"] = None
    else:
        report["environment_after"]["native_binaries_sha256"] = {"native": "changed"}
    with pytest.raises(ValueError, match="release remote diagnosis"):
        phase8.validate_diagnosis(report)


def test_frozen_candidate_loads_exact_options_and_rejects_modified_evidence(tmp_path):
    report = release_report()
    path, selected = tmp_path / "report.json", tmp_path / "selection.json"
    path.write_text(json.dumps(report, sort_keys=True))
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
