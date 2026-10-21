"""Paired Phase 7 diagnosis: retained connections, fresh queries and no acceptance claims."""

from __future__ import annotations

import argparse
import copy
import hashlib
import inspect
import json
import math
import os
import random
import statistics
from pathlib import Path

from . import profile, remote_eval
from .fixtures import file_hash, load_manifest
from .metadata_eval import QUERY_CASES, range_summary
from .persistent import PersistentWorker
from .run import environment, io_settings, redacted, source_paths

EXPERIMENTS = ("aa", "row-count", "requests", "parquet")
IDENTITY_KEYS = (
    "git_head",
    "tracked_changes",
    "native_binaries_sha256",
    "cargo_lock_sha256",
    "harness_sha256",
    "build_command",
    "build_type",
)


def require_api():
    """Fail before service provisioning when Python/native Phase 7 APIs differ."""
    import daft

    remote_eval.require_api()
    bindings = ("_vortex_diagnostics_clock", "_read_vortex_row_counts_bulk", "_read_vortex_row_counts_bulk_cached")
    if "enable_row_count_planning" not in inspect.signature(daft.read_vortex).parameters or not all(
        callable(getattr(daft.daft, name, None)) for name in bindings
    ):
        raise RuntimeError("Rebuild the native extension with Phase 7 shared-clock diagnostics and row-count planning")


def fingerprint(configuration):
    return hashlib.sha256(json.dumps(configuration, sort_keys=True, separators=(",", ":")).encode()).hexdigest()


def configurations():
    baseline = {"reader": "vortex", "policy": "off", "options": remote_eval.policy_options("off", 8)}
    baseline["options"]["enable_row_count_planning"] = False
    result = {"baseline": baseline, "baseline_copy": copy.deepcopy(baseline)}
    result["row_count"] = copy.deepcopy(baseline)
    result["row_count"]["options"]["enable_row_count_planning"] = True
    for cap in (1, 2, 4):
        result[f"requests_{cap}"] = copy.deepcopy(baseline)
        result[f"requests_{cap}"]["options"]["max_concurrent_requests"] = cap
    result["parquet"] = {"reader": "parquet", "options": {}}
    return result


def comparisons(experiments):
    result = {}
    if "aa" in experiments:
        result["identical_control"] = ("baseline", "baseline_copy")
    if "row-count" in experiments:
        result["lightweight_planning"] = ("baseline", "row_count")
    if "requests" in experiments:
        for cap in (1, 2, 4):
            result[f"requests_8_vs_{cap}"] = ("baseline", f"requests_{cap}")
    if "parquet" in experiments:
        result["parquet_vs_baseline"] = ("parquet", "baseline")
        result["parquet_vs_row_count"] = ("parquet", "row_count")
    return result


def schedule(cases, datasets, pairs, warmups, rounds, seed):
    """Adjacent pairs; each measured comparison has exactly half AB and half BA."""
    if rounds % 2:
        raise ValueError("Paired rounds must be even to balance AB and BA exactly")
    rng = random.Random(seed)
    keys = [(dataset, case, pair) for dataset in datasets for case in cases for pair in pairs]
    orders = {}
    for key in keys:
        measured = [False] * (rounds // 2) + [True] * (rounds // 2)
        rng.shuffle(measured)
        orders[key] = measured
    result = []
    pair_id = 0
    for iteration in range(-warmups, rounds):
        blocks = list(keys)
        rng.shuffle(blocks)
        for dataset, case, comparison in blocks:
            reverse = bool(rng.getrandbits(1)) if iteration < 0 else orders[(dataset, case, comparison)][iteration]
            order = (1, 0) if reverse else (0, 1)
            for position, member in enumerate(order):
                result.append(
                    {
                        "dataset": dataset,
                        "case": case,
                        "config": pairs[comparison][member],
                        "comparison": comparison,
                        "pair_id": pair_id,
                        "pair_position": position,
                        "pair_member": "B" if member else "A",
                        "pair_order": "BA" if reverse else "AB",
                        "iteration": iteration,
                        "warmup": iteration < 0,
                    }
                )
            pair_id += 1
    return result


def summarize(trials, cases, datasets, pairs, rounds):
    rows = []
    for dataset in datasets:
        for case in cases:
            for comparison, (first, second) in pairs.items():
                subset = [
                    trial
                    for trial in trials
                    if not trial["warmup"]
                    and (trial["dataset"], trial["case"], trial["comparison"]) == (dataset, case, comparison)
                ]
                valid, failed, mismatched = [], [], []
                for iteration in range(rounds):
                    members = [trial for trial in subset if trial["iteration"] == iteration]
                    by_member = {trial["pair_member"]: trial for trial in members}
                    ok = len(members) == 2 and set(by_member) == {"A", "B"}
                    if ok:
                        a, b = by_member["A"], by_member["B"]
                        ok = all(
                            trial["status"] == "passed"
                            and isinstance(trial.get("wall_seconds"), (int, float))
                            and not isinstance(trial["wall_seconds"], bool)
                            and math.isfinite(trial["wall_seconds"])
                            and trial["wall_seconds"] > 0
                            for trial in (a, b)
                        )
                    if ok and a["output_sha256"] != b["output_sha256"]:
                        mismatched.append(iteration)
                        ok = False
                    if ok:
                        valid.append((a, b))
                    else:
                        failed.append(iteration)
                row = {
                    "dataset": dataset,
                    "case": case,
                    "comparison": comparison,
                    "A": first,
                    "B": second,
                    "status": "passed" if len(valid) == rounds and len(subset) == rounds * 2 else "failed",
                    "complete_pairs": len(valid),
                    "failed_pair_iterations": failed,
                    "output_mismatch_pair_iterations": mismatched,
                    "expected_pairs": rounds,
                }
                if valid:
                    a_values, b_values = ([pair[index]["wall_seconds"] for pair in valid] for index in (0, 1))
                    row.update(
                        A_wall_seconds=range_summary(a_values),
                        B_wall_seconds=range_summary(b_values),
                        B_over_A_ratio_of_medians=statistics.median(b_values) / statistics.median(a_values),
                        paired_B_over_A=range_summary([b / a for a, b in zip(a_values, b_values)]),
                        paired_B_minus_A_seconds=range_summary([b - a for a, b in zip(a_values, b_values)]),
                        B_faster_pairs=sum(b < a for a, b in zip(a_values, b_values)),
                        measured_AB_pairs=sum(a["pair_order"] == "AB" for a, _ in valid),
                        measured_BA_pairs=sum(a["pair_order"] == "BA" for a, _ in valid),
                    )
                rows.append(row)
    return rows


def validate(cases, datasets, experiments, rounds, warmups, diagnostic_rounds, timeout, allow_dev):
    for values, supported, label in ((cases, QUERY_CASES, "cases"), (experiments, EXPERIMENTS, "experiments")):
        if not values or len(set(values)) != len(values) or not set(values) <= set(supported):
            raise ValueError(f"Choose unique supported {label}")
    if not datasets or len(set(datasets)) != len(datasets):
        raise ValueError("Choose unique fixture datasets")
    if type(rounds) is not int or rounds < (2 if allow_dev else 20) or rounds % 2:
        raise ValueError("Measured rounds must be even and at least 20 (2 for development smoke)")
    if type(warmups) is not int or warmups < (0 if allow_dev else 1):
        raise ValueError("At least one warmup is required outside development smoke")
    if type(diagnostic_rounds) is not int or diagnostic_rounds < (1 if allow_dev else 3):
        raise ValueError("At least three separate diagnostic repetitions are required")
    if isinstance(timeout, bool) or not math.isfinite(timeout) or timeout <= 0:
        raise ValueError("timeout must be finite and positive")


def markdown(report):
    lines = [
        f"# {report['experiment']}",
        "",
        f"Report status: {report['status']}; performance gate: {report['performance_gate']['status']}.",
        "Fresh sources and complete queries share one process and its connections. Source creation remains timed.",
        "Pairs are adjacent with balanced AB/BA order. Ratios below one favor B; no significance claim is made.",
        "",
        "| Dataset | Case | Comparison (B / A) | Complete pairs | Ratio of medians | Median paired ratio | B faster pairs |",
        "| --- | --- | --- | ---: | ---: | ---: | ---: |",
    ]
    for row in report["summary"]:
        if "paired_B_over_A" in row:
            lines.append(
                f"| {row['dataset']} | {row['case']} | {row['comparison']} | {row['complete_pairs']} | "
                f"{row['B_over_A_ratio_of_medians']:.3f} | {row['paired_B_over_A']['median']:.3f} | {row['B_faster_pairs']} |"
            )
        else:
            lines.append(
                f"| {row['dataset']} | {row['case']} | {row['comparison']} | 0 | unavailable | unavailable | 0 |"
            )
    lines += [
        "",
        "Identical controls measure order and run variability; they do not grant permission to relax existing acceptance thresholds.",
        "All failed samples remain in the report. Instrumented repetitions run after all performance trials and never enter these medians.",
        "Request caps are per file; eight concurrently active files can create eight times the configured logical request concurrency.",
        "No candidate is automatically selected from this diagnosis. Freeze any follow-up candidate before independent acceptance.",
        "",
    ]
    lines += [
        "## Separate diagnostic repetitions",
        "",
        "Each link contains lifecycle accounting, layer coverage, original operator counters and a Chrome/Perfetto timeline.",
        "",
        "| Dataset | Case | Configuration | Repetition | Profile |",
        "| --- | --- | --- | ---: | --- |",
    ]
    for trial in report["diagnostics"]:
        target = trial.get("profile")
        link = f"[accounting]({Path(target).with_name('index.html').as_posix()})" if target else "unavailable"
        lines.append(f"| {trial['dataset']} | {trial['case']} | {trial['config']} | {trial['iteration']} | {link} |")
    lines.append("")
    return "\n".join(lines)


def run(
    manifest_path,
    output,
    *,
    cases=QUERY_CASES,
    datasets=("sorted/multi",),
    experiments=EXPERIMENTS,
    rounds=20,
    warmups=1,
    diagnostic_rounds=3,
    seed=173,
    storage_root=None,
    timeout=600,
    allow_dev=False,
    configuration_set=None,
    comparison_set=None,
    experiment_name="phase7_paired_diagnosis",
):
    validate(cases, datasets, experiments, rounds, warmups, diagnostic_rounds, timeout, allow_dev)
    manifest_path, output = Path(manifest_path).resolve(), Path(output).resolve()
    manifest = load_manifest(manifest_path)
    if not set(datasets) <= set(manifest["datasets"]):
        raise ValueError("Selected datasets are absent from the fixture manifest")
    for dataset in datasets:
        source_paths(manifest_path, manifest["datasets"][dataset], "vortex_11", storage_root)
        source_paths(manifest_path, manifest["datasets"][dataset], "parquet", storage_root)
    io_settings()
    require_api()
    provenance = environment()
    if not allow_dev and (
        provenance.get("build_type") != "release"
        or not provenance.get("build_command")
        or not provenance.get("native_binaries_sha256")
        or not provenance.get("git_head")
        or provenance.get("tracked_changes") is not False
    ):
        raise ValueError("Diagnosis requires clean, attested release/native provenance")
    if output.exists() and (not output.is_dir() or any(output.iterdir())):
        raise FileExistsError("Output must be absent or empty; previous results are preserved")
    output.mkdir(parents=True, exist_ok=True)
    for directory in ("trials", "diagnostics", "profiles", "plans"):
        (output / directory).mkdir()
    pairs = comparisons(experiments) if comparison_set is None else comparison_set
    available = configurations() if configuration_set is None else configuration_set
    if not pairs or any(len(pair) != 2 or any(name not in available for name in pair) for pair in pairs.values()):
        raise ValueError("Each comparison must reference exactly two declared configurations")
    used = {name for pair in pairs.values() for name in pair}
    configs = {name: config for name, config in available.items() if name in used}
    settings = {
        "cases": list(cases),
        "datasets": list(datasets),
        "experiments": list(experiments),
        "rounds": rounds,
        "warmups": warmups,
        "diagnostic_rounds": diagnostic_rounds,
        "seed": seed,
        "execution_mode": "persistent-process",
        "configurations": configs,
        "configuration_sha256": {name: fingerprint(config) for name, config in configs.items()},
        "comparisons": {key: list(value) for key, value in pairs.items()},
        "development_only": allow_dev,
    }
    frozen = output / "frozen-experiment.json"
    frozen.write_text(json.dumps(settings, indent=2, sort_keys=True) + "\n")
    report = {
        "report_version": 1,
        "experiment": experiment_name,
        "status": "running",
        "purpose": "development correctness smoke" if allow_dev else "paired exploratory diagnosis; not acceptance",
        "performance_gate": {"status": "not_evaluated", "reason": "Existing acceptance thresholds remain unchanged"},
        "environment": provenance,
        "manifest": str(manifest_path),
        "manifest_sha256": file_hash(manifest_path),
        "storage_root": storage_root,
        "settings": settings,
        "frozen_experiment_sha256": file_hash(frozen),
        "trials": [],
        "diagnostics": [],
    }
    report_path = output / "report.json"

    def checkpoint():
        report_path.write_text(redacted(json.dumps(report, indent=2, sort_keys=True, allow_nan=False)) + "\n")

    checkpoint()
    jobs = schedule(cases, datasets, pairs, warmups, rounds, seed)
    child_env = {**os.environ, "DAFT_RUNNER": "native", "DAFT_PROGRESS_BAR": "0", "DAFT_ANALYTICS_ENABLED": "0"}
    with PersistentWorker(output, child_env) as session:
        for index, entry in enumerate(jobs):
            spec = {
                **entry,
                "configuration": configs[entry["config"]],
                "manifest": str(manifest_path),
                "storage_root": storage_root,
                "allow_dev": allow_dev,
                "diagnostic": False,
                "execution_mode": "persistent-process",
            }
            report["trials"].append(remote_eval.execute_child(spec, index, output, child_env, timeout, session))
            if (index + 1) % (len(cases) * len(datasets) * len(pairs) * 2) == 0:
                checkpoint()
            print(f"Performance {index + 1}/{len(jobs)}: {entry['comparison']} {entry['case']}", flush=True)
        diagnostic_jobs = remote_eval.base_schedule(cases, datasets, list(configs), 0, diagnostic_rounds, seed)
        for index, entry in enumerate(diagnostic_jobs):
            spec = {
                **entry,
                "configuration": configs[entry["config"]],
                "manifest": str(manifest_path),
                "storage_root": storage_root,
                "allow_dev": allow_dev,
                "diagnostic": True,
                "execution_mode": "persistent-process",
            }
            result = remote_eval.execute_child(spec, index, output, child_env, timeout, session)
            if result["status"] == "passed":
                try:
                    accounting = profile.export(
                        result["diagnostics"], output / "profiles" / f"{index:05d}", result.get("operator_metrics")
                    )
                    result["profile"] = f"profiles/{index:05d}/accounting.json"
                    result["profile_complete"] = accounting["complete"]
                except (KeyError, ValueError, TypeError, OSError) as error:
                    result["profile_complete"] = False
                    result["profile_error"] = redacted(str(error))
            report["diagnostics"].append(result)
            print(f"Diagnostics {index + 1}/{len(diagnostic_jobs)}: {entry['config']} {entry['case']}", flush=True)
        report["worker_session"] = {"pid": session.process.pid if session.process else None}
    report["worker_session"].update(
        started_at_utc=session.started_at_utc,
        completed_at_utc=session.completed_at_utc,
        returncode=session.process.returncode if session.process else None,
        failure=session.failure,
    )
    report["environment_after"] = environment()
    report["provenance_unchanged"] = all(
        provenance.get(key) == report["environment_after"].get(key) for key in IDENTITY_KEYS
    )
    checks = {}
    for name, path, digest in (
        ("manifest", manifest_path, report["manifest_sha256"]),
        ("frozen_experiment", frozen, report["frozen_experiment_sha256"]),
    ):
        try:
            checks[name] = {"unchanged": file_hash(path) == digest}
        except OSError as error:
            checks[name] = {"unchanged": False, "error": redacted(str(error))}
    try:
        # The manifest hash alone does not prove its referenced fixture bytes
        # stayed intact. Recheck payload hashes after all timers have stopped.
        load_manifest(manifest_path)
        checks["local_fixture_inventory"] = {"unchanged": checks["manifest"]["unchanged"]}
    except (OSError, ValueError, KeyError, TypeError) as error:
        checks["local_fixture_inventory"] = {"unchanged": False, "error": redacted(str(error))}
    report["artifact_checks_after"] = checks
    report["provenance_unchanged"] = report["provenance_unchanged"] and all(
        check["unchanged"] for check in checks.values()
    )
    report["summary"] = summarize(report["trials"], cases, datasets, pairs, rounds)
    report["status"] = (
        "passed"
        if report["provenance_unchanged"]
        and session.failure is None
        and all(trial["status"] == "passed" for trial in report["trials"])
        and all(row["status"] == "passed" for row in report["summary"])
        and all(
            trial["status"] == "passed" and trial.get("diagnostics_complete") and trial.get("profile_complete")
            for trial in report["diagnostics"]
        )
        else "failed"
    )
    checkpoint()
    (output / "report.md").write_text(markdown(report))
    return report


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--manifest", required=True, type=Path)
    parser.add_argument("--output", required=True, type=Path)
    parser.add_argument("--cases", nargs="+", choices=QUERY_CASES, default=list(QUERY_CASES))
    parser.add_argument("--datasets", nargs="+", default=["sorted/multi"])
    parser.add_argument("--experiments", nargs="+", choices=EXPERIMENTS, default=list(EXPERIMENTS))
    parser.add_argument("--rounds", type=int, default=20)
    parser.add_argument("--warmups", type=int, default=1)
    parser.add_argument("--diagnostic-rounds", type=int, default=3)
    parser.add_argument("--seed", type=int, default=173)
    parser.add_argument("--storage-root")
    parser.add_argument("--timeout", type=float, default=600)
    parser.add_argument("--allow-dev", action="store_true")
    args = parser.parse_args()
    try:
        report = run(
            args.manifest,
            args.output,
            **{key: value for key, value in vars(args).items() if key not in {"manifest", "output"}},
        )
    except (ValueError, OSError, RuntimeError, AssertionError) as error:
        parser.exit(1, redacted(str(error)) + "\n")
    return 0 if report["status"] == "passed" else 1


if __name__ == "__main__":
    raise SystemExit(main())
