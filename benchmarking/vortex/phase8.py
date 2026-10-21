"""Independent reader experiments and complete frozen-candidate acceptance."""

from __future__ import annotations

import argparse
import copy
import json
import math
from collections import Counter
from pathlib import Path

from . import phase7, remote_eval
from .fixtures import file_hash
from .metadata_eval import QUERY_CASES
from .run import environment

FLAGS = (
    "enable_large_offsets",
    "enable_execution_session_reuse",
    "enable_file_pruning",
    "enable_layout_splitting",
)
EXPERIMENTS = ("aa", "offsets", "sessions", "pruning", "splits", "combined", "parquet")
OPTION_TYPES = {
    **{key: type(value) for key, value in remote_eval.policy_options("off", 8).items()},
    "enable_row_count_planning": bool,
    **dict.fromkeys(FLAGS, bool),
}


def validate_configuration(config):
    """Require every effective option; reject implicit defaults and unknown knobs."""
    if not isinstance(config, dict) or set(config) != {"reader", "policy", "options"}:
        raise ValueError("Candidate must contain exactly reader, policy and options")
    if config["reader"] != "vortex" or config["policy"] not in remote_eval.POLICIES:
        raise ValueError("Candidate must declare a supported Vortex policy")
    options = config["options"]
    if not isinstance(options, dict) or set(options) != set(OPTION_TYPES):
        raise ValueError("Candidate must freeze every supported option without unknown controls")
    if any(type(options[key]) is not expected for key, expected in OPTION_TYPES.items()):
        raise ValueError("Candidate option types must match exactly, including bool versus int")
    for key, expected in OPTION_TYPES.items():
        if expected is int and options[key] < (0 if key == "read_coalesce_gap_bytes" else 1):
            raise ValueError(f"Invalid candidate budget: {key}")
    if not 1 <= options["discovery_concurrency"] <= 64 or not 1 <= options["metadata_concurrency"] <= 64:
        raise ValueError("Candidate discovery/metadata concurrency must be in 1..64")
    enabled, gap, maximum = remote_eval.POLICIES[config["policy"]]
    if (options["enable_read_coalescing"], options["read_coalesce_gap_bytes"], options["read_coalesce_max_bytes"]) != (
        enabled,
        gap,
        maximum,
    ):
        raise ValueError("Candidate coalescing policy differs from its options")
    return copy.deepcopy(config)


def configurations():
    base = phase7.configurations()["row_count"]
    base["options"].update(dict.fromkeys(FLAGS, False))
    result = {"baseline": base, "baseline_copy": copy.deepcopy(base), "parquet": {"reader": "parquet", "options": {}}}
    for name, flag in zip(("offsets", "sessions", "pruning", "splits"), FLAGS):
        result[name] = copy.deepcopy(base)
        result[name]["options"][flag] = True
    result["combined"] = copy.deepcopy(base)
    result["combined"]["options"].update(dict.fromkeys(FLAGS, True))
    return result


def comparisons(experiments):
    if not experiments or len(set(experiments)) != len(experiments) or not set(experiments) <= set(EXPERIMENTS):
        raise ValueError("Choose unique supported reader experiments")
    result = {}
    for name in experiments:
        if name == "aa":
            result["identical_control"] = ("baseline", "baseline_copy")
        elif name == "parquet":
            result["parquet_vs_baseline"] = ("parquet", "baseline")
            result["parquet_vs_combined"] = ("parquet", "combined")
        else:
            result[name] = ("baseline", name)
    return result


def require_api():
    import inspect

    import daft

    phase7.require_api()
    if not set(FLAGS) <= inspect.signature(daft.read_vortex).parameters.keys() or not all(
        hasattr(daft.daft.VortexSourceConfig, key) for key in FLAGS
    ):
        raise RuntimeError("Rebuild Daft with Phase 8 reader controls")


def order_diagnostics(report):
    """Descriptive order and temporal blocks; no A/A subtraction or significance claim."""
    result = []
    settings = report["settings"]
    for row in report["summary"]:
        pairs = {row["comparison"]: tuple(settings["comparisons"][row["comparison"]])}
        subset = [
            t
            for t in report["trials"]
            if (t["dataset"], t["case"], t["comparison"]) == (row["dataset"], row["case"], row["comparison"])
        ]
        groups = {order: [t for t in subset if t["pair_order"] == order] for order in ("AB", "BA")}
        for start in range(0, settings["rounds"], 10):
            groups[f"rounds_{start}_{min(start + 10, settings['rounds'])}"] = [
                t for t in subset if start <= t["iteration"] < start + 10
            ]
        for group, trials in groups.items():
            # Keep original iteration identities so malformed/missing pairs remain visible.
            summary = phase7.summarize(trials, [row["case"]], [row["dataset"]], pairs, settings["rounds"])[0]
            result.append(
                {
                    "dataset": row["dataset"],
                    "case": row["case"],
                    "comparison": row["comparison"],
                    "group": group,
                    "pairs": summary["complete_pairs"],
                    "paired_ratio": summary.get("paired_B_over_A"),
                }
            )
    return result


def request_ledger(report):
    """Diagnostic SDK sends and adapter ranges; no physical-wire interpretation."""
    result = []
    for trial in report["diagnostics"]:
        events = trial.get("diagnostics", {}).get("native", {}).get("events", [])
        operations = Counter((event["phase"], event["operation"]) for event in events)
        ranges = [event for event in events if event["operation"] in {"range", "cached_range"}]
        result.append(
            {
                "dataset": trial["dataset"],
                "case": trial["case"],
                "config": trial["config"],
                "iteration": trial["iteration"],
                "s3_sdk_head_sends": operations["io", "s3_head_headers"] if report.get("storage_root") else None,
                "s3_sdk_get_sends": operations["io", "s3_get_headers"] if report.get("storage_root") else None,
                "sdk_cpu_tasks": operations["execution", "sdk_cpu_task"],
                "ranges": [
                    {
                        key: event.get(key)
                        for key in ("phase", "operation", "file_id", "offset", "requested_bytes", "returned_bytes")
                    }
                    for event in ranges
                ],
                "scope": "Separate diagnostic query; SDK send envelopes and adapter ranges, not physical retries or wire bytes",
            }
        )
    return result


def validate_diagnosis(report):
    """Rebuild the schedule and summaries before trusting candidate evidence."""
    settings = report["settings"]
    if (
        report.get("experiment") != "phase8_paired_diagnosis"
        or report.get("status") != "passed"
        or not report.get("provenance_unchanged")
        or settings.get("development_only")
        or not str(report.get("storage_root", "")).startswith("s3://")
        or set(settings["cases"]) != set(QUERY_CASES)
    ):
        raise ValueError("Freeze requires a complete successful release remote diagnosis across all five cases")
    phase7.validate(
        settings["cases"],
        settings["datasets"],
        ("aa",),
        settings["rounds"],
        settings["warmups"],
        settings["diagnostic_rounds"],
        600,
        False,
    )
    jobs = phase7.schedule(
        settings["cases"],
        settings["datasets"],
        settings["comparisons"],
        settings["warmups"],
        settings["rounds"],
        settings["seed"],
    )
    configs = settings["configurations"]
    if len(jobs) != len(report["trials"]) or any(
        any(trial.get(key) != value for key, value in job.items())
        or trial.get("configuration") != configs[job["config"]]
        or trial.get("diagnostic", False)
        or trial.get("execution_mode") != "persistent-process"
        for trial, job in zip(report["trials"], jobs)
    ):
        raise ValueError("Diagnosis trials/configurations differ from the complete frozen schedule")
    recomputed = phase7.summarize(
        report["trials"], settings["cases"], settings["datasets"], settings["comparisons"], settings["rounds"]
    )
    if recomputed != report["summary"] or any(row["status"] != "passed" for row in recomputed):
        raise ValueError("Diagnosis summary must match complete paired raw trials")


def freeze(report_path, config_name, output):
    path, output = Path(report_path).resolve(), Path(output).resolve()
    report = json.loads(path.read_text())
    validate_diagnosis(report)
    settings = report["settings"]
    config = validate_configuration(settings["configurations"][config_name])
    if phase7.fingerprint(config) != settings["configuration_sha256"][config_name]:
        raise ValueError("Diagnosis configuration identity differs")
    selected = {
        "selection_version": 2,
        "configuration": config,
        "configuration_sha256": phase7.fingerprint(config),
        "source_revision": report["environment"]["git_head"],
        "native_binaries_sha256": report["environment"]["native_binaries_sha256"],
        "execution_mode": "persistent-process",
        "remote_manifest_sha256": report["manifest_sha256"],
        "diagnosis_report": str(path),
        "diagnosis_report_sha256": file_hash(path),
        "config_name": config_name,
    }
    with output.open("x") as stream:
        stream.write(json.dumps(selected, indent=2, sort_keys=True) + "\n")
    return selected


def load_selection(path, *, stage, manifest_path, provenance):
    selected = json.loads(Path(path).read_text())
    if selected.get("selection_version") != 2 or selected.get("execution_mode") != "persistent-process":
        raise ValueError("Acceptance requires a complete persistent-process candidate selection")
    config = validate_configuration(selected["configuration"])
    if phase7.fingerprint(config) != selected.get("configuration_sha256"):
        raise ValueError("Candidate configuration hash differs")
    report_path = Path(selected["diagnosis_report"])
    if file_hash(report_path) != selected["diagnosis_report_sha256"]:
        raise ValueError("Candidate diagnosis report changed")
    report = json.loads(report_path.read_text())
    validate_diagnosis(report)
    if (
        selected["source_revision"] != report["environment"]["git_head"]
        or selected["native_binaries_sha256"] != report["environment"]["native_binaries_sha256"]
        or selected["remote_manifest_sha256"] != report["manifest_sha256"]
        or report.get("status") != "passed"
        or report.get("provenance_unchanged") is not True
        or report["settings"]["configurations"][selected["config_name"]] != config
        or report["settings"].get("development_only")
    ):
        raise ValueError("Candidate differs from its successful release diagnosis")
    if selected["source_revision"] != provenance.get("git_head") or selected[
        "native_binaries_sha256"
    ] != provenance.get("native_binaries_sha256"):
        raise ValueError("Candidate source/native identity differs")
    if stage == "final" and file_hash(Path(manifest_path)) != selected["remote_manifest_sha256"]:
        raise ValueError("Candidate remote fixture manifest differs")
    return config


def performance_gate(report, stage):
    if report["settings"]["development_only"]:
        return {"status": "not_evaluated", "reason": "development smoke"}
    cases = report["settings"]["cases"]
    datasets = report["settings"]["datasets"]
    failures = []
    comparison, ceiling = ("parquet_vs_candidate", 1.0) if stage == "final" else ("baseline_vs_candidate", 1.05)
    for dataset in datasets:
        for case in cases:
            rows = [
                row
                for row in report["summary"]
                if (row["dataset"], row["case"], row["comparison"]) == (dataset, case, comparison)
            ]
            ratio = rows[0].get("B_over_A_ratio_of_medians") if len(rows) == 1 else None
            if (
                report["status"] != "passed"
                or len(rows) != 1
                or rows[0]["status"] != "passed"
                or not isinstance(ratio, (int, float))
                or not math.isfinite(ratio)
                or (ratio >= ceiling if stage == "final" else ratio > ceiling)
            ):
                failures.append({"dataset": dataset, "case": case, "ratio": ratio})
    return {
        "status": "failed" if failures else "passed",
        "comparison": comparison,
        "ceiling": ceiling,
        "failures": failures,
    }


def run(
    manifest_path,
    output,
    *,
    stage="diagnose",
    selection=None,
    experiments=EXPERIMENTS,
    sessions=3,
    cases=QUERY_CASES,
    datasets=("sorted/multi",),
    seed=811,
    rounds=None,
    warmups=None,
    diagnostic_rounds=3,
    storage_root=None,
    timeout=600,
    allow_dev=False,
):
    require_api()
    if type(sessions) is not int or sessions < 1 or stage not in {"diagnose", "final", "guard"}:
        raise ValueError("Choose a valid stage and a positive session count")
    rounds = (50 if stage == "final" else 20) if rounds is None else rounds
    warmups = (3 if stage == "final" else 1) if warmups is None else warmups
    configs = configurations()
    pairs = comparisons(experiments)
    if stage != "diagnose":
        remote_eval.validate_options(stage, "off", cases, rounds, warmups, allow_dev, timeout)
        if selection is None:
            raise ValueError("Final/local guard requires an explicitly frozen candidate")
        if stage == "final" and (not storage_root or not storage_root.startswith("s3://")):
            raise ValueError("Final comparison requires S3-compatible storage")
        if stage == "guard" and (
            storage_root or set(datasets) != {"sorted/single", "sorted/multi", "shuffled/single", "shuffled/multi"}
        ):
            raise ValueError("Local guard requires all four local layouts")
        configs["candidate"] = load_selection(
            selection, stage=stage, manifest_path=manifest_path, provenance=environment()
        )
        pairs = {"parquet_vs_candidate": ("parquet", "candidate"), "baseline_vs_candidate": ("baseline", "candidate")}
    output = Path(output)
    if output.exists() and (not output.is_dir() or any(output.iterdir())):
        raise FileExistsError("Output must be absent or empty")
    output.mkdir(parents=True, exist_ok=True)
    plan = {
        "stage": stage,
        "sessions": sessions,
        "seeds": [seed + i * 1009 for i in range(sessions)],
        "configurations": configs,
        "comparisons": pairs,
        "selection_sha256": file_hash(Path(selection)) if selection else None,
    }
    (output / "session-plan.json").write_text(json.dumps(plan, indent=2, sort_keys=True) + "\n")
    reports = []
    for index, session_seed in enumerate(plan["seeds"]):
        target = output / f"session-{index + 1:02d}"
        report = phase7.run(
            manifest_path,
            target,
            cases=cases,
            datasets=datasets,
            experiments=("aa",),
            rounds=rounds,
            warmups=warmups,
            diagnostic_rounds=diagnostic_rounds,
            seed=session_seed,
            storage_root=storage_root,
            timeout=timeout,
            allow_dev=allow_dev,
            configuration_set=configs,
            comparison_set=pairs,
            experiment_name="phase8_paired_diagnosis" if stage == "diagnose" else f"phase8_{stage}",
        )
        report["order_diagnostics"] = order_diagnostics(report)
        report["request_ledger"] = request_ledger(report)
        if stage != "diagnose":
            report["performance_gate"] = performance_gate(report, stage)
            if report["performance_gate"]["status"] == "failed":
                report["status"] = "failed"
        (target / "report.json").write_text(json.dumps(report, indent=2, sort_keys=True, allow_nan=False) + "\n")
        (target / "report.md").write_text(phase7.markdown(report))
        reports.append(
            {
                "report": str(target / "report.json"),
                "sha256": file_hash(target / "report.json"),
                "status": report["status"],
                "performance_gate": report["performance_gate"],
            }
        )
        # A failed report is retained. Independent predeclared sessions still run.
    result = {
        "status": "passed" if all(r["status"] == "passed" for r in reports) else "failed",
        "session_plan_sha256": file_hash(output / "session-plan.json"),
        "sessions": reports,
    }
    (output / "sessions.json").write_text(json.dumps(result, indent=2, sort_keys=True) + "\n")
    return result


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--manifest", type=Path)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--stage", choices=("diagnose", "freeze", "final", "guard"), default="diagnose")
    parser.add_argument("--selection", type=Path)
    parser.add_argument("--report", type=Path)
    parser.add_argument("--config", default="combined")
    parser.add_argument("--datasets", nargs="+", default=["sorted/multi"])
    parser.add_argument("--experiments", choices=EXPERIMENTS, nargs="+", default=list(EXPERIMENTS))
    parser.add_argument("--sessions", type=int, default=3)
    parser.add_argument("--seed", type=int, default=811)
    parser.add_argument("--rounds", type=int)
    parser.add_argument("--warmups", type=int)
    parser.add_argument("--diagnostic-rounds", type=int, default=3)
    parser.add_argument("--storage-root")
    parser.add_argument("--timeout", type=float, default=600)
    parser.add_argument("--allow-dev", action="store_true")
    args = vars(parser.parse_args(argv))
    report_path, config_name = args.pop("report"), args.pop("config")
    if args["stage"] == "freeze":
        if report_path is None:
            parser.error("--report is required to freeze a candidate")
        freeze(report_path, config_name, args["output"])
        return 0
    manifest = args.pop("manifest")
    if manifest is None:
        parser.error("--manifest is required")
    return 0 if run(manifest, **args)["status"] == "passed" else 1


if __name__ == "__main__":
    raise SystemExit(main())
