"""Same-build remote read-policy tuning and a frozen all-query acceptance gate.

Diagnostics execute separately after performance trials and never enter timing
summaries. Existing Phase 4/5 fixtures, oracles and reports are left unchanged.
"""

from __future__ import annotations

import argparse
import io
import json
import math
import os
import resource
import statistics
import subprocess
import sys
import time
from contextlib import nullcontext
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from .fixtures import cutoff, file_hash, load_manifest
from .metadata_eval import QUERY_CASES, range_summary
from .run import check_output, environment, file_read_telemetry, io_config, io_settings, redacted, source_paths
from .run import schedule as base_schedule

POLICIES = {
    "off": (False, 65536, 4194304),
    "16k_1m": (True, 16384, 1048576),
    "64k_4m": (True, 65536, 4194304),
    "256k_4m": (True, 262144, 4194304),
}
DEFAULT_POLICY = "64k_4m"
RANKING = "worst case Vortex/Parquet median; geometric mean wall seconds; total source bytes; config id"


def load_selection(path, allow_dev=False):
    path = Path(path)
    selected = json.loads(path.read_text())
    if (
        not isinstance(selected, dict)
        or selected.get("selection_version") != 1
        or selected.get("discovery_concurrency") != 8
        or selected.get("metadata_concurrency") != 8
        or not isinstance(selected.get("policy"), str)
        or selected["policy"] not in POLICIES
    ):
        raise ValueError("Selection must freeze one supported policy at discovery 8 and metadata 8")
    if not allow_dev and selected.get("development_only", True):
        raise ValueError("Final performance selection must come from a production tuning run")
    report_path = path.parent / "report.json"
    if not report_path.is_file() or file_hash(report_path) != selected.get("tuning_report_sha256"):
        raise ValueError("Selection must match the adjacent tuning report SHA256")
    report = json.loads(report_path.read_text())
    try:
        settings, provenance = report["settings"], report["environment"]
        if (
            report["experiment"] != "remote_read_policy"
            or settings["stage"] != "tune"
            or report["status"] != "passed"
            or report["correctness_status"] != "passed"
            or report["provenance_unchanged"] is not True
        ):
            raise ValueError("Selection requires a completed successful tuning report")
        validate_options(
            "tune", selected["policy"], settings["cases"], settings["rounds"], settings["warmups"], allow_dev, 600
        )
        if not allow_dev and (
            report.get("purpose") != "same-build tune remote policy comparison"
            or provenance["build_type"] != "release"
            or provenance["tracked_changes"] is not False
            or not provenance["git_head"]
            or not provenance["native_binaries_sha256"]
            or not report["storage_root"]
            or not report["storage_root"].startswith("s3://")
        ):
            raise ValueError("Selection requires clean release/native provenance and remote storage")
        configs = configurations("tune", DEFAULT_POLICY)
        if json.dumps(settings["configurations"], sort_keys=True) != json.dumps(configs, sort_keys=True):
            raise ValueError("Tuning report policy controls differ from the declared grid")
        jobs = base_schedule(
            settings["cases"],
            settings["datasets"],
            list(configs),
            settings["warmups"],
            settings["rounds"],
            settings["seed"],
        )
        trials = report["trials"]
        if len(trials) != len(jobs) or any(
            {key: trial[key] for key in job} != job
            or trial["status"] != "passed"
            or trial.get("diagnostic", False)
            or not isinstance(trial["wall_seconds"], (int, float))
            or isinstance(trial["wall_seconds"], bool)
            or not math.isfinite(trial["wall_seconds"])
            or trial["wall_seconds"] <= 0
            for trial, job in zip(trials, jobs)
        ):
            raise ValueError("Tuning report has missing, failed or invalid seeded trials")
        summary = summarize(trials, settings["datasets"], settings["cases"], configs, settings["rounds"])
        ranking = rank_policies(summary, configs)
        if summary != report["summary"] or ranking != report["ranking"]:
            raise ValueError("Tuning report summary/ranking differs from its measured trials")
        eligible = [row for row in ranking if row["discovery_concurrency"] == 8]
        if not eligible:
            raise ValueError("Tuning report has no eligible discovery-8 policy")
        winner = eligible[0]
        if (
            selected["policy"] != winner["policy"]
            or selected.get("config") != winner["config"]
            or selected.get("criterion") != RANKING
            or json.dumps(selected.get("configuration"), sort_keys=True)
            != json.dumps(configs[winner["config"]], sort_keys=True)
        ):
            raise ValueError("Selection must match the recorded winning policy and exact controls")
        if (
            selected.get("source_revision") != provenance.get("git_head")
            or selected.get("native_binaries_sha256") != provenance.get("native_binaries_sha256")
            or selected.get("manifest_sha256") != report["manifest_sha256"]
        ):
            raise ValueError("Selection provenance differs from the tuning report")
    except (KeyError, TypeError, AttributeError, AssertionError) as error:
        raise ValueError("Malformed or inconsistent tuning report") from error
    return selected


def validate_selection_identity(selected, provenance, manifest_sha256, stage):
    if selected["source_revision"] != provenance.get("git_head") or selected[
        "native_binaries_sha256"
    ] != provenance.get("native_binaries_sha256"):
        raise ValueError("Frozen selection source/native build differs from this comparison")
    if stage == "final" and selected["manifest_sha256"] != manifest_sha256:
        raise ValueError("Frozen selection remote fixture manifest differs from final comparison")


def policy_options(policy: str, discovery: int) -> dict[str, Any]:
    enabled, gap, maximum = POLICIES[policy]
    return {
        "metadata_concurrency": 8,
        "discovery_concurrency": discovery,
        "enable_read_coalescing": enabled,
        "read_coalesce_gap_bytes": gap,
        "read_coalesce_max_bytes": maximum,
        "batch_size": 131072,
        "max_concurrent_requests": 8,
        "max_in_flight_bytes": 67108864,
        "enable_projection_pushdown": True,
        "enable_filter_pushdown": True,
    }


def configurations(stage: str, candidate_policy: str) -> dict[str, dict[str, Any]]:
    result = {"parquet": {"reader": "parquet", "options": {}}}
    if stage == "tune":
        result.update(
            {
                f"{policy}_discovery{cap}": {
                    "reader": "vortex",
                    "policy": policy,
                    "options": policy_options(policy, cap),
                }
                for policy in POLICIES
                for cap in (1, 8)
            }
        )
    else:
        result.update(
            {
                "phase5": {"reader": "vortex", "policy": "off", "options": policy_options("off", 1)},
                "candidate": {
                    "reader": "vortex",
                    "policy": candidate_policy,
                    "options": policy_options(candidate_policy, 8),
                },
            }
        )
    return result


def validate_options(stage, candidate_policy, cases, rounds, warmups, allow_dev, timeout):
    if stage not in {"tune", "final", "guard"} or candidate_policy not in POLICIES:
        raise ValueError("Choose a supported stage and candidate policy")
    if not cases or len(set(cases)) != len(cases) or not set(cases) <= set(QUERY_CASES):
        raise ValueError("Select unique supported query cases")
    if not allow_dev and set(cases) != set(QUERY_CASES):
        raise ValueError("Performance comparisons require all five frozen query cases")
    if type(rounds) is not int or type(warmups) is not int:
        raise ValueError("rounds and warmups must be integers")
    if allow_dev:
        if rounds < 1 or warmups < 0:
            raise ValueError("Development smoke requires rounds >=1 and warmups >=0")
    elif stage == "final":
        if rounds != 50 or warmups != 3:
            raise ValueError("Final acceptance requires exactly 50 measured rounds and 3 warmups")
    elif rounds < 20 or warmups < 1:
        raise ValueError("Tuning/local guard requires at least 20 measured rounds and 1 warmup")
    if isinstance(timeout, bool) or not math.isfinite(timeout) or timeout <= 0:
        raise ValueError("timeout must be finite and positive")


def require_api():
    import inspect

    import daft

    names = set(inspect.signature(daft.read_vortex).parameters)
    required = {
        "enable_read_coalescing",
        "read_coalesce_gap_bytes",
        "read_coalesce_max_bytes",
        "discovery_concurrency",
        "metadata_concurrency",
    }
    bindings = ("read_vortex_metadata_bulk", "_io_glob_bulk", "_vortex_diagnostics_start", "_vortex_diagnostics_finish")
    if not required <= names or not all(hasattr(daft.daft, name) for name in bindings):
        raise RuntimeError("Requires a Phase 6 native Vortex build with coalescing, discovery and diagnostics")
    try:
        config = daft.daft.VortexSourceConfig(
            enable_read_coalescing=True, read_coalesce_gap_bytes=65536, read_coalesce_max_bytes=4194304
        )
        if config.read_coalesce_max_bytes != 4194304:
            raise RuntimeError("Native Vortex configuration differs from requested controls")
    except (TypeError, AttributeError) as error:
        raise RuntimeError("Rebuild the native extension with Phase 6 Vortex controls") from error


def query(spec, manifest):
    import daft
    from daft import col

    config = spec["configuration"]
    paths = source_paths(
        Path(spec["manifest"]),
        manifest["datasets"][spec["dataset"]],
        "parquet" if config["reader"] == "parquet" else "vortex_11",
        spec.get("storage_root"),
    )
    frame = (
        daft.read_parquet(paths, io_config=io_config())
        if config["reader"] == "parquet"
        else daft.read_vortex(paths, io_config=io_config(), **config["options"])
    )
    case = spec["case"]
    if case == "range_0.01":
        frame = frame.where(col("id") < daft.lit(cutoff(manifest["generator"]["rows"], "0.01")))
    elif case == "eq_0.01":
        frame = frame.where(col("sel_001") == daft.lit(1))
    elif case == "join":
        dimension = daft.from_pydict({"group": list(range(128)), "weight": list(range(1, 129))})
        return frame.join(dimension, on="group").agg(
            col("id").count().alias("rows"), (col("value") * col("weight")).sum().alias("weighted_sum")
        )
    reductions = [
        col("id").count().alias("rows"),
        col("id").sum().alias("sum_id"),
        col("value").count().alias("count_value"),
        col("value").sum().alias("sum_value"),
    ]
    if case == "scan_full":
        reductions += [
            col("group").sum().alias("sum_group"),
            col("flag").count().alias("count_flag"),
            col("flag").cast(daft.DataType.int64()).sum().alias("sum_flag"),
            col("label").length().sum().alias("label_length"),
            col("text").length().sum().alias("text_length"),
            *[col(name).sum().alias(name) for name in ("sel_0001", "sel_001", "sel_05", "constant")],
        ]
    return frame.agg(*reductions)


def worker(spec):
    import daft
    from daft.context import get_context

    require_api()
    daft.set_runner_native()
    if daft.daft.build_type() != "release" and not spec["allow_dev"]:
        raise ValueError("Comparisons require an attested release build")
    manifest = json.loads(Path(spec["manifest"]).read_text())
    daft.from_pydict({"x": [1]}).agg(daft.col("x").sum()).collect()
    diagnostic = spec.get("diagnostic", False)
    trace = None
    recorder = None
    if diagnostic:
        from daft.io import _vortex_diagnostics

        recorder = _vortex_diagnostics
        recorder.start()
    cpu, began = time.process_time_ns(), time.perf_counter_ns()
    execution_error = None
    try:
        with recorder.stage("source_creation") if recorder is not None else nullcontext():
            frame = query(spec, manifest)
        with recorder.stage("collection") if recorder is not None else nullcontext():
            frame.collect()
    except Exception as error:
        execution_error = error
    finally:
        wall, cpu_elapsed = time.perf_counter_ns() - began, time.process_time_ns() - cpu
        if recorder is not None:
            trace = recorder.finish()
    if execution_error is not None:
        if not diagnostic:
            raise execution_error
        return {"status": "failed", "error": redacted(str(execution_error)), "diagnostic": True, "diagnostics": trace}
    rss = int(resource.getrusage(resource.RUSAGE_SELF).ru_maxrss * (1 if sys.platform == "darwin" else 1024))
    # Cached output validation and separate explain are outside both timer and recorder.
    actual = frame.to_arrow().to_pylist()
    digest = check_output(actual, manifest["oracle"][spec["case"]])
    metrics = frame.metrics.to_pylist()
    for metric in metrics:
        metric["duration_seconds"] = metric.pop("duration").total_seconds()
    plan = io.StringIO()
    query(spec, manifest).explain(show_all=True, file=plan)
    cfg = get_context().daft_execution_config
    result = {
        "status": "passed",
        "output": actual,
        "output_sha256": digest,
        "build_type": daft.daft.build_type(),
        "operator_metrics": metrics,
        "plan": redacted(plan.getvalue()),
        "diagnostic": diagnostic,
        "read_requests": None,
        "read_requests_unavailable_reason": "Query SourceSnapshot does not export request counts; separate diagnostics are not performance trials",
        "effective_settings": {
            "scantask_max_parallel": cfg.scantask_max_parallel,
            "maintain_order": cfg.maintain_order,
            "default_morsel_size": cfg.default_morsel_size,
            "enable_scan_task_split_and_merge": cfg.enable_scan_task_split_and_merge,
        },
        **file_read_telemetry(
            metrics,
            config="parquet" if spec["configuration"]["reader"] == "parquet" else "vortex_11",
            storage_root=spec.get("storage_root"),
        ),
    }
    if diagnostic:
        complete = all(
            type(value) is int and value == 0
            for value in (
                trace.get("native", {}).get("unfinished_events"),
                trace.get("native", {}).get("dropped_events"),
                trace.get("python", {}).get("dropped_events"),
            )
        )
        result.update(
            diagnostics=trace,
            diagnostics_complete=complete,
            diagnostics_coverage=(
                "source_creation_and_collection_only; native Parquet phases and request counters are not instrumented"
                if spec["configuration"]["reader"] == "parquet"
                else "Python discovery/source/collection and native Vortex events; physical retries unavailable"
            ),
            measurement_scope="separate executed-query diagnostic; no performance claim",
        )
    else:
        result.update(
            wall_seconds=wall / 1e9,
            cpu_seconds=cpu_elapsed / 1e9,
            peak_rss_bytes=rss,
            measurement_scope="fresh source/discovery/schema/planning through complete aggregate collection",
        )
    return result


def summarize(trials, datasets, cases, configs, rounds):
    result = []
    for dataset in datasets:
        for case in cases:
            rows = []
            for config in configs:
                values = [
                    t
                    for t in trials
                    if not t["warmup"]
                    and not t.get("diagnostic", False)
                    and (t["dataset"], t["case"], t["config"]) == (dataset, case, config)
                ]
                passed = [t for t in values if t["status"] == "passed"]
                complete = len(values) == rounds and {t["iteration"] for t in values} == set(range(rounds))
                row = {
                    "dataset": dataset,
                    "case": case,
                    "config": config,
                    "trials": len(values),
                    "status": "passed" if complete and len(passed) == rounds else "failed",
                }
                if passed:
                    digests = {t["output_sha256"] for t in passed}
                    if len(digests) != 1:
                        raise AssertionError("Repeated correctness digests differ")
                    row["output_sha256"] = next(iter(digests))
                    row["metrics"] = {
                        name: range_summary([t.get(name) for t in passed])
                        for name in ("wall_seconds", "cpu_seconds", "peak_rss_bytes", "source_read_bytes")
                    }
                rows.append(row)
            parquet = rows[0]
            for row in rows[1:]:
                if row["status"] == parquet["status"] == "passed":
                    if row["output_sha256"] != parquet["output_sha256"]:
                        raise AssertionError("Configurations produced different outputs")
                    p, v = parquet["metrics"]["wall_seconds"], row["metrics"]["wall_seconds"]
                    row.update(
                        vortex_over_parquet=v["median"] / p["median"],
                        sample_ranges_overlap=max(p["min"], v["min"]) <= min(p["max"], v["max"]),
                    )
                    lookup = {
                        t["iteration"]: t["wall_seconds"]
                        for t in trials
                        if not t["warmup"]
                        and not t.get("diagnostic", False)
                        and t["status"] == "passed"
                        and (t["dataset"], t["case"], t["config"]) == (dataset, case, "parquet")
                    }
                    row["faster_paired_rounds"] = sum(
                        t["wall_seconds"] < lookup[t["iteration"]]
                        for t in trials
                        if not t["warmup"]
                        and not t.get("diagnostic", False)
                        and t["status"] == "passed"
                        and (t["dataset"], t["case"], t["config"]) == (dataset, case, row["config"])
                    )
            baseline = next((row for row in rows if row["config"] == "phase5"), None)
            if baseline is not None and baseline["status"] == "passed":
                for row in rows:
                    if row["status"] == "passed":
                        row["time_over_phase5"] = (
                            row["metrics"]["wall_seconds"]["median"] / baseline["metrics"]["wall_seconds"]["median"]
                        )
            result.extend(rows)
    return result


def rank_policies(summary, configs):
    ranking = []
    for config, setting in configs.items():
        if setting["reader"] != "vortex":
            continue
        rows = [row for row in summary if row["config"] == config]
        if not rows or any(row["status"] != "passed" or "vortex_over_parquet" not in row for row in rows):
            continue
        times = [row["metrics"]["wall_seconds"]["median"] for row in rows]
        byte_values = [row["metrics"]["source_read_bytes"]["median"] for row in rows]
        ranking.append(
            {
                "config": config,
                "policy": setting["policy"],
                "discovery_concurrency": setting["options"]["discovery_concurrency"],
                "worst_vortex_over_parquet": max(row["vortex_over_parquet"] for row in rows),
                "geomean_wall_seconds": math.exp(statistics.mean(math.log(value) for value in times)),
                "total_source_bytes": sum(byte_values) if all(value is not None for value in byte_values) else None,
            }
        )
    return sorted(
        ranking,
        key=lambda row: (
            row["worst_vortex_over_parquet"],
            row["geomean_wall_seconds"],
            row["total_source_bytes"] if row["total_source_bytes"] is not None else math.inf,
            row["config"],
        ),
    )


def performance_gate(summary, *, stage, allow_dev, all_trials_passed):
    if stage not in {"final", "guard"} or allow_dev:
        return {"status": "not_evaluated", "reason": "Tuning/development runs cannot satisfy final acceptance"}
    rows = [row for row in summary if row["config"] == "candidate"]
    ratio_key = "time_over_phase5" if stage == "guard" else "vortex_over_parquet"
    misses = [
        {"dataset": row["dataset"], "case": row["case"], ratio_key: row.get(ratio_key)}
        for row in rows
        if row["status"] != "passed"
        or (row.get(ratio_key, math.inf) > 1.05 if stage == "guard" else row.get(ratio_key, math.inf) >= 1)
    ]
    passed = all_trials_passed and rows and not misses
    return {
        "status": "passed" if passed else "failed",
        "criterion": (
            "candidate/Phase5-equivalent median <=1.05 in every local case/dataset"
            if stage == "guard"
            else "one fixed candidate median strictly below Parquet in every case/dataset"
        ),
        "misses": misses,
        "all_trials_passed": all_trials_passed,
        "note": "A median gate is not a significance test or a universal performance guarantee",
    }


def markdown(report):
    lines = [
        "# Phase6 read policy evaluation",
        "",
        f"Status: {report['status']}; purpose: {report['purpose']}.",
        f"Performance gate: {report['performance_gate']['status']}.",
        "",
        "| Dataset | Case | Configuration | Status | Median [min–max] seconds | Vortex / Parquet |",
        "| --- | --- | --- | --- | --- | ---: |",
    ]
    for row in report["summary"]:
        metric = row.get("metrics", {}).get("wall_seconds")
        value = f"{metric['median']:.6f} [{metric['min']:.6f}–{metric['max']:.6f}]" if metric else "unavailable"
        ratio = row.get("vortex_over_parquet")
        lines.append(
            f"| {row['dataset']} | {row['case']} | {row['config']} | {row['status']} | {value} | "
            + (f"{ratio:.3f}" if ratio is not None else "n/a")
            + " |"
        )
    lines += [
        "",
        "All samples and errors are retained. Ratios below one favor Vortex; ranges/paired counts are descriptive.",
        "Diagnostic captures are separate executed queries after performance trials, excluded from all timing summaries.",
        "Parquet diagnostics cover outer source creation/collection only; absent internal-phase/request events are unavailable, not zero cost.",
        "Caches are retained; fresh processes do not establish cold-cache conditions. Source bytes exclude discovery/planning and are not physical traffic.",
        "CPU covers the client process; RSS is its lifetime high-water including setup. MinIO/VM resources are excluded.",
        "",
    ]
    return "\n".join(lines)


def execute_child(spec, index, output, child_env, timeout):
    kind = "diagnostics" if spec.get("diagnostic") else "trials"
    stem = f"{index:05d}"
    spec_path, result_path = output / kind / f"{stem}-spec.json", output / kind / f"{stem}.json"
    spec_path.write_text(json.dumps(spec, sort_keys=True) + "\n")
    started = datetime.now(timezone.utc).isoformat()
    began = time.perf_counter()
    code, stderr = None, ""
    try:
        completed = subprocess.run(
            [
                sys.executable,
                "-m",
                "benchmarking.vortex.remote_eval",
                "--worker-spec",
                str(spec_path),
                "--output",
                str(result_path),
            ],
            capture_output=True,
            text=True,
            env=child_env,
            timeout=timeout,
            check=False,
        )
        code, stderr = completed.returncode, completed.stderr or ""
        if code or not result_path.is_file():
            raise RuntimeError(f"Trial exit={code}: {redacted(stderr[-6000:])}")
        trial = {**spec, **json.loads(result_path.read_text())}
        if "plan" in trial:
            relative = f"plans/{kind}-{stem}.txt"
            (output / relative).write_text(redacted(trial.pop("plan")))
            trial["plan"] = relative
    except (OSError, subprocess.TimeoutExpired, RuntimeError, ValueError) as error:
        if isinstance(error, subprocess.TimeoutExpired):
            captured = error.stderr or ""
            stderr = captured.decode(errors="replace") if isinstance(captured, bytes) else captured
        trial = {**spec, "status": "failed", "error": redacted(str(error))}
    stderr_path = f"{kind}/{stem}-stderr.log"
    (output / stderr_path).write_text(redacted(stderr))
    trial.update(
        subprocess_started_at_utc=started,
        subprocess_completed_at_utc=datetime.now(timezone.utc).isoformat(),
        subprocess_elapsed_seconds=time.perf_counter() - began,
        subprocess_returncode=code,
        worker_stderr=stderr_path,
    )
    result_path.write_text(redacted(json.dumps(trial, indent=2, sort_keys=True, allow_nan=False)) + "\n")
    return trial


def run(
    manifest_path,
    output,
    *,
    stage="tune",
    candidate_policy=DEFAULT_POLICY,
    selection=None,
    cases=QUERY_CASES,
    orders=("sorted",),
    layouts=("multi",),
    rounds=None,
    warmups=None,
    seed=42,
    allow_dev=False,
    storage_root=None,
    timeout=600,
    diagnostics=False,
):
    os.environ["DAFT_ANALYTICS_ENABLED"] = "0"
    rounds = (50 if stage == "final" else 20) if rounds is None else rounds
    warmups = (3 if stage == "final" else 1) if warmups is None else warmups
    selected = None
    if selection is not None:
        selected = load_selection(selection, allow_dev)
        if stage not in {"final", "guard"}:
            raise ValueError("A frozen selection applies only to final/local-guard stages")
        candidate_policy = selected["policy"]
    validate_options(stage, candidate_policy, cases, rounds, warmups, allow_dev, timeout)
    if stage == "guard" and storage_root is not None:
        raise ValueError("Local guard requires local fixture paths without a storage root")
    if stage != "guard" and not allow_dev and (not storage_root or not storage_root.startswith("s3://")):
        raise ValueError("Remote performance comparisons require an explicit S3-compatible storage root")
    manifest_path, output = Path(manifest_path).resolve(), Path(output).resolve()
    manifest = load_manifest(manifest_path)
    datasets = [
        key for key, value in manifest["datasets"].items() if value["order"] in orders and value["layout"] in layouts
    ]
    if not datasets:
        raise ValueError("No matching fixture datasets")
    source_paths(manifest_path, manifest["datasets"][datasets[0]], "vortex_11", storage_root)
    io_settings()
    require_api()
    provenance = environment()
    if not allow_dev and (
        provenance["build_type"] != "release"
        or not provenance["build_command"]
        or not provenance["native_binaries_sha256"]
        or provenance["tracked_changes"]
    ):
        raise ValueError("Performance requires an attested release build, native hash and clean tracked source")
    if selected is not None and not allow_dev:
        validate_selection_identity(selected, provenance, file_hash(manifest_path), stage)
    if output.exists() and (not output.is_dir() or any(output.iterdir())):
        raise FileExistsError("Report directory must be absent or empty")
    output.mkdir(parents=True, exist_ok=True)
    for directory in ("trials", "plans", "diagnostics"):
        (output / directory).mkdir()
    configs = configurations(stage, candidate_policy)
    settings = {
        "stage": stage,
        "candidate_policy": candidate_policy,
        "configurations": configs,
        "cases": list(cases),
        "datasets": datasets,
        "rounds": rounds,
        "warmups": warmups,
        "seed": seed,
        "diagnostics_requested": diagnostics,
        "local_result_buffer_capacity": 1,
        "selection": selected,
        "selection_sha256": file_hash(Path(selection)) if selection else None,
    }
    frozen = output / "frozen-policy.json"
    frozen.write_text(json.dumps(settings, indent=2, sort_keys=True) + "\n")
    report = {
        "report_version": 1,
        "experiment": "remote_read_policy",
        "status": "running",
        "purpose": "development correctness smoke; no performance claim"
        if allow_dev
        else (
            "same-build local regression guard" if stage == "guard" else f"same-build {stage} remote policy comparison"
        ),
        "environment": provenance,
        "manifest": str(manifest_path),
        "manifest_sha256": file_hash(manifest_path),
        "storage_root": storage_root,
        "io_configuration_provided": "DAFT_VORTEX_BENCH_IO_JSON" in os.environ,
        "settings": settings,
        "frozen_policy_sha256": file_hash(frozen),
        "trials": [],
        "diagnostics": [],
    }
    report_path = output / "report.json"
    report_path.write_text(json.dumps(report, indent=2, sort_keys=True) + "\n")
    child_env = {**os.environ, "DAFT_RUNNER": "native", "DAFT_PROGRESS_BAR": "0", "DAFT_ANALYTICS_ENABLED": "0"}
    jobs = base_schedule(cases, datasets, list(configs), warmups, rounds, seed)
    round_size = len(datasets) * len(cases) * len(configs)
    for index, entry in enumerate(jobs):
        spec = {
            **entry,
            "configuration": configs[entry["config"]],
            "manifest": str(manifest_path),
            "storage_root": storage_root,
            "allow_dev": allow_dev,
            "diagnostic": False,
        }
        trial = execute_child(spec, index, output, child_env, timeout)
        report["trials"].append(trial)
        if (index + 1) % round_size == 0:
            report_path.write_text(json.dumps(report, indent=2, sort_keys=True, allow_nan=False) + "\n")
        print(
            f"[{index + 1}/{len(jobs)}] {entry['dataset']} {entry['case']} {entry['config']}: {trial['status']}",
            flush=True,
        )
    if diagnostics:
        for index, entry in enumerate(base_schedule(cases, datasets, list(configs), 0, 1, seed)):
            spec = {
                **entry,
                "configuration": configs[entry["config"]],
                "manifest": str(manifest_path),
                "storage_root": storage_root,
                "allow_dev": allow_dev,
                "diagnostic": True,
            }
            report["diagnostics"].append(execute_child(spec, index, output, child_env, timeout))
    report["environment_after"] = environment()
    identity_keys = (
        "git_head",
        "tracked_changes",
        "native_binaries_sha256",
        "cargo_lock_sha256",
        "harness_sha256",
        "build_command",
        "build_type",
    )
    report["provenance_unchanged"] = all(provenance.get(k) == report["environment_after"].get(k) for k in identity_keys)
    report["summary"] = summarize(report["trials"], datasets, cases, configs, rounds)
    all_passed = all(t["status"] == "passed" for t in report["trials"])
    diagnostic_passed = all(
        t["status"] == "passed" and t.get("diagnostics_complete") is True for t in report["diagnostics"]
    )
    complete = all(row["status"] == "passed" for row in report["summary"])
    correct = all_passed and complete and report["provenance_unchanged"]
    report["correctness_status"] = "passed" if correct else "failed"
    report["diagnostics_status"] = ("passed" if diagnostic_passed else "failed") if diagnostics else "not_requested"
    report["performance_gate"] = performance_gate(
        report["summary"], stage=stage, allow_dev=allow_dev, all_trials_passed=correct
    )
    report["ranking"] = rank_policies(report["summary"], configs)
    report["status"] = (
        "passed" if correct and diagnostic_passed and report["performance_gate"]["status"] != "failed" else "failed"
    )
    report_path.write_text(redacted(json.dumps(report, indent=2, sort_keys=True, allow_nan=False)) + "\n")
    (output / "report.md").write_text(markdown(report))
    if stage == "tune" and report["status"] == "passed":
        eligible = [row for row in report["ranking"] if row["discovery_concurrency"] == 8]
        if eligible:
            selection_data = {
                "selection_version": 1,
                "policy": eligible[0]["policy"],
                "config": eligible[0]["config"],
                "configuration": configs[eligible[0]["config"]],
                "discovery_concurrency": 8,
                "metadata_concurrency": 8,
                "criterion": RANKING,
                "eligible_scope": "discovery8 configurations only; one policy across all cases",
                "tuning_report_sha256": file_hash(report_path),
                "development_only": allow_dev,
                "source_revision": provenance.get("git_head"),
                "native_binaries_sha256": provenance.get("native_binaries_sha256"),
                "manifest_sha256": report["manifest_sha256"],
            }
            (output / "selected-policy.json").write_text(json.dumps(selection_data, indent=2, sort_keys=True) + "\n")
    return report


def main(argv=None):
    os.environ["DAFT_ANALYTICS_ENABLED"] = "0"
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--manifest", type=Path)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--stage", choices=("tune", "final", "guard"), default="tune")
    parser.add_argument("--candidate-policy", choices=POLICIES, default=DEFAULT_POLICY)
    parser.add_argument("--selection", type=Path)
    parser.add_argument("--cases", choices=QUERY_CASES, nargs="+", default=list(QUERY_CASES))
    parser.add_argument("--orders", choices=("sorted", "shuffled"), nargs="+", default=["sorted"])
    parser.add_argument("--layouts", choices=("single", "multi"), nargs="+", default=["multi"])
    parser.add_argument("--rounds", type=int)
    parser.add_argument("--warmups", type=int)
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--timeout", type=float, default=600)
    parser.add_argument("--storage-root")
    parser.add_argument("--diagnostics", action="store_true")
    parser.add_argument("--allow-dev", action="store_true")
    parser.add_argument("--worker-spec", type=Path, help=argparse.SUPPRESS)
    args = vars(parser.parse_args(argv))
    try:
        spec_path = args.pop("worker_spec")
        if spec_path:
            result = worker(json.loads(spec_path.read_text()))
            args["output"].write_text(redacted(json.dumps(result, indent=2, sort_keys=True, allow_nan=False)) + "\n")
        else:
            if args["manifest"] is None:
                parser.error("--manifest is required")
            args["manifest_path"] = args.pop("manifest")
            result = run(**args)
        return 0 if result["status"] == "passed" else 1
    except (ValueError, OSError, RuntimeError, AssertionError) as error:
        parser.exit(1, redacted(str(error)) + "\n")


if __name__ == "__main__":
    raise SystemExit(main())
