"""Run each native query in a fresh process and retain every measured trial."""

from __future__ import annotations

import hashlib
import importlib.metadata
import io
import json
import os
import platform
import random
import re
import resource
import statistics
import subprocess
import sys
import time
from pathlib import Path
from typing import Any
from urllib.parse import urlsplit

from .fixtures import CASES, cutoff, file_hash, load_manifest

CONFIGS = {
    "parquet": None,
    "vortex_00": (False, False),
    "vortex_10": (True, False),
    "vortex_01": (False, True),
    "vortex_11": (True, True),
}
IO_ENV = "DAFT_VORTEX_BENCH_IO_JSON"


def redacted(text: str) -> str:
    try:
        config = json.loads(os.environ.get(IO_ENV, "{}"))
    except json.JSONDecodeError:
        return text
    if not isinstance(config, dict):
        return text
    for values in config.values():
        if not isinstance(values, dict):
            continue
        for key, value in values.items():
            if key in {"key_id", "access_key", "session_token", "bearer_token"} and value:
                text = text.replace(str(value), "<redacted>")
    return text


def io_settings() -> dict[str, Any]:
    """Validate URI-bearing configuration before persisting any diagnostics."""
    values = json.loads(os.environ.get(IO_ENV, "{}"))
    if (
        not isinstance(values, dict)
        or not set(values) <= {"s3", "http"}
        or any(not isinstance(v, dict) for v in values.values())
    ):
        raise ValueError(f"{IO_ENV} accepts only s3/http dictionaries")
    endpoint = values.get("s3", {}).get("endpoint_url")
    if endpoint is not None:
        message = "S3 endpoint_url must be a plain HTTP(S) URL without credentials/query/fragment"
        if not isinstance(endpoint, str):
            raise ValueError(message)
        try:
            parsed = urlsplit(endpoint)
        except ValueError:
            raise ValueError(message) from None
        if (
            parsed.scheme not in {"http", "https"}
            or not parsed.netloc
            or parsed.username is not None
            or parsed.query
            or parsed.fragment
        ):
            raise ValueError(message)
    return values


def io_config():
    from daft.daft import HTTPConfig, IOConfig, S3Config

    values = io_settings()
    return IOConfig(
        s3=S3Config(**values["s3"]) if "s3" in values else None,
        http=HTTPConfig(**values["http"]) if "http" in values else None,
    )


def source_paths(manifest_path: Path, dataset: dict[str, Any], config: str, storage_root: str | None) -> list[str]:
    fmt = "parquet" if config == "parquet" else "vortex"
    if storage_root:
        try:
            parsed = urlsplit(storage_root)
        except ValueError:
            raise ValueError("Invalid storage-root URI") from None
        if (
            parsed.scheme not in {"http", "https", "s3"}
            or not parsed.netloc
            or parsed.query
            or parsed.fragment
            or parsed.username is not None
        ):
            raise ValueError("storage-root must be a plain HTTP(S)/S3 prefix without credentials/query/fragment")
        return [f"{storage_root.rstrip('/')}/{relative}" for relative in dataset["paths"][fmt]]
    return [str(manifest_path.parent / relative) for relative in dataset["paths"][fmt]]


def query(spec: dict[str, Any], manifest: dict[str, Any]):
    import daft
    from daft import col

    settings = spec["settings"]
    dataset = manifest["datasets"][spec["dataset"]]
    paths = source_paths(Path(spec["manifest"]), dataset, spec["config"], spec.get("storage_root"))
    flags = CONFIGS[spec["config"]]
    if flags is None:
        frame = daft.read_parquet(paths, io_config=io_config())
    else:
        vortex_paths: list[str | Path] = [*paths]
        frame = daft.read_vortex(
            vortex_paths,
            io_config=io_config(),
            batch_size=settings["batch_size"],
            enable_projection_pushdown=flags[0],
            enable_filter_pushdown=flags[1],
            max_concurrent_requests=settings["max_concurrent_requests"],
            max_in_flight_bytes=settings["max_in_flight_bytes"],
        )
    case = spec["case"]
    rows = manifest["generator"]["rows"]
    basic = [
        col("id").count().alias("rows"),
        col("id").sum().alias("sum_id"),
        col("value").count().alias("count_value"),
        col("value").sum().alias("sum_value"),
    ]
    if case.startswith("range_"):
        frame = frame.where(col("id") < daft.lit(cutoff(rows, case.removeprefix("range_"))))
    elif case.startswith("eq_"):
        s = case.removeprefix("eq_")
        field = {"0": "constant", "0.001": "sel_0001", "0.01": "sel_001", "0.5": "sel_05", "1": "constant"}[s]
        frame = frame.where(col(field) == daft.lit(int(s != "0")))
    elif case == "scan_full":
        basic += [
            col("group").sum().alias("sum_group"),
            col("flag").count().alias("count_flag"),
            col("flag").cast(daft.DataType.int64()).sum().alias("sum_flag"),
            col("label").length().sum().alias("label_length"),
            col("text").length().sum().alias("text_length"),
        ]
        basic += [
            col(field).cast(daft.DataType.int64()).sum().alias(field)
            for field in ("sel_0001", "sel_001", "sel_05", "constant")
        ]
    elif case == "group_by":
        return (
            frame.groupby("group")
            .agg(
                col("id").count().alias("rows"),
                col("value").sum().alias("sum_value"),
                col("value").count().alias("count_value"),
            )
            .sort("group")
        )
    elif case == "join":
        dimension = daft.from_pydict({"group": list(range(128)), "weight": list(range(1, 129))})
        return frame.join(dimension, on="group").agg(
            col("id").count().alias("rows"), (col("value") * col("weight")).sum().alias("weighted_sum")
        )
    return frame.agg(*basic)


def check_output(actual: list[dict[str, Any]], expected: list[dict[str, Any]]) -> str:
    if actual != expected:
        raise AssertionError(f"Query output differs: expected {expected!r}, got {actual!r}")
    return hashlib.sha256(json.dumps(actual, sort_keys=True, allow_nan=False).encode()).hexdigest()


def _git(*args: str) -> str:
    return subprocess.check_output(["git", *args], text=True).strip()


def environment() -> dict[str, Any]:
    import pyarrow

    import daft

    try:
        memory = os.sysconf("SC_PHYS_PAGES") * os.sysconf("SC_PAGE_SIZE")
    except (OSError, ValueError, AttributeError):
        memory = None
    binaries = sorted(Path(daft.__file__).parent.rglob("*.so"))
    return {
        "git_head": _git("rev-parse", "HEAD"),
        "tracked_changes": bool(_git("diff", "HEAD", "--name-only")),
        "python": platform.python_version(),
        "platform": platform.platform(),
        "machine": platform.machine(),
        "cpu_count": os.cpu_count(),
        "physical_memory_bytes": memory,
        "daft_version": daft.daft.version(),
        "build_type": daft.daft.build_type(),
        "pyarrow": pyarrow.__version__,
        "build_command": redacted(os.environ["DAFT_VORTEX_BENCH_BUILD_COMMAND"])
        if "DAFT_VORTEX_BENCH_BUILD_COMMAND" in os.environ
        else None,
        "rustc": subprocess.check_output(["rustc", "--version"], text=True).strip(),
        "cargo_lock_sha256": file_hash(Path("Cargo.lock")),
        "arrow_packages": re.findall(
            r'\[\[package\]\]\nname = "(arrow[^\"]*)"\nversion = "([^\"]+)"', Path("Cargo.lock").read_text()
        ),
        "native_binaries_sha256": {str(p): file_hash(p) for p in binaries},
        "vortex-data": importlib.metadata.version("vortex-data"),
        "thread_environment": {
            k: os.environ[k]
            for k in (
                "DAFT_NUM_THREADS",
                "DAFT_RUNNER",
                "DAFT_SCANTASK_MAX_PARALLEL",
                "DAFT_MAINTAIN_ORDER",
                "RAYON_NUM_THREADS",
                "OMP_NUM_THREADS",
                "DAFT_ANALYTICS_ENABLED",
            )
            if k in os.environ
        },
        "harness_sha256": {p.name: file_hash(p) for p in sorted(Path(__file__).parent.glob("*.py"))},
    }


def worker(spec: dict[str, Any]) -> dict[str, Any]:
    import daft
    from daft.context import get_context, set_execution_config

    settings = spec["settings"]
    daft.set_runner_native(num_threads=settings["threads"])
    if settings["scan_tasks"] is not None:
        set_execution_config(scantask_max_parallel=settings["scan_tasks"])
    if daft.daft.build_type() != "release" and not spec["allow_dev"]:
        raise ValueError("Performance runs require a release build labeled RUST_DAFT_PKG_BUILD_TYPE=release")
    manifest = json.loads(Path(spec["manifest"]).read_text())
    # Initialize the query runtime without reading benchmark data or metadata.
    daft.from_pydict({"x": [1]}).agg(daft.col("x").sum()).collect()
    began_cpu, began = time.process_time_ns(), time.perf_counter_ns()
    frame = query(spec, manifest)
    frame.collect()
    wall_seconds = (time.perf_counter_ns() - began) / 1e9
    cpu_seconds = (time.process_time_ns() - began_cpu) / 1e9
    peak_rss = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
    peak_rss_bytes = int(peak_rss if sys.platform == "darwin" else peak_rss * 1024)
    # Small aggregate outputs, oracle checks and diagnostic plans are outside timing.
    actual = frame.to_arrow().to_pylist()
    digest = check_output(actual, manifest["oracle"][spec["case"]])
    io_bytes = None
    io_reason: str | None = "source bytes.read metric unavailable"
    operator_metrics = None
    try:
        operator_metrics = frame.metrics.to_pylist()
        counts = [
            int(descriptor["value"])
            for row in operator_metrics
            for key, descriptor in row["stats"]
            if key == "bytes.read"
        ]
        if counts:
            io_bytes, io_reason = sum(counts), None
        for metric in operator_metrics:
            metric["duration_seconds"] = metric.pop("duration").total_seconds()
    except (AttributeError, ValueError, KeyError, TypeError):
        pass
    plan = io.StringIO()
    query(spec, manifest).explain(show_all=True, file=plan)
    cfg = get_context().daft_execution_config
    return {
        "status": "passed",
        "wall_seconds": wall_seconds,
        "cpu_seconds": cpu_seconds,
        "peak_rss_bytes": peak_rss_bytes,
        "output_sha256": digest,
        "output": actual,
        "source_read_bytes": io_bytes,
        "source_read_bytes_unavailable_reason": io_reason,
        "read_requests": None,
        "read_requests_unavailable_reason": "SourceSnapshot does not export request counts",
        "operator_metrics": operator_metrics,
        "scan_decode_conversion_seconds": None,
        "scan_decode_conversion_unavailable_reason": "No separate timing instrumentation; source duration includes I/O wait",
        "plan": redacted(plan.getvalue()),
        "build_type": daft.daft.build_type(),
        "effective_settings": {
            "compute_threads_requested": settings["threads"],
            "scantask_max_parallel": cfg.scantask_max_parallel,
            "maintain_order": cfg.maintain_order,
            "default_morsel_size": cfg.default_morsel_size,
            "enable_scan_task_split_and_merge": cfg.enable_scan_task_split_and_merge,
        },
    }


def schedule(cases, datasets, configs, warmups: int, rounds: int, seed: int) -> list[dict[str, Any]]:
    rng = random.Random(seed)
    result = []
    for iteration in range(-warmups, rounds):
        block = [
            {"dataset": dataset, "case": case, "config": config, "iteration": iteration, "warmup": iteration < 0}
            for dataset in datasets
            for case in cases
            for config in configs
        ]
        rng.shuffle(block)
        result.extend(block)
    return result


def summarize(trials: list[dict[str, Any]]) -> list[dict[str, Any]]:
    groups: dict[tuple[str, str, str], list[dict[str, Any]]] = {}
    for trial in trials:
        if not trial["warmup"]:
            groups.setdefault((trial["dataset"], trial["case"], trial["config"]), []).append(trial)
    result: list[dict[str, Any]] = []
    for (dataset, case, config), values in groups.items():
        good = [v for v in values if v["status"] == "passed"]
        row: dict[str, Any] = {
            "dataset": dataset,
            "case": case,
            "config": config,
            "trials": len(values),
            "status": "passed" if len(good) == len(values) else "failed",
        }
        if good:
            row.update(
                {
                    "median_seconds": statistics.median(v["wall_seconds"] for v in good),
                    "min_seconds": min(v["wall_seconds"] for v in good),
                    "max_seconds": max(v["wall_seconds"] for v in good),
                    "median_cpu_seconds": statistics.median(v["cpu_seconds"] for v in good),
                    "max_peak_rss_bytes": max(v["peak_rss_bytes"] for v in good),
                    "output_sha256": good[0]["output_sha256"],
                    "source_read_bytes": good[0]["source_read_bytes"],
                }
            )
            if len({v["output_sha256"] for v in good}) != 1:
                raise AssertionError("Repeated outputs differ")
        result.append(row)
    baselines = {(r["dataset"], r["case"]): r for r in result if r["config"] == "parquet" and r["status"] == "passed"}
    for row in result:
        baseline = baselines.get((row["dataset"], row["case"]))
        if baseline and row["status"] == "passed":
            if baseline["output_sha256"] != row["output_sha256"]:
                raise AssertionError("Format outputs differ")
            row["parquet_time_over_this_time"] = baseline["median_seconds"] / row["median_seconds"]
    return sorted(result, key=lambda r: (r["dataset"], r["case"], r["config"]))


def markdown(report: dict[str, Any]) -> str:
    lines = [
        "# Native Daft reader comparison",
        "",
        f"Status: **{report['status']}**. Purpose: {report['purpose']}.",
        "",
        "Ratio is Parquet median time / this path's median time; below 1 means this path was slower.",
        "",
        "| Dataset | Case | Reader | Trials | Median seconds | Min–max seconds | Ratio | Peak process MiB | Status |",
        "| --- | --- | --- | ---: | ---: | --- | ---: | ---: | --- |",
    ]
    for row in report["summary"]:
        lines.append(
            f"| {row['dataset']} | {row['case']} | {row['config']} | {row['trials']} | "
            f"{row.get('median_seconds', float('nan')):.6f} | {row.get('min_seconds', float('nan')):.6f}–"
            f"{row.get('max_seconds', float('nan')):.6f} | {row.get('parquet_time_over_this_time', float('nan')):.3f} | "
            f"{row.get('max_peak_rss_bytes', 0) / 2**20:.1f} | {row['status']} |"
        )
    lines += [
        "",
        "No cache flush was performed; preflight hashes and warmups read the fixtures. A fresh process is not a cold OS cache.",
        "Peak RSS is the trial process high-water mark through query completion, including imports/runtime setup, not an allocation delta.",
        "Source read bytes exclude planning/discovery; request counts and physical-device I/O are unavailable.",
        "Queries use native value reductions; scan_full touches every column and scan_narrow touches id/value. This is not canonical TPC-H.",
        "Vortex has one Daft task per file; compute-thread settings do not cap every I/O/Vortex thread. See report.json for full provenance.",
        "",
    ]
    return "\n".join(lines)


def run(
    manifest_path: Path,
    output: Path,
    *,
    cases=CASES,
    configs=tuple(CONFIGS),
    orders=("sorted", "shuffled"),
    layouts=("single", "multi"),
    rounds=5,
    warmups=1,
    seed=42,
    allow_dev=False,
    storage_root=None,
    threads=None,
    scan_tasks=None,
    batch_size=131072,
    max_concurrent_requests=8,
    max_in_flight_bytes=67108864,
    timeout=600,
) -> dict[str, Any]:
    if rounds < 1 or warmups < 0 or (not allow_dev and (rounds < 5 or warmups < 1)):
        raise ValueError("Performance runs require >=5 rounds and >=1 warmup; dev smoke requires >=1 round")
    if not cases or not set(cases) <= set(CASES) or not configs or not set(configs) <= set(CONFIGS):
        raise ValueError("Select known, nonempty cases/configs")
    if any(
        v is not None and v < 1
        for v in (threads, scan_tasks, batch_size, max_concurrent_requests, max_in_flight_bytes, timeout)
    ):
        raise ValueError("Resource settings must be positive")
    manifest_path = manifest_path.resolve()
    manifest = load_manifest(manifest_path)
    datasets = [
        key for key, data in manifest["datasets"].items() if data["order"] in orders and data["layout"] in layouts
    ]
    if not datasets:
        raise ValueError("No matching fixture datasets")
    # Validate before persisting specs or reports: rejected credentials must
    # never be written into artifacts, including malformed URI parse errors.
    source_paths(manifest_path, manifest["datasets"][datasets[0]], configs[0], storage_root)
    io_settings()
    if output.exists() and any(output.iterdir()):
        raise FileExistsError("Report directory must be absent or empty")
    output.mkdir(parents=True, exist_ok=True)
    (output / "trials").mkdir()
    (output / "plans").mkdir()
    os.environ["DAFT_ANALYTICS_ENABLED"] = "0"
    env = environment()
    if env["build_type"] != "release" and not allow_dev:
        raise ValueError("Use the documented release build, or --allow-dev for correctness smoke only")
    if not allow_dev and (not env["build_command"] or not env["native_binaries_sha256"]):
        raise ValueError("Performance runs require DAFT_VORTEX_BENCH_BUILD_COMMAND and an identifiable native binary")
    report = {
        "report_version": 1,
        "purpose": "development correctness smoke; no performance claim"
        if allow_dev
        else "release reader-path comparison",
        "status": "running",
        "environment": env,
        "manifest_sha256": file_hash(manifest_path),
        "manifest": str(manifest_path),
        "storage_root": storage_root,
        "io_configuration_provided": IO_ENV in os.environ,
        "cache_policy": "hash preflight + warmup; OS caches not flushed; fresh trial processes",
        "settings": {
            "threads": threads,
            "scan_tasks": scan_tasks,
            "batch_size": batch_size,
            "max_concurrent_requests": max_concurrent_requests,
            "max_in_flight_bytes": max_in_flight_bytes,
            "local_result_buffer_capacity": 1,
            "seed": seed,
            "rounds": rounds,
            "warmups": warmups,
        },
        "datasets": {k: manifest["datasets"][k] for k in datasets},
        "writer_options": manifest["writer_options"],
        "files": manifest["files"],
        "trials": [],
    }
    trial_env = {**os.environ, "DAFT_RUNNER": "native", "DAFT_PROGRESS_BAR": "0", "DAFT_ANALYTICS_ENABLED": "0"}
    for index, entry in enumerate(schedule(cases, datasets, configs, warmups, rounds, seed)):
        spec = {
            **entry,
            "manifest": str(manifest_path),
            "settings": report["settings"],
            "storage_root": storage_root,
            "allow_dev": allow_dev,
        }
        spec_path, result_path = output / "trials" / f"{index:05d}-spec.json", output / "trials" / f"{index:05d}.json"
        spec_path.write_text(json.dumps(spec, sort_keys=True))
        try:
            completed = subprocess.run(
                [
                    sys.executable,
                    "-m",
                    "benchmarking.vortex",
                    "_worker",
                    "--spec",
                    str(spec_path),
                    "--output",
                    str(result_path),
                ],
                env=trial_env,
                capture_output=True,
                text=True,
                timeout=timeout,
                check=False,
            )
            if completed.returncode or not result_path.exists():
                raise RuntimeError(f"trial exit={completed.returncode}: {redacted(completed.stderr[-6000:])}")
            trial = {**entry, **json.loads(result_path.read_text())}
            plan = trial.pop("plan")
            plan_path = f"plans/{entry['dataset'].replace('/', '-')}-{entry['case']}-{entry['config']}.txt"
            (output / plan_path).write_text(plan)
            trial["plan"] = plan_path
        except (OSError, subprocess.TimeoutExpired, RuntimeError, KeyError, ValueError) as error:
            trial = {**entry, "status": "failed", "error": redacted(str(error))}
        report["trials"].append(trial)
        result_path.write_text(json.dumps(trial, indent=2, sort_keys=True) + "\n")
        (output / "report.json").write_text(json.dumps(report, indent=2, sort_keys=True) + "\n")
        print(f"[{index + 1}] {entry['dataset']} {entry['case']} {entry['config']}: {trial['status']}", flush=True)
    report["summary"] = summarize(report["trials"])
    report["status"] = "passed" if all(t["status"] == "passed" for t in report["trials"]) else "failed"
    (output / "report.json").write_text(json.dumps(report, indent=2, sort_keys=True, allow_nan=False) + "\n")
    (output / "report.md").write_text(markdown(report))
    return report
