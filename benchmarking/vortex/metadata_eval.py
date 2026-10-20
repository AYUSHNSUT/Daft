"""Compare metadata concurrency on one build; query and footer timings stay separate.

Run from the repository root with an existing Phase 4 fixture manifest. Example:
python -m benchmarking.vortex.metadata_eval --manifest PATH --output DIR --concurrency 1 8
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
from pathlib import Path
from typing import Any

from .fixtures import SCHEMA, cutoff, file_hash, load_manifest
from .run import check_output, environment, file_read_telemetry, io_config, io_settings, redacted, source_paths
from .run import schedule as base_schedule

QUERY_CASES = ("scan_narrow", "range_0.01", "eq_0.01", "join", "scan_full")
DEFAULT_CASES = QUERY_CASES[:3]
METADATA_CASE = "footer_metadata"
COUNTERS = ("head_requests", "get_requests", "list_requests", "bytes_read")


def validate_options(concurrency, cases, modes, rounds, warmups, allow_dev, timeout):
    if (
        not concurrency
        or len(set(concurrency)) != len(concurrency)
        or any(type(value) is not int or not 1 <= value <= 64 for value in concurrency)
        or 1 not in concurrency
    ):
        raise ValueError("concurrency must contain unique integers in 1..64, including baseline 1")
    if not modes or not set(modes) <= {"query", "metadata"} or len(set(modes)) != len(modes):
        raise ValueError("Select unique query/metadata modes")
    if not cases or not set(cases) <= set(QUERY_CASES) or len(set(cases)) != len(cases):
        raise ValueError("Select unique supported query cases")
    if type(rounds) is not int or rounds < (1 if allow_dev else 20):
        raise ValueError("Comparisons require at least 20 measured rounds; dev smoke requires 1")
    if type(warmups) is not int or warmups < (0 if allow_dev else 1):
        raise ValueError("Comparisons require at least 1 warmup")
    if isinstance(timeout, bool) or not math.isfinite(timeout) or timeout <= 0:
        raise ValueError("timeout must be finite and positive")


def schedule(cases, datasets, concurrency, modes, warmups, rounds, seed):
    selected = list(cases) if "query" in modes else []
    if "metadata" in modes:
        selected.append(METADATA_CASE)
    configs = [f"vortex_{value}" for value in concurrency]
    if "query" in modes:
        configs.insert(0, "parquet")
    return [
        {
            **{key: value for key, value in entry.items() if key != "config"},
            "reader": "parquet" if entry["config"] == "parquet" else "vortex",
            "metadata_concurrency": None if entry["config"] == "parquet" else int(entry["config"].split("_")[1]),
            "mode": "metadata" if entry["case"] == METADATA_CASE else "query",
        }
        for entry in base_schedule(selected, datasets, configs, warmups, rounds, seed)
        if not (entry["case"] == METADATA_CASE and entry["config"] == "parquet")
    ]


def query(spec: dict[str, Any], manifest: dict[str, Any]):
    import daft
    from daft import col

    paths = source_paths(
        Path(spec["manifest"]),
        manifest["datasets"][spec["dataset"]],
        "parquet" if spec["reader"] == "parquet" else "vortex_11",
        spec.get("storage_root"),
    )
    if spec["reader"] == "parquet":
        frame = daft.read_parquet(paths, io_config=io_config())
    else:
        frame = daft.read_vortex(
            [*paths],
            io_config=io_config(),
            metadata_concurrency=spec["metadata_concurrency"],
            batch_size=131072,
            max_concurrent_requests=8,
            max_in_flight_bytes=67108864,
            enable_projection_pushdown=True,
            enable_filter_pushdown=True,
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


def validate_metadata(entries, diagnostics, expected_rows, expected_schema, concurrency):
    if len(entries) != len(expected_rows) or diagnostics.get("num_files") != len(expected_rows):
        raise AssertionError("Bulk metadata file coverage differs")
    if [rows for _, rows in entries] != expected_rows:
        raise AssertionError("Bulk metadata row counts or input ordering differ")
    if any(schema != expected_schema for schema, _ in entries):
        raise AssertionError("Bulk metadata schema differs from the canonical fixture")
    bound = diagnostics.get("max_concurrent_files")
    if type(bound) is not int or bound != concurrency:
        raise AssertionError("Invalid metadata concurrency diagnostic")
    elapsed = diagnostics.get("elapsed_seconds")
    if isinstance(elapsed, bool) or not isinstance(elapsed, (int, float)) or not math.isfinite(elapsed) or elapsed < 0:
        raise AssertionError("Invalid native metadata elapsed time")
    for name in COUNTERS:
        if type(diagnostics.get(name)) is not int or diagnostics[name] < 0:
            raise AssertionError(f"Invalid native metadata counter: {name}")


def require_api():
    import daft

    if not hasattr(daft.daft, "read_vortex_metadata_bulk"):
        raise RuntimeError("Requires a native Vortex build with read_vortex_metadata_bulk and metadata_concurrency")


def worker(spec: dict[str, Any]) -> dict[str, Any]:
    import daft
    from daft.context import get_context
    from daft.schema import Schema

    require_api()
    daft.set_runner_native()
    if daft.daft.build_type() != "release" and not spec["allow_dev"]:
        raise ValueError("Comparisons require an attested release build")
    manifest = json.loads(Path(spec["manifest"]).read_text())
    dataset = manifest["datasets"][spec["dataset"]]
    # Runtime setup touches no benchmark file, footer or schema.
    daft.from_pydict({"x": [1]}).agg(daft.col("x").sum()).collect()
    if spec["mode"] == "metadata":
        paths = source_paths(Path(spec["manifest"]), dataset, "vortex_11", spec.get("storage_root"))
        config = io_config()
    began_cpu, began = time.process_time_ns(), time.perf_counter_ns()
    if spec["mode"] == "query":
        frame = query(spec, manifest)
        frame.collect()
    else:
        entries, diagnostics = daft.daft.read_vortex_metadata_bulk(
            paths,
            io_config=config,
            max_concurrent_files=spec["metadata_concurrency"],
            max_concurrent_requests=8,
            max_in_flight_bytes=67108864,
        )
    measured = {
        "wall_seconds": (time.perf_counter_ns() - began) / 1e9,
        "cpu_seconds": (time.process_time_ns() - began_cpu) / 1e9,
        "peak_rss_bytes": int(
            resource.getrusage(resource.RUSAGE_SELF).ru_maxrss * (1 if sys.platform == "darwin" else 1024)
        ),
    }
    cfg = get_context().daft_execution_config
    result = {
        "status": "passed",
        **measured,
        "build_type": daft.daft.build_type(),
        "effective_settings": {
            "scantask_max_parallel": cfg.scantask_max_parallel,
            "maintain_order": cfg.maintain_order,
            "default_morsel_size": cfg.default_morsel_size,
            "enable_scan_task_split_and_merge": cfg.enable_scan_task_split_and_merge,
        },
    }
    if spec["mode"] == "metadata":
        records = {record["path"]: record for record in manifest["files"]}
        expected_rows = [records[path]["rows"] for path in dataset["paths"]["vortex"]]
        validate_metadata(
            entries,
            diagnostics,
            expected_rows,
            Schema.from_pyarrow_schema(SCHEMA)._schema,
            spec["metadata_concurrency"],
        )
        for schema, _ in entries:
            for actual_field, expected_field in zip(schema.to_pyarrow_schema(), SCHEMA):
                if (actual_field.metadata or {}).get(b"daft.vortex.nullable") != str(
                    expected_field.nullable
                ).lower().encode():
                    raise AssertionError("Bulk metadata nullability differs from the canonical fixture")
        actual = [{"rows": rows} for _, rows in entries]
        result.update(
            {
                "output_sha256": check_output(actual, [{"rows": value} for value in expected_rows]),
                "output": actual,
                "metadata_diagnostics": diagnostics,
                "measurement_scope": "bulk footer metadata call only; no query execution or discovery",
                "schema_validation": "canonical field types/order and source nullability markers",
            }
        )
    else:
        actual = frame.to_arrow().to_pylist()
        metrics = frame.metrics.to_pylist()
        for metric in metrics:
            metric["duration_seconds"] = metric.pop("duration").total_seconds()
        plan = io.StringIO()
        query(spec, manifest).explain(show_all=True, file=plan)
        result.update(
            {
                "output_sha256": check_output(actual, manifest["oracle"][spec["case"]]),
                "output": actual,
                "operator_metrics": metrics,
                "plan": redacted(plan.getvalue()),
                **file_read_telemetry(
                    metrics,
                    config="parquet" if spec["reader"] == "parquet" else "vortex_11",
                    storage_root=spec.get("storage_root"),
                ),
                "measurement_scope": "fresh source/discovery/schema/planning through complete aggregate collection",
                "metadata_diagnostics": None,
                "metadata_diagnostics_note": "No separate metadata counters are inferred from query source counters; use metadata-only trials",
                "read_requests": None,
                "read_requests_unavailable_reason": "Query SourceSnapshot does not export request counts",
            }
        )
    return result


def range_summary(values):
    present = [value for value in values if value is not None]
    return {
        "median": statistics.median(present) if present else None,
        "min": min(present) if present else None,
        "max": max(present) if present else None,
        "available_samples": len(present),
        "missing_samples": len(values) - len(present),
    }


def summarize(trials):
    groups: dict[tuple[str, str, str, int | None], list[dict[str, Any]]] = {}
    for trial in trials:
        if not trial["warmup"]:
            groups.setdefault(
                (trial["dataset"], trial["case"], trial["reader"], trial["metadata_concurrency"]), []
            ).append(trial)
    result = []
    for (dataset, case, reader, concurrency), values in sorted(groups.items()):
        passed = [value for value in values if value["status"] == "passed"]
        row: dict[str, Any] = {
            "dataset": dataset,
            "case": case,
            "mode": values[0]["mode"],
            "reader": reader,
            "metadata_concurrency": concurrency,
            "trials": len(values),
            "status": "passed" if len(passed) == len(values) else "failed",
        }
        if passed:
            if len({value["output_sha256"] for value in passed}) != 1:
                raise AssertionError("Repeated correctness digests differ")
            row["output_sha256"] = passed[0]["output_sha256"]
            row["metrics"] = {
                name: range_summary([value.get(name) for value in passed])
                for name in ("wall_seconds", "cpu_seconds", "peak_rss_bytes", "source_read_bytes")
            }
            if case == METADATA_CASE:
                row["metadata_diagnostics"] = {
                    name: range_summary([value["metadata_diagnostics"][name] for value in passed])
                    for name in (*COUNTERS, "elapsed_seconds")
                }
        result.append(row)
    vortex_baselines = {
        (row["dataset"], row["case"]): row
        for row in result
        if row["reader"] == "vortex" and row["metadata_concurrency"] == 1 and row["status"] == "passed"
    }
    parquet_baselines = {
        (row["dataset"], row["case"]): row for row in result if row["reader"] == "parquet" and row["status"] == "passed"
    }
    for row in result:
        if row["status"] != "passed":
            continue
        references = []
        if row["reader"] == "vortex":
            references.append((vortex_baselines, "concurrency_1"))
        if row["mode"] == "query":
            references.append((parquet_baselines, "parquet"))
        for baselines, name in references:
            baseline = baselines.get((row["dataset"], row["case"]))
            if baseline:
                if row["output_sha256"] != baseline["output_sha256"]:
                    raise AssertionError("Reader/concurrency configurations produced different outputs")
                first, candidate = baseline["metrics"]["wall_seconds"], row["metrics"]["wall_seconds"]
                row[f"{name}_time_over_this_time"] = first["median"] / candidate["median"]
                row[f"{name}_sample_ranges_overlap"] = max(first["min"], candidate["min"]) <= min(
                    first["max"], candidate["max"]
                )
    return result


def markdown(report):
    lines = [
        "# Metadata concurrency comparison",
        "",
        f"Status: {report['status']}. Purpose: {report['purpose']}.",
        "",
        "Ratios divide the named baseline median by this setting's median. Below one is slower. Metadata-only timings are not query speedups.",
        "",
        "| Mode | Dataset | Case | Reader | Concurrency | Status | Median seconds | Min–max seconds | Vortex 1 / this | Parquet / this |",
        "| --- | --- | --- | --- | ---: | --- | ---: | --- | ---: | ---: |",
    ]
    for row in report["summary"]:
        metric = row.get("metrics", {}).get("wall_seconds")
        values = f"{metric['median']:.6f} | {metric['min']:.6f}–{metric['max']:.6f}" if metric else "n/a | n/a"
        ratios = [row.get(f"{name}_time_over_this_time") for name in ("concurrency_1", "parquet")]
        ratio_values = " | ".join(f"{ratio:.3f}" if ratio is not None else "n/a" for ratio in ratios)
        lines.append(
            f"| {row['mode']} | {row['dataset']} | {row['case']} | {row['reader']} | {row['metadata_concurrency'] if row['metadata_concurrency'] is not None else 'n/a'} | {row['status']} | {values} | {ratio_values} |"
        )
    lines += [
        "",
        "OS caches were not flushed. Hash preflight and warmups read fixture data; fresh processes do not establish cold caches.",
        "RSS is process lifetime high-water through the timed operation, including imports/runtime setup. Query file-source bytes exclude discovery/planning.",
        "Bulk metadata counters describe that separate native call only; they are not whole-query request counts or physical-device bytes.",
        "Metadata max_concurrent_files is the configured limit, not an observed peak. No persistent metadata cache is used.",
        "One build and fixed fixtures are used throughout. Query trials include the normal Parquet reader and Vortex with both pushdowns enabled; this is not an engine ranking. All errors/outliers are retained.",
        "",
    ]
    return "\n".join(lines)


def run(
    manifest_path: Path,
    output: Path,
    *,
    concurrency=(1, 8),
    cases=DEFAULT_CASES,
    modes=("query", "metadata"),
    orders=("sorted", "shuffled"),
    layouts=("single", "multi"),
    rounds=20,
    warmups=1,
    seed=42,
    allow_dev=False,
    storage_root=None,
    timeout=600,
):
    os.environ["DAFT_ANALYTICS_ENABLED"] = "0"
    validate_options(concurrency, cases, modes, rounds, warmups, allow_dev, timeout)
    manifest_path = manifest_path.resolve()
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
    ):
        raise ValueError("Comparisons require a release build, exact build command and native binary hash")
    if output.exists() and any(output.iterdir()):
        raise FileExistsError("Report directory must be absent or empty")
    output.mkdir(parents=True, exist_ok=True)
    (output / "trials").mkdir()
    (output / "plans").mkdir()
    report = {
        "report_version": 1,
        "experiment": "metadata_concurrency",
        "status": "running",
        "purpose": "development correctness smoke; no performance claim"
        if allow_dev
        else "same-build metadata concurrency comparison",
        "environment": provenance,
        "manifest": str(manifest_path),
        "manifest_sha256": file_hash(manifest_path),
        "storage_root": storage_root,
        "io_configuration_provided": "DAFT_VORTEX_BENCH_IO_JSON" in os.environ,
        "settings": {
            "concurrency": list(concurrency),
            "query_readers": ["parquet", "vortex"] if "query" in modes else [],
            "cases": list(cases),
            "modes": list(modes),
            "datasets": datasets,
            "rounds": rounds,
            "warmups": warmups,
            "seed": seed,
            "max_concurrent_requests": 8,
            "max_in_flight_bytes": 67108864,
            "batch_size": 131072,
            "local_result_buffer_capacity": 1,
        },
        "datasets": {key: manifest["datasets"][key] for key in datasets},
        "trials": [],
    }
    jobs = schedule(cases, datasets, concurrency, modes, warmups, rounds, seed)
    round_size = len(jobs) // (rounds + warmups)
    report_path = output / "report.json"
    report_path.write_text(json.dumps(report, indent=2, sort_keys=True) + "\n")
    child_env = {**os.environ, "DAFT_RUNNER": "native", "DAFT_PROGRESS_BAR": "0", "DAFT_ANALYTICS_ENABLED": "0"}
    for index, entry in enumerate(jobs):
        spec = {**entry, "manifest": str(manifest_path), "storage_root": storage_root, "allow_dev": allow_dev}
        spec_path, result_path = output / "trials" / f"{index:05d}-spec.json", output / "trials" / f"{index:05d}.json"
        spec_path.write_text(json.dumps(spec, sort_keys=True) + "\n")
        try:
            completed = subprocess.run(
                [
                    sys.executable,
                    "-m",
                    "benchmarking.vortex.metadata_eval",
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
            if completed.returncode or not result_path.exists():
                raise RuntimeError(f"Trial exit={completed.returncode}: {redacted(completed.stderr[-6000:])}")
            trial = {**entry, **json.loads(result_path.read_text())}
            if "plan" in trial:
                relative = f"plans/{index:05d}-{entry['dataset'].replace('/', '-')}-{entry['case']}-{entry['reader']}-metadata{entry['metadata_concurrency']}.txt"
                (output / relative).write_text(trial.pop("plan"))
                trial["plan"] = relative
        except (OSError, subprocess.TimeoutExpired, RuntimeError, ValueError) as error:
            trial = {**entry, "status": "failed", "error": redacted(str(error))}
        report["trials"].append(trial)
        result_path.write_text(json.dumps(trial, indent=2, sort_keys=True, allow_nan=False) + "\n")
        if (index + 1) % round_size == 0:
            report_path.write_text(json.dumps(report, indent=2, sort_keys=True, allow_nan=False) + "\n")
        print(
            f"[{index + 1}] {entry['dataset']} {entry['case']} {entry['reader']} concurrency={entry['metadata_concurrency']}: {trial['status']}",
            flush=True,
        )
    report["summary"] = summarize(report["trials"])
    report["status"] = "passed" if all(trial["status"] == "passed" for trial in report["trials"]) else "failed"
    report_path.write_text(json.dumps(report, indent=2, sort_keys=True, allow_nan=False) + "\n")
    (output / "report.md").write_text(markdown(report))
    return report


def main():
    os.environ["DAFT_ANALYTICS_ENABLED"] = "0"
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--manifest", type=Path)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--concurrency", type=int, nargs="+", default=[1, 8])
    parser.add_argument("--cases", choices=QUERY_CASES, nargs="+", default=list(DEFAULT_CASES))
    parser.add_argument("--modes", choices=("query", "metadata"), nargs="+", default=["query", "metadata"])
    parser.add_argument("--orders", choices=("sorted", "shuffled"), nargs="+", default=["sorted", "shuffled"])
    parser.add_argument("--layouts", choices=("single", "multi"), nargs="+", default=["single", "multi"])
    parser.add_argument("--rounds", type=int, default=20)
    parser.add_argument("--warmups", type=int, default=1)
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--timeout", type=float, default=600)
    parser.add_argument("--storage-root")
    parser.add_argument("--allow-dev", action="store_true")
    parser.add_argument("--worker-spec", type=Path, help=argparse.SUPPRESS)
    args = vars(parser.parse_args())
    try:
        worker_spec = args.pop("worker_spec")
        if worker_spec:
            result = worker(json.loads(worker_spec.read_text()))
            args["output"].write_text(json.dumps(result, indent=2, sort_keys=True, allow_nan=False) + "\n")
        else:
            if args["manifest"] is None:
                parser.error("--manifest is required")
            args["manifest_path"] = args.pop("manifest")
            result = run(**args)
            if result["status"] != "passed":
                raise RuntimeError("Comparison failed; inspect report.json")
    except (ValueError, OSError, RuntimeError, AssertionError) as error:
        parser.exit(1, redacted(str(error)) + "\n")


if __name__ == "__main__":
    main()
