"""Capture a correctness-checked reader baseline, not a throughput claim.

Run this file from the repository root using its development environment.
Each timed sample creates a fresh source and consumes its Arrow result. Fixture
validation, explain plans, and expected-result checks stay outside the timer.
"""

from __future__ import annotations

import argparse
import hashlib
import importlib.metadata
import io
import json
import logging
import os
import platform
import statistics
import subprocess
import time
from pathlib import Path
from typing import Any

import pyarrow as pa
from fixtures import canonical_table, semantic_checksum

logger = logging.getLogger(__name__)


def _git(*args: str) -> str | None:
    result = subprocess.run(["git", *args], capture_output=True, text=True, check=False)
    return result.stdout.strip() if result.returncode == 0 else None


def _environment() -> dict[str, Any]:
    import daft

    versions = {}
    for name in ("daft", "pyarrow", "vortex-data"):
        try:
            versions[name] = importlib.metadata.version(name)
        except importlib.metadata.PackageNotFoundError:
            versions[name] = None
    experiment = Path(__file__).parent
    source_files = (
        "baseline.py",
        "fixtures.py",
        "reference.py",
        "requirements.txt",
        "rust/Cargo.toml",
        "rust/src/main.rs",
    )
    return {
        "git_head": _git("rev-parse", "HEAD"),
        "git_branch": _git("branch", "--show-current"),
        "tracked_changes": bool(_git("diff", "HEAD", "--name-only")),
        "experiment_changes": _git("status", "--porcelain", "--untracked-files=all", "--", str(experiment)),
        "experiment_sources_sha256": {
            name: hashlib.sha256((experiment / name).read_bytes()).hexdigest()
            for name in source_files
            if (experiment / name).is_file()
        },
        "python": platform.python_version(),
        "platform": platform.platform(),
        "machine": platform.machine(),
        "versions": versions,
        "daft_source": str(Path(daft.__file__).resolve()),
        "daft_native_version": daft.daft.version(),
        "daft_build_type": daft.daft.build_type(),
        "runner": "native",
    }


def load_manifest(path: Path) -> tuple[dict[str, Any], pa.Table]:
    """Validate the fixture record and bytes before timing any query."""
    manifest = json.loads(path.read_text())
    if manifest.get("manifest_version") != 1:
        raise ValueError("Unsupported fixture manifest version")
    if manifest["versions"]["pyarrow"] != pa.__version__:
        raise ValueError("Use the recorded PyArrow version or regenerate the fixtures")
    generator = manifest["generator"]
    canonical = canonical_table(seed=generator["seed"], rows=generator["rows"])
    if semantic_checksum(canonical) != manifest["semantic_checksum"]:
        raise ValueError("Manifest does not match the canonical fixture generator")
    layouts = manifest.get("layouts")
    if not isinstance(layouts, dict) or set(layouts) != {"single", "multi", "empty"}:
        raise ValueError("Fixture manifest must contain exactly the single, multi, and empty layouts")
    root = path.parent.resolve()
    records = {}
    for record in manifest["files"]:
        relative = record["path"]
        if not isinstance(relative, str) or not relative:
            raise ValueError("A fixture record must have a non-empty relative path")
        fixture = (root / relative).resolve()
        if Path(relative).is_absolute() or not fixture.is_relative_to(root):
            raise ValueError("A fixture path escapes its manifest directory")
        if relative in records:
            raise ValueError(f"Duplicate fixture record: {relative}")
        records[relative] = record
    referenced_paths = set()
    for layout_name, layout in layouts.items():
        for file_format in ("parquet", "vortex"):
            paths = layout.get(file_format)
            if not isinstance(paths, list) or any(not isinstance(relative, str) for relative in paths):
                raise ValueError(f"Layout {layout_name}/{file_format} must contain a list of fixture paths")
            for relative in paths:
                record = records.get(relative)
                if record is None:
                    raise ValueError(f"Layout references an unlisted fixture: {relative}")
                if record["layout"] != layout_name or record["format"] != file_format:
                    raise ValueError(f"Layout/format does not match fixture record: {relative}")
                if relative in referenced_paths:
                    raise ValueError(f"A fixture path is referenced more than once: {relative}")
                referenced_paths.add(relative)
    if referenced_paths != set(records):
        raise ValueError("Fixture manifest contains file records not referenced by any layout")
    for relative, record in records.items():
        fixture = root / relative
        digest = hashlib.sha256(fixture.read_bytes()).hexdigest()
        if digest != record["sha256"]:
            raise ValueError(f"Fixture bytes changed: {relative}")
    return manifest, canonical


def _query(
    source: str,
    paths: list[str],
    query: str,
    batch_size: int,
    *,
    enable_projection_pushdown: bool = True,
    enable_filter_pushdown: bool = True,
) -> Any:
    import daft
    from daft import col

    if source == "parquet":
        frame = daft.read_parquet(paths)
    elif source == "vortex-native":
        frame = daft.read_vortex(
            paths,
            batch_size=batch_size,
            enable_projection_pushdown=enable_projection_pushdown,
            enable_filter_pushdown=enable_filter_pushdown,
        )
    else:
        from reference import read_vortex_reference

        frame = read_vortex_reference(paths, batch_size=batch_size)
    if query == "scan_all":
        return frame
    if query == "filter_projection":
        return frame.where(col("value") >= 20).select("id").sort("id")
    if query == "aggregate_value":
        return frame.agg(
            col("value").sum().alias("sum_value"),
            col("value").count().alias("count_value_nonnull"),
        )
    raise ValueError(f"Unknown query: {query}")


def _check_result(query: str, actual: pa.Table, canonical: pa.Table, expected: dict[str, Any]) -> str:
    if query == "scan_all":
        # Tasks may finish in a different order, and Daft widens string/binary
        # offsets. Compare normalized logical values in the canonical ID order.
        if actual.column_names != canonical.column_names:
            raise AssertionError("Scan output fields changed")
        equivalent_representations = (
            (pa.string(), pa.large_string(), pa.string_view()),
            (pa.binary(), pa.large_binary(), pa.binary_view()),
        )
        normalized_columns = []
        for column, actual_field, expected_field in zip(actual.columns, actual.schema, canonical.schema):
            if actual_field.type != expected_field.type and not any(
                actual_field.type in group and expected_field.type in group for group in equivalent_representations
            ):
                raise AssertionError(
                    f"Scan output type changed for {expected_field.name}: "
                    f"expected {expected_field.type}, found {actual_field.type}"
                )
            normalized_columns.append(column.cast(expected_field.type))
        normalized = pa.Table.from_arrays(normalized_columns, schema=canonical.schema)
        ordered = normalized.sort_by("id") if len(normalized) else normalized
        digest = semantic_checksum(ordered)
        if digest != semantic_checksum(canonical):
            raise AssertionError("Scan values, schema, or row multiplicity changed")
        return digest
    elif query == "filter_projection":
        if actual.to_pydict() != {"id": expected["filtered_id"]}:
            raise AssertionError("Filtered IDs differ from the independent oracle")
    else:
        values = {"sum_value": [expected["sum_value"]], "count_value_nonnull": [expected["count_value_nonnull"]]}
        if actual.to_pydict() != values:
            raise AssertionError("Aggregate values differ from the independent oracle")
    return semantic_checksum(actual)


def run_baseline(
    manifest_path: Path,
    output: Path,
    *,
    source: str = "parquet",
    repeats: int = 5,
    batch_size: int = 7,
    enable_projection_pushdown: bool = True,
    enable_filter_pushdown: bool = True,
) -> dict[str, Any]:
    if source not in {"parquet", "vortex-reference", "vortex-native"}:
        raise ValueError("Source must be parquet, vortex-reference, or vortex-native")
    if isinstance(repeats, bool) or not isinstance(repeats, int) or repeats < 1:
        raise ValueError("repeats must be positive")
    if isinstance(batch_size, bool) or not isinstance(batch_size, int) or batch_size < 1:
        raise ValueError("batch_size must be positive")
    if not isinstance(enable_projection_pushdown, bool) or not isinstance(enable_filter_pushdown, bool):
        raise TypeError("Physical pushdown settings must be booleans")
    if source != "vortex-native" and (not enable_projection_pushdown or not enable_filter_pushdown):
        raise ValueError("Physical pushdown switches require source=vortex-native")
    os.environ.setdefault("DAFT_RUNNER", "native")
    if os.environ["DAFT_RUNNER"] != "native":
        raise ValueError("Vortex evaluation baselines require DAFT_RUNNER=native")
    manifest_path, output = manifest_path.resolve(), output.resolve()
    manifest, canonical = load_manifest(manifest_path)
    file_format = "parquet" if source == "parquet" else "vortex"
    if any(not layout[file_format] for layout in manifest["layouts"].values()):
        raise ValueError(f"Manifest has no {file_format} files; generate the required fixtures first")
    if output == manifest_path or output in {
        (manifest_path.parent / record["path"]).resolve() for record in manifest["files"]
    }:
        raise ValueError("Baseline output must not overwrite a fixture or manifest")
    output.parent.mkdir(parents=True, exist_ok=True)
    plans = output.parent / f"{output.stem}-plans"
    plans.mkdir(exist_ok=True)
    report: dict[str, Any] = {
        "report_version": 1,
        "source": source,
        "native_vortex_integration": source == "vortex-native",
        "fixture_manifest_sha256": hashlib.sha256(manifest_path.read_bytes()).hexdigest(),
        "fixture_manifest": str(manifest_path),
        "environment": _environment(),
        "reader_settings": {
            "batch_size_rows": batch_size if source != "parquet" else None,
            "batch_size_policy": "explicit Vortex batch-size setting"
            if source != "parquet"
            else "Daft native Parquet reader defaults",
        },
        "timing": {
            "scope": "fresh source construction through full Arrow materialization",
            "purpose": "small-fixture correctness and baseline smoke measurement, not a speedup claim",
            "cache": "one warmup; OS caches not flushed; new source/query for every sample",
            "warmups": 1,
            "repeats": repeats,
            "io_bytes_requests": "unavailable",
        },
        "cases": [],
    }
    if source == "vortex-native":
        report["reader_settings"].update(
            enable_projection_pushdown=enable_projection_pushdown,
            enable_filter_pushdown=enable_filter_pushdown,
        )
    for layout_name, layout in manifest["layouts"].items():
        paths = [str(manifest_path.parent / relative) for relative in layout[file_format]]
        table = canonical.slice(0, 0) if layout_name == "empty" else canonical
        for query in ("scan_all", "filter_projection", "aggregate_value"):
            record: dict[str, Any] = {"layout": layout_name, "query": query}
            try:
                plan = io.StringIO()
                _query(
                    source,
                    paths,
                    query,
                    batch_size,
                    enable_projection_pushdown=enable_projection_pushdown,
                    enable_filter_pushdown=enable_filter_pushdown,
                ).explain(show_all=True, file=plan)
                plan_path = plans / f"{layout_name}-{query}.txt"
                plan_path.write_text(plan.getvalue())
                samples = []
                digest = None
                for iteration in range(repeats + 1):
                    started = time.perf_counter_ns()
                    actual = _query(
                        source,
                        paths,
                        query,
                        batch_size,
                        enable_projection_pushdown=enable_projection_pushdown,
                        enable_filter_pushdown=enable_filter_pushdown,
                    ).to_arrow()
                    elapsed = (time.perf_counter_ns() - started) / 1_000_000
                    digest = _check_result(query, actual, table, layout["expected_results"])
                    if iteration:
                        samples.append(elapsed)
                record.update(
                    status="passed",
                    samples_ms=samples,
                    median_ms=statistics.median(samples),
                    min_ms=min(samples),
                    max_ms=max(samples),
                    output_checksum=digest,
                    plan=str(plan_path.relative_to(output.parent)),
                )
            except Exception as error:
                logger.exception("Baseline case failed: %s/%s", layout_name, query)
                record.update(status="failed", error=f"{type(error).__name__}: {error}")
            report["cases"].append(record)
    report["status"] = "passed" if all(case["status"] == "passed" for case in report["cases"]) else "failed"
    output.write_text(json.dumps(report, indent=2, sort_keys=True, allow_nan=False) + "\n")
    return report


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--manifest", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--source", choices=("parquet", "vortex-reference", "vortex-native"), default="parquet")
    parser.add_argument("--repeats", type=int, default=5)
    parser.add_argument("--batch-size", type=int, default=7)
    parser.add_argument("--disable-projection-pushdown", action="store_false", dest="enable_projection_pushdown")
    parser.add_argument("--disable-filter-pushdown", action="store_false", dest="enable_filter_pushdown")
    args = parser.parse_args()
    try:
        result = run_baseline(
            args.manifest,
            args.output,
            source=args.source,
            repeats=args.repeats,
            batch_size=args.batch_size,
            enable_projection_pushdown=args.enable_projection_pushdown,
            enable_filter_pushdown=args.enable_filter_pushdown,
        )
    except (ValueError, OSError, KeyError) as error:
        parser.exit(1, f"Baseline setup failed: {error}\n")
    passed = sum(case["status"] == "passed" for case in result["cases"])
    print(
        json.dumps(
            {
                "status": result["status"],
                "cases_passed": passed,
                "cases": len(result["cases"]),
                "report": str(args.output),
            }
        )
    )
    raise SystemExit(0 if result["status"] == "passed" else 1)


if __name__ == "__main__":
    main()
