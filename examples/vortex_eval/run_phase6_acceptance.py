"""One-command terminal acceptance after an enabled release build is ready.

Optionally tune and freeze a policy, then run final performance before all
services/Python/Rust checks. The helper does not rebuild/install the Python
extension, retry trials, edit source or publish. Cargo may compile its test
binaries after the performance runs have finished.
"""

from __future__ import annotations

import argparse
import json
import os
import re
import subprocess
import sys
import time
import xml.etree.ElementTree as ET
from datetime import datetime, timezone
from pathlib import Path

from benchmarking.vortex.fixtures import file_hash, load_manifest
from benchmarking.vortex.remote_eval import (
    DEFAULT_POLICY,
    POLICIES,
    load_selection,
    require_api,
    validate_selection_identity,
)
from benchmarking.vortex.run import environment as benchmark_environment
from examples.vortex_eval.run_phase3_services import _require_environment, _start_minio
from examples.vortex_eval.run_phase4_services import _redact, _validate_endpoint
from examples.vortex_eval.run_phase5_services import _service_snapshot


def junit_counts(path):
    counts = {"tests": 0, "passed": 0, "failed": 0, "errors": 0, "skipped": 0}
    for case in ET.parse(path).getroot().iter("testcase"):
        counts["tests"] += 1
        status = (
            "failed"
            if case.find("failure") is not None
            else "errors"
            if case.find("error") is not None
            else "skipped"
            if case.find("skipped") is not None
            else "passed"
        )
        counts[status] += 1
    return counts


def run_gate(name, command, output, environment, *, junit=None, zero_skips=False, rust=False):
    log_path = output / f"{name}.log"
    began = time.perf_counter()
    result = {"command": command, "started_at_utc": datetime.now(timezone.utc).isoformat(), "log": log_path.name}
    print(f"Starting {name}; log: {log_path}", flush=True)
    with log_path.open("w") as log:
        try:
            with subprocess.Popen(
                command,
                cwd=Path(__file__).resolve().parents[2],
                env=environment,
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                text=True,
            ) as process:
                for line in process.stdout:
                    log.write(_redact(line, environment))
                    log.flush()
                code = process.wait()
        except OSError as error:
            log.write(_redact(str(error), environment) + "\n")
            code = 127
    result.update(
        returncode=code,
        elapsed_seconds=time.perf_counter() - began,
        completed_at_utc=datetime.now(timezone.utc).isoformat(),
        status="passed" if code == 0 else "failed",
    )
    (output / f"{name}-exit.txt").write_text(f"{code}\n")
    if junit is not None:
        try:
            junit.write_text(_redact(junit.read_text(), environment))
            result["junit"] = junit.name
            result["counts"] = counts = junit_counts(junit)
            if not counts["tests"] or counts["failed"] or counts["errors"] or (zero_skips and counts["skipped"]):
                result.update(status="failed", validation_error="JUnit coverage/failure/skip gate failed")
        except (OSError, ET.ParseError) as error:
            result.update(status="failed", validation_error=_redact(str(error), environment))
    if rust:
        matches = re.findall(
            r"test result: ok\. (\d+) passed; (\d+) failed; (\d+) ignored; (\d+) measured; (\d+) filtered out;",
            log_path.read_text(),
        )
        counts = {
            key: sum(int(row[index]) for row in matches)
            for index, key in enumerate(("passed", "failed", "ignored", "measured", "filtered_out"))
        }
        result.update(counts=counts, targets=len(matches))
        if (
            not matches
            or not counts["passed"]
            or counts["failed"]
            or (name == "workspace-rust" and counts["filtered_out"])
        ):
            result.update(status="failed", validation_error="Rust result coverage/filter gate failed")
    result["log_sha256"] = file_hash(log_path)
    print(f"Finished {name}: {result['status']} (exit {code})", flush=True)
    return result


def run_evaluation_gate(
    stage, manifest_path, output, environment, candidate_policy, selection=None, *, execution_mode="fresh-process"
):
    name = {"tune": "tuning", "final": "remote", "guard": "local-guard"}[stage]
    report_directory = output / ("remote-report" if stage == "final" else name)
    command = [
        sys.executable,
        "-m",
        "benchmarking.vortex.remote_eval" if stage == "guard" else "examples.vortex_eval.run_phase6_services",
        "--manifest",
        str(manifest_path),
        "--output",
        str(report_directory),
        "--stage",
        stage,
        "--candidate-policy",
        candidate_policy,
        "--rounds",
        "50" if stage == "final" else "20",
        "--warmups",
        "3" if stage == "final" else "1",
        "--diagnostics",
        "--execution-mode",
        execution_mode,
    ]
    if stage == "guard":
        command += ["--orders", "sorted", "shuffled", "--layouts", "single", "multi"]
    if selection is not None:
        command += ["--selection", str(selection)]
    gate = run_gate(name, command, output, environment)
    path = report_directory / "report.json"
    try:
        gate["report_sha256"] = file_hash(path)
        report = json.loads(path.read_text())
        if not isinstance(report, dict):
            raise ValueError("Benchmark report must be a JSON object")
        gate.update(
            correctness_status=report.get("correctness_status"),
            performance_gate=report.get("performance_gate"),
            diagnostics_status=report.get("diagnostics_status"),
        )
        if not isinstance(report.get("performance_gate"), dict):
            raise ValueError("Benchmark report is missing its performance gate")
        settings = report.get("settings")
        if not isinstance(settings, dict) or settings.get("execution_mode") != execution_mode:
            raise ValueError("Benchmark report execution mode differs from the requested comparison")
        if (
            report.get("status") != "passed"
            or report.get("correctness_status") != "passed"
            or report.get("diagnostics_status") != "passed"
            or (stage != "tune" and report["performance_gate"].get("status") != "passed")
        ):
            gate["status"] = "failed"
    except (OSError, ValueError) as error:
        gate.update(status="failed", validation_error=_redact(str(error), environment))
    return gate


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--manifest", type=Path, required=True)
    parser.add_argument(
        "--local-manifest", type=Path, help="Also guard all four local order/layout datasets with the same policy"
    )
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--docker-image", default="draft-vortex-minio:phase3")
    parser.add_argument("--candidate-policy", choices=POLICIES, default=DEFAULT_POLICY)
    parser.add_argument("--selection", type=Path)
    parser.add_argument("--execution-mode", choices=("fresh-process", "persistent-process"), default="fresh-process")
    parser.add_argument(
        "--tune", action="store_true", help="Run production tuning, then final acceptance with its frozen winner"
    )
    parser.add_argument("--expected-source")
    parser.add_argument("--expected-native-sha")
    args = parser.parse_args(argv)
    if args.tune and args.selection is not None:
        parser.error("--tune creates its own frozen selection; do not also pass --selection")
    os.environ["DAFT_ANALYTICS_ENABLED"] = "0"
    manifest_path, output = args.manifest.resolve(), args.output.resolve()
    load_manifest(manifest_path)
    local_manifest = args.local_manifest.resolve() if args.local_manifest else None
    if local_manifest is not None:
        local = load_manifest(local_manifest)
        if not {"sorted/single", "sorted/multi", "shuffled/single", "shuffled/multi"} <= local["datasets"].keys():
            parser.error("Local guard manifest must contain both orders and both layouts")
    if output.exists() and (not output.is_dir() or any(output.iterdir())):
        parser.error("Acceptance output must be absent or empty")
    require_api()
    _require_environment()
    provenance = benchmark_environment()
    if (
        provenance["build_type"] != "release"
        or not provenance["build_command"]
        or provenance["tracked_changes"]
        or not provenance["git_head"]
        or not provenance["native_binaries_sha256"]
    ):
        parser.error("Acceptance requires a clean tracked checkout and attested enabled release build")
    if args.expected_source and provenance["git_head"] != args.expected_source:
        parser.error("Source does not match --expected-source")
    if args.expected_native_sha and set(provenance["native_binaries_sha256"].values()) != {args.expected_native_sha}:
        parser.error("Native binary does not match --expected-native-sha")
    if args.selection:
        args.selection = args.selection.resolve()
        selected = load_selection(args.selection, execution_mode=args.execution_mode)
        validate_selection_identity(
            selected, provenance, file_hash(manifest_path), "final", execution_mode=args.execution_mode
        )
        args.candidate_policy = selected["policy"]
    output.mkdir(parents=True, exist_ok=True)
    report = {
        "status": "running",
        "environment": provenance,
        "manifest": str(manifest_path),
        "manifest_sha256": file_hash(manifest_path),
        "local_manifest": str(local_manifest) if local_manifest is not None else None,
        "local_manifest_sha256": file_hash(local_manifest) if local_manifest is not None else None,
        "gates": {},
        "tuning_requested": args.tune,
        "execution_mode": args.execution_mode,
        "scope": "Optional tuning, final comparison and optional local guard run sequentially before all test processes; no overlapping builds/tests or retries",
    }
    (output / "source-revision.txt").write_text(provenance["git_head"] + "\n")
    (output / "tracked-changes.txt").write_text("")
    report_path = output / "acceptance.json"

    def save():
        report_path.write_text(json.dumps(report, indent=2, sort_keys=True, allow_nan=False) + "\n")

    save()
    child_env = {**os.environ, "DAFT_RUNNER": "native", "DAFT_PROGRESS_BAR": "0", "DAFT_ANALYTICS_ENABLED": "0"}
    container = None
    primary_error = None

    def record_failure(error):
        message = _redact(str(error), child_env)
        report["status"] = "failed"
        report.setdefault("failure", message)
        report.setdefault("exception_type", type(error).__name__)
        return message

    try:
        if not child_env.get("VORTEX_MINIO_ENDPOINT"):
            container = _start_minio(args.docker_image, child_env)
        _validate_endpoint(child_env["VORTEX_MINIO_ENDPOINT"])
        report["service_before_benchmark"] = _service_snapshot(child_env, container)
        final_allowed = True
        if args.tune:
            report["gates"]["tuning"] = run_evaluation_gate(
                "tune", manifest_path, output, child_env, args.candidate_policy, execution_mode=args.execution_mode
            )
            save()
            try:
                if report["gates"]["tuning"]["status"] != "passed":
                    raise ValueError("Tuning failed; no frozen selection was accepted")
                args.selection = output / "tuning" / "selected-policy.json"
                selected = load_selection(args.selection, execution_mode=args.execution_mode)
                validate_selection_identity(
                    selected, provenance, report["manifest_sha256"], "final", execution_mode=args.execution_mode
                )
                between = benchmark_environment()
                report["environment_between_benchmarks"] = between
                if any(
                    provenance[key] != between[key]
                    for key in (
                        "git_head",
                        "tracked_changes",
                        "native_binaries_sha256",
                        "cargo_lock_sha256",
                        "harness_sha256",
                    )
                ):
                    raise ValueError("Source/native provenance changed between tuning and final acceptance")
                validate_selection_identity(
                    selected, between, report["manifest_sha256"], "final", execution_mode=args.execution_mode
                )
                args.candidate_policy = selected["policy"]
                report["frozen_selection"] = {
                    "path": "tuning/selected-policy.json",
                    "sha256": file_hash(args.selection),
                    "policy": selected["policy"],
                    "tuning_report_sha256": selected["tuning_report_sha256"],
                    "execution_mode": args.execution_mode,
                }
            except (OSError, ValueError) as error:
                final_allowed = False
                reason = _redact(str(error), child_env)
                report["gates"]["tuning"].update(status="failed", selection_validation_error=reason)
                report["gates"]["remote"] = {
                    "status": "blocked",
                    "returncode": None,
                    "executed": False,
                    "depends_on": "tuning",
                    "reason": reason,
                }
                save()
        if final_allowed:
            report["gates"]["remote"] = run_evaluation_gate(
                "final",
                manifest_path,
                output,
                child_env,
                args.candidate_policy,
                args.selection,
                execution_mode=args.execution_mode,
            )
        save()
        if local_manifest is not None:
            report["gates"]["local-guard"] = (
                run_evaluation_gate(
                    "guard",
                    local_manifest,
                    output,
                    child_env,
                    args.candidate_policy,
                    args.selection,
                    execution_mode=args.execution_mode,
                )
                if final_allowed
                else dict(report["gates"]["remote"])
            )
        report["service_after_benchmark"] = _service_snapshot(child_env, container)
        save()
        python_groups = [
            ("services", ["-m", "integration", "tests/integration/io/vortex"], True),
            ("orc-mcap", ["tests/io/test_orc.py", "tests/io/mcap/test_mcap.py"], False),
            (
                "focused",
                [
                    "tests/io/test_vortex.py",
                    "tests/io/test_vortex_discovery.py",
                    "tests/io/test_vortex_diagnostic_capture.py",
                    "tests/io/test_vortex_native_tasks.py",
                    "tests/io/test_vortex_pushdowns.py",
                    "tests/io/test_vortex_statistics.py",
                    "tests/io/test_vortex_metadata_reuse.py",
                    "tests/io/test_vortex_phase3.py",
                    "tests/io/test_vortex_lifecycle.py",
                    "benchmarking/vortex/test_remote_eval.py",
                    "benchmarking/vortex/test_persistent.py",
                    "benchmarking/vortex/test_persistent_schedule.py",
                    "examples/vortex_eval/test_phase6_services.py",
                ],
                True,
            ),
        ]
        for name, paths, zero_skips in python_groups:
            xml = output / f"{name}.xml"
            command = [sys.executable, "-m", "pytest", "-n", "0", "-q", "-rs", f"--junitxml={xml}", *paths]
            report["gates"][name] = run_gate(name, command, output, child_env, junit=xml, zero_skips=zero_skips)
            save()
        report["gates"]["s3-pooling"] = run_gate(
            "s3-pooling",
            [
                "cargo",
                "test",
                "--release",
                "--offline",
                "--locked",
                "--no-default-features",
                "-p",
                "daft-io",
                "--lib",
                "test_s3_http_client_",
            ],
            output,
            child_env,
            rust=True,
        )
        save()
        rust_env = {
            key: value for key, value in child_env.items() if key not in {"SSL_CERT_DIR", "RUST_DAFT_PKG_BUILD_TYPE"}
        }
        rust_env.update(SSL_CERT_FILE="/etc/ssl/cert.pem", CI="1", CARGO_BUILD_JOBS="1", DAFT_DASHBOARD_SKIP_BUILD="1")
        report["gates"]["workspace-rust"] = run_gate(
            "workspace-rust",
            ["cargo", "test", "--offline", "--locked", "--no-default-features", "--workspace", "--no-fail-fast"],
            output,
            rust_env,
            rust=True,
        )
        report["environment_after"] = benchmark_environment()
        report["provenance_unchanged"] = all(
            provenance[key] == report["environment_after"][key]
            for key in ("git_head", "tracked_changes", "native_binaries_sha256", "cargo_lock_sha256", "harness_sha256")
        )
        report["status"] = (
            "passed"
            if report["provenance_unchanged"] and all(gate["status"] == "passed" for gate in report["gates"].values())
            else "failed"
        )
    except BaseException as error:
        primary_error = error
        message = record_failure(error)
        if isinstance(error, Exception):
            raise RuntimeError(message) from error
        raise
    finally:
        try:
            report["service_before_cleanup"] = _service_snapshot(child_env, container)
        except Exception as error:
            report["service_snapshot_error"] = record_failure(error)
            report["service_snapshot_exception_type"] = type(error).__name__
        if container is not None:
            try:
                cleanup = subprocess.run(
                    ["docker", "rm", "--force", container], capture_output=True, text=True, check=False
                )
                report["container_cleanup_returncode"] = cleanup.returncode
                if cleanup.returncode:
                    raise RuntimeError(
                        cleanup.stderr or cleanup.stdout or "Docker cleanup returned a nonzero exit code"
                    )
            except Exception as error:
                report["cleanup_error"] = record_failure(error)
                report["cleanup_exception_type"] = type(error).__name__
        try:
            save()
        except Exception as error:
            if primary_error is None:
                raise
            print(f"Could not persist failed acceptance report: {_redact(str(error), child_env)}", file=sys.stderr)
    print(f"Phase6 acceptance: {report['status']}; {report_path}")
    return 0 if report["status"] == "passed" else 1


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except (RuntimeError, OSError, ValueError) as error:
        print(f"Phase6 acceptance failed: {error}", file=sys.stderr)
        raise SystemExit(1) from None
