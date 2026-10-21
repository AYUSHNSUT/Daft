"""Run Phase6 policy tuning/final evaluation on an owned temporary MinIO bucket.

Provisioning, uploads, service snapshots and cleanup are outside query timers.
Every error is retained; a failed final median gate returns a nonzero exit code.
"""

from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
import uuid
from pathlib import Path

from benchmarking.vortex.fixtures import load_manifest
from benchmarking.vortex.remote_eval import (
    DEFAULT_POLICY,
    POLICIES,
    QUERY_CASES,
    load_selection,
    require_api,
    validate_options,
)
from examples.vortex_eval.run_phase3_services import _start_minio
from examples.vortex_eval.run_phase4_services import _cleanup_bucket, _redact, _validate_endpoint
from examples.vortex_eval.run_phase5_services import _service_snapshot


def _require_environment(allow_dev):
    os.environ["DAFT_ANALYTICS_ENABLED"] = "0"
    require_api()
    if not allow_dev:
        import daft

        if daft.daft.build_type() != "release" or not os.environ.get("DAFT_VORTEX_BENCH_BUILD_COMMAND"):
            raise RuntimeError("Comparisons require a release build and DAFT_VORTEX_BENCH_BUILD_COMMAND")


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--manifest", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--docker-image", help="Start an explicit local MinIO image unless an endpoint is set")
    parser.add_argument("--order", choices=("sorted", "shuffled"), default="sorted")
    parser.add_argument("--layout", choices=("single", "multi"), default="multi")
    parser.add_argument("--stage", choices=("tune", "final"), default="tune")
    parser.add_argument("--candidate-policy", choices=POLICIES, default=DEFAULT_POLICY)
    parser.add_argument("--selection", type=Path)
    parser.add_argument("--cases", nargs="+", choices=QUERY_CASES, default=list(QUERY_CASES))
    parser.add_argument("--rounds", type=int)
    parser.add_argument("--warmups", type=int)
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--timeout", type=float, default=600)
    parser.add_argument("--execution-mode", choices=("fresh-process", "persistent-process"), default="fresh-process")
    parser.add_argument("--diagnostics", action="store_true")
    parser.add_argument("--allow-dev", action="store_true", help="Correctness smoke only; no performance gate")
    args = parser.parse_args(argv)
    rounds = (50 if args.stage == "final" else 20) if args.rounds is None else args.rounds
    warmups = (3 if args.stage == "final" else 1) if args.warmups is None else args.warmups
    selection_path = args.selection.resolve() if args.selection else None
    if selection_path:
        selected = load_selection(selection_path, args.allow_dev, execution_mode=args.execution_mode)
        if args.stage != "final":
            parser.error("A frozen selection applies only to the final stage")
        args.candidate_policy = selected["policy"]
    try:
        validate_options(args.stage, args.candidate_policy, args.cases, rounds, warmups, args.allow_dev, args.timeout)
    except ValueError as error:
        parser.error(str(error))
    manifest_path = args.manifest.resolve()
    manifest = load_manifest(manifest_path)
    dataset_key = f"{args.order}/{args.layout}"
    if dataset_key not in manifest["datasets"]:
        parser.error(f"Manifest does not contain dataset {dataset_key}")
    output = args.output.resolve()
    if output.exists() and (not output.is_dir() or any(output.iterdir())):
        raise FileExistsError("Report directory must be absent or empty")
    _require_environment(args.allow_dev)

    import boto3
    from botocore.config import Config

    environment = {**os.environ, "DAFT_RUNNER": "native", "DAFT_PROGRESS_BAR": "0", "DAFT_ANALYTICS_ENABLED": "0"}
    container, client = None, None
    created_bucket = False
    before_evaluation, evaluation_returncode = None, None
    bucket = f"vortex-phase6-{uuid.uuid4().hex[:20]}"
    try:
        if not environment.get("VORTEX_MINIO_ENDPOINT"):
            if not args.docker_image:
                parser.error("Set VORTEX_MINIO_ENDPOINT or supply --docker-image")
            container = _start_minio(args.docker_image, environment)
        endpoint = environment["VORTEX_MINIO_ENDPOINT"]
        _validate_endpoint(endpoint)
        key_id, access_key = environment.get("VORTEX_MINIO_ACCESS_KEY"), environment.get("VORTEX_MINIO_SECRET_KEY")
        if not key_id or not access_key:
            raise RuntimeError("Set both VORTEX_MINIO_ACCESS_KEY and VORTEX_MINIO_SECRET_KEY")
        client = boto3.client(
            "s3",
            endpoint_url=endpoint,
            region_name="us-east-1",
            aws_access_key_id=key_id,
            aws_secret_access_key=access_key,
            config=Config(signature_version="s3v4", s3={"addressing_style": "path"}),
        )
        client.create_bucket(Bucket=bucket)
        created_bucket = True
        for values in manifest["datasets"][dataset_key]["paths"].values():
            for relative in values:
                client.upload_file(str(manifest_path.parent / relative), bucket, relative)
        environment["DAFT_VORTEX_BENCH_IO_JSON"] = json.dumps(
            {
                "s3": {
                    "endpoint_url": endpoint,
                    "region_name": "us-east-1",
                    "key_id": key_id,
                    "access_key": access_key,
                    "use_ssl": endpoint.startswith("https://"),
                    "force_virtual_addressing": False,
                    "num_tries": 1,
                }
            }
        )
        command = [
            sys.executable,
            "-m",
            "benchmarking.vortex.remote_eval",
            "--manifest",
            str(manifest_path),
            "--output",
            str(output),
            "--storage-root",
            f"s3://{bucket}",
            "--orders",
            args.order,
            "--layouts",
            args.layout,
            "--stage",
            args.stage,
            "--candidate-policy",
            args.candidate_policy,
            "--cases",
            *args.cases,
            "--rounds",
            str(rounds),
            "--warmups",
            str(warmups),
            "--seed",
            str(args.seed),
            "--timeout",
            str(args.timeout),
            "--execution-mode",
            args.execution_mode,
        ]
        if selection_path:
            command += ["--selection", str(selection_path)]
        if args.diagnostics:
            command.append("--diagnostics")
        if args.allow_dev:
            command.append("--allow-dev")
        before_evaluation = _service_snapshot(environment, container)
        result = subprocess.run(command, cwd=Path(__file__).resolve().parents[2], env=environment, check=False)
        evaluation_returncode = result.returncode
        if result.returncode:
            return result.returncode
        report_path = output / "report.json"
        if not report_path.is_file():
            raise RuntimeError("Remote evaluation process produced no report")
        report = json.loads(report_path.read_text())
        if report.get("status") != "passed" or report.get("experiment") != "remote_read_policy":
            raise RuntimeError("Remote evaluation report did not pass")
        settings = report.get("settings")
        if not isinstance(settings, dict) or settings.get("execution_mode") != args.execution_mode:
            raise RuntimeError("Remote evaluation report execution mode differs from the requested comparison")
        if (
            args.stage == "final"
            and not args.allow_dev
            and report.get("performance_gate", {}).get("status") != "passed"
        ):
            raise RuntimeError("Final all-query performance gate did not pass")
    except Exception as error:
        raise RuntimeError(_redact(str(error), environment)) from None
    finally:
        try:
            diagnostics = {
                "scope": "Snapshots before/after evaluation only; no polling during timed trials",
                "evaluation_returncode": evaluation_returncode,
                "before_evaluation": before_evaluation,
                "before_cleanup": _service_snapshot(environment, container),
            }
            output.mkdir(parents=True, exist_ok=True)
            (output / "service-diagnostics.json").write_text(json.dumps(diagnostics, indent=2, sort_keys=True) + "\n")
        except Exception as error:
            print(f"Could not save service diagnostics: {_redact(str(error), environment)}", file=sys.stderr)
        try:
            if created_bucket:
                _cleanup_bucket(client, bucket)
        except Exception as error:
            raise RuntimeError(f"Temporary bucket cleanup failed: {_redact(str(error), environment)}") from None
        finally:
            if container is not None:
                cleanup = subprocess.run(
                    ["docker", "rm", "--force", container], capture_output=True, text=True, check=False
                )
                if cleanup.returncode:
                    raise RuntimeError(
                        f"Temporary container cleanup failed: {_redact(cleanup.stderr or cleanup.stdout, environment)}"
                    )
    print(f"Phase6 remote evaluation completed; report: {output / 'report.md'}")
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except (RuntimeError, OSError, ValueError) as error:
        print(f"Phase6 service evaluation failed: {error}", file=sys.stderr)
        raise SystemExit(1) from None
