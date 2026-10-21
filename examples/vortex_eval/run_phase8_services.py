"""Run Phase8 paired profiling on an owned temporary MinIO bucket.

Provisioning, uploads, service snapshots and cleanup are outside query timers.
Every error is retained. This diagnosis does not replace final performance acceptance.
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
from benchmarking.vortex.phase7 import validate
from benchmarking.vortex.phase8 import EXPERIMENTS, QUERY_CASES, require_api
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
    parser.add_argument("--datasets", nargs="+", default=["sorted/multi"])
    parser.add_argument("--experiments", nargs="+", choices=EXPERIMENTS, default=list(EXPERIMENTS))
    parser.add_argument("--cases", nargs="+", choices=QUERY_CASES, default=list(QUERY_CASES))
    parser.add_argument("--stage", choices=("diagnose", "final"), default="diagnose")
    parser.add_argument("--selection", type=Path)
    parser.add_argument("--sessions", type=int, default=3)
    parser.add_argument("--rounds", type=int)
    parser.add_argument("--warmups", type=int)
    parser.add_argument("--diagnostic-rounds", type=int, default=3)
    parser.add_argument("--seed", type=int, default=173)
    parser.add_argument("--timeout", type=float, default=600)
    parser.add_argument("--allow-dev", action="store_true", help="Correctness smoke only")
    args = parser.parse_args(argv)
    args.rounds = (50 if args.stage == "final" else 20) if args.rounds is None else args.rounds
    args.warmups = (3 if args.stage == "final" else 1) if args.warmups is None else args.warmups
    if args.stage == "final" and args.selection is None:
        parser.error("Final acceptance requires --selection")
    try:
        validate(
            args.cases,
            args.datasets,
            ("aa",),
            args.rounds,
            args.warmups,
            args.diagnostic_rounds,
            args.timeout,
            args.allow_dev,
        )
    except ValueError as error:
        parser.error(str(error))
    manifest_path = args.manifest.resolve()
    manifest = load_manifest(manifest_path)
    if not set(args.datasets) <= manifest["datasets"].keys():
        parser.error("Manifest does not contain every requested dataset")
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
    bucket = f"vortex-phase8-{uuid.uuid4().hex[:20]}"
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
        paths = {
            relative
            for dataset in args.datasets
            for values in manifest["datasets"][dataset]["paths"].values()
            for relative in values
        }
        for relative in sorted(paths):
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
            "benchmarking.vortex.phase8",
            "--stage",
            args.stage,
            "--sessions",
            str(args.sessions),
            "--manifest",
            str(manifest_path),
            "--output",
            str(output),
            "--storage-root",
            f"s3://{bucket}",
            "--datasets",
            *args.datasets,
            "--experiments",
            *args.experiments,
            "--cases",
            *args.cases,
            "--rounds",
            str(args.rounds),
            "--warmups",
            str(args.warmups),
            "--diagnostic-rounds",
            str(args.diagnostic_rounds),
            "--seed",
            str(args.seed),
            "--timeout",
            str(args.timeout),
        ]
        if args.selection:
            command += ["--selection", str(args.selection.resolve())]
        if args.allow_dev:
            command.append("--allow-dev")
        before_evaluation = _service_snapshot(environment, container)
        result = subprocess.run(command, cwd=Path(__file__).resolve().parents[2], env=environment, check=False)
        evaluation_returncode = result.returncode
        if result.returncode:
            return result.returncode
        report_path = output / "sessions.json"
        if not report_path.is_file():
            raise RuntimeError("Remote evaluation process produced no report")
        report = json.loads(report_path.read_text())
        if report.get("status") != "passed":
            raise RuntimeError("Phase8 diagnostic report did not pass correctness/provenance checks")
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
    print(f"Phase8 remote evaluation completed; reports: {output / 'sessions.json'}")
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except (RuntimeError, OSError, ValueError) as error:
        print(f"Phase8 service evaluation failed: {error}", file=sys.stderr)
        raise SystemExit(1) from None
