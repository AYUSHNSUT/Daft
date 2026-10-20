"""Compare metadata concurrency against a disposable local MinIO bucket.

Run as ``python -m examples.vortex_eval.run_phase5_services`` from the checkout.
Existing local fixtures are uploaded before timing. Each metadata or query trial
runs in a fresh process; service startup, uploads and cleanup are not measured.
The S3-compatible service is local development storage, not a cloud benchmark.
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
from benchmarking.vortex.metadata_eval import DEFAULT_CASES, QUERY_CASES, require_api, validate_options
from examples.vortex_eval.run_phase3_services import _start_minio
from examples.vortex_eval.run_phase4_services import _cleanup_bucket, _redact, _validate_endpoint


def _require_environment(allow_dev: bool) -> None:
    os.environ["DAFT_ANALYTICS_ENABLED"] = "0"
    require_api()
    if not allow_dev:
        import daft

        if daft.daft.build_type() != "release" or not os.environ.get("DAFT_VORTEX_BENCH_BUILD_COMMAND"):
            raise RuntimeError("Comparisons require a release build and DAFT_VORTEX_BENCH_BUILD_COMMAND")


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--manifest", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--docker-image", help="Start this explicit local MinIO image unless an endpoint is set")
    parser.add_argument("--order", choices=["sorted", "shuffled"], default="sorted")
    parser.add_argument("--layout", choices=["single", "multi"], default="multi")
    parser.add_argument("--cases", nargs="+", choices=QUERY_CASES, default=list(DEFAULT_CASES))
    parser.add_argument("--modes", nargs="+", choices=["query", "metadata"], default=["query", "metadata"])
    parser.add_argument("--rounds", type=int, default=20)
    parser.add_argument("--warmups", type=int, default=1)
    parser.add_argument("--concurrency", type=int, nargs="+", default=[1, 8])
    parser.add_argument("--timeout", type=float, default=600)
    parser.add_argument("--allow-dev", action="store_true", help="Correctness smoke only; no performance claim")
    args = parser.parse_args(argv)
    try:
        validate_options(
            args.concurrency, args.cases, args.modes, args.rounds, args.warmups, args.allow_dev, args.timeout
        )
    except ValueError as error:
        parser.error(str(error))

    # Reject invalid local fixtures and output paths before contacting the service.
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
    container = None
    client = None
    created_bucket = False
    bucket = f"vortex-phase5-{uuid.uuid4().hex[:20]}"
    try:
        if not environment.get("VORTEX_MINIO_ENDPOINT"):
            if not args.docker_image:
                parser.error("Set VORTEX_MINIO_ENDPOINT or supply --docker-image")
            container = _start_minio(args.docker_image, environment)
        endpoint = environment["VORTEX_MINIO_ENDPOINT"]
        _validate_endpoint(endpoint)
        key_id = environment.get("VORTEX_MINIO_ACCESS_KEY")
        access_key = environment.get("VORTEX_MINIO_SECRET_KEY")
        if not key_id or not access_key:
            raise RuntimeError("Set both VORTEX_MINIO_ACCESS_KEY and VORTEX_MINIO_SECRET_KEY for the developer service")
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
        dataset = manifest["datasets"][dataset_key]
        for relative in [relative for values in dataset["paths"].values() for relative in values]:
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
            "benchmarking.vortex.metadata_eval",
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
            "--cases",
            *args.cases,
            "--modes",
            *args.modes,
            "--concurrency",
            *(str(value) for value in args.concurrency),
            "--rounds",
            str(args.rounds),
            "--warmups",
            str(args.warmups),
            "--timeout",
            str(args.timeout),
        ]
        if args.allow_dev:
            command.append("--allow-dev")
        result = subprocess.run(command, cwd=Path(__file__).resolve().parents[2], env=environment, check=False)
        if result.returncode:
            return result.returncode
        report_path = output / "report.json"
        if not report_path.is_file():
            raise RuntimeError("Metadata evaluation process produced no report")
        report = json.loads(report_path.read_text())
        if report.get("status") != "passed" or report.get("experiment") != "metadata_concurrency":
            raise RuntimeError("Metadata evaluation report did not pass")
    except Exception as error:
        raise RuntimeError(_redact(str(error), environment)) from None
    finally:
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
                    detail = _redact(cleanup.stderr or cleanup.stdout, environment)
                    raise RuntimeError(f"Temporary container cleanup failed (exit {cleanup.returncode}): {detail}")
    print(f"S3-compatible metadata comparison completed; report: {output / 'report.md'}")
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except (RuntimeError, OSError, ValueError) as error:
        print(f"Phase 5 service evaluation failed: {error}", file=sys.stderr)
        raise SystemExit(1) from None
