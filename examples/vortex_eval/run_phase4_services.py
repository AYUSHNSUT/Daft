"""Benchmark a selected native reader matrix on a disposable MinIO bucket.

Run as ``python -m examples.vortex_eval.run_phase4_services`` from the checkout.
Generate local fixtures first. Uploads, service startup, and cleanup are outside
the benchmark timer. The storage is a local S3-compatible development service;
these results do not represent a public cloud network.
"""

from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
import uuid
from pathlib import Path
from typing import Any
from urllib.parse import urlsplit

from benchmarking.vortex.fixtures import load_manifest
from examples.vortex_eval.run_phase3_services import _start_minio


def _cleanup_bucket(client: Any, bucket: str) -> None:
    paginator = client.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=bucket):
        objects = [{"Key": item["Key"]} for item in page.get("Contents", [])]
        if objects:
            client.delete_objects(Bucket=bucket, Delete={"Objects": objects})
    client.delete_bucket(Bucket=bucket)


def _redact(message: str, environment: dict[str, str]) -> str:
    for name in ("VORTEX_MINIO_ACCESS_KEY", "VORTEX_MINIO_SECRET_KEY"):
        value = environment.get(name)
        if value:
            message = message.replace(value, "<redacted>")
    return message


def _validate_endpoint(endpoint: str) -> None:
    try:
        parsed = urlsplit(endpoint)
        valid = (
            parsed.scheme in {"http", "https"}
            and bool(parsed.hostname)
            and parsed.username is None
            and parsed.password is None
            and not parsed.query
            and not parsed.fragment
        )
        parsed.port
    except ValueError:
        valid = False
    if not valid:
        raise ValueError("MinIO endpoint must be an HTTP(S) URL without user information, query, or fragment")


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--manifest", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--docker-image", help="Start this explicit local MinIO image unless an endpoint is set")
    parser.add_argument("--order", choices=["sorted", "shuffled"], default="sorted")
    parser.add_argument("--layout", choices=["single", "multi"], default="multi")
    parser.add_argument("--cases", nargs="+", default=["scan_full", "scan_narrow", "range_0.01", "group_by", "join"])
    parser.add_argument("--configs", nargs="+", default=["parquet", "vortex_11"])
    parser.add_argument("--rounds", type=int, default=5)
    parser.add_argument("--warmups", type=int, default=1)
    parser.add_argument("--threads", type=int)
    parser.add_argument("--scan-tasks", type=int)
    parser.add_argument("--allow-dev", action="store_true", help="Correctness smoke only; no performance claim")
    args = parser.parse_args(argv)
    if args.rounds < (1 if args.allow_dev else 5) or args.warmups < (0 if args.allow_dev else 1):
        parser.error("Performance runs require at least five rounds and one warmup")
    if any(value is not None and value < 1 for value in (args.threads, args.scan_tasks)):
        parser.error("Thread and scan-task limits must be positive")

    # Validate every local file before creating or contacting the temporary bucket.
    manifest_path = args.manifest.resolve()
    manifest = load_manifest(manifest_path)
    dataset_key = f"{args.order}/{args.layout}"
    if dataset_key not in manifest["datasets"]:
        parser.error(f"Manifest does not contain dataset {dataset_key}")
    import boto3
    from botocore.config import Config

    os.environ["DAFT_ANALYTICS_ENABLED"] = "0"
    import daft

    if not args.allow_dev and daft.daft.build_type() != "release":
        parser.error("Rebuild with RUST_DAFT_PKG_BUILD_TYPE=release and maturin --release before measuring")
    if not hasattr(daft.daft, "read_vortex_schema"):
        parser.error("Rebuild with the python,vortex native features")

    environment = {**os.environ, "DAFT_RUNNER": "native", "DAFT_PROGRESS_BAR": "0", "DAFT_ANALYTICS_ENABLED": "0"}
    container = None
    client = None
    created_bucket = False
    bucket = f"vortex-phase4-{uuid.uuid4().hex[:20]}"
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
        paths = [relative for values in dataset["paths"].values() for relative in values]
        for relative in paths:
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
        output = args.output.resolve()
        command = [
            sys.executable,
            "-m",
            "benchmarking.vortex",
            "run",
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
            "--configs",
            *args.configs,
            "--cases",
            *args.cases,
            "--rounds",
            str(args.rounds),
            "--warmups",
            str(args.warmups),
        ]
        for option, value in [("--threads", args.threads), ("--scan-tasks", args.scan_tasks)]:
            if value is not None:
                command.extend([option, str(value)])
        if args.allow_dev:
            command.append("--allow-dev")
        result = subprocess.run(command, cwd=Path(__file__).resolve().parents[2], env=environment, check=False)
        if result.returncode:
            return result.returncode
        if not (output / "report.json").is_file():
            raise RuntimeError("Benchmark process produced no report")
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
    print(f"S3-compatible benchmark completed; report: {output / 'report.md'}")
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except (RuntimeError, OSError, ValueError) as error:
        print(f"Phase 4 service evaluation failed: {error}", file=sys.stderr)
        raise SystemExit(1) from None
