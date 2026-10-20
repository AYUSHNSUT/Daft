#!/usr/bin/env python3
"""Run strict local HTTP/MinIO/Ray acceptance without service-related skips.

Supply VORTEX_MINIO_ENDPOINT and local developer credentials, or pass a MinIO
image explicitly with --docker-image. No production bucket is used. The tests
create and remove unique temporary buckets, and start their own HTTP listener.
"""

from __future__ import annotations

import argparse
import importlib.util
import json
import os
import secrets
import socket
import subprocess
import sys
import tempfile
import time
import urllib.error
import urllib.request
import uuid
import xml.etree.ElementTree as ET
from pathlib import Path


def _require_environment() -> None:
    import daft

    native = daft.daft
    if not hasattr(native, "read_vortex_schema"):
        raise RuntimeError("Rebuild this checkout with Rust features python,vortex before service acceptance")
    try:
        config = native.VortexSourceConfig(max_concurrent_requests=2, max_in_flight_bytes=1024 * 1024)
    except TypeError as error:
        raise RuntimeError("The installed extension predates Phase 3; rebuild it before service acceptance") from error
    assert config.max_concurrent_requests == 2
    for module in ["pytest", "vortex", "ray", "boto3"]:
        if importlib.util.find_spec(module) is None:
            raise RuntimeError(
                f"Install the optional service-test prerequisite {module!r} into this Python environment"
            )
    with socket.socket() as probe:
        try:
            probe.bind(("127.0.0.1", 0))
        except OSError as error:
            raise RuntimeError(
                "Local service acceptance requires permission to bind loopback sockets; this environment rejected it"
            ) from error


def _start_minio(image: str, environment: dict[str, str]) -> str:
    name = f"daft-vortex-acceptance-{uuid.uuid4().hex[:12]}"
    environment["VORTEX_MINIO_ACCESS_KEY"] = "vortexdev"
    environment["VORTEX_MINIO_SECRET_KEY"] = secrets.token_hex(24)
    container_environment = {
        **environment,
        "MINIO_ROOT_USER": environment["VORTEX_MINIO_ACCESS_KEY"],
        "MINIO_ROOT_PASSWORD": environment["VORTEX_MINIO_SECRET_KEY"],
    }
    result = subprocess.run(
        [
            "docker",
            "run",
            "--rm",
            "--detach",
            "--name",
            name,
            "--publish",
            "127.0.0.1::9000",
            "--env",
            "MINIO_ROOT_USER",
            "--env",
            "MINIO_ROOT_PASSWORD",
            image,
            "server",
            "/data",
        ],
        env=container_environment,
        text=True,
        capture_output=True,
        check=False,
    )
    if result.returncode:
        detail = result.stderr.strip() or result.stdout.strip() or "Docker returned no diagnostic output"
        for credential in (container_environment["MINIO_ROOT_USER"], container_environment["MINIO_ROOT_PASSWORD"]):
            detail = detail.replace(credential, "<redacted>")
        raise RuntimeError(f"Unable to start MinIO image {image!r} (docker run exited {result.returncode}):\n{detail}")
    try:
        inspected = subprocess.run(["docker", "inspect", name], text=True, capture_output=True, check=True)
        port = json.loads(inspected.stdout)[0]["NetworkSettings"]["Ports"]["9000/tcp"][0]["HostPort"]
        endpoint = f"http://127.0.0.1:{port}"
        environment["VORTEX_MINIO_ENDPOINT"] = endpoint
        deadline = time.monotonic() + 45
        while True:
            try:
                with urllib.request.urlopen(endpoint + "/minio/health/ready", timeout=2) as response:
                    if response.status == 200:
                        return name
            except (OSError, urllib.error.URLError):
                if time.monotonic() >= deadline:
                    raise RuntimeError("Temporary MinIO did not become ready within 45 seconds") from None
            time.sleep(0.2)
    except BaseException:
        subprocess.run(["docker", "rm", "--force", name], capture_output=True, check=False)
        raise


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--docker-image", help="Explicit MinIO image to start temporarily when no endpoint is configured"
    )
    args = parser.parse_args()
    _require_environment()
    root = Path(__file__).resolve().parents[2]
    environment = {**os.environ, "DAFT_RUNNER": "native", "DAFT_PROGRESS_BAR": "0"}
    container = None
    if not environment.get("VORTEX_MINIO_ENDPOINT"):
        if not args.docker_image:
            parser.error(
                "Set VORTEX_MINIO_ENDPOINT for a local MinIO service, or supply --docker-image with a MinIO image"
            )
        container = _start_minio(args.docker_image, environment)
    try:
        with tempfile.TemporaryDirectory(prefix="daft-vortex-services-") as temporary:
            report = Path(temporary) / "results.xml"
            command = [
                sys.executable,
                "-m",
                "pytest",
                "-n",
                "0",
                "-m",
                "integration",
                "-q",
                "-rs",
                f"--junitxml={report}",
                "tests/integration/io/vortex",
            ]
            result = subprocess.run(command, cwd=root, env=environment, check=False)
            if result.returncode:
                return result.returncode
            suites = ET.parse(report).getroot().iter("testsuite")
            counts = [suite.attrib for suite in suites]
            if not counts or sum(int(count.get("tests", 0)) for count in counts) == 0:
                raise RuntimeError("Service acceptance collected no tests")
            if any(int(count.get("skipped", 0)) for count in counts):
                raise RuntimeError(
                    "Service acceptance requires zero skipped tests; install or enable the missing prerequisites"
                )
            print("HTTP Range, authenticated MinIO, and Ray service acceptance passed with zero skips.")
            return 0
    finally:
        if container is not None:
            subprocess.run(["docker", "rm", "--force", container], capture_output=True, check=False)


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except (RuntimeError, OSError) as error:
        print(f"Phase 3 service acceptance failed: {error}", file=sys.stderr)
        raise SystemExit(1) from None
