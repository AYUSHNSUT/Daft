"""Run the frozen service and remote checks outside a restricted agent session."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import subprocess
from datetime import datetime, timezone
from pathlib import Path


def identity(root):
    source = subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=root, text=True).strip()
    clean = subprocess.run(["git", "diff", "--quiet", "HEAD"], cwd=root, check=False).returncode == 0
    native = root / "daft/daft.abi3.so"
    return {"source": source, "native_sha256": hashlib.sha256(native.read_bytes()).hexdigest(), "clean": clean}


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--frozen", type=Path, required=True)
    parser.add_argument("--docker-image", default="draft-vortex-minio:phase3")
    args = parser.parse_args(argv)
    root = Path(__file__).resolve().parents[2]
    frozen = json.loads(args.frozen.read_text())
    if identity(root) != frozen["identity"]:
        raise RuntimeError("Source/native identity or tracked source differs from the frozen build")
    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    output = root / f"examples/vortex_eval/artifacts/phase8/terminal-{stamp}-{os.getpid()}"
    output.mkdir(parents=True, exist_ok=False)
    environment = {
        **os.environ,
        "DAFT_RUNNER": "native",
        "DAFT_PROGRESS_BAR": "0",
        "DAFT_ANALYTICS_ENABLED": "0",
        "DAFT_VORTEX_BENCH_BUILD_COMMAND": frozen["build_command"],
        "CI": "1",
        "CARGO_BUILD_JOBS": "2",
        "CARGO_INCREMENTAL": "0",
    }
    python = str(root / ".venv/bin/python")
    jobs = [
        (
            "remote",
            [
                python,
                "-m",
                "examples.vortex_eval.run_phase8_services",
                "--manifest",
                "examples/vortex_eval/artifacts/phase4/remote-fixtures/manifest.json",
                "--output",
                str(output / "remote"),
                "--docker-image",
                args.docker_image,
                "--sessions",
                "3",
            ],
        ),
        ("services", [python, "examples/vortex_eval/run_phase3_services.py", "--docker-image", args.docker_image]),
        (
            "orc-mcap",
            [python, "-m", "pytest", "-n", "0", "-q", "-rs", "tests/io/test_orc.py", "tests/io/mcap/test_mcap.py"],
        ),
        (
            "s3-pooling",
            [
                "cargo",
                "test",
                "--offline",
                "--locked",
                "--release",
                "--no-default-features",
                "-p",
                "daft-io",
                "s3_like::tests::test_s3_http_client",
                "--",
                "--nocapture",
            ],
        ),
        (
            "workspace-rust",
            [
                "cargo",
                "test",
                "--offline",
                "--locked",
                "--release",
                "--no-default-features",
                "--workspace",
                "--no-fail-fast",
            ],
        ),
    ]
    report = {"status": "running", "frozen": frozen, "checks": []}
    for name, command in jobs:
        if identity(root) != frozen["identity"]:
            raise RuntimeError("Frozen source/native identity changed during validation")
        log = output / f"{name}.log"
        print(f"Starting {name}; log: {log}", flush=True)
        with log.open("w") as stream:
            result = subprocess.run(
                command, cwd=root, env=environment, stdout=stream, stderr=subprocess.STDOUT, check=False
            )
        record = {
            "name": name,
            "command": command,
            "returncode": result.returncode,
            "log": str(log),
            "log_sha256": hashlib.sha256(log.read_bytes()).hexdigest(),
            "identity_unchanged": identity(root) == frozen["identity"],
        }
        record["status"] = "passed" if result.returncode == 0 and record["identity_unchanged"] else "failed"
        report["checks"].append(record)
        (output / "terminal.json").write_text(json.dumps(report, indent=2, sort_keys=True) + "\n")
        print(f"Finished {name}: {record['status']} (exit {result.returncode})", flush=True)
    report["status"] = "passed" if all(check["status"] == "passed" for check in report["checks"]) else "failed"
    (output / "terminal.json").write_text(json.dumps(report, indent=2, sort_keys=True) + "\n")
    print(f"Phase 8 terminal checks: {report['status']}; {output / 'terminal.json'}", flush=True)
    return 0 if report["status"] == "passed" else 1


if __name__ == "__main__":
    raise SystemExit(main())
