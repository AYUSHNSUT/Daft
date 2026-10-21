"""Sample all threads of a persistent query worker outside performance trials.

Uses macOS sample when available. Permission/tool failures are saved as failures;
no samples from this diagnostic enter benchmark medians.
"""

from __future__ import annotations

import argparse
import json
import os
import shutil
import subprocess
import time
from pathlib import Path

from . import phase8, remote_eval
from .fixtures import file_hash, load_manifest
from .persistent import PersistentWorker
from .run import environment


def run(manifest, output, *, config="combined", case="scan_full", dataset="sorted/multi", duration=10):
    if duration < 1 or duration > 60:
        raise ValueError("Diagnostic duration must be 1..60 seconds")
    phase8.require_api()
    sampler = shutil.which("sample")
    if sampler is None:
        raise RuntimeError("macOS sample is unavailable; use an equivalent all-thread native CPU profiler")
    manifest, output = Path(manifest).resolve(), Path(output).resolve()
    load_manifest(manifest)
    output.mkdir(parents=True, exist_ok=False)
    child_env = {**os.environ, "DAFT_RUNNER": "native", "DAFT_PROGRESS_BAR": "0", "DAFT_ANALYTICS_ENABLED": "0"}
    result = {
        "scope": "Separate all-thread stack sampling; never performance acceptance",
        "status": "running",
        "environment": environment(),
        "manifest_sha256": file_hash(manifest),
        "trials": [],
    }
    for directory in ("trials", "plans", "diagnostics"):
        (output / directory).mkdir()
    spec = {
        "dataset": dataset,
        "case": case,
        "config": config,
        "configuration": phase8.configurations()[config],
        "manifest": str(manifest),
        "storage_root": None,
        "allow_dev": True,
        "diagnostic": False,
        "execution_mode": "persistent-process",
        "iteration": 0,
        "warmup": True,
    }
    with PersistentWorker(output, child_env) as worker:
        warmup = remote_eval.execute_child(spec, 0, output, child_env, 120, worker)
        result["trials"].append(warmup)
        if warmup["status"] == "passed":
            command = [sampler, str(worker.process.pid), str(duration), "1", "-file", str(output / "stacks.txt")]
            result["command"] = command
            with (output / "sampler.log").open("w") as log:
                process = subprocess.Popen(command, stdout=log, stderr=subprocess.STDOUT)
                deadline = time.monotonic() + duration + 5
                while process.poll() is None and time.monotonic() < deadline:
                    spec.update(warmup=False, iteration=len(result["trials"]))
                    trial = remote_eval.execute_child(spec, len(result["trials"]), output, child_env, 120, worker)
                    result["trials"].append(trial)
                    if trial["status"] != "passed":
                        break
                if process.poll() is None:
                    process.terminate()
                result["sampler_returncode"] = process.wait(timeout=10)
                result["worker_pid"] = worker.process.pid
    result["environment_after"] = environment()
    result["provenance_unchanged"] = all(
        result["environment"].get(key) == result["environment_after"].get(key) for key in phase8.phase7.IDENTITY_KEYS
    )
    result["status"] = (
        "passed"
        if (
            result.get("sampler_returncode") == 0
            and (output / "stacks.txt").is_file()
            and len(result["trials"]) > 1
            and all(t["status"] == "passed" for t in result["trials"])
            and result["provenance_unchanged"]
        )
        else "failed"
    )
    (output / "sample.json").write_text(json.dumps(result, indent=2, sort_keys=True) + "\n")
    return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--manifest", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--config", choices=phase8.configurations(), default="combined")
    parser.add_argument("--case", choices=phase8.QUERY_CASES, default="scan_full")
    parser.add_argument("--dataset", default="sorted/multi")
    parser.add_argument("--duration", type=int, default=10)
    args = vars(parser.parse_args())
    return 0 if run(**args)["status"] == "passed" else 1


if __name__ == "__main__":
    raise SystemExit(main())
