"""Supervise one sequential benchmark worker without restarting failed sessions."""

from __future__ import annotations

import json
import subprocess
import sys
import threading
import time
from datetime import datetime, timezone
from pathlib import Path

from .run import redacted


class PersistentWorker:
    """Reuse process-local clients; each request still builds its own query.

    Result files are atomically replaced by the worker. Control traffic, log
    draining and polling are outside the worker's query timer. A timeout or
    worker failure invalidates the session; subsequent jobs are recorded as
    failures without a restart or retry.
    """

    def __init__(self, output: Path, environment: dict[str, str]):
        self.output = output
        self.environment = environment
        self.process = None
        self.failure = None
        self.log_path = output / "persistent-worker.log"
        self.log = None
        self.reader = None
        self.started_at_utc = None
        self.completed_at_utc = None

    def __enter__(self):
        self.started_at_utc = datetime.now(timezone.utc).isoformat()
        self.log = self.log_path.open("w")
        try:
            self.process = subprocess.Popen(
                [sys.executable, "-m", "benchmarking.vortex.remote_eval", "--worker-session"],
                stdin=subprocess.PIPE,
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                text=True,
                bufsize=1,
                env=self.environment,
            )
        except OSError as error:
            self.failure = redacted(str(error))
            self.log.write(self.failure + "\n")
        else:
            self.reader = threading.Thread(target=self._drain_log, daemon=True)
            self.reader.start()
        return self

    def _drain_log(self):
        for line in self.process.stdout:
            self.log.write(redacted(line))
            self.log.flush()

    def _stop(self):
        if self.process is not None and self.process.poll() is None:
            self.process.kill()
            self.process.wait()

    def execute(self, spec_path: Path, result_path: Path, timeout: float):
        if self.failure is not None:
            raise RuntimeError(f"Persistent worker unavailable after prior failure: {self.failure}")
        began = time.monotonic()
        try:
            if self.process.poll() is not None:
                raise RuntimeError(f"Persistent worker exited {self.process.returncode}")
            self.process.stdin.write(json.dumps({"spec": str(spec_path), "result": str(result_path)}) + "\n")
            self.process.stdin.flush()
            while not result_path.is_file():
                if self.process.poll() is not None:
                    raise RuntimeError(f"Persistent worker exited {self.process.returncode}; see {self.log_path.name}")
                if time.monotonic() - began >= timeout:
                    raise TimeoutError(f"Persistent trial exceeded {timeout:g} seconds; worker terminated")
                time.sleep(0.005)
            result = json.loads(result_path.read_text())
            if result.get("status") != "passed":
                raise RuntimeError(result.get("error", "Persistent query failed"))
            return result
        except (OSError, ValueError, RuntimeError, TimeoutError) as error:
            self.failure = redacted(str(error))
            self._stop()
            raise

    def __exit__(self, *_):
        if self.process is not None:
            if self.process.stdin is not None:
                try:
                    self.process.stdin.close()
                except OSError:
                    pass
            try:
                self.process.wait(timeout=5)
            except subprocess.TimeoutExpired:
                self.failure = self.failure or "Persistent worker did not exit after the schedule completed"
                self._stop()
            if self.process.returncode and self.failure is None:
                self.failure = f"Persistent worker exited {self.process.returncode}"
            if self.reader is not None:
                self.reader.join(timeout=5)
            self.process.stdout.close()
        if self.log is not None:
            self.log.close()
        self.completed_at_utc = datetime.now(timezone.utc).isoformat()
