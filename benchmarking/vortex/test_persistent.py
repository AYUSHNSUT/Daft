"""Real pipe/subprocess lifecycle tests without network services."""

from __future__ import annotations

import json
import os
import subprocess
import sys

import pytest

from benchmarking.vortex.persistent import PersistentWorker


@pytest.fixture
def child(monkeypatch):
    from benchmarking.vortex import persistent

    original = subprocess.Popen
    script = """
import json, os, sys, time
from pathlib import Path
for line in sys.stdin:
    request = json.loads(line)
    spec = json.loads(Path(request['spec']).read_text())
    if spec.get('exit'):
        sys.exit(7)
    if spec.get('wait'):
        time.sleep(30)
    print('private-session-secret', flush=True)
    result = Path(request['result'])
    pending = result.with_suffix('.pending')
    pending.write_text(json.dumps({'status': 'passed', 'pid': os.getpid(), 'value': spec['value']}))
    pending.replace(result)
"""

    def popen(command, **kwargs):
        assert "--worker-session" in command
        return original([sys.executable, "-u", "-c", script], **kwargs)

    monkeypatch.setenv("DAFT_VORTEX_BENCH_IO_JSON", json.dumps({"s3": {"access_key": "private-session-secret"}}))
    monkeypatch.setattr(persistent.subprocess, "Popen", popen)


def request(tmp_path, index, **values):
    spec, result = tmp_path / f"{index}-spec.json", tmp_path / f"{index}.json"
    spec.write_text(json.dumps(values))
    return spec, result


def test_reuses_one_worker_and_closes_connections_with_process(tmp_path, child):
    with PersistentWorker(tmp_path, dict(os.environ)) as session:
        first = session.execute(*request(tmp_path, 0, value=1), timeout=5)
        second = session.execute(*request(tmp_path, 1, value=2), timeout=5)
        assert first["pid"] == second["pid"] == session.process.pid
        assert (first["value"], second["value"]) == (1, 2)
    assert session.process.poll() == 0
    assert "private-session-secret" not in session.log_path.read_text()
    assert "<redacted>" in session.log_path.read_text()


@pytest.mark.parametrize("failure", ["wait", "exit"])
def test_timeout_or_crash_terminates_without_restart_or_retry(tmp_path, child, failure):
    with PersistentWorker(tmp_path, dict(os.environ)) as session:
        pid = session.process.pid
        spec, result = request(tmp_path, 0, **{failure: True})
        with pytest.raises((TimeoutError, RuntimeError)):
            session.execute(spec, result, timeout=0.2)
        assert not result.exists()
        assert session.process.poll() is not None
        with pytest.raises(RuntimeError, match="prior failure"):
            session.execute(*request(tmp_path, 1, value=2), timeout=5)
        assert session.process.pid == pid
        assert not (tmp_path / "1.json").exists()


@pytest.mark.parametrize("reader", ["parquet", "vortex"])
def test_native_session_reopens_changed_input_instead_of_reusing_results(tmp_path, reader):
    import pyarrow as pa
    import pyarrow.parquet as pq

    import daft
    from benchmarking.vortex.remote_eval import configurations

    if not hasattr(daft.daft, "_read_vortex_metadata_bulk_cached"):
        pytest.skip("requires native Vortex feature")
    vortex = pytest.importorskip("vortex")
    path = tmp_path / f"input.{reader}"
    manifest_path = tmp_path / "manifest.json"
    config = configurations("final", "off")["parquet" if reader == "parquet" else "candidate"]
    results = []
    with PersistentWorker(tmp_path, dict(os.environ, DAFT_ANALYTICS_ENABLED="0", DAFT_PROGRESS_BAR="0")) as session:
        for index, rows in enumerate((10, 13)):
            table = pa.table({"id": list(range(rows)), "value": list(range(rows))})
            if reader == "parquet":
                pq.write_table(table, path)
            else:
                vortex.io.write(table, str(path))
            expected = [
                {
                    "rows": rows,
                    "sum_id": rows * (rows - 1) // 2,
                    "count_value": rows,
                    "sum_value": rows * (rows - 1) // 2,
                }
            ]
            manifest_path.write_text(
                json.dumps(
                    {
                        "datasets": {"test": {"paths": {reader: [path.name]}}},
                        "oracle": {"scan_narrow": expected},
                    }
                )
            )
            spec, result_path = request(
                tmp_path,
                index,
                manifest=str(manifest_path),
                dataset="test",
                case="scan_narrow",
                configuration=config,
                allow_dev=True,
                execution_mode="persistent-process",
            )
            result = session.execute(spec, result_path, timeout=30)
            assert result["output"] == expected
            assert result["peak_rss_bytes"] is None
            assert "plan" not in result
            results.append(result)
    assert results[0]["worker_pid"] == results[1]["worker_pid"]
    assert results[0]["output_sha256"] != results[1]["output_sha256"]
