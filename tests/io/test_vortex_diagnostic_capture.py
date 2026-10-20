"""Safety and lifecycle of the private evaluation capture, independent of builds."""

from __future__ import annotations

import json

import pytest

from daft.io import _vortex_diagnostics as diagnostics


@pytest.fixture
def capture_backend(monkeypatch):
    calls = []
    monkeypatch.setattr(diagnostics, "_capture", None)
    monkeypatch.setattr(diagnostics._native, "_vortex_diagnostics_start", lambda: calls.append("start"), raising=False)
    monkeypatch.setattr(
        diagnostics._native,
        "_vortex_diagnostics_finish",
        lambda: {"schema_version": 1, "events": [], "dropped_events": 0},
        raising=False,
    )
    yield calls
    monkeypatch.setattr(diagnostics, "_capture", None)


def test_capture_requires_explicit_start_and_disallows_nested_capture(capture_backend):
    with pytest.raises(RuntimeError, match="No Vortex diagnostic capture"):
        diagnostics.finish()
    diagnostics.start()
    with pytest.raises(RuntimeError, match="already active"):
        diagnostics.start()
    assert capture_backend == ["start"]
    diagnostics.finish()
    assert not diagnostics.active()


def test_disabled_capture_does_not_read_clock_or_store_metrics(monkeypatch, capture_backend):
    def unexpected_clock():
        raise AssertionError("Disabled capture performed event timing")

    monkeypatch.setattr(diagnostics.time, "perf_counter_ns", unexpected_clock)
    with diagnostics.stage("discovery"):
        diagnostics.record_discovery({"head_requests": "not collected while disabled"}, token=None)
    assert capture_backend == []


def test_capture_collects_counts_and_failure_without_url_or_exception_text(capture_backend):
    secret = "https://user:password@example/data?signature=sentinel-secret"
    diagnostics.start()
    diagnostics.record_discovery(
        {"num_inputs": 2, "head_requests": 2, "get_requests": 1, "bytes_read": 512, "uri": secret},
        token=diagnostics.capture_token(),
    )
    with pytest.raises(OSError):
        with diagnostics.stage("discovery"):
            raise OSError(secret)
    result = diagnostics.finish()
    assert result["python"]["discovery"] == {
        "bulk_calls": 1,
        "inputs": 2,
        "head_requests": 2,
        "get_requests": 1,
        "list_requests": 0,
        "bytes_read": 512,
    }
    assert result["python"]["events"][0]["error"] is True
    assert "sentinel-secret" not in json.dumps(result)
    assert "example" not in json.dumps(result)
    assert result["retry_attempts"] is None


def test_late_stage_does_not_cross_capture_generations(capture_backend):
    diagnostics.start()
    old = diagnostics.stage("discovery")
    old.__enter__()
    diagnostics.finish()
    diagnostics.start()
    old.__exit__(None, None, None)
    assert diagnostics.finish()["python"]["events"] == []


def test_late_discovery_counters_do_not_cross_capture_generations(capture_backend):
    diagnostics.start()
    old = diagnostics.capture_token()
    diagnostics.finish()
    diagnostics.start()
    diagnostics.record_discovery({"head_requests": 7}, token=old)
    diagnostics.record_discovery({"head_requests": 2}, token=diagnostics.capture_token())
    result = diagnostics.finish()["python"]["discovery"]
    assert result["head_requests"] == 2
    assert result["bulk_calls"] == 1


def test_capture_event_limit_reports_loss(monkeypatch, capture_backend):
    monkeypatch.setattr(diagnostics, "_MAX_EVENTS", 1)
    diagnostics.start()
    for _ in range(3):
        with diagnostics.stage("discovery"):
            pass
    result = diagnostics.finish()
    assert len(result["python"]["events"]) == 1
    assert result["python"]["dropped_events"] == 2


@pytest.mark.parametrize("value", [True, -1, "sentinel-secret", float("nan")])
def test_capture_rejects_invalid_numeric_counters_without_echoing_values(value, capture_backend):
    diagnostics.start()
    with pytest.raises(ValueError, match="Invalid Vortex discovery diagnostic counter") as error:
        diagnostics.record_discovery({"head_requests": value}, token=diagnostics.capture_token())
    assert "sentinel-secret" not in str(error.value)
    diagnostics.finish()
