"""Private, opt-in capture for one native-runner Vortex evaluation query.

This is process-local benchmark instrumentation, not a public query metrics API.
Start before constructing the source and finish after collect, before a separate
explain query or oracle validation. No path, URL, exception text, or I/O
configuration is copied into the diagnostic artifact.
"""

from __future__ import annotations

import threading
import time
from collections.abc import Iterator
from contextlib import contextmanager
from dataclasses import dataclass, field
from typing import Any

from daft import daft as _native

_MAX_EVENTS = 50_000
_STAGES = frozenset({"discovery", "discovery_fallback", "source_creation", "collection"})
_COUNTERS = ("head_requests", "get_requests", "list_requests", "bytes_read")
_lock = threading.RLock()


@dataclass(slots=True)
class _Capture:
    started_ns: int
    events: list[dict[str, Any]] = field(default_factory=list)
    discovery: dict[str, int] = field(default_factory=lambda: {name: 0 for name in _COUNTERS})
    bulk_calls: int = 0
    inputs: int = 0
    dropped_events: int = 0


_capture: _Capture | None = None


def active() -> bool:
    return _capture is not None


def capture_token() -> object | None:
    """Retain capture identity across native calls that release the GIL."""
    return _capture


def start() -> None:
    """Start one capture; nested/concurrent captures in this process are invalid."""
    global _capture
    with _lock:
        if _capture is not None:
            raise RuntimeError("A Vortex diagnostic capture is already active")
        begin = getattr(_native, "_vortex_diagnostics_start", None)
        if begin is None:
            raise RuntimeError("Rebuild with the Phase 6 native Vortex diagnostics feature")
        begin()
        _capture = _Capture(time.perf_counter_ns())


def finish() -> dict[str, Any]:
    """Stop capture and return bounded native/Python diagnostics with explicit scope."""
    global _capture
    with _lock:
        current = _capture
        if current is None:
            raise RuntimeError("No Vortex diagnostic capture is active")
        try:
            native = _native._vortex_diagnostics_finish()
            elapsed = time.perf_counter_ns() - current.started_ns
        finally:
            _capture = None
        return {
            "schema_version": 1,
            "scope": "one native-runner query; process-local logical I/O, not physical network traffic",
            "elapsed_ns": elapsed,
            "native": native,
            "python": {
                "max_events": _MAX_EVENTS,
                "dropped_events": current.dropped_events,
                "events": current.events,
                "discovery": {
                    **current.discovery,
                    "bulk_calls": current.bulk_calls,
                    "inputs": current.inputs,
                },
            },
            "retry_attempts": None,
            "retry_attempts_unavailable_reason": "This capture does not export physical transport retry attempts",
            "timing_note": (
                "Native and Python timestamps have separate capture origins; "
                "nested/concurrent durations overlap and must not be summed into a query breakdown"
            ),
        }


@contextmanager
def stage(name: str) -> Iterator[None]:
    """Measure an optional Python stage without timing or event storage when off."""
    if name not in _STAGES:
        raise ValueError("Unknown Vortex diagnostic stage")
    current = _capture
    if current is None:
        yield
        return
    started = time.perf_counter_ns()
    error = False
    try:
        yield
    except BaseException:
        error = True
        raise
    finally:
        elapsed = time.perf_counter_ns() - started
        with _lock:
            # A late context from a finished capture must not pollute a new one.
            if _capture is current:
                if len(current.events) >= _MAX_EVENTS:
                    current.dropped_events += 1
                else:
                    current.events.append(
                        {
                            "phase": name,
                            "started_ns": started - current.started_ns,
                            "duration_ns": elapsed,
                            "error": error,
                        }
                    )


def record_discovery(metrics: dict[str, Any] | None, *, token: object | None) -> None:
    """Accumulate only numeric allowlisted bulk-discovery counters."""
    current = _capture
    if current is None or current is not token or metrics is None:
        return
    values = {}
    for name in (*_COUNTERS, "num_inputs"):
        value = metrics.get(name, 0)
        if type(value) is not int or value < 0:
            raise ValueError("Invalid Vortex discovery diagnostic counter")
        values[name] = value
    with _lock:
        if _capture is current:
            for name in _COUNTERS:
                current.discovery[name] += values[name]
            current.inputs += values["num_inputs"]
            current.bulk_calls += 1
