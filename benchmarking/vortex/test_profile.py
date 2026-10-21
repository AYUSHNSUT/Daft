from __future__ import annotations

import json

import pytest

from benchmarking.vortex import profile


def capture():
    return {
        "clock_alignment": {"native_origin_python_ns": 0, "uncertainty_ns": 0, "method": "shared_native_clock"},
        "python": {
            "dropped_events": 0,
            "events": [
                {"phase": "query", "started_ns": 0, "duration_ns": 100},
                {"phase": "source_creation", "started_ns": 0, "duration_ns": 20},
                {"phase": "collection", "started_ns": 20, "duration_ns": 80},
            ],
        },
        "native": {
            "dropped_events": 0,
            "unfinished_events": 0,
            "events": [
                {"phase": "data", "operation": "range", "started_ns": 10, "duration_ns": 30, "returned_bytes": 20},
                {"phase": "data", "operation": "range", "started_ns": 20, "duration_ns": 40, "returned_bytes": 30},
                {"phase": "execution", "operation": "batch_wait_decode", "started_ns": 5, "duration_ns": 65},
            ],
        },
    }


def test_overlapping_events_have_union_work_and_uncovered_accounting():
    result = profile.summarize(capture())
    ranges = next(row for row in result["rows"] if row["operation"] == "range")
    assert ranges["summed_event_ns"] == 70
    assert ranges["wall_union_ns"] == 50
    assert ranges["overlap_wall_ns"] == 20
    assert ranges["max_concurrent_events"] == 2
    assert ranges["returned_bytes"] == 50
    assert result["query_wall_ns"] == 100
    assert result["internal_covered_wall_ns"] == 65
    assert result["internal_uncovered_wall_ns"] == 35
    assert result["internal_overlap"]["max_concurrent_events"] == 3
    assert result["complete"]


def test_offsets_align_native_events_and_do_not_hide_outside_query():
    trace = capture()
    trace["clock_alignment"]["native_origin_python_ns"] = 50
    result = profile.summarize(trace)
    assert result["events_outside_query"] == 2
    assert result["internal_covered_wall_ns"] == 45
    assert result["internal_uncovered_wall_ns"] == 55


@pytest.mark.parametrize(
    "origin,field", [("python", "dropped_events"), ("native", "dropped_events"), ("native", "unfinished_events")]
)
def test_capture_loss_marks_accounting_incomplete(origin, field):
    trace = capture()
    trace[origin][field] = 1
    assert not profile.summarize(trace)["complete"]


def test_missing_clock_query_and_unknown_counts_are_not_reported_complete():
    trace = capture()
    del trace["native"]["unfinished_events"]
    assert not profile.summarize(trace)["complete"]
    del trace["clock_alignment"]
    with pytest.raises(ValueError, match="clock alignment"):
        profile.summarize(trace)
    trace = capture()
    trace["python"]["events"] = []
    with pytest.raises(ValueError, match="exactly one"):
        profile.summarize(trace)


def test_export_is_readable_and_does_not_copy_arbitrary_event_fields(tmp_path):
    trace = capture()
    trace["native"]["events"][0]["url"] = "https://user:secret@host/path?signature=secret"
    summary = profile.export(trace, tmp_path / "capture")
    assert json.loads((tmp_path / "capture/accounting.json").read_text()) == summary
    chrome = json.loads((tmp_path / "capture/timeline.json").read_text())
    assert len(chrome["traceEvents"]) == 6
    assert (
        next(event for event in chrome["traceEvents"] if event["name"] == "execution:batch_wait_decode")["dur"] == 0.065
    )
    assert "not a pure CPU" in (tmp_path / "capture/accounting.md").read_text()
    assert "secret" not in "".join(path.read_text() for path in (tmp_path / "capture").iterdir())
    with pytest.raises(FileExistsError):
        profile.export(trace, tmp_path / "capture")


@pytest.mark.parametrize(
    "intervals,expected", [([], 0), ([(1, 1)], 0), ([(0, 2), (2, 5)], 5), ([(0, 10), (3, 4), (12, 13)], 11)]
)
def test_interval_union_boundary_cases(intervals, expected):
    assert profile.union_ns(intervals) == expected


def test_touching_intervals_are_not_concurrent():
    assert profile.concurrency([(0, 2), (2, 5)]) == {"max_concurrent_events": 1, "overlap_wall_ns": 0}


def test_active_poll_work_is_separate_from_wall_occupancy():
    trace = capture()
    trace["native"]["events"][-1].update(active_poll_ns=11, poll_count=3)
    result = profile.summarize(trace)
    row = next(row for row in result["rows"] if row["operation"] == "batch_wait_decode")
    assert row["active_poll_ns"] == 11 and row["poll_count"] == 3
    assert row["wall_union_ns"] == 65
    assert row["summed_async_elapsed_ns"] == 65
    assert row["summed_suspended_ns"] == 54
    assert row["async_events"] == 1


def test_async_elapsed_totals_exclude_events_without_poll_instrumentation():
    trace = capture()
    trace["native"]["events"][0].update(active_poll_ns=11, poll_count=3)
    result = profile.summarize(trace)
    row = next(row for row in result["rows"] if row["operation"] == "range")
    assert row["summed_event_ns"] == 70
    assert row["summed_async_elapsed_ns"] == 30
    assert row["summed_suspended_ns"] == 19
    assert row["async_events"] == 1


def test_readable_tables_expose_poll_and_io_timings_and_escape_html(tmp_path):
    trace = capture()
    event = trace["native"]["events"][0]
    event.update(active_poll_ns=11, poll_count=3, request_wait_ns=2, byte_wait_ns=3, header_ns=4, body_ns=5)
    event["operation"] = "<script>alert(1)</script>"
    profile.export(trace, tmp_path / "capture")
    text = (tmp_path / "capture/accounting.md").read_text()
    assert "## Async polls and suspension" in text
    assert "## I/O checkpoints" in text
    assert "not query wall-time shares or CPU time" in text
    rendered = (tmp_path / "capture/index.html").read_text()
    assert "<table>" in rendered and "<th>Suspended ms</th>" in rendered
    assert "<th>Returned bytes</th>" in rendered
    assert "<script>" not in rendered
    assert "&lt;script&gt;alert(1)&lt;/script&gt;" in rendered


def test_lifecycle_residuals_and_coverage_do_not_invent_missing_timings():
    result = profile.summarize(capture())
    query, source, collect = result["envelopes"]
    assert query["child_union_ns"] == 100 and query["unattributed_ns"] == 0
    assert source["unattributed_ns"] == 20 and collect["unattributed_ns"] == 80
    assert result["coverage"]["physical planning"]["status"] == "not_observed"
    assert result["coverage"]["exclusive decoder CPU"]["status"] == "unavailable"


def test_closed_errors_and_cancellations_are_visible_separately_from_capture_loss():
    trace = capture()
    trace["native"]["events"][0]["error"] = True
    result = profile.summarize(trace)
    assert result["complete"] and result["failed_events"] == 1
    trace["native"]["events"][0].update(error=False, cancelled=True)
    result = profile.summarize(trace)
    assert result["complete"] and result["unexpected_cancellations"] == 1
    trace["native"]["events"][0]["operation"] = "first_batch"
    result = profile.summarize(trace)
    assert result["complete"] and result["unexpected_cancellations"] == 0


def test_original_operator_counter_precision_survives_export(tmp_path):
    metrics = [{"name": "Aggregate", "duration_seconds": 0.0, "stats": [["duration", {"unit": "µs", "value": 249.0}]]}]
    result = profile.export(capture(), tmp_path / "capture", metrics)
    assert result["operator_metrics"] == metrics
    assert "249.0 | µs" in (tmp_path / "capture/accounting.md").read_text()


@pytest.mark.parametrize("alignment", [None, [], "clock", {}, {"native_origin_python_ns": 0, "uncertainty_ns": -1}])
def test_malformed_clock_alignment_has_clear_error(alignment):
    trace = capture()
    trace["clock_alignment"] = alignment
    with pytest.raises(ValueError, match="clock alignment"):
        profile.summarize(trace)


def test_timeline_keeps_concurrent_ranges_in_separate_logical_lanes():
    trace = profile.chrome_trace(capture())
    ranges = [event for event in trace["traceEvents"] if event["name"] == "data:range"]
    assert len({event["tid"] for event in ranges}) == 2


@pytest.mark.parametrize("value", [None, [], {"events": None}, {"events": [None]}])
def test_malformed_event_collection_has_structured_error(value):
    trace = capture()
    trace["native"] = value
    with pytest.raises(ValueError, match="Diagnostic"):
        profile.summarize(trace)


@pytest.mark.parametrize(
    "field,value",
    [
        ("file_id", "secret"),
        ("span_id", []),
        ("thread_id", -1),
        ("error", "false"),
        ("cancelled", 1),
        ("active_poll_ns", 31),
        ("poll_count", False),
        ("returned_bytes", -1),
    ],
)
def test_invalid_numeric_counters_and_ids_are_rejected(field, value):
    trace = capture()
    trace["native"]["events"][0][field] = value
    with pytest.raises(ValueError, match="Diagnostic"):
        profile.summarize(trace)


def test_combined_source_discovery_and_schema_is_counted_as_child():
    trace = capture()
    trace["python"]["events"].append({"phase": "source_discovery_and_schema", "started_ns": 1, "duration_ns": 15})
    result = profile.summarize(trace)
    source = next(row for row in result["envelopes"] if row["phase"] == "source_creation")
    assert source["child_union_ns"] == 15 and source["unattributed_ns"] == 5
    assert result["coverage"]["combined discovery and schema"]["status"] == "observed"


def test_accumulated_decoder_work_does_not_invent_timeline_intervals(tmp_path):
    trace = capture()
    for index, event in enumerate(trace["native"]["events"][:2]):
        event.update(
            phase="parquet",
            operation="column_decoder_work",
            accumulated_work=[
                {"operation": "column_decode", "calls": 10 + index, "wall_ns": 12 + index},
                {"operation": "column_skip", "calls": 3 + index, "wall_ns": 2 + index},
            ],
        )
    result = profile.export(trace, tmp_path / "capture")
    work = {row["operation"]: row for row in result["accumulated_work"]}
    assert work["column_decode"]["calls"] == 21 and work["column_decode"]["summed_work_ns"] == 25
    assert work["column_skip"]["calls"] == 7 and work["column_skip"]["summed_work_ns"] == 5
    assert work["column_decode"]["envelopes"] == 2
    envelope = next(row for row in result["rows"] if row["operation"] == "column_decoder_work")
    assert envelope["wall_union_ns"] == 50 and envelope["summed_event_ns"] == 70
    assert not any(row["operation"] in {"column_decode", "column_skip"} for row in result["rows"])
    timeline = profile.chrome_trace(trace)["traceEvents"]
    assert len(timeline) == 6
    assert any(event["args"].get("accumulated_work") for event in timeline)
    assert "## Accumulated decoder work" in (tmp_path / "capture/accounting.md").read_text()


@pytest.mark.parametrize(
    "work",
    [
        None,
        {},
        [None],
        [{"operation": []}],
        [{"operation": "url-secret", "calls": 1, "wall_ns": 1}],
        [{"operation": "column_decode", "calls": True, "wall_ns": 1}],
        [{"operation": "column_decode", "calls": 1, "wall_ns": -1}],
        [{"operation": "column_decode", "calls": 1, "wall_ns": 1}] * 2,
        [{"operation": "column_decode", "calls": 1, "wall_ns": 1}] * 3,
    ],
)
def test_accumulated_work_requires_bounded_allowlisted_numeric_counters(work):
    trace = capture()
    trace["native"]["events"][0]["accumulated_work"] = work
    with pytest.raises(ValueError, match="Diagnostic accumulated"):
        profile.summarize(trace)
