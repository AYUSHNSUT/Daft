"""Wall-time accounting for separate executed-query diagnostic captures.

Interval unions measure observed wall occupancy, not CPU work or causal critical
paths. Nested and parallel events are retained rather than added together.
"""

from __future__ import annotations

import html
import json
from collections import defaultdict
from pathlib import Path


def union_ns(intervals):
    """Length of the union of half-open intervals, including empty input."""
    total, end = 0, None
    for start, stop in sorted(intervals):
        if stop < start:
            raise ValueError("Diagnostic interval ends before its start")
        total += max(0, stop - max(start, end if end is not None else start))
        end = max(end if end is not None else stop, stop)
    return total


def concurrency(intervals):
    changes = defaultdict(int)
    for start, end in intervals:
        if end > start:
            changes[start] += 1
            changes[end] -= 1
    active, maximum, overlap, previous = 0, 0, 0, None
    for at, change in sorted(changes.items()):
        if previous is not None and active > 1:
            overlap += at - previous
        active += change
        maximum = max(maximum, active)
        previous = at
    return {"max_concurrent_events": maximum, "overlap_wall_ns": overlap}


def _number(value):
    return type(value) is int and value >= 0


def envelopes(normalized):
    """Account for nested Python lifecycle envelopes without double counting."""
    children = {
        "query": {"source_creation", "collection"},
        "source_creation": {"discovery", "schema_inference", "source_discovery_and_schema"},
        "collection": {
            "logical_optimization",
            "physical_planning",
            "executor_setup",
            "executor_next",
            "consumer_wait",
            "runner_finalize",
        },
    }
    result = []
    python = [event for event in normalized if event["origin"] == "python"]
    for parent, phases in children.items():
        parents = [event for event in python if event["phase"] == parent]
        for event in parents:
            intervals = [
                (max(event["start"], child["start"]), min(event["end"], child["end"]))
                for child in python
                if child["phase"] in phases and child["end"] >= event["start"] and child["start"] <= event["end"]
            ]
            covered = union_ns(intervals)
            result.append(
                {
                    "phase": parent,
                    "wall_ns": event["end"] - event["start"],
                    "child_union_ns": covered,
                    "unattributed_ns": event["end"] - event["start"] - covered,
                    **concurrency(intervals),
                }
            )
    return result


def coverage(normalized):
    names = {event["phase"] for event in normalized} | {event["operation"] for event in normalized}
    expected = {
        "source and query construction": {"source_creation"},
        "combined discovery and schema": {"source_discovery_and_schema"},
        "discovery": {"discovery", "source_discovery", "discovery_materialization", "task_discovery_next"},
        "schema inference": {"schema_inference", "schema", "source_schema_inference"},
        "initial schema conversion": {"initial_schema_conversion"},
        "logical optimization": {"logical_optimization"},
        "physical planning": {"physical_planning"},
        "metadata reads and decoding": {"planning_metadata", "metadata_read", "metadata_decode"},
        "task creation": {"task_creation", "scan_task_build", "scan_tasks_materialization"},
        "executor and event loop": {"executor_next", "event_loop_wait", "native_submit"},
        "compute scheduling and permits": {"compute_queue_wait", "memory_permit_wait", "compute_task"},
        "reader scheduling queues": {
            "adapter_poll_queue",
            "metadata_decode_queue",
            "local_open_queue",
            "data_range_queue",
        },
        "client and connection pool": {"client_acquisition", "source_acquisition", "s3_pool_wait", "s3_list_pool_wait"},
        "request headers and response bodies": {
            "get_headers",
            "s3_get_headers",
            "s3_head_headers",
            "s3_list_headers",
            "response_body",
            "body_collect",
            "range",
        },
        "outer retries and backoff": {"get_attempt_headers", "resume_headers", "retry_backoff", "body_retry_backoff"},
        "decoding and asynchronous stream wait": {"batch_wait_decode", "column_decode", "column_consume"},
        "conversion and filtering": {"arrow_conversion", "predicate_filter", "residual_filter"},
        "reader schema conversion": {"schema_conversion"},
        "finalization": {"native_finalize", "worker_shutdown", "stats_finalize", "runner_finalize"},
    }
    return {
        **{
            name: {
                "status": "observed" if names & phases else "not_observed",
                "observed_events": sorted(names & phases),
            }
            for name, phases in expected.items()
        },
        **{
            name: {"status": "unavailable", "reason": reason}
            for name, reason in {
                "DNS, TCP connect and TLS negotiation": "SDK transport internals are not independently timed by this capture",
                "all physical retries and wire bytes": "Logical operations and outer attempts do not expose SDK-internal retries or packet traffic",
                "exclusive decoder CPU": "Wall spans and active polls are not exclusive CPU time; use a separate native sampling profile",
                "MinIO and Docker VM work": "Client-process capture does not attribute server CPU, storage or virtualization time",
            }.items()
        },
    }


def events(trace):
    """Normalize numeric events without copying URLs or arbitrary event fields."""
    if not isinstance(trace, dict):
        raise ValueError("Diagnostic capture must be an object")  # noqa: TRY004
    alignment = trace.get("clock_alignment", {})
    if not isinstance(alignment, dict):
        raise ValueError("Diagnostic capture is missing clock alignment")  # noqa: TRY004
    offset = alignment.get("native_origin_python_ns")
    if type(offset) is not int or not _number(alignment.get("uncertainty_ns")):
        raise ValueError("Diagnostic capture is missing clock alignment")
    normalized = []
    for origin in ("python", "native"):
        capture = trace.get(origin)
        if not isinstance(capture, dict) or not isinstance(capture.get("events"), list):
            raise ValueError("Diagnostic capture requires an event list for each origin")  # noqa: TRY004
        for event in capture["events"]:
            if not isinstance(event, dict):
                raise ValueError("Diagnostic event must be an object")  # noqa: TRY004
            start, duration = event.get("started_ns"), event.get("duration_ns")
            if not _number(start) or not _number(duration):
                raise ValueError("Diagnostic event requires nonnegative integer timing")
            phase, operation = event.get("phase"), event.get("operation", "span")
            if not isinstance(phase, str) or not isinstance(operation, str):
                raise TypeError("Diagnostic event requires phase and operation names")
            for key in ("file_id", "span_id", "thread_id"):
                if event.get(key) is not None and not _number(event[key]):
                    raise ValueError("Diagnostic event IDs must be nonnegative integers")
            for key in ("error", "cancelled"):
                if type(event.get(key, False)) is not bool:
                    raise ValueError("Diagnostic event outcome flags must be booleans")
            numeric = {}
            for key in (
                "requested_bytes",
                "returned_bytes",
                "request_wait_ns",
                "byte_wait_ns",
                "header_ns",
                "body_ns",
                "active_poll_ns",
                "poll_count",
            ):
                value = event.get(key)
                if value is not None:
                    if not _number(value):
                        raise ValueError("Diagnostic event counters must be nonnegative integers")
                    numeric[key] = value
            if numeric.get("active_poll_ns", 0) > duration:
                raise ValueError("Diagnostic event active poll time exceeds its wall duration")
            start += offset if origin == "native" else 0
            normalized.append(
                {
                    "origin": origin,
                    "phase": phase,
                    "operation": operation,
                    "layer": origin,
                    "start": start,
                    "end": start + duration,
                    "file_id": event.get("file_id"),
                    "span_id": event.get("span_id"),
                    "thread_id": event.get("thread_id"),
                    "error": event.get("error", False),
                    "cancelled": event.get("cancelled", False),
                    **numeric,
                }
            )
    return normalized


def summarize(trace):
    normalized = events(trace)
    query = [event for event in normalized if event["origin"] == "python" and event["phase"] == "query"]
    if len(query) != 1:
        raise ValueError("Diagnostic capture requires exactly one outer query interval")
    began, ended = query[0]["start"], query[0]["end"]
    groups = defaultdict(list)
    for event in normalized:
        if event["phase"] == "query" and event["origin"] == "python":
            continue
        groups[(event["origin"], event["phase"], event["operation"])].append(event)
    rows = []
    for (origin, phase, operation), group in sorted(groups.items()):
        intervals = [(max(began, e["start"]), min(ended, e["end"])) for e in group]
        intervals = [(start, end) for start, end in intervals if end >= start]
        polled = [event for event in group if "active_poll_ns" in event]
        rows.append(
            {
                "origin": origin,
                "phase": phase,
                "operation": operation,
                "events": len(group),
                "summed_event_ns": sum(e["end"] - e["start"] for e in group),
                "wall_union_ns": union_ns(intervals),
                **concurrency(intervals),
                "errors": sum(bool(e["error"]) for e in group),
                "cancelled": sum(bool(e["cancelled"]) for e in group),
                "async_events": len(polled),
                "summed_async_elapsed_ns": sum(event["end"] - event["start"] for event in polled) if polled else None,
                "summed_suspended_ns": sum(event["end"] - event["start"] - event["active_poll_ns"] for event in polled)
                if polled
                else None,
                **{
                    key: sum(e[key] for e in group if key in e) if any(key in e for e in group) else None
                    for key in (
                        "requested_bytes",
                        "returned_bytes",
                        "request_wait_ns",
                        "byte_wait_ns",
                        "header_ns",
                        "body_ns",
                        "active_poll_ns",
                        "poll_count",
                    )
                },
            }
        )
    # Outer envelopes cover lifecycle but say nothing about which internal work
    # dominates it. Compute internal coverage separately so gaps stay visible.
    outer = {"query", "source_creation", "collection"}
    internal = [
        (max(began, e["start"]), min(ended, e["end"]))
        for e in normalized
        if not (e["origin"] == "python" and e["phase"] in outer) and e["end"] >= began and e["start"] <= ended
    ]
    covered_wall = union_ns(internal)
    loss = {
        "native_dropped": trace.get("native", {}).get("dropped_events"),
        "native_unfinished": trace.get("native", {}).get("unfinished_events"),
        "python_dropped": trace.get("python", {}).get("dropped_events"),
    }
    failed_events = sum(bool(event["error"]) for event in normalized)
    unexpected_cancellations = sum(
        bool(event["cancelled"]) and event["operation"] != "first_batch" for event in normalized
    )
    complete = all(type(value) is int and value == 0 for value in loss.values())
    return {
        "schema_version": 1,
        "complete": complete,
        "loss": loss,
        "failed_events": failed_events,
        "unexpected_cancellations": unexpected_cancellations,
        "envelopes": envelopes(normalized),
        "coverage": coverage(normalized),
        "clock_alignment": trace["clock_alignment"],
        "query_wall_ns": ended - began,
        "internal_covered_wall_ns": covered_wall,
        "internal_uncovered_wall_ns": ended - began - covered_wall,
        "internal_overlap": concurrency(internal),
        "events_outside_query": sum(e["start"] < began or e["end"] > ended for e in normalized),
        "rows": rows,
        "notes": [
            "Separate instrumented query; these durations are excluded from performance trials.",
            "Capture completeness measures dropped or unfinished events. Closed operation errors or cancellations are reported separately; successful queries may recover from an attempt or prune an empty first batch.",
            "Wall unions overlap across rows. Summed event time includes parallel and nested work and is not query wall time.",
            "Uncovered time is uninstrumented work or gaps, not proven idle time. Covered envelopes can contain uninstrumented work.",
            "consumer_wait spans time paused at a result yield, including consumer processing and downstream backpressure.",
            "source_discovery ends at the first discovered item; discovery_materialization measures full enumeration. source_schema_inference includes its metadata I/O.",
            "batch_wait_decode includes asynchronous I/O and scheduling waits; it is not a pure CPU decoding measurement.",
            "response_body spans headers available through EOF, including consumer and scheduling delay; it is not pure network transfer time.",
            "Active-poll time is wall inside future poll calls, including nested work but excluding spawned work; suspended time includes scheduling waits. Neither is CPU time.",
            "Logical API events and returned bytes do not measure physical transport attempts or wire traffic.",
            "Operator metrics and process CPU are additional evidence; neither can be added to this wall-time accounting.",
            "Timeline lanes are logical display lanes, not OS threads. thread_id records the starting thread; asynchronous work may migrate.",
        ],
    }


def chrome_trace(trace):
    rows = []
    lanes = defaultdict(list)
    for event in sorted(events(trace), key=lambda item: (item["start"], -item["end"])):
        group = f"{event['origin']}/{event['phase']}/{event['file_id'] or 'query'}"
        ends = lanes[group]
        lane = next((index for index, end in enumerate(ends) if end <= event["start"]), len(ends))
        if lane == len(ends):
            ends.append(event["end"])
        else:
            ends[lane] = event["end"]
        rows.append(
            {
                "name": event["phase"] + ":" + event["operation"],
                "cat": str(event["layer"]),
                "ph": "X",
                "ts": event["start"] / 1000,
                "dur": (event["end"] - event["start"]) / 1000,
                "pid": 1,
                "tid": f"{group}/lane{lane}",
                "args": {
                    key: event[key]
                    for key in (
                        "error",
                        "cancelled",
                        "requested_bytes",
                        "returned_bytes",
                        "span_id",
                        "thread_id",
                        "file_id",
                        "active_poll_ns",
                        "poll_count",
                        "request_wait_ns",
                        "byte_wait_ns",
                        "header_ns",
                        "body_ns",
                    )
                    if key in event
                },
            }
        )
    return {"traceEvents": rows, "displayTimeUnit": "ms"}


def markdown(summary):
    lines = [
        "# Query diagnostic accounting",
        "",
        f"Capture complete: {summary['complete']}. Query wall: {summary['query_wall_ns'] / 1e6:.3f} ms.",
        f"Closed error events: {summary['failed_events']}; cancellations excluding empty first-batch spans: {summary['unexpected_cancellations']}.",
        f"Internal observed coverage: {summary['internal_covered_wall_ns'] / 1e6:.3f} ms; "
        f"uncovered: {summary['internal_uncovered_wall_ns'] / 1e6:.3f} ms.",
        "",
        "| Layer | Phase / operation | Events | Wall union ms | Summed event ms | Maximum concurrent |",
        "| --- | --- | ---: | ---: | ---: | ---: |",
    ]
    for row in summary["rows"]:
        lines.append(
            f"| {row['origin']} | {row['phase']} / {row['operation']} | {row['events']} | "
            f"{row['wall_union_ns'] / 1e6:.3f} | {row['summed_event_ns'] / 1e6:.3f} | {row['max_concurrent_events']} |"
        )
    lines += [
        "",
        "## Lifecycle accounting",
        "",
        "| Envelope | Wall ms | Child union ms | Unattributed ms |",
        "| --- | ---: | ---: | ---: |",
    ]
    for row in summary["envelopes"]:
        lines.append(
            f"| {row['phase']} | {row['wall_ns'] / 1e6:.3f} | {row['child_union_ns'] / 1e6:.3f} | {row['unattributed_ns'] / 1e6:.3f} |"
        )
    lines += [
        "",
        "## Async polls and suspension",
        "",
        "Only events with poll instrumentation contribute to this table. Elapsed = active poll + suspended wall time. "
        "These are sums of overlapping operations, not query wall-time shares or CPU time; suspension includes scheduling waits.",
        "",
        "| Phase / operation | Instrumented events | Polls | Summed async elapsed ms | Active poll ms | Suspended ms |",
        "| --- | ---: | ---: | ---: | ---: | ---: |",
    ]
    for row in summary["rows"]:
        if row["async_events"]:
            polls = row["poll_count"] if row["poll_count"] is not None else "unavailable"
            lines.append(
                f"| {row['phase']} / {row['operation']} | {row['async_events']} | {polls} | "
                f"{row['summed_async_elapsed_ns'] / 1e6:.3f} | {row['active_poll_ns'] / 1e6:.3f} | {row['summed_suspended_ns'] / 1e6:.3f} |"
            )
    lines += [
        "",
        "## I/O checkpoints",
        "",
        "Each cell sums recorded operation checkpoints. Rows can describe nested operations and must not be added together. "
        "Headers/body include SDK, consumer and scheduling delay; returned bytes are logical buffer bytes, not wire traffic. "
        "Unavailable checkpoints are shown explicitly.",
        "",
        "| Phase / operation | Request permit ms | Byte permit ms | Headers ms | Body ms | Returned bytes |",
        "| --- | ---: | ---: | ---: | ---: | ---: |",
    ]
    checkpoints = ("request_wait_ns", "byte_wait_ns", "header_ns", "body_ns")
    for row in summary["rows"]:
        if any(row[key] is not None for key in (*checkpoints, "returned_bytes")):
            values = [f"{row[key] / 1e6:.3f}" if row[key] is not None else "unavailable" for key in checkpoints]
            returned = str(row["returned_bytes"]) if row["returned_bytes"] is not None else "unavailable"
            lines.append(f"| {row['phase']} / {row['operation']} | " + " | ".join([*values, returned]) + " |")
    lines += ["", "## Coverage", ""]
    for name, value in summary["coverage"].items():
        lines.append(f"- {name}: {value['status']}. " + value.get("reason", ""))
    if summary.get("operator_metrics"):
        lines += [
            "",
            "## Operator counters",
            "",
            "These are original operator statistics in their exported units, not additive query-stage durations.",
            "",
            "| Operator | Counter | Value | Unit |",
            "| --- | --- | ---: | --- |",
        ]
        for metric in summary["operator_metrics"]:
            for name, descriptor in metric.get("stats", []):
                lines.append(f"| {metric['name']} | {name} | {descriptor['value']} | {descriptor.get('unit') or ''} |")
    lines.extend(["", *["- " + note for note in summary["notes"]], ""])
    return "\n".join(lines)


def html_body(markdown_text):
    """Render this report's small generated Markdown subset; escape all content."""
    rendered, table, list_open = [], False, False
    for line in markdown_text.splitlines():
        if not line.startswith("| ") and table:
            rendered.append("</tbody></table></div>")
            table = False
        if not line.startswith("- ") and list_open:
            rendered.append("</ul>")
            list_open = False
        if line.startswith("| "):
            cells = [cell.strip() for cell in line.strip().strip("|").split("|")]
            if all(set(cell) <= set("-: ") for cell in cells):
                continue
            tag = "td" if table else "th"
            row = "<tr>" + "".join(f"<{tag}>{html.escape(cell)}</{tag}>" for cell in cells) + "</tr>"
            if not table:
                rendered.append("<div class='table-wrap'><table><thead>" + row + "</thead><tbody>")
                table = True
            else:
                rendered.append(row)
        elif line.startswith("## "):
            rendered.append("<h2>" + html.escape(line[3:]) + "</h2>")
        elif line.startswith("# "):
            rendered.append("<h1>" + html.escape(line[2:]) + "</h1>")
        elif line.startswith("- "):
            if not list_open:
                rendered.append("<ul>")
                list_open = True
            rendered.append("<li>" + html.escape(line[2:]) + "</li>")
        elif line:
            rendered.append("<p>" + html.escape(line) + "</p>")
    if table:
        rendered.append("</tbody></table></div>")
    if list_open:
        rendered.append("</ul>")
    return "\n".join(rendered)


def export(trace, directory, operator_metrics=None):
    directory = Path(directory)
    directory.mkdir(parents=True, exist_ok=False)
    summary = summarize(trace)
    if operator_metrics is not None:
        summary["operator_metrics"] = operator_metrics
    (directory / "accounting.json").write_text(json.dumps(summary, indent=2, sort_keys=True) + "\n")
    (directory / "timeline.json").write_text(json.dumps(chrome_trace(trace), sort_keys=True) + "\n")
    text = markdown(summary)
    (directory / "accounting.md").write_text(text)
    (directory / "index.html").write_text(
        "<!doctype html><meta charset='utf-8'><title>Query diagnostic accounting</title>"
        "<style>body{background:#0b1220;color:#e6edf5;font:16px system-ui;margin:3rem;max-width:100rem}"
        "p,li{line-height:1.7;color:#b8c6db}a{color:#79bbff}h2{margin-top:2.5rem;color:#93c5fd}"
        ".table-wrap{overflow-x:auto}table{width:100%;border-collapse:collapse;font-variant-numeric:tabular-nums}"
        "th,td{padding:.65rem .8rem;text-align:right;border-bottom:1px solid #263349}"
        "th{background:#172338;color:#93c5fd}th:first-child,td:first-child{text-align:left}"
        "tr:nth-child(even){background:#101b2c}li{margin:.5rem 0}</style>"
        "<p><a href='timeline.json'>Chrome / Perfetto timeline</a> · "
        "<a href='accounting.json'>Raw accounting</a></p>" + html_body(text)
    )
    return summary
