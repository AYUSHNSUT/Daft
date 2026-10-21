# Phase 7: profile query layers and reduce avoidable work

This experiment runs only in the personal fork, on `poc/vortex-phase7`, based on
`cae2c5431` (`poc/vortex-phase6-amortized`). Phase 6 remains unaccepted on
performance: persistent remote Vortex won narrow scans and joins, but missed
Parquet on the two selective filters and full reduction. Its local guard missed
three comparisons against the previous Vortex configuration. All correctness,
service and Rust checks passed in that run.

## Questions and first changes

1. How much selective-query time is spent in discovery, schema inference,
   optimizer/task creation, repeated opens, reading, conversion and reduction?
   Planning currently constructs complete schemas that Python discards. Add an
   opt-in `enable_row_count_planning` path returning only rows and optional
   validated opening-range snapshots, with shared session initialization within
   each metadata batch. Measure that bundle as `lightweight_planning`; keep the
   old path as the same-build control. The option defaults to false.
2. Does remote full-scan concurrency exceed what this local MinIO setup handles
   efficiently? The previous diagnostic recorded 112 data ranges and 64
   overlapping requests across eight files. Compare existing per-file limits
   1, 2, 4 and 8 while holding every other setting constant. A query-wide
   semaphore is deferred until this experiment establishes a useful limit.
3. Are the small local regressions repeatable under adjacent comparisons?
   Run balanced adjacent A/B pairs and an identical-configuration A/A control.
   Do not reinterpret a failed old gate as a pass or discard its samples.

## Profiling contract

Instrumentation is opt-in and bounded. Disabled collectors do not start clocks
or allocate event records. The native collector lives in `common-runtime` so
Vortex, Parquet, common I/O and the execution engine use the same clock without
introducing reader dependencies into the engine. Python stage boundaries read
that native clock too. Reports never expose input URIs, credentials or exception
text through native events; files receive numeric capture-local URI IDs. Normalized aliases or discovery
inputs can have distinct IDs; the historical `file_count` field counts these
observed URI identities, not necessarily physical files.

Capture these boundaries:

| Layer | Measurements |
| --- | --- |
| Query API | Source/query construction and complete collection |
| Source | Discovery, schema inference, planning metadata and task construction |
| Planner | Logical optimization and physical planning |
| Execution | Pipeline translation/start, compute queue and memory permits, task work, output delivery and finalization |
| Reader | Session creation, metadata opening, schema conversion/validation, scan planning, stream setup and batches |
| I/O | Shared client lookup, logical metadata/GET calls, request and byte permits, headers, body collection and cached reads |
| Processing | Reader decode/batch polling, Arrow conversion, residual filter and operator statistics |

Every capture exports raw events, a Chrome trace, per-operation timing tables
and a wall-time coverage breakdown. Report both summed operation durations and
interval unions; the former measures concurrent accumulated work, the latter
measures wall occupancy. Do not add them together. Outer source/collection
boundaries and their uncovered gaps form the query envelope. Nested and
concurrent intervals cannot uniquely attribute a critical path without dependency
information, so ambiguous and uninstrumented intervals remain explicit.
Logical optimization can materialize scan tasks and read planning metadata;
those nested durations are not additional optimizer CPU time.

Parquet selectors may perform hundreds of thousands of tiny decode/skip calls.
Record one decoder-task envelope with bounded counters for all synchronous calls
and their summed wall durations. The envelope includes consumption/backpressure
gaps; the counters do not imply contiguous intervals, interval unions or CPU time.
This preserves complete work accounting without exhausting the event budget.

Async future polling measures synchronous **poll wall time**, including
preemption; the remainder is suspension. This is not thread CPU time or pure
codec time, and work spawned onto other tasks is not owned by the parent poll.
Whole-query process CPU remains a separate measurement. DNS, TCP/TLS setup,
physical retries, kernel queues, server work and internal codec kernels are not
separately timed by these boundaries. Logical request counts and returned bytes
must not be presented as wire traffic. Preserve raw operator counters with their
units; Python's displayed duration can have lower precision.

Diagnostics are additional fresh queries after all performance samples, with
three captures per configuration/case by default. They never enter elapsed-time
summaries or policy selection. They include instrumentation overhead. Each
performance trial creates a new source/query, checks its complete aggregate
against the fixture oracle and retains the same worker's I/O client pools.

## Optimization safety

Lightweight planning combines row-count-only metadata conversion with shared
reader initialization inside a bulk metadata call. Shared initialization is not
a cross-query metadata cache. The comparison measures the combined change and
does not attribute its result to either component alone.

The path still opens fresh metadata on every materialization. It does
not retain schema inference metadata for later use. S3 execution still performs
fresh identity validation, conditionally fetches uncached ranges, and fails closed
if object identity changes. Snapshot budgets and local/HTTP fallback remain as
before. Execution converts and validates the entire schema before projection,
filtering and limits. Unsupported types in later files may now fail at execution
instead of planning; files that are never executed need not undergo conversion.
The experimental option defaults to false.

Request-limit experiments use the existing per-file budget, not a newly claimed
query-wide bound. Changing it does not change the byte budget, coalescing policy,
projection, residual filters or output checks. No default is promoted based on a
single full-scan result or a per-query cherry-picked configuration.

## Experiment and acceptance process

Freeze source, native binary, harness, fixture hashes and options. Use an absent
or empty output directory; preserve every failed trial. A worker failure ends
that worker's useful session without restart or retry. Run sequential adjacent
pairs with exactly balanced AB/BA order, shuffled pair blocks and at least 20
measured rounds plus a warmup for release measurements. Record pair identity and
configuration hashes. A/A controls quantify the harness/environment comparison
floor; they do not excuse regressions automatically.

Run the five remote cases on authenticated temporary local MinIO, and local
controls across sorted/shuffled and single/eight-file layouts. Keep the Phase 6
results as historical evidence; the new Phase 7 diagnostic experiment is not an
acceptance gate. Once evidence supports one candidate configuration, freeze it
for a separate final comparison. The existing requirements remain: win all five
remote medians versus Parquet, and no more than 5% regression in any local case
versus the declared baseline. Neither a diagnostic run nor passing a median gate
proves universal superiority or statistical significance.

## Validation

- Collector: disabled fast path, bounded events, shared clock, capture isolation,
  cancellation/error completion and active-poll accounting.
- Report: known nested/concurrent synthetic intervals, clipping, uncovered time,
  malformed/missing captures, credential-free output and no double counting.
- Schedule: adjacent balanced pairs, identical A/A configs, reproducible order,
  no retries, failures retained and diagnostics excluded from timing.
- Reader: both planning paths return identical rows; refresh after replacement;
  cached/uncached S3 parity; schema/nullability/projection/limit validation remains.
- End to end: both formats and all query shapes; native/Ray/service acceptance,
  ORC/MCAP compatibility, focused Python/Rust checks and repository lint checks.

Rebuild the release extension after native edits. Finish every locally available
check before any push or GitHub Actions run. This agent environment cannot bind
localhost ports or access Docker's socket; prepare one terminal command for the
live service/profile run once the built extension and local checks are verified.
