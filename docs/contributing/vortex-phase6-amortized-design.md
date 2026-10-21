# Phase 6 follow-up: amortized remote query cost

Status: implemented and locally checked; persistent-process remote acceptance
is pending. This experiment belongs to the personal fork.

The completed Phase 6 comparisons start a fresh process for every query. That
measures first-query latency, including first-use S3 client initialization. The
next comparison measures repeated use of a running application, with clients and
connection pools available across queries. It does not subtract an estimated
setup cost from existing samples.

## Execution contract

Add `--execution-mode persistent-process` to Phase 6 tuning, final comparison,
local guard and their service/acceptance helpers. Retain `fresh-process` as the
default for compatibility. Existing reports keep their original meaning and
remain unchanged.

One supervised worker executes the entire seeded performance schedule for a
stage, including all reader configurations and all warmup/measured rounds.
Warmups run in that same process before measured rounds. Sharing the worker
allows Daft's existing I/O clients and TCP pools to be reused by both formats;
it does not guarantee that a particular TCP connection remains open indefinitely.
The parent preserves each trial's identity, result, timing and error.

Every trial still constructs a fresh source and DataFrame. The timer includes
source creation, file discovery, schema inference, planning, execution and
collection of the complete aggregate output. Only imports, runtime warmup,
fixture preparation and correctness validation remain outside query timing.
Never reuse a collected DataFrame or its cached result. There is no global
footer/data cache added for this benchmark; the existing candidate's validated,
per-materialization opening-range reuse remains part of the measured path.

Do not execute an untimed explain query or other file query between timed
trials. Extract plans and requested diagnostics separately after the complete
performance schedule, in the same warmed worker. Diagnostic times
do not enter performance summaries or policy selection.

## Comparison and reporting

Both readers use the same worker, fixtures, authenticated I/O configuration,
seeded interleaving and timing boundaries. OS and server caches remain retained;
this is a steady-state comparison, not a verified cold-data experiment. Connection
reuse and data-cache state are separate claims. The local guard uses the same
execution mode to catch regressions in the persistent-process path.

Store the execution mode in report settings, trial/session provenance and saved
policy selections. Selection validation binds the mode as well as the existing
build, fixture and tuning-report identities. A fresh-process selection cannot
authorize a persistent-process final run. Treat an omitted mode in historical
reports as `fresh-process`; do not pool samples or policies across modes.

CPU time remains the difference in process CPU counters around each timed
query, including its threads. Peak RSS in a persistent process is a lifetime
high-water mark: report it once at session scope, with warmups included. It is
not an independent per-trial measurement, allocation delta, or aggregate memory
limit. Fresh-process reports retain their existing per-trial RSS semantics.

The supervisor enforces a timeout for each trial. On timeout or unexpected
worker exit, terminate and reap that worker, preserve completed records and the
failure, and fail the run. Do not restart the worker, retry the trial, or silently
continue with a newly cold process. Incomplete schedules cannot pass selection
or acceptance. Save redacted worker stderr and session start/completion details.

## Acceptance

Keep the five required cases: narrow scan, 1% range, 1% equality, join and full
value reduction. Keep 20 measured rounds plus one warmup for tuning, 50 plus
three for the frozen final comparison, and 20 plus one for the four-dataset local
guard. Select one policy across all five cases with the existing ranking.

The final gate still requires the candidate median to be strictly below Parquet
in every case. The local guard still permits at most a 5% regression against the
same-build Phase 5 equivalent. Correctness failures, timeouts, incomplete
diagnostics or changed provenance fail acceptance. Passing this median gate
does not establish statistical significance or universal superiority.

Validate mode parsing and propagation, same-process scheduling and warmups,
fresh source construction, independent output checks, deferred diagnostics,
per-query CPU/session RSS semantics, timeouts and worker exit handling, and
selection rejection across modes. Run correctness smoke before release timing.
Keep the HTTP/MinIO/Ray, ORC/MCAP, focused, S3 pooling and unfiltered Rust checks.
All local checks precede any push or GitHub Actions run; benchmarks run without
concurrent builds/tests. Publish the new results separately from the preserved
[fresh-process results](vortex-phase6-results.md).

Local validation passed 697 benchmark/Vortex/orchestration tests, followed by
118 focused worker/supervisor/orchestration tests after the final reporting
changes. Small local CLI smoke runs passed 270 trials and 135 separate diagnostic
queries across both execution modes, including persistent tuning, selection,
final comparison and the four-layout guard. These are correctness checks with
`--allow-dev`, not accepted performance measurements. Pre-commit, dependency
validation and all-features/all-targets Clippy passed. The S3 socket fixtures
compile, but this agent session cannot bind localhost ports; live service and
unfiltered Rust acceptance must run in the unrestricted terminal.
