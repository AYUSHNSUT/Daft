# Native Vortex / Parquet evaluation

Run from the repository root. This harness compares two reader paths in the same
Daft build. It does not compare Daft with other engines or constitute canonical
TPC-H. Vortex 0.88.0, PyArrow, and NumPy are required for fixture generation.

Fixtures contain nullable integers/Booleans, repeated labels, high-cardinality
64-character strings, and synthetic integer equality selectors. The sorted and
shuffled variants contain identical logical rows; the latter uses a seeded affine
permutation without allocating a whole-dataset permutation. Single/multi layouts
use matching row boundaries across formats. Data is generated in bounded batches,
streamed into Parquet and Vortex writers, and recorded with file/input hashes,
logical byte counts, writer settings, and scalar Python query expectations.
Generation/encoding/input-hashing time is recorded separately from query timing.

## Small functional check

```bash
DAFT_RUNNER=native DAFT_ANALYTICS_ENABLED=0 .venv/bin/pytest -n 0 -rs \
  benchmarking/vortex/test_benchmark.py

.venv/bin/python -m benchmarking.vortex generate \
  --output examples/vortex_eval/artifacts/phase4/smoke-fixtures \
  --rows 4096 --files 8 --batch-rows 65536

.venv/bin/python -m benchmarking.vortex run \
  --manifest examples/vortex_eval/artifacts/phase4/smoke-fixtures/manifest.json \
  --output examples/vortex_eval/artifacts/phase4/smoke-report \
  --cases scan_full scan_narrow range_0.01 group_by join \
  --rounds 1 --warmups 0 --allow-dev
```

Output directories must be absent or empty. Generated data belongs under the
ignored artifact locations shown above, not in Git. `--allow-dev` marks the entire
report as correctness smoke, even when the installed extension is optimized.

## Release comparison

Build once, with the Vortex feature enabled for **all** configurations:

```bash
env -u CONDA_PREFIX CI=1 RUST_DAFT_PKG_BUILD_TYPE=release CARGO_BUILD_JOBS=2 \
  PYO3_PYTHON=.venv/bin/python DAFT_DASHBOARD_SKIP_BUILD=1 \
  .venv/bin/maturin develop --release --skip-install --offline --locked \
  --features python,vortex
```

Set `DAFT_VORTEX_BENCH_BUILD_COMMAND` to the exact successful build command before
running. The release label is an explicit attestation, not proof of optimization;
the report also records the native binary hash, source revision, harness hashes,
lockfile hash, Rust version, and Arrow package versions. `CI=1` selects the existing
headless dashboard build route; dashboard assets do not participate in query
execution. Offline builds need cached dependencies. Do not run builds or other
tests concurrently with measurements.

```bash
.venv/bin/python -m benchmarking.vortex generate \
  --output examples/vortex_eval/artifacts/phase4/fixtures-1g \
  --logical-gib 1 --files 8 --batch-rows 65536

.venv/bin/python -m benchmarking.vortex run \
  --manifest examples/vortex_eval/artifacts/phase4/fixtures-1g/manifest.json \
  --output examples/vortex_eval/artifacts/phase4/local-report \
  --rounds 5 --warmups 1
```

`--logical-gib` is an approximate per-dataset target; the manifest records actual
Arrow logical bytes. Four order/layout combinations and two encoded formats are
written by default. `--rows` specifies an exact row count. `--orders`, `--layouts`,
`--cases`, and `--configs` select smaller matrices without editing the harness.

| Configuration | Physical Vortex projection | Physical Vortex filtering |
| --- | --- | --- |
| `parquet` | Native Parquet defaults | Native Parquet defaults |
| `vortex_00` | Off | Off |
| `vortex_10` | On | Off |
| `vortex_01` | Off | On |
| `vortex_11` | On | On |

`scan_full` reduces every column, including string lengths; `scan_narrow` reduces
only `id`/`value`. Both force value access and return small aggregates.
`range_S` filters `id < floor(rows*S)`; `eq_S` compares a synthetic Int64 selector
with 0/1 to produce the same selected IDs. Supported S values are `0`, `0.001`,
`0.01`, `0.5`, and `1`; actual fractional selectivity is rounded down by row count.
`group_by` aggregates 128 groups. `join` joins the scanned fact table to an identical
128-row in-memory dimension and reduces weighted values. These are primitive query
shapes; date/decimal-dependent TPC-H queries require a separately declared change.

## Timing, resources, and interpretation

By default each trial runs in a fresh subprocess. It initializes Daft with an unrelated tiny
in-memory query, then times fresh source construction, discovery/schema/planning,
execution, and collection of the complete aggregate output. Imports, fixture
generation, correctness checks, and plan extraction are outside the timer. Plans
are obtained **after** timing. Release telemetry is disabled in every child.
Phase 6 also supports the persistent-process mode described below; Phase 4/5
behavior and historical reports remain unchanged.

Every warmup and measured round contains all selected combinations in randomized,
seeded order. Release comparisons require at least one warmup and five measured
rounds. Hash validation and warmups read the data; OS caches are not flushed.
These are explicitly cache-retained runs, not verified cold-cache results.

Reports retain every sample/error, expected-output digest, median/min/max times,
CPU seconds, process peak RSS, encoded file sizes, and source read bytes where
available. CPU seconds include process threads. RSS is the isolated process
high-water mark through query completion, including imports/runtime initialization;
it is not a query allocation delta or total-memory bound. Source read bytes include
only executed file-source nodes (`category=Source`, `type=ScanTask`), including
their metadata/payload reads. Separate discovery/schema planning and in-memory
sources, such as the join dimension, are excluded. These are returned bytes, not
disk-device I/O.

Local Parquet positional reads bypass the relevant I/O statistics, so its
`source_read_bytes` is explicitly unavailable (`null`), even if a raw counter is
zero or partial. Vortex local/remote and remote Parquet counters are retained when
exported. Each trial records coverage/reason fields, raw file/non-file source
counters for diagnostics, and the original operator metrics; raw counters must
not be interpreted as complete local Parquet bytes. Request counts remain
unavailable because the current source snapshot does not export them.

`--threads N` controls Daft compute threads; `--scan-tasks N` controls concurrent
Daft scan tasks. Neither caps every Vortex/Tokio I/O worker. Defaults preserve the
existing engine settings. A restricted `--threads 1 --scan-tasks 1` run must be
labeled accordingly, not "single-core." Vortex uses one Daft task per file, while
Parquet may schedule row groups. Report the one-file and multifile results
separately; do not attribute all scheduling differences to storage encoding.

`report.md` includes regressions as ratios below one. `report.json` retains full
provenance, settings, file sizes, and per-trial records; `plans/` contains diagnostic
plans and `trials/` contains specs/results. A failed or timed-out query makes the run
fail, while remaining cases are still reported. Winning every query is not a gate.

## Remote subset

Upload the exact manifest-relative files to a dedicated HTTP/S3 prefix, then use
`--storage-root https://host/prefix` or `--storage-root s3://bucket/prefix`.
Local manifest/file hashes are still validated before timing. Provisioning/upload
are excluded. Remote object immutability is the caller's responsibility.

If required, `DAFT_VORTEX_BENCH_IO_JSON` contains an object with `s3`/`http`
dictionaries accepted by Daft's `S3Config`/`HTTPConfig`; the same configuration is
used for both readers. Credentials are passed only through the environment and
redacted from captured diagnostics. Do not put credentials into storage-root URLs
or build-command provenance. Only a configuration-present flag is recorded.
Start with `--configs parquet vortex_11 --orders sorted --layouts multi` and the
five smoke query cases above, preserving five measured rounds for release results.

For a disposable local MinIO comparison, use the service runner below. It uploads
only the selected dataset, supplies the same authenticated configuration to both
readers, and removes its unique bucket and container on completion or failure.
The successful release build and `DAFT_VORTEX_BENCH_BUILD_COMMAND` setup above
remain prerequisites. Install `boto3` for provisioning. Service startup, uploads,
and cleanup are outside the query timer.

```bash
.venv/bin/python -m benchmarking.vortex generate \
  --output examples/vortex_eval/artifacts/phase4/remote-fixtures \
  --logical-gib 0.125 --orders sorted --layouts multi --files 8

docker build -t daft-vortex-minio:phase4 - < examples/vortex_eval/minio.Dockerfile

.venv/bin/python -m examples.vortex_eval.run_phase4_services \
  --manifest examples/vortex_eval/artifacts/phase4/remote-fixtures/manifest.json \
  --output examples/vortex_eval/artifacts/phase4/remote-report \
  --docker-image daft-vortex-minio:phase4
```

An existing MinIO image can be reused by passing its exact local tag instead.
Alternatively, set `VORTEX_MINIO_ENDPOINT`, `VORTEX_MINIO_ACCESS_KEY`, and
`VORTEX_MINIO_SECRET_KEY` for a developer endpoint; the runner then owns only its
unique bucket. Endpoint URLs must not contain credentials, queries, or fragments.
This experiment measures local S3-compatible transport and does not represent
public-cloud latency or throughput. It does not measure Ray scaling.

## Metadata planning comparison

The [Phase 5 results](../../docs/contributing/vortex-phase5-results.md) record
the completed local and MinIO comparisons, build identities and remaining regressions.

Phase 5 adds a separate harness for `read_vortex(metadata_concurrency=...)`.
It compares Vortex concurrency 1 and 8 in the same release build, alongside
contemporaneous Parquet query trials. Metadata-only trials report fresh bulk
footer calls separately from complete query timings. They expose the helper's
configured file cap, elapsed time and available Daft I/O counters; they exclude
discovery/execution and do not measure physical network requests or disk I/O.

After the enabled release build and provenance setup above, reuse existing
fixtures:

```bash
.venv/bin/python -m benchmarking.vortex.metadata_eval \
  --manifest examples/vortex_eval/artifacts/phase4/fixtures-1g/manifest.json \
  --output examples/vortex_eval/artifacts/phase5/local-report \
  --concurrency 1 8 --cases scan_narrow range_0.01 eq_0.01 join scan_full \
  --rounds 20 --warmups 1

.venv/bin/python -m examples.vortex_eval.run_phase5_services \
  --manifest examples/vortex_eval/artifacts/phase4/remote-fixtures/manifest.json \
  --output examples/vortex_eval/artifacts/phase5/remote-report \
  --docker-image draft-vortex-minio:phase3 \
  --cases scan_narrow range_0.01 eq_0.01 join scan_full \
  --rounds 20 --warmups 1
```

The image name is the existing local developer tag; substitute the exact tag of
your own MinIO build. The helper owns only its temporary bucket/container and
uses the same configured credentials for both readers. Provisioning/upload and
cleanup are outside the query timer.

The default query subset is narrow projection, 1% range and 1% equality. `--cases`
also accepts `join` and `scan_full`; `--modes metadata` isolates footer calls.
`--orders`/`--layouts` select datasets for the direct harness, and the service
helper uses `--order`/`--layout`. Normal comparisons require 20 measured rounds
and one warmup. `--allow-dev --rounds 1 --warmups 0` is correctness smoke only.

All trials use fresh processes and seeded interleaving, retain correct output
digests and failures, and record build/binary/source/fixture identity. Reports
show Parquet/Vortex query ratios and Vortex concurrency-1/concurrency-8 ratios
separately. OS caches remain retained. A concurrency setting is not a global
memory or CPU bound; each active file retains its own request/byte budgets.

Each trial also saves UTC subprocess start/completion times, its process exit
code, and full redacted worker stderr. These diagnostics are separate from the
worker's query or metadata timer. The MinIO helper saves
`service-diagnostics.json` before cleanup, including service health and, for a
container it owns, selected container state, Docker versions and bounded logs.
It takes snapshots before and after the evaluation without polling during
trials. A failed trial still fails the comparison; diagnostics add no retries or
pauses between trials. Use a new output directory for each attempt so failed
runs remain available.

## Phase 6 remote read policies

`remote_eval.py` keeps Phase 4/5 reports and behavior separate. It compares the
normal Parquet reader with explicit Vortex discovery/coalescing controls in the
same enabled release build. All configurations retain metadata concurrency 8,
both pushdowns, batches of 131,072 rows, and the existing request/byte budgets.
On `poc/vortex-phase6-metadata`, tuning and candidate configurations also enable
validated S3 opening-range reuse. The Phase 5-equivalent baseline disables it.
The public reader switch remains opt-in. Local inputs take the ordinary fresh
path, so the local guard also checks the fallback. See the
[follow-up design](../../docs/contributing/vortex-phase6-metadata-design.md).
Build changes require fresh tuning and final reports; the first Phase 6 run is
preserved as a failed performance acceptance and is not pooled with this run.
Use the attested release build and `DAFT_VORTEX_BENCH_BUILD_COMMAND` described
above; production comparisons require a clean tracked checkout.

Use `--execution-mode persistent-process` for the amortized-cost experiment. One
supervised worker executes all configurations and warmup/measured rounds in the
seeded schedule, retaining existing clients and connection pools across queries.
Every trial still builds a fresh source and DataFrame and times discovery,
schema, planning, execution and collection. Collected results are never reused.
This measures steady-state application use with retained OS/server caches, not
verified cold data. A pooled connection may still be closed by the server or
client. See the [execution contract](../../docs/contributing/vortex-phase6-amortized-design.md).

`fresh-process` remains the default. Execution mode is recorded in reports and
saved selections; tuning, final comparison and local guard must use matching
modes. Historical selections without a mode are fresh-process selections. Never
pool results across modes. The commands below explicitly select the persistent
mode; no successful performance result is implied by these examples.

For tuning alone, use authenticated local MinIO after all builds/tests stop.
The one-command acceptance flow below also runs this tuning stage automatically.

```bash
.venv/bin/python -m examples.vortex_eval.run_phase6_services \
  --manifest examples/vortex_eval/artifacts/phase4/remote-fixtures/manifest.json \
  --output examples/vortex_eval/artifacts/phase6/tuning \
  --docker-image draft-vortex-minio:phase3 \
  --stage tune --rounds 20 --warmups 1 --diagnostics \
  --execution-mode persistent-process
```

The fixed tuning grid includes Parquet and four coalescing policies at discovery
caps 1 and 8: off, 16 KiB gap / 1 MiB maximum range, 64 KiB / 4 MiB, and
256 KiB / 4 MiB. All five cases are mandatory for production runs: narrow scan,
1% range, 1% equality, join and full value reduction. This gives 945 performance
trials for one dataset, plus 45 separate diagnostic queries when requested.

The report ranks policies by the worst case Vortex/Parquet median ratio, then
geometric mean elapsed time, total execution-source returned bytes and stable
configuration ID. `selected-policy.json` recommends one policy among discovery-8
configurations for every case. Its identity and tuning-report hash are saved.
An off policy can win this selection. No case gets its own tuned policy.
`--selection` verifies the adjacent tuning report's hash, completed trial grid,
ranking and winner controls. Production reuse requires the same source/native
build; final remote acceptance also requires the same fixture manifest. The
local guard may use its separate local fixture manifest.

Run a separate local regression guard with the same three configurations and
the existing one/eight-file, sorted/shuffled corpus:

```bash
.venv/bin/python -m benchmarking.vortex.remote_eval \
  --manifest examples/vortex_eval/artifacts/phase4/fixtures-1g/manifest.json \
  --output examples/vortex_eval/artifacts/phase6/local-guard \
  --stage guard --orders sorted shuffled --layouts single multi \
  --rounds 20 --warmups 1 --candidate-policy 64k_4m \
  --execution-mode persistent-process
```

The guard accepts local fixture paths only, requires at least 20 measured rounds
and one warmup, and requires every candidate median to be at most 1.05 times
the Phase 5 equivalent median. Parquet remains a contemporaneous comparator;
the local guard does not require beating it. Use `--selection` to freeze the
tuned policy instead of the explicit preset. The final remote performance gate
below retains its stricter candidate-below-Parquet criterion.

Final acceptance freezes Parquet, the Phase 5 equivalent (coalescing off,
discovery 1, metadata reuse off), and one candidate policy at discovery 8 with
metadata reuse on. It requires exactly
50 measured rounds and 3 warmups: 795 performance trials plus 15 separate
diagnostic queries for one dataset. Every candidate median must be strictly
below the contemporary Parquet median. A tie, regression, trial error, changed
provenance, or incomplete requested diagnostic capture prevents acceptance.
This median gate does not establish statistical significance or a universal win.

Run the complete terminal acceptance with one command after the enabled release
build and local checks are finished:

```bash
.venv/bin/python -m examples.vortex_eval.run_phase6_acceptance \
  --manifest examples/vortex_eval/artifacts/phase4/remote-fixtures/manifest.json \
  --local-manifest examples/vortex_eval/artifacts/phase4/fixtures-1g/manifest.json \
  --output "examples/vortex_eval/artifacts/phase6/acceptance-$(date -u +%Y%m%dT%H%M%SZ)" \
  --docker-image draft-vortex-minio:phase3 \
  --tune --execution-mode persistent-process
```

This terminal helper does not rebuild/install the Python extension. It runs
20-round/one-warmup tuning with diagnostics, validates the saved winner, then
passes that frozen selection to the 50-round/three-warmup final comparison with
diagnostics. Both use the same temporary MinIO server and remote fixture manifest.
With `--local-manifest`, it next runs the same frozen selection through the local
guard for all four order/layout datasets, with 20 measured rounds, one warmup and
separate diagnostics. This adds 1,260 performance trials and 60 diagnostic queries.
All three stages run sequentially before any test processes. A tuning failure,
invalid selection, or source/native change blocks the dependent final comparison
and local guard and is preserved as a failed acceptance; the correctness checks
still run. A final performance miss does not block the independent local guard.
The helper then runs
strict HTTP/MinIO/Ray services with a persistent zero-skip JUnit gate,
ORC/MCAP, focused Vortex/harness tests, S3 TCP pooling tests and the full
unfiltered Rust workspace; Cargo may compile test binaries after timing finishes.
It preserves logs, exit markers, parsed counts and
`acceptance.json`, and continues checks after a final performance miss. Optional
`--expected-source` and `--expected-native-sha` guard the exact tested build.
It removes only its temporary buckets/container. To reuse a separately completed
tuning run, replace `--tune` with `--selection PATH/selected-policy.json`; these
options are mutually exclusive. For a final comparison without
the additional acceptance tests, use `run_phase6_services --stage final` with
the same selection and `--execution-mode persistent-process` when that was used
for tuning. `--candidate-policy` instead freezes an explicit preset
before timing; its default is `64k_4m`.

The direct harness is `python -m benchmarking.vortex.remote_eval`, with
`--storage-root`, `--orders` and `--layouts` matching the existing harness.
Service helpers use `--order`/`--layout`. `--allow-dev --rounds 1 --warmups 0`
permits correctness smoke only; such a run cannot satisfy the final gate or
produce a selection accepted by a production final run.

Each `--diagnostics` capture wraps one executed source-construction/collection
query **after** the performance schedule, in the same warmed worker in persistent
mode or a separate child in fresh-process mode. The recorder finishes
before cached-output validation or the separate explain query. Native/Python
events and logical I/O counters are saved separately, with explicit dropped or
unfinished-event flags. Nested and concurrent durations overlap; summing them
would not produce a query latency breakdown. `batch_wait_decode` includes I/O
wait. Parquet captures cover outer source creation and collection only; missing internal phases/request
counters are unavailable, not zero cost. Physical transport retries remain
unavailable. Diagnostic measurements
never enter the performance summaries.

Both modes retain seeded interleaving, independent result oracles, plans,
per-query CPU, returned source bytes, hashes, UTC timestamps and redacted stderr.
Fresh-process mode retains process-high-water RSS per trial. Persistent-process
mode reports lifetime peak RSS once per session, including warmups; it cannot
attribute that high-water mark to an individual query. Plans and diagnostics run
after the complete performance schedule, with no untimed explain query between
its trials. A persistent worker timeout or unexpected exit fails the run and
preserves completed samples; the worker is not restarted and trials are not
retried. There is no inter-trial pacing. The local Docker service is not an
AWS/WAN benchmark; caches are retained, and client RSS excludes MinIO/Docker
resources.

## Phase 7: paired diagnosis and full-layer traces

See [the Phase 7 design](../../docs/contributing/vortex-phase7-design.md). The new
release build exposes shared-clock Python/native diagnostic events for both
formats. Run profiling separately from performance samples; do not run builds or
tests concurrently with a comparison.

```bash
.venv/bin/python -m benchmarking.vortex.phase7 \
  --manifest examples/vortex_eval/artifacts/phase4/fixtures-1g/manifest.json \
  --output examples/vortex_eval/artifacts/phase7/local \
  --datasets sorted/single sorted/multi shuffled/single shuffled/multi \
  --experiments aa row-count parquet

.venv/bin/python -m examples.vortex_eval.run_phase7_services \
  --manifest examples/vortex_eval/artifacts/phase4/remote-fixtures/manifest.json \
  --output examples/vortex_eval/artifacts/phase7/remote \
  --docker-image draft-vortex-minio:phase3
```

Release diagnosis requires a clean tracked checkout and
`DAFT_VORTEX_BENCH_BUILD_COMMAND` set to the actual release build command. Use a
new output directory on each deliberate experiment. Defaults are 20 adjacent
balanced measured pairs, one warmup and three later diagnostic repetitions.
Comparisons isolate row-count-only planning and request caps (1/2/4 versus 8 per
file); A/A uses identical configurations. All queries construct fresh sources in
one persistent process; source creation remains included.

Each `profiles/NNNNN/` contains accounting JSON/Markdown/HTML and a Chrome trace.
The top-level report links each diagnostic to its exact workload/configuration.
Wall unions, summed event durations and active-poll time have different meanings;
read the coverage notes before attributing cost. This is exploratory evidence,
not a replacement for the unchanged final acceptance gates.
