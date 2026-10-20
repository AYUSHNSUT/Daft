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

Each trial runs in a fresh subprocess. It initializes Daft with an unrelated tiny
in-memory query, then times fresh source construction, discovery/schema/planning,
execution, and collection of the complete aggregate output. Imports, fixture
generation, correctness checks, and plan extraction are outside the timer. Plans
are obtained **after** timing. Release telemetry is disabled in every child.

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
