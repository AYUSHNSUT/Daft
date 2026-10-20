# Vortex files (experimental fork)

!!! warning "Personal fork only"

    This connector is experimental in [`AYUSHNSUT/Daft`](https://github.com/AYUSHNSUT/Daft).
    It is disabled by default and is not an upstream Daft release feature.
    Installing the Python `vortex-data` package does not enable the native reader.

[`daft.read_vortex`][daft.read_vortex] reads Vortex into ordinary Daft batches.
Column selection and a conservative predicate subset can execute inside Vortex;
subsequent Daft operators use decoded values. This is not execution of an entire
query over Vortex-encoded arrays.

## Install from the fork

Use Python 3.11 and the Rust toolchain pinned in `rust-toolchain.toml`
(`nightly-2026-05-27`). From a checkout of the fork's `poc/vortex-phase5` branch:

```bash
uv venv --python 3.11 .venv
uv pip install --python .venv/bin/python maturin==1.13.3 pyarrow==22.0.0

env -u CONDA_PREFIX CI=1 CARGO_BUILD_JOBS=2 PYO3_PYTHON=.venv/bin/python \
  RUST_DAFT_PKG_BUILD_TYPE=release \
  DAFT_DASHBOARD_SKIP_BUILD=1 .venv/bin/maturin develop \
  --release --uv --locked --features python,vortex

DAFT_RUNNER=native .venv/bin/python -c \
  'import daft; from daft import daft as native; assert hasattr(native, "read_vortex_schema"); print(daft.get_build_type())'
```

`CI=1` selects the existing headless build path, where the dashboard flag skips
building optional frontend assets. This does not disable Rust validation or
change the reader. The build-type label and `--release` are
both required by the benchmark harness; the label alone does not optimize code.
Omit `--release` and the label for an ordinary development build. Building with only
`--features python` leaves Vortex disabled. The `daft.read_vortex` export remains
importable in that build and raises an enablement error when called.

The native dependency is pinned to **Vortex 0.88.0**. The fixture writer is optional:

```bash
uv pip install --python .venv/bin/python -r examples/vortex_eval/requirements.txt
```

That file pins `vortex-data==0.88.0` and `pyarrow==22.0.0`. The writer is used by
tests and fixture generation, not by native reads. Other Vortex versions and
arbitrary files produced by future releases are outside this fork's compatibility
matrix.

## Read and query

```python
import daft

df = daft.read_vortex("data/**/*.vortex", batch_size=65536)
result = df.where(daft.col("value") >= 20).select("id", "label").limit(100)
```

Local and S3 directories are searched recursively for `*.vortex`. Explicit files
may have any extension; overlapping inputs are deduplicated. The reader accepts a
string, `pathlib.Path`, or list of those values. Discovery and first-file schema
inference occur when the DataFrame is created; data reads occur during execution.
Do not mutate inputs while a scan is being planned or executed.

Local `file://` URLs cannot have a host, query, or fragment. Their percent signs
are literal, following Daft's local discovery convention. Pass a plain path or
`Path` for names containing spaces or percent characters; an encoded
`Path.as_uri()` can name a different file.

### Planning row counts

When scan tasks are materialized, the reader refreshes the footer of every matched
file through Daft I/O and attaches its exact input row count to the native task.
These counts inform planning, including join build-side estimates. They are
refreshed for each materialization rather than cached permanently in reused or
serialized sources. File discovery and the first-file schema remain fixed; this
does not provide snapshot isolation.

Footer refresh adds metadata I/O before data execution, using the same `IOConfig`
and request budgets as other Vortex reads. This cost is included in end-to-end
planning time, but not in execution-source byte counters. Counts describe file
inputs before filtering: Daft's ordinary filter/limit estimation rules still
apply. Column statistics and aggregation pushdown remain unsupported, and this
does not enable metadata-only count pushdown.

### Remote configuration

The same [`IOConfig`][daft.io.IOConfig] is used for discovery, footer inference,
serialized tasks, and worker execution. If omitted, it comes from the planning
context's default configuration.

```python
from daft.io import IOConfig, S3Config

config = IOConfig(s3=S3Config(region_name="us-east-1", anonymous=True))
df = daft.read_vortex("s3://my-public-bucket/data/*.vortex", io_config=config)
```

HTTP(S) accepts exact file URLs. The server must provide object length and honor
byte ranges with a valid `206` response and matching `Content-Range`. HTML
directory listings and HTTP globs are unsupported. Signed query parameters are
preserved for requests and redacted from new reader diagnostics. IPv6-literal
authorities are outside the accepted matrix because shared discovery can treat
their brackets as glob syntax.

S3-compatible acceptance uses authenticated MinIO. Public AWS endpoints, TLS
certificate arrangements, and additional storage services require their own
environment validation; they are not covered by that local service test alone.
Existing S3 HEAD failures can omit the SDK's authentication code, so a rejected
credential may have a generic error category.

### Projection and filtering

Two keyword-only Boolean switches support comparisons using identical queries:

```python
baseline = daft.read_vortex(
    "data.vortex",
    enable_projection_pushdown=False,
    enable_filter_pushdown=False,
)
optimized = daft.read_vortex("data.vortex")
```

Both default to `True`. Projection reads output fields plus all fields needed by
the complete residual filter. The initial filter subset is exact-type integer
comparisons, Boolean equality/inequality, direct null checks, and safe conjuncts
under `AND`. Mixed numeric types, floats, casts, arithmetic, UDFs, `OR`, and `NOT`
fall back to Daft. Public optimizer rewrites can change an expression's form
before scan planning.

The complete original Daft residual is retained even when a predicate is pushed.
Residual filtering precedes final projection and task limits; the global limit
remains in the query plan. No limit is pushed into Vortex. Enabling a switch does
not prove that a predicate translated, bytes were pruned, or a query became faster.

## Resource controls and observability

```python
df = daft.read_vortex(
    "data.vortex",
    batch_size=131072,
    max_concurrent_requests=8,
    max_in_flight_bytes=64 * 1024 * 1024,
    metadata_concurrency=8,
)
```

These are the defaults. Batch size and request budgets require positive integers
and reject Booleans. Request count is limited by the platform semaphore capacity;
the byte budget also cannot exceed `4294967295`. A single larger requested range
fails rather than bypassing the budget.

Budgets apply per open reader, including footer reads. Multiple file tasks have
separate budgets. They bound active asynchronous requests and their requested
bytes; they do not bound decoded arrays, Vortex caches, consumer-retained batches,
all tasks, physical device I/O, or total process memory. An already dispatched
blocking filesystem operation may finish after cancellation.

Planning reads fresh footer counts in ordered windows of at most
`metadata_concurrency` files. This setting accepts integers from 1 through 64
and defaults to 8; use 1 for a serial comparison. Each concurrently opened file
has its own request/byte budget, so the aggregate can multiply by this setting.
It is independent of execution scan-task concurrency. Reused and pickled sources
refresh counts on every materialization; execution reopens and validates files.

Each file creates one Daft task. A large single file is not split into independent
Daft scan tasks. Native result delivery honors `results_buffer_size`, but that
capacity counts partitions rather than bytes, and upstream/operator buffers are
additional. Ray/Flotilla transport retains its own buffering behavior. Close an
iterator explicitly when abandoning it while retaining a reference.

Use `df.explain(show_all=True)` for the logical/physical plan. Actual physical
Vortex columns and accepted predicates are inspected by the experimental shared
native scan diagnostics used in the tests; requested flags in explain output are
not proof of pruning. Execute `df.collect()` before reading `df.metrics` for
execution byte statistics. Request counters are not currently exposed as public
query metrics, and a streamed `to_arrow()` call does not retain those metrics.

The low-level `daft.daft.read_vortex_metadata_bulk` helper returns ordered
schema/count pairs and metadata-only elapsed time plus Daft I/O counters. These
diagnostics exclude discovery and query execution. Counters are logical I/O
operations and returned bytes, not physical network traffic or device reads.
The [Phase 5 design](../contributing/vortex-phase5-design.md) describes the bounded
metadata comparison and its freshness requirements.

## Compatibility matrix

| Area | Accepted fork scope |
| --- | --- |
| Build | Optional `vortex` Cargo feature; default off |
| Native library | Vortex 0.88.0; existing Arrow conversion boundary |
| Python validation environment | Python 3.11, PyArrow 22.0.0; Ray 2.59.0 for worker tests |
| File paths | Local paths, literal `file://`, exact HTTP URLs, S3-compatible discovery |
| Runners | Native; Ray/Flotilla with the same enabled build and accessible paths on workers |
| Service evidence | IPv4 HTTP range fixture and authenticated MinIO, including actual Ray reads |
| Fields | Null, Boolean, signed/unsigned 8–64-bit integers, float32/64, strings, binary |
| Schema | Identical ordered names, types, and nullability in every executed file |
| Empty data | Empty files and zero-column cardinality supported |
| Excluded | Nested, temporal, decimal and extension types; schema evolution; writers; Azure/GCS; intra-file task splits |

The full schema is validated even when differing fields are projected away.
Vortex values are converted to Daft batches; view normalization and decoding can
allocate. No zero-copy, fixed-RSS, or prompt interruption of OS reads is promised.

The [Phase 3 acceptance record](../contributing/vortex-phase3-results.md) separates
local tests, real services, and remaining limits. The fork-specific
`Vortex experimental` workflow checks feature-on/off behavior and can run real
HTTP/MinIO/Ray acceptance. Shared CI runners enforce correctness, not timing
thresholds.

## Compare with Parquet

The evaluation harness compares Parquet and four Vortex switch combinations in
the same Daft build, with equal logical rows and independently checked results.
Install the fixture-writer requirements above first. The recorded build command
below matches the release invocation in this guide; update it if your build
differs:

```bash
export DAFT_VORTEX_BENCH_BUILD_COMMAND='env -u CONDA_PREFIX CI=1 CARGO_BUILD_JOBS=2 PYO3_PYTHON=.venv/bin/python \
  RUST_DAFT_PKG_BUILD_TYPE=release \
  DAFT_DASHBOARD_SKIP_BUILD=1 .venv/bin/maturin develop \
  --release --uv --locked --features python,vortex'

.venv/bin/python -m benchmarking.vortex generate \
  --output examples/vortex_eval/artifacts/phase4/data \
  --logical-gib 1 --orders sorted shuffled --layouts single multi \
  --files 8 --batch-rows 65536

DAFT_RUNNER=native .venv/bin/python -m benchmarking.vortex run \
  --manifest examples/vortex_eval/artifacts/phase4/data/manifest.json \
  --output examples/vortex_eval/artifacts/phase4/report \
  --cases scan_full scan_narrow range_0.01 group_by join \
  --orders sorted shuffled --layouts single multi --rounds 5 --warmups 1
```

See the [benchmark harness](https://github.com/AYUSHNSUT/Daft/tree/poc/vortex-phase4/benchmarking/vortex) for its measurement
scope, full selectivity matrix, reports, and resource accounting. Performance runs
require the build-command provenance above. Keep warm-cache
and cold-cache claims distinct. These comparisons measure this fork's two reader
paths; they do not rank Daft against DataFusion or Velox.

The [Phase 4 results](../contributing/vortex-phase4-results.md) record release-build
local and MinIO measurements, the join-planning correction, remaining regressions,
and feature-on/off validation.
