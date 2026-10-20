# Vortex integration: test and benchmark plan

**Status:** acceptance plan for the [fork design](vortex-integration-design.md). Phases 0 through 3 are complete for their declared scopes; Phase 3's I/O and worker acceptance is recorded in its [design](vortex-phase3-design.md) and [validation record](vortex-phase3-results.md). Phase 4's harness, connector documentation, personal-fork workflow, and footer row estimates are implemented. The [Phase 4 results](vortex-phase4-results.md) record completed release evaluation and local/user-terminal acceptance under its [plan](vortex-phase4-design.md); GitHub/Linux workflow execution remains separately unverified.

The [Phase 5 plan](vortex-phase5-design.md) adds ordered bulk metadata reads,
fresh bounded planning windows, HTTP concurrency/failure tests, and a separate
same-build metadata comparison. Implementation and local/user-terminal acceptance
are complete; the [results](vortex-phase5-results.md) record 1,428 original local
trials and 357 final remote trials under separate build identities. Final user
validation includes 17 HTTP/MinIO/Ray passes with zero skips, four S3 transport
passes, 112 ORC/MCAP passes with one optional S3 skip, and 2,300 unfiltered Rust
passes with 20 existing ignores. The shared S3 connector and MCAP HTTP fallback
regressions are corrected. Final GitHub/Linux execution remains separately
unverified.

## Test strategy

Use the same fixtures and query cases for the optional Python reference, native Vortex with optimizations disabled, native Vortex with optimizations enabled, and existing native Parquet. The primary correctness oracle is a canonical in-memory Daft table with independently specified expected values. A shared conversion bug must not make two readers incorrectly agree.

For unordered results, compare row multisets; apply explicit sorting where order is part of the assertion. Validate schema, values, null placement, and row counts. Use tolerances only for operations whose floating-point semantics require them, and keep exact integer/string comparisons exact.

| Existing reference | Reuse |
| --- | --- |
| [ORC tests](../../tests/io/test_orc.py) | Python reference, types, schema handling, task serialization, cancellation |
| [Native task tests](../../tests/io/test_native_tasks.py) | Native task construction and pushdown behavior |
| [Data-source join regressions](../../tests/io/test_datasource_join.py) | Columns required after joins and aggregations |
| [Parquet pushdown tests](../../tests/integration/io/parquet/test_read_pushdowns.py) | Compare pushed reads against full read followed by query operators |
| [Split task tests](../../tests/io/test_split_scan_tasks.py) | Split coverage/count regressions; currently Ray-only |
| [Parquet benchmarks](../../benchmarking/parquet/test_filter_pushdown.py) | Deterministic distributions and selectivity matrix |

## Fixtures and compatibility record: Phase 0

Generate Vortex and Parquet from the same seeded Arrow data, preserving logical rows, row order, file boundaries, and schema. Keep conversion outside timed queries. Record generator seed/version, table checksum, row counts, schema, writer version/options, codec/layout options, and bytes per file in a fixture manifest.

Use small generated local fixtures for ordinary tests; no model downloads, public buckets, or real cloud credentials. Add a checked-in compatibility fixture only when testing an older writer/encoding is necessary, with its provenance recorded. Do not check in large benchmark data.

| Fixture family | Required coverage |
| --- | --- |
| Primitive table | Nullable/non-nullable bool, signed and unsigned boundaries, float special values, UTF-8/binary including empty values |
| Cardinality | Zero rows with schema, one row, several batches, exact and uneven batch boundaries, all-null columns |
| Multiple files | Empty file first/middle/last, all files empty, unequal sizes, overlapping path inputs |
| Schema failures | Reordered/missing/extra fields and incompatible logical types fail under the initial strict policy |
| Malformed inputs | Bad magic/footer, truncation before and during payload reads, unknown encoding, unsupported logical type |
| Later types | Timestamp units/timezones, dates, decimal precision/scale, nested nulls/lists/structs only when added to the support matrix |

Phase 0 also proves native Arrow compatibility with a minimal Rust build and validates the fixture writer's Python/PyArrow compatibility. Record actual resolved versions, selected Vortex features, transitive crate delta, platform, and build profile. A documented candidate dependency is not a successful compile test.

The existing experiment pins Vortex 0.88.0 / PyArrow 22.0.0 and provides focused tests under `examples/vortex_eval/`. Its explicit `--parquet-only` mode is useful while Vortex dependencies are unavailable, but cannot satisfy the Vortex compatibility gate. Both probes normalize Arrow string/binary view types before Daft conversion; verify that export with real files. Keep filter-plus-limit cases in the native test matrix: the pinned Vortex scan builder rejects that combination, so Daft must retain the post-filter limit.

## Correct local reader: Phase 1

Implemented files: `tests/io/test_vortex.py`, `tests/io/test_vortex_native_tasks.py`, `tests/io/test_vortex_lifecycle.py`, and unit tests in `src/daft-vortex/`.

| ID | Case | Required result |
| --- | --- | --- |
| L1 | Exact file, `file://`, directory, glob, list, duplicate/overlapping inputs | Declared discovery rules; each input row appears once |
| L2 | Empty path/list, no match, missing/unreadable file, invalid batch size including bool | Consistent actionable exceptions; no silent skip |
| L3 | Every supported primitive/null case | Schema and values equal the oracle and Parquet |
| L4 | Empty file(s), empty filtered result, repeated scans | Preserve schema; no missing or duplicated rows; resources reopen correctly |
| L5 | Schema mismatch in a later file | Fail with file/type context before emitting that file's rows |
| L6 | Filter column absent from final projection | Correct results; required filter columns are available during evaluation |
| L7 | Task carrying a predicate directly | Reader honors it even without an outer Daft filter operator |
| L8 | Zero-column and constant-only projection, `count_rows()`, `count(column)` | Preserve cardinality; distinguish total rows from non-null values |
| L9 | Filter then limit across batches/files | Limit counts matching rows; global query output obeys Daft semantics |
| L10 | Native scan descriptor and Python source/config serialization round-trip | Rust `ScanTask` serde retains the full validation schema; restored Python sources create fresh native tasks. Phase 3 tests actual local-plan transport for Ray; generic `PyDataSourceTask` Python pickling is not needed by that route |
| L11 | Absent optional dependency or disabled native feature | Ordinary import/test collection succeeds; reader call gives a clear enablement error |
| L12 | Errors and schema diagnostics | Useful path and cause; no credentials in messages |
| L13 | Filter-before-limit and limit-before-filter | Match each query's own reference result; the two operator orders are not equivalent |

Test limits `{0, 1, batch_size-1, batch_size, batch_size+1, total, total+1}` and batch sizes `{1, 7, default}`. Exercise scalar comparisons, nulls, conjunctions, and unsupported expressions as Daft residuals. A direct-reader test is required because a correct outer filter can conceal an incorrect scan task.

Use direct Rust tests for zero-column conversion, Arrow ownership/type conversion, corrupt files, and residual filter/projection/limit ordering. Include a nonempty batch with zero physical columns and a literal-only projection, not only a count over it. Do not require a Python-enabled test binary for pure reader behavior.

### Resource lifecycle

Phase 1 tests direct-reader ownership on EOF, exact task-limit completion, drop, errors after a batch, slow consumption, and cancellation during a blocked asynchronous read. Use events/barriers and record started/completed/cancelled reads, concurrent request bytes, and reader drops. Verify eventual source cleanup and continued validity of retained output batches. Cancellation of already-running blocking filesystem reads is best-effort.

For the Python reference, `asyncio.to_thread` cannot interrupt a running `next()`: wait for it before closing the generator on another thread. Check direct-iterator streaming, but document the existing bridge's unbounded receiver and measure its extra buffering. Do not call that path end-to-end memory bounded.

For end-to-end query consumers, test close/drop and error cleanup separately. The existing native executor has an unbounded result channel, and closing a consumer can await query completion; Phase 1 does not promise immediate query cancellation. Report observed reader I/O and buffering, without treating these measurements or `batch_size` as a hard RSS, global queue, or cache budget. Enforced I/O budgets, bounded query buffering, and query-cancellation responsiveness are explicitly deferred to Phase 3's I/O and scheduling work.

**Gate:** supported local queries are correct; direct task semantics pass; schema/feature/lifecycle contracts hold. No native predicate-pushdown speed claim is needed.

## Projection and predicate optimization: Phase 2

Coverage lives in `tests/io/test_vortex_pushdowns.py` and the reader crate's semantic and instrumented I/O tests. The [Phase 2 design](vortex-phase2-design.md) specifies the exact supported predicate subset and acceptance checks; its [completion record](vortex-phase2-results.md) lists executed results.

| ID | Case | Required result |
| --- | --- | --- |
| P1 | One/several/reordered/zero output columns | Correct schema/order/cardinality; inspect actual physical columns requested |
| P2 | Output column plus a filter-only column | Read their union; drop filter-only columns only after filtering |
| P3 | Supported integer/null predicate; 0%, selective, and 100% matches | Pushed and non-pushed results equal the oracle |
| P4 | Supported `AND` with unsupported conjunct | Push only safe conjuncts; Daft evaluates the residual correctly |
| P5 | Partially supported `OR`, `NOT`, casts, UDFs, NaNs | Safe full fallback until the complete subtree is supported; no lost rows |
| P6 | Residual predicate with a small limit | Later matching rows are retained; no premature Vortex limit |
| P7 | Nulls, integer bounds, strings, temporal values as support expands | Translation preserves Daft semantics, including three-valued boolean behavior |
| P8 | Join then select/group-by; projection/filter reordering | Existing required-column regressions remain fixed |
| P9 | Explain/scan instrumentation | Demonstrate the intended scan optimization, not only equal final results |

Parameterize the same query over pushdown-enabled and pushdown-disabled readers. A source spy, read counter, or reader metric must demonstrate projection/pruning where the fixture layout permits it. Metadata can share physical regions with data, so do not demand zero bytes for every unselected field or a universal exact request count.

Keep `supports_count_pushdown()` false. Before ever enabling it, separately test file/split scope, partition predicates, row predicates, limits, zero matches, and `count_rows()` versus nullable-column counts. Unknown statistics never justify pruning or a zero-row answer.

**Gate:** optimization is observable and results remain equivalent; unsupported expressions fall back safely. Do not globally change other Python sources' absorption flags.

## I/O, workers, and splitting: Phase 3

Locations: reader I/O adapter Rust tests, `tests/io/test_vortex_phase3.py`, `tests/execution/test_native_result_lifecycle.py`, and `tests/integration/io/vortex/`. See the [Phase 3 validation record](vortex-phase3-results.md) for checks actually executed.

| ID | Case | Required result |
| --- | --- | --- |
| I1 | Instrumented positional reads: start/end/alignment/bounds | Exactly the requested interval, or a clear error; aligned buffer ownership remains valid |
| I2 | HTTP range support, ignored Range, short/oversized responses | Validate range semantics; never treat a byte-zero full response as an arbitrary requested range |
| I3 | Explicit endpoint/region/credentials/anonymous settings and default context config | Settings reach actual reads as well as listing; logs redact secrets |
| I4 | Missing/forbidden object, transient error, retry exhaustion, cancellation | Match Daft error/retry behavior; no silent corruption or indefinite retry |
| I5 | Footer/data reads across files and repeated tasks | Per-input statistics are isolated; bytes/requests are not counted cumulatively twice |
| I6 | Ray serialization and worker reopen | Same values as native runner; matching feature/dependency availability; no serialized live handles |
| I7 | Optional intra-file row ranges | Half-open ranges cover every row exactly once, including uneven/empty final splits |
| I8 | Split row counts and estimates | Use split cardinality, not repeated whole-file counts; unknown estimates remain unknown |
| I9 | File replacement or metadata-cache reuse | No stale footer served for different file content; cache identity/invalidation is tested |
| I10 | Concurrent request and requested-byte limits | Per-open-reader permits cover active asynchronous collection; oversize requests fail and errors/cancellation release permits |
| I11 | Full result queue, close, pending next, and shared inputs | Delivery remains bounded; close wakes blocked work; unrelated consumers survive; abandoned IDs cannot receive late messages |

Use disposable MinIO fixtures for S3 integration and an instrumented local HTTP server for byte-range behavior. These require a test environment that permits localhost sockets. Successful MinIO tests establish only the claimed S3-compatible path; Azure/GCS need separate tests before being advertised. Ray checks are required before claiming distributed support. The service runner rejects skipped prerequisites. Intra-file splitting remains unimplemented; I7/I8 apply only if it is added.

**Gate:** supported I/O schemes honor configuration, errors, cancellation, and metrics; workers and any implemented splits preserve exactly-once coverage.

## Benchmark protocol

Benchmarks are Phase 4 evidence, with a small baseline captured in Phase 0. The implemented [`benchmarking/vortex/` harness](../../benchmarking/vortex/README.md) provides deterministic batched fixture generation, independent result checks, fresh-process trials, and reports. The [Phase 4 design](vortex-phase4-design.md) defines the current evaluation scope. Its supported primitive query shapes do not implement canonical TPC-H; temporal/decimal-dependent queries require a separately declared extension.

### Comparisons

| Configuration | Purpose |
| --- | --- |
| `parquet`: native Daft + Parquet | Primary baseline, with normal Parquet optimizations |
| `vortex_00`: both Vortex pushdowns disabled | Reader/format baseline |
| `vortex_10`, `vortex_01`, `vortex_11` | Projection only, filtering only, and both enabled |
| Optional Python Vortex reference, outside the Phase 4 harness | Diagnose bridge overhead; label it separately |
| Optional same-bridge Parquet reference, outside the Phase 4 harness | Separate Python bridge cost from storage-format effects |

Keep both primary formats in the same Daft revision/build. Do not interpret a different engine's Vortex result, or a Python bridge versus native Parquet result alone, as the format's intrinsic speedup.

### Workload matrix

| Dimension | Initial cases |
| --- | --- |
| Data | Repetitive numeric, low/high-cardinality strings, mixed nullable columns |
| Layout | Sorted/shuffled logical rows, each in one file and multiple files with matching row boundaries; record internal chunk/layout settings separately |
| Query | Forced value reduction/full consumption, narrow/wide projection, equality/range filters, group-by, one join |
| Filter selectivity | 0%, 0.1%, 1%, 50%, 100% |
| Scale | Small correctness data, then approximately 1 GiB logical data per dataset; larger runs only when resources justify them |
| Storage | Local first; selected HTTP/S3-compatible runs through the accepted Phase 3 paths, reported separately |
| End-to-end | Primitive value reductions, grouped aggregation, and a join to an identical 128-row in-memory dimension; adapted TPC-H remains separate work |

Record format writer defaults and chosen compression/encodings rather than forcing equal compressed bytes. Keep equal logical data, CPU budget, runner, and result consumption. One task per Vortex file can underuse a single large file relative to Parquet row-group scheduling: match effective parallelism for format isolation and separately report each engine path's normal settings.

### Measurement rules

1. Use a release build with identical settings. Keep data generation, encoding, and correctness checks outside the timed section; report conversion/write time separately.
2. Build a fresh source/query for every repeat: reusing a collected Daft DataFrame can benchmark its cached result. The primary end-to-end timer starts before source creation and includes discovery, schema/footer reads, planning, execution, and full result consumption. Imports and fixture generation stay outside. An optional scan-only timer must use equivalent preplanning/cache policies for both formats. Never time only lazy query construction.
3. Use a real value reduction or fully drain data. Metadata-only `count(*)` is not a scan-throughput measurement; Python object conversion/terminal printing should not dominate the primary comparison.
4. Begin with one warmup and at least five measured trials, randomizing/interleaving formats. Report median and spread, per-query speedup/regression, failures, process CPU time, peak process RSS, file bytes, and available execution-source read counters. Request counts are unavailable in the current public source snapshot; do not report them as zero.
5. Separate warm-cache and verified cold-cache runs. The implemented harness retains OS caches and reads fixtures during validation/warmups; a fresh process is not proof of a cold OS cache. Record Vortex segment/footer cache policy and all thread/read-concurrency settings. `--threads 1 --scan-tasks 1` constrains Daft compute and scan tasks, not every Vortex/Tokio worker.
6. Save optimized/physical plans and correct output digests with results. Capture environment, CPU/RAM/storage, Daft revision, Vortex/PyArrow/Arrow versions, fixture manifest, and build flags.
7. Profile separately from timing runs; report scan/decode/Arrow-conversion costs where instrumentation supports attribution. Do not fabricate zero I/O metrics when a reader bypasses Daft counters.

Local Parquet uses direct positional reads that bypass `IOStatsRef`; its file-read byte metric is unavailable even when the raw operator counter is zero. Remote Parquet and Vortex expose returned execution-source bytes, excluding discovery/planning and physical-device or protocol overhead. Attribute file reads to `ScanTask` operator rows; the join's `InMemoryScan` counter describes its dimension table, not storage I/O. Preserve raw operator metrics for inspection and state any missing coverage explicitly.

There is no guaranteed improvement target. A useful result may be that Vortex helps selective scans but loses on wide scans, or that Arrow conversion dominates. As a planning illustration, halving a scan that accounts for 40% of query time yields about 1.25x overall improvement if the remaining work is unchanged; that is not a forecast for this integration.

**Gate:** every reported performance result has matching correct output, a reproducible run record, and explicit regressions/limitations. The resulting claim is about Daft's two reader paths, not a general Daft-versus-Velox ranking.

## Execution and CI plan

The native crate, selective-scan tests, and default-off `vortex` feature exist. Historical acceptance is recorded in [Phase 1](vortex-phase1-results.md), [Phase 2](vortex-phase2-results.md), and [Phase 3](vortex-phase3-results.md). Phase 4 adds the benchmark harness, [connector guide](../connectors/vortex.md), footer-count regression coverage, and [experimental workflow](../../.github/workflows/vortex-experimental.yml); its [validation record](vortex-phase4-results.md) reports release results and completed local/user-terminal checks separately from unverified GitHub execution.

The workflow is restricted to `AYUSHNSUT/Daft` and the declared phase branches. It defines enabled/disabled builds, focused correctness and existing regressions, native reader tests, harness/helper tests, and a small CLI correctness smoke run without timing thresholds. The enabled service gate requires real HTTP, authenticated MinIO, and Ray checks with zero skips when selected; manual dispatch can explicitly omit those service checks. Large performance measurements remain separate manual runs. Record actual CI outcomes separately from the existence of these checks.

Run from the repository root after the normal development setup (`make .venv`) has completed and the required dependencies are available.

```bash
# Rebuild after Rust edits, enabling the experimental feature.
make check-toolchain
env -u CONDA_PREFIX CI=1 DAFT_DASHBOARD_SKIP_BUILD=1 \
  PYO3_PYTHON=.venv/bin/python .venv/bin/maturin develop --uv --features python,vortex

# Focused native-runner tests using the built extension.
DAFT_RUNNER=native .venv/bin/pytest -n 0 \
  tests/io/test_vortex.py tests/io/test_vortex_native_tasks.py

# Pure Rust reader behavior and the feature-enabled executor.
cargo test --no-default-features -p daft-vortex
cargo check --no-default-features --features vortex -p daft-local-execution

# Existing regression coverage affected by the scan changes.
DAFT_RUNNER=native .venv/bin/pytest -n 0 \
  tests/io/test_native_tasks.py tests/io/test_datasource_join.py

# Phase 3: use an enabled build, Docker, and loopback permission.
docker build -t daft-vortex-minio:phase3 - < examples/vortex_eval/minio.Dockerfile
.venv/bin/python examples/vortex_eval/run_phase3_services.py \
  --docker-image daft-vortex-minio:phase3

# Phase 4: verify the harness and service-helper ownership before timing.
DAFT_RUNNER=native DAFT_ANALYTICS_ENABLED=0 .venv/bin/pytest -n 0 -rs \
  benchmarking/vortex/test_*.py examples/vortex_eval/test_phase4_services.py

# Build all benchmark configurations with the same enabled release extension.
env -u CONDA_PREFIX CI=1 RUST_DAFT_PKG_BUILD_TYPE=release CARGO_BUILD_JOBS=2 \
  PYO3_PYTHON=.venv/bin/python DAFT_DASHBOARD_SKIP_BUILD=1 \
  .venv/bin/maturin develop --release --skip-install --offline --locked \
  --features python,vortex

# Record the exact successful build command; the label alone is not proof of optimization.
export DAFT_VORTEX_BENCH_BUILD_COMMAND='env -u CONDA_PREFIX CI=1 RUST_DAFT_PKG_BUILD_TYPE=release CARGO_BUILD_JOBS=2 PYO3_PYTHON=.venv/bin/python DAFT_DASHBOARD_SKIP_BUILD=1 .venv/bin/maturin develop --release --skip-install --offline --locked --features python,vortex'

.venv/bin/python -m benchmarking.vortex generate \
  --output examples/vortex_eval/artifacts/phase4/fixtures-1g \
  --logical-gib 1 --files 8 --batch-rows 65536

.venv/bin/python -m benchmarking.vortex run \
  --manifest examples/vortex_eval/artifacts/phase4/fixtures-1g/manifest.json \
  --output examples/vortex_eval/artifacts/phase4/local-report \
  --rounds 5 --warmups 1
```

These build commands explicitly add the experimental feature because this checkout's Makefile does not forward a custom feature argument. `CI=1` selects the existing headless dashboard build route; `DAFT_DASHBOARD_SKIP_BUILD=1` skips optional frontend assets. The release command assumes installed Python dependencies and cached Cargo dependencies for `--skip-install --offline`. The [harness README](../../benchmarking/vortex/README.md) also documents small correctness smoke runs, remote provisioning, and build provenance. Output directories must be absent or empty. Finish builds, tests, and fixture generation before timing; do not run them concurrently with measurements.

Run default builds without Vortex and all-feature checks as well as the targeted configuration. Follow repository pre-commit stages, dependency validation, Clippy, and unused-dependency checks. When the native integration is ready, run the broader Rust/Python CI matrix; run Ray and Python API doctests for capabilities actually claimed.

Repository defaults exclude `integration`, `benchmark`, and `hypothesis` tests. `make test` rebuilds and excludes `tests/integration`; it also defaults to parallel workers. The direct serial pytest commands above are intentional for custom-feature correctness tests. Phase 4 measurements use the `python -m benchmarking.vortex` CLI, not pytest-benchmark. Ordinary tests may instead use the normal `DAFT_RUNNER=native make test EXTRA_ARGS="..."` workflow after feature forwarding is wired correctly.

Record baseline failures separately. The Phase 0 branch now includes the independent Rust doctest fix as cherry-pick `e36391a74`; it does not establish that the full workspace suite passed. Record network/localhost/system-query restrictions as execution limitations rather than passing tests. Do not disable failing checks, silently skip required suites, or claim the full suite passed from a partial run.

## Completion record for each phase

Attach the capability/support-matrix change, exact commands and versions, pass/fail/skip counts with reasons, known baseline failures, and benchmark artifacts when applicable. Every phase should leave the fork buildable with the feature disabled. The initial deliverable is a correct experimental local reader; cloud, distributed execution, advanced types, and speed claims each require their own evidence.
