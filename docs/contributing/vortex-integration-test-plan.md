# Vortex integration: test and benchmark plan

**Status:** acceptance plan for the [fork design](vortex-integration-design.md). The [Phase 0 experiment](../../examples/vortex_eval/README.md) and experimental Phase 1 native local reader exist. Consult the [Phase 1 validation record](vortex-phase1-results.md) for executed checks and remaining limits; optimization, remote-storage, and performance commands remain future work.

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
| L10 | Native scan descriptor and Python source/config serialization round-trip | Rust `ScanTask` serde retains the full validation schema; restored Python sources create fresh native tasks. Generic `PyDataSourceTask` Python pickling and Ray transport remain Phase 3 work |
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

Proposed locations: reader I/O adapter Rust tests; `tests/integration/io/vortex/`; Ray cases in the focused Vortex tests.

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

Use existing MinIO fixtures for S3 integration and an instrumented local HTTP server for byte-range behavior. These require a test environment that permits localhost sockets. Successful MinIO tests establish only the claimed S3-compatible path; Azure/GCS need separate tests before being advertised. Optional Ray tests run only once distributed support is claimed.

**Gate:** supported I/O schemes honor configuration, errors, cancellation, and metrics; workers and any implemented splits preserve exactly-once coverage.

## Benchmark protocol

Benchmarks are Phase 4 evidence, with a small baseline captured in Phase 0. Proposed harness: `benchmarking/vortex/`. Use existing Parquet benchmark conventions and adapt the [TPC-H table factory](../../benchmarking/tpch/__main__.py) for end-to-end cases.

### Comparisons

| Configuration | Purpose |
| --- | --- |
| Native Daft + Parquet | Primary baseline |
| Native Daft + Vortex, optimized scans disabled | Reader/format baseline |
| Native Daft + Vortex, each optimization enabled | Isolate projection/filter/pruning gains |
| Optional Python Vortex reference | Diagnose bridge overhead; label it separately |
| Optional same-bridge Parquet reference | Separate Python bridge cost from storage-format effects |

Keep both primary formats in the same Daft revision/build. Do not interpret a different engine's Vortex result, or a Python bridge versus native Parquet result alone, as the format's intrinsic speedup.

### Workload matrix

| Dimension | Initial cases |
| --- | --- |
| Data | Repetitive numeric, low/high-cardinality strings, mixed nullable columns |
| Layout | One file and multiple files with matching row boundaries; record internal chunk/layout settings separately |
| Query | Forced value reduction/full consumption, narrow/wide projection, equality/range filters, group-by, one join |
| Filter selectivity | 0%, 0.1%, 1%, 50%, 100% |
| Scale | Small correctness data, then approximately 1-10 GB; larger-than-RAM runs separately |
| Storage | Local first; supported remote storage after Phase 3 |
| End-to-end | TPC-H Q1/Q6, then Q3; expand without dropping regressions or unsupported-query failures |

Record format writer defaults and chosen compression/encodings rather than forcing equal compressed bytes. Keep equal logical data, CPU budget, runner, and result consumption. One task per Vortex file can underuse a single large file relative to Parquet row-group scheduling: match effective parallelism for format isolation and separately report each engine path's normal settings.

### Measurement rules

1. Use a release build with identical settings. Keep data generation, encoding, and correctness checks outside the timed section; report conversion/write time separately.
2. Build a fresh source/query for every repeat: reusing a collected Daft DataFrame can benchmark its cached result. The primary end-to-end timer starts before source creation and includes discovery, schema/footer reads, planning, execution, and full result consumption. Imports and fixture generation stay outside. An optional scan-only timer must use equivalent preplanning/cache policies for both formats. Never time only lazy query construction.
3. Use a real value reduction or fully drain data. Metadata-only `count(*)` is not a scan-throughput measurement; Python object conversion/terminal printing should not dominate the primary comparison.
4. Begin with one warmup and at least five measured trials, randomizing/interleaving formats. Report median and spread, per-query speedup/regression, failures, CPU time, peak RSS, file bytes, and observed read bytes/requests.
5. Separate warm-cache and verified cold-cache runs. A fresh process is not proof of a cold OS cache. Record Vortex segment/footer cache policy and all thread/read-concurrency settings.
6. Save optimized/physical plans and correct output digests with results. Capture environment, CPU/RAM/storage, Daft revision, Vortex/PyArrow/Arrow versions, fixture manifest, and build flags.
7. Profile separately from timing runs; report scan/decode/Arrow-conversion costs where instrumentation supports attribution. Do not fabricate zero I/O metrics when a reader bypasses Daft counters.

There is no guaranteed improvement target. A useful result may be that Vortex helps selective scans but loses on wide scans, or that Arrow conversion dominates. As a planning illustration, halving a scan that accounts for 40% of query time yields about 1.25x overall improvement if the remaining work is unchanged; that is not a forecast for this integration.

**Gate:** every reported performance result has matching correct output, a reproducible run record, and explicit regressions/limitations. The resulting claim is about Daft's two reader paths, not a general Daft-versus-Velox ranking.

## Execution and CI plan

The native crate, Phase 2 pushdown tests, and `vortex` feature now exist. The [Phase 1 implementation record](vortex-phase1-results.md) lists its completed checks; the [Phase 2 design](vortex-phase2-design.md) gives the selective-scan validation commands. Phase 3 service tests and Phase 4 benchmarks still require those paths to be implemented.

Run from the repository root after the normal development setup (`make .venv`) has completed and the required dependencies are available.

```bash
# Rebuild after Rust edits, enabling the experimental feature.
make check-toolchain
env -u CONDA_PREFIX PYO3_PYTHON=.venv/bin/python .venv/bin/maturin develop --uv --features python,vortex

# Focused native-runner tests using the built extension.
DAFT_RUNNER=native .venv/bin/pytest -n 0 \
  tests/io/test_vortex.py tests/io/test_vortex_native_tasks.py

# Pure Rust reader behavior and the feature-enabled executor.
cargo test --no-default-features -p daft-vortex
cargo check --no-default-features --features vortex -p daft-local-execution

# Existing regression coverage affected by the scan changes.
DAFT_RUNNER=native .venv/bin/pytest -n 0 \
  tests/io/test_native_tasks.py tests/io/test_datasource_join.py

# Phase 3: implement these tests and start local services first.
DAFT_RUNNER=native .venv/bin/pytest -n 0 -o addopts='' \
  -m integration tests/integration/io/vortex/

# Phase 4: implement the benchmark suite and make a release build first.
env -u CONDA_PREFIX PYO3_PYTHON=.venv/bin/python .venv/bin/maturin develop --release --uv --features python,vortex
DAFT_RUNNER=native DAFT_PROGRESS_BAR=0 .venv/bin/pytest -n 0 \
  -o addopts='' -m benchmark benchmarking/vortex/ \
  --benchmark-json=/tmp/daft-vortex-benchmarks.json
```

These build commands mirror the existing `make build` / `make build-release` recipes while explicitly adding the experimental feature. This checkout's Makefile does not forward a custom feature argument; use the explicit Maturin invocation. In an already configured environment, `--skip-install` can replace `--uv` to reuse installed Python dependencies; see the [tested offline command](vortex-phase1-results.md#using-the-reader).

Run default builds without Vortex and all-feature checks as well as the targeted configuration. Follow repository pre-commit stages, dependency validation, Clippy, and unused-dependency checks. When the native integration is ready, run the broader Rust/Python CI matrix; run Ray and Python API doctests for capabilities actually claimed.

Repository defaults exclude `integration`, `benchmark`, and `hypothesis` tests. `make test` rebuilds and excludes `tests/integration`; it also defaults to parallel workers. The direct serial pytest commands above are intentional for custom-feature builds, external integration, and benchmarks. Ordinary tests may instead use the normal `DAFT_RUNNER=native make test EXTRA_ARGS="..."` workflow after feature forwarding is wired correctly.

Record baseline failures separately. The Phase 0 branch now includes the independent Rust doctest fix as cherry-pick `e36391a74`; it does not establish that the full workspace suite passed. Record network/localhost/system-query restrictions as execution limitations rather than passing tests. Do not disable failing checks, silently skip required suites, or claim the full suite passed from a partial run.

## Completion record for each phase

Attach the capability/support-matrix change, exact commands and versions, pass/fail/skip counts with reasons, known baseline failures, and benchmark artifacts when applicable. Every phase should leave the fork buildable with the feature disabled. The initial deliverable is a correct experimental local reader; cloud, distributed execution, advanced types, and speed claims each require their own evidence.
