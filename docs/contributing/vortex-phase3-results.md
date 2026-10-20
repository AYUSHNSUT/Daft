# Vortex Phase 3: I/O and scheduling validation

**Scope:** experimental `AYUSHNSUT/Daft` fork, branch `poc/vortex-phase3`, based on Phase 2 commit `c7c8324a05cc475c5bace6c85c28722375cb81da`. Review base: `poc/vortex-phase2`.

**Status:** implementation and local acceptance are complete; real HTTP/S3/Ray service checks remain pending. The fork review PR remains unopened until that gate passes. The [design](vortex-phase3-design.md) was committed before implementation.

Implementation commits separate shared I/O (`a684aeb03`), native result scheduling (`de52a015a`), the configured Vortex reader (`24191370d`), and service acceptance (`21555b4c7`).

## Reader behavior

Vortex footer and data requests now use Daft's configured `IOClient` and caller-owned `IOStatsRef`. Local files, HTTP(S), and S3-compatible paths share the positional adapter. HTTP accepts exact URLs; S3 discovery uses existing Daft listing and glob support. Credentials and endpoint settings travel through discovery, schema inference, serialized plans, and execution. New diagnostics redact URL user information, queries, and fragments while execution retains the original URL.

Each open gets fresh object metadata and reader-local permits. There is no persistent footer cache, and Vortex read coalescing is disabled. Reads validate bounds, exact response length, aligned owned output, and HTTP/S3 range-response intervals. Full responses to range requests fail instead of being interpreted at a different offset. The shared HTTP parser accepts case-insensitive byte units and exact bounded intervals with unknown total size, consistent with [RFC 9110](https://www.rfc-editor.org/rfc/rfc9110.html#section-14.4).

The new keyword-only options are:

```python
frame = daft.read_vortex(
    "data/*.vortex",
    io_config=None,
    max_concurrent_requests=8,
    max_in_flight_bytes=64 * 1024 * 1024,
)
```

Both require positive integers and exclude Booleans. Request count is bounded by Tokio's semaphore limit; the byte budget also must fit `u32::MAX`. A single requested segment exceeding the byte budget fails clearly. Cloned readers share permits, which remain held through asynchronous response collection and alignment. Cancellation releases them. Already-dispatched blocking filesystem operations may finish afterward, outside the active asynchronous-call bound.

These are per-open-reader controls. They do not bound all files, retained Arrow results, Vortex caches, downstream operators, physical device I/O, or total RSS. Local accounting includes footer and data reads. Remote sources count their own streams; resumed reads no longer double-count bytes. Retry streams retain replacement connection permits, release failed permits before reacquiring, and release ownership on drop.

Phase 2 semantics remain: validate the complete ordered schema, retain all residual dependencies, evaluate the complete Daft predicate, preserve zero-column cardinality, and apply exact limits after filtering. No source count pushdown, new data types, writer, schema evolution, or intra-file task splitting was added.

## Result delivery and cancellation

The native executor now honors `results_buffer_size`. Its omitted/`None` local capacity is one partition. Public DataFrame iterators retain their existing `"num_cpus"` default; explicitly passing `None` selects the local capacity of one. Flotilla retains its existing unbounded transport when capacity is omitted.

A capacity of N limits queued results, with at most one additional result held by the router while sending. Consumer-held partitions and upstream/operator buffers are additional. This is backpressure at result delivery, not a hard query-memory cap.

Explicit asynchronous receiver close wakes pending result reads and blocked deliveries. Python bridge and public iterator cleanup propagate closure; interrupted background waits cancel their submitted coroutine. Normal EOF preserves execution statistics; execution errors reach the consumer and trigger cleanup. Intentional preview/iterator closure preserves the existing finished notification, while interruption retains the canceled notification.

Multiple inputs sharing a plan stay isolated. Closing one stops its delivery but may leave its upstream work running while other inputs remain. Closing the last input aborts the plan. Abandoned input IDs are unavailable until plan teardown, preventing late messages from reaching a replacement receiver; normally exhausted IDs remain reusable. A caller retaining an iterator after `break` must close it explicitly.

Ordered scans now require an explicit successful-completion marker before emitting their per-input flush. Channel closure alone can also mean a read failed. When root output closes with unfinished consumers, the executor drains worker outcomes with cancellation still selectable, preserving delayed errors instead of aborting their propagation. Genuine limit completion and explicit abandonment retain prompt abort cleanup. This shared change covers existing readers as well as Vortex.

## Worker transport

The implementation uses the existing Ray/Flotilla distributed planner. Native scan tasks travel inside serialized local physical plans with schemas, source settings, budgets, and storage configuration. Open readers, sessions, file handles, and permits are recreated on the worker.

Local plan round-trip tests and actual Ray execution are separate gates. The service suite compares native and Ray results for local files, HTTP, and MinIO, and verifies that a UDF executes outside the driver process. Transport-only success does not establish worker acceptance.

## Validation

Environment: macOS arm64, Python 3.11.15, Rust nightly `2026-05-27`, PyArrow 22.0.0, and Vortex 0.88.0. Builds use the development profile. No release-performance claim is made.

| Check | Result |
| --- | --- |
| Native reader semantics, adapter budgets, cancellation, and URI diagnostics | 36 passed |
| Shared HTTP/S3 range-response validation | 4 passed |
| Shared retry ownership/accounting regressions | 5 passed |
| Native result-channel, shared-input lifecycle, and delayed errors | 11 passed |
| Ordered-scan successful/failed completion | 2 passed |
| Format/configuration/serialization/display | 5 passed |
| Python-enabled reader and shared-executor Clippy with `-D warnings` | Passed |
| Strict mypy for changed Python APIs | Passed |
| Enabled extension build | Passed |
| Vortex Python API, direct tasks, lifecycle, pushdowns, and Phase 3 | 398 passed, none skipped |
| Shared lifecycle and existing iterator/batching/preview regressions | 166 passed; 17 existing Ray-only partition cases skipped |
| Existing native tasks, joins, Parquet, counts, and evaluation tooling | 119 passed, 5 deselected, none skipped |
| Feature-off extension and fresh import/ordinary query/enablement error | Passed |
| Feature-off Vortex/lifecycle selection | 137 passed; 288 skipped because Vortex is disabled |
| Workspace/all-feature/all-target Clippy with `-D warnings` | Passed |
| Full applicable contribution hooks | Passed |
| Real HTTP, MinIO, and Ray acceptance | Pending; 14 tests collected |
| Disposable MinIO image build | User reported success |

Focused Rust checks:

```bash
CARGO_BUILD_JOBS=2 cargo test --offline --locked --no-default-features -p daft-vortex
CARGO_BUILD_JOBS=2 cargo test --offline --locked --no-default-features \
  -p common-file-formats -p daft-scan --lib vortex
CARGO_BUILD_JOBS=2 cargo test --offline --locked --no-default-features \
  -p daft-io --lib range_response_
CARGO_BUILD_JOBS=2 cargo test --offline --locked --no-default-features \
  -p daft-io --lib test_resumable_
CARGO_BUILD_JOBS=2 cargo test --offline --locked --no-default-features \
  -p daft-local-execution --lib run::tests
CARGO_BUILD_JOBS=2 cargo test --offline --locked --no-default-features \
  -p daft-local-execution --lib sources::scan_task::completion_tests
```

Enabled rebuild and Python acceptance:

```bash
env -u CONDA_PREFIX CARGO_BUILD_JOBS=2 PYO3_PYTHON=.venv/bin/python \
  DAFT_DASHBOARD_SKIP_BUILD=1 .venv/bin/maturin develop \
  --skip-install --offline --locked --features python,vortex

DAFT_RUNNER=native DAFT_PROGRESS_BAR=0 .venv/bin/pytest -n 0 -rs \
  tests/io/test_vortex.py tests/io/test_vortex_native_tasks.py \
  tests/io/test_vortex_lifecycle.py tests/io/test_vortex_pushdowns.py \
  tests/io/test_vortex_phase3.py tests/execution/test_native_result_lifecycle.py

DAFT_RUNNER=native DAFT_PROGRESS_BAR=0 .venv/bin/pytest -n 0 -rs \
  tests/io/test_native_tasks.py tests/io/test_datasource_join.py \
  tests/io/test_parquet.py tests/dataframe/test_count.py examples/vortex_eval

DAFT_RUNNER=native DAFT_PROGRESS_BAR=0 .venv/bin/pytest -n 0 -rs \
  tests/dataframe/test_iter.py tests/dataframe/test_into_batches.py \
  tests/dataframe/test_into_partitions.py \
  tests/dataframe/test_show.py::test_native_runner_close_does_not_log_missing_event_loop \
  tests/dataframe/test_show.py::test_show_limit_does_not_log_active_nodes_warning
```

Offline builds require cached dependencies. The existing `DAFT_DASHBOARD_SKIP_BUILD=1` debug option skips dashboard assets in this environment; Rust linting remains enabled. Rebuild with `--features python` for feature-off checks, then restore `python,vortex`.

The [service runner](../../examples/vortex_eval/README.md#phase-3-service-acceptance) requires an enabled extension, loopback permission, Docker or a supplied MinIO endpoint, and the optional fixture/worker dependencies. It rejects skipped tests and cleans its own container and temporary buckets. This agent session cannot bind loopback or access Docker's socket, even with the user's daemon running; service acceptance must execute in the user's terminal. Collection alone is not acceptance.

Generated data and command logs live under ignored `examples/vortex_eval/artifacts/phase3/`. Full workspace unit tests and the entire Python suite are not represented by these focused checks.

The final enabled Python selections report 683 passes in total, with five cases deselected by repository defaults and 17 existing partition cases requiring Ray. All 27 new shared lifecycle cases pass, including missing-file error propagation with ordering both enabled and disabled. Two tooling tests verify that service-startup failures preserve Docker's image/exit-code/error diagnostics while redacting generated credentials. Feature-off acceptance reports 137 passes and 288 expected feature skips. Native focused selections total 63 passing tests. Logs include `enabled-vortex-final-tests.log`, `enabled-result-final-tests.log`, `enabled-final-regressions.log`, `disabled-final-tests.log`, `native-result-tests.log`, `ordered-scan-completion-tests.log`, and `pre-commit-verified.log`; the diagnostic follow-up has `service-diagnostics-tests.log` and `service-diagnostics-pre-commit.log`.

## Dependencies and remaining scope

The root lockfile comparison against Phase 2 reports **zero added and zero removed external Cargo packages**. `daft-vortex` now depends on existing workspace I/O/configuration crates and uses existing `bytes` in tests. Dependency validation passes; the accepted Phase 1 graph cost is unchanged.

Support remains the pinned Vortex release and flat primitive types with strict matching schemas. Files must not change between planning and execution. No intra-file Daft task splits were added. HTTP/S3 and actual worker support remain subject to their service acceptance gate. Resource controls do not establish a total RSS bound or encoded execution through Daft operators.

HTTP service acceptance uses IPv4 loopback. IPv6 literal hosts are outside the accepted matrix: preserving authority brackets during URL parsing alone does not remove the shared globber's interpretation of those brackets. The literal-authority unit test verifies request spelling, not successful IPv6 service execution.

Release-build comparisons against the same Daft engine reading Parquet, resource measurements, and a declared supported-fork matrix remain Phase 4. No improvement over Parquet, DataFusion, or Velox has been measured here.

Material AI assistance was used for implementation, tests, review, and documentation. This report distinguishes executed checks from pending external acceptance.
