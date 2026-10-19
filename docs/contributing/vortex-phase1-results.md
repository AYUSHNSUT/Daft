# Vortex Phase 1: native local reader

**Scope:** the personal `AYUSHNSUT/Daft` fork, branch `poc/vortex-phase1`.

**Status:** Phase 1 acceptance complete for the experimental local-reader scope, including reader lifecycle and public-consumer cleanup checks. The fork owner accepted the experimental dependency cost on October 20, 2026. See the [design](vortex-integration-design.md) and [acceptance plan](vortex-integration-test-plan.md).

## Using the reader

The reader is enabled by an experimental Cargo feature. In an existing development environment with dependencies already installed, rebuild the extension in place:

```bash
env -u CONDA_PREFIX CARGO_BUILD_JOBS=2 PYO3_PYTHON=.venv/bin/python \
  .venv/bin/maturin develop --skip-install --offline --locked \
  --features python,vortex
```

`--offline` requires cached Rust dependencies. `--skip-install` reuses the existing Python environment and builds the extension in place; it avoids re-installing unrelated development dependencies. A normal `make build` uses the default feature set and does not enable Vortex.

After generating the [Phase 0 fixtures](../../examples/vortex_eval/README.md):

```python
import daft
from daft import col

df = daft.read_vortex(
    "examples/vortex_eval/artifacts/phase0-vortex/single/data.vortex",
    batch_size=7,
)
df.where(col("value") >= 20).select("id", "value").show()
```

The Python `vortex-data` package is needed to generate fixtures and run writer-based tests. It is not imported by the native reader. Ordinary `import daft` works in builds without the feature; calling `read_vortex()` then explains how to enable it.

## Implemented behavior

- Local files, local `file://` paths, directories searched recursively for `*.vortex`, native globs, and lists of overlapping inputs. Explicit files can have any extension. Resolved duplicate paths are read once.
- One native scan task per file, following the existing MCAP task factory and executor route.
- Schema inference through a Rust binding. Each executed file is checked against the first file's complete ordered names, types, and nullability, including projected-away columns.
- Flat null, boolean, signed/unsigned integers, float32/64, string, and binary columns. Vortex view strings/binary are exported as ordinary Arrow buffers before Daft conversion.
- Complete Daft residual filtering, then final projection, then an exact task limit. The executor retains responsibility for a global limit across files. Counts are not answered through metadata pushdown.
- Empty inputs with a valid file schema, explicit zero-column cardinality, and streams whose retained batches remain valid after source cleanup.

The shared logical count builder now handles a zero-column input by projecting a constant before counting. This preserves row counts instead of indexing a nonexistent first column; both generic empty projections and native zero-field Vortex files have regression coverage.

Source nullability is carried in private scan-schema metadata because Daft logical fields do not store it. Public and materialized schemas remove that private marker while native task serialization retains the full validation schema.

Python source/config descriptors round-trip through pickle and create fresh native tasks. Rust `ScanTask` serialization is tested separately. The shared `PyDataSourceTask` wrapper does not implement Python pickling; worker transport and Ray remain later work.

## Deliberate limits

The native runner and local files are supported. Remote paths, Ray execution, explicit `io_config`, schema evolution, nested/temporal/extension columns, and aggregate pushdowns are rejected. `io_config=None` is required; Daft I/O integration is a later phase. Planning reads the first footer; other schemas are validated when their tasks execute.

The reader currently decodes all physical columns and applies Daft filters/projection afterward. It does not translate predicates or limits into Vortex. Selective physical-column scans belong to Phase 2.

Vortex uses its local opener on the caller's Tokio runtime. Daft byte/request metrics are unavailable for those reads. Batch size bounds requested rows rather than bytes; Vortex's per-worker concurrency and prefetch are not a fixed end-to-end memory bound. Stream ownership tests do not establish cancellation latency or prove all background I/O stops immediately.

The existing native executor uses an unbounded result channel and does not apply the public iterator's `results_buffer_size` to that channel. Closing a query consumer follows its finish path and can wait for execution. Phase 1 tests reader cleanup and consumer cleanup separately; bounded query buffering and prompt query-cancellation behavior remain Phase 3 scheduling work. Vortex also creates split descriptors and registers segment requests ahead of consumption, so a lazy batch stream alone does not bound descriptor, cache, or total query memory.

These are correctness checks using development builds and small fixtures. No performance improvement has been established.

## Validation

Native reader tests:

```bash
CARGO_BUILD_JOBS=2 cargo test --offline --locked \
  --no-default-features -p daft-vortex
```

All **17 Rust reader tests passed**. They generate real Vortex files and cover primitive values/nulls, unsigned boundaries, NaN payloads and signed zero, retained batches, residual/filter/projection/limit order, zero-column and zero-field cardinality, projected-away schema/nullability mismatches, unsupported types, and pathful failures. A file-URL regression creates distinct literal-percent and decoded-space filenames and verifies the literal-percent file is read.

Seven of those tests observe real positional reads through a recording/gated adapter over `FileReadAt`. They run the production scan pipeline with a supplied footer and coalescing disabled so payload reads are independently observable. Counters and notifications verify eventual reader release at EOF, exact limit without another consumer poll, early drop, and a source error after earlier output. Aborting a consumer blocked on a gated async read cancels queued futures without opening the gate. Three separate files cancel independently and release their reader owners and outstanding async requests. Retained batches remain usable after cleanup.

The controlled slow-consumer fixture on this 10-worker machine has 32 chunks: positional reads return 9,856 bytes and emitted batches sum to 2,048 bytes according to `RecordBatch::size_bytes()`, with peaks of 10 active reads and 3,080 in-flight requested bytes. Three concurrently blocked files register 3,696 in-flight requested bytes before cancellation. These are instrumented fixture observations, not disk-device I/O measurements or production byte limits: the normal opener can cache initial segments, coalescing can read neighboring segments, and Vortex creates split descriptors ahead of consumption.

Public and direct task tests use the rebuilt extension:

```bash
DAFT_RUNNER=native DAFT_PROGRESS_BAR=0 .venv/bin/pytest -n 0 -rs \
  tests/io/test_vortex.py tests/io/test_vortex_native_tasks.py tests/io/test_vortex_lifecycle.py
```

The enabled Python API, direct-task, and lifecycle suites passed **89 tests without skips**. Seven public-consumer cases cover explicit close, garbage-collected drop, consumer exceptions, EOF, and a later corrupt-file query. They verify executor plan-registry cleanup and retained Arrow values/buffer bytes without holding independent buffer owners that could mask lifetime errors. They do not claim immediate query cancellation.

Three focused Rust format/scan tests also passed: enum ordinal compatibility, full-schema serialization with clean output metadata/unknown statistics, and explicit generic-glob rejection, bringing Phase 1's focused Rust total to **20**. The generic count suite passed **7 tests**, and the existing Phase 0 harness passed **53**.

A final combined run with the restored Vortex-enabled extension passed **200 tests, with 5 deselected and none skipped**: the 89 Vortex cases, 7 count cases, 53 harness cases, and 51 existing native-task/join/Parquet regressions.

```bash
DAFT_RUNNER=native DAFT_PROGRESS_BAR=0 .venv/bin/pytest -n 0 -rs \
  tests/io/test_vortex.py tests/io/test_vortex_native_tasks.py tests/io/test_vortex_lifecycle.py \
  tests/dataframe/test_count.py examples/vortex_eval \
  tests/io/test_native_tasks.py tests/io/test_datasource_join.py tests/io/test_parquet.py
```

The native baseline passed all **9 query/layout cases**, with output checksums identical to the prior Parquet and Python-reference reports for the same fixtures. These are debug-build correctness results, not performance comparisons. Reproduce the native run after generating fixtures:

```bash
DAFT_RUNNER=native DAFT_PROGRESS_BAR=0 .venv/bin/python examples/vortex_eval/baseline.py \
  --manifest examples/vortex_eval/artifacts/phase0-vortex/manifest.json \
  --source vortex-native --batch-size 7 --repeats 2 \
  --output examples/vortex_eval/artifacts/phase1/vortex-native-baseline.json
```

Use your generated fixture manifest path if it differs from this recorded run.

The first agent-run I/O regressions reported **111 passed, 44 failed, 1 skipped, 5 deselected**. All native-task (11), data-source join (3), and Parquet (37) cases passed. The 44 ORC/MCAP failures hit agent-environment restrictions: macOS `sysctlbyname('hw.l1dcachesize')`, Hugging Face DNS, and localhost socket binding. The ORC error also reproduced in standalone PyArrow.

The fork owner reran the complete ORC/MCAP selection in an unrestricted terminal: **104 passed, 1 skipped in 8.72 seconds**, with all 105 cases collected. The sole skip is the S3 case because S3 configuration is absent. The supplied `/tmp/daft-phase1-io.log` was reviewed; these results clear the earlier environment-related failures without changing or disabling tests.

```bash
DAFT_RUNNER=native DAFT_PROGRESS_BAR=0 .venv/bin/pytest -n 0 -rs \
  tests/io/test_native_tasks.py tests/io/test_datasource_join.py \
  tests/io/test_orc.py tests/io/mcap/test_mcap.py tests/io/test_parquet.py

DAFT_RUNNER=native DAFT_PROGRESS_BAR=0 .venv/bin/pytest -n 0 \
  tests/dataframe/test_count.py examples/vortex_eval
```

Both Python-extension configurations were rebuilt successfully after the lifecycle changes. Without the native feature, the same Vortex suites report **27 passed and 62 skipped**, with all skips explicitly requiring the disabled feature. A fresh process imported Daft, ran an ordinary query, and confirmed that a Vortex call raises the documented enablement error. The pure-Rust feature-off executor check also passed.

All applicable pre-commit hooks passed on the changed files, including strict mypy, pinned Ruff, formatting, dependency validation, and workspace/all-feature/all-target Clippy with `-D warnings`. The initial attempt failed in the dashboard frontend build because Turbopack could not bind a localhost socket. The successful retry used the existing debug option `DAFT_DASHBOARD_SKIP_BUILD=1`, omitting dashboard assets while retaining every Rust lint. The reader's separate all-target Clippy check also passed. `cargo machete` could not run because the tool is not installed.

The complete workspace unit suite and complete Python suite were not run.

Local diagnostic logs are under the ignored `examples/vortex_eval/artifacts/phase1/` directory, including `native-lifecycle-{tests,clippy}.log`, `closure-{enabled,disabled}-tests.log`, and the reviewed user-run `io-unrestricted.log`. Reader lifecycle and controlled request-byte instrumentation are verified. Hard end-to-end memory budgets, prompt query-cancellation latency, interruption of running blocking OS reads, and cloud-worker behavior are outside this phase's acceptance claim; the first two remain explicit Phase 3 work.

## Dependency impact and existing budget

The root lockfile was resolved offline while retaining the existing Azure SDK beta, AWS-LC pins, and Tokio 1.53.1. The shared Arrow array/schema boundary moves from 59.0.0 to 59.2.0; the pre-existing Arrow 56/57/58 families remain for other dependencies. The public Vortex crate is pinned to 0.88.0 with only `files`, `tokio`, and `zstd` requested.

Compared with the pre-Phase-1 root lock, resolution adds **106 external package/version/source identities** and removes **12**. This measures the actual workspace update, rather than the independently resolved standalone probe graph from Phase 0.

The repository's existing dependency-budget script reports **107 added packages and 12 removed, a net 95**, because its current implementation also counts the new local reader package. That exceeds its limit of 25 and the check returns failure. On October 20, 2026, the fork owner accepted this dependency cost for the experiment in `AYUSHNSUT/Daft` (PR #3). This is a documented fork-only review exception; it does not make the budget check pass or change its limit or workflow.

```bash
.venv/bin/python .github/ci-scripts/rust_lockfile_diff.py \
  --base 3236db884 --head WORKTREE \
  --base-lockfile examples/vortex_eval/artifacts/phase1/root-Cargo.lock.before \
  --head-lockfile Cargo.lock --max-new 25 --markdown-summary
```

The generated package list is saved locally as `examples/vortex_eval/artifacts/phase1/dependency-budget.md`. Default builds leave the Vortex dependency disabled; workspace/all-feature builds and the lockfile still include its cost.

Material AI assistance was used for implementation, tests, and documentation. This record distinguishes executed checks from pending or unsupported behavior.
