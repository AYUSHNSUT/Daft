# Vortex Phase 2: selective-scan implementation and validation

**Scope:** experimental local reader in `AYUSHNSUT/Daft`, branch `poc/vortex-phase2`, based on Phase 1 commit `8b59043e774005ec223a55fa1e02086aa0fab54a`.

**Status:** final acceptance checks in progress. The [detailed design](vortex-phase2-design.md) was committed before implementation. The review PR targets `poc/vortex-phase1` and will be opened after verification finishes.

## What changed

Phase 1 read every physical column and evaluated filters after Arrow conversion. Phase 2 selects necessary columns in Vortex and translates a conservative filter subset before materialization. The same Daft engine still evaluates the complete original filter, produces the final column order, and applies exact task limits.

The native reader now uses one scan planner for execution and internal diagnostics. It retains output columns plus every residual dependency, normalizes supported column references, and rebinds residual indices after projection. It validates each executed file's complete ordered schema and source nullability before optimization; projecting away a mismatched column does not conceal the error.

The API adds two independent keyword-only Boolean switches, both enabled by default:

```python
import daft

df = daft.read_vortex(
    "data/*.vortex",
    batch_size=131072,
    enable_projection_pushdown=True,
    enable_filter_pushdown=True,
)
result = df.where(daft.col("value") >= 20).select("id").limit(100)
result.show()
```

Set both switches to `False` for the Phase 1 physical strategy. All four combinations retain the same final semantics. Flags survive source/config serialization and appear in task configuration display. Non-Boolean settings fail clearly.

## Supported filter translation

| Expression | Phase 2 behavior |
| --- | --- |
| Direct integer column and non-null literal of the exact same type | All six comparisons, including reversed operands; all eight signed/unsigned integer types |
| Boolean column and Boolean literal | Equality and inequality |
| Direct flat-column null / not-null check | Translated; dedicated Vortex null predicates allow suitable zone pruning |
| `AND` | Independently translate safe conjuncts |
| `OR`, `NOT`, casts, arithmetic, UDFs, floats, mixed numeric types | Complete Daft fallback for each unsupported subtree |

The complete original predicate always remains a Daft residual, including translated conjuncts. Filter-only columns remain available until that evaluation finishes. No limit is pushed into Vortex, including when only part of a predicate can be translated.

Mixed Int64/UInt64 comparisons deliberately fall back: this Daft revision can compare through Float64, so an exact integer predicate could incorrectly discard matches near 2^53 or 2^63. Floating-point equality also follows the pinned Arrow comparison semantics, including distinct signed zeros; tests preserve that existing behavior.

The internal `_vortex_scan_diagnostics` binding reports actual physical/output columns, translated filter, complete residual, fallback reasons, and requested settings. It uses the execution planner and a complete native scan schema. It is an inspection aid, not a stable public API or a runtime byte counter.

## Evidence of less reading

Five Rust tests open real Vortex files through an instrumented positional reader. The fixtures separate column/chunk payload ranges, supply the footer, and disable coalescing. Zone-specific tests disable whole-file statistics to isolate zone pruning. Each optimized result must equal the independently constructed expected rows and the disabled-optimization result.

| Controlled fixture/query | Baseline bytes | Optimized bytes | Measurement |
| --- | ---: | ---: | --- |
| Wide file, output `id`, no residual | 128,976 | 1,440 | Completed positional-request bytes |
| Wide file, two required columns, including a filter-only dependency | 128,976 | 2,880 | Completed positional-request bytes |
| Zoned integers, no matching zones | 3,120 | 0 | Requested data-payload bytes |
| Zoned integers, half the zones match | 3,120 | 1,560 | Requested data-payload bytes |
| Zoned integers, all zones match | 3,120 | 3,120 | Requested data-payload bytes |
| Null zones, `not_null()` | 3,328 | 1,656 | Requested data-payload bytes |
| Null zones, `is_null()` | 3,328 | 2,548 | Requested data-payload bytes |

Excluded payload intervals are also checked directly, rather than inferring pruning from a byte total. A non-zoned control reads the same bytes with filtering enabled: translating a predicate alone does not guarantee pruning.

These are software requests under controlled layout/cache conditions. They are not disk-device I/O, normal-opener totals, or measured speedups. Metadata reads may remain even when no data payload is needed. Production footer caching, coalescing, and filesystem caching change request behavior.

## Validation

The environment is macOS arm64, Python 3.11.15, pinned Rust nightly `2026-05-27`, PyArrow 22.0.0, and Vortex 0.88.0. Builds use the existing development profile, not release optimizations.

| Check | Result |
| --- | --- |
| Native reader semantics, real-I/O evidence, and lifecycle regressions | 28 passed |
| Rust format/scan configuration and serialization tests | 4 passed |
| Python-enabled reader/scan compilation and focused reader Clippy | Passed |
| Existing native-task/join/Parquet/count regressions and evaluation harness | 117 passed, 5 deselected |
| Final enabled Python integration, feature-off checks, and contribution hooks | Pending final run |

The native integer matrix covers signed/unsigned 8-, 16-, 32-, and 64-bit types, six comparisons, both operand orders, and all four optimization settings. Python coverage adds public planner/task observation, independent expected values, safe fallbacks, zero-column and zero-field cardinality, limits across files, hidden schema mismatches, joins/aggregations, strict flags, and serialization. Existing direct-reader and public-consumer lifecycle tests remain in place.

Two initial Python expectations were corrected after comparison with ordinary in-memory Daft and its implementation: floating equality distinguishes signed zeros, and the public projection API accepts duplicate names while deduplicating required source columns. Native scan diagnostics continue to reject duplicate physical output requests. No production behavior was changed to satisfy either assumption.

Reproduce the enabled build and acceptance suites from the repository root:

```bash
CARGO_BUILD_JOBS=2 cargo test --offline --locked --no-default-features -p daft-vortex
CARGO_BUILD_JOBS=2 cargo test --offline --locked --no-default-features \
  -p common-file-formats -p daft-scan --lib vortex

env -u CONDA_PREFIX CARGO_BUILD_JOBS=2 PYO3_PYTHON=.venv/bin/python \
  DAFT_DASHBOARD_SKIP_BUILD=1 .venv/bin/maturin develop \
  --skip-install --offline --locked --features python,vortex

DAFT_RUNNER=native DAFT_PROGRESS_BAR=0 .venv/bin/pytest -n 0 -rs \
  tests/io/test_vortex.py tests/io/test_vortex_native_tasks.py \
  tests/io/test_vortex_lifecycle.py tests/io/test_vortex_pushdowns.py \
  tests/io/test_native_tasks.py tests/io/test_datasource_join.py \
  tests/io/test_parquet.py tests/dataframe/test_count.py examples/vortex_eval
```

Offline commands require cached Rust dependencies; `--skip-install` reuses the configured Python environment. Rebuild with `--features python` to check feature-off behavior, then restore the enabled build. The existing debug option `DAFT_DASHBOARD_SKIP_BUILD=1` omits dashboard assets in this restricted environment; it does not disable Rust linting.

The [evaluation harness](../../examples/vortex_eval/README.md) accepts independent `--disable-projection-pushdown` and `--disable-filter-pushdown` flags for `vortex-native`. It records the settings and builds fresh sources for every warmup/sample. These small correctness fixtures and development builds do not establish a performance improvement.

Local logs, generated files, and baseline JSON/plan artifacts live under ignored `examples/vortex_eval/artifacts/phase2/`. The complete workspace unit suite and complete Python suite are outside this focused validation. The user's successful Phase 1 ORC/MCAP rerun remains recorded in the [Phase 1 report](vortex-phase1-results.md); those readers are unchanged here.

## Dependencies and remaining scope

Phase 2 changes no Cargo manifests or root lockfile. The dependency-budget comparison against Phase 1 reports **zero added and zero removed crates**, within budget. The already accepted Phase 1 dependency cost remains unchanged; this is not a new exemption or a policy change.

Support remains local files, native runner, flat primitive types, and strict matching schemas. There is no new remote storage, explicit I/O configuration, Ray support, schema evolution, temporal/nested types, writing, or splitting of one file into multiple Daft scan tasks. Daft's production I/O counters remain unavailable on Vortex's local opener.

Batch size and per-reader concurrency are not hard memory budgets. The shared native executor still has an unbounded result channel; consumer close can await query completion. Bounded scheduling, enforced I/O budgets, remote metrics/configuration, and prompt query cancellation remain explicit Phase 3 work. Release-build end-to-end performance evaluation remains Phase 4.

Material AI assistance was used for implementation, tests, review, and documentation. This record distinguishes executed checks, controlled fixture measurements, and unsupported behavior.
