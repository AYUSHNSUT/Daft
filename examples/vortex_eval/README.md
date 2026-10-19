# Vortex evaluation: Phase 0

The Phase 0 tools check fixture correctness, an optional Python reader, and
native Vortex-to-Daft batch compatibility. Their historical results are in the
[Phase 0 report](../../docs/contributing/vortex-phase0-results.md).
This branch also implements the experimental Phase 1 native connector; see
[setup, validation, and limits](../../docs/contributing/vortex-phase1-results.md).

Run the commands below from the repository root, using a working source-built
Daft environment. See the [development guide](../../docs/contributing/development.md)
for `make .venv` and `make build`. These are small correctness checks, not
performance benchmarks; meaningful performance runs require an optimized build.

## Install the optional tools

```bash
uv pip install --python .venv/bin/python -r examples/vortex_eval/requirements.txt
```

The experiment pins `vortex-data==0.88.0` and `pyarrow==22.0.0`. Its dependency
file is separate from Daft's runtime dependencies. Fixture generation and the
Python reference reject a different Vortex distribution version.

## Generate equivalent files

```bash
.venv/bin/python examples/vortex_eval/fixtures.py \
  --output examples/vortex_eval/artifacts/fixtures \
  --seed 42 --rows 257
```

This writes Parquet and Vortex versions of single-file, multiple-file, and empty
layouts. Both formats come from the same seeded Arrow data. The manifest records
versions, writer settings, checksums, row ranges, and independent query results;
files are read back before the completion manifest is written. The multiple-file
layout starts with an empty file to exercise schema discovery. Use a new or empty
output directory for each generation.

## Capture reader baselines

```bash
DAFT_RUNNER=native .venv/bin/python examples/vortex_eval/baseline.py \
  --manifest examples/vortex_eval/artifacts/fixtures/manifest.json \
  --source parquet --repeats 5 \
  --output examples/vortex_eval/artifacts/parquet.json

DAFT_RUNNER=native .venv/bin/python examples/vortex_eval/baseline.py \
  --manifest examples/vortex_eval/artifacts/fixtures/manifest.json \
  --source vortex-reference --batch-size 7 --repeats 5 \
  --output examples/vortex_eval/artifacts/vortex-reference.json

# Requires the extension built with Rust features python,vortex.
DAFT_RUNNER=native .venv/bin/python examples/vortex_eval/baseline.py \
  --manifest examples/vortex_eval/artifacts/fixtures/manifest.json \
  --source vortex-native --batch-size 7 --repeats 5 \
  --output examples/vortex_eval/artifacts/vortex-native.json
```

Each source runs a full scan, a filter with projection, and an aggregation over
all three layouts. Each sample constructs a fresh source and consumes its Arrow
result. Reports include query plans, correctness checks, individual timings,
versions, build type, and source hashes. There is one warmup; OS caches are not
flushed. Source construction and result conversion are timed, while fixture
validation and result checking are outside the timer.

`reference.py` reads projected Vortex Arrow batches through Daft's Python
`DataSource` interface. It supports explicit local files and matching flat
primitive schemas. Daft's Python-source bridge applies pushed filters before
pushed limits; other operators preserve query ordering where needed. Neither is
sent to Vortex, and columns needed by residual filters stay in the projection.
Empty projections use footer row counts,
so counts are unsuitable for measuring scan throughput. Strings and binary views
are normalized to ordinary Arrow buffers. The Python source bridge can buffer
without a fixed byte bound: batch size and a downstream limit do not establish
bounded memory or prompt cancellation.

## Run the isolated native compatibility probe

```bash
env CARGO_BUILD_JOBS=2 CARGO_PROFILE_DEV_DEBUG=0 \
  CARGO_TARGET_DIR="$PWD/target/vortex-phase0" \
  cargo run --locked --manifest-path examples/vortex_eval/rust/Cargo.toml -- \
  examples/vortex_eval/artifacts/fixtures/single/data.vortex 7
```

The probe checks Arrow-to-Daft conversions, values/nulls, explicit row counts,
and batch ownership across early drop and reopening. It is a separate Cargo
workspace; its lockfile and dependency resolution do not change the root
workspace. Its extra checks and scans make elapsed time unsuitable for throughput
comparisons. The profile override disables debug information without enabling
release optimizations. See the [native probe notes](rust/README.md).

## Run focused tests

```bash
DAFT_RUNNER=native .venv/bin/python -m pytest -n0 -q examples/vortex_eval -rs
```

Tests needing the optional Vortex package skip with a reason when it is absent.
A skipped test does not validate Vortex behavior, and root Daft tests do not
compile the isolated Rust probe.

## Explicit Parquet-only fallback

When Vortex cannot be installed, the existing Daft/PyArrow environment can still
check fixture generation and the Parquet baseline:

```bash
.venv/bin/python examples/vortex_eval/fixtures.py \
  --output examples/vortex_eval/artifacts/fixtures-parquet-only \
  --seed 42 --rows 257 --parquet-only

DAFT_RUNNER=native .venv/bin/python examples/vortex_eval/baseline.py \
  --manifest examples/vortex_eval/artifacts/fixtures-parquet-only/manifest.json \
  --source parquet --repeats 5 \
  --output examples/vortex_eval/artifacts/parquet-only.json
```

This mode is recorded in the manifest and cannot produce a Vortex success result.

As recorded on 20 October 2026, focused tests reported **53 passed, zero skipped**
after dependencies were installed from the user's terminal. Both readers passed
all nine baseline cases with matching outputs using the existing development
build. The isolated Rust probe passes offline type checking, builds, and runs on
all six Vortex fixtures. Keep its lockfile: it preserves Daft's compatible Azure SDK beta
instead of the incompatible stable version selected by fresh resolution.
No speedup has been measured.
