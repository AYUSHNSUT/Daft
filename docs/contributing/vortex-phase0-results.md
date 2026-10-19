# Vortex Phase 0: compatibility record

**Recorded:** October 20, 2026. **Status:** Phase 0 complete for the recorded primitive fixtures. Native connector implementation remains Phase 1.

This experiment belongs to `AYUSHNSUT/Daft`, on `poc/vortex-phase0`, created from `poc/vortex-eval` at `d3cd974150ba93dbbe8efef8370b129538fcee4b`. The requested doctest fixes were cherry-picked separately as `e36391a74a3db872921beb72e3c6b3f72c48b40a` (`docs: fix workspace Rust doctests`). No upstream Vortex PR or public `daft.read_vortex` API has been created.

See the [experiment instructions](../../examples/vortex_eval/README.md), [design](vortex-integration-design.md), and [acceptance plan](vortex-integration-test-plan.md).

## Delivered experiment

- A deterministic generator with independent query expectations, byte and logical checksums, and equivalent single-file, multi-file, and empty layouts. Full mode writes both formats and verifies each readback before publishing its manifest. An explicit Parquet-only mode records missing Vortex coverage.
- An optional Python `DataSource` reference for explicit local files. It reads projected Arrow batches, requires matching primitive schemas, retains Daft filters/limits, and preserves zero-column cardinality. It is outside the public reader API.
- An isolated Rust compatibility probe for file open, schema, streaming Arrow-to-Daft conversion, value/null roundtrips, zero-column cardinality, and partial-stream drop/reopen. Offline type checking, binary build, and runs over all six Vortex files pass.
- A baseline runner that creates a fresh query for each sample, captures explain plans, consumes the complete result, and checks correctness outside timing. Reports include versions, build profile, source hashes, fixture identity, and cache limitations.

Root `Cargo.toml`, `Cargo.lock`, `pyproject.toml`, and `uv.lock` are unchanged. Optional Python requirements, the isolated Cargo manifest, and its resolved lockfile live under `examples/vortex_eval/`.

## Environment and dependency decision

| Item | Recorded value / status |
| --- | --- |
| Host | macOS 26.1, arm64 |
| Python / PyArrow | 3.11.15 / 22.0.0 |
| Daft | Editable checkout, native `0.3.0-dev0`, **dev** build, native runner |
| Rust | `1.98.0-nightly (d1fc603d1 2026-05-26)`; repository toolchain `nightly-2026-05-27` |
| Python Vortex | `vortex-data==0.88.0`; installed and exercised |
| Rust Vortex | `vortex = =0.88.0`, defaults off; `files`, `tokio`, `zstd` |
| Rust Arrow | One `arrow-array`, `arrow-schema`, and `arrow-cast` identity each at `59.2.0`; `arrow-buffer` resolves to `59.3.1` |
| Tokio | Resolved `1.54.0`; the manifest's `1.48` is a compatible range |
| Dependency comparison | Standalone lockfile compared with the root lock; production integration delta is not yet measured |

Vortex 0.88.0 was selected from its [October 8 release](https://github.com/vortex-data/vortex/releases/tag/0.88.0). Its [workspace](https://github.com/vortex-data/vortex/blob/0.88.0/Cargo.toml) and [public crate manifest](https://github.com/vortex-data/vortex/blob/0.88.0/vortex/Cargo.toml) supplied the initial Arrow/toolchain/features. The installed Python pair and resolved native graph are now tested rather than inferred solely from those manifests.

Two API constraints affect the design:

1. Vortex's inferred string/binary Arrow schema uses view types that this Daft checkout does not directly support. Both probes request ordinary string/binary buffers. Python reader tests, both-format fixture readbacks, and native conversion roundtrips pass with this export; its allocation cost has not been measured. [Pinned Arrow export implementation](https://github.com/vortex-data/vortex/blob/0.88.0/vortex-layout/src/scan/arrow.rs).
2. Vortex 0.88.0 rejects a scan with both a filter and a limit. Preserve Daft's post-filter limit even for translated predicates. [Pinned scan builder](https://github.com/vortex-data/vortex/blob/0.88.0/vortex-layout/src/scan/scan_builder.rs).

## Executed checks

```bash
DAFT_RUNNER=native DAFT_PROGRESS_BAR=0 .venv/bin/pytest -n 0 -rs examples/vortex_eval
.venv/bin/ruff check examples/vortex_eval
.venv/bin/ruff format --check examples/vortex_eval
cargo fmt --manifest-path examples/vortex_eval/rust/Cargo.toml -- --check
cargo check --offline --locked --manifest-path examples/vortex_eval/rust/Cargo.toml
cargo test --offline --no-default-features -p common-macros --doc
cargo test --offline --no-default-features -p daft-dsl test_documented_custom_args
```

The focused Python suite returned **53 passed, zero skipped** after the optional dependencies were installed. This includes all eleven previously skipped real-format cases. Checks cover deterministic fixtures and oracles, Vortex/Parquet roundtrips, values/nulls and ordinary string/binary conversion, filter/projection/limit ordering, zero-column cardinality, strict schemas, unsupported nested types, corrupt input, scan-error cleanup, cancellation synchronization with a controlled iterator, manifest validation, numeric schema regressions, and baseline correctness. No Python runtime fixes were needed.

The imported doctest fix also received a focused check: the macro doctest completed with one intentionally ignored example, and all three downstream `test_documented_custom_args` regression cases passed. Existing no-default-feature unused-code/import warnings and a `proc-macro-error2` future-compatibility warning were emitted; there were no test failures.

## Native compatibility results

The probe built offline with its checked-in lockfile and the repository's pinned toolchain. The isolated target was `target/vortex-phase0`, with two build jobs and `CARGO_PROFILE_DEV_DEBUG=0`. This is a development build with debug information disabled, not a release build.

```bash
env CARGO_BUILD_JOBS=2 CARGO_PROFILE_DEV_DEBUG=0 \
  CARGO_TARGET_DIR="$PWD/target/vortex-phase0" \
  cargo build --offline --locked \
  --manifest-path examples/vortex_eval/rust/Cargo.toml

target/vortex-phase0/debug/daft-vortex-compatibility-probe \
  examples/vortex_eval/artifacts/phase0-vortex/single/data.vortex 7

env CARGO_BUILD_JOBS=2 CARGO_PROFILE_DEV_DEBUG=0 \
  CARGO_TARGET_DIR="$PWD/target/vortex-phase0" \
  cargo clippy --offline --locked \
  --manifest-path examples/vortex_eval/rust/Cargo.toml --no-deps -- -D warnings
```

Every Vortex file was run independently with a requested batch size of seven:

| Input | Rows | Full-scan batches | Zero-column batches |
| --- | --- | --- | --- |
| Single file | 257 | 37 | 37 |
| Multi-file part 0 | 0 | 0 | 0 |
| Multi-file part 1 | 1 | 1 | 1 |
| Multi-file part 2 | 7 | 1 | 1 |
| Multi-file part 3 | 249 | 36 | 36 |
| Empty file | 0 | 0 | 0 |

All batches contained at most seven rows. Full and zero-column row counts match the manifest; per-column null counts match the canonical Arrow fixture. Each converted batch roundtrips values/nulls through Daft. Nonempty files retain valid batch data after stream/file/session drop, and every run verifies a synthetic three-row zero-column conversion. Empty files correctly report that no real first-batch retention was exercised.

The isolated package also passed Clippy with `--no-deps -- -D warnings` and Rust formatting. These checks cover the probe rather than the full production workspace.

The macOS linker warned that `__eh_frame` exceeded 16 MB and unwinding could be slower. Linking and execution succeeded; this reinforces that the binary is a compatibility probe, not a performance build.

`native/runs.json` records the fixture-manifest, source, lockfile, and binary hashes along with each command/result; `native/build.json` records the actual build environment. Individual JSON results and diagnostic logs accompany them under `examples/vortex_eval/artifacts/phase0-vortex/`.

These checks establish the local conversion and ownership boundary for the generated `int64`, `uint64`, boolean, `float64`, UTF-8, and binary fixture columns, including nulls and numeric boundary/special values. They do not establish every primitive width/encoding, nested or temporal support, native scan-task integration, or production workspace feature compatibility.

## Reader baselines

The both-format fixtures used seed **42**, generator version **1**, and **257** rows. Six files per format cover single-file, multiple-file, and schema-bearing empty layouts. Parquet totaled **38,375 bytes** and Vortex **57,060 bytes** across those tiny fixtures; file/footer overhead makes this unsuitable for inferring compression ratios on larger data. The canonical logical checksum was:

```text
f1c12635b608835e664a1db7548743690125f849da5c481e3ba12deb31a1e554
```

All **nine cases passed for each reader**: native Daft/Parquet and the Python Vortex reference. Full scan, filter/projection, and integer sum/non-null count cover every layout. All nine output checksums match between the readers, and both reports identify the same fixture manifest. Each case ran one warmup plus five fresh-query samples; the reference requested seven rows per batch, while native Parquet used Daft defaults. The nonempty oracle has 232 non-null `value` entries and `SUM(value) = 1174`.

Local artifacts are ignored by Git:

- `examples/vortex_eval/artifacts/phase0-vortex/manifest.json`
- `examples/vortex_eval/artifacts/phase0-vortex/parquet-baseline.json`
- `examples/vortex_eval/artifacts/phase0-vortex/vortex-reference-baseline.json`
- Each baseline's matching `*-plans/` directory
- `examples/vortex_eval/artifacts/dependency-inventory.json`

These are small correctness measurements from an existing development build, with OS caches retained. No speedup, release-build throughput, memory bound, or Daft-versus-other-engine claim follows from them. PyArrow also reported denied CPU-cache/NEON system queries during generation; generation and readback succeeded, but this restricted host is unsuitable for performance conclusions.

## Dependency issue found and resolved

Initial PyPI/crates.io DNS failures were bypassed by installing/fetching from the user's terminal. The package imports and cached offline builds now work. The earlier missing-package errors and skipped tests are historical, not current coverage gaps.

Fresh standalone resolution selected `azure_storage_blob 1.1.0`, whose API no longer supplies `list_blobs_hierarchical` used by this Daft checkout. The probe's lockfile was aligned to the root lock's cached `1.1.0-beta.1`; Daft source and root dependency files were left unchanged. The compiler also corrected the probe's `SplitBy` import to `vortex::layout::scan::split_by::SplitBy`. Offline `cargo check --locked` then passed. Preserve the probe lockfile for repeatable builds.

The isolated graph re-resolves Daft's compatible dependency ranges independently, including TLS and cloud libraries already reached through Daft's I/O crates. Its changes must not be described as the number of packages Vortex would add to the existing production workspace. Phase 1 should preserve the root lock's existing resolutions wherever compatible and measure that actual integration separately.

The final probe lock contains **726 external package/version/source identities**, with **645 reachable on this host**. Compared with the root lock's 811 external identities, 410 probe identities are absent: 299 use existing package names at different versions/sources, and 111 span 108 new package names. The inventory records all differences, exact features, dependency paths, and both lockfile hashes.

There are no duplicate versions of any `arrow-*` crate in the probe. Vortex's activated public-crate features are exactly `files`, `tokio`, and `zstd`. Its cloud/object-store and DataFusion integrations are absent. The cloud SDK and Rustls/Reqwest dependencies in this graph come through Daft's existing crates, rather than the selected Vortex branch. No native-TLS or OpenSSL packages are resolved. These observations do not establish compliance for a future full-workspace Vortex feature; that build still needs its own dependency-policy checks.

## Acceptance gate

| Phase 0 requirement | Outcome |
| --- | --- |
| Isolated harness, deterministic primitives, query oracle | Passed |
| Parquet generation/readback and baseline | Passed |
| Optional dependency errors and focused offline tests | Passed |
| Real Vortex writer/readback and reference-reader correctness | Passed |
| Native dependency resolution and type checking | Passed |
| Native binary scan, Arrow conversion, and ownership smoke checks | Passed for all six fixtures |
| Resolved dependency/features inventory and lockfile | Passed for the standalone probe |
| Cancellation latency, bounded memory, Daft I/O metrics | Not established by these probes |

The Phase 0 gate is satisfied for the recorded versions and fixtures. Phase 1 can implement the native local connector using the existing source/task pattern and these fixtures as an oracle. Workspace-wide Rust/Python suites were not rerun for this isolated harness; native connector and production feature/dependency changes need their own broader checks.

Material AI assistance was used to write this fork experiment. The checks above state what was actually verified; small-fixture coverage is not a performance or general codec/type-compatibility claim.
