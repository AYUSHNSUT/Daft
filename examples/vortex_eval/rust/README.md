# Native Vortex compatibility probe

This isolated Cargo workspace checks Vortex 0.88.0's local file reader against the
checked-out Daft `RecordBatch` implementation. It does not add a Daft connector or
change the root Cargo workspace. Its own `Cargo.lock` records the experiment's
resolved dependencies. Root build/test results do not validate this binary.

The lockfile retains Daft's `azure_storage_blob` 1.1.0-beta.1 and
`azure_storage_common` 0.1.0 versions. Fresh resolution otherwise selects Blob
1.1.0, whose removed hierarchical-list API breaks this Daft checkout before the
probe compiles. Keep `--locked`; this compatibility pin changes only the probe's
dependency graph.

From the repository root, after creating the fixtures:

```bash
env CARGO_BUILD_JOBS=2 CARGO_PROFILE_DEV_DEBUG=0 \
  CARGO_TARGET_DIR="$PWD/target/vortex-phase0" \
  cargo run --offline --locked \
  --manifest-path examples/vortex_eval/rust/Cargo.toml -- \
  /absolute/path/to/fixture.vortex 7
```

`--offline` requires dependencies to have been fetched already. The profile
override disables debug information to keep this isolated development build
smaller; it does not enable release optimizations.

The binary emits JSON only after its checks succeed. It opens a file, inspects its
schema and row count, reads one batch and drops the stream/file/session, verifies
the retained batch, reopens and scans all batches, and scans a zero-column
projection. Each Arrow-to-Daft conversion checks cardinality and roundtrips
values/nulls; variable-width offsets are normalized when comparing values.
Arrow 59.2's array-data equality compares valid floating-point bit patterns,
including NaN payloads and signed zero, while ignoring storage beneath nulls.

Vortex's inferred `Utf8View`/`BinaryView` fields are exported as `Utf8`/`Binary`
because this Daft checkout's native schema conversion rejects view types. The
summary reports every normalization. Only flat primitive fixtures are accepted;
this is not a general nested-type or extension-type compatibility claim.

The public `vortex` crate uses only `files`, `tokio`, and `zstd` features. Its
standard codecs are included; cloud integrations, Parquet Variant, and mmap are
not requested. A shared two-worker Tokio runtime drives execution. The Vortex
scan-concurrency setting is **one per Vortex worker**, not one global split:
Vortex multiplies it by its available-parallelism count, which can differ from
the Tokio worker count. The JSON names this setting explicitly and does not
claim a global read-concurrency bound. `SplitBy::RowCount` controls rows per
output batch, not a byte-memory bound.

Early drop/reopen is an ownership smoke check, **not** proof of cancellation
latency or absence of background I/O. Empty fixtures cannot exercise retention of
a real first batch; their summary says so. A synthetic three-row zero-column
batch always checks Daft's explicit-cardinality constructor. The probe uses
Vortex's local opener and has no Daft I/O metrics. Conversion checking and extra
scans make its execution time unsuitable as a throughput benchmark.

The native fixture run on 2026-10-20 (Asia/Kolkata) passed all six files with a
seven-row split size. Both full scans and zero-column scans returned the same
counts:

| Fixture | Rows | Batches | Largest batch |
| --- | ---: | ---: | ---: |
| `single/data.vortex` | 257 | 37 | 7 |
| `multi/part-000.vortex` | 0 | 0 | 0 |
| `multi/part-001.vortex` | 1 | 1 | 1 |
| `multi/part-002.vortex` | 7 | 1 | 7 |
| `multi/part-003.vortex` | 249 | 36 | 7 |
| `empty/data.vortex` | 0 | 0 | 0 |

These are six independent file reads, not a test of Daft's multi-file planning.
All cases preserved the decoded Arrow values and nulls through Daft conversion,
and the four nonempty files retained a readable first batch after dropping its
reader/session. The single file and 249-row part stopped before consuming all
rows during that ownership check. Each case also preserved three rows through
the synthetic zero-column constructor. Both view-type normalizations occurred
as expected. Results and exact build/run commands are recorded under
`../artifacts/phase0-vortex/native/`, including `runs.json` and `build.json`.

The offline, locked development build succeeded with debug information disabled.
The isolated package also passed `cargo check --offline --locked`,
`cargo clippy --offline --locked --no-deps -- -D warnings`, and `rustfmt --check`.
Use this directory's manifest for Cargo commands; the captured Clippy invocation
and output are in `clippy.json` and `clippy.log` beside the run results.
The macOS linker warned that its `__eh_frame` section exceeded the compact
unwind table's 16 MB limit; the binary linked and all fixture runs passed. The
full warning and existing dependency warnings are retained in `build.log`.

The implementation follows the pinned release APIs in
[Vortex's public crate](https://github.com/vortex-data/vortex/blob/0.88.0/vortex/src/lib.rs),
[scan Arrow adapter](https://github.com/vortex-data/vortex/blob/0.88.0/vortex-layout/src/scan/arrow.rs),
[scan builder](https://github.com/vortex-data/vortex/blob/0.88.0/vortex-layout/src/scan/scan_builder.rs),
and [runtime session](https://github.com/vortex-data/vortex/blob/0.88.0/vortex-io/src/session.rs).
