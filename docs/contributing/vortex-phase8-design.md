# Reader experiments and candidate acceptance

Branch: `poc/vortex-phase8`, based on `cb351ae6a` (`poc/vortex-phase7`). This
iteration executes M1/M2 and the first contained candidates D1/S1/S4/D3 from
the [performance worklist](vortex-performance-worklist.md). M3 gains bounded
SDK task envelopes; exclusive CPU/allocation and physical transport attribution
remain investigations with explicit unavailable coverage.

## Reader candidates

All four options default to false. The same build can compare each change with
the previous Vortex configuration and with Parquet.

| Control | Behavior | Required invariants |
| --- | --- | --- |
| `enable_large_offsets` | Requests large-offset Arrow string/binary arrays directly | Identical logical schema and values; full schema validation and nullability markers; measure SDK/export plus conversion, not just the moved cast |
| `enable_execution_session_reuse` | Lazily shares immutable Vortex session initialization within a fresh native scan processor | Fresh per-file metadata/readers/validators; independent query contexts; no live session in task serialization; release with processor/streams |
| `enable_file_pruning` | Uses safely translated footer-stat predicates after complete schema validation, before scan construction | Unknown/unusable statistics fall back; complete residual retained; no skipped schema or identity checks; prune only when no match is proven |
| `enable_layout_splitting` | Uses layout sub-splitting with the same maximum rows per batch | Ordered exhaustive row coverage; bounded rows; identical filtering/limits; request/byte budgets unchanged |

The baseline explicitly enables the previously evaluated metadata reuse and
lightweight planning, disables coalescing, and retains eight-request/64 MiB
per-reader budgets and eight-file metadata/discovery concurrency. The combined
candidate enables the four new controls; its usefulness must be measured rather
than assumed. Each individual candidate changes exactly one new control.

## Diagnostics

Only diagnostic sessions install a stateless wrapper around the SDK's existing
Tokio executor. It records task queue lifetime and async task poll/suspension
envelopes, plus synchronous CPU-closure/blocking-I/O wall envelopes. It delegates
spawn and abort behavior to the same Tokio executor. Uninstrumented sessions use
the existing runtime path. Dropped work and panics retain cancellation markers;
the SDK's own panic propagation remains intact.

The wrapper does not label all CPU closures as decoding: preparation and other
SDK work may also be present. Async polling is wall time, not thread CPU.
Task intervals can overlap conversions, engine work and I/O. Diagnostic overhead
does not enter performance medians. CPU stack sampling, allocator live bytes,
DNS/TCP/TLS and server/kernel work require separate tools/evidence and must not
be inferred from suspension or response-body occupancy.

## Experiments and frozen acceptance

`benchmarking.vortex.phase8` reuses the adjacent balanced-pair runner and full
query/output checks. It runs independent persistent worker sessions with
predeclared seeds. Sources/queries remain fresh and source creation remains
timed. Each session records all options and their hashes, A/A control, individual
experiments and optional combined/Parquet comparisons. Order and ten-round time
blocks are descriptive diagnostics; they do not correct candidate timings.
Failed sessions remain preserved; later predeclared sessions are independent.

A version-2 selection records the complete candidate options, exact source and
native identities, remote fixture identity and the hash of the successful remote
diagnosis that exercised it. Missing/unknown options, type mismatches and
coalescing-policy inconsistencies fail. The older policy-only acceptance helper
and its historical reports retain their behavior; the new final/guard route uses
the complete selection and explicit persistent execution.

Final acceptance requires exactly 50 measured rounds and three warmups across
all five original remote queries. Every candidate median must be strictly below
Parquet. The local guard covers all four layouts and requires each candidate
median at most 1.05 times the declared previous Vortex baseline. Correct outputs,
complete profiles and unchanged provenance are required independently of the
performance gates. No defaults change based on diagnostic results.

## Validation and execution

1. Native/Python coverage across the four controls, primitive types and Unicode/
   binary/null/empty values; projected/filtered/limited output; zero matches;
   full-schema mismatch in a pruned/projected-away file; layout batch bounds.
2. Session creation counts across multiple files and repeated materializations;
   replacement of file contents; configuration serialization and independent
   processor lifetime; SDK task capture completeness and cancellation/error paths.
3. Harness tests for configuration freezing, source/native/fixture mismatch,
   failed/missing pairs, independent seeds, exact executed options and unchanged
   acceptance thresholds. Run the existing Vortex/Parquet/task regression suites.
4. Build the release extension, finish local checks, commit the tested source,
   then run uninstrumented comparisons without concurrent builds/test suites.
   Preserve all raw failures and profiles.
5. Complete authenticated HTTP/MinIO/Ray and compatibility/workspace checks,
   updating [the single comparison](parquet-vs-vortex.md) with measured outcomes
   and limitations. Push and open the fork-only PR into the previous branch after
   required validation. Environment-dependent checks are pending until executed.

Example local diagnosis (new output directory each time):

```bash
.venv/bin/python -m benchmarking.vortex.phase8 \
  --manifest examples/vortex_eval/artifacts/phase4/fixtures-1g/manifest.json \
  --output examples/vortex_eval/artifacts/phase8/local \
  --datasets sorted/single sorted/multi shuffled/single shuffled/multi \
  --sessions 3
```

Remote diagnosis uses the existing pinned disposable MinIO image:

```bash
.venv/bin/python -m examples.vortex_eval.run_phase8_services \
  --manifest examples/vortex_eval/artifacts/phase4/remote-fixtures/manifest.json \
  --output examples/vortex_eval/artifacts/phase8/remote \
  --docker-image draft-vortex-minio:phase3 --sessions 3
```

After reviewing the independent sessions, freeze one actually measured candidate
by name; this command does not choose the fastest option automatically:

```bash
.venv/bin/python -m benchmarking.vortex.phase8 --stage freeze \
  --report examples/vortex_eval/artifacts/phase8/remote/session-01/report.json \
  --config combined --output examples/vortex_eval/artifacts/phase8/candidate.json

.venv/bin/python -m examples.vortex_eval.run_phase8_services --stage final \
  --selection examples/vortex_eval/artifacts/phase8/candidate.json \
  --manifest examples/vortex_eval/artifacts/phase4/remote-fixtures/manifest.json \
  --output examples/vortex_eval/artifacts/phase8/final \
  --docker-image draft-vortex-minio:phase3 --sessions 1

.venv/bin/python -m benchmarking.vortex.phase8 --stage guard \
  --selection examples/vortex_eval/artifacts/phase8/candidate.json \
  --manifest examples/vortex_eval/artifacts/phase4/fixtures-1g/manifest.json \
  --output examples/vortex_eval/artifacts/phase8/guard \
  --datasets sorted/single sorted/multi shuffled/single shuffled/multi --sessions 1
```

All release invocations require `DAFT_VORTEX_BENCH_BUILD_COMMAND` set to the
actual successful build command. `--allow-dev` is correctness smoke and cannot
produce a candidate eligible for release acceptance. A changed source/native
identity requires new diagnosis and a new frozen selection.
