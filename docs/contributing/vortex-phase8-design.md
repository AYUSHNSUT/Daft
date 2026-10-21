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
the existing runtime path. Executor envelopes record completion or cancellation.
The SDK retains its existing error/panic propagation; failures handled inside
SDK tasks may only be visible at reader or query boundaries.

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

## Completed diagnosis and candidate decision

Three independent local sessions and three independent MinIO sessions passed
full query/output, provenance and profile-completeness checks. They contain
25,200 query executions, including warmups, and 1,800 separate diagnostic
captures with zero dropped/unfinished events. One additional local session
overlapped disk cleanup and is preserved separately; its declared replacement
uses the same seed. The [maintained comparison](parquet-vs-vortex.md) records all
Parquet/Vortex workload rows and report identities without pooling sessions.

The table below compares each individual option with the previous Vortex
baseline. These are changes in query latency, not Parquet/Vortex speed ratios.
Ranges span independent session medians, not confidence intervals.

| Control | Local result | MinIO result | Decision |
| --- | --- | --- | --- |
| Large offsets | Full reductions take 1.0–3.7% less time; most other differences are small or mixed | No repeatable full-reduction improvement | Keep opt-in; no promotion based on local results alone |
| Session reuse | Sorted/multi range and equality take 9.9–11.0% and 10.9–13.4% less time; shuffled/multi filters also improve | Narrow scans take 1.1–6.6% less time and full reductions 0.5–3.4% less; joins take 3.1–5.3% more time; filters vary | Validate this option alone in independent acceptance |
| Whole-file pruning | No repeatable latency benefit across the matrix | Differences vary by session | Keep opt-in; investigate applicable footer-stat coverage |
| Layout sub-splitting | Full reductions take 11.2–24.3% more time across all layouts | No consistent improvement across all cases | Exclude from the current acceptance candidate |
| All four controls | Full reductions take 8.0–19.1% more time | Only narrow scans and joins beat Parquet in every session | Exclude the bundle from the current acceptance candidate |

The frozen candidate will enable session reuse only, retaining the baseline
settings for all other controls. The first remote session supplies evidence by
position, not by best timing. Independent acceptance still requires all five
remote medians below Parquet and every local median within 5% of the baseline.
The diagnosis does not establish that these gates will pass.

The CLI case-selection fix and a sorted-JSON schedule-validation fix change the
Python harness only; the native binary is unchanged. The validator reconstructs
comparison order from the serialized summary list because JSON object key order
is sorted when saving reports. Raw schedules are then checked exactly against
that order and the recorded seed. Regression tests cover the real serialization
format and duplicate comparison entries. A fresh qualifying diagnosis on the
final harness revision will precede candidate freezing and final acceptance.

### What the profiles explain

These examples use the first local session's three separate diagnostic captures
for sorted/multi data. Values are medians of observed wall unions, not exclusive
CPU or values to add together. They never enter performance medians.

- Session reuse changes execution-time session creation from eight calls to one.
  In the range filter, its observed wall union falls from 1.020 ms to 0.186 ms.
  Schema inference and planning still create their own sessions; those costs
  are deliberately outside the execution-only lifetime being shared.
- Large offsets move the full-reduction Arrow-conversion wall union from
  47.221 ms to 0.568 ms. The complete instrumented query moves from 167.615 ms to
  163.517 ms. The much smaller end-to-end gain shows why the removed conversion
  envelope must not be claimed as independently removable query time; export,
  overlapping decode work and downstream processing still need stack evidence.
- Layout splitting increases full-reduction conversion calls from 64 to 488 and
  SDK task-queue events from 89 to 513. Their wall unions rise from 47.221 ms to
  70.071 ms and 1.732 ms to 76.318 ms, respectively. These overlapping intervals
  support investigating batch fragmentation and scheduling overhead; they do
  not establish an additive or exclusive CPU breakdown.

All-thread stack sampling remains a separate terminal-side diagnostic. Allocator
live bytes, exclusive decoder CPU and physical transport/server attribution are
still unavailable; task spans and SDK send counts do not fill those gaps.

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
  --config sessions --output examples/vortex_eval/artifacts/phase8/candidate.json

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
