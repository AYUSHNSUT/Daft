# Phase 7 profiling results

This is an experiment in the personal fork, on `poc/vortex-phase7`. The
[design](vortex-phase7-design.md) defines the measurement and correctness
requirements. Phase 6's failed performance acceptance remains unchanged.

## First measurement and resulting changes

The initial release measurement used source `45d088144bffff9ab2570660aacd356d32f75ebf`
and native SHA256
`67cfb55da7f5ef8e3c1502c7e1b0a9e49ce4f7a4ff1295af968e3351ee81dd62`.
It ran the five query shapes across sorted/shuffled and single/eight-file local
layouts, with 20 balanced adjacent comparison rounds and one warmup. Each trial
constructed a fresh source and query in the same persistent worker. Source
creation was included. No builds or test suites ran alongside the measurement.

All 3,360 timed query outputs and 240 separate diagnostic query outputs passed.
Source, binary, fixture and configuration identities remained unchanged. The run
is nevertheless incomplete: 12 Parquet diagnostic captures reached the
50,000-event limit while recording individual decode/skip operations for
shuffled selective reads. Their dropped events are preserved in the report;
they cannot support complete timing breakdowns.

The initial candidate omitted unused planning-schema conversion. Its paired
median changes ranged from 2.19% faster to 1.83% slower, with no consistent
improvement across workloads. Identical-configuration control median ratios
ranged from 0.992 to 1.020. These observations do not establish a reliable gain
from schema conversion alone.

The complete Vortex profiles identify more substantial repeated setup work.
Across the five sorted/eight-file cases, medians of three diagnostic captures
showed:

| Planning operation | Observed wall time |
| --- | ---: |
| Eight session initializations | 1.300–1.325 ms |
| Eight schema conversions | 0.033–0.037 ms |
| Complete planning metadata envelope | 2.096–2.134 ms |

The first two rows are nested within the third. They must not be added to it.
These are instrumented wall durations, not exclusive CPU time.

The resulting candidate therefore combines row-count planning with one session
initialization per bulk metadata call. Metadata, readers and object validators
remain fresh. Execution still uses its own sessions and validates full schemas.
The option remains disabled by default. The comparison measures the combined
candidate; it does not isolate the causal contribution of each change.

Parquet's tiny decode/skip operations are accumulated as bounded call-count and
synchronous-duration counters within each decoder envelope. These totals have
no invented timeline positions or interval unions. Batch consumption and
backpressure retain their separate spans.

Raw initial evidence is retained under
`examples/vortex_eval/artifacts/phase7/local-profile-initial/`, including
`report.json`, `analysis.md`, `overview.html` and individual profile timelines.

## Revised local measurement

The revised run used source `345a7d460e0fbd837b5b870f102d9c197263694c` and native
SHA256 `abdda9c07984620a2f0bc3cf0501a7c6d972889ab29e27e5352294c6d3af041f`.
It passed all 3,360 query outputs, all 240 complete diagnostic captures and
provenance checks. There were no dropped or unfinished profiling events. The
schedule, fixtures and measurement protocol stayed the same; this is a new
same-build comparison, not a before/after comparison of different runs.
The largest capture contained 7,285 events. The busiest selective Parquet capture
retained 316,184 decode/skip calls in 384 decoder envelopes and 3,573 total
events, without exceeding the bounded collector.

The clearest improvements are in eight-file queries:

| Layout / query | Baseline median ms | Candidate median ms | Median paired change | Candidate faster pairs |
| --- | ---: | ---: | ---: | ---: |
| Sorted / narrow scan | 15.505 | 14.242 | -7.98% | 20/20 |
| Sorted / 1% range filter | 6.474 | 5.008 | -22.35% | 20/20 |
| Sorted / 1% equality filter | 6.336 | 5.007 | -21.79% | 20/20 |
| Sorted / join | 60.568 | 59.620 | -2.15% | 15/20 |
| Sorted / full reduction | 157.570 | 158.202 | +0.60% | 9/20 |
| Shuffled / narrow scan | 18.776 | 17.445 | -7.18% | 19/20 |
| Shuffled / 1% range filter | 13.242 | 11.809 | -11.32% | 20/20 |
| Shuffled / 1% equality filter | 13.786 | 12.550 | -10.28% | 19/20 |

The paired change is the median of the 20 individual B/A ratios, not the ratio
of the two displayed medians. A/A paired median ratios across all 20 workloads
ranged from 0.982 to 1.007. Small changes in joins, single-file queries and full
reductions do not establish a consistent improvement. The largest candidate
regression by ratio of medians was 1.31%; this exploratory result does not
replace the separate final performance gate.

The candidate beat Parquet's median in 18 of 20 local workloads. Parquet still
won sorted/eight-file range filtering (3.900 ms versus Vortex 5.246 ms) and
equality filtering (3.149 ms versus Vortex 5.093 ms). These use the dedicated
Parquet/candidate comparison samples, so their Vortex medians differ from the
baseline/candidate samples in the table above.

## Where the time goes

For sorted/eight-file range filtering, the medians of three separate captures
show the intended reduction in repeated planning work:

| Instrumented boundary | Baseline ms | Candidate ms |
| --- | ---: | ---: |
| Whole query | 6.673 | 5.291 |
| Source creation, including discovery and schema inference | 1.390 | 1.195 |
| Logical planning, including metadata materialization | 2.694 | 1.349 |
| Planning metadata within logical planning | 2.083 | 0.796 |
| Planning session initialization within metadata work | 1.288 | 0.177 |
| Physical planning | 0.015 | 0.015 |
| Execution event-loop wait | 2.340 | 2.402 |
| Execution session initialization within execution | 0.958 | 0.976 |
| Batch polling/decode, including I/O and scheduling wait | 0.813 | 0.870 |
| Arrow conversion | 0.002 | 0.002 |
| Time outside internal instrumented boundaries | 0.410 | 0.418 |

Rows overlap and medians are not additive. Instrumentation overhead is included
here; these values are not substituted for the uninstrumented performance
medians. The source and execution-session differences are observations, not
changes the candidate intentionally makes. The capture distinguishes eight
baseline planning session creations from one candidate creation.

The candidate's sorted/eight-file full reduction has a different profile:
159.232 ms total, 0.806 ms planning metadata, 149.839 ms batch-poll occupancy,
45.746 ms Arrow-conversion occupancy and 127.113 ms engine-task occupancy.
These intervals overlap substantially. They identify execution and conversion
as follow-up targets without attributing all elapsed time to decoder CPU.

Detailed timing tables, control comparisons, raw events and per-query timelines
are in `examples/vortex_eval/artifacts/phase7/local-profile-revised/`:
`overview.html`, `analysis.md`, `report.json` and `profiles/`.

## Remote measurement

The terminal run on 2026-10-21 used source
`52a954a7862c060a11686958806f129b079c3dfe` and the same release native binary as
the revised local run. It used the eight-file sorted remote fixture through an
owned authenticated MinIO container on localhost. This measures the local
Docker/S3 path, not WAN or hosted object-storage performance.

All 1,470 query executions passed (1,400 measured and 70 warmups), along with
all 105 complete diagnostic captures and 35 paired comparisons. Source, binary,
fixture and configuration identities remained unchanged. Every comparison has
20 adjacent pairs, exactly ten in each order. There were no dropped, unfinished,
failed or unexpectedly cancelled diagnostic events.

The dedicated candidate/Parquet comparisons yielded:

| Query | Parquet median ms | Vortex candidate median ms | Vortex / Parquet |
| --- | ---: | ---: | ---: |
| Narrow scan | 26.845 | 19.975 | 0.744 |
| 1% range filter | 11.145 | 12.286 | 1.102 |
| 1% equality filter | 11.091 | 13.128 | 1.184 |
| Join | 33.073 | 31.432 | 0.950 |
| Full reduction | 164.733 | 170.358 | 1.034 |

Vortex wins two of five remote medians. The desired all-five remote win has
not been achieved. The helper's passing status verifies correctness, profile
completeness, provenance and tests; it does not grant performance acceptance.

Lightweight planning versus its same-build Vortex baseline shows median changes
of -3.58%, +5.70%, -14.24%, -0.42% and -1.29% in the same query order. Equality's
identical-configuration control itself appears 11.18% faster by ratio of medians
(11.38% by median paired ratio). The apparent equality improvement therefore
does not establish a clean causal gain. Preserve all samples and do not subtract
the control percentage as an assumed correction.

Reducing the existing per-file request cap does not establish a uniformly
better setting:

| Query | Cap 1 / cap 8 | Cap 2 / cap 8 | Cap 4 / cap 8 |
| --- | ---: | ---: | ---: |
| Narrow scan | 1.015 | 1.015 | 0.980 |
| 1% range filter | 1.064 | 0.979 | 1.018 |
| 1% equality filter | 1.058 | 0.988 | 0.939 |
| Join | 1.031 | 0.947 | 0.970 |
| Full reduction | 1.047 | 0.996 | 1.011 |

Ratios use medians from each comparison's own adjacent samples; below one favors
the reduced cap. All cap experiments leave lightweight planning disabled. Cap 1
loses every case; caps 2 and 4 trade small gains against regressions. No default
is changed, and a combined lightweight-planning/reduced-cap candidate has not
been tested.

Diagnostic captures consistently show lower total session-initialization wall
occupancy, from 2.418–2.849 ms under baseline to 1.267–1.462 ms with lightweight
planning across the five cases. Candidate planning metadata still occupies
3.839–5.878 ms. In full reduction, the 174.506 ms median diagnostic query includes
158.117 ms stream/batch occupancy and 146.773 ms I/O-body occupancy. These
overlap and include scheduling and consumer delay; they do not isolate physical
network time or decoder CPU.

Remote evidence is saved under
`examples/vortex_eval/artifacts/phase7/terminal-20261021T120240Z-87606/remote/`,
including `overview.html`, `analysis.md`, `report.json` and individual profiles.
The raw report SHA256 is
`dbda1fac19d400bcc25f7f1c18327b707baa2ffdf66369a8d4fe905cc25a38e6`.

## Validation

The rebuilt release extension passed 720 local Python query tests, 334 harness
tests plus nine subtests, and 174 targeted Rust tests. Five Python cases were
deselected by repository configuration and one existing Rust doctest was ignored.
Full-workspace/all-features/all-targets Clippy with warnings denied, formatting
and changed-file pre-commit checks passed. The decoder regression verifies
50,002 decode/skip calls, correct output and zero lost events.

The frozen terminal run additionally passed:

- HTTP Range, authenticated MinIO and Ray: 37 tests, zero skips.
- ORC/MCAP compatibility: 112 passed; one optional MCAP S3-environment skip.
- S3 connection, timeout and validator handling: seven tests.
- Full Rust workspace with no default features: 2,317 passed, 20 existing ignored
  tests/doctests, zero failures across 157 reported targets.

`terminal.json` records commands, counts, source/native identities and log hashes.
All saved log hashes were verified during review. The terminal run completed the
checks blocked by this agent sandbox's localhost, Docker, sysctl and network
restrictions. Earlier environment-blocked logs remain preserved. The final
combined performance acceptance remains unevaluated; the measured remote
candidate already misses the all-five-wins requirement.

## Next targets supported by these profiles

1. Test direct `LargeUtf8`/`LargeBinary` output from Vortex. The current
   [Arrow schema request](../../src/daft-vortex/src/read.rs) uses `Utf8`/`Binary`,
   which Daft subsequently widens to its large-offset representation. Arrow
   shares payload/null buffers but allocates and widens offsets. Compare total
   query time and both SDK/export and Daft-conversion time: moving the work
   between layers alone is not an improvement. Validate Unicode, binary, nulls,
   empty values, projection, filters and limits.
2. Evaluate execution-session reuse within each fresh scan processor. This needs
   execution-context plumbing; serialize configuration only and retain fresh
   per-file readers, metadata and validators. The current optimization is
   deliberately limited to planning batches.
3. Profile common-engine reduction kernels independently. This benchmark's
   string-length expression counts Unicode characters; substituting byte length
   would change the query and invalidate the comparison.

The profile exposes API, planning, reader, execution and I/O boundaries, plus
uncovered time and accumulated decoder work. It does not independently measure
DNS/TCP/TLS, physical SDK retries, wire bytes, server/kernel work or exclusive
codec CPU. No universal performance advantage or final acceptance is claimed.
