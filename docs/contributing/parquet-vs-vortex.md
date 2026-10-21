# Parquet vs Vortex in Daft

Last updated: **21 October 2026**. This is the maintained comparison of the two
native readers in the personal Daft fork. Results are organized by workload,
storage and execution settings. All ratios compare Parquet with Vortex in the
same measured build.

The [performance worklist](vortex-performance-worklist.md) tracks the next
investigations and proposed improvements separately from measured results.

**Speed ratio = Parquet median query time / Vortex median query time.** Above
1 favors Vortex; below 1 favors Parquet. For example, 2.0× means Vortex takes
half the time. Ratios are calculated before rounding the displayed times.

## Current measured results

- **Local files, with execution-session reuse:** Vortex wins **18 of 20
  medians** in the independent confirmation. Narrow scans are **1.99–2.99×**,
  joins **1.28–1.65×**, and full reductions **1.30–1.37×** as fast as Parquet.
  Shuffled 1% filters reach **2.83–8.29×**. Sorted eight-file filters still lose.
- **S3-compatible storage on local MinIO, with the same option:** Vortex wins
  **two of five medians**: **1.397×** for narrow scans and **1.145×** for joins.
  Range, equality and full reductions take **4.63%, 2.64% and 2.27% more time**
  than Parquet. The target of winning every remote workload **failed**.
- **File size:** Vortex files remain **1.27–3.43% larger** on these fixtures.
- **Earlier group-by coverage:** Vortex was **1.48–2.08×** as fast locally.
  Group-by has not been rerun with the current configuration.

The baseline explicitly enables projection/filter pushdown, validated metadata
reuse and lightweight row-count planning, and disables read coalescing. Metadata
reuse and lightweight planning are opt-in, not public API defaults. The four
additional reader controls described below are disabled for the baseline.
The confirmed candidate additionally enables execution-session reuse alone.
Parquet uses its native defaults. All four new controls remain opt-in; no
performance default has been promoted.

### Independent confirmation with execution-session reuse

The configuration was selected before these runs and frozen with its complete
options and source/native identities. Each query creates a fresh source and
includes source creation in the timer; clients and connections persist. These
are dedicated Parquet/candidate pairs, not ratios inferred from an optimization
against the previous Vortex reader. Earlier independent baseline sessions follow
separately and are not pooled with this confirmation.

Local confirmation uses the same 7,895,160-row corpus per layout, 20 measured
pairs (ten AB, ten BA) and one warmup. Evidence: L4.

| Layout | Query | Parquet median ms | Vortex median ms | P/V |
| --- | --- | ---: | ---: | ---: |
| Sorted / single | Narrow scan | 33.300 | 11.138 | 2.990× |
| Sorted / single | Range 1% | 4.255 | 3.445 | 1.235× |
| Sorted / single | Equality 1% | 3.926 | 3.469 | 1.132× |
| Sorted / single | Join | 97.172 | 59.014 | 1.647× |
| Sorted / single | Full reduction | 229.930 | 169.317 | 1.358× |
| Sorted / multi | Narrow scan | 32.732 | 13.654 | 2.397× |
| Sorted / multi | Range 1% | 3.565 | 4.155 | 0.858× |
| Sorted / multi | Equality 1% | 3.015 | 4.165 | 0.724× |
| Sorted / multi | Join | 81.123 | 62.152 | 1.305× |
| Sorted / multi | Full reduction | 229.936 | 168.240 | 1.367× |
| Shuffled / single | Narrow scan | 33.641 | 13.981 | 2.406× |
| Shuffled / single | Range 1% | 95.696 | 11.540 | 8.292× |
| Shuffled / single | Equality 1% | 50.632 | 12.803 | 3.955× |
| Shuffled / single | Join | 100.560 | 62.513 | 1.609× |
| Shuffled / single | Full reduction | 232.490 | 178.607 | 1.302× |
| Shuffled / multi | Narrow scan | 33.426 | 16.796 | 1.990× |
| Shuffled / multi | Range 1% | 30.880 | 10.905 | 2.832× |
| Shuffled / multi | Equality 1% | 31.996 | 11.296 | 2.832× |
| Shuffled / multi | Join | 82.952 | 64.878 | 1.279× |
| Shuffled / multi | Full reduction | 232.017 | 172.152 | 1.348× |

Sorted/multi range and equality remain **16.57% and 38.13% slower** than Parquet.
The separate candidate/previous-Vortex pairs pass all 20 local regression
checks: median changes range from 13.69% less time to 1.56% more time, within
the predeclared 5% limit. Those percentages compare Vortex configurations;
they are not Parquet/Vortex speed ratios.

Remote confirmation uses 986,895 sorted rows in eight files on authenticated
local Docker MinIO, 50 measured pairs (25 AB, 25 BA) and three warmups.
Evidence: R4. The local and remote corpora differ in size; this is not a
measurement of WAN or hosted AWS S3 performance.

| Query | Parquet median ms | Vortex median ms | P/V |
| --- | ---: | ---: | ---: |
| Narrow scan | 27.158 | 19.434 | 1.397× |
| Range 1% | 11.962 | 12.516 | 0.956× |
| Equality 1% | 11.795 | 12.107 | 0.974× |
| Join | 32.953 | 28.775 | 1.145× |
| Full reduction | 164.291 | 168.014 | 0.978× |

All query outputs and profiles passed validation. The report retains a failed
performance status because three of five medians miss the strict target. Small
differences remain descriptive: the preceding qualification's identical-control
medians varied from about 5.3% less time to 1.6% more time. No control correction,
statistical-significance claim, candidate replacement or timing retry was used.

### Repeated baseline comparisons

With execution-session reuse disabled, the baseline wins 18/20 local and 2/5
remote medians in each of three independent sessions. The following tables retain
all those results and their session variability.

#### Local files

Each layout contains **7,895,160 rows**, approximately **1.066 GB of logical
Arrow data**. Single means one file; multi means eight files. Each independent
session uses 20 measured adjacent pairs and one excluded warmup, with persistent
clients and a fresh source/query each time. Source creation is included.

Ranges span the **three session medians**, not individual samples or confidence
intervals. S1/S2/S3 retain each session's own P/V ratio; samples and range endpoints
are not pooled to calculate a ratio. Evidence: L1–L3.

| Layout | Query | Parquet median ms range | Vortex median ms range | P/V: S1 / S2 / S3 |
| --- | --- | ---: | ---: | --- |
| Sorted / single | Narrow scan | 32.609–35.123 | 11.799–12.426 | 2.881× / 2.750× / 2.764× |
| Sorted / single | Range 1% | 4.421–4.605 | 3.635–3.767 | 1.222× / 1.228× / 1.216× |
| Sorted / single | Equality 1% | 4.026–4.076 | 3.637–3.737 | 1.089× / 1.077× / 1.121× |
| Sorted / single | Join | 97.033–101.476 | 57.953–61.490 | 1.650× / 1.642× / 1.674× |
| Sorted / single | Full reduction | 241.298–269.165 | 172.259–178.761 | 1.515× / 1.388× / 1.401× |
| Sorted / multi | Narrow scan | 32.471–34.609 | 14.365–15.323 | 2.259× / 2.310× / 2.260× |
| Sorted / multi | Range 1% | 3.809–3.873 | 5.058–5.217 | 0.743× / 0.755× / 0.753× |
| Sorted / multi | Equality 1% | 3.115–3.177 | 5.041–5.054 | 0.616× / 0.629× / 0.619× |
| Sorted / multi | Join | 80.967–85.292 | 61.835–64.983 | 1.313× / 1.307× / 1.309× |
| Sorted / multi | Full reduction | 233.455–249.936 | 169.597–178.379 | 1.401× / 1.380× / 1.377× |
| Shuffled / single | Narrow scan | 33.176–35.470 | 14.736–15.186 | 2.336× / 2.285× / 2.251× |
| Shuffled / single | Range 1% | 95.675–97.156 | 12.106–12.571 | 7.985× / 7.650× / 7.903× |
| Shuffled / single | Equality 1% | 50.722–52.144 | 13.634–14.070 | 3.706× / 3.798× / 3.720× |
| Shuffled / single | Join | 101.289–106.899 | 59.941–66.584 | 1.605× / 1.602× / 1.690× |
| Shuffled / single | Full reduction | 243.338–264.757 | 176.078–189.752 | 1.395× / 1.400× / 1.382× |
| Shuffled / multi | Narrow scan | 32.398–35.134 | 17.221–18.204 | 1.933× / 1.898× / 1.881× |
| Shuffled / multi | Range 1% | 30.349–31.774 | 11.292–12.010 | 2.657× / 2.601× / 2.688× |
| Shuffled / multi | Equality 1% | 31.796–33.364 | 12.235–12.817 | 2.603× / 2.609× / 2.599× |
| Shuffled / multi | Join | 80.993–85.545 | 64.731–69.127 | 1.238× / 1.243× / 1.251× |
| Shuffled / multi | Full reduction | 237.628–254.119 | 173.135–184.187 | 1.380× / 1.351× / 1.373× |

The sorted/multi range and equality cases take **32.4–34.7%** and **59.0–62.2%**
more time through Vortex across these sessions. These losses remain visible
alongside the winning rows.

One original local session overlapped user-requested disk cleanup. Its complete
report is preserved and excluded from performance conclusions. A replacement
using the same seed was declared before that session completed; L3 is the clean
replacement. The [replacement record](../../examples/vortex_eval/artifacts/phase8/local-replacement-plan.json)
links the original data and cleanup evidence.

#### S3-compatible storage

The remote fixture has **986,895 sorted rows across eight files**, approximately
**133.257 MB logical data**. An authenticated MinIO container runs on the same
Mac through Docker. This measures an S3 client/server path, not WAN or hosted AWS
S3 performance. The smaller corpus prevents interpreting this table as a
remote-versus-local slowdown. Ranges and S1/S2/S3 have the same meaning as above.
Evidence: R1–R3.

| Query | Parquet median ms range | Vortex median ms range | P/V: S1 / S2 / S3 |
| --- | ---: | ---: | --- |
| Narrow scan | 27.355–29.784 | 20.458–23.458 | 1.398× / 1.337× / 1.270× |
| Range 1% | 12.116–12.696 | 13.390–13.938 | 0.880× / 0.948× / 0.877× |
| Equality 1% | 12.048–13.084 | 13.365–13.968 | 0.863× / 0.958× / 0.955× |
| Join | 33.340–38.322 | 31.457–31.958 | 1.133× / 1.051× / 1.199× |
| Full reduction | 174.884–181.486 | 178.908–187.456 | 0.956× / 0.984× / 0.968× |

Identical-configuration controls moved from approximately **5.3% less time to
9.4% more time** across query/session medians. Small changes need caution; no
control subtraction or statistical-significance claim is made.

### Additional opt-in reader controls

The combined configuration enables direct large-offset Arrow export, per-scan
execution-session reuse, whole-file statistics pruning and layout sub-splitting.
All four controls default to false. The following dedicated Parquet/Vortex pairs
measure that complete configuration in the same binary as the baseline. They
are not derived by multiplying Vortex-versus-Vortex ratios.

Combined local P/V ranges span the three independent session medians. This
configuration also wins 18 of 20 local medians per session; sorted/multi filters
remain slower than Parquet.

| Query | Sorted / single | Sorted / multi | Shuffled / single | Shuffled / multi |
| --- | ---: | ---: | ---: | ---: |
| Narrow scan | 2.808–2.849× | 2.287–2.328× | 2.240–2.318× | 1.931–1.998× |
| Range 1% | 1.182–1.211× | 0.839–0.856× | 7.924–8.141× | 2.757–2.832× |
| Equality 1% | 1.089–1.121× | 0.711–0.723× | 3.637–3.803× | 2.649–2.750× |
| Join | 1.618–1.681× | 1.296–1.347× | 1.594–1.641× | 1.241–1.259× |
| Full reduction | 1.264–1.374× | 1.159–1.195× | 1.254–1.305× | 1.134–1.199× |

Combined MinIO results:

| Query | Parquet median ms range | Vortex median ms range | P/V: S1 / S2 / S3 |
| --- | ---: | ---: | --- |
| Narrow scan | 28.113–29.491 | 20.405–21.651 | 1.356× / 1.378× / 1.405× |
| Range 1% | 12.392–12.622 | 12.440–13.459 | 0.921× / 1.015× / 0.946× |
| Equality 1% | 11.989–12.982 | 12.271–12.923 | 1.006× / 0.928× / 1.056× |
| Join | 32.623–35.874 | 29.845–31.882 | 1.191× / 1.093× / 1.079× |
| Full reduction | 171.863–184.453 | 174.231–181.331 | 1.017× / 0.986× / 0.960× |

Combined wins four of five remote medians in S1 and three of five in S2/S3.
Only narrow scans and joins beat Parquet in every session. The remaining wins
change by session, and no session wins every workload. These results do not
justify enabling the bundle by default. Individual-control diagnostics and
candidate selection are recorded in the [reader evaluation](vortex-phase8-design.md).

## Earlier measured coverage

These are separate saved comparisons, not samples pooled into the current
tables. They preserve the original 2×-plus results, extra selectivities and
group-by coverage. Different builds, process lifetimes and concurrency settings
prevent treating changes between tables as isolated optimization speedups.
Initial tiny-fixture/development-build setup checks established correctness;
they are excluded from performance claims.

### Standard concurrency, fresh process per query

**20 October 2026; evidence H1.** The first complete corrected release matrix
uses five measured trials plus one warmup per cell, the same local corpus and
both Vortex pushdowns. Source creation and complete query collection are timed.
There are **41/56 faster Vortex medians**; **36/56 observed min–max ranges overlap**.
The full matrix is retained below, including empty selections and regressions.
Every cell is P/V; † marks overlapping observed ranges, not statistical significance.

| Query | Sorted / single | Sorted / multi | Shuffled / single | Shuffled / multi |
| --- | ---: | ---: | ---: | ---: |
| Full reduction | 1.443† | 1.511† | 1.402† | 1.815† |
| Narrow scan | 1.476 | 1.894† | 1.868† | 2.492 |
| Range 0% | 0.725† | 0.711 | 1.221† | 0.224 |
| Range 0.1% | 0.839† | 0.516† | 7.682 | 2.282 |
| Range 1% | 1.016† | 1.065† | 4.202 | 3.302 |
| Range 50% | 4.986 | 0.914† | 7.372 | 2.725 |
| Range 100% | 5.589 | 1.647† | 3.979 | 1.566† |
| Equality 0% | 0.362† | 0.334 | 0.682† | 0.798† |
| Equality 0.1% | 0.797† | 0.454† | 3.286† | 2.023 |
| Equality 1% | 0.817† | 0.579† | 3.069† | 2.128 |
| Equality 50% | 2.475† | 1.673† | 5.038 | 2.627† |
| Equality 100% | 3.384 | 2.643 | 2.655† | 1.518† |
| Group-by | 2.006 | 1.540† | 2.083† | 1.478† |
| Join | 1.754† | 1.901† | 1.888† | 0.817† |

**20 October 2026; evidence H2.** A subsequent fresh-process comparison with
batched metadata planning used 20 measured trials plus one warmup. Vortex won
16/20 medians, with sorted/single range effectively tied. Seventeen of 20
observed sample ranges overlapped. All four layouts and all five cases follow;
these are historical, not current configuration results.

| Query | Sorted / single | Sorted / multi | Shuffled / single | Shuffled / multi |
| --- | ---: | ---: | ---: | ---: |
| Narrow scan | 2.636 | 1.934 | 2.226 | 1.733 |
| Range 1% | 1.000 | 0.682 | 7.384 | 2.264 |
| Equality 1% | 0.874 | 0.589 | 3.533 | 2.188 |
| Join | 1.716 | 1.324 | 1.677 | 1.289 |
| Full reduction | 1.416 | 1.438 | 1.344 | 1.389 |

### Restricted compute and scan concurrency

**20 October 2026; evidence H3.** This separate five-trial experiment requested
`--threads 1 --scan-tasks 1`. It is **not globally single-core or CPU-normalized**:
Vortex/Tokio/internal I/O workers remain outside these limits. It cannot replace
the standard-concurrency headline. Vortex won 18/20 medians; five observed ranges
overlapped. Cells are P/V; † has the same meaning as above.

| Query | Sorted / single | Sorted / multi | Shuffled / single | Shuffled / multi |
| --- | ---: | ---: | ---: | ---: |
| Full reduction | 3.858 | 3.290 | 3.997 | 3.168 |
| Narrow scan | 7.371 | 5.179 | 6.400 | 4.902 |
| Range 1% | 0.856† | 0.651† | 8.791 | 5.437 |
| Group-by | 2.514 | 2.191 | 2.827 | 2.315 |
| Join | 1.624† | 1.652† | 1.709 | 1.662† |

### Original remote comparison

**20 October 2026; evidence H4.** The first complete corrected MinIO comparison
used the same remote corpus, five trials plus one warmup, and fresh processes.
Parquet won all five medians with nonoverlapping observed ranges. Equality was
not measured here; group-by was. Persistent-process results above retain client
and connection setup across queries and must remain separate from this table.

| Query | Speed ratio P/V |
| --- | ---: |
| Narrow scan | 0.825 |
| Range 1% | 0.771 |
| Group-by | 0.774 |
| Join | 0.855 |
| Full reduction | 0.857 |

## Storage and resource comparison

File sizes are unchanged across these comparisons. The writers receive identical
logical rows, order and file boundaries. Sizes are **bytes**, not memory usage.

| Dataset | Parquet bytes | Vortex bytes | Vortex size increase |
| --- | ---: | ---: | ---: |
| Local sorted / single | 303,989,667 | 307,887,028 | 1.28% |
| Local sorted / multi | 304,161,063 | 308,026,776 | 1.27% |
| Local shuffled / single | 321,540,465 | 332,553,884 | 3.43% |
| Local shuffled / multi | 321,734,349 | 332,697,448 | 3.41% |
| MinIO sorted / multi | 38,082,927 | 38,610,088 | 1.38% |

The earlier standard-concurrency run H1 also measured lower process CPU and
peak RSS for these Vortex cases. Each range below is the minimum–maximum of the
**four layout medians**, not a confidence interval. These are historical resource
measurements, not current per-query memory claims.

| Query | Parquet CPU seconds | Vortex CPU seconds | Parquet peak RSS MiB | Vortex peak RSS MiB |
| --- | ---: | ---: | ---: | ---: |
| Full reduction | 2.386–2.477 | 1.156–1.540 | 841.7–1206.3 | 255.7–475.8 |
| Narrow scan | 0.240–0.360 | 0.091–0.142 | 239.2–244.8 | 119.5–182.6 |
| Group-by | 0.498–0.526 | 0.231–0.315 | 298.2–302.9 | 147.4–218.6 |
| Join | 0.646–0.865 | 0.446–0.680 | 316.7–321.5 | 197.3–314.8 |

CPU totals cover process threads. Fresh-process RSS includes imports/runtime
setup; the current persistent worker only supplies a session lifetime high-water
mark, which cannot be attributed to one query. Client metrics exclude MinIO and
the Docker VM. Local Parquet read bytes are unavailable through the shared I/O
counter, not zero. Available returned-source bytes exclude discovery/planning
and physical transport overhead. These results do not measure writer throughput.

## What is being measured

| Query | Work performed |
| --- | --- |
| Narrow scan | Counts and sums over `id`/`value` |
| Range S | Selects `id` below the first S fraction, then reduces values |
| Equality S | Filters a synthetic integer selector for the same selected IDs, then reduces values |
| Join | Joins facts to a 128-row in-memory dimension, then counts and sums weighted values |
| Full reduction | Reads every column through numeric, Boolean and Unicode string-length reductions |
| Group-by | Aggregates 128 groups |

Selectivity rounds down to an integer row count. These are complete Daft query
latencies, not isolated codec throughput or a canonical TPC-H suite.

- **Machine:** Apple M5, 10 logical CPUs, 32 GiB RAM, macOS 26.1 arm64.
  Release builds; Python 3.11.15, PyArrow 22.0.0, Vortex 0.88.0.
- **Data:** seed 42; nullable integers/Booleans, repeated labels, 64-character
  strings and integer equality selectors. Sorted/shuffled layouts contain the
  same logical values. Parquet uses Zstandard level 3, dictionaries, statistics
  and 65,536-row groups; Vortex uses its writer defaults.
- **Current execution:** native runner; eight scan tasks, ordered output,
  split/merge disabled; batch size 131,072; eight requests and 64 MiB in flight
  per Vortex reader; discovery and metadata concurrency eight. The two opt-in
  settings are explicitly enabled as described above.
- **Timer:** starts before fresh source construction and includes discovery,
  schema inference, planning, reading, computation and complete aggregate
  collection. Imports, dataset creation/upload, output validation and plan
  extraction are outside it. Clients/connections persist in the current runs;
  sources and results are not reused.
- **Baseline sampling:** three independent sessions per storage type, each with 20
  adjacent pairs, ten in each order, after one warmup. Separate instrumented
  queries never enter timing medians. Caches are retained; these are not verified
  cold-cache measurements.
- **Correctness:** the six retained complete experiment schedules passed
  **25,200 query executions** (24,000 measured plus 1,200 warmups) and **1,800
  separate diagnostic captures**, with unchanged provenance within each session
  and zero dropped/unfinished diagnostic events. Counts include control and
  configuration experiments. The interrupted local session remains separate;
  no individual timing failures are retried or removed within a session.
- **Candidate confirmation:** a fresh qualification, one remote final session
  and one local guard add **3,160 query executions** (3,000 measured and 160
  warmups) and **270 complete diagnostic captures**. Every output matches its
  fixture oracle, with zero dropped/unfinished events. Seeds are 3149, 4159 and
  5167 respectively. The remote performance gate fails despite correct results.

Observed speed ratios apply to this corpus, reader configuration and machine.
They do not rank Daft against DataFusion/Velox. Vortex-versus-Vortex pushdown or
planning improvements are excluded from the Parquet/Vortex speed ratios here.

## Evidence

Raw reports include individual samples, plans, configuration/fixture hashes,
compiler/build identity and errors. Artifacts are ignored by Git and remain in
the local workspace; the tables in this file are the reviewable checked-in
record. Directory names below preserve their original locations.

| ID | Raw report | Measured source |
| --- | --- | --- |
| L4 | [Session-reuse local confirmation](../../examples/vortex_eval/artifacts/phase8/acceptance-20261021T164449Z-51632/local-guard/session-01/report.json) | `b36572cc6ac5d0ff275e33ae8af2e961e0f02ab7` |
| R4 | [Session-reuse remote confirmation](../../examples/vortex_eval/artifacts/phase8/acceptance-20261021T164449Z-51632/remote-final/session-01/report.json) | Same as L4 |
| Q1 | [Candidate qualification](../../examples/vortex_eval/artifacts/phase8/acceptance-20261021T164449Z-51632/qualification/session-01/report.json) | Same as L4 |
| L1 | [Local session 1](../../examples/vortex_eval/artifacts/phase8/local-profile/session-01/report.json) | `ec0eb98216c7319cdaea09b12220b0cf44c134f6` |
| L2 | [Local session 2](../../examples/vortex_eval/artifacts/phase8/local-profile/session-02/report.json) | `ec0eb98216c7319cdaea09b12220b0cf44c134f6` |
| L3 | [Local session 3](../../examples/vortex_eval/artifacts/phase8/local-profile-clean/session-01/report.json) | `ec0eb98216c7319cdaea09b12220b0cf44c134f6` |
| R1 | [Remote session 1](../../examples/vortex_eval/artifacts/phase8/remote-terminal-20261021T161012Z-44223/remote/session-01/report.json) | `d02e23aa53d180d0e85561ea822e10119b266e2d` |
| R2 | [Remote session 2](../../examples/vortex_eval/artifacts/phase8/remote-terminal-20261021T161012Z-44223/remote/session-02/report.json) | `d02e23aa53d180d0e85561ea822e10119b266e2d` |
| R3 | [Remote session 3](../../examples/vortex_eval/artifacts/phase8/remote-terminal-20261021T161012Z-44223/remote/session-03/report.json) | `d02e23aa53d180d0e85561ea822e10119b266e2d` |
| H5 | [Prior persistent local](../../examples/vortex_eval/artifacts/phase7/local-profile-revised/report.json) | `345a7d460e0fbd837b5b870f102d9c197263694c` |
| H6 | [Prior persistent MinIO](../../examples/vortex_eval/artifacts/phase7/terminal-20261021T120240Z-87606/remote/report.json) | `52a954a7862c060a11686958806f129b079c3dfe` |
| H1 | [Earlier broad local matrix](../../examples/vortex_eval/artifacts/phase4/join-stats-fix/local-report/report.json) | `df7acfed1e52fcd4af194ed48b1382b178aa3b7d` |
| H2 | [Earlier 20-sample local matrix](../../examples/vortex_eval/artifacts/phase5/local-report/report.json) | `68fc70a7957f63c22f6beb1c319001f1f52ce778` |
| H3 | [Restricted concurrency](../../examples/vortex_eval/artifacts/phase4/join-stats-fix/restricted-report/report.json) | Same as H1 |
| H4 | [Original MinIO](../../examples/vortex_eval/artifacts/phase4/join-stats-fix/remote-report-20261020T171349Z/report.json) | Same as H1 |

SHA-256 report identities:

```text
L4  0cf1efed5cbd1c9e580719c89510045c102220a2df36e9e6c7ec09c9e185f94a
R4  d1ea231faed6fe656e1613d6531f2ae8c18bd72e304c48a76bc0e64cf6d4f2b2
Q1  e24c91ff0eae141dad27761a1626f100e392de5a631cf6e9b1e12c3eb7463068
L1  b44087e9b69e867d0fbdbc9f78355398960db7c799d9c3d67361b513f825f9e9
L2  a36ff4093bb67e2cde14beed20bdee6d0a4ba4f0ebeb565872b9153004d8709b
L3  f3f0299f3e55b92e9db3dd354137f70b65816b9e9783972bf6d9be4fc57cce0f
R1  385a06d737ee0bd1da4b3ce3c858c3165aebcc0a8adf5d9a716cd984f2c6ede3
R2  117e54a967b4e6aa21234fdde23c257be7bed58e167730c84632743f6b0a255c
R3  5259f669e79d761a5594a80aac5042e59eb008a9eef9607ed1e75443b0e23c68
H5  f103275c30e986b1c1f177d405f5133b04606d46cfd9c97114f47934794cdb8b
H6  dbda1fac19d400bcc25f7f1c18327b707baa2ffdf66369a8d4fe905cc25a38e6
H1  68566db52b2ed394ba7e1afc771a154df28388ad2f3f1caf36f793a4bc7d3171
H2  698070770c612e76f26c73a77063023e01b9dae28fdc4527e46cec53fa83cfc3
H3  9683b4cd19eab580ed8cdff6c22d3f78bdcc8509e00d5e90942a72f9d8f3c612
H4  9a1424c62fd53994b6ff8c76b288111b9804101d7915434e6070de5b51240c98
```

L1–L4, R1–R4 and Q1 use the same native binary:
`307b4452a633d577787e47e4162246660ec7495db56d885adf416aa6da42a568`.
The local source is `ec0eb9821`; the remote source is `d02e23aa5`, which adds
only the tested Python CLI case-selection fix. Confirmation source `b36572cc6`
adds the sorted-JSON schedule-validation fix and documentation without changing
the native reader. Subsequent test-only and documentation fixes do not replace
the measured source identity. H5 and H6 use the earlier binary:
`abdda9c07984620a2f0bc3cf0501a7c6d972889ab29e27e5352294c6d3af041f`.
The local and remote fixture manifests respectively hash to
`0f27c0d14df2cdf9e9cf4ac593a59fb1d2a637a85624634591abae56b8b9fd80`
and `eef77e8455ebfeb97536a2e1b8d205faec975d60f3f1af94145b9bc20f458d4c`.

## Updating this comparison

Update this file with every completed Parquet/Vortex performance evaluation;
keep it the single current comparison rather than creating a new summary per
implementation milestone. Use the [harness](../../benchmarking/vortex/README.md)
for commands and the [measurement protocol](vortex-integration-test-plan.md)
for validation requirements.

1. Read the complete saved comparison, verify output/provenance checks, and
   calculate P/V from the dedicated Parquet/Vortex samples in that build.
2. Update the date, exact settings, every comparable workload row and win/loss
   counts. Retain regressions, ties, failures and unavailable coverage. A failed
   or partial run must not replace a complete table with only its passing subset.
3. Keep local/remote, fresh/persistent processes and restricted concurrency
   separate. Never mix builds or select the best historical value per workload.
4. Preserve older coverage with its date/configuration when it has not been
   rerun. Record new raw-report paths/hashes before superseding current numbers;
   keep earlier evidence and failures available.
5. Distinguish experimental settings from defaults, timing medians from profiles,
   and correctness completion from performance acceptance. Recalculate headline
   ranges from the whole corresponding matrix.
