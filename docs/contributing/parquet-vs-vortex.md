# Parquet vs Vortex in Daft

Last updated: **21 October 2026**. This is the maintained comparison of the two
native readers in the personal Daft fork. Results are organized by workload,
storage and execution settings. All ratios compare Parquet with Vortex in the
same measured build.

**Speed ratio = Parquet median query time / Vortex median query time.** Above
1 favors Vortex; below 1 favors Parquet. For example, 2.0× means Vortex takes
half the time. Ratios are calculated before rounding the displayed times.

## Current measured results

- **Local files:** Vortex wins 18 of 20 workload/layout medians. Narrow scans are
  **1.92–2.71×**, joins **1.30–1.71×**, and full reductions **1.34–1.43×** as fast.
  Shuffled 1% filters reach **2.59–8.06×**. Sorted eight-file filters still lose.
- **S3-compatible storage on local MinIO:** Vortex wins two of five medians:
  **1.34×** for narrow scans and **1.05×** for joins. Selective filters and the
  full reduction remain slower than Parquet.
- **File size:** Vortex files are **1.27–3.43% larger** on these fixtures.
- **Earlier group-by coverage:** Vortex was **1.48–2.08×** as fast locally.
  Group-by has not been rerun with the current configuration.

These headline results use one explicit experimental Vortex configuration:
projection/filter pushdown, validated metadata reuse and lightweight row-count
planning enabled; read coalescing disabled. Metadata reuse and lightweight
planning are opt-in, not public API defaults. Parquet uses its native defaults.
Neither reader wins every workload. The all-five-remote-workloads target remains
unmet; correctness success is separate from performance acceptance.

### Local files

Each layout contains **7,895,160 rows**, approximately **1.066 GB of logical
Arrow data**. Single means one file; multi means eight files. Each row below
uses 20 measured paired trials and one excluded warmup, with persistent clients
and a fresh source/query each time. Source creation is included. Evidence: L.

| Layout | Query | Parquet median ms | Vortex median ms | Speed ratio P/V |
| --- | --- | ---: | ---: | ---: |
| Sorted / single | Narrow scan | 32.600 | 12.051 | 2.705× |
| Sorted / single | Range 1% | 4.507 | 3.757 | 1.200× |
| Sorted / single | Equality 1% | 4.074 | 3.590 | 1.135× |
| Sorted / single | Join | 97.837 | 57.222 | 1.710× |
| Sorted / single | Full reduction | 232.133 | 163.269 | 1.422× |
| Sorted / multi | Narrow scan | 32.397 | 14.546 | 2.227× |
| Sorted / multi | Range 1% | 3.900 | 5.246 | 0.743× |
| Sorted / multi | Equality 1% | 3.149 | 5.093 | 0.618× |
| Sorted / multi | Join | 80.586 | 59.689 | 1.350× |
| Sorted / multi | Full reduction | 225.877 | 158.379 | 1.426× |
| Shuffled / single | Narrow scan | 33.439 | 15.504 | 2.157× |
| Shuffled / single | Range 1% | 98.668 | 12.240 | 8.061× |
| Shuffled / single | Equality 1% | 51.370 | 13.818 | 3.718× |
| Shuffled / single | Join | 100.439 | 61.144 | 1.643× |
| Shuffled / single | Full reduction | 232.489 | 173.612 | 1.339× |
| Shuffled / multi | Narrow scan | 33.327 | 17.402 | 1.915× |
| Shuffled / multi | Range 1% | 32.074 | 11.656 | 2.752× |
| Shuffled / multi | Equality 1% | 32.804 | 12.655 | 2.592× |
| Shuffled / multi | Join | 82.044 | 63.337 | 1.295× |
| Shuffled / multi | Full reduction | 229.517 | 165.252 | 1.389× |

Each of the 18 winning rows was faster in all 20 adjacent pairs. Each losing
row was slower in all 20 pairs. The sorted/multi range and equality cases take
**34.5% and 61.7% more time** through Vortex, respectively. These counts describe
this experiment; they are not confidence intervals or universal guarantees.

### S3-compatible storage

The remote fixture has **986,895 sorted rows across eight files**, approximately
**133.257 MB logical data**. An authenticated MinIO container runs on the same
Mac through Docker. This measures an S3 client/server path, not WAN or hosted
AWS S3 performance. The corpus is smaller than the local one, so the two tables
do not measure a remote-versus-local slowdown. Evidence: R.

| Query | Parquet median ms | Vortex median ms | Speed ratio P/V | Vortex faster pairs |
| --- | ---: | ---: | ---: | ---: |
| Narrow scan | 26.845 | 19.975 | 1.344× | 20/20 |
| Range 1% | 11.145 | 12.286 | 0.907× | 6/20 |
| Equality 1% | 11.091 | 13.128 | 0.845× | 6/20 |
| Join | 33.073 | 31.432 | 1.052× | 15/20 |
| Full reduction | 164.733 | 170.358 | 0.967× | 7/20 |

Vortex takes 25.6% less time for narrow scans and 5.0% less for joins. It takes
10.2%, 18.4% and 3.4% more time for range, equality and full reduction. Small
differences need caution: the same run's identical-configuration Vortex equality
control moved approximately 11%. No statistical significance is claimed.

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
- **Sampling:** current comparisons use 20 adjacent pairs, ten in each order,
  after one warmup. Separate instrumented queries never enter timing medians.
  Caches are retained; these are not verified cold-cache measurements.
- **Correctness:** the current complete experiment schedules passed 4,830 query
  executions and 345 separate diagnostic captures with unchanged recorded
  provenance. This includes control/configuration experiments beyond the 25
  Parquet/Vortex rows shown here. No failed timing samples are retried or removed.

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
| L | [Current local](../../examples/vortex_eval/artifacts/phase7/local-profile-revised/report.json) | `345a7d460e0fbd837b5b870f102d9c197263694c` |
| R | [Current MinIO](../../examples/vortex_eval/artifacts/phase7/terminal-20261021T120240Z-87606/remote/report.json) | `52a954a7862c060a11686958806f129b079c3dfe` |
| H1 | [Earlier broad local matrix](../../examples/vortex_eval/artifacts/phase4/join-stats-fix/local-report/report.json) | `df7acfed1e52fcd4af194ed48b1382b178aa3b7d` |
| H2 | [Earlier 20-sample local matrix](../../examples/vortex_eval/artifacts/phase5/local-report/report.json) | `68fc70a7957f63c22f6beb1c319001f1f52ce778` |
| H3 | [Restricted concurrency](../../examples/vortex_eval/artifacts/phase4/join-stats-fix/restricted-report/report.json) | Same as H1 |
| H4 | [Original MinIO](../../examples/vortex_eval/artifacts/phase4/join-stats-fix/remote-report-20261020T171349Z/report.json) | Same as H1 |

SHA-256 report identities:

```text
L   f103275c30e986b1c1f177d405f5133b04606d46cfd9c97114f47934794cdb8b
R   dbda1fac19d400bcc25f7f1c18327b707baa2ffdf66369a8d4fe905cc25a38e6
H1  68566db52b2ed394ba7e1afc771a154df28388ad2f3f1caf36f793a4bc7d3171
H2  698070770c612e76f26c73a77063023e01b9dae28fdc4527e46cec53fa83cfc3
H3  9683b4cd19eab580ed8cdff6c22d3f78bdcc8509e00d5e90942a72f9d8f3c612
H4  9a1424c62fd53994b6ff8c76b288111b9804101d7915434e6070de5b51240c98
```

L and R use the same native binary:
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
