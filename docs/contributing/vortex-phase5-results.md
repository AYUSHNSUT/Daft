# Vortex Phase 5: bounded metadata planning results

**Implementation and local/user-terminal validation are complete for the declared experimental scope.** `metadata_concurrency=8` overlaps fresh file-footer reads while preserving ordering, strict validation and row counts for planning. Original local measurements show **19.9–23.8% lower eight-file metadata-call medians**, with smaller end-to-end query changes. The final MinIO comparison shows **14.2% lower metadata-call** and **4.2–9.3% lower query medians** versus concurrency 1 in the same build. Metadata request counts and returned bytes are unchanged.

Parquet still has the lower median in **all five final remote queries**, and sorted selective local regressions remain. Every final remote comparison's observed ranges overlap. These measurements describe this fork's implemented readers and metadata setting, not an engine ranking or a statistically established universal speedup.

Scope: personal `AYUSHNSUT/Daft`, branch `poc/vortex-phase5`, review base `poc/vortex-phase4`. Final accepted source is **`d45dc6478ef35a62528ad4058ce201a160d48fec`**. See the [design](vortex-phase5-design.md), [connector guide](../connectors/vortex.md), and [Phase 4 baseline](vortex-phase4-results.md). GitHub/Linux CI for the final revision remains separately unverified.

## Original local comparison

The local matrix uses clean `68fc70a79` and native `1e568…c8318`, before the later transport/compatibility corrections. Both caps and Parquet use the same build within this experiment. C1/C8 mean configured metadata file caps 1/8, not observed peak concurrency.

### Metadata-only results

The native bulk helper receives the dataset's explicit files once per trial. Call-wall timing surrounds the bulk call; its separately returned native elapsed diagnostic is shown alongside it. Both exclude discovery and query execution. Values are **median [min–max] over 20 measured trials**, excluding one warmup. C1/C8 mean configured metadata file caps 1/8, not observed peak concurrency.

| Layout | C1 call wall (ms) | C8 call wall (ms) | C8 time change | Native elapsed C1 / C8 (ms) | C8 faster rounds /20 |
| --- | ---: | ---: | ---: | ---: | ---: |
| sorted/single | 0.802 [0.759–4.091] | 0.806 [0.775–5.419] | +0.5% | 0.791 / 0.795 | 9 |
| sorted/multi | 2.478 [2.409–18.232] | 1.984 [1.850–10.597] | -19.9% | 2.466 / 1.971 | 18 |
| shuffled/single | 0.822 [0.778–4.098] | 0.810 [0.782–3.863] | -1.5% | 0.810 / 0.799 | 13 |
| shuffled/multi | 2.564 [2.394–16.233] | 1.955 [1.847–9.351] | -23.8% | 2.552 / 1.942 | 20 |

Eight-file C8 calls are faster in 18/20 sorted and 20/20 shuffled paired rounds. Their ranges still overlap. One-file controls show no consistent gain. The metadata counters below are identical at both caps in every measured sample; GET is the **Daft logical I/O counter**, including local file reads, not a claim about HTTP traffic.

| Layout | CPU C1 / C8 (ms) | RSS C1 / C8 (MiB) | GET at either cap | HEAD / LIST | Returned bytes at either cap |
| --- | ---: | ---: | ---: | ---: | ---: |
| sorted/single | 0.848 / 0.850 | 97.12 / 97.11 | 1 | 0 / 0 | 65,535 |
| sorted/multi | 2.708 / 3.401 | 97.43 / 99.30 | 8 | 0 / 0 | 524,280 |
| shuffled/single | 0.865 / 0.859 | 97.11 / 97.10 | 1 | 0 / 0 | 65,535 |
| shuffled/multi | 2.796 / 3.337 | 97.59 / 99.18 | 8 | 0 / 0 | 524,280 |

Eight-file metadata-only CPU rises approximately **19–26%**, and process RSS medians rise **1.6–1.9 MiB**. Less wall time is not less CPU work. RSS includes process imports/runtime setup, not just live footer allocations.

### End-to-end query effects

The query interval includes fresh source creation, discovery, first-file schema inference, all task-metadata windows, planning, execution and complete value-reduction collection. The following cells are **100 × (C8 median / C1 median − 1)**: negative is less elapsed time. All 20 query pairs have overlapping observed ranges.

| Query | Sorted/single | Sorted/multi | Shuffled/single | Shuffled/multi |
| --- | ---: | ---: | ---: | ---: |
| scan_narrow | +0.8% | -3.8% | +1.0% | -4.9% |
| range_0.01 | -1.0% | -9.5% | +2.5% | -9.5% |
| eq_0.01 | +1.8% | -8.4% | -1.2% | -4.7% |
| join | -0.5% | -1.1% | -0.1% | -1.6% |
| scan_full | +0.2% | -1.3% | +3.8% | +0.2% |

Absolute eight-file medians, with the contemporaneous Parquet comparator from the same build/rounds:

| Query | Sorted/multi C1 → C8 (ms) | Sorted/multi P (ms) | Shuffled/multi C1 → C8 (ms) | Shuffled/multi P (ms) |
| --- | ---: | ---: | ---: | ---: |
| scan_narrow | 17.421 → 16.759 | 32.411 | 20.284 → 19.297 | 33.436 |
| range_0.01 | 7.652 → 6.926 | 4.723 | 15.248 → 13.800 | 31.249 |
| eq_0.01 | 7.678 → 7.033 | 4.142 | 15.674 → 14.939 | 32.687 |
| join | 58.655 → 57.986 | 76.758 | 61.612 → 60.650 | 78.200 |
| scan_full | 148.932 → 146.998 | 211.392 | 152.511 → 152.745 | 212.091 |

Single-file query median changes range from roughly 1.2% lower to 3.8% higher. Sorted wide scans are 1.3% lower, while shuffled/eight-file wide scans are 0.2% higher. Query RSS median changes range from about −1.5 to +6.5 MiB. Execution returned-byte ranges are unchanged for every C1/C8 query pair; the observed latency differences are not query-byte pruning.

Public-source window size follows the cap, so changing C1 to C8 can amortize client/runtime setup across fewer bulk invocations as well as overlap files. Metadata-only mode makes **one bulk invocation at either cap**. Do not subtract independently measured medians to invent a planning percentage, or attribute the entire query change solely to parallel I/O.

### Contemporary Parquet comparison

Each cell is **Parquet median wall / Vortex C8 median wall**: above one favors Vortex. † marks overlapping observed min–max ranges; 17 of 20 pairs overlap.

| Query | Sorted/single | Sorted/multi | Shuffled/single | Shuffled/multi |
| --- | ---: | ---: | ---: | ---: |
| scan_narrow | 2.636† | 1.934† | 2.226† | 1.733† |
| range_0.01 | 1.000† | 0.682† | 7.384 | 2.264† |
| eq_0.01 | 0.874† | 0.589 | 3.533† | 2.188† |
| join | 1.716† | 1.324 | 1.677† | 1.289† |
| scan_full | 1.416† | 1.438† | 1.344† | 1.389† |

Vortex C8 has the faster median in 16/20 pairs, but sorted/single 1% range is approximately tied. Sorted/single equality remains **14.4% slower** than Parquet; sorted/eight-file range and equality remain **46.7% and 69.8% slower**. Phase 4 timings are historical context only; no prior-build samples are pooled into this experiment.

## Local method and provenance


The original local report validates **1,428 passing trials**, 1,360 measured + 68 warmups: 1,260 query trials (five cases × three configurations × four datasets × 21 rounds) and 168 metadata-only trials (two caps × four datasets × 21). It includes 1,260 saved query plans. Every group has one warmup and 20 measured repetitions in exact seeded/interleaved order. Independent query oracles and ordered metadata schema/nullability/count results are checked outside timing.

Each trial is a fresh process with an unrelated runtime-initialization query before timing. Imports, process startup, fixture generation, result validation and saved-plan extraction are excluded. Metadata mode reports only the bulk call, not discovery; query source counters report execution file scans, not planning metadata. Query request counts remain unavailable. `InMemoryScan` is excluded, and **local Parquet file bytes are unavailable, not zero**. None of these counters measure physical-device reads, packet bytes, or unique file coverage.

The same validated Phase 4 fixtures supply 7,895,160 identical rows per local dataset, approximately 1.066 GB Arrow logical bytes, in sorted/shuffled and one/eight-file layouts. Parquet writer settings are Zstandard level 3 with dictionaries/statistics and 65,536-row groups; Vortex is 0.88.0 defaults. Fixture manifest SHA-256: `0f27c0d14df2cdf9e9cf4ac593a59fb1d2a637a85624634591abae56b8b9fd80`. Generation overlapped earlier builds and includes input generation/hashing; its duration is not comparative writer performance.

The machine is Apple M5/arm64, 10 logical CPUs, 32 GiB RAM; reports record macOS 26.1, Python 3.11.15, PyArrow 22.0.0 and Vortex 0.88.0. Query settings retain both pushdowns, batch size 131,072, eight requests/64 MiB per open reader, result buffer one partition and the same execution configuration. Metadata file caps are varied independently of execution scan-task concurrency.

The local release build is explicitly attested; binary SHA-256 is **`1e568e1f4ce24dfb8da0d0d7bbfed38a9ebcbe5be646a9629b714bcd424c8318`**. Lockfile SHA-256 is `03fd70fdfb1f1e6a291985a954b24eb92c3631c45287d07fc5e716faa576854d`. Full build command, compiler/package/harness hashes, source-change status, settings and raw samples are retained in the report. Current `local-report/report.json` SHA-256 is **`698070770c612e76f26c73a77063023e01b9dae28fdc4527e46cec53fa83cfc3`**.

The local source is `68fc70a7957f63c22f6beb1c319001f1f52ce778`. These measurements precede the shared S3 connector and MCAP compatibility fixes; they are not relabeled as measurements of the final build. The current remote comparison uses its own provenance below, and no samples are pooled across builds.

Accepted local analysis is `examples/vortex_eval/artifacts/phase5/local-analysis.{json,md}`. Hash preflight and warmups read data; caches are retained, not verified cold. Builds/tests do not overlap timings. Outliers are retained: the largest local max/min spread is 47.28× for one Parquet range group. Paired-round counts do not eliminate variability or establish significance.

## Semantics and resource bounds


`metadata_concurrency` is a strict integer in **1..64**, default 8. Every materialization refreshes counts, including reused/pickled sources, in ordered bounded windows. Discovery, deduplication/order, first-error ordering, path/count association, credentials/redaction and strict full schema/nullability validation remain preserved. Failures do not create invented zero-count tasks. Existing joins retain small-dimension build planning.

Bulk calls share client/runtime setup within a batch but **do not reuse footers across distinct opens**. Execution reopens files and validates before projection or `limit=0`; fresh counts do not provide snapshot isolation. Source serialization includes the cap, while native task wire format is unchanged. No global planner, predicate, count-pushdown or Arrow-conversion rules change.

The cap bounds active metadata files and returned schema/count windows. Existing request/byte budgets apply per open reader, so aggregate work can multiply by active files. This is not a query-wide byte or RSS cap, and a printed cap is not an observed peak. CPU includes process threads; RSS is its high-water through completion. Remote CPU/RSS describe the Daft client and exclude MinIO/Docker VM resources.

## Final user-terminal MinIO comparison

The final unpaced run passes **357 trials: 340 measured + 17 warmups**, with exact coverage across 17 groups and 315 saved query plans. Independent analysis verifies order, query oracles, metadata counters and clean build provenance. The dataset contains **986,895 rows**, approximately **133.3 MB Arrow logical bytes**, sorted into eight files per format and served by authenticated local Docker MinIO. This is not AWS/WAN performance or Ray scaling.

Metadata-call wall time falls from **109.861 [103.399–123.934] ms** at C1 to **94.312 [89.522–131.049] ms** at C8: **14.2% lower**, faster in **18/20** measured paired rounds. Observed ranges overlap. Both caps record **eight HEAD + eight GET calls, zero LIST calls and 524,280 returned bytes** in every measured sample. CPU medians are **56.420 / 56.103 ms** and RSS medians **106.67 / 108.04 MiB**: approximately 0.6% lower CPU and 1.37 MiB higher RSS.

Query wall values are **median [min–max] ms** over 20 measured samples. P/C8 above one favors Vortex; all five Parquet/C8 and all five C1/C8 observed ranges overlap.

| Query | Parquet | Vortex C1 | Vortex C8 | C8 vs C1 | P / C8 |
| --- | ---: | ---: | ---: | ---: | ---: |
| scan_narrow | 128.396 [115.586–144.029] | 142.525 [124.782–150.143] | 129.224 [121.452–135.493] | -9.3% | 0.994 |
| range_0.01 | 109.760 [103.724–118.352] | 132.410 [117.181–151.258] | 121.700 [109.569–141.879] | -8.1% | 0.902 |
| eq_0.01 | 111.598 [105.704–123.617] | 131.173 [125.325–138.943] | 119.297 [114.341–127.968] | -9.1% | 0.935 |
| join | 135.731 [116.266–149.668] | 151.331 [144.873–167.271] | 139.096 [131.345–153.111] | -8.1% | 0.976 |
| scan_full | 274.146 [256.704–348.600] | 298.642 [260.217–333.580] | 286.107 [269.896–345.003] | -4.2% | 0.958 |

C8 is faster than C1 in **18/20 narrow, 16/20 range, 20/20 equality, 20/20 join and 14/20 full-scan** paired rounds. Nevertheless, C8 remains **0.6% slower than Parquet for narrow, 10.9% for range, 6.9% for equality, 2.5% for join and 4.4% for full scan**. Lower execution bytes do not guarantee lower complete-query latency.

The following cells are medians; CPU/RSS cover the Daft trial process, **excluding MinIO/Docker VM**. Execution returned bytes exclude discovery and planning metadata.

| Query | CPU P / C1 / C8 (ms) | RSS P / C1 / C8 (MiB) | Execution returned bytes P / C1 / C8 |
| --- | ---: | ---: | ---: |
| scan_narrow | 87.03 / 79.82 / 77.51 | 125.14 / 122.08 / 122.55 | 4,939,392 / 2,386,356 / 2,386,356 |
| range_0.01 | 62.97 / 73.49 / 71.45 | 115.02 / 118.23 / 118.68 | 1,337,679 / 757,036 / 757,036 |
| eq_0.01 | 62.96 / 72.58 / 70.70 | 115.74 / 118.77 / 119.09 | 1,337,801 / 757,036 / 757,036 |
| join | 138.48 / 115.04 / 113.35 | 151.31 / 160.45 / 158.63 | 4,947,843 / 3,254,996 / 3,254,996 |
| scan_full | 236.30 / 210.49 / 212.25 | 207.15 / 224.50 / 234.22 | 39,103,583 / 39,027,972 / 39,027,972 |

C8 query CPU medians are 1.5–2.9% lower except the full scan, which is 0.8% higher. RSS median changes range from 1.82 MiB lower to 9.72 MiB higher. Query execution bytes remain unchanged between caps. Query request counts are unavailable; metadata counters describe their separate call, not whole-query requests, physical traffic or unique byte coverage.

Final evidence is under `examples/vortex_eval/artifacts/phase5/acceptance-20261020T215603Z/`: `remote-analysis.{json,md}`, `acceptance-audit.json`, raw trials, saved plans and logs. Source is **`d45dc6478ef35a62528ad4058ce201a160d48fec`**, native SHA-256 **`ac10e49fc47eaeef874bc752fa0b42354bd534d86812906c3707a7eae2e4a9dd`**, report SHA-256 **`d0f70e0e825a3204a6993294b29ad035eaba40c4185f426c21d8b089c77918a4`**. Remote manifest SHA-256 is `eef77e8455ebfeb97536a2e1b8d205faec975d60f3f1af94145b9bc20f458d4c`; lockfile and recorded release command match the declared configuration.

## Transport and compatibility corrections

An initial remote run recorded 295 passed / 62 failed trials across both readers, alongside 419 matching Docker VM forwarding `EADDRNOTAVAIL` errors. Exact resource inventory was not captured. Review found that the shared S3 HTTP connector was recreated per request, discarding its pool despite the SDK's cache contract. Connector caching now preserves distinct timeout settings, proxy routing, TLS and runtime sleep behavior for both formats. Four real TCP reuse/proxy/timeout tests pass. No pacing or retry changes were introduced; the failed report and diagnostics remain preserved.

Broader validation then exposed MCAP's existing HTTP-200 full-file fallback being blocked by strict shared range checks. A typed range error now distinguishes an ignored range from a malformed partial response. MCAP's initial large-file probe permits an explicit full-read fallback only for the ignored-range case; small remote files use a full GET directly. Both full-buffer paths verify the file size. Vortex retains strict 206/Content-Range validation. Malformed partial responses, authentication and unrelated I/O errors do not silently fall back. The final ORC/MCAP and strict Vortex service suites pass.

An intermediate `0713406` / native `6f88…7c71` run also passed 357 remote trials, but still had the MCAP regression. It is historical evidence only: its timings are not pooled into the final matrix above or substituted for the corrected build.

## Validation record

All five final user-terminal command groups exited 0 on the clean accepted source. Agent gates and user-terminal gates are separate; counts overlap and are not a unique-test total.

| Gate | Observed outcome | Evidence under `examples/vortex_eval/artifacts/phase5/` |
| --- | --- | --- |
| Broad native Python core | **5,389 passed, 2,090 existing skips** | `mcap-http-fix/core-regressions.{log,xml}` |
| Release native CI selection | **99 passed**: 59 scan + 40 Vortex | `mcap-http-fix/native-release-tests.log` |
| Native HTTP / MCAP selections | **Four / 16 passed**; two MCAP cases filtered in this selection | `mcap-http-fix/{http-range-tests,native-mcap-tests}.log` |
| Audited agent-local Rust | **2,229 passed, zero failed, 20 ignored, 71 explicit service filters**, exit 0; 157 targets | `mcap-http-fix/local-validation-summary.json` |
| Feature off | **154 passed, 317 expected feature skips**; generic selection **224 passed, 17 runner skips** | `mcap-http-fix/feature-modes/` logs/XML |
| Feature on | **552 passed, zero skips** | `mcap-http-fix/feature-modes/release-tests.xml` |
| Local MCAP in both feature modes | **22 passed, 13 deselected in each** | Feature-mode MCAP logs |
| CLI correctness smokes | **100 Phase 4 + 17 metadata trials passed**, no performance claim | `mcap-http-fix/{phase4-smoke,metadata-smoke}/report.json` |
| Hooks / docs before final terminal run | **14 applicable hooks passed**, including all-feature/all-target Clippy with warnings denied; MkDocs passed with existing warnings | `mcap-http-fix/{pre-commit,docs-build}.log` |
| User S3 TCP behavior | **Four passed**, zero failed | Final acceptance `s3-pooling-tests.log` |
| User ORC / MCAP | **112 passed, one optional S3-environment skip**, 12.15s | Final acceptance `orc-mcap.{log,xml}` |
| User HTTP / MinIO / Ray | **17 passed, zero skips**, 24.46s | Final acceptance `service-acceptance.log` |
| Final remote matrix | **357 passed**, exact oracles/counters/provenance validated | Final acceptance `remote-analysis.{json,md}` |
| Full user Rust workspace | **2,300 passed, zero failed, 20 ignored, zero filtered**, exit 0; all 157 targets | Final acceptance `workspace-rust-tests.log` and exit/source capture |

The final full Rust log SHA-256 is `b2480a01c531f165292795ebeceafca80912fb218c67a271ff0f6b514264830b`. The earlier Linux CI native step exposed a release-only assertion requiring a conditional `Debug` implementation; the assertion was corrected and its exact release selection passes locally. Earlier skipped CI steps are not passed, and a final GitHub/Linux run is separately unverified. User-terminal completion does not certify that runner.

## Limits and follow-up

The reader remains experimental, personal-fork-only and default-off, with the existing strict primitive schema and accepted storage/runner matrix. The local and remote matrices use separate builds and dataset sizes; no cross-build causal speedup is inferred. Caches were not flushed, all errors/outliers were retained, and paired counts/range overlap are descriptive rather than significance tests. These are not standard TPC-H, codec-throughput or DataFusion/Velox comparisons.

Cross-open footer caching needs strong object-version and conditional-read semantics. Request merging, discovery fast paths, residual removal, intra-file execution splits and encoding/conversion changes remain separate work. Material AI assistance supported implementation, independent review, tests, analysis and documentation; local, user-terminal and GitHub evidence are recorded separately.
