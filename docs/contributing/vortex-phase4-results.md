# Vortex Phase 4: corrected planning and reader-path evaluation

**Release evaluation and local/user-terminal acceptance are complete for the declared experimental scope.** Footer row metadata corrects the observed join build-side choice. In the main matrix, Vortex both-on joins have **60–97ms medians**, compared with the pre-fix baseline's 1.14–1.64s. Vortex's current join median beats Parquet in three layouts, but **loses in shuffled/multifile (P/V 0.817)**; every default join's observed ranges overlap. These runs were taken at different times and Parquet also changed, so their before/after ratio does not isolate the fix's causal speedup.

Current local/default medians favor Vortex by **1.40–1.82× for full scans, 1.48–2.49× for narrow scans, and 1.48–2.08× for grouping**. Selective-filter regressions remain. On the smaller MinIO corpus, Parquet's median wins all five queries (P/V 0.771–0.857). These are implemented Daft reader paths, not a Daft/DataFusion/Velox ranking or standard TPC-H result.

Scope: experimental `AYUSHNSUT/Daft`, branch `poc/vortex-phase4`, reviewed against `poc/vortex-phase3`. Measured source is clean commit **`df7acfed1e52fcd4af194ed48b1382b178aa3b7d`**. See the [design](vortex-phase4-design.md), [connector guide](../connectors/vortex.md), and [harness](https://github.com/AYUSHNSUT/Daft/tree/poc/vortex-phase4/benchmarking/vortex). GitHub/Linux CI execution remains unverified.

## Default local results

Each cell is **Parquet median wall / Vortex 11 median wall**; above one favors Vortex, below one is a regression. Vortex 11 enables both projection and filtering. Each path has five measured trials plus one warmup. † means the observed min–max ranges overlap; it is not a significance test. The main matrix has 41 faster/15 slower Vortex medians and 36/56 overlapping ranges. All outliers remain in the raw samples. Across the full five-configuration default matrix, the largest observed max/min spread is 12.71×; these median comparisons do not establish statistical significance.

| Case | Sorted/single | Sorted/multi | Shuffled/single | Shuffled/multi |
| --- | ---: | ---: | ---: | ---: |
| scan_full | 1.443† | 1.511† | 1.402† | 1.815† |
| scan_narrow | 1.476 | 1.894† | 1.868† | 2.492 |
| range_0.01 | 1.016† | 1.065† | 4.202 | 3.302 |
| eq_0.01 | 0.817† | 0.579† | 3.069† | 2.128 |
| group_by | 2.006 | 1.540† | 2.083† | 1.478† |
| join | 1.754† | 1.901† | 1.888† | 0.817† |

### Join planning and resource evidence

The pre-fix source's unknown row count became zero and selected the fact table as the hash-build input. Vortex now refreshes each file footer through configured Daft I/O, attaching exact input row counts to existing native task metadata. Global join rules are unchanged; filtered estimates still use standard rules, and no count/aggregation pushdown was added. All per-file footer planning I/O is included in new end-to-end timings.

All 28 saved default/restricted join plans choose the small dimension side. All 168 corresponding join trials record 7,895,160 fact rows probed in/out and pass output checks. The small-build `build rows inserted` counter remains zero, an instrumentation gap rather than proof of an empty build. The table below shows **median [min–max]** across five measured main-matrix trials.

| Layout | P wall (ms) | V11 wall (ms) | P RSS (MiB) | V11 RSS (MiB) |
| --- | ---: | ---: | ---: | ---: |
| sorted/single | 145.0 [93.4–150.6] | 82.7 [54.4–305.0] | 321.5 [319.9–323.1] | 219.0 [208.2–239.8] |
| sorted/multi | 114.0 [77.1–122.2] | 60.0 [58.6–92.2] | 316.7 [312.0–318.5] | 314.8 [288.8–320.6] |
| shuffled/single | 157.2 [94.8–558.6] | 83.2 [57.3–95.7] | 321.3 [317.7–326.3] | 197.3 [183.5–203.9] |
| shuffled/multi | 78.9 [77.1–130.4] | 96.5 [61.5–385.1] | 319.6 [312.8–328.6] | 298.9 [278.0–314.8] |

A separate earlier 48-trial focused join experiment reported Vortex medians of 55–63ms versus Parquet 79–97ms. It is not merged into the main matrix; its shuffled/multifile Vortex maximum was 274.64ms. Use the completed matrix above for current summaries, not the most favorable focused run.

For the following resource summary, ranges are the **minimum–maximum of the four layout medians**, not trial confidence intervals. CPU covers process threads; RSS is the process high-water through completion, including imports/runtime setup.

| Case | P CPU (s) | V11 CPU (s) | P RSS (MiB) | V11 RSS (MiB) |
| --- | ---: | ---: | ---: | ---: |
| scan_full | 2.386–2.477 | 1.156–1.540 | 841.7–1206.3 | 255.7–475.8 |
| scan_narrow | 0.240–0.360 | 0.091–0.142 | 239.2–244.8 | 119.5–182.6 |
| group_by | 0.498–0.526 | 0.231–0.315 | 298.2–302.9 | 147.4–218.6 |
| join | 0.646–0.865 | 0.446–0.680 | 316.7–321.5 | 197.3–314.8 |

## Restricted compute and scan concurrency

This separate experiment requests `--threads 1 --scan-tasks 1`. It is **not globally single-core or CPU-normalized**: Vortex/Tokio and internal I/O workers remain outside those limits. It reports 18 faster/2 slower Vortex medians, with five of 20 ranges overlapping.

| Case | Sorted/single | Sorted/multi | Shuffled/single | Shuffled/multi |
| --- | ---: | ---: | ---: | ---: |
| scan_full | 3.858 | 3.290 | 3.997 | 3.168 |
| scan_narrow | 7.371 | 5.179 | 6.400 | 4.902 |
| range_0.01 | 0.856† | 0.651† | 8.791 | 5.437 |
| group_by | 2.514 | 2.191 | 2.827 | 2.315 |
| join | 1.624† | 1.652† | 1.709 | 1.662† |

Full scans favor Vortex by 3.17–4.00×, narrow scans 4.90–7.37×, grouping 2.19–2.83×, and joins 1.62–1.71×. Sorted 1% range queries still lose (P/V 0.651–0.856). Keep this experiment separate from the default results.

## Pushdowns: measured benefits and regressions

- Projection-only narrow scans are **6.33–8.86×** faster than Vortex with both flags off, with returned bytes dropping from approximately 294–317 MiB to 14–36 MiB across layouts.
- Sorted 1% ranges with both flags improve **24.24–25.45×** over both off, returning 0.301–0.735 MiB. Filter-only shuffled 1% ranges improve 2.27–3.05× while bytes remain essentially unchanged; that is not evidence of I/O pruning.
- Flags do not guarantee a win: full-scan both-off/both-on median ratios range **0.665–1.046**, and sorted 1% equality loses against Parquet. Some queried paths have wide ranges/outliers. These five-sample observations do not isolate codec, conversion, scheduling, or thermal/background causes. Full four-configuration attribution remains in `local-analysis.{json,md}`.

## User-terminal HTTP/MinIO/Ray and remote results

The current service suite reports **14 passed, zero skipped in 21.36s**. The user then completed the post-fix MinIO matrix: **60 passed trials**, 50 measured plus 10 warmups, with matching independent outputs. The same clean source, native binary, and fixture identities are recorded. This is sorted/eight-file local Docker MinIO, not AWS/WAN performance or Ray scaling.

Wall values are median [min–max]; other cells show medians. Paired resource columns are Parquet / Vortex 11. CPU/RSS cover **only the Daft trial process, excluding MinIO/Docker VM**. File bytes cover returned execution-source data, not discovery/planning or transport overhead.

| Case | P wall (s) | V11 wall (s) | P/V | CPU P / V11 (s) | RSS P / V11 (MiB) | File bytes P / V11 (MiB) |
| --- | ---: | ---: | ---: | --- | --- | --- |
| scan_full | 0.285 [0.275–0.317] | 0.333 [0.327–0.341] | 0.857 | 0.249 / 0.242 | 201.5 / 226.2 | 37.292 / 37.220 |
| scan_narrow | 0.131 [0.129–0.144] | 0.159 [0.153–0.182] | 0.825 | 0.096 / 0.092 | 127.7 / 124.7 | 4.711 / 2.276 |
| range_0.01 | 0.115 [0.114–0.117] | 0.149 [0.143–0.183] | 0.771 | 0.065 / 0.080 | 116.4 / 120.2 | 1.276 / 0.722 |
| group_by | 0.132 [0.131–0.139] | 0.170 [0.160–0.185] | 0.774 | 0.104 / 0.105 | 135.6 / 135.1 | 4.719 / 3.104 |
| join | 0.140 [0.133–0.149] | 0.164 [0.159–0.166] | 0.855 | 0.145 / 0.125 | 161.4 / 160.7 | 4.719 / 3.104 |

All five remote median regressions have nonoverlapping observed ranges. The refreshed per-file footer reads are included in planning time; their separate causal cost has not been isolated. Preserve these regressions alongside local improvements.

## Corpus, method, and provenance

Identical logical rows are generated in bounded 65,536-row batches with seed 42: nullable integers/Booleans, repeated labels, 64-character strings, and Int64 equality selectors. Sorted/shuffled inputs have identical values; single/eight-file layouts match boundaries across formats. Parquet uses Zstandard level 3, dictionaries/statistics, and 65,536-row groups; Vortex uses 0.88.0 writer defaults. Sizes below are bytes, not equalized compression targets.

| Corpus/layout | Rows | Logical bytes | Parquet bytes | Vortex bytes |
| --- | ---: | ---: | ---: | ---: |
| Local shuffled/multi | 7,895,160 | 1,066,058,180 | 321,734,349 | 332,697,448 |
| Local shuffled/single | 7,895,160 | 1,066,058,175 | 321,540,465 | 332,553,884 |
| Local sorted/multi | 7,895,160 | 1,066,058,180 | 304,161,063 | 308,026,776 |
| Local sorted/single | 7,895,160 | 1,066,058,175 | 303,989,667 | 307,887,028 |
| MinIO sorted/multi | 986,895 | 133,257,290 | 38,082,927 | 38,610,088 |

Generation overlapped compilation and includes generation/input hashing; recorded write durations are **not comparable writer-throughput measurements**. The approximately 1 GiB logical target is per dataset; exact sizes/hashes are in the manifests. Main manifest SHA-256: `0f27c0d14df2cdf9e9cf4ac593a59fb1d2a637a85624634591abae56b8b9fd80`; remote: `eef77e8455ebfeb97536a2e1b8d205faec975d60f3f1af94145b9bc20f458d4c`.

Each fresh trial process initializes Daft with an unrelated query, then times source creation/discovery/schema and all footer planning through complete value-reduction collection. Imports, process startup, generation/upload, oracle checks, and saved-plan extraction are excluded. One warmup plus five measured rounds are seeded/interleaved; input hashing and warmups retain OS cache contents. These are **not verified cold-cache results**. Min/max are observed spread, not confidence intervals; no significance claim is made.

Only file ScanTask metrics count toward bytes; `InMemoryScan` is excluded. **Local Parquet bytes are unavailable, not zero**, because its direct read path bypasses I/O counters. Available bytes exclude planning/discovery and are not physical-device reads, packet bytes, unique file coverage, or decoded memory. Request counts and separate decode/conversion timings are unavailable. Saved unpruned join plans use one scan task per file, with split/merge false; Parquet internal decoding parallelism differs. Emitted-morsel `task.count` is not a file-task count.

Environment: Apple M5 (user capture), 10 logical CPUs, 32 GiB RAM, macOS 26.1 arm64, Python 3.11.15, PyArrow 22.0.0, Vortex 0.88.0, pinned nightly Rust. Both local experiments retain batch size 131,072, Vortex per-reader budgets of 8 requests/64 MiB, result buffer one partition, and maintain-order true; Parquet uses its native I/O defaults. Effective concurrent scan tasks are eight/default and one/restricted. The attested optimized command is recorded in every report (`CI=1` selects headless dashboard assets):

```bash
env -u CONDA_PREFIX CI=1 CARGO_BUILD_JOBS=2 PYO3_PYTHON=.venv/bin/python \
  DAFT_DASHBOARD_SKIP_BUILD=1 RUST_DAFT_PKG_BUILD_TYPE=release \
  .venv/bin/maturin develop --release --skip-install --offline --locked \
  --features python,vortex
```

Native SHA-256: `43466e8192130e4ea956de28cc57da6d279579091f8bd6434d9c4265f6d68f03`; lockfile: `03fd70fdfb1f1e6a291985a954b24eb92c3631c45287d07fc5e716faa576854d`. Reports record full compiler/package/harness hashes, clean revision, settings, plans, individual samples, and errors. Paths below are under `examples/vortex_eval/artifacts/phase4/join-stats-fix/`.

| Accepted current experiment | Trials | SHA-256 of report.json |
| --- | ---: | --- |
| `local-report/` | 1,680 = 1,400 measured + 280 warmups | `68566db52b2ed394ba7e1afc771a154df28388ad2f3f1caf36f793a4bc7d3171` |
| `restricted-report/` | 240 = 200 measured + 40 warmups | `9683b4cd19eab580ed8cdff6c22d3f78bdcc8509e00d5e90942a72f9d8f3c612` |
| `remote-report-20261020T171349Z/` | 60 = 50 measured + 10 warmups | `9a1424c62fd53994b6ff8c76b288111b9804101d7915434e6070de5b51240c98` |

These three complete experiments contain **1,980 passing trials**; the earlier 48 focused joins are separate. Exact local coverage, every oracle output, shared provenance, 28 join plans and 168 fact-probe checks are verified in `local-validation.{json,md}`. Full analysis is in `local-analysis.{json,md}` and `remote-analysis.{json,md}`. Pre-fix baselines remain under the parent `phase4/` artifacts and are not mixed into current medians.

## Validation

| Gate | Recorded outcome | Evidence |
| --- | --- | --- |
| Broad native Python | 5,400 passed, 2,090 existing skips, five deselected | `core-regressions.log` / XML |
| Enabled release | 484 passed, zero skips | `release-tests.log` |
| Feature-off release | 137 passed, 305 expected feature skips | `disabled-tests.log` |
| Audited local Rust workspace | 2,225 passed, zero failed, 20 ignored, 67 explicit service filters | `workspace-local-rust-tests-summary.json` |
| Workspace all-feature Clippy and applicable hooks | Passed | `rust-pre-commit-final.log`, Python/doc logs |
| User HTTP/MinIO/Ray | 14 passed, zero skips | `service-acceptance.log` |
| Current user unfiltered Rust | **2,292 passed, zero failed, 20 ignored, zero filtered; exit 0** | `user-workspace-rust-tests.log`, source/exit capture, `user-acceptance-summary.{json,md}` |
| Final documentation | MkDocs and applicable contribution hooks passed | `final-mkdocs-build.log`, `final-docs-pre-commit.log` |
| GitHub/Linux CI | Workflow defined/pushed; execution unverified | No Linux certification claimed |

The full user-terminal Rust run completes all 157 targets: 78 unit targets with 2,272 passes, three integration targets with 17 passes, and 76 doctest targets with three passes. Its captured revision is `df7acfed1`, tracked changes are empty, and Cargo exit is zero. Log SHA-256: `3297f3bcad1d874b3770f6a24ccbe4ac122f9eae7c0287a49a4fd59c854445d7`. The filtered agent gate is reported separately. Some cloud smoke tests can return successfully without credentials; this does not certify every cloud provider. The real HTTP/MinIO/Ray suite requires zero skips.

To reproduce, use the [harness setup and commands](https://github.com/AYUSHNSUT/Daft/tree/poc/vortex-phase4/benchmarking/vortex) with the recorded release build above. Run the full local matrix without case/config restrictions, then the separate restricted matrix:

```bash
.venv/bin/python -m benchmarking.vortex run \
  --manifest examples/vortex_eval/artifacts/phase4/fixtures-1g/manifest.json \
  --output examples/vortex_eval/artifacts/phase4/repro-local --rounds 5 --warmups 1

.venv/bin/python -m benchmarking.vortex run \
  --manifest examples/vortex_eval/artifacts/phase4/fixtures-1g/manifest.json \
  --output examples/vortex_eval/artifacts/phase4/repro-restricted \
  --configs parquet vortex_11 \
  --cases scan_full scan_narrow range_0.01 group_by join \
  --threads 1 --scan-tasks 1 --rounds 5 --warmups 1

.venv/bin/python -m examples.vortex_eval.run_phase4_services \
  --manifest examples/vortex_eval/artifacts/phase4/remote-fixtures/manifest.json \
  --output examples/vortex_eval/artifacts/phase4/repro-remote \
  --docker-image draft-vortex-minio:phase3
```

The existing image name reflects the user's local build tag. See the service helper instructions for building an image or supplying an existing endpoint. Set `DAFT_RUNNER=native`, disable progress/analytics, and export `DAFT_VORTEX_BENCH_BUILD_COMMAND` to the actual release invocation before measuring. Generate missing manifests with the harness using `--logical-gib 1 --orders sorted shuffled --layouts single multi --files 8 --batch-rows 65536` for local inputs and `--logical-gib 0.125 --orders sorted --layouts multi --files 8 --batch-rows 65536` for remote inputs. Generated data and detailed logs/reports are ignored local artifacts; the committed tables and hashes summarize them.

## Limits

The feature remains experimental, personal-fork-only, and default-off: strict matching flat primitive schemas, local/exact-HTTP/S3-compatible paths, native/Ray scope, and one task per file. Writes, schema evolution, broader types, and intra-file task splitting remain outside scope. Vortex values cross the Arrow/Daft conversion boundary; no full query execution on compressed arrays is implied.

Per-reader request budgets are not global RSS caps, and dispatched OS reads may finish after cancellation. RSS is measured, not guaranteed. Variable samples and median outcomes here do not establish a universal engine winner. Material AI assistance supported implementation, tests, independent review, analysis, and documentation; agent-run and user-terminal evidence are separated.

## Appendix: all default workload ratios

P/V is Parquet median wall / Vortex 11 median wall; † marks overlapping observed min–max ranges. Five measured samples per cell; no statistical significance is inferred.

| Case | Sorted/single | Sorted/multi | Shuffled/single | Shuffled/multi |
| --- | ---: | ---: | ---: | ---: |
| scan_full | 1.443† | 1.511† | 1.402† | 1.815† |
| scan_narrow | 1.476 | 1.894† | 1.868† | 2.492 |
| range_0 | 0.725† | 0.711 | 1.221† | 0.224 |
| range_0.001 | 0.839† | 0.516† | 7.682 | 2.282 |
| range_0.01 | 1.016† | 1.065† | 4.202 | 3.302 |
| range_0.5 | 4.986 | 0.914† | 7.372 | 2.725 |
| range_1 | 5.589 | 1.647† | 3.979 | 1.566† |
| eq_0 | 0.362† | 0.334 | 0.682† | 0.798† |
| eq_0.001 | 0.797† | 0.454† | 3.286† | 2.023 |
| eq_0.01 | 0.817† | 0.579† | 3.069† | 2.128 |
| eq_0.5 | 2.475† | 1.673† | 5.038 | 2.627† |
| eq_1 | 3.384 | 2.643 | 2.655† | 1.518† |
| group_by | 2.006 | 1.540† | 2.083† | 1.478† |
| join | 1.754† | 1.901† | 1.888† | 0.817† |
