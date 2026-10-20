# Vortex Phase 4: evaluation and supported fork feature

**Scope:** experimental `AYUSHNSUT/Daft` fork, branch `poc/vortex-phase4`, based on Phase 3 commit `e90b54bb2acd4eb42935c51474ce2c551c52d834`. Review base: `poc/vortex-phase3`.

**Status:** implementation and validation in progress. The [integration design](vortex-integration-design.md) and [test plan](vortex-integration-test-plan.md#benchmark-protocol) define the preceding phases and measurement contract.

## Questions and boundaries

Measure the complete implemented Daft reader paths: where does Vortex improve or regress relative to Parquet, which pushdowns explain the difference, and what CPU, memory, and storage costs accompany it? Both formats use the same Daft revision, release build, logical data, query, and result consumption. These results do not rank Daft against DataFusion or Velox.

The phase adds a reproducible evaluation harness, a results report, installation and connector documentation, and fork-specific automated coverage. It preserves the current reader API and supported primitive types. Writing, schema evolution, temporal/decimal/nested types, intra-file task splitting, global memory caps, and execution on compressed arrays remain separate work.

The existing TPC-H queries contain date predicates that this reader cannot load directly. Initial end-to-end workloads therefore use supported primitive scan, aggregate, and join shapes. Any adapted TPC-H workload must document its schema and query changes and be labelled adapted; no standard TPC-H result is claimed.

## Delivery sequence

1. Record this design, create `benchmarking/vortex/`, and implement deterministic batched fixture generation. Store equal logical rows in both formats, with a manifest containing writer settings, file boundaries, hashes, and independent expected results. Keep generated files outside version control.
2. Implement a correctness-checked runner and report generator. Provide a small smoke configuration and configurable larger runs; fail clearly on a non-release build for performance runs. Record exact commands, revision, source hashes, build identity, library versions, effective configuration, and machine information available to the process.
3. Validate the runner on small data, build the enabled release extension, and measure local datasets at approximately 1 GiB of logical data. Expand toward 10 GiB only where available resources and observed run costs justify it. Include both one-file and multiple-file layouts and ordered/shuffled filter distributions.
4. Run a selected remote subset through the accepted HTTP/S3-compatible paths when the environment permits services. Keep local and remote results separate. Ray correctness is already accepted in Phase 3; report any new Ray performance experiment separately from native reader measurements.
5. Add installation/API/connector navigation and an explicit support matrix. Add personal-fork CI for feature-on/off correctness and real service acceptance; large benchmarks are manual and have no shared-runner timing thresholds.
6. Run relevant existing regressions, contribution hooks, and feature-on/off checks. Publish the measured outcomes, failures, skips, and limitations. Open the personal-fork PR against Phase 3 after its acceptance gates have completed.

## Benchmark matrix

| Dimension | Required initial coverage |
| --- | --- |
| Reader | Native Parquet; native Vortex with neither pushdown, projection only, filter only, and both |
| Data | Repetitive integers, low/high-cardinality strings, and nullable primitives |
| Ordering | Pruning-friendly ordered data and shuffled data |
| Files | One file and matching multiple-file row boundaries |
| Queries | Full value consumption, narrow projection, integer equality/range filters, grouped reduction, and a join |
| Filter selectivity | 0%, 0.1%, 1%, 50%, and 100%, with actual selected rows recorded |
| Scale | Small correctness fixtures, then approximately 1 GiB logical data; larger runs explicitly identified |
| Storage | Local baseline; selected HTTP/S3-compatible experiments reported independently |

Parquet keeps its normal optimizations. Writer defaults and codec settings are recorded, not adjusted to force equal compressed sizes. Vortex schedules one task per file while Parquet can schedule row groups: use an explicitly controlled comparison for reader isolation and retain the normal-settings comparison so this limitation remains visible.

## Measurement contract

- Generate, encode, and check fixtures outside query timers. Record generation/write cost separately.
- Create a fresh source and query for every sample. The primary timer starts before discovery/schema/planning and ends after full result consumption. Imports and process startup are excluded and identified separately.
- Use value reductions or drain native batches; neither metadata-only counts nor terminal/Python object rendering measure scan throughput.
- Run one warmup and at least five measured trials per reported configuration, with a deterministic randomized/interleaved order. Preserve individual samples, median, spread, failures, and regressions.
- Validate results against an independent oracle outside the timer. Compare integer results exactly and document any floating-point tolerance before measuring. Save optimized/physical plans outside the timing interval.
- Report wall time, process CPU, peak process RSS with its measurement scope, encoded file sizes, and available execution-source returned-byte counters. Public execution metrics do not expose request counts; report them as unavailable rather than zero. Planning/discovery reads are included in wall time but may be absent from execution-source counters. These counters are not physical device I/O measurements.
- Identify OS cache state honestly. Repeated runs are warm-cache experiments; a fresh process alone does not establish a cold OS cache. Keep profiling separate from benchmark timings.
- Record CPU/thread budgets, Vortex request/byte budgets, batch size, file count, hardware, storage path class, and cache policy. Daft compute threads and concurrent scan tasks can be constrained, but separate I/O/Vortex workers mean this is not a globally single-core experiment. Do not publish credentials or signed URLs in artifacts.

## Tests and acceptance

Small tests cover deterministic generation and manifests, independent result validation, query/configuration selection, sample ordering, release-build enforcement, failure reporting, aggregation, and CLI smoke execution. Deliberately mismatched results or changed fixture bytes must fail validation rather than produce a performance claim.

Re-run the existing Vortex reader/task/lifecycle/pushdown suites and relevant ordinary Parquet/native-task/iterator/count regressions. Rebuild without the optional feature to verify normal imports/queries and actionable reader enablement errors, then restore the enabled release build. Run public API examples, documentation checks available locally, and applicable contribution hooks. Shared code unchanged in this phase inherits Phase 3's focused native and real service evidence; additional testing is reported with exact counts and scope.

The phase is complete when the supported local matrix has reproducible correct release-build results with resource measurements, the selected service checks have explicit outcomes, documented supported behavior has automated coverage, and the report exposes regressions and remaining limitations. There is no minimum speedup gate. Any unavailable environment-dependent acceptance remains pending and must be disclosed before opening the completed review PR.

## Review artifacts

Commit the harness, small tests, workflow, documentation, and a compact human-readable validation report. Keep large generated data, binary wheels, detailed logs, raw timing samples, and profiler output in ignored artifact directories. Record artifact paths, reproduction commands, checksums, and the measured source identity in the report so the summary can be audited and regenerated.

Material AI assistance is used for implementation, tests, review, and documentation. The results report distinguishes checks run by the agent, checks run in the user's terminal, and unexecuted configurations.
