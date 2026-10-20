# Vortex Phase 5: bounded metadata planning

**Scope:** personal `AYUSHNSUT/Daft` fork. Branch `poc/vortex-phase5` starts at Phase 4 commit `e23bd0b1002d069a09cda84dfdb7e0d46fbb9799`; review target is `poc/vortex-phase4`.

**Status:** implementation and validation in progress. The [Phase 4 results](vortex-phase4-results.md) supply the baseline; the [connector guide](../connectors/vortex.md) defines supported inputs and semantics.

## Problem and objective

The Phase 4 MinIO queries take 24–48 ms longer through Vortex than Parquet, despite fewer returned execution bytes on projected queries. Sorted selective local queries also lose when little data is needed. These measurements suggest fixed planning/I/O overhead, but do not isolate its contribution.

Source inspection identifies a concrete serialization point: `VortexSource.get_tasks` synchronously reads one footer at a time. Each call obtains an I/O client/runtime and opens the file through configured Daft I/O. First-file schema inference and execution have their own opens. Footer counts are necessary: removing them would restore the large hash-build regression fixed in Phase 4.

This phase measures metadata cost and overlaps independent file metadata reads. The objective is lower end-to-end latency while retaining fresh cardinality, deterministic task order, schema validation, authentication, and bounded work. There is no guaranteed speedup or universal format ranking.

## Delivery steps

1. Add a native ordered bulk metadata reader using the existing I/O runtime and client. Bound active file opens rather than spawning a task for every path. Preserve the first error in input order and drop outstanding futures when the operation ends.
2. Add `metadata_concurrency=8` to `read_vortex`, accepting integers from 1 through 64. Materialize tasks in ordered windows of at most that many files. Refresh metadata on every materialization, including reused and pickled sources. Reuse the native client/runtime within each batch instead of initializing that path per file.
3. Return metadata-only diagnostics from the low-level bulk helper: elapsed time, file count, configured concurrency cap, and available Daft I/O GET/HEAD/LIST/returned-byte counters. The cap is not an observed peak. Counters describe that operation; they exclude discovery and execution, and are not physical network/device measurements.
4. Add an isolated comparison harness. Compare concurrency 1 with 8 in the same optimized build, using identical existing fixtures, independent result checks, interleaved trials, and fresh processes. Keep metadata-only and end-to-end query measurements separate.
5. Validate enabled/disabled builds, existing Vortex/Parquet/task/iterator behavior, and real authenticated HTTP/MinIO/Ray service acceptance. Publish results and remaining regressions, then open the personal-fork PR against Phase 4.

Remote acceptance also exposed a shared S3 transport problem: the HTTP client
created a new connector for each request, discarding its connection pool. Both
formats failed during bursts of Docker Desktop port-forwarding errors. Restore
SDK-managed connector reuse while retaining proxy, TLS and timeout settings;
test TCP reuse, distinct timeout pools, proxy routing and read timeouts. This
repair affects both readers, so the remote comparison must use a fresh build
and report, with no samples pooled from the failed attempt. It does not cache
file metadata or change retry settings. Preserve trial stderr/timestamps and
service diagnostics before cleanup; do not add pacing or retry failed trials.

Compatibility validation also found that strict shared HTTP range checks blocked
MCAP's existing whole-file fallback. Distinguish an ignored range (HTTP 200)
from a malformed partial response. MCAP may explicitly fetch and verify a full
buffer after its initial range probe, preserving indexed ordering, filters and
limits. Vortex continues to require validated partial responses. Malformed 206
responses and unrelated I/O errors must fail without a whole-file fallback.

## Data flow and resource bounds

The constructor continues to infer the full first-file schema. On each `get_tasks` invocation, the source partitions its resolved, deduplicated paths into bounded windows. A native batch runs independent footer reads concurrently with ordered results. Each returned count is associated with its original path and passed into the existing native scan-task metadata.

`metadata_concurrency` limits active metadata files and the source's window of returned schemas/counts. Existing per-reader request/byte budgets continue to apply to each open. Aggregate in-flight work can multiply by the number of active files; this is not a global RSS or query-wide byte cap. Metadata concurrency is independent of execution scan-task concurrency.

Execution still reopens files and validates the full schema/nullability before projection or `limit=0`. File discovery and its ordering/deduplication remain unchanged. The new reader shares configuration/client setup within a batch; it does not reuse a footer across distinct opens. Current I/O metadata does not supply a strong version identity or conditional range-read contract. URL/size caching could reuse stale offsets after same-size replacement, so cross-open footer caching requires a separate design.

Public source serialization includes the concurrency setting. Native tasks retain their existing wire representation, file-level counts, budgets, and worker-local execution behavior. No global planner, predicate, count-pushdown, or Arrow-conversion rules change in this phase.

## Tests and acceptance

| Area | Required evidence |
| --- | --- |
| Bulk metadata | Empty/nonempty and zero-column counts/schema; stable path/result ordering; strict concurrency/budget validation; available finite nonnegative diagnostics |
| Source planning | Bounded window sizes, exact count/path association, configuration forwarding, duplicate paths, and fresh counts across repeated/pickled materialization |
| Concurrency | Delayed authenticated HTTP files prove overlap at concurrency above one, sequential behavior at one, and the configured active-file bound; failures release owned work |
| Correctness | Existing joins still build the small dimension and probe fact rows; filters, limits, full schema/nullability checks, lifecycle and credential redaction remain correct |
| Builds | Rebuilt native extension, focused Rust tests in debug and release profiles, applicable Clippy/format/dependency checks, and feature-off import/query/enablement behavior |
| Services | Real HTTP/MinIO/Ray tests with zero service skips; controlled remote comparison uses the existing local MinIO image |
| HTTP compatibility | Vortex rejects ignored or misaligned ranges; small/large MCAP reads preserve ordering, filters and limits on servers without ranges; reject malformed partial responses and full-file size mismatches |
| Performance | At least 20 measured repetitions for the short targeted comparison plus warmups; preserve every sample, CPU/RSS, provenance, and regressions |

A failed metadata read must not create tasks with invented zero rows. Repeated planning must still observe replaced files. No background build/test load should overlap measured queries. Compare same-build concurrency settings before attributing changes to this implementation; keep Phase 4 measurements as a separately identified historical baseline.

## Evaluation and follow-up

Use the existing approximately 1 GiB local fixtures and smaller MinIO corpus. Start with sorted/shuffled and one/eight-file layouts, narrow projections, selective predicates, and joins; retain a wide-scan guard. Measure the full source-creation-through-collection interval and metadata-only time/counters separately. Increase repetitions or investigate outliers before claiming a repeatable gain.

Request merging, discovery fast paths, removal of exact residual predicates, layout-aligned execution splits, and conversion/encoding changes remain subsequent independently measured work. They should follow the remaining bottlenecks rather than be bundled into a metadata experiment.

Commit implementation, tests, the small harness and a concise results record. Keep generated data/logs/raw samples in ignored artifacts. Material AI assistance supports implementation, review, tests and documentation; record actual local, user-terminal and GitHub outcomes separately.
