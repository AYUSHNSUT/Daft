# Vortex Phase 6: remote query latency

**Scope:** experimental personal `AYUSHNSUT/Daft` fork. The implementation starts
on `poc/vortex-phase6` at Phase 5 commit
`a6d60cdc6635111b158185e8506700986f95cb9c`; its first review targets
`poc/vortex-phase5`. Status: implementation and validation in progress.

The [first acceptance results](vortex-phase6-results.md) miss the remote median
gate. Correctness, local regression guards, ORC/MCAP, and the unfiltered Rust
workspace pass; the remaining work is documented with that failed run.

The [Phase 5 results](vortex-phase5-results.md) leave Vortex median query latency
approximately 0.8–12 ms above Parquet on the local authenticated MinIO corpus.
Projected queries already return fewer execution-source bytes. Request/setup
overhead and selective-query CPU are hypotheses to measure, not established
causal explanations of the entire gap.

## Completion criterion

The same frozen Vortex configuration must have a lower median than contemporary
Parquet in all five existing remote cases: narrow scan, 1% range, 1% equality,
join and full scan. Final acceptance uses 50 measured rounds and three warmups
per case/configuration, fresh child processes, and seeded interleaving. Record
all samples, independent oracle results, plans, CPU, RSS, bytes, errors and exact
source/binary/fixture identity. Caches remain retained.

The fork owner selected median wins in one final run as the gate. Sample ranges
and paired win counts remain visible but are not significance gates. This is
acceptance for the declared native-runner MinIO corpus, not a universal format
ranking or public-cloud result. No failed trials may be omitted or retried into
passing acceptance.

Start with diagnostics, bounded remote read merging and parallel discovery. If
those changes miss the gate, keep Phase 6 open and implement separately reviewed
follow-ups for version-safe metadata reuse and selective-query CPU work. Do not
silently expand the storage/type matrix or fall back to another file format.

## First implementation

Add keyword-only experimental reader settings:

| Setting | Default | Contract |
| --- | ---: | --- |
| `enable_read_coalescing` | `True` | Strict boolean; merge remote segment reads only |
| `read_coalesce_gap_bytes` | `65536` | Nonnegative strict integer; zero allows contiguous reads |
| `read_coalesce_max_bytes` | `4194304` | Positive strict integer; bound the actual merged span |
| `discovery_concurrency` | `8` | Strict integer from 1 through 64; one reproduces serial discovery |

Validate before I/O even when merging is disabled. Carry the three read settings
through source configuration, native task serialization and worker execution.
Keep local-file coalescing disabled. Existing metadata concurrency, scan
concurrency, schema checks, count estimates and complete residual filtering are
unchanged in the first implementation. Workers require the matching rebuilt
experimental binary; no persisted native-plan compatibility is promised.

### Bounded remote read merging

Use the pinned Vortex 0.88 positional-reader batch API to merge actual requested
spans. Keep its automatic coalescing configuration disabled: that implementation
aligns starts down using the global segment alignment, including for an
individual segment, which can push a previously valid request over its budget.
Join nearby requests only when their actual contiguous span, including gaps,
fits both the configured merge cap and the existing request-byte budget. Split
the returned buffer into correctly aligned results for each original request.
Individual requests retain their existing bounds and error behavior, including
requests larger than the merge cap. Group futures retain the existing request
and byte semaphores and are owned by the returned stream, not detached tasks.

Merging affects data-segment requests, not HEAD requests or the standard initial
65,535-byte footer read. Do not predict footer-count savings from this change.
Retain strict range-response/length checks, credential redaction, and cancellation
of owned work. Request permits bound active fetches, not returned buffers or
global query memory. Report additional returned bytes and RSS explicitly.

### Ordered parallel discovery

Add a thin native bulk-glob operation using the existing Daft I/O client and
runtime. Bound submitted work and preserve ordered per-input outcomes rather
than raising the first completion-order error. Python applies existing directory
fallback, literal URL escaping, first-error behavior, normalization and
deduplication in original input order. An earlier input's directory fallback
must finish before a later input's failure is surfaced.

Remote inputs may overlap; local discovery behavior remains unchanged. Do not
create a thread pool per query, trust a discovered file size as an immutable
identity, or cache footer/count results across materializations.

### Diagnostics

Provide a private opt-in native-runner diagnostic collector for the evaluation
harness. Capture exactly one query, from before discovery through collection;
stop before oracle validation and the separately created explain query. Record
discovery, schema, planning-metadata, execution-open, first-batch and completion
timing, plus logical I/O operations, requested/returned range sizes, header/body
duration and permit waits where observable. Include conversion and residual
filter timing for later attribution.

No collector, event timing or event allocation is enabled in normal performance
trials. Diagnostic events use sequential file identifiers rather than URLs or
credentials, have a finite event cap, and expose dropped-event counts. Logical
operations are not packet counts; retries unavailable to a particular collector
must remain explicitly unavailable. Nested diagnostic captures fail clearly.
Keep diagnostics private rather than changing public query metrics.

## Evaluation and follow-ups

The same-build baseline explicitly sets read merging off, discovery concurrency
one, and metadata concurrency eight. Compare it with contemporary Parquet and
one candidate across every case. Tuning is separate from final acceptance:
merging off or gap/max pairs 16 KiB/1 MiB, 64 KiB/4 MiB and 256 KiB/4 MiB,
combined with discovery concurrency one/eight. Use at least 20 measured rounds
and one warmup for tuning. Select the policy minimizing the worst Vortex/Parquet
median ratio across the five cases; break ties by geometric mean elapsed time,
then fewer returned bytes. Freeze that policy before final acceptance.

Use local sorted/shuffled and one/eight-file controls. Investigate any local
median regression above 5%; do not claim the default is ready with an unexplained
guard regression. Add controlled HTTP response-delay checks at 0, 5 and 20 ms
for request shape and concurrency; label these separately from real MinIO.

If the gate remains unmet, follow with:

1. Query-scoped validated footer reuse: carry only serialized metadata and
   validators, not handles. Require fresh object identity validation and
   conditional/versioned S3 reads. Inputs without suitable validators keep the
   fresh-open path. Refresh materialization counts; abort on replacement during
   execution. Limit retained serialized footers to 64 MiB per query and bypass
   reuse above the cap.
2. Selective-query CPU: measure decode, Arrow conversion and residual filtering;
   remove duplicate filtering only for predicates proven exact by differential
   tests. Unsupported predicates retain their residuals. Test scan concurrency
   one/two/four with existing ordering and I/O budgets, and choose one policy for
   all five queries.

Each follow-up retains the original fixture identities and its own build/report
provenance. Measurements from different builds are never pooled.

## Test and publication gates

Native tests cover merged slices, overlap/adjacency, alignment at nonzero offsets,
tight byte budgets, malformed/short ranges, errors and cancellation. Discovery
tests prove bounded overlap and deterministic results/errors with barriers rather
than speed thresholds, including mixed inputs, signed URLs, directory fallbacks,
duplicate paths and credentials. Existing mutation, schema, join-cardinality,
serialization, zero-column, limit and residual semantics remain required.

Rebuild feature-on and feature-off binaries. Run focused debug/release Rust,
Python/core regressions, ORC/MCAP compatibility, real HTTP/MinIO/Ray acceptance,
applicable contribution hooks, all-feature/all-target Clippy and documentation
builds. Keep feature/service skips explicit; enabled service acceptance requires
zero skips. Run the full unfiltered Rust workspace in an unrestricted terminal
when the agent sandbox cannot exercise sockets or network services.

Complete available local checks before pushing or triggering Actions. Provide a
single terminal acceptance command for environment-dependent checks and verify
its saved exit markers, source/binary identities and complete results. Preserve
user files and previous reports. Open personal-fork draft PRs only; Linux CI is a
separate outcome. Material AI assistance is disclosed with actual verification.
