# Vortex performance improvements and investigation worklist

Updated **21 October 2026** against source `42c380f60`. This is a proposed worklist,
not a record of implemented optimizations or new benchmark runs. The maintained
[Parquet/Vortex comparison](parquet-vs-vortex.md) remains the results reference.

The immediate objective is to close the five measured losses while preserving
the existing local wins. Broader coverage must establish where those conclusions
continue to hold. An observed span is an investigation target, not a prediction
that its entire duration can be removed.

## The gaps to close

These are arithmetic differences between the dedicated comparison medians.
The percentage denominator is **current Vortex time**, so these percentages
differ from how much slower Vortex is relative to Parquet. A tie is not a win.

| Workload | Time Vortex must save to tie Parquet | Share of Vortex latency |
| --- | ---: | ---: |
| Local sorted/eight-file range, 1% | 1.346 ms | 25.65% |
| Local sorted/eight-file equality, 1% | 1.944 ms | 38.17% |
| MinIO range, 1% | 1.141 ms | 9.28% |
| MinIO equality, 1% | 2.037 ms | 15.52% |
| MinIO full reduction | 5.625 ms | 3.30% |

The strongest clues from existing evidence are:

- **Selective scans have substantial setup overhead.** Separate local range
  captures show source creation of about 0.536 ms for Parquet versus 1.195 ms
  for Vortex, and logical planning of 0.228 versus 1.349 ms. Executor-next
  occupancy is 3.001 versus 2.469 ms. These are three-capture medians, not the
  uninstrumented performance samples; their differences are not additive.
- **Selective Vortex queries use more client CPU despite fewer execution bytes.**
  Local range CPU medians are 8.093 ms for Parquet versus 15.820 ms for Vortex;
  MinIO range is 8.099 versus 16.979 ms. MinIO selective Vortex reads return
  232,756 execution-source bytes versus about 1,337,700 for Parquet. Those byte
  counters exclude separate discovery/planning and physical transport overhead.
- **Full remote scans need a different explanation.** Vortex uses less client
  CPU, 165.500 versus 196.903 ms, yet has higher wall latency. The approximately
  147 ms response-body occupancy includes consumer/scheduler delay and overlaps
  decoding. It does not establish 147 ms of physical network transfer.
- **Execution session setup remains.** A local selective capture has about
  0.98 ms execution-session occupancy after planning sessions were shared.
- **Conversion remains substantial for wide scans.** A representative 159 ms
  local full reduction has about 46 ms of Arrow-conversion occupancy, whereas
  a selective output has about 0.002 ms. These are overlapping intervals,
  not exclusive CPU costs or guaranteed savings.

The remote request audit is identical across three captures of each workload:

| Query | Parquet HEAD | Vortex HEAD | Parquet GET | Vortex GET |
| --- | ---: | ---: | ---: | ---: |
| Narrow scan | 17 | 25 | 25 | 25 |
| Range, 1% | 17 | 25 | 10 | 11 |
| Equality, 1% | 17 | 25 | 11 | 11 |
| Join | 17 | 25 | 25 | 33 |
| Full reduction | 17 | 25 | 25 | 121 |

These are instrumented SDK request-send envelopes, **not physical retry attempts
or TCP connections**. For Vortex selective scans, discovery performs eight
HEADs, schema inference one, planning eight, and execution eight. Schema and
planning fetch nine 65,535-byte opening ranges; execution reuses eight cached
opening ranges. Only one of eight files reads data: two ranges. Full reduction
reads 112 data ranges across the eight files.

## What is already present

Do not repeat these as new implementation work: persistent I/O clients and S3
connection pools; bounded parallel discovery/metadata reads; bounded range
coalescing; validated S3 planning-to-execution opening-byte reuse; shared planning
sessions; projection and conservative filter pushdown; fresh source construction
inside the timer; balanced adjacent A/B order; independent output oracles; and
bounded shared-clock profiles for both readers.

The next work concerns remaining duplication, execution setup, missing
attribution and workload coverage. The existing request-cap sweep did not find
a universally better cap. Lightweight planning was disabled in that sweep.

## Priorities and effort

**P0** is required measurement/acceptance infrastructure. **P1** is the first
set of focused experiments. **P2** depends on profiling or expands coverage.
**P3** is a larger architectural direction. **S** means a contained change or
experiment, **M** coordinated changes within a subsystem, **L** changes across
lifecycles/subsystems, and **XL** a substantial execution architecture change.
These are relative effort estimates, not delivery or speedup promises.

### Measurement and acceptance

| ID | Priority / effort | Work and concrete deliverable |
| --- | --- | --- |
| M1 | P0 / M | **Freeze the actual candidate in final acceptance.** Extend the selection schema to record every reader option, including row-count planning, metadata reuse, batch size, request limits and future split/runtime options. The current final helper only selects older coalescing/discovery policies and omits row-count planning. Test configuration roundtrips, exact executed options, unknown/missing option rejection and source/native/fixture identity; preserve the existing thresholds. |
| M2 | P0 / S–M | **Measure run variability and drift.** Keep the existing balanced adjacent pairs; add a predeclared number of independent persistent sessions and schedule seeds, order/time-block diagnostics, and host/Docker activity records. Investigate the approximately 11% identical-control equality difference. Use uncertainty estimates that respect paired observations and session dependence; never subtract the control percentage from candidate results. |
| M3 | P0 / M | **Profile spawned CPU work and scheduler delay.** Sample every Daft/Vortex/Tokio worker thread and add bounded task enqueue/start/end, prepare/decode/filter/export counters with query/file/task correlation. Consumer `try_next()` polling does not include CPU work in spawned tasks. Keep synchronous work, CPU samples, queue delay and wall intervals distinct; retain errors/cancellation and measure profiler overhead separately. |
| M4 | P0 / M | **Measure allocation, copy volume and live memory.** Attribute allocation counts/bytes, offset widening, snapshots, aligned I/O buffers and retained Arrow/Daft buffers. Distinguish allocator live bytes, sampled RSS, process lifetime high-water and encoded snapshot budgets. Shared buffers must not be counted twice. Include a slow consumer and cancellation to expose retention. |
| M5 | P0 / M | **Complete the I/O and server ledger.** Correlate discovery/schema/planning/execution requests, validators, cache-hit reasons and range identities. Distinguish requested, returned, unique and duplicate bytes, SDK sends and actual attempts/retries, reused/new connections, first byte, body progress and consumer progress where observable. Pair client traces with MinIO/Docker CPU, storage and request metrics. Keep unsupported transport metrics explicitly unavailable. |
| M6 | P1 / S–M | **Make the comparison resource-complete.** Surface current client CPU medians and valid byte coverage alongside timings. Add equivalent local Parquet positional-read accounting before comparing read bytes. Report first-batch latency and latency spread separately from full query latency. Verify diagnostics do not contaminate timed trials; never sum overlapping stage spans into a fabricated CPU/network breakdown. |

### Source creation, metadata and planning

| ID | Priority / effort | Work and concrete deliverable |
| --- | --- | --- |
| S1 | P1 / M | **Share execution-session initialization within a fresh scan processor.** Extend the existing planning-only reuse through an execution context. Keep separate per-file readers, footers, validators and scan state; serialize configuration, not live sessions. Measure creation counts and CPU for 1/8/many files. Test concurrent queries, repeated collection, empty input, mixed schemas, cancellation and Ray serialization. |
| S2 | P1 / M–L | **Fuse discovery with native opening where freshness permits.** Discovery currently discards size information when it returns path strings; native opening asks again. Investigate a bounded object descriptor with strong identity and provenance, not just a cached size. Compare exact paths/lists with directory/glob discovery. Mutation, delayed/repeated collection, missing files and deterministic error ordering must retain their contracts. |
| S3 | P1 / M–L | **Reuse the first schema open for planning.** Schema inference discards row count/opening bytes, then planning fetches the first footer again. Evaluate a materialization-scoped validated descriptor/snapshot, or an explicit-schema API for callers that can supply the complete source schema. Preserve source nullability, validation after object replacement, delayed collection and worker boundaries. Measure actual eliminated calls; do not turn source construction into a long-lived metadata cache. |
| S4 | P1 / M | **Prune a whole file before constructing its SDK scan.** The pinned SDK exposes `VortexFile::can_prune`; our reader does not call it. Test it after full schema validation using a safely translated predicate. The SDK already prunes data later, so the incremental benefit may be only layout/scan preparation. Missing/inexact stats, nulls and unsupported predicates must fall back; projected-away schema mismatches must still fail. |
| S5 | P2 / L | **Move proven no-match decisions into planning.** Planning already reads all footers. Investigate passing conservative statistics or a no-match proof to a validation-only execution task, avoiding full session/layout setup for irrelevant files. Preserve fresh identity checks and full schema-error behavior. This is separate from S4 and cannot simply delete the seven execution HEADs for filtered-out files. |
| S6 | P2 / L | **Make cardinality work demand-driven and improve useful statistics.** Determine which simple queries require all-file exact counts before execution. Explore deferred counts or reused fresh planning metadata, while retaining exact/approximate/unknown distinctions and reliable join sizing. Vortex tasks currently reject column statistics. Never restore unknown-as-zero estimates or drop footer counts globally: that caused the original poor join build-side choice. Test filtered/skewed joins and task memory estimates. |
| S7 | P2 / M | **Pipeline metadata beyond fixed windows.** Current Python batches and native ordered buffering can hold later work behind a slow file. Compare bounded continuous admission with 8/64/512 files and injected metadata skew. Preserve total concurrency, input order, first-error rules, cancellation and snapshot capacity. There is no demonstrated benefit for today's eight-file fixture. |
| S8 | P2 / M–L | **Reuse parsed footer/schema work if it is material.** Encoded opening bytes are reused today, but parsing/layout/schema construction still repeats. First isolate those costs. Compare query-local parsed-footer/schema reuse with encoded fallback for distributed tasks. Audit session compatibility, nullability, mutation and memory lifetimes, including whether the SDK's `with_footer` path preserves the initial segment cache. |

### Decoding, conversion and scheduling

| ID | Priority / effort | Work and concrete deliverable |
| --- | --- | --- |
| D1 | P1 / S–M | **Request `LargeUtf8`/`LargeBinary` directly.** The reader requests small offsets and Daft then widens them. The pinned SDK can build the requested offset type directly. Measure SDK export plus Daft conversion, allocation/copy bytes and total query time; merely moving the cast is not a win. Test Unicode, binary, long/empty/all-null values, sliced arrays, many batches, projections and filters/limits. Expect relevance to wide scans, not tiny selective outputs. |
| D2 | P2 / S–M | **Avoid repeated batch-schema and wrapper allocation.** `RecordBatch::try_from` converts the Arrow schema for each batch. Investigate retaining the validated projected schema per stream and using an appropriate existing constructor. Measure first on small batches/wide schemas; keep no-column cardinality, metadata/nullability checks and malformed-batch errors. SDK execution-context reuse is a separate lifetime/concurrency question. |
| D3 | P1 / S–M | **Compare layout-aligned and fixed-row splits.** The reader forces `SplitBy::RowCount`; the pinned SDK supports layout sub-splitting with a maximum row count. Hold the row bound constant and measure repeated segment reads, decode work, first batch, RSS and full latency. Test uneven chunks and very wide rows; do not switch to unbounded whole-layout batches. |
| D4 | P2 / L | **Separate CPU scheduling from I/O only if profiles justify it.** The pinned Tokio executor runs `spawn_cpu` closures on Tokio tasks. Investigate runnable delays and body-poll stalls under decode load, then compare a bounded CPU/runtime adapter with the existing path. Preserve panic/error propagation, cancellation, worker budgets and progress; avoid an unbounded blocking-pool substitution. |
| D5 | P2 / M | **Tune file × split × request parallelism together.** SDK concurrency multiplies by worker count; `.with_concurrency(1)` is not one split globally. After M3/M4, use a small predefined grid with fixed resource ceilings and held-out confirmation. Include one large file, many files, wide batches and simultaneous queries. A request-cap sweep alone cannot diagnose decoder oversubscription. |
| D6 | P2 / M–L | **Elide only proven redundant residual work.** The full Daft predicate and filter-only columns are retained even after pushdown. Profile their cost first; then represent exact pushed conjuncts separately from fallback residuals. Validate null/three-valued logic, integer bounds, partial AND, reordered columns and filter-before-limit. Existing tiny filtered outputs suggest limited benefit for the current selective losses. |
| D7 | P2 / S–M | **Push limits into the SDK only without predicates.** The pinned SDK supports a limit but rejects simultaneous filter-plus-limit. Preserve full schema validation and the outer exact/global limit. Test 0/1/batch boundaries, multiple files, empty projection, ordering, above-file limits, and prompt I/O/decode cancellation. This benefits preview/head workloads absent from the current core matrix. |
| D8 | P2 / M–L | **Add exact metadata-only counts where semantics allow.** Investigate the existing aggregation-pushdown interface for unfiltered count-star. Count-column, nulls, filters, limits, schema errors and mutation need separate treatment. This is additional query coverage; it must not substitute a metadata query for the current value-consuming full reduction. |

### I/O, buffering and cache behavior

| ID | Priority / effort | Work and concrete deliverable |
| --- | --- | --- |
| I1 | P1 / S–M | **Explain full-scan range amplification and retest coalescing.** Map the 112 Vortex data ranges to physical layouts and `read_ranges` calls. Existing coalescing cannot merge ranges arriving in different calls. Compare adjacent-only merging, bounded cross-call windows and gap policies under controlled delay/bandwidth. Track useful/extra bytes, allocations, first batch and complete latency. Coalescing already exists and prior tuning disabled it; do not enable it globally without new evidence. |
| I2 | P2 / L | **Bound query-wide resources and fairness.** Per-reader request/byte limits multiply across files and exclude decoded buffers. Evaluate query/worker-wide ceilings for requests, live splits and decoded data under multi-query load. Track throughput, p95/p99, memory, fairness and cancellation. Existing permit waits are tiny; raising limits is not currently a supported optimization. |
| I3 | P2 / M | **Reduce adapter and snapshot copies.** Instrument streamed-body assembly, cached-range `to_vec`, snapshot capture/serialization and alignment repairs. Try aligned single-chunk adoption or reference-counted slices where ownership permits. Keep exact-length/partial-response validation and alignment; check that a small slice does not retain a huge parent allocation. |
| I4 | P2 / M–L | **Retain a local file handle for positioned reads.** The current generic path stats each range and opens the file again during collection. Compare a fresh per-reader handle/fstat with bounded positioned reads and syscall counts. No shared seek cursor. Define path replacement/truncation/symlink behavior and retain cancellation, alignment, error handling and platform compatibility. |
| I5 | P2 / M–L | **Investigate opening-read size with the actual SDK constraint.** For files at least that large, the pinned opener reads at least 65,535 bytes; smaller files cap the read at their size. A smaller local knob will not reduce the current fixture's opening reads. Measure real footer/postscript distributions before proposing suffix-first/two-stage opening or larger initial reads. Weigh bytes against extra round trips; test tiny/truncated/malformed files and limits. This may need an SDK/opening-path change. |
| I6 | P2 / L | **Extend metadata reuse beyond S3 with strong identity semantics.** HTTP needs strong validators and conditional range requests; local files need a defined identity/mutation model. Test weak/missing ETags, redirects, authentication, range-ignore servers, partial responses and in-place mutation. Equal size is not a safe identity. Unsupported cases retain fresh-read fallback. |
| I7 | P2 / M | **Improve snapshot admission under pressure.** Failed partial captures can consume conservative reservations without returning capacity; completion order can affect admission. Measure useful-hit rate, temporary copies and serialized/live bytes with hundreds of files and skewed footer sizes. Consider deterministic admission or safe reclamation only if worthwhile; no racing budget undercount or unbounded cache. |

### Broader query and engine opportunities

| ID | Priority / effort | Work and concrete deliverable |
| --- | --- | --- |
| Q1 | P2 / M–L per family | **Expand exact predicate coverage.** Current translation supports conservative integer/Boolean comparisons, direct-column null/not-null checks and safe conjunctions. Evaluate demanded string comparisons, IN, OR/NOT and coercion families individually. Prove equivalence and use differential/property tests for nulls, NaN/signed zero where relevant, bounds and unsupported subtrees. New expression coverage does not automatically accelerate the current integer cases. |
| Q2 | P2 / L | **Partition giant or skewed files into bounded engine tasks.** One file currently becomes one Daft source task, though the SDK has internal split parallelism. Investigate natural splits for distributed scheduling without repeated footer opens per tiny task. Test disjoint/exhaustive row coverage, limits, ordering, retries, fresh validators and skew. Local benefit is not established. |
| Q3 | P2 / M–L | **Profile common-engine reductions and joins on in-memory data.** Investigate Unicode length, allocation of intermediate expressions, aggregate fusion, nullable kernels, hashing and build/probe buffers. The string-length query counts Unicode characters; replacing it with byte length changes semantics. Compare both formats after any common-engine change and keep shared-engine gains distinct from reader gains. |
| Q4 | P3 / XL | **Dynamic join filters and later materialization.** Explore propagating safe build-side filters to probe scans, with explicit join-type legality, distributed updates and cancellation. Measure selective joins with realistic dimensions/skew. Current mostly favorable join results do not make this the first target. |
| Q5 | P3 / XL | **Preserve view/encoded arrays deeper into Daft.** This requires changes to physical types, kernels, joins/grouping, ownership, serialization and Python/Ray boundaries. Prototype one operator with explicit conversion fallback and buffer-level evidence. Direct large-offset export is the smaller first experiment; encoded execution is a separate architecture project. |
| Q6 | P2 / M–L | **Evaluate dependency and type coverage changes separately.** Audit a pinned Vortex/Arrow upgrade for relevant fixes/APIs only when needed, with build, semantics and performance comparisons. Nested, decimal and temporal support can enable more representative workloads but expands the reader contract. Do not mix a dependency/type expansion with a small optimization and attribute the whole result to it. |

### Evaluation coverage and promotion

| ID | Priority / effort | Work and concrete deliverable |
| --- | --- | --- |
| V1 | P1 / S–M | **Restore the full existing query matrix.** Rerun group-by and 0%, 0.1%, 1%, 50%, 100% range/equality cases on all four layouts. Earlier empty/selective cases sometimes lose badly. Add limits and wide/narrow projections with complete oracles; do not make the five current losses the only regression tests. |
| V2 | P1 / S–M | **Compare equal corpora and separate lifetime costs.** Run identical files/rows locally and through MinIO. Current local and remote corpora differ eightfold in rows. Preserve persistent clients plus fresh source/query as the primary mode; separately measure first application query, idle/expired connections and any source-reuse diagnostic. Verified cold-cache claims require actual cache-state evidence. |
| V3 | P2 / M | **Broaden data and writer/layout sensitivity.** Add held-out seeds, genuinely random permutations, independent equality keys, cardinality/null/skew variation, long/short Unicode and binary values, and compressible/incompressible data. Existing shuffled data uses an affine permutation and many columns derive from IDs. Vary file/chunk/row-group sizes and writer settings with identical logical inputs; report size/write cost separately and do not present per-case tuned maxima as defaults. |
| V4 | P1 / M | **Test controlled network conditions.** Use local authenticated HTTP/MinIO with bounded response delay, bandwidth and TLS conditions. Delay injection is not the same as actual WAN RTT. Capture client and server resources together. Hosted storage can be a later separately scoped experiment; the immediate investigations need no cloud service. |
| V5 | P2 / L | **Test scale, resource limits and platforms.** Sweep file counts and working sets beyond cache; include wide rows, skew, slow consumers, concurrent queries, long-lived workers and memory pressure. Then verify Linux/other hardware and Ray scale when local evidence warrants it. The current single active process-local profiler needs isolation changes before per-query multi-query traces can be trusted. |
| V6 | P0 before promotion / M | **Run independent held-out acceptance on one frozen candidate.** Preserve 50 measured rounds/three warmups, one candidate below Parquet in each original remote case, and each original local case at most 1.05× its declared Vortex baseline. Use explicit persistent mode and M1's complete option schema. Extra coverage supplements these gates. Retain every failure and update the single comparison file with all outcomes; do not retune on the final samples. |

## Recommended execution order

1. **Make the next result trustworthy:** M1 and M2, plus targeted M3–M5 captures
   of local selective queries and remote full reduction. Extend the request
   audit rather than rerunning unchanged benchmarks until a pass appears.
2. **Run independent contained candidates:** D1 (direct large offsets), S1
   (execution sessions), S4 (post-validation file pruning), and D3 (split alignment).
   Give each its own switch/control and correctness tests. Numeric narrow scans
   are a useful negative control for D1; one-file scans are a control for S1.
3. **Address the measured remote setup and range costs:** S2/S3 with an explicit
   freshness design, and I1 with range/copy/server evidence. Escalate to S5/S6 or
   D4 only if simpler changes leave material costs and profiles support them.
4. **Validate interactions, then freeze:** combine only independently useful
   changes, test interactions and resource budgets, run V1/V2/V4, and perform V6
   on independent samples. Keep Q4/Q5 separate from this iteration.

Do not promise a 46 ms gain from the conversion span, an eight-HEAD saving by
removing validation, or a universal concurrency setting. Exact end-to-end
benefits must be measured. In particular, metadata work alone is unlikely to
explain the complete full-reduction cost, and conversion is negligible for the
current tiny filtered outputs.

## Required correctness and process gates

- Preserve equal logical inputs and full-output oracles for Parquet/Vortex.
  Include nulls, empty files, signed/unsigned bounds, Unicode/binary, projection
  reordering, zero columns, supported/fallback predicates, and filter/limit order.
- For metadata/open changes: replacement between construction/planning/execution,
  changed credentials, weak/missing validators, conditional-read failure,
  repeated collection and full schema validation of projected-away fields.
- For lifecycles: native and Ray, error/panic/cancellation, slow consumers,
  partial/short responses, multiple queries, snapshot/byte budgets and no leaked
  handles or tasks. Extend existing focused tests instead of duplicating them.
- Rebuild after native edits. Run the relevant local suites and lint checks
  before pushing; shared I/O/engine changes also require appropriate compatibility
  and workspace checks. Finish live HTTP/MinIO/Ray checks where sockets/Docker
  are available. No builds or test suites may overlap performance measurements.
- Freeze source/native/harness/configuration/fixture identities, preserve raw
  samples and failed runs, keep profiling separate from timing, and update
  [the maintained comparison](parquet-vs-vortex.md) only with accurately scoped
  completed evidence. Defaults stay experimental until their promotion is justified.

## Implementation and evidence anchors

| Area | Current source |
| --- | --- |
| Discovery, schema inference, metadata windows and task construction | [`daft/io/_vortex.py`](../../daft/io/_vortex.py), `VortexSource.__init__` / `get_tasks` |
| Sessions, export schemas, metadata, full-schema validation, scans and residual limits | [`read.rs`](../../src/daft-vortex/src/read.rs), `session_for_open`, `schemas`, `stream_vortex`, `stream_opened_vortex` |
| Safe predicate translation and required columns | [`plan.rs`](../../src/daft-vortex/src/plan.rs), `translate_filter` / `plan_scan` |
| Range merging, permits, cached reads and aligned body assembly | [`io.rs`](../../src/daft-vortex/src/io.rs), `read_ranges` / `collect_exact` |
| Serialized opening bytes and admission | [`snapshot.rs`](../../src/daft-vortex/src/snapshot.rs) |
| Execution context and per-file dispatch | [`scan_task.rs`](../../src/daft-local-execution/src/sources/scan_task.rs), [`scan_task_reader.rs`](../../src/daft-local-execution/src/sources/scan_task_reader.rs) |
| Type widening and per-batch conversion | [`array/mod.rs`](../../src/daft-core/src/array/mod.rs), [`daft-recordbatch`](../../src/daft-recordbatch/src/lib.rs) |
| Local reads, S3 clients and transport | [`local.rs`](../../src/daft-io/src/local.rs), [`s3_like.rs`](../../src/daft-io/src/s3_like.rs) |
| Shared profiler and wall-time accounting | [`query_profile.rs`](../../src/common/runtime/src/query_profile.rs), [`profile.py`](../../benchmarking/vortex/profile.py) |
| Frozen selection and current exploratory comparisons | [`remote_eval.py`](../../benchmarking/vortex/remote_eval.py), [`phase7.py`](../../benchmarking/vortex/phase7.py) |

Pinned SDK **0.88.0** source was inspected in the local Cargo registry:
`vortex-file/src/file.rs` (`can_prune`, `file_stats`, `footer`, `splits`),
`vortex-file/src/open.rs` (`with_footer`, minimum initial read),
`vortex-layout/src/scan/{scan_builder,split_by,arrow}.rs` (spawned tasks, limits,
worker concurrency, layout sub-splitting and export),
`vortex-io/src/runtime/tokio.rs` (`spawn_cpu`), and
`vortex-arrow/src/executor/byte.rs` (offset builders). These observations concern
the pinned implementation, not an assumption about the latest upstream release.

Numerical evidence comes from reports L and R, with their source/native/report
hashes recorded in [the comparison's evidence section](parquet-vs-vortex.md#evidence).
Request counts use all three candidate/Parquet captures per remote workload;
CPU values use the dedicated uninstrumented comparison samples. Source and
planner timings are separate three-capture medians. Keep those evidence types
distinct when assessing an experiment.
