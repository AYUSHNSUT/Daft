# Vortex Phase 3: Daft I/O, resource controls, and workers

**Scope:** experimental `AYUSHNSUT/Daft` fork only.

**Branch:** `poc/vortex-phase3`, based on completed Phase 2 commit `c7c8324a05cc475c5bace6c85c28722375cb81da`.

**Review base:** `poc/vortex-phase2`. Keep this work separate from earlier phases and upstream. Open the fork review PR after implementation and acceptance work; distinguish unavailable external checks from passing checks.

**Status:** implementation and acceptance plan. A completion record will report actual commands, results, and unresolved limits.

## Problem and intended behavior

[Phase 2](vortex-phase2-results.md) proves correct native selective scans and reduced positional requests on controlled files. Its local Vortex opener bypasses Daft's I/O configuration and production counters. Explicit `IOConfig` and non-local paths are rejected. The native query result channel is also unbounded, and abandoning a query follows a finish path that can await full execution.

Phase 3 connects Vortex's byte requests to Daft's existing I/O stack and gives local query consumers explicit backpressure and cancellation. Add only tested remote schemes and worker transport. Retain complete schema validation, conservative predicate translation, complete residual evaluation, exact limits, and feature separation from Phase 2.

This phase does not expand data types, add writing or schema evolution, enable source metadata counts, or claim an end-to-end speedup. Splitting a file into several Daft tasks is optional and remains separately gated; Vortex's internal batching is not Daft task splitting.

## Delivery steps

| Step | Implementation | Acceptance evidence |
| --- | --- | --- |
| Native I/O adapter | Implement Vortex positional reads over `daft_io::IOClient`; route schema/footer/data reads through it | Exact ranges, alignment, errors, ownership, configuration, and counters |
| Read budgets | Enforce per-open-reader request-count and requested-byte permits, including footer reads | Deterministic concurrent and cancellation tests; no oversized bypass |
| Local result flow | Honor `results_buffer_size`; bounded default for the local runner; explicit abandoned-consumer cancellation | Slow/full-queue tests, EOF/error stats, pending-next cleanup, cross-format regressions |
| Remote paths | File, HTTP(S), and S3-compatible reads using existing Daft configuration and discovery | Real range server and S3-compatible service; permission, retry, malformed response, and configuration checks |
| Worker execution | Serialize native scan plans and reopen resources on workers; enable the existing Ray/Flotilla route only with matching tests | Native-plan transport round-trip plus actual worker execution |

The I/O adapter and shared executor changes must remain separately reviewable. Reader-local permits cannot bound result queues, and a queue bound cannot bound Vortex caches or downstream aggregation state.

## Native adapter and read contract

The read path becomes:

```mermaid
flowchart LR
    A["Vortex scan/footer"] --> B["Daft-backed positional reader"]
    B --> C["Request and byte permits"]
    C --> D["Daft IOClient and IOStatsRef"]
    D --> E["Local file / HTTP / S3"]
```

Use the pinned Vortex 0.88.0 public interfaces and existing Daft dependencies. Add internal dependencies from `daft-vortex` to `daft-io` and Python configuration bindings; no new external dependency family is planned.

Native calls follow existing reader conventions:

```text
stream_vortex(uri, io_client, io_stats, read_options, convert_options)
read_vortex_schema(uri, io_client, io_stats, read_options)
```

Retain sessions and file owners in stream state. The adapter handles empty reads, checked offset arithmetic, file bounds, exact requested length, aligned owned output, and short/oversized responses. It must never reinterpret a full HTTP response at offset zero as data requested at another offset. Validate bounded HTTP range responses, including status and `Content-Range`, below the adapter where response headers remain visible.

Do not add an independent cloud SDK or manually copy selected credentials into a second client. Propagate the actual `IOConfig` through discovery, schema inference, task serialization, and execution. Support exact HTTP URLs rather than inventing HTTP directory listings or HTML glob discovery. Preserve the established literal-percent convention for local file URLs.

Use caller-provided `IOStatsRef` for execution requests. Count footer and data requests once, and isolate per-input statistics across files and repeated tasks. Planning/schema inference has its own context; do not silently present it as execution-only metrics. Reuse Daft's retries and error mapping, with sensitive URI/configuration values redacted from added diagnostics.

Disable Vortex read coalescing initially so its merged requests cannot silently defeat an individual request budget. Do not introduce a persistent footer cache without an identity/invalidation policy. Files must remain unchanged between planning and execution, as in earlier phases.

## Resource options and exact scope

Extend keyword-only Vortex options and serializable source configuration with:

```python
daft.read_vortex(
    path,
    io_config=None,
    batch_size=131072,
    *,
    enable_projection_pushdown=True,
    enable_filter_pushdown=True,
    max_concurrent_requests=8,
    max_in_flight_bytes=64 * 1024 * 1024,
)
```

Both new controls require positive integers, excluding booleans. The native schema binding receives the same configuration and budgets so footer reads cannot bypass them. The scan configuration preserves settings across serialization and displays their requested values.

Permits are shared by clones of one opened positional reader and cover request execution, response collection, and alignment. A single Vortex request larger than `max_in_flight_bytes` returns a clear error; it does not receive an unaccounted exception. Users can raise the budget for files whose encoded segments require larger requests. Cancellation and error paths release acquired permits, including when a future is dropped while waiting.

These limits bound outstanding requested bytes and requests **per opened reader**. They do not cap retained decoded batches, caches, all concurrent files, join/aggregation state, or process RSS. Track those distinctions in tests and results rather than advertising a total-query memory guarantee.

## Local backpressure and cancellation

Forward the existing `results_buffer_size` argument through the local runner and native executor instead of dropping it. Resolve an omitted local value to a bounded capacity of one result partition. Validate explicit capacities before execution. Keep the existing distributed shared-plan behavior unless an explicit bounded setting is supported and tested; do not silently change Flotilla scheduling to solve the local consumer problem.

Distinguish successful completion from abandonment:

- EOF drains the input normally and preserves execution statistics and surfaced errors.
- Explicit close, generator abandonment, and consumer exceptions close owned iterators and cancel abandoned execution promptly.
- Cancellation while a result queue is full must not wait for the consumer to drain it.
- Multiple active input IDs sharing one plan must remain isolated. Finishing or abandoning one input must not abort unrelated inputs.
- Close must be idempotent and safe around a pending asynchronous `next()` call.

Add explicit asynchronous receiver close and propagate cleanup through `iter_partitions`, `to_arrow_iter`, and `iter_rows`. Do not rely on garbage collection timing as the primary cancellation mechanism. Already-running blocking OS reads may complete; cancellation promises must identify which asynchronous work is actually interruptible.

Shared scheduler changes need ordinary in-memory and existing-reader regressions, independent of Vortex. No fixed sleep or wall-clock race belongs in deterministic acceptance tests; use barriers/events and counters.

## Ray and serialization

Use the existing distributed planner and Flotilla worker path. In this checkout, native scan tasks are carried in the serializable local physical plan, so generic Python `PyDataSourceTask` pickling is not a prerequisite for this route. Test the real transported representation rather than inventing an unrelated wrapper format.

Serialize paths, schemas, source flags, read budgets, and storage configuration. Never serialize open sessions, live file handles, semaphores, or readers. Reopen on each worker with the same feature/configuration requirements. Test worker-side failures and repeated execution, and compare actual Ray results to the native runner. A serialized-plan round-trip alone does not establish working distributed execution.

## Acceptance matrix

| Area | Required cases |
| --- | --- |
| Local byte reads | Offset/length/bounds, empty ranges, alignment, owned buffers, missing/truncated files |
| Budgets | Concurrent request/byte caps, oversized request error, waiting cancellation, read error, released permits |
| HTTP | Correct 206 ranges, ignored ranges, wrong offset/total, short/oversized body, HEAD/size behavior, retry/error paths |
| S3-compatible storage | Endpoint/region/anonymous or credentials reach listing, schema, and execution; missing/forbidden objects |
| Metrics | Footer plus data, per-input isolation, repeated scans, no duplicate accounting |
| Local result queue | Slow consumer, full-queue close, pending-next close, EOF, error, repeated close, multiple active inputs |
| Public queries | All Phase 2 filter/projection flags, limits, zero columns, schema errors, joins/counts, retained batches |
| Worker transport | Native physical-plan serialization, fresh reopen, actual Ray local-file and supported remote reads |
| Feature separation | Enabled and disabled builds, import/collection, clear enablement errors |
| Static checks | Applicable pre-commit, dependency validation, all-feature/all-target Clippy, no unreviewed external graph growth |

Keep local adapter/scheduler tests socket-independent where possible, using real files plus deterministic private test backends. Real HTTP and S3 service tests are still required before claiming remote acceptance; mocks and skips are not substitutes. Use existing local HTTP/MinIO conventions, with no required public bucket or production credentials.

## Environment and execution plan

Initial inspection finds Ray, boto3, moto, PyArrow, and Vortex installed. The Docker CLI exists, but its daemon socket is absent. Binding a loopback TCP port from this agent session returns `EPERM`. Thus this session cannot currently launch a local HTTP server, MinIO, or Ray cluster. Continue implementation and socket-independent checks, prepare exact service commands and logs, and report real service checks as pending until they execute successfully in an environment that permits them.

1. Record this plan, then implement adapter, public/config/worker plumbing, and shared result flow in coordinated ownership areas.
2. Type-check and run focused Rust suites offline, serializing Cargo work to avoid cache/lock contention.
3. Rebuild the extension with `python,vortex`; run local public/transport/resource tests and the full focused Phase 2 regression selection.
4. Run real HTTP/S3/Ray acceptance where services can start. Preserve unavailable checks explicitly and provide runnable commands; do not weaken assertions or label skips as validation.
5. Rebuild without Vortex, check feature separation, and restore the enabled extension.
6. Run contribution hooks and dependency comparison, write the completion record, commit, push only to the personal fork, and open the review PR after acceptance work.

Example enabled rebuild in this configured environment:

```bash
env -u CONDA_PREFIX CARGO_BUILD_JOBS=2 PYO3_PYTHON=.venv/bin/python \
  DAFT_DASHBOARD_SKIP_BUILD=1 .venv/bin/maturin develop \
  --skip-install --offline --locked --features python,vortex
```

No phase is complete merely because its unit tests pass. Claim local I/O, remote schemes, query cancellation, and distributed execution only to the extent demonstrated by the corresponding tests. Release-build performance evaluation remains Phase 4.
