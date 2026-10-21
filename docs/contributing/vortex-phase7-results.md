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

## Remaining evidence

The revised candidate requires a new complete local paired measurement and
remote request-concurrency sweep. Remote runs use an owned temporary MinIO
container, with the same persistent-worker and fresh-source protocol. They
compare request limits 1, 2, 4 and 8 per file independently of the planning
candidate. Service, socket and compatibility checks must also pass before
publishing this phase.

The profile exposes API, planning, reader, execution and I/O boundaries, plus
uncovered time and accumulated decoder work. It does not independently measure
DNS/TCP/TLS, physical SDK retries, wire bytes, server/kernel work or exclusive
codec CPU. No universal performance advantage or final acceptance is claimed.
