# Phase 6 follow-up: validated S3 footer reuse

This experiment starts at `1d13a16ab` on the personal fork's
`poc/vortex-phase6-metadata` branch. The [first acceptance](vortex-phase6-results.md)
missed four remote median gates despite correct results. The original performance
gate is unchanged. No additional statistical or infrastructure retries turn that
failed run into a pass.

## Contract

Add an experimental `enable_metadata_reuse` reader switch, initially opt-in.
Planning still reads current footer metadata on each materialization. For S3
objects with a usable strong ETag, it also records the object size, ETag, optional
version ID, and the bounded encoded byte ranges used to open that footer. These
are serialized into that file's native task, not stored in a global cache or in
the reusable Python source. Never serialize handles, readers, sessions or decoded
Arrow arrays.

The recorded ranges preserve Vortex's normal initial footer buffer, including
encoded segments that happen to be in that buffer. Reusing only a parsed footer
would discard those prefetched segments and could introduce new data requests.
Retained serialized snapshots are limited to 64 MiB across one Vortex source
materialization; files outside the remaining budget use their normal fresh-open
path. Separate sources have separate budgets, so this is not a query-wide
memory limit.

Execution performs a fresh metadata request before using any saved bytes. The
URI, object size, ETag and version ID must match. A changed or unavailable
validator fails the cached task, rather than silently applying old counts or
footers to a new object. All uncached reads use `If-Match` and, when available,
the recorded version ID. Every transport retry and resumed body read must retain
those conditions. An S3 precondition failure or mismatched response identity
reports that the object changed; there is no unconditional retry fallback.

This validates the snapshot at execution open and during subsequent remote reads.
Already validated cached bytes behave as bytes fetched at that open; this is not
cross-file snapshot isolation or detection of changes after the last I/O.
HTTP/local inputs and S3 responses without usable validators retain the existing
fresh-open path. Metadata reuse preserves schema validation, filtering,
projection and limits. Cached reads still obey the per-request length bound;
they perform no network I/O and consume no request/byte permits. Physical reads
retain the existing permits and in-flight byte budget.

## Implementation boundaries

1. Extend Daft I/O with fresh read metadata and conditional gets. Existing calls
   retain their ordinary behavior. Other object sources return size-only metadata
   and reject unsupported conditional requests.
2. Add bounded Vortex opening-range capture and a private, versioned serialized
   snapshot. Supply validated ranges to the regular SDK opener, preserving its
   footer parsing and prefetched-segment behavior.
3. Carry one opaque snapshot in each native Vortex task configuration. Debug and
   explain output display its byte count, never its contents. Workers require
   the matching experimental binary.
4. Reset the budget and metadata on each Python source materialization. The
   public switch is strict Boolean and defaults off until acceptance establishes
   the candidate. The follow-up harness explicitly enables it for candidates and
   disables it for the Phase 5-equivalent baseline.

## Tests and acceptance

Native tests cover strong-validator parsing, preserved conditions on resumed
reads, rejected preconditions, bounded capture and serialization, invalid or
wrong-object snapshots, cached-range alignment, fresh identity mismatch,
fallback paths, and release of owned resources. Existing complete-schema,
residual, projection, limit and lifecycle tests remain required.

Python tests cover strict validation, configuration/task serialization, per-query
budget exhaustion, and fresh metadata for reused sources. Real MinIO tests cover
stable reads, same-size object replacement between planning and execution, and
matching Ray worker execution. Scripted S3 SDK responses cover missing/weak
validators, conditional range headers and version IDs, mismatched response
identity and precondition failures before an uncached read. The corrected HTTP
delay matrix remains required. A scripted S3 endpoint also replaces an object
immediately after execution validation, proving that the real Vortex data path
rejects its next uncached GET using the original condition.

Use the same fixture hashes and one newly attested enabled release binary. Run
local checks before any push or Actions run. Repeat separate tuning (20+1), frozen
final acceptance (50+3), the four-dataset local guard (20+1), and the full service,
ORC/MCAP and unfiltered Rust acceptance. Never pool samples across builds. A miss
keeps Phase 6 incomplete; the first run's residual-filter timing does not justify
prioritizing residual elision over repeated remote opens.
