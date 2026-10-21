# Vortex Phase 6: first remote acceptance

Status: **not accepted**. The first implementation improves the Phase 5-equivalent
configuration, but misses the declared all-five-query median gate against Parquet.
This is the experimental personal fork, not an upstream or public-cloud result.

## Frozen run

The report is `examples/vortex_eval/artifacts/phase6/acceptance-20261020T234447Z-12679/acceptance.json`.
Source was `c37e8ae9914fd9887551f88a809487f64b0ab861`; the enabled release binary
SHA256 was `2cccc6783fb85a9fdcd0c03c6b390d980fbb33ae47ee03e44edf28676b7a11dd`.
Source and binary identities remained unchanged through acceptance. The remote
manifest SHA256 was `eef77e8455ebfeb97536a2e1b8d205faec975d60f3f1af94145b9bc20f458d4c`;
the local manifest was `0f27c0d14df2cdf9e9cf4ac593a59fb1d2a637a85624634591abae56b8b9fd80`.

Tuning used 20 measured rounds and one warmup. Its predeclared worst-case ratio
ranking selected coalescing **off**, discovery concurrency **8**, and metadata
concurrency **8**. The 64 KiB/4 MiB policy had a lower geometric mean in tuning,
but a slightly worse worst-case ratio, so it was not selected. This is not evidence
that merging is always harmful.

Final acceptance retained that one selection for all five queries, with 50
measured rounds and three warmups. Every result matched the independent oracle.

| Remote case | Parquet median | Vortex median | Vortex relative to Parquet |
| --- | ---: | ---: | ---: |
| Narrow scan | 130.46 ms | 126.42 ms | 3.10% faster |
| 1% range | 111.09 ms | 117.15 ms | 5.46% slower |
| 1% equality | 111.87 ms | 115.53 ms | 3.28% slower |
| Join | 135.99 ms | 137.17 ms | 0.87% slower |
| Full value reduction | 282.65 ms | 286.96 ms | 1.52% slower |

The candidate was 0.50–5.55% faster than the same-build Phase 5-equivalent
configuration, depending on the query. Sample ranges overlapped in all five
Parquet comparisons. These median results do not establish significance or a
universal format ranking. Repeating the unchanged run until it passes would not
satisfy this evaluation protocol.

## Correctness and compatibility

All 3,000 timed tuning/final/local-guard trials passed their correctness checks.
All 120 separate diagnostic captures completed without reported event loss.
The four-dataset local guard passed its 5% regression threshold.

The unrestricted terminal also passed 585 focused tests with zero skips,
112 ORC/MCAP tests with one optional S3 skip, four S3 pooling tests, and the full
Rust workspace: 2,305 passed, 20 ignored, zero filtered across 157 targets.

Service acceptance had 24 passes and two failures. Both failures required a
strict request-count reduction in the HTTP delay fixture, although all row,
authentication, range, budget, and diagnostic assertions passed. Adjacent file
segments need not arrive in the same SDK batch; the 0 ms and 5 ms cases produced
ten physical data reads with either setting. The 20 ms case did merge.

The corrected integration assertion requires complete range coverage and no
request-count increase, while the native file-driver regression supplies a
deterministic batch and proves the actual three-to-one merge. The corrected HTTP
matrix still needs execution in the unrestricted terminal. Performance gates
and the saved failed report remain unchanged.

## Evidence for the follow-up

The captured eight-file queries perform 17 native HEAD requests and 17 initial
footer reads: one schema open, eight planning opens, and eight execution opens.
The execution-open envelopes span roughly 6–17 ms in these individual traces.
These are separate diagnostic queries; overlapping durations must not be summed
or substituted for the measured medians.

In the selective traces, complete residual filtering takes about 0.03 ms after
Vortex filtering. Removing that work is not the first target for a remaining
4–6 ms gap. The next experiment follows the accepted design's version-validated,
query-scoped footer reuse: preserve fresh metadata checks and conditional reads,
reuse only bounded serialized metadata, and fall back when identity cannot be
validated. Phase 6 stays open until a new build passes the original gates.
