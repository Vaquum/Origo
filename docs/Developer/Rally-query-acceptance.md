# Rally query deployment acceptance

This freezes slice [#489](https://github.com/Vaquum/Origo/issues/489)'s measurement
protocol before any timed production run. It establishes canonical transport and
reuse; [Explorer #50](https://github.com/Vaquum/Market-State-Cube-Explorer/issues/50)
owns GUI behavior and projection performance. No timed production verdict is
recorded by this document.

## Frozen corpus

All intervals are UTC and half-open. Each case runs all three detector modes for
each listed scale. The immutable CLI manifest additionally records concrete accepted
pins, source hashes, actual probe bounds and deployment identity before timing.

| Analysis interval | Scales | Expected ATR bulk start |
| --- | --- | --- |
| 2026-06-27 11:39 to 11:55 | bps, ATR | 2026-06-27 07:45 |
| 2026-06-27 10:00 to 11:00 | bps, ATR | 2026-06-27 06:15 |
| 2026-06-26 00:00 to 2026-06-27 00:00 | bps, ATR | 2026-06-25 20:15 |
| 2026-06-25 00:00 to 2026-06-27 00:00 | bps | Not applicable |
| 2026-06-25 03:45 to 2026-06-27 00:00 | ATR | 2026-06-25 00:00 |

This is 24 discovery cases. Freeze bps target 30, control pullback 10 and swing
reversal 10; ATR target 1, control pullback 0.5 and swing reversal 0.5. Anchored cadence
is one minute. For each case run the first discovery as cold, then five warm rounds.
Cold means the first request of that case in this run; production caches are not
flushed. Actual first native swing seed and causal warmup are disclosed. Point
predecessor searches return at most one row in `[anchor-24h, anchor)`.

Additional bounded authentic windows prove positive control/swing/ATR outcomes,
exact ties, overlaps, partial base cells, member excursions outside endpoint prices, genuine empty results
and separate unknown/left/right censoring. Existing 2017 989-label and 2026 four-label
legacy exports run unchanged; the cube claims no 2017 coverage.

## Numerical and resource expectations

The independent oracle reads the same concrete native identities in native ID order.
Event decisions, endpoints, confirmation, membership keys, counts and partiality use
exact comparisons. Volumes use chronological `math.fsum`, absolute tolerance
`1e-8 USDT` and relative tolerance `1e-12`; this allows only low-order Float64 reduction
differences, never eligibility changes. The oracle independently reconstructs causal
bars, sparse contributions and version-1 content hashes.

The existing deployed merge must retain two CPUs and 2 GiB memory. Every required
case must succeed within 295 seconds, 8,000,000 returned native rows, 256 MiB of five
typed native columns, 512 MiB of output and 1.5 GiB aggregate worker RSS. Whole-cell
columns are separate bounded working data, not native row admission. Returned probe
rows are charged even if already present in the bulk scan. Each ClickHouse statement
uses at most four threads, 4 GiB memory and the smaller of 60 seconds and the remaining
wall budget, without spill. Refusal is evidence of failure for a required capability
case, never a successful benchmark. No increased limit or reduced corpus is accepted.

Record end-to-end p50/p95/max, actual bulk/probe rows and bytes, staged/final bytes,
worker RSS, cgroup current/max memory and OOM events, per-statement settings/time/memory,
64 GiB result inventory/reservations and the source-capacity reserve plus 8 GiB disk
floor. Deployment/source hashes must match the verified merged commit before timing.

## Concurrency and ingestion

Run one rally discovery with one external ordinary cube query. A second rally must
be rejected; supported access renewals have p95 at most one second. Prove no third
heavy query, OOM, source reserve breach or incompatible ordinary-result change.
Capture 30 minutes of actual input-feed receipts, heartbeat, coverage and capacity
before and after the workload, plus authoritative receipts across the entire workload. A query-attributed failure, missing heartbeat or
worsening ingestion backlog fails acceptance. A failed query-service receipt or input-feed
receipt during the workload also fails; after-workload health cannot hide it. These observations come from the
existing authoritative stores, not operator-entered truth flags.

Independently reduce retained canonical files through 200 bounded grid/filter/replay
views. Prove zero discovery POSTs, actual detector calls, ClickHouse statements and
new published result IDs over that interval, using canonical container logs, query
logs and unchanged owned result inventory. This transport test does not substitute
for Explorer's own GUI, heap, payload or projection timing proof.

## Running and verifying

After the merged build is deployed, run:

```sh
PYTHONPATH=. python tools/benchmark_market_state_rallies.py acceptance --report /tmp/rally-acceptance.json
PYTHONPATH=. python tools/benchmark_market_state_rallies.py verify-report --report /tmp/rally-acceptance.json
```

The CLI acquires read-only production evidence automatically. It refuses to time an
unmerged or mismatched deployment. The report, companion checksum and immutable
`.evidence` directory retain the frozen manifest and every referenced observation.
`verify-report` recomputes the verdict from evidence; missing/skipped cases, unverifiable
facts, synthetic records, identity mismatch, undeclared tolerance, enlarged deployment
or threshold breach fail verification. Unit tests validate these rules without
claiming a production PASS.

## Approved corrected-source deferral

Production activation history currently has zero partitions with multiple revisions.
The operator approved deferring the actual corrected-source before/after production
case. Record that case as deferred/unobserved with its real activation-history query,
never PASS. Canonical hash sensitivity still uses distinct unchanged authentic native
records, and generation-only component additions must preserve evidence identity.
When an authentic corrected revision becomes available, reassess this explicit
deferral; it cannot be silently converted into proof. This deferral alone blocks
neither slice #489 nor PRD #487 closeout.
