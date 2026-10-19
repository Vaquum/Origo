# Performance investigation log

Continuous, append-only record of performance investigations, benchmarks and
optimizations throughout Origo. Read it before performance work and before any
ingestion work, together with the [ingestion performance rules](Ingestion-performance.md).

Append each completed attempt and its findings as the work proceeds, before
declaring the task complete. Record pending attempts explicitly and append their
results in new entries referencing the original. Record successes, failures, regressions,
unchanged results and disproved hypotheses. Use `not measured`, `unknown` or
`none established` when appropriate; never invent a measurement or learning.
Do not edit or remove recorded entries. Append corrections referencing the entry they
supersede so the evolution remains visible.

## Entry format

Use the next unused entry number and a UTC date. Keep `I tried → I found →
Progress/Next` as the narrative, with enough context to reproduce and interpret
the result:

```markdown
## Entry NNN — YYYY-MM-DD

- Task/evidence: issue or PR, tested commit, retained report/log links.
- Context: real input identity, partitions and row count; hardware, versions,
  settings and concurrency; measured stages and exclusions.
- I tried: question or hypothesis, change and exact command/method.
- I found — good: supported gains or confirmed behavior, including correctness.
- I found — bad: failures, regressions, unchanged results or rejected hypotheses.
- Measurements: baseline/result, elapsed time, completed source rows/s,
  peak memory and database rows read where relevant; explain unmeasured values.
- Progress/Next: decision, applicability limits and next step; corrected or
  superseded entry numbers where applicable.
```

## Entry 001 — 2026-10-19

- Task/evidence: [#551](https://github.com/Vaquum/Origo/issues/551). Retrospective
  review of the [Playground log](https://github.com/Vaquum/Origo-Playground/blob/main/spec/live-performance-investigation-log.md)
  and Origo implementation at [e8959ab6](https://github.com/Vaquum/Origo/commit/e8959ab6ab423cc3ab05b688214ba0c83f356215).
- Context: documentation and implementation review; no runtime experiment.
- I tried: consolidated the earlier investigations and current Binance source
  implementation into twenty ingestion performance rules and established this log.
- I found — good: bounded columnar work, generation-scoped reuse, measured query
  reads and pinned publication provide concrete practices to carry forward.
- I found — bad: earlier assumptions were superseded by evidence, including
  unconditional proof-only reconcile and page-size probes that omitted production
  authentication. Retain those failed approaches and their corrections.
- Measurements: not measured; this review ran no benchmark and claims no new
  speedup. Historical measurements remain attributed to their original records.
- Progress/Next: apply the linked rules and append each future performance
  attempt with its actual evidence and positive and negative findings.

## Entry 002 — 2026-10-19

- Task/evidence: [#553](https://github.com/Vaquum/Origo/issues/553), investigation of merged [#548](https://github.com/Vaquum/Origo/pull/548) at `e8959ab6`. Private original inputs and reports remain under `storage/book-cost/` in the owning checkout; no provider payload is published.
- Context: production spot native run `a250fb46-6ab0-4573-a857-e054b064a9b4`, `2025-06-30T05Z`. Current input: 626,505 level rows, 3,058,472 bytes; six preceding hours plus current/following inputs total 32,486,703 bytes. QueryFinish receipts were correlated with native and HTTP insert connections.
- I tried: separated native run time from database work, then profiled the original replay stages on recorded immutable inputs. Earlier workstation profiles used a broken `_decimal` extension and Python decimal fallback; those CPU timings are not production measurements.
- I found — good: retained input pins and four production component hashes give a complete numeric reference. Production uses native decimal. Database work was not the dominant original cost.
- I found — bad: the reader materializes every level as a Python dictionary, repeatedly parses decimal strings, retries the accumulated warmup from scratch, checks 200-level availability between its one-second output grids, and immediately repeats a completed canonical audit. Native-ID interleaving can also produce false sequence gaps.
- Measurements: original run 229.887 s; 201.335 s before its first component, not exclusively replay CPU. Native database receipts: 147 queries, 4.226 s summed execution, 2,724,748 rows read; separate HTTP inserts 0.405 s. An isolated original accepted replay measured 57.11 s on the server, excluding input preparation, network, writes and Dagster. Profiling overhead raised that replay to 142.51 s. No backfill throughput is inferred from these stage timings.
- Progress/Next: stop bulk production work and first remove the repeated work and per-level Python path. Preserve complete private validation and activation.

## Entry 003 — 2026-10-19

- Task/evidence: [#553](https://github.com/Vaquum/Origo/issues/553), canceled native backfill `eqsybmbq`; private final inventory `storage/book-cost/bulk-failure-inventory-final-20261019.json` and native canceled-state screenshot.
- Context: 5,125 submitted child run records, observed through 08:14:58 UTC. Cancellation was requested in native Dagster; unrelated routine jobs were retained.
- I tried: enumerated actual errors separately from canceled children and verified both the parent and global queue drained.
- I found — good: at 08:13 UTC the parent was CANCELED, with 102 SUCCESS, 113 FAILURE and 4,910 CANCELED children; zero queued/starting/started/canceling children. At 08:14:52 UTC the global queue was empty and routine metadata/audit work remained running.
- I found — bad: actual failures comprised four lookback HTTP 404s, 25 unproven-depth errors, five tracked-price-cap errors, 78 sequence errors and one duplicate-level error. The four 404s requested an earlier `2025-06-28T05Z` lookback, not the selected-hour archive. The UI's failed aggregate includes canceled children and is not an error count. Not every sequence failure has been individually proved to be a missing archive chain.
- Measurements: no complete backfill coverage or throughput established. Queue observations are timestamped, not a claim that all freshness checks were green. No subsequent production load test or resumed backfill is part of this local investigation.
- Progress/Next: keep production stopped until local correctness and consistent complete-partition speed are established.

## Entry 004 — 2026-10-19

- Task/evidence: [#553](https://github.com/Vaquum/Origo/issues/553); private `storage/book-columnar-replay/matched-*.json`, `tile-sweep.jsonl`, prototype source hashes and original input pins.
- Context: Apple arm64 MacBookPro18,2, ten CPUs/64 GiB; Python 3.11.11 with native decimal, Polars 1.44.2; owned ClickHouse 25.3.2.39 capped at two CPUs/2 GiB. Original spot `2026-10-04T09Z`, spot `2025-07-15T20Z`, futures `2026-10-04T09Z`, and six-hour warmup spot `2025-06-30T05Z`. Provider requests and Dagster launch excluded.
- I tried: exact decimal dictionaries, bounded NumPy tiles, typed RowBinary output and a single set-based query over existing fixed hash chunks. Then increased warmup/sample tiles from 128/256 to 2,048/1,024.
- I found — good: all four persisted component hashes matched the recorded references for all four cases. The set-based hash read at most 36,000 rows per query in the owned fixture; 24 QueryFinish receipts had a maximum 107.63 MiB query memory. This is a measured bound for these complete hourly grids, not arbitrary component size.
- I found — bad: complete local pipeline times were inconsistent (approximately 6, 10 and 43 s for the first three cases). Larger tiles roughly doubled replay CPU from 4.19 s to 8.24/8.46 s in the June case; reject that change. These runs do not establish a matched end-to-end speedup or the few-second target. Workstation load averages later exceeded 90; CPU and wall time must remain separate. Peak Python RSS reached approximately 1.26 GiB in an earlier dense-input variant.
- Measurements: cached-input replay-only timings omit raw decoding, setup and database work. A compiled-buffer prototype later processed the six-million-row June replay in 0.168 s and retained all four hashes; its complete pipeline still took 22.375 s under contention. Moving proof-region tracking into the native state loop also preserved all four hashes, with 0.942 s total Python-process CPU in replay/packing but an 81.868 s complete pipeline under severe contention. Neither kernel result is full-partition qualification. Native decimal backends differ locally versus production; exact output hashes were checked rather than assumed.
- Progress/Next: reject the larger-tile approach. Version the smaller compiled state loop, decode each file once per lookback, retain exact message/clock/checkpoint proofs and measure the remaining complete path repeatedly. Consistent few-second partitions, matched hash ablations, deployed concurrency and routine-service headroom remain unqualified.

## Entry 005 — 2026-10-19

- Task/evidence: correction to Entry 004's memory unit. The earlier dense-input report records 1,263.97 MiB (approximately 1.234 GiB), rather than 1.26 GiB. No new experiment or performance claim.
- Progress/Next: retain exact units in subsequent qualification reports.

## Entry 006 — 2026-10-19

- Task/evidence: [#554](https://github.com/Vaquum/Origo/pull/554), head `2dcff512`; failed [runtime CI installation](https://github.com/Vaquum/Origo/actions/runs/39237455944/job/118009873849).
- Context: isolated Ubuntu test guest, Python 3.11.17; no runtime tests executed. The package build invokes the new C++17 extension's compiler.
- I tried: published the source and ran the existing required CI gates before local checks.
- I found — good: lint, typing, fail-loud, slice, version, ruleset, Conventional Commits and CodeQL gates passed on this head.
- I found — bad: installation failed because `g++` was absent. This is a missing declared build prerequisite, not a test waiver or a reason to retry unchanged inputs.
- Measurements: no runtime or speed qualification from this failed job. At 10:23:45 UTC the production backfill remained CANCELED; one routine run was queued and two refreshes, metadata maintenance and one audit were active. A subsequent read found only the routine audit active; no canceled-backfill work resumed.
- Progress/Next: install the compiler in the existing CI dependency phase, push that fix, then run the required local checks and complete current-head CI. No production deployment or restart.

## Entry 007 — 2026-10-19

- Task/evidence: [#554](https://github.com/Vaquum/Origo/pull/554), self-review of its complete published diff against the original sampler.
- I tried: checked the minute evidence separately from the four numeric component hashes.
- I found — bad: the prototype used the last emitted 100-ms grid's update ID as the minute's terminal update ID. The original seal records the last accepted exchange event strictly before the minute boundary; genuine updates can occur after that final grid. Numeric component hashes alone do not establish the evidence metadata's identity.
- I found — good: that exact terminal cursor can be selected from the already validated event headers, without another level scan or replay. Extend the existing original-hour assertion to compare both minute cursors against the independent reference.
- Measurements: not measured; this is a correctness repair, not a timing result.
- Progress/Next: push the corrected cursor selection before local validation. Complete numeric and evidence checks together.

## Entry 008 — 2026-10-19

- Task/evidence: [#554](https://github.com/Vaquum/Origo/pull/554), local validation `3554d837-485e-43c2-8410-196e0b5710e2` and [current-head runtime failure](https://github.com/Vaquum/Origo/actions/runs/39239577254).
- Context: original recorded inputs; local ordinary phase on Python 3.11.11 with two workers, isolated source state and unchanged assertions/limits. One Linux-only multiple-loopback test is skipped on macOS and remains required in Linux CI.
- I tried: the complete ordinary contract matrix after publishing the implementation and compiler prerequisite.
- I found — good: 243 local cases passed before interruption, including both complete original-hour numeric and minute-cursor comparisons. The native extension installed in Linux CI after the compiler fix.
- I found — bad: history fault injection still targeted the retired row reader; rejection-identity setup implicitly required an old preliminary prelude; one opening-seed test also removed its closing witness; a manual-retry assertion fixed the internal replay-attempt count. Preserve the same observable faults at the columnar boundary, deliberately exercise all identity dependencies, provide the untouched tail for the opening fault, and assert a genuine retry while retaining exact automatic-suppression checks. The old tracked-price restriction now needs an accepted archive assertion; live limits remain unchanged. An interrupted native replay produces no partial RowBinary files, strengthening the previous cleanup boundary.
- Measurements: local ordinary phase failed after 247.92 s, with 243 passed, two failed and one platform skip. Linux CI exposed the additional opening-fault and retry-count assumptions. No speed qualification or completed suite is claimed. The measurement command's main timers exclude imports; an external launcher must measure complete cold-process cost.
- Progress/Next: push these contract-preserving repairs before another check. Repeat the required matrix, then measure complete source builds with matched inputs and separately measured startup.

## Entry 009 — 2026-10-19

- Task/evidence: [#554](https://github.com/Vaquum/Origo/pull/554), [runtime failure on ddbc053](https://github.com/Vaquum/Origo/actions/runs/39241791317) and four inline review findings.
- I tried: the current contract matrix and review of all canonical return paths and native depth boundaries.
- I found — good: Linux CI passed 305 cases before interruption; all other required gates passed.
- I found — bad: two remaining tests asserted the retired preliminary lookback and cursor-hash query shape. Preserve the extra zero-prelude attempt and measure the actual set-based query's memory. Review also found missing superseded-failure recovery on retained-after-fetch and repair paths, a lost snapshot projection assertion, and failure to extend an adjacent proven region immediately after snapshot refresh. These receive direct repairs and existing-contract regression coverage, without restoring the repeated full audit.
- Measurements: CI failed after 169.97 s with two failures; no speed claim. The local benchmark now uses the shared canonical admission and activation path; complete timing remains pending.
- Progress/Next: publish these repairs, complete local and CI checks, then measure original recordings against a genuine owned ClickHouse data mount. Production backfill remains stopped.

## Entry 010 — 2026-10-19
- Task/evidence: [#554](https://github.com/Vaquum/Origo/pull/554), published implementation `494abd6`, then three-line memory change `acd8f26`. Private SHA-pinned manifests, original response headers, complete JSON reports and exact commands are retained under `storage/book-columnar-replay/`.
- Context: ten-CPU arm64 Mac, Python 3.11.11/native decimal, Polars 1.44.2 with two threads; owned ClickHouse 25.3.2.39 limited to two CPUs/2 GiB, genuine host data mount and UUID. Fresh database and unchanged audit state each iteration; OS file cache warm; no simultaneous local test suite.
- I tried: `tools/benchmark_book_replay.py` through the shared canonical storage admission, fetch, replay, four writes/proofs and activation; three repetitions per original spot snapshot, futures snapshot and spot update-only hour. External launcher measures imports and cold-process completion separately. All twelve component proofs per case match the independently recorded references exactly.
- Measurements (current, spot-snapshot): complete partition 2.336, 2.655, 1.979 s; Python CPU 0.850, 0.484, 0.502 s; cold start to first completion 2.931 s; cumulative peak Python RSS 537.64 MiB.
- Measurements (current, perp-snapshot): complete partition 3.158, 2.840, 2.795 s; Python CPU 2.011, 1.672, 1.641 s; cold start to first completion 3.741 s; cumulative peak Python RSS 1617.23 MiB.
- Measurements (current, spot-update-only): complete partition 4.550, 3.916, 4.091 s; Python CPU 3.066, 2.620, 2.722 s; cold start to first completion 5.133 s; cumulative peak Python RSS 1220.61 MiB.
- Measurements (retained-columns, perp-snapshot): complete partition 3.203, 2.896, 3.635 s; Python CPU 2.014, 1.713, 1.897 s; cold start to first completion 3.807 s; cumulative peak Python RSS 1910.25 MiB.
- Measurements (cursor-hash, spot-snapshot): complete partition 2.917, 2.215, 2.435 s; Python CPU 0.901, 0.523, 0.515 s; cold start to first completion 3.584 s; cumulative peak Python RSS 539.14 MiB.
- Measurements (cursor-hash, perp-snapshot): complete partition 3.475, 3.396, 5.510 s; Python CPU 2.060, 1.673, 1.869 s; cold start to first completion 4.088 s; cumulative peak Python RSS 1599.12 MiB.
- Measurements (cursor-hash, spot-update-only): complete partition 5.345, 4.353, 4.918 s; Python CPU 3.208, 2.723, 3.006 s; cold start to first completion 6.189 s; cumulative peak Python RSS 1217.80 MiB.
- I found — good: all repeated complete local outputs match. Dropping no-longer-needed archive columns before level validation reduced paired dense-input peak RSS from 1,910.25 to 1,617.23 MiB. QueryFinish evidence for 84 set-based hash queries read at most 36,000 rows per query, with at most 112,859,400 bytes query memory.
- I found — bad: dense replay still uses approximately 1.58 GiB of Python-process memory. Matched cursor-hash ablation has variable wall time; do not infer a universal speedup or throughput from the query-count reduction. The restored projection test initially failed because an original Parquet file has twelve physical columns, not the hardcoded eleven; require two projected columns against the observed schema.
- Limits/Next: these are completed local partitions, not provider-network or Dagster-launch timings. No production concurrency, routine-service headroom, historical coverage, backfill ETA or combined server/client peak is established. The June six-hour lookback has raw captures but lacks original response headers, so it is not yet a full authenticated-fetch benchmark. Complete current-head contracts and native local GUI verification; production remains stopped.
