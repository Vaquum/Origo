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

## Entry 011 — 2026-10-19

- Task/evidence: [#554](https://github.com/Vaquum/Origo/pull/554), failed local ordinary validation on `1f113cd` and [CI on ed93f9c](https://github.com/Vaquum/Origo/actions/runs/39245271561). Local native GUI run `32eada37-0cb3-49ee-9980-791afe1065de`, retry `83bd2568-51fb-4b88-a650-17c431049811`; private screenshots, persisted proofs and activation receipts under `storage/book-columnar-replay/`.
- I tried: full contracts plus actual native partition selection, launch and retry with original provider bytes and headers, using unchanged capacity checks. No production execution or provider requests.
- I found — good: the native GUI run completed both steps in seven seconds; its retained-generation retry completed in six seconds. All four persisted hashes match the independent references. Retry left exactly one activation, generation and build; no queued or active local runs remained. These single GUI observations are workflow checks, not a repeated throughput benchmark. At 11:29 UTC production remained CANCELED with zero active children and no global queued or active runs.
- I found — bad: local contracts passed 319 cases before one remaining query-log assertion searched for the retired cursor query. Preserve its memory limit while observing the one set-based query. The first GUI attempt failed in repeated schema setup; an isolated `CREATE OR REPLACE VIEW` reproduced the same missing-file rename on the macOS shared mount. Moving the owned fixture to a genuine Linux volume made that same statement succeed, without a product workaround or relaxed admission guard. GUI runtime used Python 3.12; the repeated pipeline measurements in Entry 010 used Python 3.11.11.
- Measurements: local failed ordinary phase 348.48 s, one failure, 319 passes and one platform skip. Linux CI later passed 367 cases before another opening-depth fault lacked its untouched closing witness; add the original following archive so tail validation does not preempt the intended opening failure. The same depth rejection and zero-output assertion remain required. No new production speed, coverage or headroom claim.
- Progress/Next: publish these contract repairs, finish the entire current-input local and Linux matrices, re-read reviews after CI and keep the backfill stopped.

## Entry 012 — 2026-10-19
- Task/evidence: [#554](https://github.com/Vaquum/Origo/pull/554), boundary review `4313342288`, implementation `9c93831`/`e20af79`, actual-capture regressions `1df95d1`; private `boundary-pruning-ablation.json` and `qualified-closing-summary.json`.
- I tried: checked whether native-ID pruning of the following archive could hide a receive-ordered sequence gap. Reassociated genuine captured clocks/IDs, preserving original levels and the real opening bridge; no market values or IDs were invented.
- I found — bad: the prior filter silently accepted the injected gap for both spot and futures. The original following files also contain pre-boundary events, and futures uses its previous-final-ID bridge rather than spot contiguity; initial regression setup assumed otherwise and failed. Preserve the actual prefix and use each venue's native bridge.
- I found — good: retain every pre-boundary event plus the received prefix through the earliest real closing witness. Both regressions now reject BOOK_SEQUENCE_GAP with zero grid files (2 passed in the focused check). A controlled ablation confirms that the previous pruning accepts those exact same fault inputs.
- Re-measurement (spot-snapshot): complete local partition 2.926, 2.241, 2.858 s; Python CPU 0.966, 0.501, 0.567 s; cold process to first completion 3.546 s; peak Python RSS 534.38 MiB. All four persisted reference hashes match in every run.
- Re-measurement (perp-snapshot): complete local partition 4.012, 3.355, 2.868 s; Python CPU 2.278, 1.737, 1.554 s; cold process to first completion 4.647 s; peak Python RSS 1601.91 MiB. All four persisted reference hashes match in every run.
- Re-measurement (spot-update-only): complete local partition 5.237, 4.746, 4.215 s; Python CPU 3.188, 2.707, 2.617 s; cold process to first completion 5.865 s; peak Python RSS 1215.73 MiB. All four persisted reference hashes match in every run.
- Limits/Next: same original manifests, fresh databases, shared admission and four outputs as Entry 010; provider transfer/pacing and Dagster launch remain excluded. Local suites were not running during these repetitions. No production ETA, concurrency or coverage claim. Stop obsolete validation when product inputs change; finish the final current-input matrix and re-read current-head reviews after CI. Production remains stopped.

## Entry 013 — 2026-10-19
- Task/evidence: [#554](https://github.com/Vaquum/Origo/pull/554); source `905ac1f`, private `qualified-unpruned-summary.json`, `qualified-simple-summary.json`, source hashes and original manifests. Supersedes the following-file cutoff approach in Entry 012.
- I tried: matched original workloads with the following-file pruning removed, then published the simpler reader and repeated all nine complete pipeline measurements. No other local test suite ran during these repetitions.
- I found — bad: a shared receipt cutoff can omit a lower closing native ID received late. A format-dependent cutoff (`4dcf02a`) repaired that case, but added rules and parameters. The first fresh fixture attempt also reached ClickHouse during startup and failed with EOF before completing any partition; require an actual SELECT 1 readiness receipt before the timed run. Linux CI passed 1,011 cases before a remaining test demanded the removed second canonical audit; update that exact count from two retained reads to one, preserving its complete real-data evidence assertions.
- I found — good: the unpruned ablation still completed in 2.233–5.478 s with all four reference hashes unchanged. Remove the optimization entirely, including its extra end-clock and ordering arguments. Every following-file message reaches the established ordering/planning checks; no cutoff can conceal a gap or discard its valid bridge. The existing captured-message regression covers all three reviewed boundary cases.
- Published-reader measurement (spot-snapshot): complete partition 2.612, 2.195, 2.212 s; Python CPU 1.070, 0.677, 0.680 s; cold process to first completion 3.259 s; peak Python RSS 584.47 MiB. All four persisted reference hashes match in each repetition.
- Published-reader measurement (perp-snapshot): complete partition 4.066, 4.084, 3.779 s; Python CPU 3.105, 2.803, 2.670 s; cold process to first completion 4.681 s; peak Python RSS 1665.38 MiB. All four persisted reference hashes match in each repetition.
- Published-reader measurement (spot-update-only): complete partition 4.903, 5.314, 4.597 s; Python CPU 3.614, 3.323, 3.220 s; cold process to first completion 5.532 s; peak Python RSS 1202.34 MiB. All four persisted reference hashes match in each repetition.
- Limits/Next: same original bytes/headers, fresh setup, actual storage admission, four writes/proofs and activation as Entry 010. Provider transfer/pacing, production concurrency/headroom, complete historical coverage and backfill ETA remain unqualified. Finish the final current-input contract matrix and review evidence; production remains stopped.

## Entry 014 — 2026-10-19

- Task/evidence: [#554](https://github.com/Vaquum/Origo/pull/554), [runtime CI on 14ddb93](https://github.com/Vaquum/Origo/actions/runs/39250814012).
- I found — good: 1,016 Linux cases passed before interruption. Local lint, benchmark CLI, captured boundary regressions and focused book regressions passed on this source.
- I found — bad: after repairing the retained-read count, the same full real-archive contract also asserted the removed `phase=reconciled` message. Require the actual `operation=canonical phase=completed` event instead; keep every row-count, evidence, partition selection and duplicate-detection assertion. Include the complete failing scenario in the focused check before the final full matrix.
- Measurements: no new performance experiment or product change; Entry 013's published-reader measurements still apply. Current-input full contracts and CI remain required.

## Entry 015 — 2026-10-19

- Task/evidence: [#554](https://github.com/Vaquum/Origo/pull/554), native GUI backfills `pjkqltbh` and `ezxvotwm`, original spot hours 2025-07-15T20Z and 2026-10-04T09Z. Private before/after run receipts, screenshots and persisted component proofs are retained under `storage/book-columnar-replay/`.
- I found — bad: each book partition still launched the unpartitioned completion observer, repeating source-wide health work and another worker process. Sources with no consumers need only canonical completion in their backfill selection; preserve their dedicated health job and recover generic backfill failures directly after successful canonical execution. Sources with consumers keep their publication barrier.
- I tried: selected the same two original hours in native Dagster controls, with fresh databases, identical original bytes/headers, genuine Linux ClickHouse volume, unchanged storage guards, two-CPU ClickHouse and two-CPU GUI containers. No simultaneous local contract suite. Python 3.12 GUI runtime differs from Entry 013's Python 3.11.11 benchmark.
- Measurements: update-only hour 13.572 to 8.445 s; snapshot hour 7.242 to 3.967 s, including each run's worker launch and complete canonical execution. Both selected partitions succeeded, with one canonical asset each; all eight persisted component proofs match the independent references. These are paired single workflow observations, not a repeated throughput benchmark. Parent elapsed time was 49 s before and 57 s after, including the private queue and daemon polling; do not count that delay as processing time or infer total backfill throughput.
- I found — bad: CI on a3b71c8 exposed a cadence test creating runs with both single-partition and range tags; the reduced job now exposes that invalid native combination. Remove the inherited single-partition tag when constructing its explicit range case. Assert the canonical-only backfill and retained dedicated health job. Preserve the same outstanding-work suppression and unrelated-failure checks.
- Limits/Next: Entry 013's repeated processing measurements remain applicable to the unchanged replay core. Provider transfer, production concurrency/headroom and complete historical coverage remain unqualified. At 12:24 UTC production backfill remained canceled with zero active children and no queued runs; the metadata-maintenance job was active. Local GUI fixtures are stopped after collecting evidence. Complete current-source contracts and CI before any deployment consideration.

## Entry 016 — 2026-10-19

- Task/evidence: [#554](https://github.com/Vaquum/Origo/pull/554), implementation `1ca17d0`, scheduled slice reviews on #553, private `native-receipts-qualification.jsonl` and `native-receipts-point-query-log.json`.
- I found — bad: the no-consumer shortcut removed authoritative native child receipts along with the redundant full-selection wait. Restore the receipt write and exact partition revision/build/generation/component-proof checks; only consumer sources need the full-selection join. Restore the actual book job's proof-deletion and concurrent-activation negative tests, and preserve its required acceptance-node identity. Keep the global observer out of the hourly job, with its dedicated health job and generic failure recovery retained.
- I tried: three fresh-database native multiprocess executions for each of the same two genuine spot hours, using normal storage guards, original bytes/headers, four outputs and actual child receipts. Python 3.12, owned two-CPU GUI execution container and two-CPU/2-GiB ClickHouse, no simultaneous local contract suite. This uses Dagster's execute_job API; a final GUI rerun was unavailable because browser tabs failed to attach. Entry 015's earlier GUI selection/launch remains separately recorded and is not represented as current-code timing. Queue dispatch, parent polling and provider transfer are excluded here.
- Measurements: update-only child run 7.078, 7.863, 6.906 s; snapshot child run 3.847, 3.835, 3.640 s, including native step-worker launch and receipt verification. Each database has both selected original hours; all eight persisted independent-reference proofs and both child receipts match in every repetition. No production throughput or ETA follows from these measurements.
- Physical-read evidence: the actual point-read SELECTs each read one activation row in the two-month original-input database (29,808 bytes query memory). This establishes pruning in that small genuine case, not realistic accumulated-history scaling. Compressed input and Python RSS measurements from Entry 013 still describe the unchanged replay core; no combined-process peak is inferred.
- Review disposition: the five-second upper collector-receipt-age limit is live-only on base ac0d304 (BookSampler.accept checks archive_start_ms is None). Archives already allow delayed receipts, while retaining ahead-of-receipt rejection and exchange-to-grid freshness. Do not introduce a new historical receipt-age restriction. Correct the slice's routing, numeric local qualification, packaging, migration and explicit no-merge release boundary. Full current-input contracts/CI remain required; production stays stopped.

## Entry 017 — 2026-10-19

- Task/evidence: [#554](https://github.com/Vaquum/Origo/pull/554), [CI on d6c67d1](https://github.com/Vaquum/Origo/actions/runs/39256294833). Tests never started: Ubuntu unattended-upgrades held the dpkg frontend lock during compiler installation.
- Repair: use the existing compiler when present; otherwise install it with apt's bounded 300-second lock wait. Keep every test and dependency requirement. Do not retry unchanged CI inputs. Local lint and CLI passed before this workflow edit; stop obsolete validation and verify the published final inputs again. At 13:01 UTC production remained canceled with zero active backfill children and zero queued runs; routine metadata maintenance was active.
- Measurements: no new performance claim; Entry 016's repeated receipt-preserving native executions remain current. No merge, deployment or resumption.

## Entry 018 — 2026-10-19

- Task/evidence: [#554](https://github.com/Vaquum/Origo/pull/554), [Linux CI on 4b33325](https://github.com/Vaquum/Origo/actions/runs/39256869079), local complete ordinary run 59f1b511-3aba-4b38-974c-ae1da400b3fd.
- Measurements: Linux passed 1,057 cases before the cleanup-verdict fault test failed; the full local pass collected 1,084 passes, eight failures and one existing platform skip in 442.73 s. Neither matrix passed. All 24 focused source/book regressions passed.
- I found — bad: the cleanup test injected its integrity error into the removed repeated reconcile, so only the secondary capacity-cleanup error occurred. Inject the same primary failure through canonical build and its repair path, retaining the exact verdict/log assertions. Seven other local failures are unchanged rally API tests rejecting 1.78–2.06 GB of working reservation against their unchanged 1.5-GiB budget after heavy book work in the same pytest worker; those cases passed in Linux CI. No rally code or resource limit changes are justified.
- Repair/Next: keep every ordinary case, but run the independent rally-service module in fresh local pytest processes rather than carrying archive-replay allocator state into it. Record that runner as a validation input. Resource tests remain a separate serial phase, all limits unchanged. Reproduce the primary-verdict failure first, then complete both current-input matrices. Replay code and Entry 016's measurements remain unchanged; production stays stopped.

## Entry 019 — 2026-10-19

- Task/evidence: [#554](https://github.com/Vaquum/Origo/pull/554), review 4314350901 and [CI on a898956](https://github.com/Vaquum/Origo/actions/runs/39258542576). Linux ordinary/resource execution passed; its acceptance manifest then found that parametrizing a consolidated provisional-recovery test renamed its required fresh-case identity.
- Repairs: preserve each original fresh-case ID while retaining the new retained/repair cases. An update chain that fails while crossing from a pre-hour clock into the opening grid has an unproven seed, so allow the existing bounded prelude search; gaps after an established in-hour clock remain sequence failures. Reassociate complete genuine July messages across files, preserving every value/clock/ID, to exercise the missing opening bridge and require the unchanged original reconstructed-hour digest.
- Local validation environment: Docker Desktop stopped before one focused setup; the recorded failure was a missing docker.sock, not a replay result. Restore the daemon, verify the unchanged database fixture image builds, and record the actual engine dependency in the validation inputs. Never turn these setup errors into passes or unchanged retries.
- Next: publish these repairs, reproduce the opening-boundary case, repeat the original complete performance workloads, and complete current-input local/CI gates. Production remains stopped; its role is final acceptance after local proof, never iterative performance experimentation. No new timing claim is made here.

## Entry 020 — 2026-10-19

- Task/evidence: [#554](https://github.com/Vaquum/Origo/pull/554), source `3660c49`, private `qualified-opening-summary.json` and `opening-query-log.jsonl`. The genuine-message opening-bridge regression, closing-boundary regression, focused contracts, acceptance collection, lint and benchmark CLI passed. The original required acceptance identities are present; no manifest or resource budget changed.
- I tried: repeat all three Entry 013 original workloads three times, each in a fresh scratch database, after the opening-chain repair. Same Mac, Python 3.11.11/native decimal, Polars 1.44.2 with two threads, ClickHouse 25.3.2.39 limited to two CPUs/2 GiB and the real host data mount. Original bytes/headers, ordinary shared admission, complete setup, four writes/proofs and activation; no simultaneous local contract suite. OS input cache warm; provider transfer/pacing and Dagster startup excluded.
- Measurements (spot snapshot): 2.794, 2.108, 2.047 s; Python CPU 1.040, 0.654, 0.662 s; external cold process to first completion 3.401 s; peak cumulative Python RSS 591.06 MiB.
- Measurements (futures snapshot): 3.749, 3.363, 3.414 s; Python CPU 2.837, 2.431, 2.473 s; external cold process to first completion 4.345 s; peak cumulative Python RSS 1671.98 MiB.
- Measurements (spot update-only): 4.653, 4.347, 4.235 s; Python CPU 3.431, 3.048, 3.058 s; external cold process to first completion 5.235 s; peak cumulative Python RSS 1242.59 MiB. Every persisted component hash matches its independent original reference in all nine builds.
- Database comparison: 756 completed scratch queries total 5.516 s of query execution across the nine builds; the 36 component-proof queries read at most 36,000 rows and use at most 112,859,400 bytes of query memory. These small fresh databases do not qualify accumulated-history scaling. Python RSS is not combined-process peak memory. No production throughput, provider behavior, concurrency headroom or whole-backfill ETA is inferred.
- Next: finish the published current-input ordinary/resource suites and CI, then re-read reviews and live-base evidence. No merge, deployment or resumption; production remains the final acceptance boundary after local proof.

## Entry 021 — 2026-10-19

- Task/evidence: [#554](https://github.com/Vaquum/Origo/pull/554), review 4314581409. All eight declared local checks passed on `9a2f1c0`: 1,093 ordinary cases plus one existing platform skip, ten serial resource cases, focused regressions, collection, lint, CLI and repeated complete performance qualification. Review nevertheless found another real opening-state omission; those passes do not discharge it.
- Repair: an initial snapshot has no proven exchange clock. A missing link before that first clock must request the same bounded seed search as a pre-hour update, rather than reject the hour as an established in-hour gap. Change only that condition. Exercise both initial-state forms by reassociating complete genuine July messages; retain the unchanged reconstructed-hour digest and all established-gap rejections. No clock, update ID or level is invented.
- Next: publish the repair before checks, prove both opening cases, re-profile the unchanged valid processing path against the original independent component references, and finish current-input local/CI/review evidence. Production remains stopped; no merge, deployment or resumption.

## Entry 022 — 2026-10-19

- Task/evidence: [#554](https://github.com/Vaquum/Origo/pull/554), managed focused check `5b2210fe-db5a-42ff-bf16-ccfc469d00c3`. Twenty-six cases passed; the new snapshot variant failed before replay because the selected July 15 update-only prefix contains no snapshot. This is a test-input mistake, not a successful product result.
- Repair: inspect the genuine recordings, then use the actual July 30 snapshot and July 31 reconstruction case for the untimed-snapshot variant. Reassociate complete recorded messages and require that case's independently recorded original-hour digest. Keep the July 15 pre-hour-update variant and its own digest. No generated market records, weaker assertions, unchanged retries or production experiments.

## Entry 023 — 2026-10-19

- Task/evidence: [#554](https://github.com/Vaquum/Origo/pull/554), source `0dc9703`; all 27 focused contracts, acceptance collection, lint, CLI and closing-boundary regressions pass. Both genuine opening-state cases recover with their original hour digests. The review finding has a public evidenced fix and resolved thread. Private reports preserve both benchmark batches rather than replacing the slower result.
- Measurements: same three original workloads, three fresh databases each, same setup/transport/output/proof/activation boundary and hardware as Entry 020. Spot snapshot 2.908/2.279/2.653 s; futures snapshot 4.391/3.574/3.539 s; spot update-only 5.334/4.713/4.453 s. All independent component hashes match; peak Python RSS stays below 1,670 MiB. External cold times are 3.569/5.019/6.004 s. The update-only cold launch exceeds the specified six-second qualification by 0.004 s; this batch is not a strict cold-limit pass.
- Diagnosis/comparison: startup adds 0.627–0.670 s; one-minute host load during those first partitions was 23–28, versus about five in Entry 020. Subsequent observation found load 9.78 with 53.33% CPU idle. These observations suggest contention, not a proved causal attribution. Preserve the miss and make one comparison without product changes or interference with other workloads. Later load during that comparison was still 18–19; do not call the host quiescent or infer a guaranteed bound.
- Comparison measurements: spot snapshot 2.518/2.154/2.296 s, futures snapshot 3.917/3.836/3.856 s, spot update-only 5.250/4.901/4.914 s. External cold times 3.133/4.530/5.880 s; peak cumulative Python RSS 601.33/1669.05/1233.97 MiB. All 36 independent component comparisons match. This batch meets the recorded local qualification; both batches remain evidence of shared-host variation. Provider transfer, native worker startup, production concurrency/headroom and complete coverage remain outside this measurement.
- Next: finish final published-head local/CI/review evidence. Do not change processing code to chase four milliseconds of cold-start variation, claim a portable latency guarantee, or restart production. No merge, deployment or resumption.

## Entry 024 — 2026-10-19

- Task/evidence: [#555](https://github.com/Vaquum/Origo/issues/555), production3.34.22/e2c4e602 after [#554](https://github.com/Vaquum/Origo/pull/554); complete native/event/database recordings in `storage/book-remote-acceptance`.
- I tried: after the operator authorized a bounded production retry, launch native spot selection `ugcqnhmx` for July15T20Z and October4T09Z/T10Z, then `mjysewkk` for June28T07Z–T22Z. No production patch or configuration change.
- I found — good: the first three succeeded in11.854/13.211/21.338s native run time. Twelve of the sixteen historical hours succeeded in22.125–44.096s (median27.066s). Fifteen activations, sixty component proofs and sixty physical output counts matched36000/3600/60/60 rows per hour. Eight hashes for the two independent local reference hours matched exactly.
- I found — bad: June28T07Z/08Z/13Z/15Z failed with the same preceding05Z404. Cancellation was requested immediately after the first failed inventory; remaining children finished while cancellation propagated. Final parent CANCELED; zero active children and zero global queued runs at14:47:57UTC. Consistent few-second native cost and sustained whole-backfill throughput remain unproven.
- Measurements: successful component write/validation stages0.870–1.174s; fetch/replay before those stages5.510–29.388s; after validation until canonical completion2.455–9.146s. These boundaries do not isolate HTTP, decoding or activation. Query statements naming the fifteen successful build IDs total18.265s and maximum individual query memory112777288bytes; partial attribution, not total database cost or combined peak memory. Source-row throughput and routine freshness under sustained load were not measured.
- Progress/Next: production bulk stopped, return locally; retain the original replay failure when lookback discovery fails and measure actual authenticated HTTP stages separately. Do not infer a whole-backfill ETA from these small selections.

## Entry 025 — 2026-10-19

- Task/evidence: [#555](https://github.com/Vaquum/Origo/issues/555); unchanged native replay at e2c4e602 with private diagnostic builds, original hashes in `storage/book-remote-acceptance/originals/capture.json` and original June28T06Z–T08Z fixture pins.
- I tried: capture the remaining June28T09Z–T16Z originals locally with the production authentication shape, then replay all four failures with every available prefix from06Z. Separately prototype a backward knowledge pass using later unchanged levels and opposite-side zero witnesses, including the first following checkpoint.
- I found — good: every original failure reproduces as BOOK_KNOWN_DEPTH_EXHAUSTED before the missing05Z lookup. For07Z with06Z prefix, the first failure is07:56:24UTC: requested200th ask107508.50 exceeds proven107506.11. The current/following selected objects exist; the missing object is older context. Local lookup independently returned404 for05Z.
- I found — bad: all fifteen retrospective prototype attempts still rejected; zero recovered hours. Discard the prototype; it changes no product code and is not evidence that later snapshots can recover these hours. Present originals do not establish complete requested depth. More remote retries with unchanged inputs are not a repair.
- Measurements: baseline replay-only attempts0.280–1.578s across all four targets/prefixes, excluding input preparation, network, database and native launch. These are negative attempts, not delivered throughput. Detailed failure clocks, sides, thresholds and input pins are retained locally. Combined peak memory not measured.
- Progress/Next: preserve the concrete replay reason plus exact missing dependency; freshly recheck negative HTTP evidence before automatic rejection reuse. This removes repeat work without relabeling missing depth as accepted coverage. Provider correction or additional authoritative input remains necessary for any genuinely unprovable state.

## Entry 026 — 2026-10-19

- Task/evidence: continuing operator mandate under [#525](https://github.com/Vaquum/Origo/issues/525); local `storage/book-remote-acceptance/http-ablation/results.json`, exact existing authenticated POST/Bearer/GET/Range transport; unchanged version3.34.22 network/replay behavior.
- I tried: matched October4T09Z/T10Z current/following body plus metadata calls, preserving payload hashes, local shared provider pacing and original request limits. Compare new per-call connections, per-object connection reuse, and two bounded transfers with reuse.
- I found — good: completed sequential baseline batches7.273s (cold token) and11.567s; connection-reuse batches5.936/6.099s; one two-transfer batch3.453s, identical original full-body hashes. This identifies overlap as a candidate, not a demonstrated repeated gain.
- I found — bad: the second two-transfer batch hit the unchanged30s ReadTimeout and did not finish. The provider/network contribution is variable; the failed batch is retained, not replaced with the earlier success. No product transport change is justified from one successful parallel batch.
- Measurements: only the four real HTTP calls and process-local token when cold; no replay, database, Dagster or memory measurement. Maximum successful baseline individual metadata call5.402s. No sustained throughput or completion-time projection.
- Progress/Next: finish the narrow rejection correction separately. Further local transport qualification must record each request failure and repeat matched variants before selecting an implementation; retain the current timeout, error and quota behavior.

## Entry 028 — 2026-10-19

- Task/evidence: [#555](https://github.com/Vaquum/Origo/issues/555), review [4315534894](https://github.com/Vaquum/Origo/pull/556#discussion_r4315534894).
- I tried: distinguish the concrete missing archive from the request wrapper's generic HTTP404 before retaining a lookback rejection.
- I found — bad: `_response` can mint/refresh a token before asking for the archive; that endpoint's404 has the same generic error code. Catching only the code could hide an authentication-service failure during either fetch or rejected-input discovery. The initial eleven targeted regressions passed but did not cover this boundary.
- Progress/Next: use an archive-specific SourceError subtype without changing its public404 code; add both original-input token-refresh transitions. Stop superseded local verification, publish the correction, then repeat required checks. No performance gain or production success is claimed by this repair.

## Entry 030 — 2026-10-19

- Task/evidence: [#555](https://github.com/Vaquum/Origo/issues/555), scheduled document review and code inspection of final-prefix termination.
- I found — bad: advancing rejection policy5 to6 would invalidate unchanged existing rejections despite an additive dependency representation. The missing-object branch also wrote non-whitelisted errors as rejection evidence, although the reader could not reuse them.
- Progress/Next: retain policy5; write negative evidence only for the existing rejection codes. Extend existing owners for automatic suppression/explicit retry, metadata changes, newly missing present inputs, partition-targeted token404, and final-prefix acceptance. Reassociate complete original July15 messages across one object boundary as an explicitly controlled protocol fault; preserve every market value, clock, ID and expected output hash. No new throughput result is claimed; published current-input checks follow this correction.

## Entry 027 — 2026-10-19

- Task/evidence: [#557](https://github.com/Vaquum/Origo/issues/557), local authenticated overlap-only ablation retained in the attached book-backfill-cost checkout at `storage/book-remote-acceptance/http-overlap-only/results.json`. Same four production-shaped requests and original October4T09Z/T10Z body hashes as Entry026; no connection cache or transport limit change.
- I tried: separate overlap from connection reuse. Alternate three batches of two concurrent existing-transport requests with three sequential batches, including both body downloads and final Range metadata. The first parallel batch includes token minting; the others reuse the same process token.
- I found — good: every batch completed; parallel3.478/2.928/3.387s versus sequential6.602/6.131/4.957s. Original full-body hashes match. The median batch reduction is44.8% in this local sample, attributable to overlap without the proposed connection cache. Adopt only a two-request executor for the existing required pair.
- I found — bad: Entry026's earlier connection-reuse timeout remains evidence of network variability; these six successful batches do not disprove it. Do not add a connection/session cache based on this result or infer that all prelude/request/launch overhead is solved.
- Measurements: exact authenticated POST/Bearer/GET/Range behavior, local existing shared quota ledger; request-batch wall time only. Provider transfer on the production host, complete pipeline, native launch, sustained bulk throughput and peak combined memory are not measured by this ablation.
- Progress/Next: overlap only current/following bodies and their final identity checks, preserve deterministic errors and every original proof. Require complete local/native qualification before another bounded production attempt; retain the genuine unprovable-hour failures.

## Entry 029 — 2026-10-19

- Task/evidence: [#557](https://github.com/Vaquum/Origo/issues/557), reviews [4315605320](https://github.com/Vaquum/Origo/pull/558#discussion_r4315605320) and [4315605336](https://github.com/Vaquum/Origo/pull/558#discussion_r4315605336).
- I found — bad: eagerly retrieving the closing future could surface its transport error before current identity/size/preparation validation or an earlier dependency identity. The initial twelve original-input concurrency/error tests did not cover validation priority.
- Progress/Next: retain the two-request resource scope but consume the closing future only at its original validation point. Add six distinct validation boundaries using genuine bodies and explicitly faulted metadata/preparation. Include the missing Entries024–026 verbatim so the failed experiment referenced by Entry027 is present in this PR's log. No new performance result is claimed by the review correction.

## Entry 031 — 2026-10-19

- Task/evidence: [#557](https://github.com/Vaquum/Origo/issues/557), failed native-source CI run39276669427, historical404 test.
- I found — bad: the test asserted that the error names the last callback's file. Independent current/following requests now finish in either order; callback completion order is not the selected-partition failure contract.
- Progress/Next: assert the selected current archive explicitly, retaining terminal failure, no-retry, open-failure and no-canonical-proof assertions. This is a fixture correction for actual failed CI, not a retry waiver. No performance result is claimed.

## Entry 032 — 2026-10-19

- Task/evidence: [#557](https://github.com/Vaquum/Origo/issues/557), product dca724f5618854d4d147938d1c62288a10d0442c, [complete local qualification and stage evidence](https://github.com/Vaquum/Origo/pull/558#issuecomment-6271695576). Exact authenticated requests, fresh scratch databases, native cold workers, app2CPU/3GiB and CH2CPU/2GiB in owned local Linux aarch64 containers. This is not production hardware or a sustained-capacity result.
- I tried: three alternating repetitions for genuine spot/perpetual snapshot and spot snapshot-free hours, sequential adapter00953f3 versus bounded overlap. The first harness attempt failed before HTTP because retries config omitted the multiprocess selector; fixed and retained. An independent cached-input stage profile excludes network, database and startup.
- I found — good: all18 native runs,72 independent component hashes,72 physical counts and18 completion receipts match;33,861,318 completed pinned current-source rows across both variants. Baseline native seconds: spot10.709/9.626/11.501, perp13.966/14.243/13.949, snapshot-free20.080/21.396/18.331. Overlap: spot7.345/8.346/7.374, perp11.357/13.624/11.947, snapshot-free19.160/17.434/17.784. Median reductions31.1%,14.5%,11.4%. Cold-process verified completion and launcher/teardown are separately reported.
- I found — bad: snapshot-free17–19s still misses the consistent few-second goal. Each run has93 database statements including setup and verification, at most0.626s summed query duration and36000 rows per individual scan; the database does not account for the remaining elapsed time. First harness timing hooks logged each HTTP span twice; use interval unions, never double their durations or request counts. No production restart or backfill ETA follows.
- Measurements: cached-body fetch/revalidation0.750–0.948s spot,2.122–2.235s perp,3.432–3.633s snapshot-free; native replay14–86ms across attempts. App cgroup peaks859–2027MiB; separate ClickHouse peak1679MiB. Combined simultaneous peak is not measured. Exact runtime/image/input pins and inclusive stage boundaries are in the evidence.
- Further local ablation: each metadata recheck asks the API for another signed storage reference. Authenticated API302 took0.931–1.474s versus R2 Range206 0.189–0.281s; provider-spacing state is0, so no observed provider-header pacing constraint explains that gap. Three matched two-worker batches verified unchanged original identities using already-issued unversioned300-second R2 references, keeping the logical shared quota and timeout. A filename-shape assertion initially failed: the actual object ends .parquet.zst; corrected by using the returned opaque reference. Full batch timings and the retained failure are linked. This next candidate changes no product code in #558.
- Progress/Next: finish current-input checks for the bounded-overlap change; separately qualify operation-scoped reference reuse before implementation/adoption. Keep fresh ETag checks, expiry handling, original provider failures and credentials out of logs/persistence. GitHub Actions is currently degraded; do not substitute historical CI for current-head success. Production bulk remains stopped.

## Entry 033 — 2026-10-19

- Task/evidence: [#559](https://github.com/Vaquum/Origo/issues/559), [exact local reference ablation](https://github.com/Vaquum/Origo/pull/558#issuecomment-6271695576), following Entry032. The earlier metadata-source-path assertion failed because real storage names add .zst; retained in the local managed log, then corrected using the returned object reference.
- I tried: three alternating batches of four genuine archive metadata checks, two requests at a time, normal authenticated API resolution versus the already-issued unversioned R2 GET reference. Every attempt charges the same logical provider ledger; existing timeout and request transport remain unchanged.
- I found — good: API batches2.379/2.416/2.055s; issued-reference batches0.589/0.535/0.587s. All original ETag identities match; median batch reduction75.3%. Actual references expire after300s and sign the host. This is HTTP-only evidence, not complete-pipeline latency or a production throughput projection.
- Implementation/Next: retain at most26 references only inside the existing source execution scope. Recheck the pinned physical object's ETag, reserve the existing5+30-second request budget before signature expiry, and re-resolve the official route on expiration or a referenced401/403/404. Retain shared logical provider charging, authentication-failure provenance, two request workers, unchanged output identity, and redacted transport errors; never persist/log signed URLs. Only matching HTTPS unversioned R2 keys qualify. Unsupported references keep the prior route. Finish full current-input contracts and repeated complete native qualification before deployment; production remains stopped.

## Entry 034 — 2026-10-19

- Task/evidence: [#559](https://github.com/Vaquum/Origo/issues/559), current source1bc8222, local native qualification attempt and sanitized reference observation retained in `storage/book-download-references`.
- I found — bad: focused protocol tests42/42 passed, but their controlled URL used the wrapped .parquet.zst suffix even for plain Parquet bodies. The initial native comparisons preserved all delivered proofs but failed to activate reference reuse for the two plain-file cases; metadata still took1.14–1.22s. Wrapped-file checks did use the reference in0.33–0.53s. Stop the owned local batch; its partial results remain evidence, not completed qualification.
- Diagnosis: exact spot/perpetual October4 references match the requested .parquet key; July15 matches .parquet.zst. Both are unversioned host-signed300-second references. No provider request or source clock ordering guarantee was inferred from the filename.
- Repair/Next: accept only those two exact key forms; make the protocol fixture suffix follow its genuine body's wrapper magic. Repeat current-input checks and the complete alternating native comparison. No new native speed claim or production restart. Operator confirmed that, after qualification, other hours should continue while genuine provider-data gaps remain visibly failed.

## Entry 035 — 2026-10-19

- Task/evidence: [#559](https://github.com/Vaquum/Origo/issues/559), review [4316512247](https://github.com/Vaquum/Origo/pull/560#discussion_r4316512247).
- I found — bad: optional reference date/expiry parsing could raise ValueError after a successful archive response. Such metadata must not prevent ordinary data ingestion. An unsupported replacement must also invalidate any older remembered reference for the same file.
- Repair/Next: clear the old entry first; malformed, nonfinite, overflowing or too-short reference lifetimes remain unusable and keep the original API route. Extend the existing protocol owner for malformed dates, malformed/overflowing expiry and stale-entry invalidation, keeping every genuine market row unchanged. Re-run published current-input contracts and native qualification; no speed or deployment result is claimed by this repair.
