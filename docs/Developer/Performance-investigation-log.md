# Performance investigation log

Continuous, append-only record of performance investigations, benchmarks and
optimizations throughout Origo. Read it before performance work and before any
ingestion work, together with the [ingestion performance rules](Ingestion-performance.md).

Append an entry for every attempt while doing the work, and complete its findings
before declaring the task complete. Record successes, failures, regressions,
unchanged results and disproved hypotheses. Use `not measured`, `unknown` or
`none established` when appropriate; never invent a measurement or learning.
Preserve previous entries. Append corrections referencing the entry they
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
