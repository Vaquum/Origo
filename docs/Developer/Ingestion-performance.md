# Ingestion performance rules

Read and apply these rules before any ingestion or performance work. Record each
performance investigation, benchmark and optimization in the
[performance investigation log](Performance-investigation-log.md), including
failed approaches and unchanged results.

1. **Profile every stage.** Compare end-to-end timings with database query logs before identifying the bottleneck.
2. **Benchmark real workloads** on deployed hardware, using identical inputs, controlled audit state and isolated scratch writes.
3. Don't **count database activity** as delivered throughput; measure completed source rows, elapsed time and resource use.
4. **Keep processing columnar.** Use vectorized parsing, bounded native database transport and set-based proof/write operations.
5. Don't **rescan accumulated history** on every partition or append; bound audit and hashing work.
6. **Verify physical pruning.** Measure rows scanned; logically bounded SQL can still read the entire history.
7. **Tune measured constraints.** Sweep concurrency with enough partitions; enforce shared provider quotas, pacing and bounded request windows.
8. Don't **generalize request behavior** across authentication, hosts, cursors or page limits; test the exact production request.
9. **Preserve deterministic identity:** raw source IDs, timestamp units, precision and stable payloads; exclude run-volatile metadata.
10. **Recheck numeric outputs** after changing transport, block sizes, ordering or aggregation threads; floating-point results and hashes can change.
11. Don't **invent source guarantees** about ordering, uniqueness, contiguity or historical availability.
12. **Audit complete coverage** against the expected partition set and authoritative proofs; the latest successful date proves no earlier coverage.
13. Don't **infer minute completeness** from a closed interval or one empty response; verify boundaries, page continuity and empty-minute evidence.
14. **Reconcile from proof.** Skip matching data; repair mismatches, recompute proof after mutation and preserve append-only canonical history.
15. Don't **rebuild unchanged outputs.** Reuse valid generations and files; invalidate only consumers whose actual inputs changed.
16. **Activate complete generations.** Validate privately, preserve previous visibility on failure, and publish staged files only after rechecking pinned inputs.
17. **Isolate live capture** from expensive repair and publication; processing capacity must exceed incoming work.
18. **Protect routine capacity.** Benchmark backfills alongside ingestion and publication, measuring dispatch latency, contention and consumer freshness.
19. **Share execution safeguards** across runners and native Dagster launches; version configuration and verify actual deployed dependencies.
20. Don't **declare success** from passing tests, heartbeats or deployment alone; fix root causes and verify delivered outcomes through every declared consumer.

These rules combine the
[Playground investigation log](https://github.com/Vaquum/Origo-Playground/blob/main/spec/live-performance-investigation-log.md)
with Origo's [source onboarding](Source-onboarding.md),
[orchestration](Orchestration.md) and
[recorded implementation history](../../CHANGELOG.md).
Historical measurements apply to their recorded workloads and environments;
remeasure the current path before choosing limits or claiming an improvement.
Independent floating-point rebuilds need not be bit-identical. Per-file and
manifest replacement does not make all outputs one atomic transaction.
