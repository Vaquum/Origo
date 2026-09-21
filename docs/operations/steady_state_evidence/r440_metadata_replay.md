# S439 / R440: isolated regression evidence

This report establishes the first regression gate only. It does not certify the whole
S439 implementation, production recovery, or steady state.

## Inputs and environment

Production metadata was captured read-only at approximately 12:59 UTC on September 21, 2026.
It contains 25,380 activation records, 123,323 component receipts and four anchors,
with no raw market payloads or secrets. Exact SELECTs, cutoff and checksums are in
`tests/fixtures/steady_state/provenance.json`. This is a bounded multi-query capture,
not a database-wide transaction; the replay verifies all active component proofs.

Tests use an owned local Docker Desktop container built from the repository digest-pinned
ClickHouse 25.3.2.39 image on aarch64. No production mutation or load test was performed.

## Reproduction and repair

The original query through source_current_partitions fails with Code 241 under the
512 MiB test cap on this authentic corpus. Its separate production failure was at 2 GiB.
The replacement uses two source-scoped queries and an O(n log n) interval merge.

| Source | Elapsed seconds | Peak server query MiB | Peak traced Python MiB |
|---|---:|---:|---:|
| binance_spot_trades | 1.141 | 36.11 | 21.86 |
| binance_perp_trades | 0.665 | 21.89 | 11.45 |
| binance_spot_aggtrades | 0.651 | 22.55 | 12.09 |
| binance_perp_aggtrades | 0.462 | 14.91 | 8.47 |

Elapsed includes both reads and Python proof validation/coverage calculation. Python
memory is tracemalloc peak, not total RSS. Raw query IDs, read rows/bytes and memory
are retained in the adjacent JSON. These are reference-host measurements, not production
latency or sustainable-ingestion capacity measurements.

M16 injects a source-local lookup error while the other three sources execute genuine
adapter response replay, ClickHouse build/activation and Parquet/Arrow publication.
All twelve series per unaffected source are physically checked against manifest hashes.
Restoring the failed lookup exercises that source's real build/publication and recovery.
Only the replay calendar is restricted to each authentic recorded minute.

## Validation

```sh
pytest tests/origo_source_native/test_steady_state_metrics.py -q -s
pytest tests/origo_source_native/test_steady_state_capacity.py -q -s
pytest tests/origo_source_native/test_provisional_worker.py -q
pytest tests/origo_source_native -q --maxfail=1
```

Initial combined lookup/worker run: 16 passed. Isolation run: 1 passed.
Initial full source suite: 471 passed in 552.53 seconds. Later CI results belong to
the exact PR head and do not retroactively certify untested changes.

## Outstanding acceptance

Post-deployment 30-tick recovery, the remaining S439 implementation, the six-hour
mixed-workload trial and the 72-hour production window remain unestablished. This
report is not authorization to close S439 or declare the service steady.
