# Market state cube delivery

[PRD-0022](https://github.com/Vaquum/Origo/issues/462) is the locked product specification.
Review concerns implementation correctness, performance, resource use and proof.
Only an explicit operator amendment changes L01–L17.

## Base projection — slice #466

`origo.sources.profiles.market_state` supplies two component declarations over one
logical base projection. Slice #469 registers both in the live spot source,
with applicability from 2021-01-01 and a persisted `market_state` activation group.
The base projection and its operational lifecycle do not yet deliver the query
API, Arrow file lifecycle or whole-history performance acceptance.

The builder reads its build-owned, validated `raw` or `raw_latest` table and writes
the corresponding precreated `market_state` or `market_state_latest` table.
The source runtime owns staging, component validation and activation. Failed
build output must never be activated or reused as a completed attempt.

| Column | Type | Meaning |
| --- | --- | --- |
| `time_index` | UInt64 | Floor of normalized UTC microseconds since 2021-01-01 divided by 56,250,000 |
| `price_index` | UInt64 | Floor of recorded price divided by 125 USDT |
| `first_trade_at` | DateTime64(6) | Earliest actual constituent trade timestamp |
| `volume` | Float64 | Sum of recorded `quote_quantity` in USDT |
| `trade_count` | UInt32 | Individual trade count |
| `taker_buy_volume` | Float64 | USDT volume where `is_buyer_maker = 0` |
| `taker_buy_trade_count` | UInt32 | Individual taker-buy count |

The primary key is `(time_index, price_index)`. All trades before the history
start are excluded. The recorded provider integer timestamp is not used for
membership because its unit changes between archives. Source normalization owns
that conversion; this builder uses the normalized datetime only.

A global cell can begin before its owning provisional minute. `first_trade_at`
is inside the owning interval and therefore remains the component's validation
time column. Existing source revision, build and partition metadata identifies
the contribution. Adjacent accepted minute contributions to the same global key
must be summed once; canonical replacement must supersede them. Both physical
component tables store the same base resolution. No coarser grids or raw copies
are introduced by this module.

Counts use ClickHouse `accurateCast(..., 'UInt32')`, rejecting overflow before
narrowing. Post-build count sums explicitly widen to UInt64 and must equal both
the eligible raw count and the eligible raw taker-buy count.

## Numerical proof

The base aggregation uses `sumKahan` and `sumKahanIf`, with one aggregation thread
and input ordered by normalized datetime and trade ID. Results retain Float64
roundoff; different build/block layouts do not promise identical final bits.
Stored component hashes still validate the exact retained contents.

Before implementation, slice #466 fixed independent volume comparison to
`math.fsum` over the existing Float64 quote quantities, with absolute tolerance
**1e-8 USDT** and relative tolerance **1e-12**. Compensated summation reduces
accumulation error; these thresholds allow low-order differences between direct
and fragment reductions on the authentic fixture magnitudes. Keys, counts,
taker side and eligibility permit no tolerance. Whole-history acceptance must
separately justify its thresholds before its first benchmark; these fixture
checks cannot establish whole-history numerical or performance acceptance.

Tests use checksum-verified, unchanged Binance CSV/REST records already committed
in the repository. Independent keys use original timestamp units and Decimal
prices, including authentic exact 125-USDT edges. Scalar conversion-limit probes
test UInt32 arithmetic without fabricating market records. Both legacy content
hashes and current binary content hashes remain supported; schema_version stays 1.

Run the first slice's engine checks with:

```sh
pytest tests/origo_source_native/test_market_state_projection.py -q
```

## Registration and operational rollout — slice #469

Deployment creates both revisions tables and current views while the persisted
component group remains off. Every build reads the flag under the shared heavy
maintenance fence; the enable operation holds that fence exclusively. Deployment
recreates Dagster, Dagit and the spot provisional worker, checks that their former
containers (and child writers) retired, then runs `origo.sources.rollout`.
No operator toggle, source configuration or separate projection launch is required.
The rollout marker is stored in `source_component_rollout_log`; a missing row is off.

This release is the binary rollback floor after activation. Pre-compatible images
reject expanded component inventories and must not be deployed after enablement.
Deployment enforces the floor: before any container is replaced, the new image runs
`python -m origo.sources.compatibility`, which fails when production's activations
name a component the image does not declare.
Native rollback to a retained legacy generation remains supported by this release:
old product hashes are accepted and the cube view hides the unselected addition.

Existing accepted days and minutes gain only their missing projection. The upgrade
holds the existing partition and maintenance fences, validates retained products,
reads a private view over retained raw, and preserves the original revision,
build identity, product rows and hashes. It adds a generation selecting the new
component hash. No provider refetch or second raw copy is required.
Completed, validated additions are reused after interruption; only never-activated
incomplete additions may be removed and rebuilt. The current view requires the
component hash in the selected activation, so staged additions stay invisible.

Native canonical reconciliation selects accepted days missing enabled components
in the batch slots that changed or failed partitions leave, so the history upgrade
never delays a repair; Dagster backfill, selected gaps and retries use the same lifecycle. The provisional
worker handles fresh minutes first, then a bounded batch of missing accepted
minutes. Existing consumer readiness remains independent of incomplete cube
coverage. Upgrade failures are visible as `component_upgrade` failures with scope
`NONE`, leaving valid existing products available. Capacity evidence is keyed to
the enabled component footprint, and admission/build share the maintenance fence.

Cube-only activation updates publication metadata without rebuilding unchanged
bar exports. Mount and Hugging Face renderers retain the full source-state tokens
for monitoring and also fingerprint the component inputs they actually consume.
Matching inputs preserve monthly Parquet files, Arrow links and Hugging Face
version directories/upload evidence. A legacy manifest can establish equality
only through an actual retained activation whose full token matches its evidence.
Missing proof or changed inputs takes the normal render path. Publication checks
canonical inputs again before commit and retains the original pinned provisional
records; it cannot claim minutes that arrived during rendering.

The existing monitor evaluates **M1** (current cube proof, raw/trade and taker-count
agreement, 180-second freshness) and **M2** (calendar coverage from 2021-01-01).
The same law tape, Sources catalog, `/law` view and alert path carry these results.
Historical gaps remain visible even while current ingestion is healthy; deployment
and registration alone do not establish complete historical coverage.

Run the real-engine operational checks with:

```sh
pytest tests/origo_source_native/test_market_state_registration.py -q
```

Native GUI acceptance on 2026-09-24 used the committed, checksum-proven
2025-01-01 capture in a disposable ClickHouse/Dagster environment. Selecting the
retained day in the native partition bar and launching one run upgraded generation
1 to 2; native **Re-execute all** kept generation 2. Both runs succeeded in about
six seconds. SQL confirmed all seven original hashes and the build/revision were
unchanged, with one raw build and one cube receipt: 15 cells accounted for all
12,000 captured trades and 5,310 taker buys. Capacity sampling was controlled test
evidence; this does not establish production capacity or complete historical coverage.

## Remaining PRD delivery

- Protected query snapshots, exact dyadic selections, sparse Arrow files, POCs,
  a local API and expiry 24 hours after actual last access.
- Disclosed real-history benchmark and resource/recovery evidence, including
  full history at base resolution and ingestion/publication contention.
