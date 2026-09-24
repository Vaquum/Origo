# Market state cube delivery

[PRD-0022](https://github.com/Vaquum/Origo/issues/462) is the locked product specification.
Review concerns implementation correctness, performance, resource use and proof.
Only an explicit operator amendment changes L01–L17.

## Base projection — slice #466

`origo.sources.profiles.market_state` supplies two component declarations over one
logical base projection. They are not registered in the live spot source yet.
This slice delivers the aggregation module; it does not claim deployed coverage,
a query API, file lifecycle, historical migration or production performance.

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

## Activation prerequisites and remaining delivery

1. Deliver mixed-footprint validation and component applicability without
   changing existing source history or publication semantics. Deploy compatibility
   before activating expanded footprints: old binaries reject additional hashes.
2. Build missing eligible contributions from retained raw under source partition
   and maintenance locks; preserve existing product data and hash validation.
   Make interrupted/resumed upgrade and rollback safe without raw duplication.
   Keep incomplete additions invisible until their evidence is activated.
3. Integrate automatic backfill, gap/retry, corrections, reconciliation, readiness
   and capacity remeasurement. Verify native Dagster behavior on isolated real
   data in the actual GUI before claiming operator delivery.
4. Deliver protected query snapshots, exact dyadic selections, sparse Arrow files,
   POCs, a local API and demonstrated actual-access expiry. Freeze signatures,
   schemas, errors, lifecycle and resource decisions in their slice before code.
5. Run the disclosed real-history benchmark and resource/recovery evidence,
   including full history at base resolution and ingestion/publication contention.

Adding these declarations to `SPOT_COMPONENTS` before the upgrade would make
retained validation and canonical readiness reject existing accepted history.
The unregistered boundary is therefore deliberate and tested.
