# Micro: book, flow and observed response

`POST /v1/market-state/micro` extends the existing server-local market-state
service. `origo.query.market_state_reader.micro` is its supported JSON caller.
It reads the existing spot individual-trade, aggregate-trade and top20 sources;
there is no new producer, projection, schedule or persisted market product.

## Request

Exactly `from`, `to`, `step`, `as_of` and `trade_pin_digest` are required.
Times and step are integer UTC milliseconds. Intervals are `[from, to)`;
`0 <= from < to <= as_of`. Both bounds align to the step.

- Detail: `step=1000`, at most 30 seconds.
- Overview: a positive multiple of 60000, at most 31 days and 4096 bins.
- The digest is SHA-256 of compact JSON containing the sorted relevant
  `[partition_key, revision, build_id]` individual-trade identities. Relevance
  means the accepted source partition overlaps the requested interval. JSON
  uses the default ASCII escaping and separators `(',', ':')`.

The Explorer bridge derives the digest from the pack it actually holds. A
canonical replacement or relevant correction returns `409 micro_cube_changed`;
an unrelated source/component update does not invalidate it. Source reads hold
the existing shared maintenance fences from before pinning through delivery
construction, preventing reclamation of a consumed generation. A busy cleanup
returns `503 source_maintenance`. Queries share the service's existing two slots.

## Response: `origo-micro@1`

The response repeats `from`, `to`, `step`, `asOf`, `nearBps=0.1` and
`tradePinDigest`. `sources` identifies every selected partition, revision,
build and coverage interval separately for `trades`, `groups` and `book`.

`bins` contains one item per requested interval:

- `at`, `until`: exclusive-upper interval edges, milliseconds.
- `coveredMs`: accepted interval coverage for each source. Incomplete trade or
  grouping coverage makes its corresponding object null, including a missing
  source; it never becomes measured zero.
- `book`: interval statistics and the **last eligible observation**. It contains
  `exchangeTimeUs`, `observedAtUs`, `mid`, `spreadBps`, `bid`, `ask`,
  `bidReachBps`, `askReachBps` and the number of distinct observed exchange
  `seconds`. Compare seconds with `step/1000` to identify sample gaps.
  No eligible sample makes the object null. Each side's depth is null if the
  twentieth level does not reach the complete 0.1 bp radius around this mid.
  `meanBid` and `meanAsk` average the genuine one-Hz samples for which both
  radii are captured; `nearSeconds` is their denominator. Missing seconds and
  incomplete radii contribute neither invented zero depth nor forward-filled
  quotes. `meanSpreadBps` averages observed spreads. `firstMid`,
  `firstExchangeTimeUs` and `firstObservedAtUs` identify the first eligible
  quote; `maxObservedAtUs` bounds every receipt contributing to the interval.
  A replay consumer must check that maximum, not merely the final quote's receipt.
  `normalDepth` is mean total near depth in the preceding UTC hour, excluding
  the displayed interval. It is null with fewer than 1800 eligible observed
  seconds; `normalSeconds` discloses that support. The baseline uses only quotes
  received before their minute closed. Detail has no hourly normalization.
  Overview therefore selects an additional hour of book inputs, whose native
  identities also appear in `sources.book`. This baseline is independent of
  the viewport and requested bin width.
- `flow`: `buy`, `sell`, individual execution `count`, `buyVwap`, `sellVwap`.
- `groups`: exchange aggregate `count`, `buy`, `sell`, `meanNotional` and,
  in detail only, `maxNotional`. Overview's maximum is null because minute
  sufficient statistics cannot recover it.

Depth and notional are USDT. Flow notional is `price * quantity`; side VWAP is
that notional divided by side base quantity. Buys have `is_buyer_maker=0`.
Overview sums the existing one-minute bar sufficient statistics; detail reads
actual individual executions and actual aggregate records. Float64 reductions
retain normal rounding; counts and interval membership are exact. Aggregate
notional is context about grouping of the same executions: **never add it to
individual flow** and never equate a group with a complete parent order.

Detail also supplies `anchor`, the last eligible book in the immediately
preceding second, or null. A price-response plot must retain gaps and identify
its actual baseline and quote times. This is observed co-movement, not proof of
causal impact, cancellation, replenishment or hidden liquidity. The capture is
20 levels at 1 Hz, not an order-event tape.

All book receipt timestamps are strictly before `as_of`; exchange timestamps
remain separate and preserve microseconds. Raw canonical source timestamps have
microsecond precision; the pre-existing provisional raw format has millisecond
precision. Historical queries reconstruct the currently accepted generations;
they do not claim historical first-publication knowledge of trade corrections.

## Bounds and failure

Statements use at most two threads, 2 GiB, ten execution seconds, five million
read rows and 2 GiB read bytes. Physical reads constrain source date and exact
partition/revision/build identities. Response JSON is at most 4 MiB; exceeding
it returns `413 micro_result_too_large`. Invalid requests return 400. Slot
exhaustion returns 503 with the existing retry header; failures enter the
existing query receipts. No successful-looking partial response is delivered.

Acceptance uses the genuine October 24 book/execution fixture and a coeval
accepted aggregate excerpt with its original IDs and source-pin provenance.
Production measurements and their limitations belong in the performance log.

The API retains at most eight request results, each within the same 4-MiB
limit. It holds all source fences before testing a cache hit. A single query
checks the append-only activation counts/timestamps and immutable anchors of
all three sources. A changed watermark resolves their selected identities in
one native statement; an unchanged relevant set can reuse its result even
after an unrelated activation. Book identities include the baseline hour.
Corrections, canonical replacement and rollback activations invalidate their
affected results. This cache is transient request reuse, not source history.
The Explorer bridge always reaches this validator and relies on the existing
two-slot API admission; it neither trusts a whole-pack cache nor queues Micro
behind unrelated motion/bar work.
