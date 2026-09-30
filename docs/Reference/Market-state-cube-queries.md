# Market state cube queries

The market state cube ([PRD-0022](https://github.com/Vaquum/Origo/issues/462)) turns Binance
BTCUSDT spot trades since 2021-01-01 into a grid of time columns and price rows. A local
service answers one selection at a time with two Arrow IPC files: `cells.arrow` holds every
cell that has trades, and `summary.arrow` holds the grid totals, both POCs and the source
state the result was read from. On request, the cells also carry the base volume, path length,
dwell and trade prices of [PRD-0023](https://github.com/Vaquum/Origo/issues/478) (see
[Measures](#measures)). Files expire 24 hours after they were last read through the cube
reader.

## Calling the service

The service runs on the production host as the Compose service `market-state`:

- from the host network: `http://127.0.0.1:8486`
- from the Compose network `tdw-control-plane_default`: `http://market-state:8486`

Result files live on the Docker volume `tdw-control-plane_market-state`. The service returns
paths under `/opt/origo/market-state`. A consuming container mounts the volume read-only, at
that path or anywhere else; a consumer mounted elsewhere replaces the
`/opt/origo/market-state` prefix. Renewal is keyed by the last two path components, so the
mount point does not matter:

```sh
docker run --rm --network host -v tdw-control-plane_market-state:/opt/origo/market-state:ro \
  "ghcr.io/vaquum/origo-dagster:$SHA" python -c "
from origo.query.market_state_reader import query, read_table
result = query(t1='2026-09-01T00:00:00Z', t2='2026-09-02T00:00:00Z', tR=900, pR=1000)
cells = read_table(result.cells)
summary = read_table(result.summary)
print(cells.num_rows, summary.to_pylist()[0]['poc'])"
```

`origo/query/market_state_reader.py` needs only the standard library and pyarrow. A consumer
can import it from the Origo package or use a copy of the file pinned to a release.

## Request

`POST /v1/market-state/query` takes one JSON object of at most 64 KiB. Every key is optional,
and a missing key means the same as `null`:

| Key | Type | Meaning |
| --- | --- | --- |
| `t1`, `t2` | ISO 8601 string with an offset | Time range. Digits beyond microseconds are truncated. |
| `p1`, `p2` | JSON number or decimal string | Price range in USDT, at least 0. |
| `tR` | JSON number | Column width: 56.25 × 2ⁿ seconds (56.25, 112.5, 225, …, 900, 3600, …). |
| `pR` | JSON number | Row height: 125 × 2ᵐ USDT (125, 250, 500, 1000, …). |
| `measures` | JSON array of names | Detail measures to add, each at most once, in any order: `base_volume`, `path_length`, `dwell`, `high`, `low`, `open`, `close`. `[]` adds none. See [Measures](#measures). |

- **Rounding.** Supplied bounds round to the nearest edge of the 56.25 s × 125 USDT base
  lattice; an exact midpoint rounds up. Time edges count from 2021-01-01 00:00:00 UTC.
- **Omitted time bounds** cover the whole history: from 2021-01-01 to the data cutoff,
  rounded up to the next base edge so the latest trades are included.
- **Omitted price bounds** cover the occupied rows inside the selected time window,
  regardless of any price bound you supplied. With `path_length` or `dwell`, a row the path
  moved through or held in is occupied too.
- **Defaults.** `tR` defaults to 56.25 and `pR` to 125.
- **Clipping.** A time range reaching outside the covered history is clipped, and
  `clipped` says so. Nothing is clipped on price.
- **Empty rectangles** are normal results with no cells, zero totals and null POCs. They
  arise in three ways:
  - both time bounds round onto one edge;
  - a supplied price bound lies beyond the other side's automatic extent, and the interval
    collapses onto the supplied bound;
  - a time window has no trades while both price bounds are automatic, and both price bounds
    are then `null`.

## Response

```json
{"result_id": "…",
 "cells": "/opt/origo/market-state/results/<result_id>/cells.arrow",
 "summary": "/opt/origo/market-state/results/<result_id>/summary.arrow",
 "expires_after_seconds": 86400,
 "expires_at": "2026-09-26T07:00:00.000000+00:00",
 "effective": {"t1": "…", "t2": "…", "p1": 108000.0, "p2": 112000.0, "tR": 900.0, "pR": 1000.0},
 "clipped": {"t1": false, "t2": false},
 "data_cutoff": "2026-09-25T06:21:00.000000+00:00",
 "canonical_through": "2026-09-25T00:00:00.000000+00:00",
 "last_column_unfinished": false,
 "state_token": "…",
 "cell_count": 96}
```

- **`data_cutoff`**: the end of the contiguous cube coverage the result was read from.
  Nothing after the first day or minute without cube data is read, so uncovered time never
  appears as zero trades.
- **`canonical_through`**: the end of the canonical daily archives. Cells after it come from
  provisional minutes, which the day's archive later replaces.
- **`last_column_unfinished`**: the selection reaches past `data_cutoff`, so its last base
  cell may still gain trades.

## Files

`cells.arrow` holds one row per cell with trades, ordered by `(time_index, price_index)`:

| Column | Type | Meaning |
| --- | --- | --- |
| `time_index` | uint64 | `floor((t − 2021-01-01) / tR)`; the column spans `[2021-01-01 + time_index × tR, + tR)` |
| `price_index` | uint64 | `floor(price / pR)`; the row spans `[price_index × pR, + pR)` USDT |
| `volume` | float64 | USDT volume, the sum of `quote_quantity` |
| `trade_count` | uint64 | Individual trades |
| `taker_buy_volume` | float64 | USDT volume of trades where the buyer is not the maker |
| `taker_buy_trade_count` | uint64 | Individual taker-buy trades |

Cells without trades are omitted; all four of their measures are zero.

`summary.arrow` holds exactly one row:

| Columns | Meaning |
| --- | --- |
| `result_id`, `created_at` | The result and when it was written |
| `t1`, `t2`, `p1`, `p2`, `tR`, `pR` | The effective rectangle and resolutions; `p1`/`p2` are null only for a trade-free window with automatic prices |
| `first_column_partial`, `last_column_partial`, `first_row_partial`, `last_row_partial` | The rectangle cuts that edge column or row; its cell sums only the selected base cells |
| `last_column_unfinished` | As in the response |
| `volume`, `trade_count`, `taker_buy_volume`, `taker_buy_trade_count` | Grid totals over every emitted cell |
| `poc`, `taker_buy_poc` | Centre of the price row with the largest (taker-buy) volume, `(J + 0.5) × pR`; the lower row wins a tie; null without qualifying volume |
| `cell_count` | Rows in `cells.arrow` |
| `data_cutoff`, `canonical_through`, `state_token` | The source state the result reads |

Row sums, totals and POCs are `math.fsum` over the emitted cells, so a consumer reproduces
them exactly from `cells.arrow`. Cell volumes are compensated (`sumKahan`) sums of the base
cells.

Both files carry schema metadata `origo.market_state`, a JSON object holding:

- `schema_version` 1, or 2 for a request with measures;
- the request as received, with `measures` only when some were requested;
- the grid: `t0`, `tR`, `pR` and both exponents;
- the data cutoff and the state token;
- `pins`, the `[partition_key, generation, revision, build_id]` of every partition the
  result read.

## Measures

A request that names `measures` adds their columns to `cells.arrow` after the six above, in
this order:

| Column | Type | Meaning |
| --- | --- | --- |
| `base_volume` | float64 | Base volume in BTC, the sum of the trades' quantities |
| `path_length` | float64 | USDT the price travelled inside the cell, trade by trade |
| `dwell` | float64 | Seconds the price spent inside the cell |
| `high`, `low` | float64, null without trades | The cell's highest and lowest trade price |
| `open`, `open_at` | float64 and timestamp[us, UTC], null without trades | The cell's first trade by trade ID: its price and time |
| `close`, `close_at` | float64 and timestamp[us, UTC], null without trades | The cell's last trade by trade ID: its price and time |

`open` adds `open_at`, and `close` adds `close_at`. `summary.arrow` adds a total for each of
`base_volume`, `path_length` and `dwell` that was requested, after its other fields.

How they are measured:

- **The trade path** is the last-trade price as a step function of time. Each move between
  consecutive trades belongs to the later trade's column. It covers the price span between the
  two trades, rising or falling alike, and each half-open row takes the part of that span
  inside it. A row whose lower edge is the top of the span gets nothing: a rise from 125 to
  250 and a fall from 250 to 125 both give all 125 USDT to the row [125, 250).
- **Dwell.** Each price holds from its trade until the next trade. Trades at one timestamp are
  separate steps: a sweep through several prices at one instant adds path and no time. With a
  price bound, a column's dwell is its time inside the selected rows, not its whole time.
- **Partitions.** Each day, and each provisional minute until its day replaces it, is
  measured from its own trades. Its last price holds until its end, and its first trade's
  price also covers its start before that trade. A partition without trades has no path or
  dwell, and dwell never crosses a partition, so uncovered time is never credited. A
  provisional minute lacks the move
  into its first trade, credits its start to its first trade's row, has no dwell without
  trades, and has millisecond times.
- **Cells without trades.** A row the path crossed or a price held in without trading has
  path length or dwell but no trades. Such cells appear only when `path_length` or `dwell` is
  requested. Their four measures above are zero and their prices null, they count in
  `cell_count`, and they belong to the automatic price extent.
- **Exact sums.** Base volume, path length and dwell are stored as integers (satoshis, cents
  and microseconds) and summed as integers. Each cell value and each summary total is its
  integer sum divided once, correctly rounded. A summary total is therefore not `math.fsum`
  of the emitted cells, which can differ from it in the last bit.

Useful derivations:

- **VWAP** of a cell, or of the whole grid from the totals: `volume ÷ base_volume`.
- **Path per row height:** `path_length ÷ pR`. It is not a count of row crossings: chop inside
  one row adds path without crossing anything.
- **Time at price:** a cell's `dwell` as a share of its column's dwell.
- **A column's open, high, low and close:** request a `pR` whose single row holds the whole
  price range, so each column is one cell. Rows are aligned to multiples of `pR`, so width
  alone is not enough: [31875, 32125) crosses 32000 and needs `pR` 64000, not 250.

A request with measures reads only partitions that hold both the cube and its detail
component, so its `data_cutoff` can be earlier than a request's without them while history
is being upgraded; `outside_coverage` then reports that earlier cutoff. A request without
measures runs exactly as before.

## Expiry

A file expires 24 hours after its last read through the cube reader. Publication counts as
the first read, and each file has its own clock.

`open_file` and `read_table` call the service before every read. A file that was already
reclaimed raises `FileNotFoundError` before any bytes are returned. Plain `pyarrow.OSFile`
or `pyarrow.memory_map` reads of the same path work but do not renew the file.

## Errors

| Status | Body | When |
| --- | --- | --- |
| 400 | `{"error": "invalid_request", "reason": …, "field": …}` | See the reasons below |
| 409 | `{"error": "outside_coverage", "history_start", "data_cutoff"}` | The time range lies wholly outside the covered history |
| 503 | `{"error": "busy"}`, `Retry-After: 5` | Two queries are already running |
| 503 | `{"error": "source_maintenance"}`, `Retry-After: 5` | A cleanup reclaimed a build the query read, or held the source fence past 60 s |
| 507 | `{"error": "result_storage_full", …}` | The results would breach the 64 GiB budget or the disk floor source ingestion needs |
| 500 | `{"error": "export_failed", "reason": …}` | The export failed; nothing was published |

The 400 reasons are:

- `invalid_json`: malformed JSON, duplicate keys, `NaN`/`Infinity`, or a non-object body;
- `unknown_field`;
- `invalid_time` and `time_zone_required`;
- `invalid_price`: not a finite, non-negative number, or at or beyond 2⁵³ USDT;
- `bounds_out_of_order`;
- `unsupported_resolution`: not an exact dyadic multiple of the base width, or beyond the
  Float64 range;
- `invalid_measures`: not an array of distinct names from the list above.

The reader raises `MarketStateError` with the status and body. It retries only connection
failures during renewals, for up to 120 s; a query is never retried.

## Limits

- **Concurrency:** two queries at once; renewals never wait for a query.
- **ClickHouse:** each query runs with 4 threads, 4 GiB of memory and 60 s, and never spills
  to disk. A stalled transfer fails after 90 s.
- **Result budget:** 64 GiB of result files.
- **Disk floor:** results never lower free disk below the largest source capacity reserve
  plus 8 GiB.
- **Latency:** the PRD's target is that most requests finish in a few seconds, including full
  history at 56.25 s × 125 USDT: 4.3 M cells in a 207 MB file. The acceptance run of #476
  measures every case in production, and its report is posted on #462.
- **Measures** run a second statement over the detail component beside the cells statement,
  with the same limits. At full history and the finest grid, every measure took about three
  times as long and wrote 2.5 times the bytes of the same request without, in a production-size
  measurement for PRD-0023.

## Exact rally discovery

`POST /v1/market-state/rallies` discovers BTCUSDT spot events from native trades under
[PRD-0025](https://github.com/Vaquum/Origo/issues/487). It takes at most 64 KiB:

```python
from decimal import Decimal
from origo.query.market_state_reader import rallies, read_table

request = {
    'definition': {'mode': 'first_hit', 'scale': 'bps', 'target': Decimal('30'), 'anchor_minutes': 1},
    'analysis': {'start': '2026-06-27T11:39:00Z', 'end': '2026-06-27T11:55:00Z'},
    'expected_state': held_cube_state,
}
result = rallies(request, timeout_seconds=300)
events = read_table(result.rallies)
contributions = read_table(result.rally_cells)
diagnostics = read_table(result.summary)
```

`held_cube_state` contains the already held pack's `data_cutoff` and `pack_pin_digest`.
Compute the digest as SHA-256 of UTF-8 compact JSON
`[cutoff.isoformat(timespec='microseconds'), sorted(pins.items())]`, with UTC `+00:00`
and each pin value `[revision, build_id]`. Generation is excluded. Concrete pins stay
in the held pack; discovery never uploads them. A changed commitment returns
`409 pack_state_changed`, requiring an explicit cube refresh.

The definition accepts `first_hit`, `controlled_advance` or `swing`, with `bps` or
`atr` scale. Anchored modes require integer `anchor_minutes` from 1 to 1440;
controlled advance also requires `pullback`. Swing requires `reversal`.
Irrelevant fields and nonnumeric quantities are rejected. See the exact
[detector arithmetic and causal ATR contract](../Developer/Rally-detection.md).
Analysis starts at or after 2021-01-01. Its exclusive observation ceiling is the
minimum of `analysis.end` and the held cutoff. Required native context is derived
by the service. Missing covered ATR bars yield context diagnostics.

The immutable result contains three Arrow IPC files with shared JSON metadata
`origo.market_state_rallies`:

| File | Contents |
| --- | --- |
| `rallies.arrow` | Every confirmed event, its exact reference/member/confirmation IDs and timestamps, four additive measures, semantic ID and content evidence hash. |
| `rally_cells.arrow` | Sparse event × base-cell contributions: exact members, four additive measures, first/last member IDs and times, whole base-cell count/volume and count-based `partial`. |
| `summary.arrow` | One typed row: confirmed count, left/right-censored and unknown-context counts, and `diagnostics_as_of`. Empty results retain all three schemas. |

IDs and counts are UInt64; times are UTC microseconds. Swing's confirmation trade
is outside membership. Contributions may overlap across events and must never be
summed as a deduplicated union without accounting for overlap. Event measures are
`volume`, `trade_count`, `taker_buy_volume` and `taker_buy_trade_count`; path length,
dwell and indicators are explicitly unavailable for event-only membership.

The response includes the three paths, expiry, normalized analysis and observation
ceiling, definition/membership versions, whole-pack and relevant-source digests,
actual bulk/probe accounting and diagnostic counts. Shared metadata additionally
records relevant pins, source builds, detector build hash and diagnostic reasons.
Semantic IDs exclude grid, filters and source revisions. Version-1 evidence hashes
include typed event fields, exact little-endian native member IDs and canonical
contribution rows; whole-cell context and source identities are excluded.

Changing the display grid, event filters or replay time uses these retained files
locally. Discovery accepts no grid, result filters, result ID or `known_at` field.
Replay includes an event only when `confirmed_at < rally_known_at <= observation_ceiling`;
ceiling-only diagnostics are hidden at earlier replay times. It never retests a
filtered event's target. A new discovery requires an explicit refresh or changed
analysis/definition.

Any supported-reader read renews the entire rally triple for 24 hours. Ordinary
results retain their per-file expiry. Direct memory-mapped reads do not renew it.
The reader performs one POST without retries and accepts a finite positive timeout
of at most 300 seconds.

Admission limits are a 48-hour actual bulk scan, 8,000,000 native rows, 256 MiB of
five typed native columns, 512 MiB of canonical files and a 295-second server wall
deadline. Bounded returned predecessor probes count toward rows/bytes, with their
24-hour search bounds disclosed separately. Whole-cell columns are additional
bounded working data. Aggregate worker memory must remain within 1.5 GiB in the
existing 2 GiB container. One rally and two total heavy queries may run concurrently.
Budget refusals identify their limit; `503` busy/maintenance responses carry
`Retry-After: 5`. Relevant identity changes return `409 source_state_changed`;
storage admission returns `507`, and an expired server deadline returns `504`.
