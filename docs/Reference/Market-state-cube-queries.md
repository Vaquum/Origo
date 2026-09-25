# Market state cube queries

The market state cube ([PRD-0022](https://github.com/Vaquum/Origo/issues/462)) turns Binance
BTCUSDT spot trades since 2021-01-01 into a grid of time columns and price rows. A local
service answers one selection at a time with two Arrow IPC files: `cells.arrow` holds every
cell that has trades, and `summary.arrow` holds the grid totals, both POCs and the source
state the result was read from. Files expire 24 hours after they were last read through the
cube reader.

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

- **Rounding.** Supplied bounds round to the nearest edge of the 56.25 s × 125 USDT base
  lattice; an exact midpoint rounds up. Time edges count from 2021-01-01 00:00:00 UTC.
- **Omitted time bounds** cover the whole history: from 2021-01-01 to the data cutoff,
  rounded up to the next base edge so the latest trades are included.
- **Omitted price bounds** cover the occupied rows inside the selected time window,
  regardless of any price bound you supplied.
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

- `schema_version` 1;
- the request as received;
- the grid: `t0`, `tR`, `pR` and both exponents;
- the data cutoff and the state token;
- `pins`, the `[partition_key, generation, revision, build_id]` of every partition the
  result read.

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
  Float64 range.

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
