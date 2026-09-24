# Binance rally export

`origo.query.binance_rallies.export_binance_rallies` finds Binance BTCUSDT spot rallies and writes them to three Arrow files, with their trades and depth-200 order book. A rally is a UTC minute from which the price reaches 30 basis points above the last trade before that minute, within 240 minutes. You select rallies by a UTC time range or by rally ID, and each rally's rows can include a lead-in of minutes before it.

Only spot is available. Perpetual rallies will follow as their own slice under [PRD-0006](https://github.com/Vaquum/Origo/issues/313), once Origo has a perpetual order book.

The export reads Origo's ClickHouse, which listens only on the production host. It therefore runs inside the production Dagster container.

## Quick start

These commands run on your own machine. Set `ORIGO_HOST` to your SSH destination for the production host, the host named by the deploy workflow's `ORIGO_HOST` variable. Your SSH user must be able to run `docker` there.

1. Export the rallies anchored between 11:39 and 11:55 UTC on 2026-06-27, each with five minutes of lead-in:

   ```bash
   ssh "$ORIGO_HOST" docker exec -i tdw-control-plane-dagster-1 python - <<'EOF'
   from datetime import UTC, datetime
   from pathlib import Path

   from origo.query.binance_rallies import export_binance_rallies

   paths = export_binance_rallies(
       output_dir=Path('/tmp/rallies-2026-06-27'),
       start=datetime(2026, 6, 27, 11, 39, tzinfo=UTC),
       end=datetime(2026, 6, 27, 11, 55, tzinfo=UTC),
       minutes_before=5,
   )
   print(*paths, sep='\n')
   EOF
   ```

   It prints the three files it wrote:

   ```text
   /tmp/rallies-2026-06-27/rallies.arrow
   /tmp/rallies-2026-06-27/trades.arrow
   /tmp/rallies-2026-06-27/book.arrow
   ```

2. Copy the directory to your machine, then remove it from the container:

   ```bash
   ssh "$ORIGO_HOST" docker cp tdw-control-plane-dagster-1:/tmp/rallies-2026-06-27 - | tar -xf -
   ssh "$ORIGO_HOST" docker exec tdw-control-plane-dagster-1 rm -r /tmp/rallies-2026-06-27
   ```

   Copy the files off right away. Every merge to `main` deploys and replaces the container, and its `/tmp` goes with it.

3. Read the files with any Arrow IPC reader. With [polars](https://pola.rs) (the examples here were run with polars 1.44 and pyarrow 25):

   ```python
   import polars as pl

   rallies = pl.read_ipc('rallies-2026-06-27/rallies.arrow')
   trades = pl.read_ipc('rallies-2026-06-27/trades.arrow')
   book = pl.read_ipc('rallies-2026-06-27/book.arrow')

   rally = rallies.row(0, named=True)
   rally_trades = trades.filter(
       pl.col('trade_id').is_between(rally['first_trade_id'], rally['hit_trade_id'])
   )
   rally_book = book.filter(
       pl.col('observed_at').is_between(rally['first_snapshot_time'], rally['last_snapshot_time'])
   )
   ```

The window holds four rallies:

| `rally_id` | anchor | reference price | hit price | time to hit | trades | snapshots |
|---|---|---|---|---|---|---|
| `binance:spot:BTCUSDT:r30v1:t1782560340` | 11:39 | 60,209.74 | 60,390.72 | 6 min 38.810458 s | 17,385 | 699 |
| `binance:spot:BTCUSDT:r30v1:t1782560460` | 11:41 | 60,220.61 | 60,401.55 | 4 min 39.007212 s | 15,276 | 579 |
| `binance:spot:BTCUSDT:r30v1:t1782560520` | 11:42 | 60,224.79 | 60,405.52 | 3 min 39.095231 s | 13,852 | 519 |
| `binance:spot:BTCUSDT:r30v1:t1782560580` | 11:43 | 60,233.28 | 60,413.99 | 2 min 40.878016 s | 11,628 | 461 |

The rallies overlap. Together they span 17,962 trades and 701 snapshots, and the files hold each trade and snapshot once, however many rallies share it.

Every whole minute is judged as its own anchor. One price move therefore usually yields several overlapping rallies, each with its own reference price, target and hit.

The 11:40 anchor is a rally too, but it hits at 12:55:01.699664, after the window ends, so this range leaves it out ([By time range](#by-time-range)).

## Choosing rallies

Pass either `rally_ids`, or both `start` and `end`.

### By time range

`start` and `end` must be timezone-aware. The range selects every rally that lies fully inside `[start, end)`: anchored at or after `start`, with its hit before `end`.

- Anchors are whole minutes. A `start` inside a minute begins at the next whole minute.
- A rally that hits at or after `end` is left out entirely, never clipped. To catch a rally that begins in your window but completes after it, move `end` later.

On the quick-start window:
- 11:39 to 11:55 returns the four rallies above.
- 11:40 to 11:55 returns the rallies anchored at 11:41, 11:42 and 11:43.
- 11:39 to 11:45:40.878016, the 11:43 rally's hit time, drops that rally.
- 11:39 to any `end` after 12:55:01.699664 also returns the 11:40 rally.

To list the rallies of a longer period, export the period and read `rallies.arrow`. The trades and book of every rally are written with it; see [Limits and performance](#limits-and-performance).

### By rally ID

Every rally has a stable ID, `binance:spot:BTCUSDT:r30v1:t<seconds>`, where `<seconds>` is the anchor minute in Unix time. Pass IDs to fetch the same rallies again, for example with another lead-in or boundary. Run the call inside the container, as in the quick start:

```python
paths = export_binance_rallies(
    output_dir=Path('/tmp/rallies-by-id'),
    rally_ids=[
        'binance:spot:BTCUSDT:r30v1:t1782560340',
        'binance:spot:BTCUSDT:r30v1:t1782560580',
    ],
    boundary='before',
    minutes_before=1,
)
```

- IDs can come in any order and repeat.
- An ID whose anchor is not a rally in the data Origo holds raises `ValueError`.
- An ID and a range that select the same rallies export the same rows; only the recorded request differs.

To build an ID from an anchor time:

```python
anchor = datetime(2026, 6, 27, 11, 39, tzinfo=UTC)
rally_id = f'binance:spot:BTCUSDT:r30v1:t{int(anchor.timestamp())}'
```

### Lead-in: `minutes_before`

`minutes_before` (default `0`) starts each rally's rows that many minutes before its anchor. The lead-in is context only: it never changes which rallies are found, or their IDs, references or hits. It counts toward the [48-hour read limit](#limits-and-performance).

In the quick start, `minutes_before=5` moves the 11:39 rally's start to 11:34:00. Its rows begin with the trade at 11:34:00.159108 and the snapshot at 11:34:00.395.

### Where rows start: `boundary`

Each rally's rows start at `s` = anchor − `minutes_before`:

- `'after'` (default): the first trade and the first snapshot at or after `s`.
- `'before'`: the last trade and the last snapshot before `s`. Use it when you need the state in force at `s`: the last traded price and the book as it stood.

Rows end at the hit: the hit trade, and the last snapshot at or before the hit time. Nothing after the hit is exported.

With `'before'` and a one-minute lead-in, the ID example above starts the 11:43 rally at two records:
- trade 6453967737, the last trade before 11:42:00;
- the snapshot observed at 11:41:59.392.

A rally can have no rows to start from in a file. Its bounds for that file are then null, and it has no rows there:
- **`'before'`, no trade in the 24 hours before `s`:** `first_trade_id` is null.
- **`'before'`, no snapshot in the 24 hours before `s`:** the snapshot bounds are null.
- **`'after'`, no snapshot between `s` and the hit:** the snapshot bounds are null. This covers every rally that hits before the book begins on 2026-06-15.

## Working with the files

A rally's rows are:
- in `trades.arrow`, the trades with `first_trade_id ≤ trade_id ≤ hit_trade_id`;
- in `book.arrow`, the snapshots with `first_snapshot_time ≤ observed_at ≤ last_snapshot_time`.

A null bound selects no rows. The recipes below use the frames from the quick start.

**Rallies within a horizon.** A rally falls within horizon `h` when its `time_to_hit` is below `h`. The studied grid is 1, 3, 5, 15, 30, 60, 120 and 240 minutes. In the quick-start window, three of the four rallies hit within 5 minutes:

```python
from datetime import timedelta

fast = rallies.filter(pl.col('time_to_hit') < timedelta(minutes=5))
```

**Every rally's rows in one table.** Join the rallies to the rows their bounds select:

```python
rally_trades = rallies.join_where(
    trades,
    pl.col('trade_id') >= pl.col('first_trade_id'),
    pl.col('trade_id') <= pl.col('hit_trade_id'),
)
rally_book = rallies.join_where(
    book,
    pl.col('observed_at') >= pl.col('first_snapshot_time'),
    pl.col('observed_at') <= pl.col('last_snapshot_time'),
)
```

This repeats shared rows once per rally. In the quick-start window, 17,962 trades become 58,141 rows and 701 snapshots become 2,258 rows.

**Best bid, best ask and mid.** Bids run from the highest price and asks from the lowest, so the first level of each side is the top of the book:

```python
top = book.select(
    'observed_at',
    best_bid=pl.col('bids').list.first().struct.field('price'),
    best_ask=pl.col('asks').list.first().struct.field('price'),
).with_columns(mid=(pl.col('best_bid') + pl.col('best_ask')) / 2)
```

**Provenance.** Each file records the definition, the request and the source data it read ([Metadata](#metadata)):

```python
import json

import pyarrow.ipc

metadata = json.loads(
    pyarrow.ipc.open_file('rallies-2026-06-27/rallies.arrow').schema.metadata[b'origo.rallies']
)
```

**Research caveats:**
- Overlapping rallies share rows, so they are not independent samples. Split training and test data by time.
- The hit is known only afterwards: a rally is a label, not an entry signal.

## Reference

### `export_binance_rallies`

```python
def export_binance_rallies(
    *,
    output_dir: Path,
    rally_ids: Sequence[str] | None = None,
    start: datetime | None = None,
    end: datetime | None = None,
    boundary: Literal['after', 'before'] = 'after',
    minutes_before: int = 0,
) -> tuple[Path, Path, Path]
```

| Parameter | Meaning |
|---|---|
| `output_dir` | The directory to create. It must not exist; its parent must. |
| `rally_ids` | Rally IDs to export. An empty list writes three empty files. |
| `start`, `end` | A timezone-aware range, `start` before `end`. It selects the rallies anchored in `[start, end)` whose hit comes before `end`. |
| `boundary` | `'after'` (default) or `'before'`: where each rally's rows start. |
| `minutes_before` | A non-negative integer, default `0`: the lead-in before each anchor, in minutes. |

Pass either `rally_ids`, or both `start` and `end`. The call returns the paths of `rallies.arrow`, `trades.arrow` and `book.arrow`, in that order.

| Raises | When |
|---|---|
| `ValueError` | The selectors are wrong: both, neither, or only one of `start` and `end`. |
| `ValueError` | The range is invalid: a naive datetime, or `start` not before `end`. |
| `ValueError` | `boundary` is not `'after'` or `'before'`. |
| `ValueError` | `minutes_before` is negative or not an integer. |
| `ValueError` | A rally ID is malformed or not in its canonical form. |
| `ValueError` | A rally ID is not a rally in the data Origo holds. |
| `ValueError` | The request would read more than 48 hours of trades. |
| `FileExistsError` | `output_dir` exists. It is left untouched. |
| `FileNotFoundError` | The parent of `output_dir` does not exist. This is raised after the reads, when the files are written. |

The export writes into a hidden staging directory beside `output_dir`, and renames it into place once all three files are complete. A failed export leaves nothing behind.

It reaches ClickHouse over HTTP, configured by the container's environment:

| Variable | Default |
|---|---|
| `CLICKHOUSE_HOST` | `clickhouse` |
| `CLICKHOUSE_HTTP_PORT` | `8123` |
| `CLICKHOUSE_USER` | `default` |
| `CLICKHOUSE_PASSWORD` | none: required |
| `CLICKHOUSE_DATABASE` | `origo` |

The export stores nothing in Origo. Each request reads ClickHouse afresh.

### Rally definition `r30v1`

| Term | Definition |
|---|---|
| Anchor `t` | A UTC minute boundary. |
| Reference | The last trade before `t`, searched in the 24 hours before `t`. With no trade there, `t` is not a rally. |
| Target | The reference price × 1.003: 30 basis points up. |
| Hit | The first trade at or after `t` priced at or above the target. |
| Rally | An anchor whose hit comes before `t` + 240 minutes. |
| Time to hit | The hit time − `t`. |
| Horizon `h` | 1, 3, 5, 15, 30, 60, 120 or 240 minutes. A rally falls within `h` when its time to hit is below `h`. Horizons filter rallies; they are not part of the ID. |

Trades are taken in trade-ID order, which is time order: the source requires strictly increasing trade IDs with non-decreasing timestamps. Trades that share a timestamp are ordered by trade ID.

For example, the 11:39 rally in the quick start runs as follows:
- **Reference:** 60,209.74, traded at 11:38:57.620284.
- **Target:** 60,390.37.
- **Hit:** trade 6453973877 at 60,390.72, at 11:45:38.810458.
- **Time to hit:** 6 minutes 38.810458 seconds.

### Rally IDs

`binance:spot:BTCUSDT:r30v1:t<seconds>` names the market, the symbol, the definition version and the anchor.

`<seconds>` is the anchor's Unix time: a multiple of 60, without leading zeros. Only this exact form is accepted.

An ID names an anchor under a definition. It does not freeze the data behind it; see [Reproducibility](#reproducibility).

### Output files

Each export directory holds three Arrow IPC files, the Feather v2 format, with ZSTD-compressed buffers. All timestamps are UTC. An export with no rallies still writes all three files, empty, with their full schemas.

**`rallies.arrow`** holds one row per rally, ordered by `anchor_time`:

| Column | Type | Nullable | Meaning |
|---|---|---|---|
| `rally_id` | `string` | no | The rally ID. |
| `anchor_time` | `timestamp[us, UTC]` | no | The anchor `t`. |
| `reference_trade_id` | `uint64` | no | The last trade before `t`. |
| `reference_time` | `timestamp[us, UTC]` | no | Its time. |
| `reference_price` | `float64` | no | Its price. |
| `hit_trade_id` | `uint64` | no | The first trade at or after `t` at or above the target. |
| `hit_time` | `timestamp[us, UTC]` | no | Its time. |
| `hit_price` | `float64` | no | Its price. |
| `time_to_hit` | `duration[us]` | no | `hit_time` − `anchor_time`. |
| `first_trade_id` | `uint64` | yes | The first trade of the rally's rows. |
| `first_snapshot_time` | `timestamp[ms, UTC]` | yes | The first snapshot of the rally's rows. |
| `last_snapshot_time` | `timestamp[ms, UTC]` | yes | The last snapshot at or before `hit_time`. |

**`trades.arrow`** holds every trade of any exported rally once, ordered by `trade_id`. No column is nullable:

| Column | Type | Meaning |
|---|---|---|
| `trade_id` | `uint64` | The Binance trade ID. |
| `timestamp` | `timestamp[us, UTC]` | The trade time, on Binance's clock. |
| `price` | `float64` | The price in USDT. |
| `quantity` | `float64` | The quantity in BTC. |
| `quote_quantity` | `float64` | The quantity in USDT, as Binance reports it. |
| `is_buyer_maker` | `bool` | True when the buyer was the maker, so the taker sold. |
| `is_best_match` | `bool` | Binance's best-match flag. |

**`book.arrow`** holds every order-book snapshot of any exported rally once, ordered by `observed_at`. No column is nullable:

| Column | Type | Meaning |
|---|---|---|
| `observed_at` | `timestamp[ms, UTC]` | When the depth collector received the snapshot, on the collector's clock. |
| `last_update_id` | `uint64` | Binance's `lastUpdateId` for the snapshot. |
| `bids` | `list<struct<price: float64, quantity: float64>>` | 200 bid levels, highest price first. |
| `asks` | `list<struct<price: float64, quantity: float64>>` | 200 ask levels, lowest price first. |

### Metadata

All three files carry the same JSON document under the schema metadata key `origo.rallies`:

| Key | Content |
|---|---|
| `definition` | The rally definition: `version`, `market`, `symbol`, `target_bps`, `max_time_to_hit_minutes`, `horizon_grid_minutes` and `lookback_hours`. |
| `request` | In ID mode, `rally_ids` (canonical, in anchor order). In range mode, `start` and `end`. Always `boundary` and `minutes_before`. |
| `sources.trades` | The `source`, `binance_spot_trades`, and the `partitions` read, each with its `partition_key`, `provisional` flag, `revision` and `build_id`. |
| `sources.book` | The `table`, read with `FINAL`. `spans` lists the inclusive spans read. `last_before` lists the `[from, to)` windows searched for the last snapshot before a start; it is empty with `boundary='after'`. |

The quick-start export records:

```json
{
  "definition": {
    "horizon_grid_minutes": [1, 3, 5, 15, 30, 60, 120, 240],
    "lookback_hours": 24,
    "market": "spot",
    "max_time_to_hit_minutes": 240,
    "symbol": "BTCUSDT",
    "target_bps": 30,
    "version": "r30v1"
  },
  "request": {
    "boundary": "after",
    "end": "2026-06-27T11:55:00+00:00",
    "minutes_before": 5,
    "start": "2026-06-27T11:39:00+00:00"
  },
  "sources": {
    "book": {
      "last_before": [],
      "read": "FINAL",
      "spans": [["2026-06-27T11:34:00+00:00", "2026-06-27T11:45:40.878016+00:00"]],
      "table": "origo.binance_spot_depth200_snapshots"
    },
    "trades": {
      "partitions": [
        {
          "build_id": "a14ba941-63cd-4eae-965f-a125538396c0",
          "partition_key": "2026-06-26",
          "provisional": false,
          "revision": "0c137f25005f982686dc40664615df61bc185ab05340b83f3862b815b849f9af"
        },
        {
          "build_id": "18e8bf1f-54c6-4f3b-8df8-6d9315006644",
          "partition_key": "2026-06-27",
          "provisional": false,
          "revision": "663f81f2e09114d80003a0ec4a27ea2c54edc8de18297fc0f07812b65e3d0db4"
        }
      ],
      "source": "binance_spot_trades"
    }
  }
}
```

The 2026-06-26 partition appears because the reference search looks back 24 hours from the start of the rows.

### Data and coverage

**Trades** come from the `binance_spot_trades` source:
- Binance's official daily archives from 2017-08-17.
- Provisional minutes where no archive is active yet, which covers the current day. They trail the clock by a few minutes, and the day's archive replaces them once Origo activates it.
- Each request pins the partitions that are current when it starts, and reads only those.

**The book** is `origo.binance_spot_depth200_snapshots`, read with `FINAL`: one snapshot per second since 2026-06-15 14:31 UTC, with 200 levels a side.

**Clocks:** trade timestamps are Binance's. `observed_at` is the collector's receive time. A rally's book bounds compare the two clocks directly.

**Precision:**
- Trade timestamps are in microseconds in the daily archives from 2025-01-01.
- They are in milliseconds before 2025 and in provisional minutes.
- `observed_at` is in milliseconds.

The export adds no data checks of its own: it exports what the sources hold, gaps included.

### Limits and performance

One request reads at most 48 hours of trades, lead-in included:
- **A range** costs its length from its first whole minute, plus `minutes_before`.
- **IDs** less than 240 minutes apart are read together. Each such group costs the span from its first anchor to 240 minutes past its last, plus `minutes_before`. Without a lead-in, at most twelve IDs that are each at least four hours apart fit in one request.

Split larger requests.

Measured on production on 2026-09-24, inside the container:

| Request | Rallies | Trades | Snapshots | Files | Time |
|---|---|---|---|---|---|
| 2026-06-27 11:39–11:55 with `minutes_before=5` | 4 | 17,962 | 701 | 0.6 MB | 0.7 s |
| 2026-09-22 12:00–18:00 | 276 | 1,114,387 | 21,189 | 26 MB | 3.0 s |

The 6-hour export peaked at 0.9 GB of memory. The book makes up most of the file size.

### Reproducibility

Repeating a request over unchanged sources writes identical rows and metadata.

Sources change in two ways:
- The current day's provisional minutes give way to the official archive.
- A partition can be re-activated at a new revision.

`sources.trades.partitions` tells whether two exports read the same trades. After such a change, a rally ID can resolve to a different reference or hit, or stop being a rally.

The book table is not revisioned. `sources.book.spans` records what was read.
