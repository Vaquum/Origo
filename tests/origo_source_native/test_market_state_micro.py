"""Micro's native read contract on coeval, unmodified captured market rows."""

from __future__ import annotations

import hashlib
import json
import math
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import cast
from uuid import uuid4

import polars as pl
import pytest

from origo.assets.create_origo_database import get_clickhouse_settings, make_clickhouse_client
from origo.query.market_state import RequestError
from origo.query.market_state_micro import (
    SPECS,
    MicroRequest,
    parse_micro_request,
    read_micro,
    trade_digest,
)
from origo.query.market_state_reader import micro
from origo.query.market_state_results import ResultStore
from origo.sources.contracts import Client, Row, SourceError, StateRecord
from origo.sources.locking import source_lock
from origo.sources.storage import SourceStore
from origo.workers.market_state_api import MarketStateApi, serve
from origo.workers.report import Reporter

FIXTURE = Path(__file__).parent / 'fixtures' / 'binance_rallies' / 'top20'
START = datetime(2026, 10, 24, 14, 24, tzinfo=UTC)
END = START + timedelta(minutes=8)
EPOCH = datetime(1970, 1, 1, tzinfo=UTC)


def test_micro_rejects_invalid_shapes_and_unbounded_reads() -> None:
    """Malformed or oversized requests cannot reach source reads."""
    valid = {'from': 1792851960000, 'to': 1792851990000, 'step': 1000, 'as_of': 1792851990000, 'trade_pin_digest': 'a' * 64}
    assert parse_micro_request(json.dumps(valid).encode()).detail
    for fault in ({'step': True}, {'to': valid['to'] + 1000}, {'from': valid['from'] + 1},
                  {'trade_pin_digest': 'x'}, {'unexpected': 1}):
        with pytest.raises(RequestError):
            parse_micro_request(json.dumps(valid | fault).encode())
    with pytest.raises(RequestError):
        parse_micro_request(b'{"from":1,"from":2}')


def _seed(client: Client) -> tuple[SourceStore, ...]:
    """Accepted partition envelopes are test controls, not full-day coverage evidence.

    Only the genuine eight-minute excerpt is queried. No market value, timestamp,
    ID, book level or aggregate grouping is generated or shifted.
    """
    stores = tuple(SourceStore(client, 'origo', spec) for spec in SPECS)
    build = uuid4()
    for store in stores:
        store.setup(anchor=START)
        partition = store.spec.canonical.partition('2026-10-24T14Z' if 'top20' in store.spec.key else '2026-10-24')
        prefix: Row = (partition.start.date(), partition.key, 'captured', build)
        if 'top20' in store.spec.key:
            frame = pl.read_parquet(FIXTURE / 'books.parquet')
            rows: list[Row] = []
            for source in frame.iter_rows(named=True):
                source['datetime'] = EPOCH + timedelta(seconds=source['datetime'])
                for side in ('bids', 'asks'):
                    source[side] = [tuple(level.values()) for level in source[side]]
                rows.append((*prefix, *source.values()))
            store.execute(f'INSERT INTO {store.component_table("depth20")} VALUES', rows)
        else:
            aggregate = 'aggtrades' in store.spec.key
            frame = pl.read_parquet(FIXTURE / ('aggtrades.parquet' if aggregate else 'trades.parquet'))
            rows = []
            for source in frame.iter_rows(named=True):
                moment = source['datetime'].replace(tzinfo=UTC) if aggregate else source['timestamp']
                if aggregate:
                    raw = tuple(source.values())
                else:
                    raw = (source['trade_id'], source['price'], source['quantity'], source['quote_quantity'],
                           (moment - EPOCH) // timedelta(microseconds=1), int(source['is_buyer_maker']), int(source['is_best_match']), moment)
                rows.append((*prefix, *raw))
            store.execute(f'INSERT INTO {store.component_table("raw")} VALUES', rows)
            # Build exactly the same existing minute sufficient statistics from these rows.
            store.execute(f'''INSERT INTO {store.component_table("time")}
                (source_date,partition_key,revision,build_id,datetime,liquidity_sum,maker_liquidity,no_of_trades,volume,maker_volume)
                SELECT source_date,partition_key,revision,build_id,toStartOfMinute(datetime),
                    sum(price*quantity),sumIf(price*quantity,is_buyer_maker=1),count(),sumKahan(quantity),sumKahanIf(quantity,is_buyer_maker=1)
                FROM {store.component_table("raw")} GROUP BY source_date,partition_key,revision,build_id,toStartOfMinute(datetime)''')
        store.insert_activation(StateRecord(partition, 1, 'captured', build, ()), 'micro-capture-control')
    return stores


def test_micro_native_arithmetic_clocks_gaps_grouping_and_generation_fence(
    origo_test_env: dict[str, str], tmp_path: Path,
) -> None:
    """The actual read rejects stale pins and exposes gaps, never duplicated volume or future quotes."""
    client = cast(Client, make_clickhouse_client(get_clickhouse_settings()))
    try:
        stores = _seed(client)
        start = int(datetime(2026, 10, 24, 14, 26, tzinfo=UTC).timestamp() * 1000)
        digest = trade_digest(stores[0].records())
        request = MicroRequest(start, start + 30_000, 1000, start + 30_000, digest)
        api = MarketStateApi(ResultStore(tmp_path / 'results'), Reporter('http://127.0.0.1:9'), tmp_path)
        server = serve(api, port=0)
        try:
            answer = micro({'from': request.start, 'to': request.end, 'step': request.step,
                            'as_of': request.as_of, 'trade_pin_digest': digest},
                           url=f'http://127.0.0.1:{server.server_port}')
        finally:
            server.shutdown()
            server.server_close()
        bins = cast(list[dict[str, object]], answer['bins'])
        assert len(bins) == 30 and answer['schema'] == 'origo-micro@1'
        raw = pl.read_parquet(FIXTURE / 'trades.parquet').filter(
            (pl.col('timestamp').dt.epoch('ms') >= start) & (pl.col('timestamp').dt.epoch('ms') < start + 30_000))
        groups = pl.read_parquet(FIXTURE / 'aggtrades.parquet').filter(
            (pl.col('datetime').dt.epoch('ms') >= start) & (pl.col('datetime').dt.epoch('ms') < start + 30_000))
        flows = [cast(dict[str, object], row['flow']) for row in bins]
        grouping = [cast(dict[str, object], row['groups']) for row in bins]
        assert sum(int(str(flow['count'])) for flow in flows) == raw.height
        assert sum(int(str(group['count'])) for group in grouping) == groups.height < raw.height
        for side, maker in (('buy', False), ('sell', True)):
            reference = math.fsum(price * quantity for price, quantity in raw.filter(pl.col('is_buyer_maker') == maker).select('price', 'quantity').iter_rows())
            assert math.fsum(float(str(flow[side])) for flow in flows) == pytest.approx(reference, rel=1e-12, abs=1e-6)
        books = pl.read_parquet(FIXTURE / 'books.parquet')
        for row in bins:
            eligible = books.filter(
                (pl.col('exchange_time_us') >= int(str(row['at'])) * 1000)
                & (pl.col('exchange_time_us') < int(str(row['until'])) * 1000)
                & (pl.col('observed_at_us') < request.as_of * 1000))
            if eligible.is_empty():
                assert row['book'] is None
                continue
            sample = cast(dict[str, object], row['book'])
            assert sample['seconds'] == 1
            assert int(str(sample['observedAtUs'])) < request.as_of * 1000
            genuine = books.filter(pl.col('exchange_time_us') == sample['exchangeTimeUs']).row(0, named=True)
            mid = (genuine['bids'][0]['1'] + genuine['asks'][0]['1']) / 2
            assert sample['mid'] == mid
            for side, field in (('bids', 'bid'), ('asks', 'ask')):
                expected = math.fsum(level['1'] * level['2'] for level in genuine[side] if abs(level['1'] / mid - 1) * 10000 <= 0.1)
                assert sample[field] == pytest.approx(expected, rel=1e-12, abs=1e-6)
        # A receipt after the replay edge must not leak the ending snapshot.
        early = read_micro(client, 'origo', tmp_path, MicroRequest(start, start + 1000, 1000, start + 1000, digest))
        assert cast(list[dict[str, object]], early['bins'])[0]['book'] is None
        # Genuine out-of-excerpt book gaps stay absent, although a partition envelope exists.
        gap = read_micro(client, 'origo', tmp_path, MicroRequest(start + 600_000, start + 630_000, 1000, start + 630_000, digest))
        assert all(row['book'] is None for row in cast(list[dict[str, object]], gap['bins']))
        unavailable = read_micro(client, 'origo', tmp_path, MicroRequest(0, 30_000, 1000, 30_000, trade_digest(())))
        assert all(row['flow'] is None and row['groups'] is None and row['book'] is None
                   for row in cast(list[dict[str, object]], unavailable['bins']))
        with pytest.raises(RequestError, match='generations differ'):
            read_micro(client, 'origo', tmp_path, MicroRequest(start, start + 30_000, 1000, start + 30_000, '0' * 64))
        with source_lock(tmp_path, SPECS[2].key, 'heavy'):
            with pytest.raises(SourceError, match='already held'):
                read_micro(client, 'origo', tmp_path, request)
        # Overview uses the existing minute components; its notional and counts reconcile.
        overview = read_micro(client, 'origo', tmp_path, MicroRequest(start, start + 60_000, 60_000, start + 60_000, digest))
        coarse = cast(dict[str, object], cast(list[dict[str, object]], overview['bins'])[0]['flow'])
        whole_minute = pl.read_parquet(FIXTURE / 'trades.parquet').filter(
            (pl.col('timestamp').dt.epoch('ms') >= start) & (pl.col('timestamp').dt.epoch('ms') < start + 60_000))
        assert coarse['count'] == whole_minute.height
        assert float(str(coarse['buy'])) + float(str(coarse['sell'])) == pytest.approx(
            math.fsum(price * quantity for price, quantity in whole_minute.select('price', 'quantity').iter_rows()), rel=1e-12, abs=1e-6)
        provenance = json.loads((FIXTURE / 'provenance.json').read_text())
        assert hashlib.sha256((FIXTURE / 'aggtrades.parquet').read_bytes()).hexdigest() == provenance['aggtrades']['sha256']
    finally:
        client.disconnect()
