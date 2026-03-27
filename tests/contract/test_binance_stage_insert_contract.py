from __future__ import annotations

import polars as pl
from origo_control_plane.utils.binance_canonical_event_ingest import (
    create_staged_binance_spot_trade_csv_or_raise,
)


class _RecordingClickHouseClient:
    def __init__(self) -> None:
        self.create_queries: list[str] = []
        self.insert_queries: list[str] = []
        self.insert_params: list[list[list[str]]] = []
        self.insert_columnar_flags: list[bool] = []

    def execute(
        self,
        query: str,
        params: list[list[str]] | None = None,
        *,
        columnar: bool = False,
    ) -> list[tuple[object, ...]]:
        normalized = query.strip().upper()
        if normalized.startswith('CREATE TABLE'):
            self.create_queries.append(query)
            return []
        if normalized.startswith('INSERT INTO'):
            if params is None:
                raise RuntimeError('Expected columnar params for INSERT')
            self.insert_queries.append(query)
            self.insert_params.append(params)
            self.insert_columnar_flags.append(columnar)
            return []
        raise RuntimeError(f'Unexpected query shape: {query}')


def test_create_staged_binance_spot_trade_csv_uses_native_columnar_insert() -> None:
    client = _RecordingClickHouseClient()
    frame = pl.DataFrame(
        {
            'trade_id': [1, 2],
            'price_text': ['41000.10', '41001.20'],
            'quantity_text': ['0.01', '0.02'],
            'quote_quantity_text': ['410.001', '820.024'],
            'timestamp_raw': ['1710000000000', '1710000001000'],
            'is_buyer_maker': [False, True],
            'is_best_match': [True, False],
        }
    )

    stage_table = create_staged_binance_spot_trade_csv_or_raise(
        client=client,  # type: ignore[arg-type]
        database='origo',
        frame=frame,
    )

    assert stage_table.startswith('__origo_binance_stage_')
    assert len(client.create_queries) == 1
    assert len(client.insert_queries) == 1
    assert client.insert_columnar_flags == [True]

    inserted_columns = client.insert_params[0]
    assert inserted_columns == [
        ['1', '2'],
        ['41000.10', '41001.20'],
        ['0.01', '0.02'],
        ['410.001', '820.024'],
        ['1710000000000', '1710000001000'],
        ['0', '1'],
        ['1', '0'],
    ]
