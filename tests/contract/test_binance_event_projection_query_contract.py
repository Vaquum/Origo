from __future__ import annotations

from typing import Literal

import pytest

from origo.query.binance_aligned_1s import (
    CANONICAL_ALIGNED_1S_REQUIRED_SCHEMA,
    assert_canonical_aligned_1s_storage_contract,
    build_binance_aligned_1s_sql,
)
from origo.query.binance_native import build_binance_native_query_spec
from origo.query.native_core import TimeRangeWindow

_WINDOW = TimeRangeWindow(
    start_iso='2024-01-01T00:00:00Z',
    end_iso='2024-01-01T01:00:00Z',
)
_BINANCE_DATASETS: tuple[
    Literal['binance_spot_trades'], ...
] = ('binance_spot_trades',)


class _FakeQueryResult:
    def __init__(self, rows: list[tuple[str, str]]) -> None:
        self.result_rows = rows


class _FakeClickHouseClient:
    def __init__(self, rows: list[tuple[str, str]]) -> None:
        self._rows = rows

    def query(self, _sql: str) -> _FakeQueryResult:
        return _FakeQueryResult(self._rows)


def _schema_rows(
    *,
    overrides: dict[str, str] | None = None,
) -> list[tuple[str, str]]:
    schema = dict(CANONICAL_ALIGNED_1S_REQUIRED_SCHEMA)
    if overrides is not None:
        schema.update(overrides)
    return [(column_name, schema[column_name]) for column_name in sorted(schema)]


def test_binance_native_specs_use_canonical_projection_tables() -> None:
    expected: dict[
        Literal['binance_spot_trades'],
        str,
    ] = {
        'binance_spot_trades': 'canonical_binance_spot_trades_native_v1',
    }

    for dataset in _BINANCE_DATASETS:
        spec = build_binance_native_query_spec(
            dataset=dataset,
            select_columns=None,
            window=_WINDOW,
        )
        assert spec.table_name == expected[dataset]


def test_binance_aligned_sql_uses_canonical_aggregate_table() -> None:
    for dataset in _BINANCE_DATASETS:
        sql = build_binance_aligned_1s_sql(
            dataset=dataset,
            window=_WINDOW,
            database='origo',
        )

        assert 'FROM origo.canonical_aligned_1s_aggregates' in sql
        assert "view_id = 'aligned_1s_raw'" in sql
        assert f"stream_id = '{dataset}'" in sql
        assert 'binance_trades' not in sql
        assert 'binance_agg_trades' not in sql
        assert 'binance_futures_trades' not in sql


def test_binance_aligned_storage_contract_requires_table() -> None:
    client = _FakeClickHouseClient(rows=[])

    with pytest.raises(RuntimeError, match='does not exist'):
        assert_canonical_aligned_1s_storage_contract(client=client, database='origo')


def test_binance_aligned_storage_contract_requires_all_columns() -> None:
    rows = [
        row
        for row in _schema_rows()
        if row[0] != 'first_source_offset_or_equivalent'
    ]
    client = _FakeClickHouseClient(rows=rows)

    with pytest.raises(RuntimeError, match='missing columns'):
        assert_canonical_aligned_1s_storage_contract(client=client, database='origo')


def test_binance_aligned_storage_contract_requires_exact_types() -> None:
    client = _FakeClickHouseClient(
        rows=_schema_rows(overrides={'bucket_event_count': 'UInt64'})
    )

    with pytest.raises(RuntimeError, match='column type mismatch'):
        assert_canonical_aligned_1s_storage_contract(client=client, database='origo')


def test_binance_aligned_storage_contract_accepts_expected_schema() -> None:
    client = _FakeClickHouseClient(rows=_schema_rows())

    assert_canonical_aligned_1s_storage_contract(client=client, database='origo')
