from __future__ import annotations

from dataclasses import dataclass

import pytest

from origo.query import bitcoin_stream_aligned_1s as stream_query
from origo.query.binance_aligned_1s import CANONICAL_ALIGNED_1S_REQUIRED_SCHEMA
from origo.query.native_core import LatestRowsWindow


class _FakeQueryResult:
    def __init__(self, rows: list[tuple[str, str]]) -> None:
        self.result_rows = rows


class _FakeClickHouseClient:
    def __init__(self, schema_rows: list[tuple[str, str]]) -> None:
        self._schema_rows = schema_rows

    def query(self, sql: str) -> _FakeQueryResult:
        if 'FROM system.columns' in sql:
            return _FakeQueryResult(self._schema_rows)
        return _FakeQueryResult([])

    def close(self) -> None:
        return None


@dataclass(frozen=True)
class _FakeSettings:
    host: str = 'localhost'
    port: int = 8123
    username: str = 'default'
    password: str = 'secret'
    database: str = 'origo'


def _schema_rows(
    *,
    overrides: dict[str, str] | None = None,
) -> list[tuple[str, str]]:
    schema = dict(CANONICAL_ALIGNED_1S_REQUIRED_SCHEMA)
    if overrides is not None:
        schema.update(overrides)
    return [(column_name, schema[column_name]) for column_name in sorted(schema)]


def _run_query_with_schema(
    *,
    monkeypatch: pytest.MonkeyPatch,
    schema_rows: list[tuple[str, str]],
) -> None:
    client = _FakeClickHouseClient(schema_rows=schema_rows)

    def _resolve_settings(auth_token: str | None = None) -> _FakeSettings:
        del auth_token
        return _FakeSettings()

    def _get_client(**_kwargs: object) -> _FakeClickHouseClient:
        return client

    monkeypatch.setattr(
        stream_query,
        'resolve_clickhouse_http_settings',
        _resolve_settings,
    )
    monkeypatch.setattr(stream_query, 'get_client', _get_client)

    stream_query.query_bitcoin_stream_aligned_1s_data(
        dataset='bitcoin_block_headers',
        window=LatestRowsWindow(rows=1),
    )


def test_bitcoin_stream_aligned_storage_contract_requires_table(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    with pytest.raises(RuntimeError, match='does not exist'):
        _run_query_with_schema(monkeypatch=monkeypatch, schema_rows=[])


def test_bitcoin_stream_aligned_storage_contract_requires_all_columns(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    rows = [
        row
        for row in _schema_rows()
        if row[0] != 'first_source_offset_or_equivalent'
    ]
    with pytest.raises(RuntimeError, match='missing columns'):
        _run_query_with_schema(monkeypatch=monkeypatch, schema_rows=rows)


def test_bitcoin_stream_aligned_storage_contract_requires_exact_types(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    rows = _schema_rows(overrides={'bucket_event_count': 'UInt64'})
    with pytest.raises(RuntimeError, match='column type mismatch'):
        _run_query_with_schema(monkeypatch=monkeypatch, schema_rows=rows)
