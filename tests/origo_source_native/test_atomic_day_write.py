from __future__ import annotations

from collections.abc import Mapping, Sequence
from typing import Any

import pytest

from origo.utils.atomic_day_write import _staging_table_name, replace_day_via_staging

DATABASE = 'origo'
MAIN_TABLE = 'binance_daily_futures_trades'
COLUMNS = ('futures_trade_id', 'price', 'datetime')
DATE = '2019-09-08'
STAGING = _staging_table_name(MAIN_TABLE, DATE)


class _FakeClient:
    """Records executed statements; answers count() with scripted values."""

    def __init__(self, staging_count: int, live_count: int) -> None:
        self.statements: list[str] = []
        self._staging_count = staging_count
        self._live_count = live_count

    def execute(
        self,
        query: str,
        params: object | None = None,
        settings: Mapping[str, object] | None = None,
    ) -> list[tuple[object, ...]]:
        self.statements.append(query)
        if 'SELECT count()' in query:
            table = STAGING if STAGING in query else MAIN_TABLE
            return [(self._staging_count if table == STAGING else self._live_count,)]
        return []

    def disconnect(self) -> None:
        return None


def _rows(n: int) -> list[tuple[object, ...]]:
    return [(i, float(i), '2019-09-08 00:00:00') for i in range(n)]


def _mutates_live_table(statements: Sequence[str]) -> bool:
    # The staging table name is `<MAIN_TABLE>__staging_...`, so match only the
    # exact live-table mutation shapes (main followed by newline / space / as a
    # MOVE destination), never the staging references.
    joined = '\n'.join(statements)
    delete_on_live = f'ALTER TABLE {DATABASE}.{MAIN_TABLE}\n' in joined
    move_into_live = f'TO TABLE {DATABASE}.{MAIN_TABLE}' in joined
    insert_into_live = f'INSERT INTO {DATABASE}.{MAIN_TABLE} ' in joined
    return delete_on_live or move_into_live or insert_into_live


def test_happy_path_promotes_via_atomic_move_partition() -> None:
    client = _FakeClient(staging_count=3, live_count=3)

    result = replace_day_via_staging(client, DATABASE, MAIN_TABLE, DATE, COLUMNS, _rows(3))

    assert result == 3
    joined = '\n'.join(client.statements)
    assert f'CREATE TABLE {DATABASE}.{STAGING} AS {DATABASE}.{MAIN_TABLE}' in joined
    assert f'ALTER TABLE {DATABASE}.{MAIN_TABLE}\n            DELETE' in joined
    # Promotion is a metadata part-move, NOT a splittable INSERT..SELECT that
    # could commit a partial day on cancellation.
    assert (
        f"MOVE PARTITION ID '201909' TO TABLE {DATABASE}.{MAIN_TABLE}" in joined
    )
    assert f'INSERT INTO {DATABASE}.{MAIN_TABLE} ' not in joined


def test_staging_mismatch_leaves_live_table_untouched() -> None:
    # Staging built with the wrong count -> raise before any live-table write.
    client = _FakeClient(staging_count=99, live_count=0)

    with pytest.raises(ValueError, match='Staging row count mismatch'):
        replace_day_via_staging(client, DATABASE, MAIN_TABLE, DATE, COLUMNS, _rows(3))

    assert not _mutates_live_table(client.statements)


def test_staging_table_dropped_on_success_and_failure() -> None:
    ok = _FakeClient(staging_count=2, live_count=2)
    replace_day_via_staging(ok, DATABASE, MAIN_TABLE, DATE, COLUMNS, _rows(2))
    assert f'DROP TABLE IF EXISTS {DATABASE}.{STAGING}' in ok.statements[-1]

    bad = _FakeClient(staging_count=5, live_count=0)
    with pytest.raises(ValueError):
        replace_day_via_staging(bad, DATABASE, MAIN_TABLE, DATE, COLUMNS, _rows(2))
    assert f'DROP TABLE IF EXISTS {DATABASE}.{STAGING}' in bad.statements[-1]


def test_staging_dropped_before_rebuild() -> None:
    client = _FakeClient(staging_count=1, live_count=1)
    replace_day_via_staging(client, DATABASE, MAIN_TABLE, DATE, COLUMNS, _rows(1))
    drop_idx = client.statements.index(f'DROP TABLE IF EXISTS {DATABASE}.{STAGING}')
    create_idx = next(
        i for i, s in enumerate(client.statements) if s.startswith(f'CREATE TABLE {DATABASE}.{STAGING}')
    )
    assert drop_idx < create_idx


def test_no_staging_table_remains_after_materialize(
    materialize_binance_futures_raw_assets: Any,
    query_origo: Any,
) -> None:
    materialize_binance_futures_raw_assets(partition_key='2019-09-08')

    remaining = query_origo(
        f"""
        SELECT count()
        FROM system.tables
        WHERE database = '{DATABASE}' AND name LIKE '%__staging_%'
        """
    )

    assert remaining[0][0] == 0


def test_replacing_a_day_preserves_other_days_in_the_month(
    materialize_binance_futures_raw_assets: Any,
    query_origo: Any,
) -> None:
    """MOVE PARTITION must append the day into the live month, not replace it.

    Both days share the 201909 partition; re-ingesting one must leave the
    other's rows byte-for-byte intact and leave no duplicate for the
    re-ingested day.
    """
    materialize_binance_futures_raw_assets(partition_key='2019-09-08')
    materialize_binance_futures_raw_assets(partition_key='2019-09-09')

    def _day_count(day: str) -> int:
        return query_origo(
            f"""
            SELECT count()
            FROM {DATABASE}.{MAIN_TABLE}
            WHERE toDate(datetime) = toDate('{day}')
            """
        )[0][0]

    day1_before = _day_count('2019-09-08')
    day2_before = _day_count('2019-09-09')
    assert day1_before > 0 and day2_before > 0

    # Re-ingest only day 2; day 1 (same month partition) must be untouched.
    materialize_binance_futures_raw_assets(partition_key='2019-09-09')

    assert _day_count('2019-09-08') == day1_before
    assert _day_count('2019-09-09') == day2_before

    duplicate_ids = query_origo(
        f"""
        SELECT count()
        FROM (
            SELECT futures_trade_id
            FROM {DATABASE}.{MAIN_TABLE}
            WHERE toDate(datetime) = toDate('2019-09-09')
            GROUP BY futures_trade_id
            HAVING count() > 1
        )
        """
    )
    assert duplicate_ids[0][0] == 0
