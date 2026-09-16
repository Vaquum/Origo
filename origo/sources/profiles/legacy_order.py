"""Stable raw input ordering for the frozen floating-point projection formulas."""

from collections.abc import Mapping

from ..contracts import Client, Row


class OrderedRawClient:
    def __init__(self, client: Client, table: str) -> None:
        self.client, self.table = client, table

    def execute(
        self, query: str, params: object | None = None, settings: Mapping[str, object] | None = None
    ) -> list[Row]:
        # MergeTree parts may be read in different orders after inserts or merges.
        # The legacy formulas need the same ordered input on every execution.
        ordered = query.replace(
            f'FROM {self.table}',
            f'FROM (SELECT * FROM {self.table} ORDER BY datetime, trade_id)',
        )
        return self.client.execute(ordered, params, settings)

    def disconnect(self) -> None:
        raise RuntimeError('A projection cannot disconnect its build-owned connection.')
