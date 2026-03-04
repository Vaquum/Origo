from __future__ import annotations

from typing import Any

import polars as pl


def _schema_items(frame: pl.DataFrame) -> list[dict[str, str]]:
    return [{'name': name, 'dtype': str(dtype)} for name, dtype in frame.schema.items()]


def build_wide_rows_envelope(
    frame: pl.DataFrame,
    mode: str = 'native',
    source: str | None = None,
) -> dict[str, Any]:
    return {
        'mode': mode,
        'source': source,
        'row_count': frame.height,
        'schema': _schema_items(frame),
        'rows': frame.to_dicts(),
    }
