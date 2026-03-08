from __future__ import annotations

from collections.abc import Iterable, Sequence
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Literal

import polars as pl

type ExchangeDataset = Literal[
    'spot_trades',
    'spot_agg_trades',
    'futures_trades',
    'futures_agg_trades',
    'okx_spot_trades',
    'bybit_spot_trades',
]


@dataclass(frozen=True)
class _ExchangeIntegritySpec:
    tuple_size: int
    id_index: int
    timestamp_index: int
    price_index: int
    quantity_index: int
    quote_quantity_index: int | None
    side_index: int | None
    bool_indices: tuple[int, ...]
    first_last_trade_indices: tuple[int, int] | None
    datetime_index: int | None
    enforce_monotonic_timestamp: bool
    frame_columns: tuple[str, ...]


@dataclass(frozen=True)
class ExchangeIntegrityReport:
    dataset: ExchangeDataset
    rows_checked: int
    min_id: int
    max_id: int
    sequence_gap_count: int
    anomaly_checks_performed: int

    def to_dict(self) -> dict[str, int | str]:
        return {
            'dataset': self.dataset,
            'rows_checked': self.rows_checked,
            'min_id': self.min_id,
            'max_id': self.max_id,
            'sequence_gap_count': self.sequence_gap_count,
            'anomaly_checks_performed': self.anomaly_checks_performed,
        }


_EXCHANGE_INTEGRITY_SPECS: dict[ExchangeDataset, _ExchangeIntegritySpec] = {
    'spot_trades': _ExchangeIntegritySpec(
        tuple_size=8,
        id_index=0,
        timestamp_index=4,
        price_index=1,
        quantity_index=2,
        quote_quantity_index=3,
        side_index=None,
        bool_indices=(5, 6),
        first_last_trade_indices=None,
        datetime_index=7,
        enforce_monotonic_timestamp=False,
        frame_columns=(
            'trade_id',
            'price',
            'quantity',
            'quote_quantity',
            'timestamp',
            'is_buyer_maker',
            'is_best_match',
            'datetime',
        ),
    ),
    'spot_agg_trades': _ExchangeIntegritySpec(
        tuple_size=8,
        id_index=0,
        timestamp_index=5,
        price_index=1,
        quantity_index=2,
        quote_quantity_index=None,
        side_index=None,
        bool_indices=(6,),
        first_last_trade_indices=(3, 4),
        datetime_index=7,
        enforce_monotonic_timestamp=False,
        frame_columns=(
            'agg_trade_id',
            'price',
            'quantity',
            'first_trade_id',
            'last_trade_id',
            'timestamp',
            'is_buyer_maker',
            'datetime',
        ),
    ),
    'futures_trades': _ExchangeIntegritySpec(
        tuple_size=7,
        id_index=0,
        timestamp_index=4,
        price_index=1,
        quantity_index=2,
        quote_quantity_index=3,
        side_index=None,
        bool_indices=(5,),
        first_last_trade_indices=None,
        datetime_index=6,
        enforce_monotonic_timestamp=False,
        frame_columns=(
            'futures_trade_id',
            'price',
            'quantity',
            'quote_quantity',
            'timestamp',
            'is_buyer_maker',
            'datetime',
        ),
    ),
    'futures_agg_trades': _ExchangeIntegritySpec(
        tuple_size=7,
        id_index=0,
        timestamp_index=5,
        price_index=1,
        quantity_index=2,
        quote_quantity_index=None,
        side_index=None,
        bool_indices=(6,),
        first_last_trade_indices=(3, 4),
        datetime_index=None,
        enforce_monotonic_timestamp=False,
        frame_columns=(
            'futures_agg_trades_id',
            'price',
            'quantity',
            'first_trade_id',
            'last_trade_id',
            'timestamp',
            'is_buyer_maker',
        ),
    ),
    'okx_spot_trades': _ExchangeIntegritySpec(
        tuple_size=8,
        id_index=1,
        timestamp_index=6,
        price_index=3,
        quantity_index=4,
        quote_quantity_index=5,
        side_index=2,
        bool_indices=(),
        first_last_trade_indices=None,
        datetime_index=7,
        enforce_monotonic_timestamp=False,
        frame_columns=(
            'instrument_name',
            'trade_id',
            'side',
            'price',
            'size',
            'quote_quantity',
            'timestamp',
            'datetime',
        ),
    ),
    'bybit_spot_trades': _ExchangeIntegritySpec(
        tuple_size=13,
        id_index=1,
        timestamp_index=7,
        price_index=4,
        quantity_index=5,
        quote_quantity_index=6,
        side_index=3,
        bool_indices=(),
        first_last_trade_indices=None,
        datetime_index=8,
        enforce_monotonic_timestamp=True,
        frame_columns=(
            'symbol',
            'trade_id',
            'trd_match_id',
            'side',
            'price',
            'size',
            'quote_quantity',
            'timestamp',
            'datetime',
            'tick_direction',
            'gross_value',
            'home_notional',
            'foreign_notional',
        ),
    ),
}


def _expect_row_size(*, row: Sequence[Any], expected_size: int, row_number: int) -> None:
    if len(row) != expected_size:
        raise ValueError(
            f'Exchange integrity schema/type check failed at row={row_number}: '
            f'expected tuple size={expected_size}, got={len(row)}'
        )


def _expect_int(*, value: Any, label: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        raise ValueError(f'Exchange integrity schema/type check failed for {label}')
    return value


def _expect_positive_number(*, value: Any, label: str) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise ValueError(f'Exchange integrity schema/type check failed for {label}')
    numeric_value = float(value)
    if numeric_value <= 0:
        raise ValueError(
            f'Exchange integrity anomaly check failed for {label}: '
            f'expected positive number, got={numeric_value}'
        )
    return numeric_value


def _expect_boolean_like(*, value: Any, label: str) -> None:
    if isinstance(value, bool):
        return
    if isinstance(value, int) and value in {0, 1}:
        return
    if isinstance(value, str) and value.lower() in {'true', 'false', '0', '1'}:
        return
    raise ValueError(f'Exchange integrity schema/type check failed for {label}')


def _expect_side(*, value: Any, label: str) -> None:
    if not isinstance(value, str):
        raise ValueError(f'Exchange integrity schema/type check failed for {label}')
    if value not in {'buy', 'sell'}:
        raise ValueError(
            f'Exchange integrity anomaly check failed for {label}: '
            f'expected one of [buy, sell], got={value}'
        )


def _expect_datetime(*, value: Any, label: str) -> None:
    if not isinstance(value, datetime):
        raise ValueError(f'Exchange integrity schema/type check failed for {label}')


def run_exchange_integrity_suite_rows(
    *, dataset: ExchangeDataset, rows: Iterable[Sequence[Any]]
) -> ExchangeIntegrityReport:
    spec = _EXCHANGE_INTEGRITY_SPECS[dataset]

    min_id: int | None = None
    max_id: int | None = None
    previous_id: int | None = None
    previous_timestamp: int | None = None
    rows_checked = 0
    anomaly_checks_performed = 0

    for row_number, row in enumerate(rows, start=1):
        _expect_row_size(row=row, expected_size=spec.tuple_size, row_number=row_number)

        id_value = _expect_int(
            value=row[spec.id_index],
            label=f'{dataset}.row[{row_number}].id',
        )
        if id_value < 0:
            raise ValueError(
                f'Exchange integrity anomaly check failed for {dataset}.row[{row_number}].id: '
                f'expected >= 0, got={id_value}'
            )

        if previous_id is not None:
            if id_value <= previous_id:
                raise ValueError(
                    f'Exchange integrity sequence-gap check failed for {dataset}: '
                    f'row={row_number}, previous_id={previous_id}, current_id={id_value}'
                )
            if id_value != previous_id + 1:
                raise ValueError(
                    f'Exchange integrity sequence-gap check failed for {dataset}: '
                    f'row={row_number}, expected_next_id={previous_id + 1}, got={id_value}'
                )
        previous_id = id_value

        if min_id is None or id_value < min_id:
            min_id = id_value
        if max_id is None or id_value > max_id:
            max_id = id_value

        timestamp_value = _expect_int(
            value=row[spec.timestamp_index],
            label=f'{dataset}.row[{row_number}].timestamp',
        )
        if timestamp_value <= 0:
            raise ValueError(
                f'Exchange integrity anomaly check failed for {dataset}.row[{row_number}].timestamp: '
                f'expected > 0, got={timestamp_value}'
            )
        anomaly_checks_performed += 1
        if spec.enforce_monotonic_timestamp and previous_timestamp is not None:
            if timestamp_value < previous_timestamp:
                raise ValueError(
                    f'Exchange integrity monotonic-time check failed for {dataset}: '
                    f'row={row_number}, previous_timestamp={previous_timestamp}, '
                    f'current_timestamp={timestamp_value}'
                )
            anomaly_checks_performed += 1
        previous_timestamp = timestamp_value

        _expect_positive_number(
            value=row[spec.price_index],
            label=f'{dataset}.row[{row_number}].price',
        )
        anomaly_checks_performed += 1

        _expect_positive_number(
            value=row[spec.quantity_index],
            label=f'{dataset}.row[{row_number}].quantity',
        )
        anomaly_checks_performed += 1

        if spec.quote_quantity_index is not None:
            _expect_positive_number(
                value=row[spec.quote_quantity_index],
                label=f'{dataset}.row[{row_number}].quote_quantity',
            )
            anomaly_checks_performed += 1

        if spec.side_index is not None:
            _expect_side(
                value=row[spec.side_index],
                label=f'{dataset}.row[{row_number}].side',
            )
            anomaly_checks_performed += 1

        if spec.first_last_trade_indices is not None:
            first_trade_id = _expect_int(
                value=row[spec.first_last_trade_indices[0]],
                label=f'{dataset}.row[{row_number}].first_trade_id',
            )
            last_trade_id = _expect_int(
                value=row[spec.first_last_trade_indices[1]],
                label=f'{dataset}.row[{row_number}].last_trade_id',
            )
            if first_trade_id < 0 or last_trade_id < 0 or first_trade_id > last_trade_id:
                raise ValueError(
                    f'Exchange integrity anomaly check failed for {dataset}.row[{row_number}]: '
                    f'first_trade_id={first_trade_id}, last_trade_id={last_trade_id}'
                )
            anomaly_checks_performed += 1

        for bool_index in spec.bool_indices:
            _expect_boolean_like(
                value=row[bool_index],
                label=f'{dataset}.row[{row_number}].bool_col[{bool_index}]',
            )
            anomaly_checks_performed += 1

        if spec.datetime_index is not None:
            _expect_datetime(
                value=row[spec.datetime_index],
                label=f'{dataset}.row[{row_number}].datetime',
            )
            anomaly_checks_performed += 1

        rows_checked += 1

    if rows_checked == 0:
        raise ValueError(
            f'Exchange integrity schema/type check failed for {dataset}: no rows found'
        )
    if min_id is None or max_id is None:
        raise RuntimeError(
            f'Exchange integrity internal error for {dataset}: id bounds missing after validation'
        )

    contiguous_span = max_id - min_id + 1
    sequence_gap_count = contiguous_span - rows_checked
    if sequence_gap_count != 0:
        raise ValueError(
            f'Exchange integrity sequence-gap check failed for {dataset}: '
            f'min_id={min_id}, max_id={max_id}, rows={rows_checked}, '
            f'gap_count={sequence_gap_count}'
        )

    return ExchangeIntegrityReport(
        dataset=dataset,
        rows_checked=rows_checked,
        min_id=min_id,
        max_id=max_id,
        sequence_gap_count=sequence_gap_count,
        anomaly_checks_performed=anomaly_checks_performed,
    )


def run_exchange_integrity_suite_frame(
    *, dataset: ExchangeDataset, frame: pl.DataFrame
) -> ExchangeIntegrityReport:
    spec = _EXCHANGE_INTEGRITY_SPECS[dataset]
    missing_columns = sorted(set(spec.frame_columns).difference(frame.columns))
    if len(missing_columns) > 0:
        raise ValueError(
            f'Exchange integrity schema/type check failed for {dataset}: '
            f'missing columns={missing_columns}'
        )

    selected = frame.select(list(spec.frame_columns))
    return run_exchange_integrity_suite_rows(
        dataset=dataset,
        rows=selected.iter_rows(),
    )
