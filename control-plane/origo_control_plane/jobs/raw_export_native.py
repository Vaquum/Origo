import hashlib
import json
import os
from collections.abc import Mapping
from pathlib import Path
from typing import Any, Literal, cast

import dagster as dg
import polars as pl
from dagster import OpExecutionContext

from origo.query.aligned_core import AlignedDataset, query_aligned_data
from origo.query.binance_native import BinanceDataset, query_binance_native_data
from origo.query.bitcoin_native import BitcoinDataset, query_bitcoin_native_data
from origo.query.bybit_native import BybitDataset, query_bybit_native_data
from origo.query.etf_native import ETFDataset, query_etf_native_data
from origo.query.fred_native import FREDDataset, query_fred_native_data
from origo.query.native_core import (
    LatestRowsWindow,
    MonthWindow,
    QueryWindow,
    RandomRowsWindow,
    TimeRangeWindow,
)
from origo.query.okx_native import OKXDataset, query_okx_native_data

job: Any = getattr(dg, 'job')
op: Any = getattr(dg, 'op')
type ExportDataset = (
    BinanceDataset
    | BitcoinDataset
    | BybitDataset
    | ETFDataset
    | FREDDataset
    | OKXDataset
)
type ExportMode = Literal['native', 'aligned_1s']
_ALIGNED_EXPORT_DATASETS: frozenset[AlignedDataset] = frozenset(
    {
        'spot_trades',
        'spot_agg_trades',
        'futures_trades',
        'okx_spot_trades',
        'bybit_spot_trades',
        'etf_daily_metrics',
        'fred_series_metrics',
        'bitcoin_block_fee_totals',
        'bitcoin_block_subsidy_schedule',
        'bitcoin_network_hashrate_estimate',
        'bitcoin_circulating_supply',
    }
)


def _require_env(name: str) -> str:
    value = os.environ.get(name)
    if value is None or value.strip() == '':
        raise RuntimeError(f'{name} must be set and non-empty')
    return value


def _expect_dict(value: Any, label: str) -> dict[str, Any]:
    if not isinstance(value, dict):
        raise RuntimeError(f'{label} must be an object')
    raw_map = cast(dict[Any, Any], value)
    normalized: dict[str, Any] = {}
    for raw_key, raw_value in raw_map.items():
        if not isinstance(raw_key, str):
            raise RuntimeError(f'{label} keys must be strings')
        normalized[raw_key] = raw_value
    return normalized


def _expect_list(value: Any, label: str) -> list[Any]:
    if not isinstance(value, list):
        raise RuntimeError(f'{label} must be a list')
    return cast(list[Any], value)


def _read_dataset(value: Any) -> ExportDataset:
    if value not in {
        'spot_trades',
        'spot_agg_trades',
        'futures_trades',
        'okx_spot_trades',
        'bybit_spot_trades',
        'etf_daily_metrics',
        'fred_series_metrics',
        'bitcoin_block_headers',
        'bitcoin_block_transactions',
        'bitcoin_mempool_state',
        'bitcoin_block_fee_totals',
        'bitcoin_block_subsidy_schedule',
        'bitcoin_network_hashrate_estimate',
        'bitcoin_circulating_supply',
    }:
        raise RuntimeError(f'Unsupported export dataset: {value}')
    return cast(ExportDataset, value)


def _read_window(payload: dict[str, Any]) -> QueryWindow:
    month_year = payload.get('month_year')
    n_rows = payload.get('n_rows')
    n_random = payload.get('n_random')
    time_range = payload.get('time_range')

    selected_count = sum(
        [
            month_year is not None,
            n_rows is not None,
            n_random is not None,
            time_range is not None,
        ]
    )
    if selected_count != 1:
        raise RuntimeError(
            'Exactly one window mode must be set for export: '
            'month_year | n_rows | n_random | time_range'
        )

    if month_year is not None:
        month_year_list = _expect_list(month_year, 'month_year')
        if len(month_year_list) != 2:
            raise RuntimeError(
                'month_year must contain exactly two items: [month, year]'
            )
        month, year = month_year_list[0], month_year_list[1]
        if not isinstance(month, int) or not isinstance(year, int):
            raise RuntimeError('month_year values must be integers')
        return MonthWindow(month=month, year=year)

    if n_rows is not None:
        if not isinstance(n_rows, int):
            raise RuntimeError('n_rows must be an integer')
        return LatestRowsWindow(rows=n_rows)

    if n_random is not None:
        if not isinstance(n_random, int):
            raise RuntimeError('n_random must be an integer')
        return RandomRowsWindow(rows=n_random)

    time_range_list = _expect_list(time_range, 'time_range')
    if len(time_range_list) != 2:
        raise RuntimeError(
            'time_range must contain exactly two items: [start_iso, end_iso]'
        )
    start_iso, end_iso = time_range_list[0], time_range_list[1]
    if not isinstance(start_iso, str) or not isinstance(end_iso, str):
        raise RuntimeError('time_range values must be strings')
    return TimeRangeWindow(start_iso=start_iso, end_iso=end_iso)


def _read_fields(value: Any) -> tuple[str, ...] | None:
    if value is None:
        return None
    raw_fields = _expect_list(value, 'fields')
    fields: list[str] = []
    for raw_field in raw_fields:
        if not isinstance(raw_field, str) or raw_field.strip() == '':
            raise RuntimeError('fields must contain non-empty strings')
        fields.append(raw_field)
    return tuple(fields)


def _read_bool(value: Any, label: str) -> bool:
    if not isinstance(value, bool):
        raise RuntimeError(f'{label} must be a boolean')
    return value


def _read_export_tags(tags: Mapping[str, str]) -> tuple[ExportDataset, str]:
    dataset = _read_dataset(tags.get('origo.export.dataset'))
    export_format = tags.get('origo.export.format')
    if export_format not in {'parquet', 'csv'}:
        raise RuntimeError(f'Unsupported or missing export format tag: {export_format}')
    return dataset, export_format


def _write_export(frame: pl.DataFrame, export_dir: Path, export_format: str) -> Path:
    output_path = export_dir / f'raw_export.{export_format}'
    if export_format == 'parquet':
        frame.write_parquet(str(output_path))
    elif export_format == 'csv':
        frame.write_csv(str(output_path))
    else:
        raise RuntimeError(f'Unsupported export format: {export_format}')
    return output_path


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open('rb') as file_obj:
        while True:
            chunk = file_obj.read(1024 * 1024)
            if not chunk:
                break
            digest.update(chunk)
    return digest.hexdigest()


def _classify_failure_code(error: Exception) -> str:
    message = str(error)
    if 'strict=true export rejects mutable windows' in message:
        return 'EXPORT_STRICT_MUTABLE_WINDOW'
    if (
        message.startswith('Unsupported')
        or message.startswith('Missing required export request tag')
        or message.startswith('Exactly one window mode must be set for export')
        or 'must be' in message
    ):
        return 'EXPORT_REQUEST_CONTRACT_ERROR'
    return 'EXPORT_EXECUTION_ERROR'


def _write_failure_metadata(
    *,
    run_id: str,
    mode: ExportMode | None,
    dataset: ExportDataset | None,
    export_format: str | None,
    error_code: str,
    error_message: str,
) -> None:
    export_root = Path(_require_env('ORIGO_EXPORT_ROOT_DIR'))
    export_dir = export_root / run_id
    export_dir.mkdir(parents=True, exist_ok=True)
    failure_path = export_dir / 'error.json'
    failure_path.write_text(
        json.dumps(
            {
                'run_id': run_id,
                'mode': mode,
                'dataset': dataset,
                'format': export_format,
                'error_code': error_code,
                'error_message': error_message,
            },
            sort_keys=True,
            separators=(',', ':'),
        ),
        encoding='utf-8',
    )


@op(name='origo_raw_export_native_step')
def origo_raw_export_native_step(context: OpExecutionContext) -> None:
    mode: ExportMode | None = None
    dataset: ExportDataset | None = None
    export_format: str | None = None

    try:
        tags = context.run.tags
        raw_mode = tags.get('origo.export.mode')
        if raw_mode not in {'native', 'aligned_1s'}:
            raise RuntimeError(f'Unsupported or missing export mode tag: {raw_mode}')
        mode = cast(ExportMode, raw_mode)

        dataset, export_format = _read_export_tags(tags)
        raw_request_json = tags.get('origo.export.request_json')
        if raw_request_json is None or raw_request_json.strip() == '':
            raise RuntimeError(
                'Missing required export request tag: origo.export.request_json'
            )

        request_payload = _expect_dict(
            json.loads(raw_request_json), 'origo.export.request_json'
        )
        select_columns = _read_fields(request_payload.get('fields'))
        include_datetime = _read_bool(
            request_payload.get('include_datetime'), 'include_datetime'
        )
        strict = _read_bool(request_payload.get('strict'), 'strict')
        window = _read_window(request_payload)

        # Mutable windows are allowed only when strict=false.
        if strict and isinstance(window, (LatestRowsWindow, RandomRowsWindow)):
            raise RuntimeError(
                'strict=true export rejects mutable windows (n_rows and n_random)'
            )

        if mode == 'native':
            if dataset in {'spot_trades', 'spot_agg_trades', 'futures_trades'}:
                frame = query_binance_native_data(
                    dataset=cast(BinanceDataset, dataset),
                    select_columns=select_columns,
                    window=window,
                    include_datetime=include_datetime,
                    datetime_iso_output=False,
                    auth_token=None,
                    show_summary=False,
                )
            elif dataset == 'okx_spot_trades':
                frame = query_okx_native_data(
                    dataset=dataset,
                    select_columns=select_columns,
                    window=window,
                    include_datetime=include_datetime,
                    datetime_iso_output=False,
                    auth_token=None,
                    show_summary=False,
                )
            elif dataset == 'bybit_spot_trades':
                frame = query_bybit_native_data(
                    dataset=dataset,
                    select_columns=select_columns,
                    window=window,
                    include_datetime=include_datetime,
                    datetime_iso_output=False,
                    auth_token=None,
                    show_summary=False,
                )
            elif dataset == 'fred_series_metrics':
                frame = query_fred_native_data(
                    dataset=dataset,
                    select_columns=select_columns,
                    window=window,
                    include_datetime=include_datetime,
                    datetime_iso_output=False,
                    auth_token=None,
                    show_summary=False,
                )
            elif dataset in {
                'bitcoin_block_headers',
                'bitcoin_block_transactions',
                'bitcoin_mempool_state',
                'bitcoin_block_fee_totals',
                'bitcoin_block_subsidy_schedule',
                'bitcoin_network_hashrate_estimate',
                'bitcoin_circulating_supply',
            }:
                frame = query_bitcoin_native_data(
                    dataset=cast(BitcoinDataset, dataset),
                    select_columns=select_columns,
                    window=window,
                    include_datetime=include_datetime,
                    datetime_iso_output=False,
                    auth_token=None,
                    show_summary=False,
                )
            else:
                frame = query_etf_native_data(
                    dataset=cast(ETFDataset, dataset),
                    select_columns=select_columns,
                    window=window,
                    include_datetime=include_datetime,
                    datetime_iso_output=False,
                    auth_token=None,
                    show_summary=False,
                )
        elif mode == 'aligned_1s':
            if dataset not in _ALIGNED_EXPORT_DATASETS:
                raise RuntimeError(
                    f'aligned_1s mode does not support dataset={dataset}'
                )
            frame = query_aligned_data(
                dataset=dataset,
                window=window,
                selected_columns=select_columns,
                auth_token=None,
                show_summary=False,
                datetime_iso_output=False,
            )
        else:
            raise RuntimeError(f'Unsupported export mode tag: {mode}')

        export_root = Path(_require_env('ORIGO_EXPORT_ROOT_DIR'))
        export_dir = export_root / context.run_id
        export_dir.mkdir(parents=True, exist_ok=True)
        output_path = _write_export(frame, export_dir, export_format=export_format)
        checksum_sha256 = _sha256_file(output_path)
        metadata_path = export_dir / 'metadata.json'

        metadata_path.write_text(
            json.dumps(
                {
                    'run_id': context.run_id,
                    'mode': mode,
                    'format': export_format,
                    'dataset': dataset,
                    'path': str(output_path),
                    'row_count': frame.height,
                    'columns': frame.columns,
                    'checksum_sha256': checksum_sha256,
                },
                sort_keys=True,
                separators=(',', ':'),
            ),
            encoding='utf-8',
        )

        context.add_output_metadata(
            {
                'artifact_path': str(output_path),
                'metadata_path': str(metadata_path),
                'checksum_sha256': checksum_sha256,
                'row_count': frame.height,
                'column_count': frame.width,
            }
        )
    except Exception as exc:
        _write_failure_metadata(
            run_id=context.run_id,
            mode=mode,
            dataset=dataset,
            export_format=export_format,
            error_code=_classify_failure_code(exc),
            error_message=str(exc),
        )
        raise


@job(name='origo_raw_export_native_job')
def origo_raw_export_native_job() -> None:
    origo_raw_export_native_step()
