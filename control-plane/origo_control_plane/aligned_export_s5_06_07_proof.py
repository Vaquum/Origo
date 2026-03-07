from __future__ import annotations

import hashlib
import json
import os
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import Any

import polars as pl

from origo.query.aligned_core import AlignedDataset, query_aligned_data
from origo.query.native_core import TimeRangeWindow
from origo_control_plane.jobs.raw_export_native import origo_raw_export_native_job


@dataclass(frozen=True)
class _AlignedExportCase:
    case_id: str
    dataset: AlignedDataset
    fields: tuple[str, ...]
    time_range: tuple[str, str]


_CASE_DEFS: tuple[_AlignedExportCase, ...] = (
    _AlignedExportCase(
        case_id='spot_trades_time_range',
        dataset='spot_trades',
        fields=('aligned_at_utc', 'open_price', 'close_price', 'trade_count'),
        time_range=('2017-08-17T12:00:00Z', '2017-08-17T13:00:00Z'),
    ),
    _AlignedExportCase(
        case_id='etf_daily_metrics_time_range',
        dataset='etf_daily_metrics',
        fields=(
            'source_id',
            'metric_name',
            'valid_from_utc',
            'valid_to_utc_exclusive',
            'metric_value_string',
        ),
        time_range=('2026-03-05T12:00:00Z', '2026-03-07T12:00:00Z'),
    ),
)
_FORMATS: tuple[str, ...] = ('parquet', 'csv')


def _canonical_rows_for_hash(frame: pl.DataFrame) -> list[dict[str, str]]:
    normalized = frame.select(
        [
            pl.when(pl.col(column).is_null())
            .then(pl.lit('__NULL__'))
            .otherwise(pl.col(column).cast(pl.String))
            .alias(column)
            for column in frame.columns
        ]
    ).sort(frame.columns)
    return normalized.to_dicts()


def _stable_hash(rows: list[dict[str, Any]]) -> str:
    canonical = json.dumps(rows, sort_keys=True, separators=(',', ':'))
    return hashlib.sha256(canonical.encode('utf-8')).hexdigest()


def _read_exported_frame(*, artifact_path: Path, export_format: str) -> pl.DataFrame:
    if export_format == 'parquet':
        return pl.read_parquet(str(artifact_path))
    if export_format == 'csv':
        return pl.read_csv(str(artifact_path), try_parse_dates=True)
    raise RuntimeError(f'Unsupported export format: {export_format}')


def _roundtrip_expected_frame_for_format(
    *, frame: pl.DataFrame, export_format: str
) -> pl.DataFrame:
    with TemporaryDirectory(prefix='origo-s5-expected-roundtrip-') as tmp_dir:
        tmp_path = Path(tmp_dir) / f'expected.{export_format}'
        if export_format == 'parquet':
            frame.write_parquet(str(tmp_path))
            return pl.read_parquet(str(tmp_path))
        if export_format == 'csv':
            frame.write_csv(str(tmp_path))
            return pl.read_csv(str(tmp_path), try_parse_dates=True)
        raise RuntimeError(f'Unsupported export format: {export_format}')


def _run_case(*, case: _AlignedExportCase, export_format: str) -> dict[str, Any]:
    request_payload = {
        'mode': 'aligned_1s',
        'format': export_format,
        'dataset': case.dataset,
        'fields': list(case.fields),
        'month_year': None,
        'n_rows': None,
        'n_random': None,
        'time_range': [case.time_range[0], case.time_range[1]],
        'include_datetime': True,
        'strict': True,
    }
    tags = {
        'origo.export.mode': 'aligned_1s',
        'origo.export.format': export_format,
        'origo.export.dataset': case.dataset,
        'origo.export.request_json': json.dumps(request_payload, separators=(',', ':')),
    }

    execution = origo_raw_export_native_job.execute_in_process(tags=tags)
    if not execution.success:
        raise RuntimeError(
            f'S5-C6/C7 proof export run failed for case={case.case_id} format={export_format}'
        )

    run_id = execution.run_id
    if run_id is None or run_id.strip() == '':
        raise RuntimeError(
            f'S5-C6/C7 proof expected non-empty run_id for case={case.case_id} '
            f'format={export_format}'
        )

    export_root = Path(os.environ['ORIGO_EXPORT_ROOT_DIR'])
    export_dir = export_root / run_id
    metadata_path = export_dir / 'metadata.json'
    if not metadata_path.exists():
        raise RuntimeError(
            f'S5-C6/C7 proof expected metadata.json for case={case.case_id} format={export_format}'
        )
    metadata = json.loads(metadata_path.read_text(encoding='utf-8'))
    if metadata.get('mode') != 'aligned_1s':
        raise RuntimeError(
            f'S5-C6/C7 proof expected metadata mode=aligned_1s for case={case.case_id} '
            f'format={export_format}'
        )
    if metadata.get('format') != export_format:
        raise RuntimeError(
            f'S5-C6/C7 proof expected metadata format={export_format} '
            f'for case={case.case_id}'
        )
    artifact_path_raw = metadata.get('path')
    if not isinstance(artifact_path_raw, str) or artifact_path_raw.strip() == '':
        raise RuntimeError(
            f'S5-C6/C7 proof expected metadata path for case={case.case_id} format={export_format}'
        )
    artifact_path = Path(artifact_path_raw)
    if not artifact_path.exists():
        raise RuntimeError(
            f'S5-C6/C7 proof expected artifact file at path={artifact_path} '
            f'for case={case.case_id} format={export_format}'
        )

    exported_frame = _read_exported_frame(
        artifact_path=artifact_path,
        export_format=export_format,
    )
    expected_frame_raw = query_aligned_data(
        dataset=case.dataset,
        window=TimeRangeWindow(
            start_iso=case.time_range[0],
            end_iso=case.time_range[1],
        ),
        selected_columns=case.fields,
        auth_token=None,
        show_summary=False,
        datetime_iso_output=False,
    )
    expected_frame = _roundtrip_expected_frame_for_format(
        frame=expected_frame_raw,
        export_format=export_format,
    )

    if exported_frame.columns != list(case.fields):
        raise RuntimeError(
            f'S5-C6/C7 proof expected exported columns={list(case.fields)} '
            f'for case={case.case_id} format={export_format}, got={exported_frame.columns}'
        )
    if exported_frame.height == 0:
        raise RuntimeError(
            f'S5-C6/C7 proof expected non-empty exported rows for case={case.case_id} '
            f'format={export_format}'
        )

    exported_rows = _canonical_rows_for_hash(exported_frame)
    expected_rows = _canonical_rows_for_hash(expected_frame)

    if len(exported_rows) != len(expected_rows):
        raise RuntimeError(
            f'S5-C6/C7 proof expected parity row_count for case={case.case_id} '
            f'format={export_format}'
        )

    exported_hash = _stable_hash(exported_rows)
    expected_hash = _stable_hash(expected_rows)
    if exported_hash != expected_hash:
        raise RuntimeError(
            f'S5-C6/C7 proof expected parity content hash for case={case.case_id} '
            f'format={export_format}'
        )

    checksum_sha256 = metadata.get('checksum_sha256')
    if not isinstance(checksum_sha256, str) or checksum_sha256.strip() == '':
        raise RuntimeError(
            f'S5-C6/C7 proof expected non-empty metadata checksum for case={case.case_id} '
            f'format={export_format}'
        )

    return {
        'case_id': case.case_id,
        'dataset': case.dataset,
        'format': export_format,
        'run_id': run_id,
        'window': {'time_range': [case.time_range[0], case.time_range[1]]},
        'row_count': exported_frame.height,
        'columns': exported_frame.columns,
        'artifact_path': str(artifact_path),
        'metadata_path': str(metadata_path),
        'artifact_checksum_sha256': checksum_sha256,
        'expected_rows_hash_sha256': expected_hash,
        'exported_rows_hash_sha256': exported_hash,
        'parity_match': exported_hash == expected_hash,
    }


def run_s5_06_07_proof() -> dict[str, Any]:
    with TemporaryDirectory(prefix='origo-s5-aligned-export-') as export_dir:
        os.environ['ORIGO_EXPORT_ROOT_DIR'] = export_dir

        results: list[dict[str, Any]] = []
        for case in _CASE_DEFS:
            for export_format in _FORMATS:
                results.append(_run_case(case=case, export_format=export_format))

    by_format: dict[str, bool] = {}
    for export_format in _FORMATS:
        format_results = [item for item in results if item['format'] == export_format]
        by_format[export_format] = all(
            bool(item['parity_match']) for item in format_results
        )

    return {
        'generated_at_utc': datetime.now(UTC).isoformat(),
        'proof_scope': 'Slice 5 S5-C6/S5-C7 aligned exports (parquet/csv)',
        'results': results,
        'parity_match': all(by_format.values()),
        'parity_match_by_format': by_format,
    }


def main() -> None:
    result = run_s5_06_07_proof()
    repo_root = Path(__file__).resolve().parents[2]
    output_path = repo_root / (
        'spec/slices/slice-5-raw-query-aligned-1s/'
        'capability-proof-s5-c6-c7-aligned-export.json'
    )
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps(result, indent=2, sort_keys=True), encoding='utf-8')
    print(json.dumps(result, indent=2, sort_keys=True))


if __name__ == '__main__':
    main()
