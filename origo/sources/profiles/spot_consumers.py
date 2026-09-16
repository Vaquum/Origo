from __future__ import annotations

import hashlib
import importlib
import json
import os
from collections.abc import Callable
from pathlib import Path
from typing import Protocol, cast
from uuid import uuid4

import polars as pl

from origo.assets.build_bar_store_arrow import build_series_frame
from origo.assets.publish_binance_spot_klines_to_mount import SPECS
from origo.query.binance_spot_kline_rollups import dollar_month, time_month

from ..contracts import ConsumerSpec, Snapshot, SnapshotReader
from ..storage import SourceStore


class _TimeExport(Protocol):
    def __call__(
        self,
        *,
        kline_size_seconds: int,
        start_date_limit: str,
        end_date_limit: str,
        table_name: str,
        database_name: str,
    ) -> pl.DataFrame: ...


class _DollarExport(Protocol):
    def __call__(
        self,
        *,
        dollar_size: float,
        start_date_limit: str,
        end_date_limit: str,
        table_name: str,
        database_name: str,
    ) -> pl.DataFrame: ...


def _fsync(path: Path) -> None:
    with path.open('rb') as handle:
        os.fsync(handle.fileno())


def _renderer(kind: str) -> Callable[[SnapshotReader, Snapshot, str], None]:
    def publish(reader: SnapshotReader, snapshot: Snapshot, destination: str) -> None:
        if not isinstance(reader, SourceStore):
            raise TypeError('The spot compatibility renderer requires its declared SQL store.')
        root = Path(destination).resolve()
        if not Path(destination).is_absolute() or root.parts[-2:] != (reader.spec.key, kind):
            raise ValueError('A shadow destination must be absolute and scoped to this source key.')
        if not snapshot.records:
            raise RuntimeError('A consumer cannot publish an empty source state.')
        canonical_only = kind == 'huggingface_shadow'
        build = root / 'versions' / (snapshot.token + '-' + uuid4().hex)
        build.mkdir(parents=True)
        database = 'source_consumer_' + uuid4().hex
        reader.execute(f'CREATE DATABASE {database}')
        files: list[dict[str, object]] = []
        try:
            for component in reader.spec.components:
                if component.key not in ('time', 'dollar', 'time_latest', 'raw_latest'):
                    continue
                columns = ', '.join(column.name for column in component.columns)
                states = tuple(
                    (record.partition.key, record.revision, record.build_id)
                    for record in snapshot.records
                    if record.partition.provisional == component.provisional
                )
                predicate = '(partition_key, revision, build_id) IN %(states)s' if states else '0'
                reader.execute(
                    f'CREATE VIEW {database}.{component.key} AS SELECT {columns} '
                    f'FROM {reader.component_table(component.key)} WHERE {predicate}',
                    {'states': states} if states else None,
                )
            months = sorted(
                {
                    (record.partition.start.year, record.partition.start.month)
                    for record in snapshot.records
                }
            )
            for series in SPECS:
                if kind == 'huggingface_shadow':
                    end = max(record.partition.end for record in snapshot.records).strftime(
                        '%Y-%m-%d %H:%M:%S'
                    )
                    if series.family == 'time':
                        module = importlib.import_module(
                            'origo.utils.publish_binance_spot_kline_snapshot_to_huggingface'
                        )
                        export_time = cast(
                            _TimeExport, module._get_binance_spot_klines_from_1m_projection
                        )
                        frame = export_time(
                            kline_size_seconds=series.size * 60,
                            start_date_limit='2020-01-01',
                            end_date_limit=end,
                            table_name='time',
                            database_name=database,
                        )
                    else:
                        module = importlib.import_module(
                            'origo.utils.publish_binance_spot_dollar_kline_snapshot_to_huggingface'
                        )
                        export_dollar = cast(_DollarExport, module._get_binance_spot_dollar_klines)
                        frame = export_dollar(
                            dollar_size=float(series.size * 1000000),
                            start_date_limit='2020-01-01',
                            end_date_limit=end,
                            table_name='dollar',
                            database_name=database,
                        )
                    target = build / f'{series.name}.parquet'
                    frame.write_parquet(target, compression='zstd')
                    files.append(_manifest_file(build, target, frame.height))
                else:
                    for year, month in months:
                        if series.family == 'time':
                            frame = time_month(
                                interval_minutes=series.size,
                                year=year,
                                month=month,
                                base_table='time',
                                latest_table='time_latest',
                                database=database,
                            )
                        else:
                            frame = dollar_month(
                                ratio=series.size,
                                year=year,
                                month=month,
                                base_table='dollar',
                                raw_latest_table='raw_latest',
                                database=database,
                            )
                        target = build / series.sub_path / f'{year:04d}' / f'{month:02d}.parquet'
                        target.parent.mkdir(parents=True, exist_ok=True)
                        frame.write_parquet(target, compression='zstd')
                        files.append(_manifest_file(build, target, frame.height))
                    if kind == 'arrow':
                        shaped = build_series_frame(series, build)
                        target = build / f'{series.name}.arrow'
                        shaped.df.write_ipc(target, compression='uncompressed')
                        files.append(_manifest_file(build, target, shaped.df.height))
            manifest = {
                'source_key': reader.spec.key,
                'state_token': snapshot.token,
                'active_through': max(
                    record.partition.end for record in snapshot.records
                ).isoformat(),
                'kind': kind,
                'files': files,
                'version': build.name,
            }
            target = build / 'manifest.json'
            target.write_text(json.dumps(manifest, sort_keys=True, indent=2) + '\n')
            _fsync(target)
            if reader.snapshot(canonical_only=canonical_only).token != snapshot.token:
                raise RuntimeError(
                    'Consumer state changed while rendering; staged output remains unpublished.'
                )
            pending = root / ('.latest-' + uuid4().hex)
            pending.write_text(json.dumps(manifest, sort_keys=True, indent=2) + '\n')
            _fsync(pending)
            os.replace(pending, root / 'latest.json')
        finally:
            reader.execute(f'DROP DATABASE {database} SYNC')

    return publish


def _manifest_file(root: Path, path: Path, row_count: int) -> dict[str, object]:
    _fsync(path)
    with path.open('rb') as handle:
        digest = hashlib.file_digest(handle, 'sha256').hexdigest()
    return {
        'path': str(path.relative_to(root)),
        'row_count': row_count,
        'sha256': digest,
    }


SPOT_CONSUMERS = (
    ConsumerSpec('parquet', _renderer('parquet')),
    ConsumerSpec('arrow', _renderer('arrow')),
    ConsumerSpec('huggingface_shadow', _renderer('huggingface_shadow'), canonical_only=True),
)
