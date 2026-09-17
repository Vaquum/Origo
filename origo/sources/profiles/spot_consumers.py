"""Public spot publications rendered from a pinned source state.

``mount`` owns the local Parquet mirror and the Arrow bar store that the legacy
mirror and bar-store jobs published; ``huggingface`` owns the twelve public
Hugging Face datasets. Both render from ClickHouse views pinned to one snapshot.
"""

from __future__ import annotations

import hashlib
import importlib
import json
import os
from collections.abc import Callable, Iterator
from contextlib import contextmanager
from datetime import timedelta
from pathlib import Path
from typing import Protocol, cast
from uuid import uuid4

import polars as pl
from huggingface_hub import HfApi

from origo.assets.build_bar_store_arrow import (
    LATEST_NAME,
    build_series_frame,
    parquet_source_root,
    publish_series,
    series_store_dir,
)
from origo.assets.publish_binance_spot_klines_to_mount import (
    EXPORT_START_MONTH,
    EXPORT_START_YEAR,
    SPECS,
    MountKlineSpec,
    month_path,
)
from origo.query.binance_spot_kline_rollups import dollar_month, time_month

from ..contracts import ConsumerSpec, Snapshot, SnapshotReader, StateRecord
from ..hashing import state_token
from ..storage import SourceStore

EXPORT_START_DATE = f'{EXPORT_START_YEAR:04d}-{EXPORT_START_MONTH:02d}-01'
# The retired publishers' query, dataset-card and credential helpers, until they move here.
time_snapshot = importlib.import_module(
    'origo.utils.publish_binance_spot_kline_snapshot_to_huggingface'
)
dollar_snapshot = importlib.import_module(
    'origo.utils.publish_binance_spot_dollar_kline_snapshot_to_huggingface'
)


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


# The public Hugging Face datasets, exactly as the retired per-series publishers named them.
HUGGINGFACE_DATASETS: dict[str, tuple[str, str | None, str, str]] = {
    'time_1m': (
        'vaquum/binance_btcusdt_1m_klines',
        'HUGGINGFACE_DATASET_REPO_ID',
        'btcusdt_1m_kline_20200101_to_',
        '1-minute',
    ),
    'time_15m': (
        'vaquum/binance_btcusdt_15m_klines',
        None,
        'btcusdt_15m_kline_20200101_to_',
        '15-minute',
    ),
    'time_30m': (
        'vaquum/binance_btcusdt_30m_klines',
        None,
        'btcusdt_30m_kline_20200101_to_',
        '30-minute',
    ),
    'time_1h': (
        'vaquum/binance_btcusdt_1h_klines',
        None,
        'btcusdt_1h_kline_20200101_to_',
        '1-hour',
    ),
    'time_2h': (
        'vaquum/binance_btcusdt_2h_klines',
        None,
        'btcusdt_2h_kline_20200101_to_',
        '2-hour',
    ),
    'time_4h': (
        'vaquum/binance_btcusdt_4h_klines',
        None,
        'btcusdt_4h_kline_20200101_to_',
        '4-hour',
    ),
    'dollar_1M': (
        'vaquum/binance_btcusdt_1M_dollar_klines',
        None,
        'btcusdt_1M_dollar_kline_20200101_to_',
        '1M-dollar',
    ),
    'dollar_15M': (
        'vaquum/binance_btcusdt_15M_dollar_klines',
        None,
        'btcusdt_15M_dollar_kline_20200101_to_',
        '15M-dollar',
    ),
    'dollar_30M': (
        'vaquum/binance_btcusdt_30M_dollar_klines',
        None,
        'btcusdt_30M_dollar_kline_20200101_to_',
        '30M-dollar',
    ),
    'dollar_60M': (
        'vaquum/binance_btcusdt_60M_dollar_klines',
        None,
        'btcusdt_60M_dollar_kline_20200101_to_',
        '60M-dollar',
    ),
    'dollar_120M': (
        'vaquum/binance_btcusdt_120M_dollar_klines',
        None,
        'btcusdt_120M_dollar_kline_20200101_to_',
        '120M-dollar',
    ),
    'dollar_240M': (
        'vaquum/binance_btcusdt_240M_dollar_klines',
        None,
        'btcusdt_240M_dollar_kline_20200101_to_',
        '240M-dollar',
    ),
}


def _fsync(path: Path) -> None:
    with path.open('rb') as handle:
        os.fsync(handle.fileno())


def _sha256(path: Path) -> str:
    with path.open('rb') as handle:
        return hashlib.file_digest(handle, 'sha256').hexdigest()


def _write_atomic(path: Path, payload: bytes) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    pending = path.parent / f'.{path.name}.partial-{uuid4().hex}'
    pending.write_bytes(payload)
    _fsync(pending)
    os.replace(pending, path)


def _write_parquet(frame: pl.DataFrame, target: Path) -> None:
    target.parent.mkdir(parents=True, exist_ok=True)
    pending = target.parent / f'.{target.name}.partial-{uuid4().hex}'
    frame.write_parquet(pending, compression='zstd')
    _fsync(pending)
    os.replace(pending, target)


def _root(destination: str, reader: SnapshotReader, kind: str) -> tuple[SourceStore, Path]:
    if not isinstance(reader, SourceStore):
        raise TypeError('The spot renderers require their declared SQL store.')
    root = Path(destination).resolve()
    if not Path(destination).is_absolute() or root.parts[-2:] != (reader.spec.key, kind):
        raise ValueError(
            'A publication destination must be absolute and scoped to this source key.'
        )
    return reader, root


def _previous_manifest(root: Path) -> dict[str, object]:
    path = root / 'latest.json'
    if not path.exists():
        return {}
    manifest: object = json.loads(path.read_text())
    if not isinstance(manifest, dict):
        raise ValueError('Publication manifest must be an object.')
    return cast(dict[str, object], manifest)


def _commit_manifest(reader: SourceStore, root: Path, manifest: dict[str, object]) -> None:
    if reader.snapshot(canonical_only=True).token != manifest['state_token']:
        raise RuntimeError(
            'Canonical state changed while rendering; staged output remains unpublished.'
        )
    root.mkdir(parents=True, exist_ok=True)
    _write_atomic(
        root / 'latest.json', (json.dumps(manifest, sort_keys=True, indent=2) + '\n').encode()
    )


@contextmanager
def _pinned(reader: SourceStore, snapshot: Snapshot) -> Iterator[str]:
    """Views over the exact component rows of one snapshot, in a throwaway database."""
    database = 'source_consumer_' + uuid4().hex
    reader.execute(f'CREATE DATABASE {database}')
    try:
        reader.execute(
            f'CREATE TABLE {database}.pinned_state '
            '(partition_key String, revision String, build_id UUID, provisional UInt8) ENGINE=Memory'
        )
        reader.execute(
            f'INSERT INTO {database}.pinned_state VALUES',
            [
                (
                    record.partition.key,
                    record.revision,
                    record.build_id,
                    int(record.partition.provisional),
                )
                for record in snapshot.records
            ],
        )
        for component in reader.spec.components:
            if component.key not in ('time', 'dollar', 'time_latest', 'raw_latest'):
                continue
            columns = ', '.join(column.name for column in component.columns)
            reader.execute(
                f'CREATE VIEW {database}.{component.key} AS SELECT {columns} '
                f'FROM {reader.component_table(component.key)} '
                'WHERE (partition_key, revision, build_id) IN '
                f'(SELECT partition_key, revision, build_id FROM {database}.pinned_state '
                f'WHERE provisional={int(component.provisional)})'
            )
        yield database
    finally:
        reader.execute(f'DROP DATABASE {database} SYNC')


def month_tokens(source: str, snapshot: Snapshot) -> dict[str, str]:
    """One state token per exported month, from the pinned records that start in it."""
    grouped: dict[str, list[StateRecord]] = {}
    for record in snapshot.records:
        month = record.partition.start.strftime('%Y-%m')
        if month < EXPORT_START_DATE[:7]:
            continue
        grouped.setdefault(month, []).append(record)
    return {month: state_token(source, tuple(grouped[month])) for month in sorted(grouped)}


def _month_frame(series: MountKlineSpec, year: int, month: int, database: str) -> pl.DataFrame:
    if series.family == 'time':
        return time_month(
            interval_minutes=series.size,
            year=year,
            month=month,
            base_table='time',
            latest_table='time_latest',
            database=database,
        )
    return dollar_month(
        ratio=series.size,
        year=year,
        month=month,
        base_table='dollar',
        raw_latest_table='raw_latest',
        database=database,
    )


def _entry(path: Path, row_count: int, **extra: object) -> dict[str, object]:
    return {'path': str(path), 'row_count': row_count, 'sha256': _sha256(path), **extra}


def _mount(reader: SnapshotReader, snapshot: Snapshot, destination: str) -> None:
    """Refresh the Parquet mirror months whose pinned state changed, then the Arrow series.

    Month files and Arrow versions live at the public roots (``LOCAL_PARQUET_DIR`` and
    ``LOCAL_ARROW_DIR``); the manifest under ``destination`` records which state they hold.
    """
    store, root = _root(destination, reader, 'mount')
    if not snapshot.records:
        raise RuntimeError('A consumer cannot publish an empty source state.')
    previous = _previous_manifest(root)
    previous_months = cast(dict[str, str], previous.get('month_tokens') or {})
    previous_files = {
        str(cast(dict[str, object], entry)['path']): cast(dict[str, object], entry)
        for entry in cast(list[object], previous.get('files') or [])
    }
    tokens = month_tokens(store.spec.key, snapshot)
    files: list[dict[str, object]] = []
    rebuilt: set[str] = set()
    with _pinned(store, snapshot) as database:
        for month, token in tokens.items():
            year, number = int(month[:4]), int(month[5:7])
            for series in SPECS:
                target = month_path(series.sub_path, year, number)
                entry = previous_files.get(str(target))
                if (
                    entry is not None
                    and previous_months.get(month) == token
                    and target.is_file()
                    and entry.get('sha256') == _sha256(target)
                ):
                    files.append(entry)
                    continue
                frame = _month_frame(series, year, number, database)
                if frame.height == 0:
                    continue
                _write_parquet(frame, target)
                files.append(_entry(target, frame.height, series=series.name, month=month))
                rebuilt.add(series.name)
    parquet_root = parquet_source_root()
    for series in SPECS:
        latest = series_store_dir(series.name) / LATEST_NAME
        entry = next(
            (
                item
                for item in previous_files.values()
                if item.get('series') == series.name and item.get('kind') == 'arrow'
            ),
            None,
        )
        if series.name not in rebuilt and entry is not None and latest.is_symlink():
            current = latest.resolve()
            if str(current) == entry['path'] and current.is_file():
                files.append(entry)
                continue
        build = build_series_frame(series, parquet_root)
        if build.df.height == 0:
            continue
        publish_series(series.name, build)
        current = latest.resolve()
        files.append(_entry(current, build.df.height, series=series.name, kind='arrow'))
    manifest: dict[str, object] = {
        'source_key': store.spec.key,
        'state_token': store.canonical_token(snapshot),
        'pinned_token': snapshot.token,
        'active_through': max(record.partition.end for record in snapshot.records).isoformat(),
        'kind': 'mount',
        'month_tokens': tokens,
        'files': files,
        'version': snapshot.token,
    }
    _commit_manifest(store, root, manifest)


def _huggingface(reader: SnapshotReader, snapshot: Snapshot, destination: str) -> None:
    """Upload every public dataset snapshot from the canonical state and keep a local copy."""
    store, root = _root(destination, reader, 'huggingface')
    if not snapshot.records:
        raise RuntimeError('A consumer cannot publish an empty source state.')
    end = max(record.partition.end for record in snapshot.records)
    export_end_date = (end - timedelta(days=1)).strftime('%Y-%m-%d')
    end_limit = end.strftime('%Y-%m-%d %H:%M:%S')
    build = root / 'versions' / (snapshot.token + '-' + uuid4().hex)
    build.mkdir(parents=True)
    api = HfApi(token=cast(Callable[[], str], time_snapshot._get_huggingface_token)())
    files: list[dict[str, object]] = []
    uploads: list[dict[str, object]] = []
    with _pinned(store, snapshot) as database:
        for series in SPECS:
            default_repo_id, repo_id_env, file_prefix, resolution_label = HUGGINGFACE_DATASETS[
                series.name
            ]
            if series.family == 'time':
                export_time = cast(
                    _TimeExport, time_snapshot._get_binance_spot_klines_from_1m_projection
                )
                frame = export_time(
                    kline_size_seconds=series.size * 60,
                    start_date_limit=EXPORT_START_DATE,
                    end_date_limit=end_limit,
                    table_name='time',
                    database_name=database,
                )
            else:
                export_dollar = cast(_DollarExport, dollar_snapshot._get_binance_spot_dollar_klines)
                frame = export_dollar(
                    dollar_size=float(series.size * 1000000),
                    start_date_limit=EXPORT_START_DATE,
                    end_date_limit=end_limit,
                    table_name='dollar',
                    database_name=database,
                )
            if frame.height == 0:
                continue
            file_name = f'{file_prefix}{export_end_date.replace("-", "")}.parquet'
            folder = build / series.name
            folder.mkdir()
            parquet = folder / file_name
            frame.write_parquet(parquet, compression='zstd')
            digest = _sha256(parquet)
            label = series.name.split('_', 1)[1]
            if series.family == 'time':
                card = cast(Callable[..., str], time_snapshot._build_dataset_card)(
                    export_end_date=export_end_date,
                    row_count=frame.height,
                    file_name=file_name,
                    cadence_label=label,
                    resolution_label=resolution_label,
                )
            else:
                card = cast(Callable[..., str], dollar_snapshot._build_dataset_card)(
                    export_end_date=export_end_date,
                    row_count=frame.height,
                    file_name=file_name,
                    size_label=label,
                    resolution_label=resolution_label,
                    database_name=store.database,
                )
            (folder / 'README.md').write_text(card, encoding='utf-8')
            (folder / 'latest.json').write_text(
                cast(Callable[..., str], time_snapshot._build_snapshot_metadata)(
                    export_end_date=export_end_date,
                    file_name=file_name,
                    row_count=frame.height,
                    file_sha256=digest,
                ),
                encoding='utf-8',
            )
            repo_id = cast(Callable[..., str], time_snapshot._get_huggingface_dataset_repo_id)(
                repo_id_env=repo_id_env, default_repo_id=default_repo_id
            )
            api.create_repo(repo_id=repo_id, repo_type='dataset', exist_ok=True)
            api.upload_folder(
                folder_path=str(folder),
                repo_id=repo_id,
                repo_type='dataset',
                commit_message=f'Add BTCUSDT {label} klines snapshot through {export_end_date}',
                delete_patterns=[f'{file_prefix}*.parquet'],
            )
            files.append(
                {
                    'path': str(parquet.relative_to(build)),
                    'row_count': frame.height,
                    'sha256': digest,
                }
            )
            uploads.append(
                {
                    'series': series.name,
                    'repo_id': repo_id,
                    'file_name': file_name,
                    'row_count': frame.height,
                    'sha256': digest,
                }
            )
    manifest: dict[str, object] = {
        'source_key': store.spec.key,
        'state_token': store.canonical_token(snapshot),
        'pinned_token': snapshot.token,
        'active_through': end.isoformat(),
        'kind': 'huggingface',
        'export_end_date': export_end_date,
        'uploads': uploads,
        'files': files,
        'version': build.name,
    }
    _commit_manifest(store, root, manifest)


Renderer = Callable[[SnapshotReader, Snapshot, str], None]

SPOT_CONSUMERS = (
    ConsumerSpec('mount', cast(Renderer, _mount), public=True),
    ConsumerSpec('huggingface', cast(Renderer, _huggingface), canonical_only=True, public=True),
)
