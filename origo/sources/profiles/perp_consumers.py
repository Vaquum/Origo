"""Public perp publications rendered from a pinned source state.

``mount`` owns the local Parquet mirror and the Arrow bar store; ``huggingface``
owns the twelve public Hugging Face datasets. Both render from ClickHouse views
pinned to one snapshot. Series paths carry the perp prefix so the perp mirror
never collides with spot's.
"""

from __future__ import annotations

import hashlib
import json
import os
import shutil
import time
from collections.abc import Callable, Iterator
from contextlib import contextmanager
from datetime import timedelta
from pathlib import Path
from typing import cast
from uuid import uuid4

import polars as pl
from huggingface_hub import HfApi

from origo.query.binance_spot_kline_rollups import dollar_month, time_month
from origo.utils.arrow_store import (
    LATEST_NAME,
    StagedSeries,
    activate_series,
    build_series_frame,
    discard_series,
    parquet_source_root,
    series_source_files,
    series_store_dir,
    stage_series,
)

from ..contracts import ConsumerSpec, Snapshot, SnapshotReader, StateRecord
from ..hashing import state_token
from ..storage import SourceStore
from .formulas import huggingface_time as time_snapshot
from .formulas import perp_huggingface as perp_snapshot
from .formulas.perp_series import EXPORT_START_MONTH, EXPORT_START_YEAR, SPECS
from .formulas.spot_series import MountKlineSpec, month_path

EXPORT_START_DATE = f'{EXPORT_START_YEAR:04d}-{EXPORT_START_MONTH:02d}-01'
# A staging directory or partial manifest older than this belongs to a render that died.
ORPHAN_STAGING_MAX_AGE_SECONDS = 3600


# The public Hugging Face datasets, exactly as the retired per-series publishers named them.
HUGGINGFACE_DATASETS: dict[str, tuple[str, str | None, str, str]] = {
    'perp_time_1m': (
        'vaquum/binance_btcusdt_perp_1m_klines',
        'HUGGINGFACE_DATASET_REPO_ID',
        'btcusdt_perp_1m_kline_20200101_to_',
        '1-minute',
    ),
    'perp_time_15m': (
        'vaquum/binance_btcusdt_perp_15m_klines',
        None,
        'btcusdt_perp_15m_kline_20200101_to_',
        '15-minute',
    ),
    'perp_time_30m': (
        'vaquum/binance_btcusdt_perp_30m_klines',
        None,
        'btcusdt_perp_30m_kline_20200101_to_',
        '30-minute',
    ),
    'perp_time_1h': (
        'vaquum/binance_btcusdt_perp_1h_klines',
        None,
        'btcusdt_perp_1h_kline_20200101_to_',
        '1-hour',
    ),
    'perp_time_2h': (
        'vaquum/binance_btcusdt_perp_2h_klines',
        None,
        'btcusdt_perp_2h_kline_20200101_to_',
        '2-hour',
    ),
    'perp_time_4h': (
        'vaquum/binance_btcusdt_perp_4h_klines',
        None,
        'btcusdt_perp_4h_kline_20200101_to_',
        '4-hour',
    ),
    'perp_dollar_1M': (
        'vaquum/binance_btcusdt_perp_1M_klines',
        None,
        'btcusdt_perp_1M_kline_20200101_to_',
        '1M-dollar',
    ),
    'perp_dollar_15M': (
        'vaquum/binance_btcusdt_perp_15M_klines',
        None,
        'btcusdt_perp_15M_kline_20200101_to_',
        '15M-dollar',
    ),
    'perp_dollar_30M': (
        'vaquum/binance_btcusdt_perp_30M_klines',
        None,
        'btcusdt_perp_30M_kline_20200101_to_',
        '30M-dollar',
    ),
    'perp_dollar_60M': (
        'vaquum/binance_btcusdt_perp_60M_klines',
        None,
        'btcusdt_perp_60M_kline_20200101_to_',
        '60M-dollar',
    ),
    'perp_dollar_120M': (
        'vaquum/binance_btcusdt_perp_120M_klines',
        None,
        'btcusdt_perp_120M_kline_20200101_to_',
        '120M-dollar',
    ),
    'perp_dollar_240M': (
        'vaquum/binance_btcusdt_perp_240M_klines',
        None,
        'btcusdt_perp_240M_kline_20200101_to_',
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


def _clear_orphan_staging(parquet_root: Path, root: Path, now: float) -> None:
    """Remove staging directories and partial manifests a hard-killed render left behind.

    Renders of one consumer are serialized, so anything older than the grace window is an
    orphan; the window keeps a render that is still running out of reach.
    """
    cutoff = now - ORPHAN_STAGING_MAX_AGE_SECONDS
    for orphan in parquet_root.glob('.staging-*'):
        if orphan.is_dir() and orphan.stat().st_mtime < cutoff:
            shutil.rmtree(orphan, ignore_errors=True)
    for orphan in root.glob('.*.partial-*'):
        if orphan.is_file() and orphan.stat().st_mtime < cutoff:
            orphan.unlink(missing_ok=True)


def _root(destination: str, reader: SnapshotReader, kind: str) -> tuple[SourceStore, Path]:
    if not isinstance(reader, SourceStore):
        raise TypeError('The perp renderers require their declared SQL store.')
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


def _require_canonical(reader: SourceStore, token: object) -> None:
    if reader.snapshot(canonical_only=True).token != token:
        raise RuntimeError(
            'Canonical state changed while rendering; staged output remains unpublished.'
        )


def _write_manifest(root: Path, manifest: dict[str, object]) -> None:
    root.mkdir(parents=True, exist_ok=True)
    _write_atomic(
        root / 'latest.json', (json.dumps(manifest, sort_keys=True, indent=2) + '\n').encode()
    )


def _commit_manifest(reader: SourceStore, root: Path, manifest: dict[str, object]) -> None:
    _require_canonical(reader, manifest['state_token'])
    _write_manifest(root, manifest)


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
    Months render into a staging directory beside the mirror and Arrow versions are written
    without flipping ``latest``; only a render whose canonical state is unchanged moves the
    months into place and activates the versions, so a discarded render leaves the public
    roots exactly as the manifest describes them. Staging left by a render that died is
    swept once it is older than the grace window.
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
    state = store.canonical_token(snapshot)
    parquet_root = parquet_source_root()
    _clear_orphan_staging(parquet_root, root, time.time())
    staging = parquet_root / f'.staging-{uuid4().hex}'
    files: list[dict[str, object]] = []
    staged_months: dict[Path, Path] = {}
    rebuilt: set[str] = set()
    staged_series: list[StagedSeries] = []
    try:
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
                    pending = staging / target.relative_to(parquet_root)
                    _write_parquet(frame, pending)
                    staged_months[target] = pending
                    files.append(
                        {
                            'path': str(target),
                            'row_count': frame.height,
                            'sha256': _sha256(pending),
                            'series': series.name,
                            'month': month,
                        }
                    )
                    rebuilt.add(series.name)
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
            months = {path: path for path in series_source_files(series, parquet_root)}
            months.update(
                (target, pending)
                for target, pending in staged_months.items()
                if target.is_relative_to(parquet_root / series.sub_path)
            )
            build = build_series_frame(
                series, parquet_root, files=[months[key] for key in sorted(months)]
            )
            staged = stage_series(series.name, build)
            if staged is not None:
                staged_series.append(staged)
        _require_canonical(store, state)
        for target, pending in staged_months.items():
            target.parent.mkdir(parents=True, exist_ok=True)
            os.replace(pending, target)
        for staged in staged_series:
            if activate_series(staged).status == 'skipped_not_newer':
                discard_series(staged)
            current = (series_store_dir(staged.series) / LATEST_NAME).resolve()
            files.append(_entry(current, staged.row_count, series=staged.series, kind='arrow'))
    except BaseException:
        for staged in staged_series:
            discard_series(staged)
        raise
    finally:
        shutil.rmtree(staging, ignore_errors=True)
    manifest: dict[str, object] = {
        'source_key': store.spec.key,
        'state_token': state,
        'pinned_token': snapshot.token,
        'active_through': max(record.partition.end for record in snapshot.records).isoformat(),
        'kind': 'mount',
        'month_tokens': tokens,
        'files': files,
        'version': snapshot.token,
    }
    _write_manifest(root, manifest)


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
    api = HfApi(token=time_snapshot.get_huggingface_token())
    files: list[dict[str, object]] = []
    uploads: list[dict[str, object]] = []
    with _pinned(store, snapshot) as database:
        for series in SPECS:
            default_repo_id, repo_id_env, file_prefix, resolution_label = HUGGINGFACE_DATASETS[
                series.name
            ]
            if series.family == 'time':
                frame = perp_snapshot.get_perp_klines_from_1m_projection(
                    kline_size_seconds=series.size * 60,
                    start_date_limit=EXPORT_START_DATE,
                    end_date_limit=end_limit,
                    table_name='time',
                    database_name=database,
                )
            else:
                frame = perp_snapshot.get_perp_dollar_klines(
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
            label = series.name.split('_')[2]
            if series.family == 'time':
                card = perp_snapshot.build_time_dataset_card(
                    export_end_date=export_end_date,
                    row_count=frame.height,
                    file_name=file_name,
                    cadence_label=label,
                    resolution_label=resolution_label,
                )
            else:
                card = perp_snapshot.build_dollar_dataset_card(
                    export_end_date=export_end_date,
                    row_count=frame.height,
                    file_name=file_name,
                    size_label=label,
                    resolution_label=resolution_label,
                    database_name=store.database,
                )
            (folder / 'README.md').write_text(card, encoding='utf-8')
            (folder / 'latest.json').write_text(
                time_snapshot.build_snapshot_metadata(
                    export_end_date=export_end_date,
                    file_name=file_name,
                    row_count=frame.height,
                    file_sha256=digest,
                ),
                encoding='utf-8',
            )
            repo_id = time_snapshot.get_huggingface_dataset_repo_id(
                repo_id_env=repo_id_env, default_repo_id=default_repo_id
            )
            api.create_repo(repo_id=repo_id, repo_type='dataset', exist_ok=True)
            api.upload_folder(
                folder_path=str(folder),
                repo_id=repo_id,
                repo_type='dataset',
                commit_message=f'Add BTCUSDT {label} perp klines snapshot through {export_end_date}',
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

PERP_CONSUMERS = (
    ConsumerSpec('mount', cast(Renderer, _mount), public=True),
    ConsumerSpec('huggingface', cast(Renderer, _huggingface), canonical_only=True, public=True),
)
