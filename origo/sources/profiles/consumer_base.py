"""Shared mount/huggingface publication renderers (PRD-0013 rows 5, 8, 13, 14).

Spot and perp declare their consumer parameters on a ConsumerDeclaration; the base
owns the mount mirror, the Arrow bar store, the Hugging Face uploads, manifests,
and orphan sweeping. Rollout state stays per-source: each source module keeps its
own ConsumerSpec tuple (LIVE spot publishes; CANARY perp renders a local shadow).

Scoping conventions this scaffold enforces (rows 8, 14):
- Series names carry the source scope as the first segment (``perp_time_1m``);
  legacy spot series are unscoped (``time_1m``). Label derivation is a declaration
  parameter so no renderer guesses the split.
- Publication destinations are ``<...>/<source_key>/<kind>``; ``_root`` rejects
  anything else. Mirror sub-paths come from the declared series specs.
- Per-source repo overrides are source-qualified
  (``HUGGINGFACE_PERP_DATASET_REPO_ID``, never a bare shared name); the datasets
  map carries the env slot per series. The upload credential (``HF_TOKEN``) stays
  shared.
- Checkpoint directories are owner-scoped the same way (``.checkpoints-{key}``; spot's
  legacy ``.checkpoints``); new sources pass ``scope_staging_to_source=True``. The
  unscoped ``.staging-*`` sweep is legacy and preserved byte-identical, pinned by the
  framework consumer tests.

Mount renders are resumable (S439): each rendered month is checkpointed under the
mirror keyed by its state token, a retry after process death queries only the months
without a verified checkpoint, and the commit links checkpoints into the mirror before
the manifest records the generation. The manifest claims ``delivered_through`` as
certified contiguous coverage, never the newest partition end.
"""

from __future__ import annotations

import json
import logging
import shutil
import time
from collections.abc import Callable, Iterator
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from typing import cast
from uuid import uuid4

import polars as pl
from huggingface_hub import HfApi

from origo.query.binance_spot_kline_rollups import dollar_month, time_month
from origo.steady_state.ownership import assert_execution_owner
from origo.steady_state.publication import (
    CheckpointJournal,
    MonthCheckpoint,
    delivered_coverage,
    file_identity,
    file_sha256,
    write_atomic,
)
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

from ..contracts import Snapshot, SnapshotReader, SourceError, StateRecord, beat_worker
from ..hashing import state_token
from ..storage import SourceStore
from .formulas import huggingface_time as time_snapshot
from .formulas.spot_series import MountKlineSpec, month_path

log = logging.getLogger(__name__)

# A staging directory or partial manifest older than this belongs to a render that died.
ORPHAN_STAGING_MAX_AGE_SECONDS = 3600
# A mount render that must query more months than this does not fit the worker's
# watchdog budget; the worker defers it to the native full-history publisher instead
# of dying mid-render every tick. Months already checkpointed by an interrupted full
# render do not count: finishing them is links, not queries. Steady state touches
# 1-3 months.
MOUNT_WORKER_MONTH_CAP = 4
# source_date margin around a rendered month. Rows are written under their own
# partition day, so a week dwarfs any boundary straddle while the monthly parts
# still prune the scan to ~3 parts.
PRUNE_MARGIN_DAYS = 7

DatasetEntry = tuple[str, str | None, str, str]
"""One datasets-map row: (default_repo_id, repo_id_env, file_prefix, resolution_label)."""


@dataclass(frozen=True)
class ConsumerDeclaration:
    """Per-source consumer parameters; everything else is shared machinery."""

    specs: tuple[MountKlineSpec, ...]
    export_start_date: str
    datasets: dict[str, DatasetEntry]
    scope_staging_to_source: bool
    renderer_label: str
    label_of: Callable[[str], str]
    commit_infix: str
    id_column: str = 'trade_id'
    quote_expr: str = 'quote_quantity'


def _staging_prefix(owner: str | None) -> str:
    return f'.{owner}-staging-' if owner else '.staging-'


def _clear_orphan_staging(
    parquet_root: Path, root: Path, now: float, *, staging_owner: str | None
) -> None:
    """Remove staging directories and partial manifests a hard-killed render left behind.

    Staging directories carry the source key and each consumer sweeps only its own, so a
    concurrent render of another source is never matched; the grace window protects a
    render that is still running. A None owner keeps the legacy unscoped sweep.
    """
    cutoff = now - ORPHAN_STAGING_MAX_AGE_SECONDS
    for orphan in parquet_root.glob(_staging_prefix(staging_owner) + '*'):
        if orphan.is_dir() and orphan.stat().st_mtime < cutoff:
            shutil.rmtree(orphan, ignore_errors=True)
    for orphan in root.glob('.*.partial-*'):
        if orphan.is_file() and orphan.stat().st_mtime < cutoff:
            orphan.unlink(missing_ok=True)


def _root(
    destination: str, reader: SnapshotReader, kind: str, *, renderer_label: str
) -> tuple[SourceStore, Path]:
    if not isinstance(reader, SourceStore):
        raise TypeError(f'The {renderer_label} renderers require their declared SQL store.')
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
    assert_execution_owner()
    if reader.snapshot(canonical_only=True).token != token:
        raise RuntimeError(
            'Canonical state changed while rendering; staged output remains unpublished.'
        )


def _write_manifest(root: Path, manifest: dict[str, object]) -> None:
    assert_execution_owner()
    root.mkdir(parents=True, exist_ok=True)
    write_atomic(
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
            columns = ', '.join(['source_date', *(column.name for column in component.columns)])
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


def month_tokens(source: str, snapshot: Snapshot, *, export_start_date: str) -> dict[str, str]:
    """One state token per exported month, from the pinned records that start in it."""
    grouped: dict[str, list[StateRecord]] = {}
    for record in snapshot.records:
        month = record.partition.start.strftime('%Y-%m')
        if month < export_start_date[:7]:
            continue
        grouped.setdefault(month, []).append(record)
    return {month: state_token(source, tuple(grouped[month])) for month in sorted(grouped)}


def _prune_bounds(year: int, month: int) -> tuple[str, str]:
    """source_date window for one rendered month: the month plus a margin that
    dwarfs any boundary straddle while still pruning to ~3 monthly parts."""
    first = date(year, month, 1)
    last = date(year + 1, 1, 1) if month == 12 else date(year, month + 1, 1)
    margin = timedelta(days=PRUNE_MARGIN_DAYS)
    return (first - margin).isoformat(), (last + margin).isoformat()


def _month_frame(
    series: MountKlineSpec,
    year: int,
    month: int,
    database: str,
    *,
    decl: ConsumerDeclaration,
    base_cut: str | None = None,
    base_day: str | None = None,
) -> pl.DataFrame:
    source_after, source_before = _prune_bounds(year, month)
    if series.family == 'time':
        return time_month(
            interval_minutes=series.size,
            year=year,
            month=month,
            base_table='time',
            latest_table='time_latest',
            database=database,
            source_after=source_after,
            source_before=source_before,
            base_cut=base_cut,
        )
    return dollar_month(
        ratio=series.size,
        year=year,
        month=month,
        base_table='dollar',
        raw_latest_table='raw_latest',
        database=database,
        id_column=decl.id_column,
        quote_expr=decl.quote_expr,
        source_after=source_after,
        source_before=source_before,
        base_day=base_day,
    )


def _reusable(
    entry: dict[str, object] | None, same_token: bool, target: Path
) -> dict[str, object] | None:
    """The previous manifest entry when its month token is unchanged and the mirror file
    still holds the recorded bytes: an unchanged stat identity proves it without reading
    the file; a moved identity is re-hashed once and the entry re-stamped."""
    if entry is None or not same_token or not target.is_file():
        return None
    identity = file_identity(target)
    if entry.get('identity') == identity:
        return entry
    if entry.get('sha256') != file_sha256(target):
        return None
    return {**entry, 'identity': identity}


def months_to_render(
    decl: ConsumerDeclaration,
    tokens: dict[str, str],
    previous_months: dict[str, str],
    previous_files: dict[str, dict[str, object]],
) -> list[str]:
    """Months whose mirror files differ from the previous manifest: token drift,
    a missing month file, or bytes that no longer hash to the recorded digest."""
    changed: list[str] = []
    for month, token in tokens.items():
        year, number = int(month[:4]), int(month[5:7])
        for series in decl.specs:
            target = month_path(series.sub_path, year, number)
            entry = previous_files.get(str(target))
            if _reusable(entry, previous_months.get(month) == token, target) is None:
                changed.append(month)
                break
    return changed


def _entry(path: Path, row_count: int, **extra: object) -> dict[str, object]:
    return {'path': str(path), 'row_count': row_count, 'sha256': file_sha256(path), **extra}


def mount(
    reader: SnapshotReader,
    snapshot: Snapshot,
    destination: str,
    *,
    decl: ConsumerDeclaration,
    allow_full: bool = False,
) -> None:
    """Refresh the Parquet mirror months whose pinned state changed, then the Arrow series.

    Month files and Arrow versions live under the shared mirror roots (``LOCAL_PARQUET_DIR``
    and ``LOCAL_ARROW_DIR``); the manifest under ``destination`` records which state they
    hold. Each rendered month is checkpointed beside the mirror under its month token and
    Arrow versions are written without flipping ``latest``; only a render whose canonical
    state is unchanged links the checkpoints into place and activates the versions, so a
    discarded render leaves the shared roots exactly as the manifest describes them. A
    retry after process death reuses every verified checkpoint and queries only the rest;
    a month whose token changed is invalidated alone. Checkpoints are cleared once the
    manifest records them.

    A render that must query more than ``MOUNT_WORKER_MONTH_CAP`` months does not fit
    the worker's watchdog budget; unless ``allow_full`` (the native full-history
    publisher), it defers with ``RENDER_DEFERRED`` instead of dying mid-render every tick.
    """
    store, root = _root(destination, reader, 'mount', renderer_label=decl.renderer_label)
    if not snapshot.records:
        raise RuntimeError('A consumer cannot publish an empty source state.')
    started = time.monotonic()
    previous = _previous_manifest(root)
    previous_months = cast(dict[str, str], previous.get('month_tokens') or {})
    previous_files = {
        str(cast(dict[str, object], entry)['path']): cast(dict[str, object], entry)
        for entry in cast(list[object], previous.get('files') or [])
    }
    tokens = month_tokens(store.spec.key, snapshot, export_start_date=decl.export_start_date)
    parquet_root = parquet_source_root()
    owner = store.spec.key if decl.scope_staging_to_source else None
    journal = CheckpointJournal(parquet_root, owner)
    invalidated = journal.prune(tokens)
    changed = months_to_render(decl, tokens, previous_months, previous_files)
    pending = [month for month in changed if not journal.covered(decl.specs, month, tokens[month])]
    if len(pending) > MOUNT_WORKER_MONTH_CAP and not allow_full:
        raise SourceError(
            'RENDER_DEFERRED',
            f'Mount render must query {len(pending)} months (cap {MOUNT_WORKER_MONTH_CAP}; '
            f'{len(changed) - len(pending)} already checkpointed); '
            f'native publish_{store.spec.key}_mount_job will resume the durable bulk prerequisite.',
        )
    log.info(
        'mount render source=%s months=%d changed=%d pending=%d invalidated=%d',
        store.spec.key,
        len(tokens),
        len(changed),
        len(pending),
        invalidated,
    )
    changed_set = set(changed)
    state = store.canonical_token(snapshot)
    coverage = delivered_coverage(store.anchor(), snapshot, decl.specs)
    now = time.time()
    _clear_orphan_staging(parquet_root, root, now, staging_owner=owner)
    journal.sweep_partials(now)
    files: list[dict[str, object]] = []
    staged_months: dict[Path, MonthCheckpoint] = {}
    rebuilt: set[str] = set()
    staged_series: list[StagedSeries] = []
    queries = reused = 0
    try:
        with _pinned(store, snapshot) as database:
            # Watermarks are render-constant: resolve once instead of re-scanning
            # the pinned base per month. An empty base falls back to the inline
            # per-query cut with identical semantics.
            cut_rows = store.execute(f'SELECT max(datetime) FROM {database}.time')
            base_cut = str(cut_rows[0][0]) if cut_rows and cut_rows[0][0] is not None else None
            day_rows = store.execute(f'SELECT max(toDate(start_datetime)) FROM {database}.dollar')
            base_day = str(day_rows[0][0]) if day_rows and day_rows[0][0] is not None else None
            for index, (month, token) in enumerate(tokens.items()):
                year, number = int(month[:4]), int(month[5:7])
                if month in changed_set:
                    log.info(
                        'mount render source=%s month=%s progress=%d/%d',
                        store.spec.key,
                        month,
                        index + 1,
                        len(tokens),
                    )
                for series in decl.specs:
                    target = month_path(series.sub_path, year, number)
                    entry = _reusable(
                        previous_files.get(str(target)), previous_months.get(month) == token, target
                    )
                    if entry is not None:
                        files.append(entry)
                        continue
                    checkpoint = journal.find_month(series, month, token)
                    if checkpoint is None:
                        began = time.monotonic()
                        frame = _month_frame(
                            series,
                            year,
                            number,
                            database,
                            decl=decl,
                            base_cut=base_cut,
                            base_day=base_day,
                        )
                        checkpoint = journal.write_month(
                            series, month, token, frame, render_seconds=time.monotonic() - began
                        )
                        queries += 1
                        beat_worker()
                    else:
                        reused += 1
                    if checkpoint.row_count == 0:
                        continue
                    staged_months[target] = checkpoint
                    files.append(
                        {
                            'path': str(target),
                            'row_count': checkpoint.row_count,
                            'sha256': checkpoint.sha256,
                            'series': series.name,
                            'month': month,
                        }
                    )
                    rebuilt.add(series.name)
        for index, series in enumerate(decl.specs):
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
            log.info(
                'mount render source=%s series=%s progress=%d/%d',
                store.spec.key,
                series.name,
                index + 1,
                len(decl.specs),
            )
            months = {path: path for path in series_source_files(series, parquet_root)}
            months.update(
                (target, checkpoint.path)
                for target, checkpoint in staged_months.items()
                if target.is_relative_to(parquet_root / series.sub_path)
            )
            build = build_series_frame(
                series, parquet_root, files=[months[key] for key in sorted(months)]
            )
            staged = stage_series(series.name, build)
            if staged is not None:
                staged_series.append(staged)
        _require_canonical(store, state)
        for target, checkpoint in staged_months.items():
            journal.install(checkpoint, target)
        for staged in staged_series:
            if activate_series(staged).status == 'skipped_not_newer':
                discard_series(staged)
            current = (series_store_dir(staged.series) / LATEST_NAME).resolve()
            files.append(_entry(current, staged.row_count, series=staged.series, kind='arrow'))
    except BaseException:
        for staged in staged_series:
            discard_series(staged)
        raise
    installed = {str(target) for target in staged_months}
    files = [
        {**entry, 'identity': file_identity(Path(str(entry['path'])))}
        if str(entry['path']) in installed
        else entry
        for entry in files
    ]
    manifest: dict[str, object] = {
        'source_key': store.spec.key,
        'state_token': state,
        'pinned_token': snapshot.token,
        'active_through': max(record.partition.end for record in snapshot.records).isoformat(),
        'delivered_through': coverage.delivered_through.isoformat(),
        'coverage': coverage.manifest(),
        'kind': 'mount',
        'month_tokens': tokens,
        'files': files,
        'version': snapshot.token,
        'generation': int(str(previous.get('generation', 0))) + 1,
        'committed_at': datetime.now(UTC).isoformat(),
        'render': {
            'month_queries': queries,
            'checkpoints_reused': reused,
            'checkpoints_invalidated': invalidated,
            'months_changed': len(changed),
            'seconds': round(time.monotonic() - started, 3),
        },
    }
    _write_manifest(root, manifest)
    journal.clear()


def huggingface(
    reader: SnapshotReader,
    snapshot: Snapshot,
    destination: str,
    *,
    decl: ConsumerDeclaration,
    hf_api: type[HfApi],
    time_klines: Callable[..., pl.DataFrame],
    dollar_klines: Callable[..., pl.DataFrame],
    time_card: Callable[..., str],
    dollar_card: Callable[..., str],
    upload: bool = True,
    kind: str = 'huggingface',
) -> None:
    """Render every dataset snapshot from the canonical state and keep a local copy.

    With `upload`, also upload to the public dataset repos; without it (the CANARY
    shadow consumer) the render stays local and the manifest records no uploads.

    The snapshot callables arrive per call (not on the declaration) so the per-source
    wrappers resolve them through their module attributes at call time; that keeps
    the formula-module patch seams working.
    """
    store, root = _root(destination, reader, kind, renderer_label=decl.renderer_label)
    if not snapshot.records:
        raise RuntimeError('A consumer cannot publish an empty source state.')
    end = max(record.partition.end for record in snapshot.records)
    coverage = delivered_coverage(store.anchor(), snapshot, decl.specs)
    export_end_date = (end - timedelta(days=1)).strftime('%Y-%m-%d')
    end_limit = end.strftime('%Y-%m-%d %H:%M:%S')
    build = root / 'versions' / (snapshot.token + '-' + uuid4().hex)
    build.mkdir(parents=True)
    api = hf_api(token=time_snapshot.get_huggingface_token()) if upload else None
    files: list[dict[str, object]] = []
    uploads: list[dict[str, object]] = []
    with _pinned(store, snapshot) as database:
        for series in decl.specs:
            default_repo_id, repo_id_env, file_prefix, resolution_label = decl.datasets[series.name]
            if series.family == 'time':
                frame = time_klines(
                    kline_size_seconds=series.size * 60,
                    start_date_limit=decl.export_start_date,
                    end_date_limit=end_limit,
                    table_name='time',
                    database_name=database,
                )
            else:
                frame = dollar_klines(
                    dollar_size=float(series.size * 1000000),
                    start_date_limit=decl.export_start_date,
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
            digest = file_sha256(parquet)
            label = decl.label_of(series.name)
            if series.family == 'time':
                card = time_card(
                    export_end_date=export_end_date,
                    row_count=frame.height,
                    file_name=file_name,
                    cadence_label=label,
                    resolution_label=resolution_label,
                )
            else:
                card = dollar_card(
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
            files.append(
                {
                    'path': str(parquet.relative_to(build)),
                    'row_count': frame.height,
                    'sha256': digest,
                }
            )
            if upload:
                assert api is not None
                repo_id = time_snapshot.get_huggingface_dataset_repo_id(
                    repo_id_env=repo_id_env, default_repo_id=default_repo_id
                )
                api.create_repo(repo_id=repo_id, repo_type='dataset', exist_ok=True)
                api.upload_folder(
                    folder_path=str(folder),
                    repo_id=repo_id,
                    repo_type='dataset',
                    commit_message=(
                        f'Add BTCUSDT {label} {decl.commit_infix}klines snapshot '
                        f'through {export_end_date}'
                    ),
                    delete_patterns=[f'{file_prefix}*.parquet'],
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
        'delivered_through': coverage.delivered_through.isoformat(),
        'coverage': coverage.manifest(),
        'kind': kind,
        'export_end_date': export_end_date,
        'uploads': uploads,
        'files': files,
        'version': build.name,
    }
    _commit_manifest(store, root, manifest)


def huggingface_shadow(
    reader: SnapshotReader,
    snapshot: Snapshot,
    destination: str,
    *,
    decl: ConsumerDeclaration,
    hf_api: type[HfApi],
    time_klines: Callable[..., pl.DataFrame],
    dollar_klines: Callable[..., pl.DataFrame],
    time_card: Callable[..., str],
    dollar_card: Callable[..., str],
) -> None:
    """Render the snapshot files locally without uploading; the CANARY shadow publication."""
    huggingface(
        reader,
        snapshot,
        destination,
        decl=decl,
        hf_api=hf_api,
        time_klines=time_klines,
        dollar_klines=dollar_klines,
        time_card=time_card,
        dollar_card=dollar_card,
        upload=False,
        kind='huggingface_shadow',
    )
