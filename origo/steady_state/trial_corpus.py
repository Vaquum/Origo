"""Authentic archive inputs for isolated, paced runtime trials.

The replay serialization is derived from verified official CSV rows. It is explicitly
not claimed to be an HTTP capture. Original IDs, timestamps, decimal text and owned
flags remain unchanged except the existing spot REST microsecond-to-millisecond rule.
Ancillary fields absent from the archive are omitted, never fabricated.
"""

from __future__ import annotations

import hashlib
import zipfile
from dataclasses import dataclass
from datetime import UTC, date, datetime
from pathlib import Path

import requests

from origo.sources.adapters.binance_archive import BinanceArchiveDaily
from origo.sources.registry import SOURCE_REGISTRY

MAX_ARCHIVE_BYTES = 256 * 1024 * 1024
MAX_CSV_BYTES = 1536 * 1024 * 1024
SOURCES = (
    'binance_spot_trades',
    'binance_perp_trades',
    'binance_spot_aggtrades',
    'binance_perp_aggtrades',
)


@dataclass(frozen=True)
class ArchiveReference:
    source_key: str
    day: date
    path: Path
    sha256: str
    url: str
    checksum_observed_at: datetime

    def document(self) -> dict[str, object]:
        return {
            'source_key': self.source_key,
            'day': self.day.isoformat(),
            'path': str(self.path),
            'sha256': self.sha256,
            'url': self.url,
            'checksum_observed_at': self.checksum_observed_at.isoformat(),
        }


def _hash(path: Path) -> str:
    with path.open('rb') as stream:
        return hashlib.file_digest(stream, 'sha256').hexdigest()


def archive_url(source: str, day: date) -> str:
    if source not in SOURCES:
        raise ValueError('A trial cannot add or substitute a source identity.')
    market = 'spot' if '_spot_' in source else 'futures/um'
    kind = 'aggTrades' if source.endswith('aggtrades') else 'trades'
    return (
        f'https://data.binance.vision/data/{market}/daily/{kind}/BTCUSDT/'
        f'BTCUSDT-{kind}-{day.isoformat()}.zip'
    )


def acquire_archive(source: str, day: date, cache: Path) -> ArchiveReference:
    url = archive_url(source, day)
    checksum = requests.get(url + '.CHECKSUM', timeout=(5, 30))
    checksum.raise_for_status()
    if len(checksum.content) > 4096:
        raise ValueError('The official checksum response exceeded its bound.')
    fields = checksum.text.strip().split()
    name = url.rsplit('/', 1)[-1]
    if len(fields) != 2 or fields[1].lstrip('*') != name or len(fields[0]) != 64:
        raise ValueError('The checksum does not describe the requested archive.')
    int(fields[0], 16)
    digest = fields[0].lower()
    observed = datetime.now(UTC)
    target = cache / source / name
    target.parent.mkdir(parents=True, exist_ok=True)
    if not target.is_file():
        pending = target.with_suffix('.pending')
        with requests.get(url, timeout=(5, 30), stream=True) as response:
            response.raise_for_status()
            size = 0
            with pending.open('wb') as output:
                for block in response.iter_content(1024 * 1024):
                    size += len(block)
                    if size > MAX_ARCHIVE_BYTES:
                        raise ValueError('The archive download exceeded its byte bound.')
                    output.write(block)
        if _hash(pending) != digest:
            raise ValueError('Downloaded archive differs from the official checksum.')
        pending.replace(target)
    if _hash(target) != digest:
        raise ValueError('Cached archive differs from the pinned official input.')
    return ArchiveReference(source, day, target, digest, url, observed)


def archive_csv(reference: ArchiveReference) -> bytes:
    if _hash(reference.path) != reference.sha256:
        raise ValueError('The archived trial input was changed after verification.')
    with zipfile.ZipFile(reference.path) as archive:
        members = archive.infolist()
        expected = reference.path.name.removesuffix('.zip') + '.csv'
        if len(members) != 1 or members[0].filename != expected:
            raise ValueError('The official archive has an unexpected member inventory.')
        if members[0].file_size > MAX_CSV_BYTES:
            raise ValueError('The uncompressed archive exceeds the declared input bound.')
        return archive.read(members[0])


def canonical_rows(reference: ArchiveReference) -> tuple[bytes, dict[str, int]]:
    spec = next(spec for spec in SOURCE_REGISTRY if spec.key == reference.source_key)
    adapter = spec.canonical
    if not isinstance(adapter, BinanceArchiveDaily):
        raise TypeError('The trial requires the registered native archive adapter.')
    partition = adapter.partition(reference.day.isoformat())
    # The source's own archive cleaning rules, not a benchmark-specific row filter.
    cleaned, dropped = adapter.clean_rows(archive_csv(reference), partition)
    return cleaned, dropped


def prepare_tape(reference: ArchiveReference, target: Path) -> dict[str, object]:
    import io

    import polars as pl

    cleaned, dropped = canonical_rows(reference)
    aggregate = reference.source_key.endswith('aggtrades')
    spot = '_spot_' in reference.source_key
    columns = (
        ['a', 'p', 'q', 'f', 'l', 'T', 'm']
        if aggregate
        else ['id', 'price', 'qty', 'quoteQty', 'time', 'isBuyerMaker']
    )
    if spot:
        columns.append('M' if aggregate else 'isBestMatch')
    first = cleaned.split(b'\n', 1)[0].split(b',', 1)[0]
    frame = pl.read_csv(
        io.BytesIO(cleaned), has_header=not first.isdigit(), new_columns=columns, infer_schema=False
    )
    if frame.width != len(columns):
        raise ValueError('Archive tape columns differ from the source contract.')
    identity, timestamp = ('a', 'T') if aggregate else ('id', 'time')
    integer_columns = [identity, timestamp] + (['f', 'l'] if aggregate else [])
    frame = frame.with_columns(pl.col(name).cast(pl.Int64) for name in integer_columns)
    if spot:
        frame = frame.with_columns(
            pl.when(pl.col(timestamp) >= 10**15)
            .then(pl.col(timestamp) // 1000)
            .otherwise(pl.col(timestamp))
            .alias(timestamp)
        )
    booleans = ['m' if aggregate else 'isBuyerMaker']
    if spot:
        booleans.append('M' if aggregate else 'isBestMatch')
    for name in booleans:
        values = frame.get_column(name).str.to_lowercase()
        if not values.is_in(['true', 'false']).all():
            raise ValueError('An archive flag cannot be normalized without inventing a value.')
        frame = frame.with_columns((pl.col(name).str.to_lowercase() == 'true').alias(name))
    if not frame.height or frame.null_count().select(pl.sum_horizontal(pl.all())).item():
        raise ValueError('The trial input must contain complete, non-null native rows.')
    if (frame.get_column(identity).diff().drop_nulls() <= 0).any():
        raise ValueError('The trial archive IDs are duplicated or unordered.')
    if (frame.get_column(timestamp).diff().drop_nulls() < 0).any():
        raise ValueError('The trial archive timestamps regress.')
    start = int(datetime.combine(reference.day, datetime.min.time(), UTC).timestamp() * 1000)
    if frame.filter((pl.col(timestamp) < start) | (pl.col(timestamp) >= start + 86_400_000)).height:
        raise ValueError('The archive contains rows outside its declared day.')
    target.parent.mkdir(parents=True, exist_ok=True)
    frame.write_ipc(target, compression='zstd')
    return {
        **reference.document(),
        'kind': 'derived_official_archive_replay',
        'tape_path': str(target),
        'tape_sha256': _hash(target),
        'rows': frame.height,
        'columns': list(frame.columns),
        'dropped': dropped,
        'timestamp_rule': 'existing_spot_rest_milliseconds' if spot else 'unchanged_milliseconds',
        'http_capture': False,
        'rpi_label': 'not_present_in_official_archive',
    }
