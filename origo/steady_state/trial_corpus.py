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
