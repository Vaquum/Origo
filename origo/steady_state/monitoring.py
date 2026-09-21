"""The truthful evaluations behind the monitor worker (PRD-0017 SS-01..SS-04, SS-11).

Pure functions over the authorities the monitor reads: certified coverage metadata
(``origo.steady_state.coverage``), publication manifests and the files they name, the
depth Arrow store, the receipt and container-log tables, and the pinned inventory and
policy. Nothing here writes; the monitor composes the results into findings and keeps
only its cursor.

The rules these functions hold:

- A source's age is the wall clock against its contiguous frontier ``F``, never the newest
  accepted end ``N`` or a manifest write time.
- A consumer's delivered end ``P`` is bounded by the source's verified coverage and by the
  files the manifest actually names; a missing manifest, series or file is a failure.
- Reads of the receipt and log authorities are paged to completion before any cursor
  advances; rows sharing a stamp with a page boundary are re-read, never skipped.
- File content is hashed once per immutable identity within a per-tick byte budget;
  unchanged identities are reused, never rehashed every minute.
"""

from __future__ import annotations

import hashlib
import json
import urllib.parse
import urllib.request
from collections.abc import Callable, Hashable, Mapping, Sequence
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Generic, TypeVar, cast

from origo.sources.contracts import Client, identifier

from .coverage import Coverage
from .policy import Inventory, Policy

T = TypeVar('T')
HASH_BUDGET_BYTES = 64 * 1024 * 1024
PAGE_LIMIT = 1000
MAX_PAGES_PER_TICK = 20
MAX_OPEN_UNITS = 10_000
ARROW_VERSION_HEX = 16
RESOLUTION_QUERY_SETTINGS = {
    'max_memory_usage': 256 * 1024 * 1024,
    'max_execution_time': 5,
    'max_threads': 2,
    'max_rows_to_read': 5_000_000,
    'max_result_rows': 20_000,
    'result_overflow_mode': 'throw',
    'read_overflow_mode': 'throw',
}


def _utc(value: object) -> datetime:
    if not isinstance(value, datetime):
        raise TypeError(f'Expected a datetime, got {type(value).__name__}.')
    return value.replace(tzinfo=UTC) if value.tzinfo is None else value.astimezone(UTC)


def _object(value: object, what: str) -> dict[str, object]:
    if not isinstance(value, dict):
        raise ValueError(f'{what} must be an object.')
    return cast(dict[str, object], value)


# --- bounds -----------------------------------------------------------------------------


@dataclass(frozen=True)
class Bounds:
    """The per-tick breach thresholds the monitor applies, taken from the pinned policy."""

    source_lag_seconds: float
    uncovered_minute_age_seconds: float
    mount_lag_seconds: float
    required_series: int
    depth_close_to_arrow_seconds: float
    consumer_completion_seconds: float
    canonical_delivered_age_seconds: float
    evaluation_seconds: float

    @classmethod
    def from_policy(cls, policy: Policy) -> Bounds:
        return cls(
            policy.bound('SS-01', 'lag_seconds_max').bound,
            policy.bound('SS-01', 'uncovered_due_minute_age_seconds_max').bound,
            policy.bound('SS-02', 'lag_seconds_max').bound,
            int(policy.bound('SS-02', 'required_series_count').bound),
            policy.bound('SS-03', 'close_to_arrow_seconds_max').bound,
            policy.bound('SS-04', 'consumer_completion_minutes_max').bound * 60,
            policy.bound('SS-04', 'delivered_age_hours_max').bound * 3600,
            policy.bound('SS-11', 'evaluation_seconds_max').bound,
        )


def required_workers(inventory: Inventory, *, own: str) -> tuple[str, ...]:
    """Feeds whose heartbeat must exist and be fresh; the monitor does not watch itself."""
    return tuple(
        name.removesuffix('.heartbeat') for name in inventory.workers.values()
        if name.removesuffix('.heartbeat') != own
    )


# --- source age --------------------------------------------------------------------------


@dataclass(frozen=True)
class SourceAge:
    """U, N, C, F and the debt behind F for one source at one instant."""

    source: str
    due: datetime
    newest_end: datetime
    canonical_end: datetime
    contiguous_end: datetime
    missing_minutes: int
    oldest_missing: datetime | None
    incomplete_partitions: int

    @property
    def lag_seconds(self) -> float:
        return max(0.0, (self.due - self.contiguous_end).total_seconds())

    @property
    def newest_lag_seconds(self) -> float:
        return max(0.0, (self.due - self.newest_end).total_seconds())

    @property
    def oldest_gap_age_seconds(self) -> float:
        if self.oldest_missing is None:
            return 0.0
        return max(0.0, (self.due - self.oldest_missing).total_seconds())

    def metadata(self) -> dict[str, object]:
        return {
            'U': self.due.isoformat(),
            'N': self.newest_end.isoformat(),
            'C': self.canonical_end.isoformat(),
            'F': self.contiguous_end.isoformat(),
            'lag_seconds': int(self.lag_seconds),
            'newest_lag_seconds': int(self.newest_lag_seconds),
            'missing_minutes': self.missing_minutes,
            'oldest_gap_age_seconds': int(self.oldest_gap_age_seconds),
            'incomplete_partitions': self.incomplete_partitions,
        }


def source_age(source: str, coverage: Coverage) -> SourceAge:
    return SourceAge(
        source,
        coverage.due,
        coverage.newest_end,
        coverage.canonical_end,
        coverage.contiguous_end,
        coverage.missing_minutes,
        coverage.oldest_missing,
        len(coverage.incomplete_partitions),
    )


def covered_minute(coverage: Coverage, minute: datetime) -> bool:
    """Whether a selected, complete interval certifies the minute starting at ``minute``."""
    end = minute + timedelta(minutes=1)
    return any(item.start <= minute and end <= item.end for item in coverage.intervals)


# --- manifests and file evidence ---------------------------------------------------------


@dataclass(frozen=True)
class FileEntry:
    path: str
    sha256: str
    row_count: int
    series: str
    month: str | None
    arrow: bool


@dataclass(frozen=True)
class Upload:
    series: str
    repo_id: str
    file_name: str
    sha256: str


@dataclass(frozen=True)
class Manifest:
    kind: str
    active_through: datetime
    version: str
    state_token: str
    files: tuple[FileEntry, ...]
    month_tokens: dict[str, str]
    uploads: tuple[Upload, ...]

    @property
    def series(self) -> frozenset[str]:
        return frozenset(entry.series for entry in self.files)


def parse_manifest(text: str) -> Manifest:
    """A consumer manifest or a ``ValueError`` naming what it lacks; nothing is defaulted."""
    data = _object(json.loads(text), 'Publication manifest')
    kind = data.get('kind')
    version = data.get('version')
    token = data.get('state_token')
    through = data.get('active_through')
    if not isinstance(kind, str) or not isinstance(version, str) or not isinstance(token, str):
        raise ValueError('Publication manifest lacks kind/version/state_token evidence.')
    if not isinstance(through, str):
        raise ValueError('Publication manifest lacks active_through.')
    raw_files = data.get('files')
    if not isinstance(raw_files, list):
        raise ValueError('Publication manifest lacks file evidence.')
    files: list[FileEntry] = []
    for item in cast(list[object], raw_files):
        entry = _object(item, 'Publication file evidence')
        path = entry.get('path')
        digest = entry.get('sha256')
        rows = entry.get('row_count')
        if not isinstance(path, str) or not isinstance(digest, str) or not path or not digest:
            raise ValueError('Publication file evidence requires a path and a sha256.')
        if isinstance(rows, bool) or not isinstance(rows, int):
            raise ValueError('Publication file evidence requires an integer row_count.')
        series = entry.get('series')
        month = entry.get('month')
        files.append(
            FileEntry(
                path,
                digest,
                rows,
                series if isinstance(series, str) else Path(path).parts[0],
                month if isinstance(month, str) else None,
                entry.get('kind') == 'arrow',
            )
        )
    months: dict[str, str] = {}
    for month, month_token in _object(data.get('month_tokens') or {}, 'month_tokens').items():
        if not isinstance(month_token, str):
            raise ValueError('Publication month tokens must be strings.')
        months[month] = month_token
    uploads: list[Upload] = []
    for item in cast(list[object], data.get('uploads') or []):
        upload = _object(item, 'Publication upload evidence')
        series = upload.get('series')
        repo_id = upload.get('repo_id')
        file_name = upload.get('file_name')
        digest = upload.get('sha256')
        if not (
            isinstance(series, str)
            and isinstance(repo_id, str)
            and isinstance(file_name, str)
            and isinstance(digest, str)
            and series
            and repo_id
            and file_name
            and digest
        ):
            raise ValueError('Publication upload evidence requires series/repo/file/sha256.')
        uploads.append(Upload(series, repo_id, file_name, digest))
    return Manifest(
        kind, _utc(datetime.fromisoformat(through)), version, token, tuple(files), months, tuple(uploads)
    )


@dataclass
class FileEvidence:
    """What one tick could establish about a manifest's files within the hashing budget."""

    missing: list[str] = field(default_factory=list[str])
    mismatched: list[str] = field(default_factory=list[str])
    reused: int = 0
    hashed: int = 0
    deferred: int = 0

    @property
    def invalid(self) -> bool:
        return bool(self.missing or self.mismatched)

    def metadata(self) -> dict[str, object]:
        return {
            'files_missing': len(self.missing),
            'files_mismatched': len(self.mismatched),
            'files_reused': self.reused,
            'files_hashed': self.hashed,
            'files_deferred': self.deferred,
        }


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with open(path, 'rb') as handle:
        for chunk in iter(lambda: handle.read(1 << 20), b''):
            digest.update(chunk)
    return digest.hexdigest()


def _verified_state(state: dict[str, object]) -> tuple[dict[str, str], dict[str, str]]:
    months = {
        str(month): str(token)
        for month, token in _object(state.get('months') or {}, 'verified months').items()
    }
    paths = {
        str(path): str(digest)
        for path, digest in _object(state.get('paths') or {}, 'verified paths').items()
    }
    return months, paths


def verify_files(
    manifest: Manifest,
    base: Path,
    state: dict[str, object],
    *,
    budget_bytes: int = HASH_BUDGET_BYTES,
) -> FileEvidence:
    """Every named file must exist and be non-empty. Content is hashed once per immutable
    identity: a month whose token is unchanged since its files last hashed clean, a path
    whose sha256 is unchanged, an Arrow version whose name carries its content hash.
    Hashing stops at the byte budget; deferred files are counted, not assumed.

    ``state`` is the consumer's slot in the monitor cursor and is updated in place.
    """
    months_ok, paths_ok = _verified_state(state)
    evidence = FileEvidence()
    pending: list[tuple[int, FileEntry, Path]] = []
    month_entries: dict[str, int] = {}
    for entry in manifest.files:
        location = Path(entry.path)
        if not location.is_absolute():
            location = base / 'versions' / manifest.version / entry.path
        try:
            size = location.stat().st_size if location.is_file() else -1
        except OSError:
            size = -1
        if size <= 0:
            evidence.missing.append(entry.path)
            continue
        if entry.arrow:
            expected = f'{entry.series}.{entry.sha256[:ARROW_VERSION_HEX]}.arrow'
            if location.name == expected:
                evidence.reused += 1
            else:
                evidence.mismatched.append(entry.path)
            continue
        if entry.month is not None:
            month_entries[entry.month] = month_entries.get(entry.month, 0) + 1
            if months_ok.get(entry.month) == manifest.month_tokens.get(entry.month):
                evidence.reused += 1
                continue
        elif paths_ok.get(entry.path) == entry.sha256:
            evidence.reused += 1
            continue
        pending.append((size, entry, location))
    pending.sort(key=lambda item: item[0])
    used = 0
    clean_by_month: dict[str, int] = {}
    for size, entry, location in pending:
        if used + size > budget_bytes:
            evidence.deferred += 1
            continue
        used += size
        if _sha256_file(location) != entry.sha256:
            evidence.mismatched.append(entry.path)
            continue
        evidence.hashed += 1
        if entry.month is not None:
            clean_by_month[entry.month] = clean_by_month.get(entry.month, 0) + 1
        else:
            paths_ok[entry.path] = entry.sha256
    for month, clean in clean_by_month.items():
        if clean == month_entries[month] and month in manifest.month_tokens:
            months_ok[month] = manifest.month_tokens[month]
    state['months'] = {
        month: token for month, token in months_ok.items() if manifest.month_tokens.get(month) == token
    }
    listed = {entry.path: entry.sha256 for entry in manifest.files}
    state['paths'] = {path: digest for path, digest in paths_ok.items() if listed.get(path) == digest}
    return evidence


def delivered_end(manifest: Manifest, age: SourceAge, *, canonical_only: bool) -> datetime:
    """P: what the manifest claims, capped by the coverage the source has actually
    certified. A manifest cannot deliver more than its source proves."""
    frontier = age.canonical_end if canonical_only else age.contiguous_end
    return min(manifest.active_through, frontier)


RemoteVerifier = Callable[[str, str], str | None]
HUB_ENDPOINT = 'https://huggingface.co'
HUB_TIMEOUT_SECONDS = 10


def remote_file_sha256(repo_id: str, file_name: str) -> str | None:
    """The sha256 the Hub records for an LFS file, or ``None`` when the path is absent.
    One bounded metadata request (the ``paths-info`` endpoint ``huggingface_hub`` itself
    uses); the file's bytes are never downloaded and no credential is sent."""
    request = urllib.request.Request(
        f'{HUB_ENDPOINT}/api/datasets/{repo_id}/paths-info/main',
        data=urllib.parse.urlencode({'paths': file_name, 'expand': 'false'}).encode(),
        method='POST',
        headers={'Accept': 'application/json'},
    )
    with urllib.request.urlopen(request, timeout=HUB_TIMEOUT_SECONDS) as response:
        listed: object = json.loads(response.read())
    if not isinstance(listed, list):
        raise ValueError(f'{repo_id}: the Hub did not list paths.')
    for item in cast(list[object], listed):
        entry = _object(item, f'{repo_id} path entry')
        if entry.get('type') != 'file' or entry.get('path') != file_name:
            continue
        lfs = entry.get('lfs')
        oid = _object(lfs, f'{repo_id}/{file_name} lfs').get('oid') if lfs is not None else None
        return oid if isinstance(oid, str) else None
    return None


def verify_uploads(
    manifest: Manifest,
    required_series: frozenset[str],
    state: dict[str, object],
    *,
    verifier: RemoteVerifier,
    now: datetime,
    retry_after_seconds: float = 3600.0,
) -> tuple[str, ...]:
    """What the remote publication fails to prove, once per manifest version.

    The remote is consulted when the version changes and again after ``retry_after`` when
    the last attempt could not verify; a verified version is not re-asked every minute.
    Returns problems as text; an empty tuple means every required series is present on
    the Hub with the manifest's sha256.
    """
    uploaded = {upload.series: upload for upload in manifest.uploads}
    problems = [f'no upload recorded for {series}' for series in sorted(required_series - set(uploaded))]
    if problems:
        return tuple(problems)
    if state.get('version') == manifest.version and state.get('verified') is True:
        return ()
    checked = state.get('checked_at')
    if (
        state.get('version') == manifest.version
        and isinstance(checked, (int, float))
        and now.timestamp() - float(checked) < retry_after_seconds
    ):
        return tuple(str(item) for item in cast(list[object], state.get('problems') or []))
    for series in sorted(required_series):
        upload = uploaded[series]
        try:
            remote = verifier(upload.repo_id, upload.file_name)
        except Exception as error:
            problems.append(f'{upload.repo_id}/{upload.file_name}: {type(error).__name__}: {error}'[:200])
            continue
        if remote is None:
            problems.append(f'{upload.repo_id}/{upload.file_name}: absent on the Hub')
        elif remote != upload.sha256:
            problems.append(f'{upload.repo_id}/{upload.file_name}: Hub sha256 differs')
    state['version'] = manifest.version
    state['checked_at'] = now.timestamp()
    state['verified'] = not problems
    state['problems'] = problems
    return tuple(problems)


# --- depth Arrow store -------------------------------------------------------------------


@dataclass(frozen=True)
class DepthState:
    series: str
    manifest_present: bool
    latest_minute: datetime | None
    latest_chunk_present: bool
    missing_minutes: tuple[datetime, ...]

    def lag_seconds(self, due: datetime) -> float | None:
        if self.latest_minute is None:
            return None
        return max(0.0, (due - (self.latest_minute + timedelta(minutes=1))).total_seconds())


def depth_state(
    root: Path,
    series: str,
    *,
    manifest_name: str,
    chunk_pattern: str,
    retention_minutes: int,
    due: datetime,
    close_to_arrow_seconds: float,
) -> DepthState:
    """The committed Arrow evidence of one depth path: its latest manifest minute, that
    minute's chunk, and every closed minute inside retention that has no chunk yet
    although its deadline has passed."""
    directory = root / series
    manifest = directory / manifest_name
    if not manifest.is_file():
        return DepthState(series, False, None, False, ())
    data = _object(json.loads(manifest.read_text(encoding='utf-8')), f'{manifest}')
    key = data.get('source_partition_key')
    if not isinstance(key, str):
        raise ValueError(f'{manifest} does not name source_partition_key.')
    latest = _utc(datetime.fromisoformat(key)).replace(second=0, microsecond=0)
    latest_chunk = (directory / latest.strftime(chunk_pattern)).is_file()
    # A minute closing at T must be committed by T + bound; the oldest minutes near the
    # retention cutoff are pruned by the store itself and are not demanded.
    newest_due = due - timedelta(minutes=1) - timedelta(seconds=close_to_arrow_seconds)
    newest_due = newest_due.replace(second=0, microsecond=0)
    oldest_due = due - timedelta(minutes=retention_minutes - 2)
    missing: list[datetime] = []
    minute = oldest_due
    while minute <= newest_due:
        if not (directory / minute.strftime(chunk_pattern)).is_file():
            missing.append(minute)
        minute += timedelta(minutes=1)
    return DepthState(series, True, latest, latest_chunk, tuple(missing))


# --- paged reads -------------------------------------------------------------------------


class PaginationOverflow(RuntimeError):
    """More rows share one second than a page holds; the read cannot be made complete."""


@dataclass(frozen=True)
class PagedRead(Generic[T]):
    rows: tuple[T, ...]
    read_through: datetime
    complete: bool


def read_pages(
    reader: Callable[[datetime, datetime], list[T]],
    stamp: Callable[[T], datetime],
    identity: Callable[[T], Hashable],
    since: datetime,
    until: datetime,
    *,
    limit: int = PAGE_LIMIT,
    max_pages: int = MAX_PAGES_PER_TICK,
) -> PagedRead[T]:
    """Read ``(since, until]`` completely through a reader bounded to ``limit`` rows.

    The driver binds bounds at one-second precision, so a full page is closed at the
    whole second of its final stamp and the rows of that second are read again on their
    own: the fractional rows after the second when the final stamp has a fraction, the
    rows on the exact second when it has none (only those can straddle the page). Rows are
    de-duplicated by identity, so re-reading a boundary never repeats a fact and never
    loses one; a second holding a whole page or more cannot be completed and says so.
    ``read_through`` is the stamp every row up to which has been read; when the page
    budget runs out it stays behind so the next tick resumes there.
    """
    found: dict[Hashable, T] = {}
    cursor = since
    pages = 0
    while True:
        page = reader(cursor, until)
        pages += 1
        for row in page:
            found.setdefault(identity(row), row)
        if len(page) < limit:
            return PagedRead(tuple(found.values()), until, True)
        last = stamp(page[-1])
        second = last.replace(microsecond=0)
        if last == second:
            group = reader(second - timedelta(seconds=1), second)
            wanted = [row for row in group if stamp(row) == second]
        else:
            group = reader(second, second + timedelta(seconds=1))
            wanted = [
                row for row in group
                if second < stamp(row) < second + timedelta(seconds=1) and stamp(row) <= until
            ]
        if len(group) >= limit:
            raise PaginationOverflow(
                f'{limit} or more rows are stamped within {second.isoformat()}; '
                'the read cannot be completed at this page size.'
            )
        for row in wanted:
            found.setdefault(identity(row), row)
        cursor = second
        if pages >= max_pages:
            return PagedRead(tuple(found.values()), second, False)


# --- durable receipt findings ------------------------------------------------------------


@dataclass(frozen=True)
class OpenUnit:
    """A failed worker unit that no later evidence has resolved."""

    feed: str
    series: str
    minute: datetime
    recorded_at: datetime
    error_code: str
    error: str

    @property
    def key(self) -> str:
        return f'{self.feed}|{self.series}|{self.minute.isoformat()}'

    @property
    def publication(self) -> bool:
        return ':' in self.series

    def to_json(self) -> dict[str, str]:
        return {
            'recorded_at': self.recorded_at.isoformat(),
            'error_code': self.error_code,
            'error': self.error[:300],
        }

    @classmethod
    def from_json(cls, key: str, data: Mapping[str, object]) -> OpenUnit:
        feed, series, minute = key.split('|', 2)
        return cls(
            feed,
            series,
            _utc(datetime.fromisoformat(minute)),
            _utc(datetime.fromisoformat(str(data['recorded_at']))),
            str(data.get('error_code', '')),
            str(data.get('error', '')),
        )


def resolved_units(
    client: Client,
    database: str,
    units: Sequence[OpenUnit],
    covered: Callable[[OpenUnit], bool],
) -> set[str]:
    """Keys of the units a later fact resolves: an OK receipt for the same minute unit, a
    later OK publication of the same series, or certified coverage of the minute."""
    resolved = {unit.key for unit in units if not unit.publication and covered(unit)}
    table = f'{identifier(database)}.worker_minute_log'
    minute_units = [unit for unit in units if not unit.publication and unit.key not in resolved]
    if minute_units:
        rows = client.execute(
            f"""SELECT feed, series, minute, max(recorded_at) FROM {table}
            WHERE status = 'OK' AND (feed, series, minute) IN %(units)s
            GROUP BY feed, series, minute""",
            {
                'units': [
                    (unit.feed, unit.series, unit.minute.replace(tzinfo=None)) for unit in minute_units
                ]
            },
            settings=RESOLUTION_QUERY_SETTINGS,
        )
        latest_ok = {
            (str(row[0]), str(row[1]), _utc(row[2])): _utc(row[3]) for row in rows
        }
        for unit in minute_units:
            ok_at = latest_ok.get((unit.feed, unit.series, unit.minute))
            if ok_at is not None and ok_at > unit.recorded_at:
                resolved.add(unit.key)
    publication_units = [unit for unit in units if unit.publication]
    if publication_units:
        rows = client.execute(
            f"""SELECT feed, series, max(recorded_at) FROM {table}
            WHERE status = 'OK' AND (feed, series) IN %(pairs)s AND recorded_at > %(since)s
            GROUP BY feed, series""",
            {
                'pairs': sorted({(unit.feed, unit.series) for unit in publication_units}),
                'since': min(unit.recorded_at for unit in publication_units).replace(tzinfo=None),
            },
            settings=RESOLUTION_QUERY_SETTINGS,
        )
        latest_series_ok = {(str(row[0]), str(row[1])): _utc(row[2]) for row in rows}
        for unit in publication_units:
            ok_at = latest_series_ok.get((unit.feed, unit.series))
            if ok_at is not None and ok_at > unit.recorded_at:
                resolved.add(unit.key)
    return resolved
