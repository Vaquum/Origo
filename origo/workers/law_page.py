"""Read-only public view of the monitor's observation tape."""
from __future__ import annotations

import argparse
import gzip
import hashlib
import json
import logging
import math
import re
import secrets
import socket
import struct
import sys
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
from collections.abc import Callable, Sequence
from datetime import UTC, datetime, timedelta
from http.server import BaseHTTPRequestHandler, HTTPServer
from pathlib import Path
from socketserver import ThreadingMixIn
from typing import TypeAlias, cast

Json: TypeAlias = None | bool | int | float | str | list['Json'] | dict[str, 'Json']
Document: TypeAlias = dict[str, Json]
ROOT = Path('/var/lib/origo-law')
MAX_RECORD = 1024 * 1024
READ_BUDGET = 4 * MAX_RECORD
PAGE_SIZE = 1000
REQUEST_SLOTS = 4
REQUEST_TIMEOUT = 2.0
MAX_SAMPLE_BYTES = 96 * MAX_RECORD
MAX_CATALOG_BYTES = 16 * MAX_RECORD
MAX_EVENT_IDENTITIES = 3_000_000
MAX_GATE_DAYS = 20_000
OWN_GATE = 'monitor.data_current'
CORE_SOURCES = (
    'binance_spot_trades', 'binance_spot_aggtrades', 'binance_perp_trades',
    'binance_perp_aggtrades', 'binance_spot_depth20_1m', 'binance_spot_depth200_1m',
)
log = logging.getLogger(__name__)


def _object(value: Json) -> Document:
    return value if isinstance(value, dict) else {}


def _objects(value: Json) -> list[Document]:
    return [_object(item) for item in value if isinstance(item, dict)] if isinstance(value, list) else []


def _instant(value: Json) -> datetime:
    if not isinstance(value, str) or len(value) > 40:
        raise ValueError('invalid_time')
    stamp = datetime.fromisoformat(value.replace('Z', '+00:00'))
    if stamp.tzinfo is None or stamp.utcoffset() != timedelta(0):
        raise ValueError('invalid_time')
    return stamp.astimezone(UTC)


def _reject_number(value: str) -> Json:
    raise ValueError(f'invalid_number:{value}')


def _decode(raw: bytes) -> Document:
    value = cast(Json, json.loads(raw, parse_constant=_reject_number))
    if not isinstance(value, dict):
        raise ValueError('invalid_record')
    return value


def _number(value: Json) -> float | None:
    return float(value) if isinstance(value, (int, float)) and not isinstance(value, bool) and math.isfinite(value) else None


def _identity(event: Document) -> tuple[str, str, str]:
    gate, version, evidence = (event.get(key) for key in ('gate_id', 'definition_version', 'evidence_id'))
    return gate if isinstance(gate, str) else '', version if isinstance(version, str) else '', evidence if isinstance(evidence, str) else ''


def _valid_report(report: Document) -> bool:
    try:
        start, end, slot = (_instant(report.get(key)) for key in ('evaluation_start', 'evaluation_end', 'sampling_slot'))
        feeds = {str(feed.get('source_key')): feed for feed in _objects(report.get('feeds'))}
        states: list[str] = []
        if report.get('schema_version') != 1 or end < start or slot != start.replace(second=0, microsecond=0):
            return False
        inventory = report.get('inventory')
        if (not isinstance(inventory, list) or not all(isinstance(key, str) for key in inventory)
                or len(inventory) != len(set(str(key) for key in inventory))
                or not set(CORE_SOURCES) <= set(str(key) for key in inventory)
                or set(feeds) != set(inventory) or len(feeds) != len(_objects(report.get('feeds')))):
            return False
        for source in feeds:
            predicates = _object(feeds[source].get('predicates'))
            required = ('D1',) if source.endswith('_1m') else ('R1', 'C1', 'C2')
            if not predicates or (source in CORE_SOURCES and not set(required) <= set(predicates)):
                return False
            for key in predicates:
                state = str(_object(predicates.get(key)).get('status'))
                if state not in ('PASS', 'FAIL', 'UNKNOWN', 'NOT_DUE') or (state == 'NOT_DUE' and key != 'C1'):
                    return False
                states.append(state)
        expected = 'FAIL' if 'FAIL' in states else 'UNKNOWN' if 'UNKNOWN' in states else 'PASS'
        return report.get('status') == expected
    except (ValueError, TypeError):
        return False


def _sample_event(record: Document) -> Document:
    events = [event for event in _objects(record.get('gates')) if event.get('gate_id') == OWN_GATE]
    if (not _valid_report(record) or len(events) != 1 or not all(_identity(events[0]))
            or _instant(events[0].get('evaluated_at')).date() != _instant(record.get('sampling_slot')).date()):
        raise ValueError('invalid_sample_event')
    return events[0]


def _weight(value: Json) -> int:
    if isinstance(value, dict):
        return sys.getsizeof(value) + sum(sys.getsizeof(key) + _weight(item) for key, item in value.items())
    if isinstance(value, list):
        return sys.getsizeof(value) + sum(_weight(item) for item in value)
    return sys.getsizeof(value)


def _packed_position(packed: bytearray, value: bytes) -> int:
    lo, hi = 0, len(packed) // 16
    while lo < hi:
        middle = (lo + hi) // 2
        if packed[middle * 16:(middle + 1) * 16] < value:
            lo = middle + 1
        else:
            hi = middle
    return lo * 16


def _micros(stamp: datetime) -> int:
    return (stamp - datetime(1970, 1, 1, tzinfo=UTC)) // timedelta(microseconds=1)


def _sample_brief(report: Document) -> Document:
    feeds = _objects(report.get('feeds'))
    core = sorted((gate, version) for gate, version, _ in map(_identity, _objects(report.get('gates'))) if gate.startswith('law.'))
    expected = {f"law.{name}:{feed.get('source_key')}" for feed in feeds for name in _object(feed.get('predicates'))} | {'law.inventory'}
    complete = {key for key, version in core if version} == expected and len(core) == len(expected)
    return {
        'slot': report.get('sampling_slot'), 'start': report.get('evaluation_start'),
        'status': report.get('status'), 'version': report.get('schema_version'),
        'policy': hashlib.sha256(json.dumps([core, report.get('schema_version'), report.get('inventory'), [(feed.get('source_key'), _object(_object(_object(feed.get('predicates')).get('R1')).get('evidence')).get('budget_seconds'), _object(_object(_object(feed.get('predicates')).get('C2')).get('evidence')).get('anchor'), str(_object(_object(_object(feed.get('predicates')).get('C1')).get('evidence')).get('deadline'))[11:16], _object(_object(_object(feed.get('predicates')).get('D1')).get('evidence')).get('expected_slots'), _object(_object(_object(feed.get('predicates')).get('D1')).get('evidence')).get('max_missing')) for feed in feeds]], sort_keys=True).encode()).hexdigest() if complete else None,
        'ages': {str(feed.get('source_key')): _object(_object(_object(feed.get('predicates')).get('R1')).get('evidence')).get('age_seconds') for feed in feeds},
        'not_due': any(_object(_object(feed.get('predicates')).get('C1')).get('status') == 'NOT_DUE' for feed in feeds),
    }


def consecutive_window(samples: list[Document], terminal: str) -> list[Document]:
    clear: list[Document] = []
    for sample in reversed(sorted(samples, key=lambda item: str(item.get('slot')))):
        if sample.get('status') != 'PASS' or not sample.get('policy'):
            break
        if clear and (_instant(clear[-1].get('slot')) - _instant(sample.get('slot')) != timedelta(minutes=1)
                      or sample.get('policy') != clear[-1].get('policy')):
            break
        clear.append(sample)
    return clear if clear and clear[0].get('slot') == terminal else []


class TapeCache:
    def __init__(self, root: Path = ROOT) -> None:
        self.root = root
        self.lock = threading.RLock()
        self.latest: Document | None = None
        self.latest_error = 'missing_report'
        self.catalogs: dict[str, Document] = {}
        self.catalog_versions: dict[str, str] = {}
        self.catalog_files: dict[str, str] = {}
        self.definition_bytes = 0
        self.definitions: dict[str, dict[str, Document]] = {}
        self.sample_bytes = self.catalog_bytes = self.event_count = 0
        self.history_limited = False
        self.discarding: set[str] = set()
        self.last_events: dict[str, Document] = {}
        self.active_definitions: dict[str, str] = {}
        self.positions: dict[str, int] = {}
        self.samples: dict[str, Document] = {}
        self.days: dict[tuple[str, str], Document] = {}
        self.seen: dict[tuple[str, str], bytearray] = {}
        self.index: dict[tuple[str, str], bytearray] = {}
        self.cursors: dict[str, tuple[str, str, int, int]] = {}
        self.observed: dict[tuple[str, str], int] = {}
        self.loading = True
        self.bytes_read = 0
        self._latest_stamp: tuple[str, int, int] | None = None
        self._last_catalog_scan = 0.0

    def _paths(self, now: datetime) -> list[Path]:
        floor = (now - timedelta(days=30)).date().isoformat()
        return sorted(path for pattern in ('samples-????-??-??.jsonl', 'gate-events-????-??-??.jsonl')
                      for path in self.root.glob(pattern) if path.name[-16:-6] >= floor and not path.is_symlink())

    def _load_catalog(self, version: str, *, current: bool = False) -> None:
        if version in self.catalogs or (version in self.catalog_versions and not current) or not re.fullmatch(r'[a-zA-Z0-9_-]{1,128}', version):
            return
        path = self.root / f'catalog-{version}.json'
        try:
            if path.is_symlink() or path.stat().st_size > MAX_RECORD:
                raise ValueError('invalid_catalog')
            record = _decode(path.read_bytes())
            if record.get('schema_version') == 1 and record.get('version') == version:
                for descriptor in _objects(record.get('gates')):
                    gate, definition = str(descriptor.get('id')), str(descriptor.get('definition_version'))
                    if definition not in self.definitions.get(gate, {}):
                        semantic = {**descriptor, 'code': None}
                        weight = _weight(semantic) + 256
                        if self.definition_bytes + weight > MAX_CATALOG_BYTES // 2:
                            self.history_limited = True
                        else:
                            self.definitions.setdefault(gate, {})[definition] = semantic
                            self.definition_bytes += weight
                sha = str(record.get('deployed_sha'))
                if len(self.catalog_versions) < 10_000:
                    self.catalog_versions[version] = sha
                    if self.catalog_files.setdefault(sha, version) != version:
                        self.catalog_files[sha] = ''
                else:
                    self.history_limited = True
                self.catalogs[version] = record
                active = str(self.latest.get('catalog_version')) if self.latest else version
                if active in self.catalogs and active != version:
                    self.catalogs[active] = self.catalogs.pop(active)
                def cache_weight() -> int:
                    return self.definition_bytes + sum(_weight(item) for item in self.catalogs.values()) + sum(sys.getsizeof(key) + sys.getsizeof(value) + 128 for key, value in self.catalog_versions.items())
                while len(self.catalogs) > 2 or (cache_weight() > MAX_CATALOG_BYTES and len(self.catalogs) > 1):
                    del self.catalogs[next(iter(self.catalogs))]
                self.catalog_bytes = cache_weight()
                if self.catalog_bytes > MAX_CATALOG_BYTES:
                    self.history_limited = True
                    self.catalogs.pop(version)
                    self.catalog_bytes = cache_weight()
        except (OSError, ValueError) as error:
            log.warning('Law catalog unreadable: %s', type(error).__name__)

    def _catalogs(self) -> None:
        if time.monotonic() - self._last_catalog_scan < 5:
            return
        self._last_catalog_scan = time.monotonic()
        deadline = time.monotonic() + 0.05
        for path in self.root.glob('catalog-*.json'):
            if time.monotonic() > deadline:
                break
            self._load_catalog(path.stem[8:])

    def refresh_latest(self, now: datetime) -> None:
        with self.lock:
            paths = sorted(self.root.glob('samples-????-??-??.jsonl'))
            if not paths:
                self.latest, self.latest_error = None, 'missing_report'
                return
            path = paths[-1]
            try:
                stat = path.stat()
                stamp = (path.name, stat.st_mtime_ns, stat.st_size)
                if stamp == self._latest_stamp:
                    if self.latest:
                        self._load_catalog(str(self.latest.get('catalog_version')), current=True)
                    return
                self._latest_stamp = stamp
                if path.is_symlink():
                    raise ValueError('invalid_record')
                with path.open('rb') as stream:
                    stream.seek(max(0, stat.st_size - MAX_RECORD))
                    raw = stream.read(MAX_RECORD)
                self.bytes_read += len(raw)
                lines = raw.split(b'\n')[:-1]
                if stat.st_size > MAX_RECORD:
                    lines = lines[1:]
                if not lines:
                    self.latest_error = 'incomplete_report'
                    return
                record = _decode(lines[-1])
                if not _valid_report(record) or _instant(record.get('evaluation_end')) > now + timedelta(seconds=5):
                    raise ValueError('invalid_record')
                self.latest, self.latest_error = record, ''
                self._load_catalog(str(record.get('catalog_version')), current=True)
                self.active_definitions = {str(gate.get('id')): str(gate.get('definition_version')) for gate in _objects(self.catalogs.get(str(record.get('catalog_version')), {}).get('gates'))}
                self.last_events = {key: event for key, event in self.last_events.items() if self.active_definitions.get(key) == event.get('definition_version')}
                for event in _objects(record.get('gates')):
                    self._last_event(event)
            except (OSError, ValueError) as error:
                self.latest_error = 'malformed_report'
                log.warning('Law report unreadable: %s', type(error).__name__)

    def _sample(self, record: Document, offset: int | None = None) -> None:
        slot = str(record.get('sampling_slot', ''))
        if not slot:
            self.history_limited = True
            return
        if slot in self.samples:
            self.history_limited = True
            self.samples[slot]['status'] = 'UNKNOWN'
            return
        valid = _valid_report(record)
        if not valid:
            self.history_limited = True
        brief: Document = _sample_brief(record) if valid else {'slot': slot, 'status': 'UNKNOWN'}
        weight = _weight(brief) + sys.getsizeof(slot) + 128
        if self.sample_bytes + weight > MAX_SAMPLE_BYTES:
            self.history_limited = True
            return
        self.samples[slot] = brief
        self.sample_bytes += weight
        if valid and offset is not None and any(event.get('gate_id') == OWN_GATE for event in _objects(record.get('gates'))):
            self._event(_sample_event(record), offset)

    def _last_event(self, event: Document) -> None:
        gate, version, evidence = _identity(event)
        if evidence and self.active_definitions.get(gate) == version:
            prior = self.last_events.get(gate)
            if prior is None or _instant(event.get('evaluated_at')) > _instant(prior.get('evaluated_at')):
                self.last_events[gate] = event

    def _event(self, event: Document, offset: int | None = None) -> None:
        identity = _identity(event)
        if not all(identity):
            self.history_limited = True
            return
        if identity[0] not in self.definitions or self.event_count >= MAX_EVENT_IDENTITIES:
            self.history_limited = True
            return
        stamp = _instant(event.get('evaluated_at'))
        key = (identity[0], stamp.date().isoformat())
        if key not in self.seen and len(self.seen) >= MAX_GATE_DAYS:
            self.history_limited = True
            return
        digest = hashlib.sha256(json.dumps(identity).encode()).digest()[:16]
        packed = self.seen.setdefault(key, bytearray())
        position = _packed_position(packed, digest)
        if bytes(packed[position:position + 16]) == digest:
            return
        packed[position:position] = digest
        self._last_event(event)
        self.event_count += 1
        if offset is not None:
            indexed = self.index.setdefault(key, bytearray())
            pointer = struct.pack('>QQ', _micros(stamp), offset)
            position = _packed_position(indexed, pointer)
            indexed[position:position] = pointer
        day = self.days.setdefault(key, {
            'day': key[1], 'pass_count': 0, 'fail_count': 0, 'wait_count': 0,
            'unknown_count': 0, 'evaluation_count': 0, 'not_observed': False,
        })
        name = {'PASS': 'pass_count', 'FAIL': 'fail_count', 'EXPECTED_WAIT': 'wait_count'}.get(str(event.get('outcome')), 'unknown_count')
        day[name] = int(str(day[name])) + 1
        day['evaluation_count'] = int(str(day['evaluation_count'])) + 1
        self.observed[key] = self.observed.get(key, 0) | (1 << (stamp.hour * 60 + stamp.minute))

    def advance(self, now: datetime, *, budget_seconds: float = 0.05) -> None:
        deadline = time.monotonic() + budget_seconds
        read = 0
        with self.lock:
            self._catalogs()
            paths = sorted(self._paths(now), key=lambda path: path.name[-16:-6], reverse=True)
            for path in paths:
                if time.monotonic() >= deadline or read >= READ_BUDGET:
                    break
                position = self.positions.get(path.name, 0)
                try:
                    with path.open('rb') as stream:
                        stream.seek(position)
                        while time.monotonic() < deadline and read < READ_BUDGET:
                            raw = stream.readline(MAX_RECORD + 1)
                            if not raw:
                                break
                            read += len(raw)
                            if path.name in self.discarding or len(raw) > MAX_RECORD:
                                self.history_limited = True
                                position = stream.tell()
                                if raw.endswith(b'\n'):
                                    self.discarding.discard(path.name)
                                else:
                                    self.discarding.add(path.name)
                                continue
                            if not raw.endswith(b'\n'):
                                break
                            position = stream.tell()
                            try:
                                if len(raw) > MAX_RECORD:
                                    raise ValueError('oversized_record')
                                record = _decode(raw)
                                if path.name.startswith('samples-'):
                                    self._sample(record, position - len(raw))
                                elif record.get('gate_id') != OWN_GATE:
                                    self._event(record, position - len(raw))
                            except (ValueError, TypeError) as error:
                                self.history_limited = True
                                log.warning('Law history record unreadable: %s', type(error).__name__)
                        self.positions[path.name] = position
                except OSError as error:
                    log.warning('Law history unavailable: %s', type(error).__name__)
            self.bytes_read += read
            self.loading = any(self.positions.get(path.name, 0) < path.stat().st_size for path in paths)
            floor = (now - timedelta(days=30)).isoformat()
            self.last_events = {gate: event for gate, event in self.last_events.items() if str(event.get('evaluated_at')) >= floor}
            for slot in tuple(self.samples):
                if slot < floor:
                    self.sample_bytes -= _weight(self.samples.pop(slot)) + sys.getsizeof(slot) + 128
            for mapping in (self.days, self.seen, self.observed, self.index):
                for key in tuple(mapping):
                    if key[1] < floor[:10]:
                        if mapping is self.seen:
                            self.event_count -= len(self.seen[key]) // 16
                        del mapping[key]

    def _definitions(self, gate: str) -> list[Document]:
        return list(self.definitions.get(gate, {}).values())

    def _daily(self, gate: str, start: datetime, end: datetime) -> list[Json]:
        definitions = self._definitions(gate)
        periodic = any(item.get('cadence') == 'periodic' for item in definitions)
        days: list[Json] = []
        day = start.replace(hour=0, minute=0, second=0, microsecond=0)
        while day < end:
            key = (gate, day.date().isoformat())
            row = dict(self.days.get(key, {'day': key[1], 'pass_count': 0, 'fail_count': 0, 'wait_count': 0, 'unknown_count': 0, 'evaluation_count': 0, 'not_observed': True}))
            first = int((max(start, day) - day).total_seconds() // 60)
            stop = math.ceil((min(end, day + timedelta(days=1)) - day).total_seconds() / 60)
            mask = ((1 << (stop - first)) - 1) << first
            row['observed_slots'] = (self.observed.get(key, 0) & mask).bit_count() if periodic else None
            row['expected_slots'] = stop - first if periodic else None
            row['counts_scope'] = 'UTC day; coverage counts intersecting UTC minutes'
            days.append(row)
            day += timedelta(days=1)
        return days

    def current(self, now: datetime) -> Document:
        self.refresh_latest(now)
        with self.lock:
            record = self.latest
            age = max(0.0, (now - _instant(record.get('evaluation_end'))).total_seconds()) if record else None
            reason = self.latest_error or ('stale_report' if age is None or age > 120 else '')
            catalog: Document | None = self.catalogs.get(str(record.get('catalog_version'))) if record else None
            if not catalog and not reason:
                reason = 'catalog_unavailable'
            status = 'UNKNOWN' if reason else str(record.get('status')) if record else 'UNKNOWN'
            clear = consecutive_window(list(self.samples.values()), str(record.get('sampling_slot'))) if record and status == 'PASS' else []
            delays: Document = {}
            if record:
                prior = self.samples.get((_instant(record.get('sampling_slot')) - timedelta(hours=1)).isoformat())
                for feed in _objects(record.get('feeds')):
                    key = str(feed.get('source_key'))
                    current = _number(_object(_object(_object(feed.get('predicates')).get('R1')).get('evidence')).get('age_seconds'))
                    previous = _number(_object(prior.get('ages')).get(key)) if prior else None
                    delays[key] = current - previous if current is not None and previous is not None else None
            all_gates: dict[str, Document] = {}
            for key, versions in self.definitions.items():
                all_gates[key] = {**next(reversed(versions.values())), 'retired': True}
            if catalog:
                for gate in _objects(catalog.get('gates')):
                    all_gates[str(gate.get('id'))] = gate
                catalog = {**catalog, 'gates': list[Json](all_gates.values())}
            return {
                'status': status, 'reason': reason, 'checked_at': record.get('evaluation_end') if record else None,
                'age_seconds': age, 'last_report': record, 'catalog': catalog, 'history_loading': self.loading, 'history_limited': self.history_limited,
                'delay_change_1h_seconds': delays, 'consecutive_clear_slots': len(clear),
                'consecutive_clear_seconds': (_instant(clear[0].get('start')) - _instant(clear[-1].get('start'))).total_seconds() if clear else 0,
                'not_due_slots': sum(sample.get('not_due') is True for sample in clear),
                'catalog_key': f'{record.get("catalog_version")}:{len(self.catalog_versions)}' if record else None,
                'last_gate_events': dict(self.last_events),
            }

    def overview(self, now: datetime) -> Document:
        with self.lock:
            start = now.replace(hour=0, minute=0, second=0, microsecond=0) - timedelta(days=29)
            dates = [(start + timedelta(days=index)).date().isoformat() for index in range(30)]
            return {'catalog_version': self.latest.get('catalog_version') if self.latest else None, 'days': list[Json](dates), 'end': now.isoformat(), 'loading': self.loading, 'limited': self.history_limited,
                'gates': {gate: [[row.get(key) for key in ('day', 'pass_count', 'fail_count', 'wait_count', 'unknown_count', 'evaluation_count', 'observed_slots', 'expected_slots')]
                    for row in _objects(self._daily(gate, start, now)) if row.get('evaluation_count')]
                    for gate in self.definitions}}

    def history(self, query: dict[str, list[str]], now: datetime) -> Document:
        with self.lock:
            return self._history(query, now)

    def _history(self, query: dict[str, list[str]], now: datetime) -> Document:
        if set(query) - {'gate_id', 'from', 'to', 'cursor'} or any(len(values) != 1 for values in query.values()):
            raise ValueError('invalid_history_query')
        gate = query.get('gate_id', [''])[0]
        prefix = 'samples' if gate == OWN_GATE else 'gate-events'
        start, end = _instant(query.get('from', [''])[0]), _instant(query.get('to', [''])[0])
        if start >= end or end - start > timedelta(days=30) or end > now + timedelta(minutes=1):
            raise ValueError('invalid_history_range')
        definitions = self._definitions(gate)
        if not definitions:
            raise ValueError('unknown_gate')
        signature = hashlib.sha256(f'{gate}|{start.isoformat()}|{end.isoformat()}'.encode()).hexdigest()[:16]
        before: tuple[str, int, int] = (end.date().isoformat(), _micros(end), 0)
        cursor = query.get('cursor', [''])[0]
        if cursor:
            saved = self.cursors.get(cursor)
            if saved is None or saved[0] != signature:
                raise ValueError('invalid_cursor')
            before = (saved[1], saved[2], saved[3])
        events: list[Document] = []
        read, more = 0, False
        deadline = time.monotonic() + 0.2
        last: tuple[str, int, int] = before
        days = sorted((day for key, day in self.index if key == gate
            and start.date().isoformat() <= day <= before[0]), reverse=True)
        for day in days:
            packed = self.index[(gate, day)]
            bound = struct.pack('>QQ', before[1], before[2]) if day == before[0] else struct.pack('>QQ', _micros(end), 0)
            position = _packed_position(packed, bound) - 16
            with (self.root / f'{prefix}-{day}.jsonl').open('rb') as stream:
                while position >= 0:
                    stamp, offset = cast(tuple[int, int], struct.unpack('>QQ', packed[position:position + 16]))
                    if stamp < _micros(start):
                        break
                    if len(events) >= PAGE_SIZE or read >= READ_BUDGET or time.monotonic() >= deadline:
                        more = True
                        break
                    stream.seek(offset)
                    raw = stream.readline(MAX_RECORD + 1)
                    read += len(raw)
                    last = (day, stamp, offset)
                    position -= 16
                    try:
                        if len(raw) > MAX_RECORD or not raw.endswith(b'\n'):
                            raise ValueError('invalid_indexed_record')
                        record = _decode(raw)
                        if gate == OWN_GATE:
                            record = _sample_event(record)
                        if record.get('gate_id') != gate or _micros(_instant(record.get('evaluated_at'))) != stamp:
                            raise ValueError('history_index_mismatch')
                        events.append(record)
                    except (ValueError, TypeError) as error:
                        self.history_limited = True
                        log.warning('Law history record unreadable: %s', type(error).__name__)
            if more:
                break
        continuation = None
        if more:
            continuation = secrets.token_urlsafe(24)
            self.cursors[continuation] = (signature, last[0], last[1], last[2])
            while len(self.cursors) > 8:
                del self.cursors[next(iter(self.cursors))]
        loading = any(self.positions.get(path.name, 0) < path.stat().st_size
            for path in self._paths(now) if path.name.startswith(prefix + '-')
            and start.date().isoformat() <= path.name[-16:-6] <= end.date().isoformat())
        provenances = {(str(event.get('definition_version')), str(event.get('deployed_sha')), str(event.get('catalog_version', ''))) for event in events}
        originals: list[Json] = []
        metadata_read = 0
        metadata_deadline = time.monotonic() + 0.05
        for version, sha, recorded_catalog in sorted(provenances):
            catalog_version = recorded_catalog or self.catalog_files.get(sha)
            if catalog_version and self.catalog_versions.get(catalog_version) == sha and metadata_read < READ_BUDGET and time.monotonic() < metadata_deadline:
                path = self.root / f'catalog-{catalog_version}.json'
                if path.is_symlink() or path.stat().st_size > MAX_RECORD:
                    raise ValueError('invalid_catalog')
                raw = path.read_bytes()
                metadata_read += len(raw)
                originals.extend({**descriptor, 'catalog_version': catalog_version} for descriptor in _objects(_decode(raw).get('gates'))
                    if descriptor.get('id') == gate and descriptor.get('definition_version') == version
                    and _object(descriptor.get('code')).get('deployed_sha') == sha)
        return {'gate_id': gate, 'start': start.isoformat(), 'end': end.isoformat(), 'loading': loading,
                'days': self._daily(gate, start, end), 'definitions': originals,
                'events': list[Json](events), 'next_cursor': continuation, 'limited': self.history_limited}


class LawServer(ThreadingMixIn, HTTPServer):
    daemon_threads = True
    block_on_close = False

    def __init__(self, address: tuple[str, int], cache: TapeCache, clock: Callable[[], datetime]) -> None:
        self.cache, self.clock = cache, clock
        self.slots = threading.BoundedSemaphore(REQUEST_SLOTS)
        super().__init__(address, LawHandler)

    def process_request(self, request: socket.socket | tuple[bytes, socket.socket], client_address: tuple[str, int]) -> None:
        if not self.slots.acquire(blocking=False):
            self.shutdown_request(request)
            return
        super().process_request(request, client_address)

    def process_request_thread(self, request: socket.socket | tuple[bytes, socket.socket], client_address: tuple[str, int]) -> None:
        try:
            super().process_request_thread(request, client_address)
        finally:
            self.slots.release()


class LawHandler(BaseHTTPRequestHandler):
    @property
    def law_server(self) -> LawServer:
        return cast(LawServer, self.server)

    def handle(self) -> None:
        def expire() -> None:
            try:
                self.connection.shutdown(socket.SHUT_RDWR)
            except OSError as error:
                log.debug('Law request already closed: %s', type(error).__name__)
        self.connection.settimeout(REQUEST_TIMEOUT)
        self.header_timer = threading.Timer(REQUEST_TIMEOUT, expire)
        self.header_timer.daemon = True
        self.header_timer.start()
        try:
            super().handle()
        except OSError as error:
            self.close_connection = True
            log.debug('Law client disconnected: %s', type(error).__name__)
        finally:
            self.header_timer.cancel()

    def parse_request(self) -> bool:
        parsed = super().parse_request()
        self.header_timer.cancel()
        self.connection.settimeout(10)
        return parsed

    def do_GET(self) -> None:
        if len(self.path) > 2048:
            self._send(400, b'{"reason":"invalid_request"}', 'application/json')
            return
        request = urllib.parse.urlsplit(self.path)
        try:
            if request.path == '/law':
                self._send(200, PAGE.encode(), 'text/html; charset=utf-8')
            elif request.path == '/law.json':
                data = self.law_server.cache.current(self.law_server.clock())
                data.pop('catalog', None)
                self._send(200, json.dumps(data, allow_nan=False, separators=(',', ':')).encode(), 'application/json')
            elif request.path == '/law/catalog.json':
                self._send(200, json.dumps(self.law_server.cache.current(self.law_server.clock()).get('catalog'), separators=(',', ':')).encode(), 'application/json')
            elif request.path == '/law/gates.json':
                self._send(200, json.dumps(self.law_server.cache.overview(self.law_server.clock()), separators=(',', ':')).encode(), 'application/json')
            elif request.path == '/law/history.json':
                data = self.law_server.cache.history(urllib.parse.parse_qs(request.query), self.law_server.clock())
                self._send(200, json.dumps(data, allow_nan=False).encode(), 'application/json')
            else:
                self._send(404, b'{"reason":"not_found"}', 'application/json')
        except (ValueError, OSError):
            self._send(400, b'{"reason":"invalid_or_unavailable_history"}', 'application/json')

    def _send(self, status: int, body: bytes, content_type: str) -> None:
        compressed = 'gzip' in self.headers.get('Accept-Encoding', '') and len(body) > 1024
        if compressed:
            body = gzip.compress(body, compresslevel=1)
        try:
            self.send_response(status)
            self.send_header('Content-Type', content_type)
            self.send_header('Content-Length', str(len(body)))
            if compressed:
                self.send_header('Content-Encoding', 'gzip')
                self.send_header('Vary', 'Accept-Encoding')
            self.send_header('Cache-Control', 'no-store')
            self.send_header('X-Content-Type-Options', 'nosniff')
            self.send_header('Content-Security-Policy', "default-src 'self'; script-src 'unsafe-inline'; style-src 'unsafe-inline'; img-src 'self' data:; connect-src 'self'; frame-ancestors 'none'; base-uri 'none'")
            self.end_headers()
            self.wfile.write(body)
        except OSError as error:
            self.close_connection = True
            log.debug('Law client disconnected: %s', type(error).__name__)

    def log_message(self, format: str, *args: object) -> None:
        log.debug(format, *args)


class HealthHandler(LawHandler):
    def do_GET(self) -> None:
        self._send(200 if self.path == '/healthz' else 404, b'ok\n' if self.path == '/healthz' else b'not found\n', 'text/plain')


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument('--check', action='store_true')
    parser.add_argument('--root', type=Path, default=ROOT)
    parser.add_argument('--port', type=int, default=8484)
    parser.add_argument('--health-port', type=int, default=8485)
    args = parser.parse_args(argv)
    if args.check:
        try:
            with urllib.request.urlopen(f'http://127.0.0.1:{args.health_port}/healthz', timeout=3) as response:
                return 0 if response.status == 200 and response.read(16) == b'ok\n' else 1
        except (OSError, urllib.error.URLError):
            return 1
    cache = TapeCache(args.root)
    cache.refresh_latest(datetime.now(UTC))
    stop = threading.Event()

    def refresh() -> None:
        while not stop.wait(0.25):
            try:
                now = datetime.now(UTC)
                cache.refresh_latest(now)
                cache.advance(now)
            except OSError as error:
                log.warning('Law tape unavailable: %s', type(error).__name__)

    thread = threading.Thread(target=refresh, daemon=True)
    thread.start()
    server = LawServer(('0.0.0.0', args.port), cache, lambda: datetime.now(UTC))
    health = HTTPServer(('0.0.0.0', args.health_port), HealthHandler)
    health_thread = threading.Thread(target=health.serve_forever, daemon=True)
    health_thread.start()
    try:
        server.serve_forever()
    finally:
        stop.set()
        server.server_close()
        health.shutdown()
        health.server_close()
        health_thread.join(timeout=1)
        thread.join(timeout=1)
    return 0

PAGE = r'''<!doctype html><html lang="en"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1"><title>Origo · Data observatory</title><style>
:root{--ink:#172b36;--muted:#687780;--line:#dce3e4;--paper:#f5f7f6;--green:#187862;--red:#b84236;--amber:#946414;--blue:#356e9b}*{box-sizing:border-box}body{margin:0;background:var(--paper);color:var(--ink);font:14px/1.5 system-ui,-apple-system,BlinkMacSystemFont,"Segoe UI",sans-serif}button,a,summary{touch-action:manipulation}button,select{font:inherit}button{cursor:pointer}a{color:var(--blue)}button:focus-visible,a:focus-visible,select:focus-visible{outline:3px solid #66a7d3;outline-offset:3px}header{height:76px;border-bottom:1px solid var(--line);display:flex;align-items:center;justify-content:space-between;padding:0 4vw;background:white}.brand{font-weight:800;letter-spacing:.22em;font-size:21px}.brand b{color:var(--green);margin-right:12px}.eyebrow{font-size:11px;letter-spacing:.13em;text-transform:uppercase;color:var(--muted);font-weight:700}.top-status{display:flex;gap:12px;align-items:center}.badge{display:inline-flex;gap:6px;align-items:center;border-radius:5px;padding:3px 7px;font-size:11px;font-weight:700;white-space:nowrap}.PASS,.CURRENT{color:var(--green);background:#eaf4ef}.FAIL,.FAILED,.STALE{color:var(--red);background:#fbefec}.EXPECTED_WAIT,.WAITING,.NOT_DUE{color:var(--amber);background:#f9f2e4}.UNKNOWN,.NOT_EVALUATED,.INACTIVE{color:var(--muted);background:#edf0f1}main{max-width:1500px;margin:auto;padding:34px 4vw 60px}.intro{display:flex;justify-content:space-between;gap:24px;align-items:end}h1{font-size:30px;letter-spacing:-.045em;line-height:1.2;margin:6px 0 9px}p{margin:8px 0}.muted{color:var(--muted)}.metrics{display:flex;gap:28px}.metric strong{font-size:25px;font-weight:550;display:block;line-height:1.2}.metric span{font-size:11px;color:var(--muted)}nav{display:flex;margin:28px 0 24px;gap:5px;border-bottom:1px solid var(--line)}nav button{border:0;background:none;padding:13px 20px;color:var(--muted);border-bottom:2px solid transparent}nav button[aria-selected=true]{border-color:var(--green);color:var(--ink);font-weight:700}.toolbar{display:flex;align-items:center;justify-content:space-between;gap:12px;margin-bottom:18px}.legend{display:flex;gap:14px;font-size:11px;color:var(--muted)}.dot{display:inline-block;width:7px;height:7px;border-radius:50%;background:currentColor;margin-right:5px}.layout{display:grid;grid-template-columns:minmax(0,1fr);gap:20px}.layout.has-detail{grid-template-columns:minmax(0,1fr) 310px}.source{position:relative;background:white;border:1px solid var(--line);border-radius:10px;margin-bottom:15px;padding:20px;overflow:hidden}.source-head{display:flex;align-items:center;justify-content:space-between;gap:10px;margin-bottom:16px}.source-title{border:0;background:none;padding:0;text-align:left;font-size:16px;font-weight:650}.source-sub{font-size:11px;color:var(--muted);margin-top:3px}.diagram{display:grid;grid-template-columns:104px 1fr;gap:22px;position:relative;align-items:center}.hub{z-index:1;background:#f0f5f3;border:1px solid #c9dad4;border-radius:50%;width:94px;height:94px;display:flex;flex-direction:column;align-items:center;justify-content:center;color:var(--ink);font-weight:700}.hub small{font-size:9px;color:var(--muted);font-weight:500}.lanes{display:flex;flex-direction:column;gap:12px;z-index:1}.lane-label{font-size:9px;text-transform:uppercase;letter-spacing:.1em;color:var(--muted);margin-bottom:5px}.nodes{display:flex;gap:7px;flex-wrap:wrap}.node{border:1px solid var(--line);background:white;border-radius:6px;padding:8px;min-width:69px;text-align:left;position:relative;flex:0 1 auto}.node:hover,.node.selected{border-color:#74a795;box-shadow:0 0 0 2px #e2efea}.node-name{font-size:12px;font-weight:650;display:block}.node .badge{font-size:10px;padding:1px 3px;margin-top:5px}.node.target{border-style:dashed}.wires{position:absolute;inset:0;width:100%;height:100%;pointer-events:none;overflow:visible}.wires path{fill:none;stroke:#cad9d3;stroke-width:1}.wires .target-edge{stroke:#a7bfcf;stroke-dasharray:3 3}.source-foot{display:flex;flex-wrap:wrap;gap:12px;border-top:1px solid #edf0ef;margin-top:15px;padding-top:10px;font-size:10px;color:var(--muted)}.source-foot strong{color:var(--ink);font-weight:600}.detail{position:sticky;top:18px;align-self:start;background:#fff;border:1px solid var(--line);border-radius:9px;padding:20px;max-height:calc(100vh - 36px);overflow:auto}.detail h2{font-size:18px;margin:10px 0}.close{float:right;border:0;background:none;color:var(--muted);font-size:19px}.detail dl{margin:15px 0}.detail dt{font-size:10px;color:var(--muted);text-transform:uppercase;margin-top:12px}.detail dd{margin:3px 0;overflow-wrap:anywhere;font-size:12px}.link-button{border:1px solid var(--line);border-radius:5px;background:#fff;padding:6px 9px;color:var(--blue);font-size:11px;display:inline-block;margin:3px}.note{background:#f1f5f3;border-left:2px solid #88aa9a;padding:10px;font-size:12px;margin:12px 0}.empty{padding:44px 24px;background:white;border:1px dashed var(--line);border-radius:9px;color:var(--muted)}.gate{position:relative;background:#fff;border:1px solid var(--line);border-radius:8px;padding:15px 18px;margin-bottom:9px}.gate-heading{display:flex;align-items:center;gap:9px;margin-bottom:8px}.gate-heading>button:first-child{font-size:12px;font-weight:650;border:0;background:none;text-align:left;padding:0;flex:1}.info-wrap{position:relative}.info{border:1px solid #c4cfd2;border-radius:50%;background:white;width:19px;height:19px;font:italic 12px Georgia;color:var(--muted)}.tooltip{display:none;position:absolute;right:0;top:25px;width:300px;padding:15px;z-index:10;background:#172b36;color:white;border-radius:7px;box-shadow:0 8px 24px #172b3633;font-size:11px}.tooltip a{color:#a7d6fa}.tooltip strong{display:block;color:#cadbdc;font-size:9px;text-transform:uppercase;margin-top:9px}.info-wrap:hover .tooltip,.info-wrap:focus-within .tooltip,.info-wrap.open .tooltip{display:block}.heatmap{display:grid;grid-template-columns:repeat(30,minmax(3px,1fr));gap:3px}.day{height:18px;border:0;border-radius:2px;background:#edf0ef;padding:0}.day.pass{background:#b7dacf}.day.fail{background:#cc7265}.day.wait{background:#e2cb90}.day.gap{background:repeating-linear-gradient(135deg,#e5e9e8,#e5e9e8 3px,#f6f8f7 3px,#f6f8f7 6px)}.day.selected{outline:2px solid var(--ink);outline-offset:1px}.gate-meta{display:flex;justify-content:space-between;gap:10px;font-size:9px;color:var(--muted);margin-top:6px}.context{display:flex;gap:6px;align-items:center;flex-wrap:wrap;font-size:11px}.context button{border:1px solid #d5e0dd;border-radius:20px;background:#fff;padding:4px 9px}.chart-card{background:#fff;border:1px solid var(--line);border-radius:9px;padding:20px;margin-bottom:14px}.chart-card h3{font-size:14px;margin:0 0 8px}.spark{height:130px;width:100%;display:block}.spark path{fill:none;stroke:var(--green);stroke-width:2}.spark line{stroke:var(--line);stroke-dasharray:3 4}.spark text{fill:var(--muted);font:10px system-ui}.trend-empty{height:100px;display:flex;align-items:center;justify-content:center;color:var(--muted);font-size:12px}.event{padding:10px 0;border-top:1px solid var(--line);font-size:11px}.event time{color:var(--muted)}.event dl{display:grid;grid-template-columns:1fr 1fr;gap:3px;margin:7px 0}.event dt,.event dd{font-size:10px;margin:0}.event dd{text-align:right}.recovery-metrics{display:flex;gap:24px;flex-wrap:wrap;margin:10px 0}.recovery-metrics strong{display:block;font-size:20px;font-weight:500}.recovery-metrics small{color:var(--muted);font-size:10px}.notice{color:var(--amber);font-size:12px;padding:9px 0}.footnote{margin-top:25px;font-size:10px;color:var(--muted)}select{padding:6px;border:1px solid var(--line);border-radius:5px;background:white;color:var(--ink)}@media(max-width:1050px){.layout.has-detail{grid-template-columns:1fr}.detail{position:relative;top:0;max-height:none;grid-row:1}.diagram{grid-template-columns:80px 1fr;gap:13px}.hub{width:72px;height:72px;font-size:11px}.metrics{gap:15px}.node{min-width:61px}}@media(max-width:600px){header{height:62px;padding:0 18px}.brand{font-size:17px}.top-status .eyebrow{display:none}main{padding:24px 16px}.intro{display:block}h1{font-size:25px}.metrics{margin-top:18px;justify-content:space-between}.metric strong{font-size:21px}nav{margin-top:20px}nav button{padding:11px 16px}.source{padding:14px}.diagram{grid-template-columns:1fr}.hub{border-radius:5px;width:auto;height:auto;padding:7px;flex-direction:row;gap:8px;justify-content:start}.wires{display:none}.nodes{gap:5px}.node{flex:1 0 65px}.toolbar{align-items:start}.legend{gap:6px;font-size:9px}.tooltip{position:fixed;top:100px;right:18px;width:calc(100vw - 36px)}.source-head{align-items:start}.heatmap{gap:2px}.day{height:23px}}
.filters{display:flex;gap:8px;flex-wrap:wrap;margin-bottom:16px}.filters:empty{display:none}.filters input{min-width:220px;flex:1;border:1px solid var(--line);border-radius:5px;padding:8px;font:inherit}.filters select{max-width:250px}.layout:not(.has-detail) #content[data-view=sources]{display:grid;grid-template-columns:repeat(2,minmax(0,1fr));gap:15px}.layout:not(.has-detail) #content[data-view=sources] .source{margin-bottom:0}@media(max-width:1150px){.layout:not(.has-detail) #content[data-view=sources]{grid-template-columns:1fr}}</style></head><body><header><div class="brand"><b>◈</b>ORIGO</div><div class="top-status"><span class="eyebrow">Data observatory</span><span id="health" class="badge UNKNOWN">◌ Waiting for evidence</span></div></header><main><section class="intro"><div><div class="eyebrow">Source to reader · live evidence</div><h1>Know where your data stands.</h1><div id="summary" class="muted">Loading the latest observation…</div></div><div class="metrics" id="metrics"></div></section><nav aria-label="Observatory views" role="tablist"><button role="tab" data-view="sources" aria-selected="true">Sources</button><button role="tab" data-view="gates" aria-selected="false">Gates</button><button role="tab" data-view="recovery" aria-selected="false">Recovery</button></nav><div class="toolbar"><div id="context" class="context"></div><div class="legend"><span style="color:var(--green)">● Current</span><span style="color:var(--red)">● Attention</span><span>◌ Unverified</span></div></div><div id="filters" class="filters"></div><div id="message" role="status" aria-live="polite"></div><div id="layout" class="layout"><section id="content" aria-label="Sources"></section><aside id="detail" class="detail" hidden></aside></div><p class="footnote">UTC throughout · Observations describe their evidence time. Core-law status is separate from runtime gate outcomes. Operational actions remain in Dagit.</p></main><script>
'use strict';
const $=s=>document.querySelector(s), esc=v=>String(v??'—').replace(/[&<>"']/g,c=>({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]));
const state=Object.fromEntries(new URLSearchParams(location.search));state.view=['sources','gates','recovery'].includes(state.view)?state.view:'sources';
let data=null, selectionError='', histories=new Map(), requests=new Set(), receivedAt=0, receivedAge=0, refreshing=false, catalogKey=null, cachedCatalog=null, overviewKey=null, overviewLoading=false;
const badge=s=>`<span class="badge ${['PASS','CURRENT','FAIL','FAILED','STALE','EXPECTED_WAIT','WAITING','NOT_DUE','UNKNOWN','NOT_EVALUATED','INACTIVE'].includes(s)?s:'UNKNOWN'}">${['PASS','CURRENT'].includes(s)?'●':['FAIL','FAILED','STALE'].includes(s)?'!':'◌'} ${esc((s||'UNKNOWN').replaceAll('_',' ').toLowerCase())}</span>`;
const duration=s=>typeof s!=='number'?'—':Math.abs(s)<60?`${Math.round(s)}s`:Math.abs(s)<3600?`${Math.round(s/60)}m`:`${(s/3600).toFixed(1)}h`;
const lagChange=s=>typeof s!=='number'?'—':s===0?'0s · unchanged':`${s<0?'-':'+'}${duration(Math.abs(s))} · ${s<0?'less lag':'more lag'}`;
const utc=s=>s?new Date(s).toISOString().replace('T',' ').replace(/\.\d+Z$/,' UTC'):'Not observed';
const label=s=>String(s||'').replaceAll('_',' '), safeLink=s=>{try{const u=new URL(s,location.origin);return ['http:','https:'].includes(u.protocol)&&!u.username&&!u.password?esc(u.href):''}catch{return ''}};
const sources=()=>data?.catalog?.sources||[], gates=()=>data?.catalog?.gates||[], report=()=>data?.last_report||{}, feed=id=>(report().feeds||[]).find(x=>x.source_key===id), observation=id=>{const o=(report().projections||[]).find(x=>x.id===id);return o&&data.reason?{...o,status:'UNKNOWN',reason:data.reason}:o}, event=id=>{const g=descriptor(id),current=(report().gates||[]).find(x=>x.gate_id===id),last=data?.last_gate_events?.[id];if(g?.cadence==='event-driven'&&last?.definition_version===g.definition_version)return {...last,last_recorded:true};return current&&!data.reason?current:{outcome:g?.cadence==='event-driven'?'NOT_EVALUATED':'UNKNOWN'}}, descriptor=id=>gates().find(x=>x.id===id), source=id=>sources().find(x=>x.id===id), projection=id=>sources().flatMap(s=>s.projections).find(p=>p.id===id);
function select(values){Object.assign(state,values);for(const k of Object.keys(state))if(!state[k])delete state[k];history.replaceState(null,'',location.pathname+'?'+new URLSearchParams(state));render()}
function codeLink(code){return code&&/^[a-f0-9]{40}$/.test(code.deployed_sha)&&safeLink(code.url)?`<a href="${safeLink(code.url)}" target="_blank" rel="noopener">${esc(code.path)}:${esc(code.line)} ↗</a>`:'Code location unavailable · UNKNOWN'}
function tooltip(g){return `<strong>What this gate is</strong>${esc(g.purpose)}<strong>Governs · ${esc(g.role)}</strong>${esc(g.governed_action)}<strong>Fails or waits when</strong>${esc(g.condition)}<strong>Configured thresholds</strong>${Object.entries(g.thresholds||{}).map(([k,v])=>`${esc(label(k))}: ${esc(v)}`).join('<br>')||'No numeric threshold'}<strong>Evidence · ${esc(g.cadence)}</strong>${esc(g.evidence_source)}<strong>Deployed code</strong>${codeLink(g.code)}`}
function sourceCard(s){const f=feed(s.id),r=f?.predicates?.R1,e=r?.evidence||{},obs=s.projections.map(p=>observation(p.id)),problems=obs.filter(o=>!o||['FAILED','STALE','UNKNOWN'].includes(o.status)).length;return `<article class="source" data-source-card="${esc(s.id)}"><div class="source-head"><div><button class="source-title" data-source="${esc(s.id)}">${esc(label(s.name))}</button><div class="source-sub">${esc(s.rollout_stage)} · ${s.projections.length} declared stages</div></div>${badge(data.reason?'UNKNOWN':Object.values(f?.predicates||{}).some(p=>p.status==='FAIL')?'FAIL':obs.some(o=>o&&['FAILED','STALE'].includes(o.status))?'FAIL':problems?'UNKNOWN':'CURRENT')}</div><div class="diagram"><svg class="wires" aria-hidden="true"></svg><button class="hub" data-source="${esc(s.id)}"><span>◈ Source</span><small>${esc(s.rollout_stage)}</small></button><div class="lanes">${['canonical','provisional','depth','consumer'].map(l=>{const nodes=s.projections.filter(p=>p.lane===l);return nodes.length?`<div class="lane"><div class="lane-label">${l==='consumer'?'Published outputs':esc(l)}</div><div class="nodes">${nodes.map(p=>`<button class="node ${state.projection===p.id?'selected':''} ${p.current_target?'target':''}" data-projection="${esc(p.id)}" data-target="${esc(p.current_target||'')}" aria-label="${esc(s.name+' '+p.name+' '+(observation(p.id)?.status||'UNKNOWN'))}"><span class="node-name">${esc(label(p.name))}</span>${badge(observation(p.id)?.status||'UNKNOWN')}</button>`).join('')}</div></div>`:''}).join('')}</div></div><div class="source-foot"><span>${f?.predicates?.C2?.status==='PASS'&&f?.predicates?.C1?.status==='PASS'?'Readable through':'Latest readable end'} <strong>${e.reader_end?esc(utc(e.reader_end)):'Not observed'}</strong></span><span>Delay <strong>${duration(e.age_seconds)}</strong>${e.budget_seconds!=null?' / '+duration(e.budget_seconds)+' budget':''}</span>${f?.predicates?.C2?`<span>Older history ${badge(f.predicates.C2.status)}</span>`:''}${f?.predicates?.D1?`<span>Depth gaps <strong>${esc(f.predicates.D1.evidence?.missing_slots)}</strong> / 1,440 slots</span>`:''}</div></article>`}
function drawWires(){document.querySelectorAll('.diagram').forEach(d=>{const svg=d.querySelector('svg'),hub=d.querySelector('.hub').getBoundingClientRect(),box=d.getBoundingClientRect();let paths='';d.querySelectorAll('.node').forEach(n=>{const rect=n.getBoundingClientRect(),x=hub.right-box.left,y=hub.top+hub.height/2-box.top,ex=rect.left-box.left,ey=rect.top+rect.height/2-box.top;paths+=`<path d="M${x},${y} H${x+9} V${ey} H${ex}"/>`;const target=n.dataset.target;if(target){const t=[...d.querySelectorAll('.node')].find(o=>o.dataset.projection===target||o.dataset.projection.endsWith(':'+target));if(t){const b=t.getBoundingClientRect();paths+=`<path class="target-edge" d="M${rect.left+rect.width/2-box.left},${rect.top-box.top} V${b.bottom-box.top}"/>`}}});svg.innerHTML=paths})}
function blankDays(g,days,end){return days.map(day=>({day,pass_count:0,fail_count:0,wait_count:0,unknown_count:0,evaluation_count:0,not_observed:true,observed_slots:g.cadence==='periodic'?0:null,expected_slots:g.cadence==='periodic'?Math.ceil(Math.min(86400000,Date.parse(end)-Date.parse(day+'T00:00:00Z'))/60000):null}))}
async function loadOverview(){const target=data,key=data?.checked_at+'|'+data?.history_loading;if(overviewLoading||overviewKey===key)return;overviewLoading=true;try{const response=await fetch('/law/gates.json',{signal:AbortSignal.timeout(10000)});if(!response.ok)throw Error();const overview=await response.json(),rows={};if(data!==target)return;if(overview.catalog_version!==target.last_report?.catalog_version)throw Error();for(const gate of gates()){const days=blankDays(gate,overview.days,overview.end),indexed=new Map(days.map(d=>[d.day,d]));for(const row of overview.gates[gate.id]||[]){const day=indexed.get(row[0]);if(day)Object.assign(day,{pass_count:row[1],fail_count:row[2],wait_count:row[3],unknown_count:row[4],evaluation_count:row[5],observed_slots:row[6],expected_slots:row[7],not_observed:false})}rows[gate.id]=days}data.gate_days=rows;overviewKey=key;if(state.view==='gates')renderContent()}catch{$('#message').textContent='Gate history unavailable. Current source observations remain visible.'}finally{overviewLoading=false}}
function gateCard(g){const days=data.gate_days?.[g.id]||[],ev=event(g.id),filtered=state.source&&!g.scope?.some(id=>id===state.source||id.startsWith(state.source+':'));if(filtered||(state.family&&!g.id.startsWith(state.family))||(state.search&&!JSON.stringify([g.name,g.purpose,g.condition,g.scope]).toLowerCase().includes(state.search.toLowerCase()))||(state.attention==='1'&&!['FAIL','UNKNOWN'].includes(ev?.outcome)))return '';return `<article class="gate"><div class="gate-heading"><button data-gate="${esc(g.id)}">${esc(g.name)}${g.retired?' · retired':''}</button><span class="info-wrap"><button class="info" aria-label="About ${esc(g.name)}" aria-expanded="false" data-info>i</button><span class="tooltip" role="tooltip">${tooltip(g)}</span></span>${badge(ev?.outcome||'NOT_EVALUATED')}</div><div class="gate-meta" style="margin:0 0 7px">${esc((g.scope||[]).map(label).join(' · '))}</div><div class="heatmap" aria-label="30-day observations for ${esc(g.name)}">${days.map(d=>{const gap=d.not_observed||(d.expected_slots!=null&&d.observed_slots<d.expected_slots),kind=d.fail_count?'fail':d.unknown_count||gap?'gap':d.wait_count?'wait':'pass';return `<button class="day ${kind}" data-day="${d.day}" data-gate-day="${esc(g.id)}" title="${d.day} UTC day totals · ${d.evaluation_count} evaluations · ${d.fail_count} failed${gap?' · observation gaps':''}" aria-label="${d.day}: ${d.fail_count} failed, ${d.evaluation_count} evaluations${gap?', observation gaps':''}"></button>`}).join('')}</div><div class="gate-meta"><span>${esc(days[0]?.day||'History loading')} → today · ${esc(g.role)}</span><span>${ev?.last_recorded?'Last recorded · current blocking unknown':g.cadence==='event-driven'?'Event-driven · no event ≠ pass':'Periodic · gaps remain visible'}${ev?.evaluated_at?' · '+esc(utc(ev.evaluated_at)):''}</span></div></article>`}
function relevant(p,o){return [...new Set([...(o?.gate_ids||[]),...gates().filter(g=>(g.scope||[]).includes(p.id)||g.id.startsWith('law.')&&(g.scope||[]).includes(p.source_key)).map(g=>g.id)])].sort((a,b)=>gateRank(descriptor(a)||{})-gateRank(descriptor(b)||{}))}
function fields(values){return Object.entries(values||{}).filter(([,v])=>v!==null&&typeof v!=='object').map(([k,v])=>`<dt>${esc(label(k))}</dt><dd>${esc(v)}</dd>`).join('')}
function eventList(events,definitions=[]){return events.map(e=>`<div class="event">${badge(e.outcome)} <time>${esc(utc(e.evaluated_at))}</time><p>${esc(e.effect)} · ${esc(label(e.reason))}</p><dl>${fields(e.evidence)}</dl><details><summary>Definition at evaluation</summary>${definitions.find(d=>e.deployed_sha&&d.definition_version===e.definition_version&&d.code?.deployed_sha===e.deployed_sha&&(!e.catalog_version||d.catalog_version===e.catalog_version))?tooltip(definitions.find(d=>e.deployed_sha&&d.definition_version===e.definition_version&&d.code?.deployed_sha===e.deployed_sha&&(!e.catalog_version||d.catalog_version===e.catalog_version))):'Original deployed code unavailable · UNKNOWN'}</details>${e.dagit_url&&safeLink(e.dagit_url)?`<a href="${safeLink(e.dagit_url)}" target="_blank" rel="noopener">Dagit evidence ↗</a>`:''}</div>`).join('')}
function interval(){const end=state.to||data?.checked_at||new Date().toISOString(),start=state.from||new Date(new Date(end)-86400000).toISOString();return {start,end}}
function storeHistory(key,value){histories.delete(key);histories.set(key,value);while(histories.size>8)histories.delete(histories.keys().next().value)}
function getHistory(key){const value=histories.get(key);if(value){histories.delete(key);histories.set(key,value)}return value}
function historyKey(id){const {start,end}=interval();return id+'|'+start+'|'+end}
function mergeDefinitions(events,definitions){
const key=(id,version,catalog,sha)=>JSON.stringify([id,version,catalog||'',sha||'']);
const wanted=new Set(events.map(e=>key(e.gate_id,e.definition_version,e.catalog_version,e.deployed_sha))),unique=new Map();
for(const d of definitions){const id=key(d.id,d.definition_version,d.catalog_version,d.code?.deployed_sha);if(wanted.has(id)||wanted.has(key(d.id,d.definition_version,null,d.code?.deployed_sha)))unique.set(id,d)}
return [...unique.values()]}
async function loadHistory(id,more=false){const key=historyKey(id);if(requests.has(key)||histories.has(key)&&!more)return;requests.add(key);const {start,end}=interval(),old=getHistory(key),q=new URLSearchParams({gate_id:id,from:start,to:end});if(more&&old?.next_cursor)q.set('cursor',old.next_cursor);try{const response=await fetch('/law/history.json?'+q);if(!response.ok)throw Error('History unavailable');const value=await response.json();value.events=[...(more?old.events:[]),...value.events];if(value.loading)value.error='History is still indexing; these observations are incomplete.';if(value.events.length>5000){value.events=value.events.slice(0,5000);value.limited=true;value.next_cursor=null}value.definitions=mergeDefinitions(value.events,[...(more?old.definitions||[]:[]),...(value.definitions||[])]);if(value.limited)value.error='History coverage is incomplete: a record or cache limit prevented a complete replay.';storeHistory(key,value);renderDetail();if(state.view==='recovery')renderContent()}catch{storeHistory(key,{error:'History unavailable. The current observation remains visible.',events:[]});renderDetail()}finally{requests.delete(key)}}
function trend(events,key='age_seconds',width=710){const unit=key.includes('seconds')?'s':'';const pts=events.map(e=>({x:Date.parse(e.evaluated_at),y:e.evidence?.[key]})).filter(p=>typeof p.y==='number'&&Number.isFinite(p.y)).sort((a,b)=>a.x-b.x);if(!pts.length)return '<div class="trend-empty">No numeric observations in this interval.</div>';const lo=pts[0].x,hi=pts.at(-1).x,max=Math.max(1,...pts.map(p=>p.y));let d='';pts.forEach((p,i)=>{const x=42+(p.x-lo)/Math.max(1,hi-lo)*(width-70),y=107-p.y/max*85;d+=(i&&p.x-pts[i-1].x<=120000?'L':'M')+x+','+y+' ';if(pts.length===1)d+='l1,0 '});return `<svg class="spark" viewBox="0 0 ${width} 130" role="img" aria-label="Observed ${esc(label(key))}; gaps are not interpolated"><line x1="42" y1="107" x2="${width-28}" y2="107"/><text x="2" y="22">${Math.round(max)}${unit}</text><text x="12" y="110">0${unit}</text><path d="${d}"/><text x="42" y="128">${esc(new Date(lo).toISOString().slice(11,16))} UTC</text><text x="${width-95}" y="128">${esc(new Date(hi).toISOString().slice(11,16))} UTC</text></svg>`}
function numericChart(h){if(!h?.events?.length)return '';const keys=[...new Set(h.events.flatMap(e=>Object.entries(e.evidence||{}).filter(([,v])=>typeof v==='number').map(([k])=>k)))];if(!keys.length)return '';const metric=keys.includes(state.metric)?state.metric:keys.includes('age_seconds')?'age_seconds':keys[0];return `<label class="eyebrow" for="history-metric">Observed values</label><select id="history-metric" style="max-width:100%;margin-top:7px">${keys.map(k=>`<option value="${esc(k)}" ${k===metric?'selected':''}>${esc(label(k))}</option>`).join('')}</select>${trend(h.events,metric,260)}`}
function renderDetail(){const box=$('#detail'),g=descriptor(state.gate),p=projection(state.projection),s=source(state.source)||source(p?.source_key);box.hidden=!g&&!p&&!s;$('#layout').classList.toggle('has-detail',!box.hidden);if(box.hidden)return;let body='<button class="close" aria-label="Close details" data-close>&times;</button><div class="eyebrow">Evidence, in context</div>';if(g){const h=getHistory(historyKey(g.id));body+=`<h2>${esc(g.name)}</h2>${badge(event(g.id)?.outcome||'NOT_EVALUATED')}${event(g.id)?.last_recorded?`<p class="muted">Last recorded ${esc(utc(event(g.id).evaluated_at))} · current blocking unknown</p>`:''}<div class="note">${esc(g.governed_action)}</div><dl><dt>Fails / waits when</dt><dd>${esc(g.condition)}</dd><dt>Thresholds</dt><dd>${esc(JSON.stringify(g.thresholds))}</dd><dt>Deployed definition</dt><dd>${codeLink(g.code)}</dd></dl><button class="link-button" data-range="24">Past 24 hours</button><button class="link-button" data-range="720">Past 30 days</button><p class="eyebrow">${esc(interval().start.slice(0,16))} → ${esc(interval().end.slice(0,16))} UTC</p>${numericChart(h)}${h?esc(h.error||'')+(h.events.length?eventList(h.events,h.definitions):'<p class="muted">No attributable evaluations in this interval.</p>'):'<p class="muted">Loading original evaluations…</p>'}${h?.next_cursor?'<button class="link-button" data-more>Load next observations</button>':''}`;loadHistory(g.id)}else if(p){const o=observation(p.id);body+=`<h2>${esc(label(p.name))}</h2><p class="muted">${esc(s?.name)} / ${esc(p.lane)}</p>${badge(o?.status||'UNKNOWN')}<dl><dt>Evidence</dt><dd>${p.lane==='consumer'?'Published artifact':p.lane==='depth'?'Depth store observation':'Active build / component proof'} · ${esc(o?.evidence_id||'Not observed')}</dd><dt>Data through</dt><dd>${esc(utc(o?.data_through))}</dd><dt>Evidence time</dt><dd>${esc(utc(o?.evidence_at))}</dd><dt>Reason</dt><dd>${esc(label(o?.reason||'unknown'))}</dd><dt>Definition</dt><dd>${codeLink(p.code)}</dd></dl>${o?.dagit_url&&safeLink(o.dagit_url)?`<a class="link-button" href="${safeLink(o.dagit_url)}" target="_blank" rel="noopener">Open in Dagit ↗</a>`:''}<p class="eyebrow">Relevant gates</p>${relevant(p,o).map(id=>`<button class="link-button" data-gate="${esc(id)}">${esc(descriptor(id)?.name||id)} →</button>`).join('')||'<p class="muted">No attributable gate evaluation.</p>'}`;}else if(s){const f=feed(s.id);body+=`<h2>${esc(label(s.name))}</h2><p class="muted">${esc(s.rollout_stage)} · ${s.projections.length} stages</p>${Object.entries(f?.predicates||{}).map(([k,v])=>`<p><strong>${esc(k)}</strong> ${badge(v.status)}</p><dl>${fields(v.evidence)}</dl>`).join('')}<button class="link-button" data-source-gates="${esc(s.id)}">Explore related gates →</button>`}box.innerHTML=body}
function attention(s){const states=Object.values(feed(s.id)?.predicates||{}).map(p=>p.status);return states.includes('FAIL')?0:states.includes('UNKNOWN')?1:2}function gateRank(g){return {FAIL:0,UNKNOWN:1,EXPECTED_WAIT:2}[event(g.id)?.outcome]??3}function renderContent(){if(state.view==='gates')loadOverview();let html='';if(state.view==='sources')html=[...sources()].sort((a,b)=>attention(a)-attention(b)).map(sourceCard).join('');else if(state.view==='gates')html=[...gates()].sort((a,b)=>gateRank(a)-gateRank(b)).map(gateCard).join('');else html=sources().filter(s=>!state.source||s.id===state.source).map(s=>{const f=feed(s.id),r=f?.predicates?.R1,e=r?.evidence||{},id=gates().find(g=>g.scope?.includes(s.id)&&(g.id.includes('R1')||g.id.toLowerCase().includes('r1')))?.id,h=id?getHistory(historyKey(id)):null;if(id)loadHistory(id);return `<article class="chart-card"><h3><button class="source-title" data-source="${esc(s.id)}">${esc(label(s.name))}</button></h3><div class="recovery-metrics"><div><strong>${duration(e.age_seconds)}</strong><small>Reader delay · ${duration(e.budget_seconds)} budget</small></div><div><strong>${lagChange(data.delay_change_1h_seconds?.[s.id])}</strong><small>Change versus one hour ago</small></div><div><strong>${badge(f?.predicates?.C1?.status||f?.predicates?.D1?.status||'UNKNOWN')}</strong><small>${f?.predicates?.C1?'Daily archive':'Depth continuity'}</small></div></div>${h?.next_cursor?`<p class="notice">Incomplete chart · ${h.events.length} observations loaded. <button class="link-button" data-history-more="${esc(id)}">Load next observations</button></p>`:''}${h?.error?`<p class="notice">${esc(h.error)}</p>`:''}${h?trend(h.events):'<div class="trend-empty">'+(id?'Loading original numeric observations…':'No mapped delay observations.')+'</div>'}<div class="source-foot"><span>Older history ${badge(f?.predicates?.C2?.status||'UNKNOWN')}</span>${f?.predicates?.C1?.evidence?.deadline?`<span>Archive deadline ${esc(utc(f.predicates.C1.evidence.deadline))}</span>`:''}</div></article>`}).join('');$('#content').innerHTML=html||'<div class="empty">No catalog is available yet. Current status remains unknown until the monitor records a complete observation.</div>';$('#content').setAttribute('aria-label',state.view);$('#content').dataset.view=state.view;requestAnimationFrame(drawWires)}
function render(){if(!data)return;const age=data.age_seconds,obs=(report().projections||[]).map(o=>observation(o.id)),needs=(report().feeds||[]).filter(f=>Object.values(f.predicates||{}).some(p=>p.status==='FAIL')).length;$('#health').outerHTML=`<span id="health" class="badge ${data.status}">Core law · ${esc(data.status.toLowerCase())}</span>`;$('#summary').textContent=needs?`${needs} sources need attention. Select a source or stage to see its evidence and blocker.`:'Follow every declared stage from its source to its published output.';$('#metrics').innerHTML=`<div class="metric"><strong>${sources().length}</strong><span>authoritative sources</span></div><div class="metric"><strong>${obs.filter(o=>o.status==='CURRENT').length} / ${sources().reduce((n,s)=>n+s.projections.length,0)}</strong><span>stages verified current</span></div><div class="metric"><strong>${duration(age)}</strong><span>since latest report</span></div>`;document.querySelectorAll('[data-view]').forEach(b=>b.setAttribute('aria-selected',b.dataset.view===state.view));$('#context').innerHTML=`<span class="eyebrow">${state.view==='sources'?'Source → projections → outputs':state.view==='gates'?'30 days of gate evidence':'Reader recovery'}</span>`+['source','projection','gate'].filter(k=>state[k]).map(k=>`<button data-clear="${k}" title="Clear ${k}">${esc(k==='source'?source(state[k])?.name||state[k]:k==='gate'?descriptor(state[k])?.name||state[k]:projection(state[k])?.name||state[k])} &times;</button>`).join('');$('#filters').innerHTML=state.view==='gates'?`<input id="gate-search" type="search" aria-label="Find a gate" placeholder="Find a gate or condition…" value="${esc(state.search||'')}"><select id="gate-family" aria-label="Gate family"><option value="">All gate families</option>${[...new Set(gates().map(g=>g.id.split(':')[0].split('.').slice(0,2).join('.')))].sort().map(f=>`<option value="${esc(f)}" ${state.family===f?'selected':''}>${esc(label(f))}</option>`).join('')}</select><select id="gate-source" aria-label="Gate source"><option value="">All sources</option>${sources().map(s=>`<option value="${esc(s.id)}" ${state.source===s.id?'selected':''}>${esc(label(s.name))}</option>`).join('')}</select><button class="link-button" data-attention aria-pressed="${state.attention==='1'}">${state.attention==='1'?'✓ ':''}Needs attention</button>`:'';$('#message').innerHTML=(selectionError?`<div class="notice">${esc(selectionError)}</div>`:'')+(data.reason?`<div class="notice">Current status cannot be verified: ${esc(label(data.reason))}. Last checked ${esc(utc(data.checked_at))}.</div>`:'')+(data.history_limited?'<div class="notice">History coverage is incomplete: a record or cache limit prevented a complete replay. The current observation remains available.</div>':'')+(data.history_loading?'<div class="muted" style="font-size:11px;margin-bottom:10px">Current observation is ready. Earlier history is loading independently.</div>':'')+(state.view==='recovery'?`<div class="note">Core-law consecutive clear window: ${duration(data.consecutive_clear_seconds)} / 72h · ${data.consecutive_clear_slots} distinct slots. ${data.not_due_slots} slots include an archive not yet due.</div>`:'');renderContent();renderDetail()}
document.addEventListener('click',e=>{const b=e.target.closest('button');if(!b)return;if(b.dataset.view)select({view:b.dataset.view});else if(b.dataset.source)select({source:b.dataset.source,projection:null,gate:null});else if(b.dataset.projection){const p=projection(b.dataset.projection);select({source:p?.source_key,projection:p?.id,gate:null})}else if(b.dataset.gate)select({gate:b.dataset.gate,view:'gates'});else if(b.dataset.gateDay){const start=b.dataset.day+'T00:00:00Z',end=new Date(Math.min(Date.parse(data.checked_at)||Date.now(),Date.parse(start)+86400000)).toISOString();select({gate:b.dataset.gateDay,from:start,to:end})}else if(b.hasAttribute('data-info')){const on=b.parentElement.classList.toggle('open');b.setAttribute('aria-expanded',String(on))}else if(b.hasAttribute('data-close'))select({projection:null,gate:null,source:null});else if(b.dataset.clear)select({[b.dataset.clear]:null});else if(b.dataset.sourceGates)select({view:'gates',source:b.dataset.sourceGates});else if(b.dataset.range){const end=new Date(data.checked_at||Date.now());select({from:new Date(end-Number(b.dataset.range)*3600000).toISOString(),to:end.toISOString()})}else if(b.hasAttribute('data-attention'))select({attention:state.attention==='1'?null:'1'});else if(b.dataset.historyMore)loadHistory(b.dataset.historyMore,true);else if(b.hasAttribute('data-more')&&state.gate)loadHistory(state.gate,true)});
document.addEventListener('input',e=>{if(e.target.id==='gate-search'){state.search=e.target.value;history.replaceState(null,'',location.pathname+'?'+new URLSearchParams(state));renderContent()}});document.addEventListener('change',e=>{if(e.target.id==='history-metric')select({metric:e.target.value});if(e.target.id==='gate-family')select({family:e.target.value});if(e.target.id==='gate-source')select({source:e.target.value,projection:null})});
window.addEventListener('resize',()=>requestAnimationFrame(drawWires));window.addEventListener('popstate',()=>{for(const k of Object.keys(state))delete state[k];Object.assign(state,Object.fromEntries(new URLSearchParams(location.search)));state.view=state.view||'sources';render()});
async function refresh(){if(refreshing)return;refreshing=true;const controller=new AbortController(),timeout=setTimeout(()=>controller.abort(),5000);try{const response=await fetch('/law.json',{signal:controller.signal});if(!response.ok)throw Error();const latest=await response.json();if(catalogKey!==latest.catalog_key){const catalogResponse=await fetch('/law/catalog.json',{signal:controller.signal});if(!catalogResponse.ok)throw Error();const candidate=await catalogResponse.json();if(candidate?.version!==latest.last_report?.catalog_version)throw Error('catalog_report_mismatch');cachedCatalog=candidate;catalogKey=latest.catalog_key}data={...latest,catalog:cachedCatalog,gate_days:data?.last_report?.catalog_version===latest.last_report?.catalog_version?data?.gate_days:undefined};overviewKey=null;receivedAt=performance.now();receivedAge=data.age_seconds;for(const [key,value] of histories)if(value.loading)histories.delete(key);const invalidTime=(state.from||state.to)&&(!state.from||!state.to||!Number.isFinite(Date.parse(state.from))||!Number.isFinite(Date.parse(state.to))||Date.parse(state.from)>=Date.parse(state.to)||Date.parse(state.to)-Date.parse(state.from)>30*86400000);if(invalidTime){selectionError='Invalid UTC interval in this URL.';delete state.from;delete state.to}const invalid=(state.source&&!source(state.source))||(state.projection&&!projection(state.projection))||(state.gate&&!descriptor(state.gate));if(invalid){selectionError='Unknown catalog selection in this URL.';for(const k of ['source','projection','gate'])delete state[k]}render()}catch{if(data){data={...data,status:'UNKNOWN',reason:'observation_service_unavailable'};render()}else{$('#message').textContent='Observation service unavailable. No current status can be verified.';$('#health').textContent='Core law · unknown'}}finally{clearTimeout(timeout);refreshing=false}}
refresh();setInterval(()=>{if(data&&typeof receivedAge==='number'&&receivedAge+(performance.now()-receivedAt)/1000>120&&data.reason!=='stale_report'){data={...data,status:'UNKNOWN',reason:'stale_report',age_seconds:receivedAge+(performance.now()-receivedAt)/1000};render()}},1000);setInterval(refresh,30000);
</script></body></html>'''

if __name__ == '__main__':
    raise SystemExit(main())
