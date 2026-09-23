"""Bounded observations of the partitions and rows current readers actually select."""
from __future__ import annotations

import json
import logging
import time
from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC, date, datetime, timedelta
from importlib.metadata import version
from pathlib import Path
from typing import TYPE_CHECKING, Literal, TypedDict, cast

from origo.sources.contracts import Client, RevisionedSourceSpec, RolloutStage, Row, identifier
from origo.sources.registry import SOURCE_REGISTRY
from origo.workers.depth import DEPTH_SPECS

if TYPE_CHECKING:
    from origo.law_catalog import GateEvaluation, ProjectionObservation

LAW_SCHEMA_VERSION = 1
LAW_TAPE_ROOT = Path('/var/lib/origo-law')
LAW_SAMPLE_NAME = 'samples-%Y-%m-%d.jsonl'
LAW_INVENTORY = (
    'binance_spot_trades', 'binance_spot_aggtrades',
    'binance_perp_trades', 'binance_perp_aggtrades',
    'binance_spot_depth20_1m', 'binance_spot_depth200_1m',
)
LAW_ANCHORS = {
    'binance_spot_trades': date(2017, 8, 17),
    'binance_spot_aggtrades': date(2017, 8, 17),
    'binance_perp_trades': date(2019, 9, 8),
    'binance_perp_aggtrades': date(2019, 12, 31),
}
R1_SPOT_BUDGET_SECONDS = 180
R1_PERP_BUDGET_SECONDS = 300
C1_SPOT_DEADLINE = (4, 30)
C1_PERP_DEADLINE = (10, 30)
D1_EXPECTED_SLOTS = 1440
D1_MAX_MISSING_SLOTS = 2
CANONICAL_COMPONENTS = ('raw', 'time', 'dollar', 'volume', 'tick', 'imbalance', 'aligned')
PROVISIONAL_COMPONENTS = ('raw_latest', 'time_latest', 'dollar_latest')
LAW_QUERY_SETTINGS = {
    'max_memory_usage': 536870912,
    'max_execution_time': 5,
    'max_threads': 1,
    'read_overflow_mode': 'throw',
    'timeout_overflow_mode': 'throw',
}
LAW_EVALUATION_TIMEOUT_SECONDS = 20
LawStatus = Literal['PASS', 'FAIL', 'UNKNOWN', 'NOT_DUE']
OverallStatus = Literal['PASS', 'FAIL', 'UNKNOWN']
LawPredicate = Literal['R1', 'C1', 'C2', 'D1', 'inventory']
Scalar = str | int | float | bool | None


class PredicateReport(TypedDict):
    status: LawStatus
    reason: str
    evidence: dict[str, Scalar]


class FeedReport(TypedDict):
    source_key: str
    predicates: dict[LawPredicate, PredicateReport]


class LawReport(TypedDict):
    schema_version: int
    application_version: str
    sampling_slot: str
    evaluation_start: str
    evaluation_end: str
    inventory: list[str]
    status: OverallStatus
    feeds: list[FeedReport]
    catalog_version: str
    deployed_sha: str
    projections: list[ProjectionObservation]
    gates: list[GateEvaluation]


def _utc(value: object) -> datetime:
    if not isinstance(value, datetime):
        raise ValueError('Expected an evidence timestamp.')
    return value.replace(tzinfo=UTC) if value.tzinfo is None else value.astimezone(UTC)


def _result(status: LawStatus, reason: str, **evidence: Scalar) -> PredicateReport:
    return {'status': status, 'reason': reason, 'evidence': evidence}


class _Queries:
    def __init__(self, client: Client) -> None:
        self.client = client
        self.deadline = time.monotonic() + LAW_EVALUATION_TIMEOUT_SECONDS

    def __call__(self, query: str, params: dict[str, object]) -> list[Row]:
        remaining = self.deadline - time.monotonic()
        if remaining <= 0:
            raise TimeoutError('Law evaluation deadline exhausted.')
        rows = self.client.execute(query, params, settings={
            **LAW_QUERY_SETTINGS, 'max_execution_time': min(5.0, remaining),
        })
        if time.monotonic() >= self.deadline:
            raise TimeoutError('Law evaluation deadline exhausted.')
        return rows


@dataclass(frozen=True)
class _Proof:
    key: str
    provisional: bool
    start: datetime
    end: datetime
    revision: str
    build: str
    hashes: dict[str, str]
    components: tuple[tuple[str, int, str, datetime], ...]

    @classmethod
    def read(cls, row: Row) -> _Proof:
        raw: object = json.loads(str(row[6]))
        if not isinstance(raw, list):
            raise ValueError('Invalid activation hashes.')
        values = cast(list[object], raw)
        for pair in values:
            if not isinstance(pair, list):
                raise ValueError('Invalid activation hash pair.')
            items = cast(list[object], pair)
            if len(items) != 2 or not all(isinstance(value, str) for value in items):
                raise ValueError('Invalid activation hash pair.')
        pairs = cast(list[list[str]], raw)
        hashes = dict(pairs)
        if len(hashes) != len(pairs):
            raise ValueError('Duplicate activation hashes.')
        proofs = cast(list[tuple[object, ...]], row[7])
        return cls(str(row[0]), bool(row[1]), _utc(row[2]), _utc(row[3]), str(row[4]),
                   str(row[5]), hashes, tuple(
                       (str(p[0]), int(str(p[1])), str(p[2]), _utc(p[3])) for p in proofs
                   ))

    def component(self, key: str) -> tuple[str, int, str, datetime] | None:
        matches = [item for item in self.components if item[0] == key]
        return matches[0] if len(matches) == 1 and matches[0][2] == self.hashes.get(key) else None

    def verdict(self) -> PredicateReport:
        expected = PROVISIONAL_COMPONENTS if self.provisional else CANONICAL_COMPONENTS
        if set(self.hashes) != set(expected) or len(self.components) != len(expected):
            return _result('UNKNOWN', 'proof_inventory_invalid')
        if any(self.component(key) is None for key in expected):
            return _result('UNKNOWN', 'proof_identity_or_hash_invalid')
        raw = self.component('raw_latest' if self.provisional else 'raw')
        if raw is None:
            raise ValueError('Validated proof lost its raw component.')
        return _result('PASS' if raw[1] > 0 else 'FAIL', 'active_proof' if raw[1] else 'raw_proof_empty',
                       raw_proof_rows=raw[1], **{f'hash_{key}': value for key, value in self.hashes.items()})


def _proofs(query: _Queries, database: str, source: str, build: str) -> list[_Proof]:
    rows = query(f"""SELECT a.partition_key, a.provisional, a.partition_start, a.partition_end,
        a.revision, a.build_id, a.component_hashes, p.proofs
        FROM {database}.source_active_partitions a LEFT JOIN (
            SELECT source_key, partition_key, provisional, revision, build_id,
                groupArray((component, row_count, content_hash, completed_at)) AS proofs
            FROM {database}.source_component_log
            WHERE source_key=%(source)s AND (NOT provisional OR toString(build_id)=%(build)s)
            GROUP BY source_key, partition_key, provisional, revision, build_id
        ) p USING (source_key, partition_key, provisional, revision, build_id)
        WHERE source_key=%(source)s AND (NOT provisional OR toString(build_id)=%(build)s)""",
        {'source': source, 'build': build})
    return [_Proof.read(row) for row in rows]


def _edge(query: _Queries, database: str, source: str, now: datetime) -> list[Row]:
    return query(f"""WITH current AS (
            SELECT * FROM {database}.source_current_partitions WHERE source_key=%(source)s
        ), edge AS (SELECT max(partition_end) AS reader_end FROM current)
        SELECT c.partition_key, c.provisional, c.revision, c.build_id, edge.reader_end,
            least(toStartOfMinute(edge.reader_end), toStartOfMinute(toDateTime(%(now)s, 'UTC'))) - INTERVAL 1 MINUTE AS minute
        FROM current c CROSS JOIN edge
        WHERE c.partition_start <= minute AND c.partition_end >= minute + INTERVAL 1 MINUTE""",
        {'source': source, 'now': now})


def _r1(query: _Queries, database: str, spec: RevisionedSourceSpec, now: datetime,
        edges: list[Row], proofs: list[_Proof]) -> PredicateReport:
    budget = R1_PERP_BUDGET_SECONDS if '_perp_' in spec.key else R1_SPOT_BUDGET_SECONDS
    if not edges:
        return _result('FAIL', 'reader_empty', reader_end=None, budget_seconds=budget)
    if len(edges) != 1:
        return _result('UNKNOWN', 'reader_identity_ambiguous', budget_seconds=budget)
    key, provisional, revision, build, end, minute = edges[0]
    selected = [p for p in proofs if (p.key, p.provisional, p.revision, p.build) ==
                (str(key), bool(provisional), str(revision), str(build))]
    evidence: dict[str, Scalar] = dict(
        reader_end=_utc(end).isoformat(), age_seconds=max(0.0, (now - _utc(end)).total_seconds()),
        budget_seconds=budget, selected_minute=_utc(minute).isoformat(), partition_key=str(key),
        provisional=bool(provisional), revision=str(revision), build_id=str(build), row_count=None,
    )
    if len(selected) != 1:
        return _result('UNKNOWN', 'active_proof_missing', **evidence)
    proof = selected[0].verdict()
    evidence.update(proof['evidence'])
    if proof['status'] != 'PASS':
        return _result(proof['status'], proof['reason'], **evidence)
    raw = next(component for component in spec.components
               if component.key == ('raw_latest' if provisional else 'raw'))
    rows = query(f"""SELECT count() FROM {database}.{identifier(spec.names.prefix)}_{identifier(raw.key)}_revisions
        WHERE source_date=%(date)s AND partition_key=%(key)s AND revision=%(revision)s
          AND build_id=%(build)s AND {identifier(raw.time_column)}>=%(minute)s
          AND {identifier(raw.time_column)}<%(end)s""", {
        'date': _utc(minute).date(), 'key': str(key), 'revision': str(revision), 'build': build,
        'minute': _utc(minute), 'end': _utc(minute) + timedelta(minutes=1),
    })
    evidence['row_count'] = int(str(rows[0][0]))
    if not evidence['row_count']:
        return _result('FAIL', 'reader_minute_empty', **evidence)
    stale = (now - _utc(end)).total_seconds() > budget
    return _result('FAIL' if stale else 'PASS', 'reader_stale' if stale else 'reader_current', **evidence)


def _calendar(proofs: list[_Proof], day: date) -> PredicateReport:
    start = datetime.combine(day, datetime.min.time(), UTC)
    matches = [p for p in proofs if not p.provisional and
               (p.key == day.isoformat() or p.start.date() == day)]
    if not matches:
        return _result('FAIL', 'canonical_day_missing')
    if len(matches) != 1 or matches[0].key != day.isoformat() or matches[0].start != start or matches[0].end != start + timedelta(days=1):
        return _result('UNKNOWN', 'canonical_day_bounds_invalid')
    return matches[0].verdict()


def _c1(proofs: list[_Proof], source: str, now: datetime) -> PredicateReport:
    hour, minute = C1_PERP_DEADLINE if '_perp_' in source else C1_SPOT_DEADLINE
    deadline = now.replace(hour=hour, minute=minute, second=0, microsecond=0)
    day = now.date() - timedelta(days=1)
    report = _calendar(proofs, day)
    if report['reason'] == 'canonical_day_missing' and now < deadline:
        report = _result('NOT_DUE', 'archive_not_due')
    report['evidence'].update(day=day.isoformat(), deadline=deadline.isoformat())
    return report


def _c2(query: _Queries, database: str, proofs: list[_Proof], source: str, now: datetime) -> PredicateReport:
    anchor = LAW_ANCHORS[source]
    rows = query(f'SELECT anchor FROM {database}.source_anchor_log WHERE source_key=%(source)s', {'source': source})
    stored = _utc(rows[0][0]) if len(rows) == 1 else None
    expected = max(0, (now.date() - timedelta(days=1) - anchor).days)
    evidence: dict[str, Scalar] = dict(anchor=anchor.isoformat(), stored_anchor=stored.isoformat() if stored else None,
        expected_days=expected, valid_days=0, first_invalid_day=None)
    if stored != datetime.combine(anchor, datetime.min.time(), UTC):
        return _result('UNKNOWN', 'anchor_mismatch', **evidence)
    valid = 0
    first: PredicateReport | None = None
    # Calendar membership is independent of the reader's provisional clipping algorithm.
    by_day: dict[date, list[_Proof]] = {}
    for proof in proofs:
        if not proof.provisional:
            by_day.setdefault(proof.start.date(), []).append(proof)
    for offset in range(expected):
        day = anchor + timedelta(days=offset)
        result = _calendar(by_day.get(day, []), day)
        if result['status'] == 'PASS':
            valid += 1
        elif first is None:
            first = result
            evidence['first_invalid_day'] = day.isoformat()
        elif result['status'] == 'FAIL' and first['status'] != 'FAIL':
            first = result
    evidence['valid_days'] = valid
    return _result(first['status'] if first else 'PASS', first['reason'] if first else 'canonical_calendar_complete', **evidence)


def _projections(spec: RevisionedSourceSpec, proofs: list[_Proof], now: datetime, r1: PredicateReport) -> list[ProjectionObservation]:
    observations: list[ProjectionObservation] = []
    for component in spec.components:
        record = max((p for p in proofs if p.provisional == component.provisional), key=lambda p: p.end, default=None)
        proof = record.component(component.key) if record else None
        budget = R1_PERP_BUDGET_SECONDS if '_perp_' in spec.key else R1_SPOT_BUDGET_SECONDS
        current = record is not None and (
            (now - record.end).total_seconds() <= budget if component.provisional
            else record.end >= now.replace(hour=0, minute=0, second=0, microsecond=0)
        )
        waiting = (not component.provisional and record is not None
                   and record.end == now.replace(hour=0, minute=0, second=0, microsecond=0) - timedelta(days=1)
                   and _c1(proofs, spec.key, now)['status'] == 'NOT_DUE')
        observation: ProjectionObservation = {
            'id': f'{spec.key}:{component.key}', 'status': 'UNKNOWN' if proof is None else 'FAILED' if proof[1] == 0 else 'CURRENT' if current else 'WAITING' if waiting else 'STALE',
            'observed_at': now.isoformat(), 'evidence_at': proof[3].isoformat() if proof else None,
            'evidence_id': f'{record.build}:{component.key}:{proof[2]}' if record and proof else None,
            'data_through': record.end.isoformat() if record and proof else None,
            'reason': 'validated_activation' if proof else 'component_proof_missing',
            'gate_ids': [], 'dagit_url': None,
        }
        if (record is not None and component.key in ('raw', 'raw_latest')
                and record.build == r1['evidence'].get('build_id')
                and r1['evidence'].get('row_count') is not None):
            observation.update(
                status='CURRENT' if r1['status'] == 'PASS' else 'STALE' if r1['reason'] == 'reader_stale' else 'FAILED',
                reason='physical_reader_edge' if r1['status'] == 'PASS' else r1['reason'],
                evidence_at=now.isoformat(),
                evidence_id=f"{record.build}:reader:{r1['evidence']['selected_minute']}:{now.isoformat()}",
                gate_ids=[f'law.R1:{spec.key}'],
            )
        observations.append(observation)
    return observations


def _trade(query: _Queries, database: str, spec: RevisionedSourceSpec, now: datetime) -> tuple[FeedReport, list[ProjectionObservation]]:
    if {c.key for c in spec.components if not c.provisional} != set(CANONICAL_COMPONENTS) or {c.key for c in spec.components if c.provisional} != set(PROVISIONAL_COMPONENTS):
        return {'source_key': spec.key, 'predicates': {name: _result('UNKNOWN', 'profile_mismatch') for name in ('R1', 'C1', 'C2')}}, []
    edges = _edge(query, database, spec.key, now)
    proofs = _proofs(query, database, spec.key, str(edges[0][3]) if len(edges) == 1 else '')
    def observed(call: Callable[[], PredicateReport]) -> PredicateReport:
        try:
            return call()
        except Exception as error:
            logging.getLogger(__name__).exception('Law predicate failed for %s', spec.key)
            return _result('UNKNOWN', 'evaluation_timeout' if isinstance(error, TimeoutError) else 'evidence_unavailable')

    predicates: dict[LawPredicate, PredicateReport] = {
        'C1': _c1(proofs, spec.key, now),
        'R1': observed(lambda: _r1(query, database, spec, now, edges, proofs)),
        'C2': observed(lambda: _c2(query, database, proofs, spec.key, now)),
    }
    return {'source_key': spec.key, 'predicates': predicates}, _projections(spec, proofs, now, predicates['R1'])


def _depth(query: _Queries, database: str, source: str, now: datetime) -> FeedReport:
    end = now.replace(second=0, microsecond=0)
    rows = query(f"""SELECT uniqExact(datetime), maxOrNull(datetime) FROM {database}.{identifier(source)}
        WHERE datetime>=%(start)s AND datetime<%(end)s AND toStartOfMinute(datetime)=datetime""",
        {'start': end - timedelta(minutes=D1_EXPECTED_SLOTS), 'end': end})
    missing = D1_EXPECTED_SLOTS - int(str(rows[0][0]))
    newest = _utc(rows[0][1]).isoformat() if rows[0][1] is not None else None
    return {'source_key': source, 'predicates': {'D1': _result(
        'PASS' if missing <= D1_MAX_MISSING_SLOTS else 'FAIL',
        'depth_complete' if missing <= D1_MAX_MISSING_SLOTS else 'depth_minutes_missing',
        newest_minute=newest, expected_slots=D1_EXPECTED_SLOTS, missing_slots=missing)}}


def evaluate(client: Client, database: str, now: datetime) -> LawReport:
    now = _utc(now)
    database = identifier(database)
    query = _Queries(client)
    specs = {spec.key: spec for spec in SOURCE_REGISTRY}
    inventory = list(LAW_INVENTORY) + sorted(spec.key for spec in SOURCE_REGISTRY
        if spec.rollout_stage == RolloutStage.LIVE and spec.key not in LAW_INVENTORY)
    depths = {spec.projection_table_name for spec in DEPTH_SPECS}
    feeds: list[FeedReport] = []
    projections: list[ProjectionObservation] = []
    feed: FeedReport
    for source in inventory:
        names: tuple[LawPredicate, ...] = ('R1', 'C1', 'C2') if source in LAW_ANCHORS else ('D1',)
        try:
            if source in LAW_ANCHORS and source in specs:
                feed, observations = _trade(query, database, specs[source], now)
                projections.extend(observations)
            elif source in depths and source in LAW_INVENTORY:
                feed = _depth(query, database, source, now)
            else:
                feed = {'source_key': source, 'predicates': {'inventory': _result('UNKNOWN', 'inventory_uncovered')}}
        except Exception as error:
            logging.getLogger(__name__).exception('Law observation failed for %s', source)
            reason = 'evaluation_timeout' if isinstance(error, TimeoutError) or time.monotonic() >= query.deadline else 'evidence_unavailable'
            feed = {'source_key': source, 'predicates': {name: _result('UNKNOWN', reason) for name in names}}
        feeds.append(feed)
    statuses = {predicate['status'] for feed in feeds for predicate in feed['predicates'].values()}
    status: OverallStatus = 'FAIL' if 'FAIL' in statuses else 'UNKNOWN' if 'UNKNOWN' in statuses else 'PASS'
    return {
        'schema_version': LAW_SCHEMA_VERSION, 'application_version': version('origo'),
        'sampling_slot': now.replace(second=0, microsecond=0).isoformat(),
        'evaluation_start': now.isoformat(), 'evaluation_end': (now + timedelta(seconds=max(0, LAW_EVALUATION_TIMEOUT_SECONDS - (query.deadline - time.monotonic())))).isoformat(),
        'inventory': inventory, 'status': status, 'feeds': feeds,
        'catalog_version': '', 'deployed_sha': '', 'projections': projections, 'gates': [],
    }
