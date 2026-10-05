"""Recorded operational evidence, shared by the law page and notification mail."""
from __future__ import annotations

import base64
import hashlib
import json
import math
import struct
import zlib
from collections.abc import Sequence
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from typing import Literal, NotRequired, TypeAlias, TypedDict, cast

Json: TypeAlias = None | bool | int | float | str | list['Json'] | dict[str, 'Json']
Document: TypeAlias = dict[str, Json]
SummaryStatus = Literal['PASS', 'FAIL', 'UNKNOWN', 'EXPECTED_WAIT']
Lifecycle = Literal['new', 'ongoing', 'recovered', 'reopened', 'expected_wait', 'historical_events']
Verification = Literal['verified', 'unverified', 'expected_wait']


class Coverage(TypedDict):
    window_start: str | None
    window_end: str | None
    observed_slots: int
    expected_slots: int
    complete: bool
    reason: str


class Measurement(TypedDict):
    name: str
    value: int | float | None
    unit: str
    threshold: int | float | None
    observed_at: str
    definition_version: str
    window_start: NotRequired[str | None]
    window_end: NotRequired[str | None]


class Trend(TypedDict):
    value: int | float | None
    unit: str
    current_at: str | None
    previous_at: str | None
    description: str
    unavailable_reason: str | None


class SummaryCard(TypedDict):
    id: str
    label: str
    value: str
    unit: str
    status: SummaryStatus
    badge: str
    explanation: str
    coverage: Coverage
    trend: Trend
    target: Document


class Incident(TypedDict):
    group_id: str
    kind: Literal['condition', 'event_stream']
    lifecycle: Lifecycle
    status: SummaryStatus
    had_eligible_failure: bool
    verification: Verification
    label: str
    first_seen: str | None
    last_seen: str | None
    observations: int
    coverage: Coverage
    measurements: list[Measurement]
    trend: Trend
    evidence_refs: list[str]
    transitions: NotRequired[list[str]]
    scope_count: NotRequired[int]
    recovery_pending: NotRequired[bool]
    failing_minutes: NotRequired[int]
    display_lines: NotRequired[list[str]]


class OperatorSummary(TypedDict):
    schema_version: int
    sampling_slot: str | None
    catalog_version: str | None
    observed_at: str | None
    status: SummaryStatus
    cards: list[SummaryCard]
    incidents: list[Incident]
    coverage: Coverage
    omitted_groups: int | None
    delivery_status: str
    prepared_at: NotRequired[str]
    law_observed: NotRequired[bool]
    law_sample_ref: NotRequired[str | None]


class NotificationObservation(TypedDict):
    group_id: str
    check: str
    scope: str
    kind: Literal['condition', 'event_stream']
    status: Literal['FAIL', 'UNKNOWN', 'PASS', 'EXPECTED_WAIT', 'NO_NEW_EVENTS']
    definition_version: str
    detector_keys: list[str]
    eligible: bool | None
    read_window_key: str | None
    measurements: list[Measurement]
    evidence_refs: list[str]
    complete: bool


class ObservedRead(TypedDict):
    window_start: str | None
    window_end: str | None
    count: int | None
    counts_limited: bool
    complete: bool


class ObservationFrame(TypedDict):
    sampling_slot: str
    catalog_version: str | None
    law_sample_ref: str | None
    law_observed: bool
    checks_complete: dict[str, bool]
    read_windows: dict[str, ObservedRead]
    observations: list[NotificationObservation]
    omitted_groups: int | None
    complete: bool
    notification_transitions: NotRequired[list['NotificationTransition']]


class NotificationTransition(TypedDict):
    group_id: str
    sampling_slot: str
    lifecycle: Lifecycle
    verification: Verification
    description: str


LAW_NAMES: dict[str, str] = {
    'R1': 'Live source data is fresh',
    'C1': 'Authoritative archives are ready',
    'C2': 'Historical coverage is complete',
    'D1': 'Depth minutes are complete',
    'M1': 'Market state cube is fresh',
    'M2': 'Market state history is complete',
}
LAW_COPY: dict[str, str] = {
    'R1': 'Checks the selected reader frontier and actual rows in its closed minute.',
    'C1': 'Checks daily archive deadlines and hourly provider availability, activation and original reader proofs.',
    'C2': 'Checks the historical calendar and anchor using canonical proofs, not a physical full-history scan.',
    'D1': 'Counts distinct depth minutes in the checked rolling day, with the recorded grace and missing-minute allowance.',
    'M1': 'Checks activated cube evidence and matching trade and taker-buy counts in the selected reader window.',
    'M2': 'Checks activated market state proofs for every due canonical day from 2021-01-01. Missing migration coverage remains visible.',
}
THEME: dict[str, str] = {
    'ink': '#172b36', 'muted': '#687780', 'line': '#dce3e4', 'paper': '#f5f7f6',
    'green': '#187862', 'red': '#b84236', 'amber': '#946414', 'blue': '#356e9b',
}
MAX_SUMMARY_BYTES = 16 * 1024
OPERATIONS = (('error_lines', 'monitor.no_error_logs'), ('failed_receipts', 'monitor.workers_alive'))
OPERATION_RECORD = struct.Struct('>qqq?qqq?')
_CHECK_NAMES = {
    'workers_alive': 'Worker heartbeats', 'queue_bounded': 'Queued runs',
    'collectors_serving': 'Collectors serving', 'publication_current': 'Published outputs',
    'no_error_logs': 'Error lines', 'dagster_reachable': 'Dagit availability',
    'run_failure': 'Failed runs', 'check_failed': 'Failed asset checks',
    'failed_receipts': 'Failed worker receipts', 'error_logs': 'Error lines',
    'data_current': 'Data-law evidence',
}


def _object(value: Json) -> Document:
    return value if isinstance(value, dict) else {}


def _objects(value: Json) -> list[Document]:
    return [item for item in value if isinstance(item, dict)] if isinstance(value, list) else []


def _text(value: Json) -> str | None:
    return value if isinstance(value, str) else None


def _number(value: Json) -> float | None:
    return float(value) if isinstance(value, (int, float)) and not isinstance(value, bool) and math.isfinite(value) else None


def _instant(value: Json) -> datetime:
    if not isinstance(value, str) or len(value) > 40:
        raise ValueError('invalid_utc_time')
    stamp = datetime.fromisoformat(value.replace('Z', '+00:00'))
    if stamp.tzinfo is None or stamp.utcoffset() != timedelta(0):
        raise ValueError('invalid_utc_time')
    return stamp


def duration(value: float | int | None) -> str:
    if value is None:
        return '—'
    if abs(value) < 60:
        return f'{math.floor(value + .5)}s'
    if abs(value) < 3600:
        return f'{math.floor(value / 60 + .5)}m'
    return f'{value / 3600:.1f}h'


def _shown(value: Json) -> str:
    number = _number(value)
    return '—' if number is None else f'{number:,.0f}' if number.is_integer() else f'{number:,.3f}'.rstrip('0').rstrip('.')


def _utc(value: str | None) -> str:
    return _instant(value).strftime('%Y-%m-%d %H:%M:%S UTC') if value else 'Not observed'


def badge(status: SummaryStatus) -> str:
    return f"{'●' if status == 'PASS' else '!' if status == 'FAIL' else '◌'} {status.replace('_', ' ').lower()}"


def unavailable_trend(reason: str = 'comparison_not_recorded') -> Trend:
    return {'value': None, 'unit': '', 'current_at': None, 'previous_at': None,
            'description': 'Trend unavailable', 'unavailable_reason': reason}


def _coverage(value: Document) -> Coverage:
    return {'window_start': _text(value.get('window_start')), 'window_end': _text(value.get('window_end')),
            'observed_slots': int(_number(value.get('observed_slots')) or 0),
            'expected_slots': int(_number(value.get('expected_slots')) or 0),
            'complete': value.get('complete') is True, 'reason': str(value.get('reason') or 'history_unavailable')}


def coverage_text(value: Coverage) -> str:
    return (f"{value['observed_slots']:,} / {value['expected_slots']:,} samples"
            f"{' · incomplete' if not value['complete'] else ''} · through {_utc(value['window_end'])}")


def repeat_text(incident: Incident) -> str:
    count = incident['observations']
    if incident['first_seen'] is not None and incident['coverage']['reason'] == 'complete_episode':
        return f'Observed in {count} minute checks; repeated {max(0, count - 1)} times after the first observation'
    return f'Observed in {count} minute checks; start/repeat baseline unavailable'


def incident_rank(incident: Incident) -> tuple[int, int, int, str]:
    state = (0 if incident['status'] == 'FAIL' else 1 if incident['status'] == 'UNKNOWN'
             else 2 if incident['status'] == 'EXPECTED_WAIT' else 3 if incident['lifecycle'] == 'recovered' else 4)
    lifecycle = {'reopened': 0, 'new': 1, 'ongoing': 2}.get(incident['lifecycle'], 3)
    return state, lifecycle, -incident.get('scope_count', 1), incident['group_id']


def _rollup(statuses: Sequence[SummaryStatus]) -> SummaryStatus:
    return ('FAIL' if 'FAIL' in statuses else 'UNKNOWN' if not statuses or 'UNKNOWN' in statuses
            else 'EXPECTED_WAIT' if 'EXPECTED_WAIT' in statuses else 'PASS')


def _status(value: Json) -> SummaryStatus:
    if value in ('PASS', 'CURRENT'):
        return 'PASS'
    if value in ('FAIL', 'FAILED', 'STALE'):
        return 'FAIL'
    if value in ('EXPECTED_WAIT', 'NOT_DUE', 'WAITING', 'INACTIVE'):
        return 'EXPECTED_WAIT'
    return 'UNKNOWN'


def _publication_status(observation: Document) -> SummaryStatus:
    policy = _object(observation.get('publication_policy'))
    if observation.get('status') == 'FAILED' or policy.get('reason') in ('stale', 'unreadable'):
        return 'FAIL'
    if observation.get('status') == 'UNKNOWN' or not policy or policy.get('reason') == 'unknown':
        return 'UNKNOWN'
    if observation.get('status') in ('CURRENT', 'INACTIVE'):
        return _status(observation.get('status'))
    if policy.get('reason') in ('backfill_active', 'within_budget'):
        return 'EXPECTED_WAIT'
    if policy.get('reason') == 'not_applicable':
        return 'FAIL' if observation.get('status') == 'STALE' and observation.get('reason') != 'publication_state_changed' else 'EXPECTED_WAIT'
    return 'UNKNOWN'


def _card(key: str, label: str, value: str, status: SummaryStatus, explanation: str,
          coverage: Coverage, *, unit: str = '') -> SummaryCard:
    return {'id': key, 'label': label, 'value': value, 'unit': unit, 'status': status,
            'badge': badge(status), 'explanation': explanation, 'coverage': coverage,
            'trend': unavailable_trend(), 'target': {'view': 'overview', 'overview': key}}


def _cards(current: Document, catalog: Document, coverage: Coverage) -> list[SummaryCard]:
    report = _object(current.get('last_report'))
    unavailable = bool(current.get('reason'))
    sources = {str(item.get('id')): item for item in _objects(catalog.get('sources'))}
    descriptors = {str(item.get('id')): item for item in _objects(catalog.get('gates'))}
    feeds = {str(item.get('source_key')): item for item in _objects(report.get('feeds'))}
    operational_gates = _objects(current.get('operational_gates'))
    operations_unavailable = unavailable and (not operational_gates or current.get('evidence_expired') is True)
    events = {str(item.get('gate_id')): item for item in [*_objects(report.get('gates')), *operational_gates]}
    ids = catalog.get('law_gate_ids')
    required = [item for item in ids if isinstance(item, str)] if isinstance(ids, list) else []
    cards: list[SummaryCard] = []
    for family, title in LAW_NAMES.items():
        members = [key for key in required if key.startswith(f'law.{family}:')]
        results = [_object(_object(feeds.get(key.split(':', 1)[1], {}).get('predicates')).get(family))
                   if key in descriptors and not unavailable else {} for key in members]
        states: list[SummaryStatus] = [_status(result.get('status')) for result in results]
        values = [_object(result.get('evidence')) for result in results]
        passed, waiting, failed, unknown = (states.count(state) for state in ('PASS', 'EXPECTED_WAIT', 'FAIL', 'UNKNOWN'))
        value = f'Unknown / {len(members) or "—"} expected' if unknown == len(members) else f'{passed} / {len(members)}'
        reason = 'Canonical calendar and anchor proofs'
        if family in ('R1', 'M1'):
            ages = [_number(item.get('age_seconds')) for item in values]
            reason = f'Worst reader delay {duration(max(cast(list[float], ages)) if ages and None not in ages else None)}'
        elif family == 'C1':
            reason = f'{waiting} not due · {failed} overdue · {unknown} unknown'
        elif family == 'D1':
            missing = [_number(item.get('missing_slots')) for item in values]
            expected = [_number(item.get('expected_slots')) for item in values]
            value = (f'{_shown(sum(cast(list[float], expected)) - sum(cast(list[float], missing)))} / {_shown(sum(cast(list[float], expected)))}'
                     if values and None not in missing and None not in expected else 'Unknown / incomplete')
            reason = ' · '.join(f"{str(sources.get(key.split(':', 1)[1], {}).get('name') or key.split(':', 1)[1]).replace('_', ' ')}: {_shown(item.get('missing_slots'))} gaps" for key, item in zip(members, values)) or 'Depth evidence unavailable'
        cards.append(_card(family, title, value, _rollup(states), reason + '. ' + LAW_COPY[family], coverage,
                           unit='minutes' if family == 'D1' else 'members'))
    def evidence(check: str) -> Document:
        return {} if operations_unavailable else _object(events.get('monitor.' + check, {}).get('evidence'))
    def check_status(check: str) -> SummaryStatus:
        return 'UNKNOWN' if operations_unavailable else _status(events.get('monitor.' + check, {}).get('outcome'))
    def total(metric: str) -> tuple[str, Coverage, str]:
        item = _object(_object(_object(current.get('operations_history')).get(metric)).get('60m'))
        count = _number(item.get('count'))
        formatted = ('Not recorded' if count is None else f"{'Historical ' if operations_unavailable else ''}{'' if item.get('complete') else '≥ '}{_shown(count)}")
        cov = _coverage(item)
        detail = (f"{_shown(item.get('observed_slots'))} / {_shown(item.get('expected_slots'))} samples · {duration(_number(item.get('covered_seconds')))} covered"
                  f"{'' if item.get('complete') else ' · incomplete'} · through {_utc(cov['window_end'])}"
                  f"{' · latest read unavailable' if cov['reason'] == 'latest_read_unavailable' else ''}") if item else 'Not recorded in this observation'
        return formatted, cov, detail
    nodes = [node for source in sources.values() for node in _objects(source.get('projections')) if node.get('lane') == 'consumer']
    projections = {str(node.get('id')): node for node in _objects(report.get('projections'))}
    output_states = ['UNKNOWN' if unavailable else _publication_status(projections.get(str(node.get('id')), {})) for node in nodes]
    current_count, pending, failed, unknown = (output_states.count(state) for state in ('PASS', 'EXPECTED_WAIT', 'FAIL', 'UNKNOWN'))
    output_value = f'{"Unknown" if unavailable or unknown == len(nodes) else current_count} / {len(nodes) or "—"}'
    cards.append(_card('outputs', 'Published outputs', output_value, _rollup(cast(list[SummaryStatus], output_states)),
                       f'{pending} pending · {failed} failed · {unknown} unknown. Updates may be pending between publication ticks.', coverage, unit='outputs'))
    workers, queue, dagit, collectors = (evidence(name) for name in ('workers_alive', 'queue_bounded', 'dagster_reachable', 'collectors_serving'))
    receipt_count, receipt_coverage, receipt_detail = total('failed_receipts')
    cards.append(_card('workers', 'Worker heartbeats', f"{_shown(workers.get('workers_fresh'))} / {_shown(workers.get('workers_expected'))}", check_status('workers_alive'),
                       f"{_shown(workers.get('workers_unknown'))} unknown members · {receipt_count} failed receipts / 60m · {receipt_detail}. Heartbeats do not prove successful work.", receipt_coverage, unit='workers'))
    cards.append(_card('queue', 'Queued runs', _shown(queue.get('queued_runs')) if dagit.get('reachable') is True else 'Unknown',
                       _rollup([check_status('queue_bounded'), check_status('dagster_reachable')]),
                       f"Threshold {_shown(queue.get('queue_threshold'))} · {_shown(dagit.get('unhealthy_daemons'))} unhealthy daemons", coverage, unit='runs'))
    cards.append(_card('collectors', 'Collectors serving', f"{_shown(collectors.get('collectors_serving'))} / {_shown(collectors.get('collectors_expected'))}",
                       check_status('collectors_serving'), 'Last completed minute · existing monitor probes', coverage, unit='collectors'))
    error_count, error_coverage, error_detail = total('error_lines')
    cards.append(_card('errors', 'Error lines · observed / 60m', error_count, check_status('no_error_logs'), error_detail, error_coverage, unit='lines'))
    cards.append(_card('clear', 'Clear data-law observations', 'Unknown' if unavailable else f"{duration(_number(current.get('consecutive_clear_seconds')))} / 72h",
                       'UNKNOWN' if unavailable else _status(current.get('status')),
                       f"{_shown(current.get('consecutive_clear_slots'))} distinct samples · {_shown(current.get('not_due_slots'))} include archives not yet due", coverage, unit='hours'))
    return cards


def _frames(history: Sequence[ObservationFrame], now: datetime) -> list[ObservationFrame]:
    # Conflicting duplicate slots cannot count as successful evidence.
    unique: dict[str, ObservationFrame] = {}
    floor = now.replace(second=0, microsecond=0) - timedelta(hours=24)
    for frame in history:
        stamp = _instant(frame['sampling_slot'])
        if floor <= stamp <= now:
            previous = unique.get(frame['sampling_slot'])
            unique[frame['sampling_slot']] = frame if previous is None or previous == frame else {
                **frame, 'complete': False, 'law_observed': False, 'observations': [], 'checks_complete': {}, 'omitted_groups': None}
    return sorted(unique.values(), key=lambda row: row['sampling_slot'])


def _history_coverage(frames: Sequence[ObservationFrame], now: datetime) -> Coverage:
    end = now.replace(second=0, microsecond=0)
    start = end - timedelta(minutes=1439)
    covered = [frame for frame in frames if start <= _instant(frame['sampling_slot']) <= end and frame['complete']]
    complete = len(covered) == 1440
    return {'window_start': start.isoformat(), 'window_end': end.isoformat(), 'observed_slots': len(covered),
            'expected_slots': 1440, 'complete': complete, 'reason': 'complete' if complete else 'missing_or_incomplete_slots'}


def measurement_trend(current: NotificationObservation, current_at: str,
                      previous: NotificationObservation | None, previous_at: str | None) -> Trend:
    if previous is None or previous_at is None:
        return unavailable_trend('exact_hour_comparison_unavailable')
    if _instant(current_at) - _instant(previous_at) != timedelta(hours=1):
        return unavailable_trend('exact_hour_comparison_unavailable')
    if current['definition_version'] != previous['definition_version']:
        return unavailable_trend('incompatible_definition')
    if not current['complete'] or not previous['complete']:
        return unavailable_trend('incomplete_endpoints')
    family = current['check'].removeprefix('law.').split(':', 1)[0]
    metric = 'missing_slots' if family == 'D1' else 'age_seconds' if family in ('R1', 'M1') else ('valid_hours' if any(m['name'] == 'valid_hours' for m in current['measurements']) else 'valid_days') if family in ('C2', 'M2') else ''
    left = next((item for item in previous['measurements'] if item['name'] == metric), None)
    right = next((item for item in current['measurements'] if item['name'] == metric), None)
    if not metric or left is None or right is None or left['value'] is None or right['value'] is None:
        return unavailable_trend('measurement_not_recorded')
    if left['definition_version'] != right['definition_version']:
        return unavailable_trend('incompatible_definition')
    delta = right['value'] - left['value']
    if family == 'D1':
        if not left.get('window_end') or not right.get('window_end'):
            return unavailable_trend('checked_window_not_recorded')
        description = (f"{delta:+g} minutes · {'fewer' if delta < 0 else 'more' if delta > 0 else 'unchanged'} gaps in the checked rolling window; "
                       f"{left.get('window_start') or 'Start not recorded'} → {left.get('window_end')}; "
                       f"{right.get('window_start') or 'Start not recorded'} → {right.get('window_end')}. "
                       'Repair versus old gaps leaving the window cannot be determined from these counts.')
    elif family in ('C2', 'M2'):
        description = f'{delta:+g} validated {"hours" if metric == "valid_hours" else "days"} in the checked calendar · point-to-point observation; this does not establish recovery.'
    else:
        description = ('0s · unchanged' if delta == 0 else f"{'−' if delta < 0 else '+'}{duration(abs(delta))} · {'less lag' if delta < 0 else 'more lag'}") + ' · point-to-point observation'
    return {'value': delta, 'unit': 'minutes' if family == 'D1' else ('hours' if metric == 'valid_hours' else 'days') if family in ('C2', 'M2') else 'seconds', 'current_at': current_at,
            'previous_at': previous_at, 'description': description, 'unavailable_reason': None}


@dataclass
class _Episode:
    observation: NotificationObservation
    first_seen: str | None = None
    last_seen: str | None = None
    previous_slot: str | None = None
    lifecycle: Lifecycle = 'ongoing'
    verification: Verification = 'verified'
    failure_resume: Verification | None = None
    status: SummaryStatus = 'PASS'
    active: bool = False
    had_failure: bool = False
    was_recovered: bool = False
    passes: int = 0
    observations: int = 0
    failing_minutes: int = 0
    observed_slots: int = 0
    start_known: bool = False
    complete: bool = True
    last_event: str | None = None
    quiet_seconds: float = 0
    last_read_end: str | None = None
    transitions: list[NotificationTransition] = field(default_factory=lambda: [])

    def transition(self, slot: str, description: str) -> None:
        self.transitions.append({'group_id': self.observation['group_id'], 'sampling_slot': slot,
                                 'lifecycle': self.lifecycle, 'verification': self.verification, 'description': description})


def _condition(episode: _Episode, observation: NotificationObservation | None, frame: ObservationFrame) -> None:
    slot = frame['sampling_slot']
    previous_slot = episode.previous_slot
    contiguous = previous_slot is not None and _instant(slot) - _instant(previous_slot) == timedelta(minutes=1)
    compatible = observation is not None and observation['definition_version'] == episode.observation['definition_version']
    old_verification, old_status = episode.verification, episode.status
    old_active = episode.active
    state = observation['status'] if observation is not None and observation['complete'] else 'UNKNOWN'
    if not contiguous or not compatible:
        episode.passes = 0
        if previous_slot is not None:
            episode.complete = False
            if episode.active and old_verification != 'unverified':
                episode.verification = old_verification = 'unverified'
                episode.transition(slot, 'Unverified; missing or incompatible observations')
    if observation is not None:
        episode.observation = observation
        episode.observed_slots += int(observation['complete'])
    episode.previous_slot = slot
    episode.status = _status(state)
    if state != 'FAIL':
        episode.failure_resume = None
    episode.complete = episode.complete and observation is not None and observation['complete']
    if state in ('FAIL', 'UNKNOWN'):
        if not old_active and old_status == 'PASS' and episode.first_seen is not None:
            episode.first_seen = None
            episode.observations = episode.failing_minutes = 0
            episode.had_failure = False
            episode.complete = observation is not None and observation['complete']
        episode.passes = 0
        episode.observations += int(observation is not None)
        episode.failing_minutes += int(state == 'FAIL')
        episode.start_known = episode.start_known or (episode.first_seen is None and old_status == 'PASS' and contiguous)
        episode.first_seen = episode.first_seen or slot
        episode.last_seen = slot
        if state == 'UNKNOWN':
            episode.verification = 'unverified'
            if not episode.active:
                episode.active = True
                episode.lifecycle = 'new'
                episode.transition(slot, 'New · Unverified')
            elif old_verification != 'unverified':
                episode.transition(slot, 'Unverified; incident remains unresolved')
        elif observation is not None and observation['eligible'] is True:
            resumed_from = episode.failure_resume or old_verification
            episode.failure_resume = None
            episode.verification = 'verified'
            episode.active = True
            episode.had_failure = True
            if not old_active:
                episode.lifecycle = 'reopened' if episode.was_recovered else 'new'
                episode.transition(slot, 'Reopened' if episode.was_recovered else 'New')
            else:
                episode.lifecycle = 'ongoing'
                if resumed_from != 'verified':
                    episode.transition(slot, 'Ongoing · Verification restored' if resumed_from == 'unverified' else 'Ongoing · Expected wait ended')
        else:
            # Keep the deferred transition while current evidence is already verified.
            if old_active and old_verification != 'verified':
                episode.failure_resume = old_verification
            episode.verification = 'verified'
    elif state == 'EXPECTED_WAIT':
        episode.passes = 0
        episode.verification = 'expected_wait'
        episode.lifecycle = 'expected_wait'
        if episode.active and old_status != 'EXPECTED_WAIT':
            episode.transition(slot, 'Expected wait; incident remains unresolved')
    elif state == 'PASS':
        episode.verification = 'verified'
        if episode.active:
            episode.passes += 1
            if episode.passes >= 2:
                episode.active = False
                episode.was_recovered = True
                episode.lifecycle = 'recovered'
                episode.transition(slot, 'Recovered' if episode.had_failure else 'Verification restored')
        elif not episode.was_recovered and episode.observations:
            episode.lifecycle = 'historical_events'


def _event(episode: _Episode, observation: NotificationObservation | None, frame: ObservationFrame) -> None:
    slot = frame['sampling_slot']
    contiguous = episode.previous_slot is not None and _instant(slot) - _instant(episode.previous_slot) == timedelta(minutes=1)
    check = observation['check'] if observation is not None else episode.observation['check']
    read_key = observation['read_window_key'] if observation is not None else episode.observation['read_window_key']
    read = frame['read_windows'].get(read_key or '')
    read_start = read['window_start'] if read else None
    read_end = read['window_end'] if read else None
    read_seconds = (_instant(read_end) - _instant(read_start)).total_seconds() if read_start and read_end else 0
    verified = (frame['checks_complete'].get(check, False) and read is not None and read['complete']
                and not read['counts_limited'] and read['count'] is not None and read_seconds > 0
                and (observation is not None or frame['omitted_groups'] == 0))
    compatible = observation is None or observation['definition_version'] == episode.observation['definition_version']
    if observation is not None:
        verified = verified and observation['complete']
        episode.observation = observation
    if (not contiguous or not compatible) and episode.previous_slot is not None:
        episode.complete = False
        episode.quiet_seconds = 0
    episode.previous_slot = slot
    episode.observed_slots += int(verified)
    episode.complete = episode.complete and verified
    previous_verification = episode.verification
    if observation is not None and observation['status'] == 'FAIL':
        episode.status, episode.verification = 'FAIL', 'verified'
        if not episode.active or episode.quiet_seconds >= 86400:
            episode.lifecycle = 'new'
            episode.first_seen, episode.observations = slot, 0
            episode.start_known = episode.quiet_seconds >= 86400
            episode.transition(slot, 'New historical event episode')
        else:
            episode.lifecycle = 'ongoing'
            if previous_verification == 'unverified':
                episode.transition(slot, 'Ongoing · Verification restored')
        episode.active, episode.had_failure = True, True
        episode.last_event = episode.last_seen = slot
        episode.observations += 1
        episode.quiet_seconds = 0
    elif not verified or observation is not None and observation['status'] == 'UNKNOWN':
        episode.status, episode.verification = 'UNKNOWN', 'unverified'
        episode.quiet_seconds = 0
        if not episode.active:
            episode.active, episode.lifecycle = True, 'new'
            episode.first_seen = slot
            episode.transition(slot, 'New · Unverified')
        elif previous_verification != 'unverified':
            episode.transition(slot, 'Unverified; event history incomplete')
    else:
        if episode.last_read_end is None or read_start is not None and _instant(read_start) >= _instant(episode.last_read_end):
            episode.quiet_seconds = (episode.quiet_seconds if read_start == episode.last_read_end else 0) + read_seconds
        else:
            episode.quiet_seconds = 0
            episode.complete = False
        episode.status, episode.verification = 'PASS', 'verified'
        episode.lifecycle = 'ongoing' if episode.last_event and _instant(slot) - _instant(episode.last_event) < timedelta(hours=1) else 'historical_events'
        if previous_verification == 'unverified':
            episode.transition(slot, 'Verification restored · No further events observed')
    episode.last_read_end = read_end


def _reduce(history: Sequence[ObservationFrame], now: datetime) -> tuple[list[ObservationFrame], dict[str, _Episode]]:
    frames = _frames(history, now)
    episodes: dict[str, _Episode] = {}
    for frame in frames:
        observations = {item['group_id']: item for item in frame['observations']}
        for key in observations:
            if key not in episodes:
                episodes[key] = _Episode(observations[key])
        for key, episode in episodes.items():
            observation = observations.get(key)
            if episode.observation['kind'] == 'event_stream':
                _event(episode, observation, frame)
            else:
                _condition(episode, observation, frame)
            if _instant(frame['sampling_slot']) < now.replace(second=0, microsecond=0) - timedelta(minutes=1439):
                episode.observations = episode.failing_minutes = episode.observed_slots = 0
    captured_slots = {frame['sampling_slot'] for frame in frames if 'notification_transitions' in frame}
    for key, episode in episodes.items():
        recorded = [transition for frame in frames for transition in frame.get('notification_transitions', [])
                    if transition['group_id'] == key]
        episode.transitions = sorted([*(transition for transition in episode.transitions if transition['sampling_slot'] not in captured_slots), *recorded],
                                     key=lambda transition: (transition['sampling_slot'], transition['description']))
    return frames, episodes


def lifecycle_transitions(history: Sequence[ObservationFrame], *, now: datetime) -> list[NotificationTransition]:
    _, episodes = _reduce(history, now)
    return sorted((item for episode in episodes.values() for item in episode.transitions),
                  key=lambda item: (item['sampling_slot'], item['group_id'], item['description']))


def _incident(key: str, episode: _Episode, frames: Sequence[ObservationFrame], now: datetime) -> Incident:
    observation = episode.observation
    current_present = bool(frames) and any(item['group_id'] == key for item in frames[-1]['observations'])
    end = frames[-1]['sampling_slot'] if current_present else None
    previous_at = (_instant(end) - timedelta(hours=1)).isoformat() if end else None
    previous = next((item for frame in frames if frame['sampling_slot'] == previous_at for item in frame['observations'] if item['group_id'] == key), None)
    family = observation['check'].removeprefix('law.').split(':', 1)[0]
    label = LAW_NAMES.get(family, _CHECK_NAMES.get(observation['check'].removeprefix('monitor.'), observation['check'].replace('_', ' ')))
    if observation['scope']:
        label += ' · ' + observation['scope'].replace('_', ' ')
    coverage = _history_coverage(frames, now)
    coverage['observed_slots'] = min(coverage['expected_slots'], episode.observed_slots)
    coverage['complete'] = coverage['complete'] and episode.complete
    coverage['reason'] = 'complete_episode' if episode.start_known and episode.complete else 'start_or_coverage_unavailable'
    return {'group_id': key, 'kind': observation['kind'], 'lifecycle': episode.lifecycle, 'status': episode.status,
            'had_eligible_failure': episode.had_failure, 'verification': episode.verification, 'label': label,
            'first_seen': episode.first_seen, 'last_seen': episode.last_seen, 'observations': episode.observations,
            'coverage': coverage, 'measurements': observation['measurements'],
            'trend': measurement_trend(observation, end, previous, previous_at) if end else unavailable_trend('current_observation_missing'),
            'evidence_refs': observation['evidence_refs'], 'scope_count': 1,
            'recovery_pending': observation['kind'] == 'condition' and episode.active and episode.status == 'PASS', 'failing_minutes': episode.failing_minutes,
            'transitions': [f"{item['description']} at {item['sampling_slot']}" for item in episode.transitions]}


def build_summary(current: Document, catalog: Document, history: Sequence[ObservationFrame], *, now: datetime) -> OperatorSummary:
    frames, episodes = _reduce(history, now)
    coverage = _history_coverage(frames, now)
    report = _object(current.get('last_report'))
    observed_at = _text(current.get('checked_at')) or _text(report.get('evaluation_end'))
    stale = observed_at is None or (now - _instant(observed_at)).total_seconds() >= 120
    fault = bool(current.get('reason')) or stale
    effective = {**current, 'reason': current.get('reason') or ('stale_report' if stale else ''), 'evidence_expired': stale}
    cards = _cards(effective, catalog, coverage)
    status = 'UNKNOWN' if fault else _rollup([card['status'] for card in cards[:6]])
    inventory = next((item for item in _objects(report.get('gates')) if item.get('gate_id') == 'law.inventory'), Document())
    if not fault:
        status = _rollup([status, _status(inventory.get('outcome'))])
    incidents = sorted((_incident(key, episode, frames, now) for key, episode in episodes.items()
                        if episode.active or episode.observations or episode.was_recovered), key=incident_rank)
    omitted = None if any(frame['omitted_groups'] is None for frame in frames) else max((frame['omitted_groups'] or 0 for frame in frames), default=0)
    summary: OperatorSummary = {'schema_version': 1, 'sampling_slot': _text(report.get('sampling_slot')) or (frames[-1]['sampling_slot'] if frames else None),
        'catalog_version': _text(catalog.get('version')), 'observed_at': observed_at, 'prepared_at': now.isoformat(),
        'status': status, 'cards': cards, 'incidents': incidents, 'coverage': coverage,
        'omitted_groups': omitted, 'delivery_status': str(current.get('delivery_status') or 'Not recorded'),
        'law_observed': bool(report),
        'law_sample_ref': frames[-1]['law_sample_ref'] if frames else None}
    for incident in incidents:
        incident['display_lines'] = incident_lines(incident, 4)
    # Detail yields before identity rows; any truncation remains explicit.
    if len(json.dumps(summary, separators=(',', ':'), ensure_ascii=False).encode()) > MAX_SUMMARY_BYTES:
        for incident in incidents:
            incident['measurements'] = incident['measurements'][:2]
            incident['evidence_refs'] = incident['evidence_refs'][:1]
            transitions = incident.get('transitions', [])
            if len(transitions) > 4:
                incident['transitions'] = [*transitions[:2], f'{len(transitions) - 4} additional recorded transitions', *transitions[-2:]]
            incident['display_lines'] = incident_lines(incident, 2)
    while len(json.dumps(summary, separators=(',', ':'), ensure_ascii=False).encode()) > MAX_SUMMARY_BYTES and incidents:
        incidents.pop()
        if summary['omitted_groups'] is not None:
            summary['omitted_groups'] += 1
    if len(json.dumps(summary, separators=(',', ':'), ensure_ascii=False).encode()) > MAX_SUMMARY_BYTES:
        raise ValueError('operator_summary_exceeds_limit')
    return summary


def _identity(event: Document) -> tuple[str, str, str]:
    gate, version, evidence = (event.get(key) for key in ('gate_id', 'definition_version', 'evidence_id'))
    return gate if isinstance(gate, str) else '', version if isinstance(version, str) else '', evidence if isinstance(evidence, str) else ''


def _micros(stamp: datetime) -> int:
    return (stamp - datetime(1970, 1, 1, tzinfo=UTC)) // timedelta(microseconds=1)


def _canonical_policy(feed: Document) -> Json:
    evidence = _object(_object(_object(feed.get('predicates')).get('C1')).get('evidence'))
    return 'provider_availability' if evidence.get('canonical_interval') == 'hour' else str(evidence.get('deadline'))[11:16]


def sample_brief(report: Document) -> Document:
    feeds = _objects(report.get('feeds'))
    core = sorted((gate, version) for gate, version, _ in map(_identity, _objects(report.get('gates'))) if gate.startswith('law.'))
    expected = {f"law.{name}:{feed.get('source_key')}" for feed in feeds for name in _object(feed.get('predicates'))} | {'law.inventory'}
    complete = {key for key, version in core if version} == expected and len(core) == len(expected)
    return {
        'slot': report.get('sampling_slot'), 'start': report.get('evaluation_start'),
        'status': report.get('status'), 'version': report.get('schema_version'),
        'policy': hashlib.sha256(json.dumps([core, report.get('schema_version'), report.get('inventory'), [(feed.get('source_key'), _object(_object(_object(feed.get('predicates')).get('R1')).get('evidence')).get('budget_seconds'), _object(_object(_object(feed.get('predicates')).get('C2')).get('evidence')).get('anchor'), _canonical_policy(feed), _object(_object(_object(feed.get('predicates')).get('D1')).get('evidence')).get('expected_slots'), _object(_object(_object(feed.get('predicates')).get('D1')).get('evidence')).get('max_missing')) for feed in feeds]], sort_keys=True).encode()).hexdigest() if complete else None,
        'ages': {str(feed.get('source_key')): _object(_object(_object(feed.get('predicates')).get('R1')).get('evidence')).get('age_seconds') for feed in feeds},
        'not_due': any(_object(_object(feed.get('predicates')).get('C1')).get('status') == 'NOT_DUE' for feed in feeds),
    }


def operation_sample(report: Document) -> bytes:
    gates = {event.get('gate_id'): event for event in _objects(report.get('gates'))}
    values: list[int | bool] = []
    for metric, gate in OPERATIONS:
        evidence = _object(gates.get(gate, {}).get('evidence'))
        count = _number(evidence.get(metric))
        try:
            start, end = (_instant(evidence.get(key)) for key in ('window_start', 'window_end'))
            valid = end > start and not start.microsecond and not end.microsecond
            left, right = _micros(start), _micros(end)
        except ValueError:
            left = right = 0
            valid = False
        count_valid = count is not None and count.is_integer() and 0 <= count <= 1000
        values.extend((left, right, int(count) if valid and count_valid and count is not None else -1,
                       evidence.get('counts_limited') is True or count == 1000))
    return OPERATION_RECORD.pack(*values)


def operations_totals(records: dict[str, bytes], *, loading: bool, limited: bool) -> Document:
    result: Document = {}
    unpacked = [cast(tuple[int, int, int, bool, int, int, int, bool], OPERATION_RECORD.unpack(value))
                for value in records.values()]
    newest = cast(tuple[int, int, int, bool, int, int, int, bool], OPERATION_RECORD.unpack(records[max(records)])) if records else None
    epoch = datetime(1970, 1, 1, tzinfo=UTC)
    for index, (metric, _) in enumerate(OPERATIONS):
        latest_missing = newest is not None and newest[index * 4 + 2] < 0
        intervals = sorted((row[index * 4], row[index * 4 + 1], row[index * 4 + 2], row[index * 4 + 3])
                           for row in unpacked)
        end = max((row[1] for row in intervals), default=0)
        horizons: Document = {}
        for horizon, minutes in (('60m', 60), ('24h', 1440)):
            start = end - minutes * 60_000_000
            candidates = [row for row in intervals if row[2] >= 0 and start <= row[0] < row[1] <= end]
            overlaps: set[int] = set()
            previous_end = 0
            group: list[int] = []
            for position, row in enumerate(candidates):
                if row[0] < previous_end:
                    overlaps.update(group)
                    overlaps.add(position)
                    group.append(position)
                else:
                    group = [position]
                previous_end = max(previous_end, row[1])
            accepted = [row for position, row in enumerate(candidates) if position not in overlaps]
            covered = sum(row[1] - row[0] for row in accepted) / 1_000_000
            capped = limited or any(row[3] for row in accepted)
            crossing = any(row[0] < start < row[1] for row in intervals)
            complete = bool(end and not latest_missing and not loading and not capped and not overlaps and not crossing
                            and len(accepted) == minutes and covered == minutes * 60)
            reason = ('complete' if complete else 'history_loading' if loading else 'history_limited' if limited
                      else 'overlapping_intervals' if overlaps else 'boundary_interval' if crossing
                      else 'counts_limited' if capped else 'latest_read_unavailable' if latest_missing and accepted
                      else 'missing_intervals' if accepted else 'history_unavailable')
            horizons[horizon] = {
                'count': sum(row[2] for row in accepted) if accepted else None,
                'window_start': (epoch + timedelta(microseconds=start)).isoformat() if end else None,
                'window_end': (epoch + timedelta(microseconds=end)).isoformat() if end else None,
                'observed_slots': len(accepted), 'expected_slots': minutes,
                'covered_seconds': covered, 'complete': complete, 'limited': capped, 'reason': reason,
            }
        result[metric] = horizons
    return result


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


def _reject_number(value: str) -> Json:
    raise ValueError(f'invalid_number:{value}')


def decode_observation_record(raw: bytes) -> tuple[ObservationFrame, Document | None]:
    """Decode one bounded sidecar record without loading evaluator dependencies."""
    if len(raw) > 8192:
        raise ValueError('notification_record_exceeds_limit')
    envelope = cast(Json, json.loads(raw, parse_constant=_reject_number))
    if not isinstance(envelope, dict) or not isinstance(envelope.get('frame'), str):
        raise ValueError('invalid_notification_envelope')
    inflater = zlib.decompressobj()
    try:
        decoded = inflater.decompress(base64.b64decode(str(envelope['frame']), validate=True), 128 * 1024 + 1)
    except zlib.error as error:
        raise ValueError('invalid_notification_compression') from error
    if len(decoded) > 128 * 1024 or not inflater.eof or inflater.unused_data:
        raise ValueError('notification_frame_exceeds_limit_or_incomplete')
    value = cast(Json, json.loads(decoded, parse_constant=_reject_number))
    if not isinstance(value, dict):
        raise ValueError('invalid_notification_frame')
    slot = value.get('sampling_slot')
    if not isinstance(slot, str) or _instant(slot).second or _instant(slot).microsecond:
        raise ValueError('invalid_notification_slot')
    for key in ('catalog_version', 'law_sample_ref'):
        if value.get(key) is not None and not isinstance(value[key], str):
            raise ValueError('invalid_notification_reference')
    if not isinstance(value.get('complete'), bool) or not isinstance(value.get('law_observed'), bool):
        raise ValueError('invalid_notification_completeness')
    omitted = value.get('omitted_groups')
    if omitted is not None and (not isinstance(omitted, int) or isinstance(omitted, bool) or omitted < 0):
        raise ValueError('invalid_omitted_groups')
    checks, reads, observations = (value.get(key) for key in ('checks_complete', 'read_windows', 'observations'))
    if not isinstance(checks, dict) or not all(isinstance(item, bool) for item in checks.values()):
        raise ValueError('invalid_notification_checks')
    if not isinstance(reads, dict):
        raise ValueError('invalid_notification_reads')
    for read in reads.values():
        if not isinstance(read, dict):
            raise ValueError('invalid_notification_read')
        for key in ('window_start', 'window_end'):
            if read.get(key) is not None:
                _instant(read[key])
        count = read.get('count')
        if count is not None and (not isinstance(count, int) or isinstance(count, bool) or count < 0):
            raise ValueError('invalid_notification_count')
        if not isinstance(read.get('counts_limited'), bool) or not isinstance(read.get('complete'), bool):
            raise ValueError('invalid_notification_read_coverage')
    if not isinstance(observations, list) or len(observations) > 64:
        raise ValueError('invalid_notification_groups')
    for observation in observations:
        if not isinstance(observation, dict):
            raise ValueError('invalid_notification_observation')
        if any(not isinstance(observation.get(key), str) for key in ('group_id', 'check', 'scope', 'definition_version')):
            raise ValueError('invalid_notification_identity')
        if observation.get('kind') not in ('condition', 'event_stream') or observation.get('status') not in ('PASS', 'FAIL', 'UNKNOWN', 'EXPECTED_WAIT', 'NO_NEW_EVENTS'):
            raise ValueError('invalid_notification_state')
        if not isinstance(observation.get('complete'), bool) or observation.get('eligible') is not None and not isinstance(observation['eligible'], bool):
            raise ValueError('invalid_notification_eligibility')
        if observation.get('read_window_key') is not None and not isinstance(observation['read_window_key'], str):
            raise ValueError('invalid_notification_read_reference')
        for key in ('detector_keys', 'evidence_refs'):
            entries = observation.get(key)
            if not isinstance(entries, list) or not all(isinstance(item, str) for item in entries):
                raise ValueError('invalid_notification_evidence')
        measurements = observation.get('measurements')
        if not isinstance(measurements, list):
            raise ValueError('invalid_notification_measurements')
        for measurement in measurements:
            if not isinstance(measurement, dict):
                raise ValueError('invalid_notification_measurement')
            if any(not isinstance(measurement.get(key), str) for key in ('name', 'unit', 'observed_at', 'definition_version')):
                raise ValueError('invalid_notification_measurement_identity')
            for key in ('value', 'threshold'):
                if measurement.get(key) is not None and _number(measurement[key]) is None:
                    raise ValueError('invalid_notification_measurement_number')
            for key in ('window_start', 'window_end'):
                if measurement.get(key) is not None:
                    _instant(measurement[key])
    transitions = value.get('notification_transitions')
    if transitions is not None:
        if not isinstance(transitions, list):
            raise ValueError('invalid_notification_transitions')
        for transition in transitions:
            if (not isinstance(transition, dict) or transition.get('sampling_slot') != slot
                    or not isinstance(transition.get('group_id'), str) or not isinstance(transition.get('description'), str)
                    or transition.get('lifecycle') not in ('new', 'ongoing', 'recovered', 'reopened', 'expected_wait', 'historical_events')
                    or transition.get('verification') not in ('verified', 'unverified', 'expected_wait')):
                raise ValueError('invalid_recorded_notification_transition')
    brief = envelope.get('brief')
    if brief is not None and not isinstance(brief, dict):
        raise ValueError('invalid_notification_brief')
    return cast(ObservationFrame, value), brief


def measurement_label(name: str) -> str:
    return {
        'age_seconds': 'Reader delay', 'missing_slots': 'Missing minutes',
        'expected_slots': 'Checked minutes', 'raw_proof_rows': 'Recorded source rows',
        'day_count': 'Recorded days', 'expected_days': 'Expected days', 'valid_days': 'Validated days',
        'missing_days': 'Missing days', 'unknown_days': 'Unverified days',
        'expected_hours': 'Expected hours', 'valid_hours': 'Validated hours', 'missing_hours': 'Missing hours', 'first_invalid_hour': 'First invalid hour', 'hour': 'Authoritative hour', 'canonical_interval': 'Authority interval',
        'queued_runs': 'Queued runs', 'queue_threshold': 'Queue threshold',
        'unhealthy_daemons': 'Unhealthy daemons', 'workers_fresh': 'Fresh worker heartbeats',
        'workers_expected': 'Expected workers', 'workers_unknown': 'Unverified workers',
        'collectors_serving': 'Collectors serving', 'collectors_expected': 'Expected collectors',
        'lag_seconds': 'Publication delay', 'grace_seconds': 'Publication allowance',
        'event_count': 'Observed events', 'heartbeat_age_seconds': 'Worker heartbeat age',
        'committed_age_seconds': 'Committed capture age', 'response_age_seconds': 'Collector response age',
        'durable_capture_age_seconds': 'Durable capture age', 'spool_bytes': 'Capture storage used',
    }.get(name, name.replace('_', ' ').capitalize())


def measurement_text(measurement: Measurement) -> str:
    value = 'Not recorded' if measurement['value'] is None else f"{_shown(measurement['value'])} {measurement['unit']}"
    threshold = '' if measurement['threshold'] is None else f"; threshold {_shown(measurement['threshold'])} {measurement['unit']}"
    return f"{measurement_label(measurement['name'])}: {value}{threshold}; observed {_utc(measurement['observed_at'])}"


def incident_lines(incident: Incident, detail: int = 4) -> list[str]:
    lifecycle = incident['lifecycle'].replace('_', ' ')
    if incident['lifecycle'] == 'recovered' and not incident['had_eligible_failure']:
        lifecycle = 'verification restored'
    states = {'FAIL': 'failing', 'UNKNOWN': 'unverified', 'EXPECTED_WAIT': 'expected wait', 'PASS': 'passing'}
    first_label = 'First observed' if incident['coverage']['reason'] == 'complete_episode' else 'Observed since at least'
    lines = [incident['label'], f"{lifecycle} · currently {states[incident['status']]} · {incident['verification']}",
             repeat_text(incident), coverage_text(incident['coverage']),
             f"{first_label}: {incident['first_seen'] or 'Not recorded'}; last observed: {incident['last_seen'] or 'Not recorded'}",
             incident['trend']['description']]
    if incident['kind'] == 'condition':
        lines.append(f"{incident.get('failing_minutes', 0)} observed failing minutes; {max(0, incident['coverage']['expected_slots'] - incident['coverage']['observed_slots'])} unobserved minutes in the comparison window. Elapsed duration is not continuous downtime.")
    if incident.get('recovery_pending', False):
        lines.append('Recovery pending: one passing observation; a second consecutive pass is required.')
    lines.extend(incident.get('transitions', []))
    lines.extend(measurement_text(measurement) for measurement in incident['measurements'][:detail])
    if len(incident['measurements']) > detail:
        lines.append(f"{len(incident['measurements']) - detail} measurement details omitted.")
    if not incident['measurements']:
        lines.append('Measurements: Not recorded.')
    if detail and incident['evidence_refs']:
        lines.append('Evidence: ' + '; '.join(incident['evidence_refs'][:detail]))
    return lines
