"""One shared operator summary, bounded mail bodies and a private durable delivery intent."""

from __future__ import annotations

import base64
import hashlib
import json
import logging
import math
import time
import urllib.parse
import uuid
from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Literal, NotRequired, Protocol, TypedDict, cast

from origo.alerts.email import REQUEST_TIMEOUT_SECONDS, AlertSettings, DeliveryError, send_alert
from origo.alerts.brief import render_brief
from origo.observatory import OperatorSummary

BODY_BYTES = 64 * 1024
CURSOR_BYTES = 256 * 1024
GLOBAL_INTERVAL_SECONDS = 3600
UNSENT_HORIZON_SECONDS = 24 * 3600
RETRY_INTERVAL_SECONDS = 60
MAX_LOST_INTERVALS = 32
log = logging.getLogger('origo.alerts')


@dataclass(frozen=True)
class EmailContent:
    subject: str
    html: str
    text: str


class LostInterval(TypedDict):
    start: str
    end: str
    reason: str


class PendingNotification(TypedDict):
    batch_id: str
    idempotency_key: str
    endpoint: str
    payload_base64: str
    payload_sha256: str
    prepared_at: float
    expires_at: float
    evidence_start: str | None
    evidence_end: str | None
    first_dispatch_at: float | None
    attempt_started_at: float | None
    attempt_deadline: float | None
    next_attempt_at: float
    last_completion_at: float | None
    attempts: int
    outcome: Literal['pending', 'uncertain', 'retryable', 'rejected', 'acknowledged']
    acknowledgement_id: str | None
    acknowledgement_at: float | None
    digest_date: NotRequired[str | None]
    disclosed_intervals: NotRequired[list[LostInterval]]
    acceptance_uncertain: NotRequired[bool]


class DeliveryReceipt(TypedDict):
    batch_id: str
    outcome: Literal['rejected', 'uncertain', 'acknowledged']
    acknowledgement_id: str | None
    completed_at: float
    evidence_start: str | None
    evidence_end: str | None
    reason: str


class DeliveryCursor(Protocol):
    pending_notification: PendingNotification | None
    last_delivery: DeliveryReceipt | None
    next_distinct_at: float
    notified_through: str | None
    expired_through: str | None
    lost_intervals: list[LostInterval]
    last_digest_date: str


class Transport(Protocol):
    def __call__(
        self,
        payload: bytes,
        *,
        api_key: str,
        api_url: str,
        idempotency_key: str,
    ) -> str: ...


def dashboard_link(base: str | None, target: dict[str, object]) -> str | None:
    if base is None:
        return None
    supported = ('view', 'gate', 'source', 'family', 'overview')
    parameters = [
        (key, str(target[key])) for key in supported if key in target and target[key] is not None
    ]
    return base + ('?' + urllib.parse.urlencode(parameters) if parameters else '')


def render_email(
    summary: OperatorSummary,
    *,
    dashboard_url: str | None,
    dagit_written: bool,
) -> EmailContent:
    return EmailContent(*render_brief(summary, dashboard_url=dashboard_url, dagit_written=dagit_written))


def _iso(timestamp: float) -> str:
    return datetime.fromtimestamp(timestamp, UTC).isoformat()


def _later(first: str | None, second: str | None) -> str | None:
    if first is None:
        return second
    if second is None:
        return first
    return max(first, second, key=lambda value: datetime.fromisoformat(value).timestamp())


def add_loss_interval(cursor: DeliveryCursor, start: str, end: str, reason: str) -> None:
    start_at = datetime.fromisoformat(start).astimezone(UTC)
    end_at = datetime.fromisoformat(end).astimezone(UTC)
    if end_at < start_at:
        raise ValueError('Notification loss interval ends before its start.')
    interval = LostInterval(start=start_at.isoformat(), end=end_at.isoformat(), reason=reason[:512])
    intervals = sorted([*cursor.lost_intervals, interval], key=lambda item: item['start'])
    merged: list[LostInterval] = []
    for item in intervals:
        if merged and merged[-1]['reason'] == item['reason'] and item['start'] <= merged[-1]['end']:
            merged[-1]['end'] = max(merged[-1]['end'], item['end'])
        else:
            merged.append(item.copy())
    while len(merged) > MAX_LOST_INTERVALS:
        first, second = merged[:2]
        reason_text = '; '.join(dict.fromkeys((first['reason'], second['reason'])))
        merged[:2] = [
            LostInterval(
                start=first['start'],
                end=max(first['end'], second['end']),
                reason=('Coalesced loss/uncertainty: ' + reason_text)[:512],
            )
        ]
    cursor.lost_intervals = merged
    cursor.expired_through = _later(cursor.expired_through, end)


def prune_unsent(cursor: DeliveryCursor, now: datetime) -> None:
    # Acknowledgements and replay coverage refer to minute sampling slots.
    cutoff = (
        now.astimezone(UTC).replace(second=0, microsecond=0)
        - timedelta(seconds=UNSENT_HORIZON_SECONDS)
    ).isoformat()
    baseline = _later(cursor.notified_through, cursor.expired_through)
    if baseline is not None and datetime.fromisoformat(baseline) < datetime.fromisoformat(cutoff):
        add_loss_interval(
            cursor, baseline, cutoff, 'Unsent evidence exceeded the 24-hour replay horizon.'
        )


def plan_notification(
    cursor: DeliveryCursor,
    summary: OperatorSummary,
    settings: AlertSettings,
    now: datetime,
    dagit_written: bool,
    *,
    transitions: bool,
) -> bool:
    timestamp = now.timestamp()
    if cursor.pending_notification is not None or timestamp < cursor.next_distinct_at:
        return False
    utc_now = now.astimezone(UTC)
    today = utc_now.date().isoformat()
    digest_due = utc_now.hour >= settings.digest_hour_utc and cursor.last_digest_date != today
    if not transitions and not digest_due:
        return False
    evidence_end = summary['coverage']['window_end']
    if evidence_end is not None:
        prune_unsent(cursor, datetime.fromisoformat(evidence_end))
    disclosed = [interval.copy() for interval in cursor.lost_intervals]
    prepared = summary.copy()
    prepared['prepared_at'] = utc_now.isoformat()
    prepared['digest'] = digest_due
    disclosures = [prepared['delivery_status']]
    if settings.dashboard_url_fault:
        disclosures.append(settings.dashboard_url_fault)
    disclosures.extend(
        f'Delivery/history uncertainty {item["start"]} through {item["end"]}: {item["reason"]}'
        for item in disclosed
    )
    prepared['delivery_status'] = '\n'.join(value for value in disclosures if value)
    content = render_email(
        prepared, dashboard_url=settings.public_dashboard_url, dagit_written=dagit_written
    )
    payload = json.dumps(
        {
            'from': settings.email_from,
            'to': list(settings.email_to),
            'subject': content.subject,
            'html': content.html,
            'text': content.text,
        },
        ensure_ascii=False,
        separators=(',', ':'),
    ).encode('utf-8')
    batch_id = str(uuid.uuid4())
    pending = PendingNotification(
        batch_id=batch_id,
        idempotency_key=f'origo/{batch_id}',
        endpoint=settings.resend_api_url,
        payload_base64=base64.b64encode(payload).decode('ascii'),
        payload_sha256=hashlib.sha256(payload).hexdigest(),
        prepared_at=timestamp,
        expires_at=timestamp + GLOBAL_INTERVAL_SECONDS,
        evidence_start=summary['coverage']['window_start'],
        evidence_end=summary['sampling_slot'],
        first_dispatch_at=None,
        attempt_started_at=None,
        attempt_deadline=None,
        next_attempt_at=timestamp,
        last_completion_at=None,
        attempts=0,
        outcome='pending',
        acknowledgement_id=None,
        acknowledgement_at=None,
        digest_date=today if digest_due else None,
        disclosed_intervals=disclosed,
        acceptance_uncertain=False,
    )
    if len(json.dumps(pending, ensure_ascii=False).encode()) > CURSOR_BYTES:
        raise ValueError('Private notification envelope exceeds 256 KiB; no delivery prepared.')
    cursor.pending_notification = pending
    return True


def _remaining_disclosures(
    intervals: list[LostInterval],
    disclosed: list[LostInterval],
) -> list[LostInterval]:
    remaining = [item.copy() for item in intervals]
    for known in disclosed:
        next_intervals: list[LostInterval] = []
        for item in remaining:
            if (
                item['reason'] != known['reason']
                or item['end'] < known['start']
                or item['start'] > known['end']
            ):
                next_intervals.append(item)
            else:
                if item['start'] < known['start']:
                    next_intervals.append(
                        LostInterval(start=item['start'], end=known['start'], reason=item['reason'])
                    )
                if item['end'] > known['end']:
                    next_intervals.append(
                        LostInterval(start=known['end'], end=item['end'], reason=item['reason'])
                    )
        remaining = next_intervals
    while len(remaining) > MAX_LOST_INTERVALS:
        first, second = remaining[:2]
        remaining[:2] = [
            LostInterval(
                start=first['start'],
                end=max(first['end'], second['end']),
                reason=('Coalesced loss/uncertainty: ' + first['reason'] + '; ' + second['reason'])[
                    :512
                ],
            )
        ]
    return remaining


def _retire(
    cursor: DeliveryCursor,
    pending: PendingNotification,
    outcome: Literal['rejected', 'uncertain', 'acknowledged'],
    completed_at: float,
    reason: str,
) -> None:
    cursor.last_delivery = DeliveryReceipt(
        batch_id=pending['batch_id'],
        outcome=outcome,
        acknowledgement_id=pending['acknowledgement_id'],
        completed_at=completed_at,
        evidence_start=pending['evidence_start'],
        evidence_end=pending['evidence_end'],
        reason=reason[:512],
    )
    if outcome == 'acknowledged':
        cursor.notified_through = _later(cursor.notified_through, pending['evidence_end'])
        digest_date = pending.get('digest_date')
        if digest_date:
            cursor.last_digest_date = max(cursor.last_digest_date, digest_date)
        # A subsequent loss interval must survive acceptance of an earlier disclosure.
        disclosed = pending.get('disclosed_intervals', [])
        cursor.lost_intervals = _remaining_disclosures(cursor.lost_intervals, disclosed)
    elif outcome == 'uncertain' or pending.get('acceptance_uncertain', False):
        add_loss_interval(
            cursor,
            pending['evidence_start'] or _iso(pending['prepared_at']),
            pending['evidence_end'] or _iso(completed_at),
            reason,
        )
    cursor.pending_notification = None


def attempt_delivery(
    cursor: DeliveryCursor,
    settings: AlertSettings | None,
    now: datetime,
    persist: Callable[[], None],
    *,
    completion_clock: Callable[[], datetime] | None = None,
    sender: Transport | None = None,
) -> str:
    pending = cursor.pending_notification
    if pending is None:
        return 'idle'
    timestamp = now.timestamp()
    if pending['outcome'] in {'acknowledged', 'rejected'}:
        outcome = 'acknowledged' if pending['outcome'] == 'acknowledged' else 'rejected'
        _retire(
            cursor,
            pending,
            outcome,
            pending['acknowledgement_at'] or pending['last_completion_at'] or timestamp,
            'Recovered a durable terminal delivery outcome.',
        )
        persist()
        return outcome
    if timestamp >= pending['expires_at'] - REQUEST_TIMEOUT_SECONDS:
        _retire(
            cursor,
            pending,
            'uncertain',
            timestamp,
            'Snapshot expired without acknowledgement; provider acceptance may be unknown.',
        )
        persist()
        return 'expired'
    if settings is None:
        return 'disabled'
    if timestamp < pending['next_attempt_at']:
        return 'waiting'
    if pending['first_dispatch_at'] is None and timestamp < cursor.next_distinct_at:
        return 'waiting'
    payload = _payload(pending)
    pending['attempts'] += 1
    pending['first_dispatch_at'] = (
        pending['first_dispatch_at'] if pending['first_dispatch_at'] is not None else timestamp
    )
    pending['attempt_started_at'] = timestamp
    pending['attempt_deadline'] = timestamp + REQUEST_TIMEOUT_SECONDS
    pending['last_completion_at'] = None
    pending['next_attempt_at'] = timestamp + RETRY_INTERVAL_SECONDS
    cursor.next_distinct_at = max(
        cursor.next_distinct_at, timestamp + REQUEST_TIMEOUT_SECONDS + GLOBAL_INTERVAL_SECONDS
    )
    started_monotonic = time.monotonic()
    persist()
    dispatch_at = timestamp + max(0.0, time.monotonic() - started_monotonic)
    if dispatch_at >= pending['expires_at'] - REQUEST_TIMEOUT_SECONDS:
        _retire(
            cursor,
            pending,
            'uncertain',
            dispatch_at,
            'Snapshot expired while persisting the attempt reservation; no request started.',
        )
        persist()
        return 'expired'
    transport = sender or send_alert

    def completed() -> float:
        return max(
            timestamp,
            completion_clock().timestamp()
            if completion_clock
            else timestamp + time.monotonic() - started_monotonic,
        )

    try:
        identifier = transport(
            payload,
            api_key=settings.resend_api_key,
            api_url=pending['endpoint'],
            idempotency_key=pending['idempotency_key'],
        )
        if not identifier.strip():
            raise DeliveryError(
                'Transport returned no acknowledgement; acceptance is uncertain.',
                disposition='uncertain',
            )
    except DeliveryError as error:
        if error.disposition == 'uncertain':
            pending['acceptance_uncertain'] = True
        completion = completed()
        cursor.next_distinct_at = max(cursor.next_distinct_at, completion + GLOBAL_INTERVAL_SECONDS)
        pending['last_completion_at'] = completion
        pending['next_attempt_at'] = completion + max(
            RETRY_INTERVAL_SECONDS, error.retry_after or 0.0
        )
        if error.disposition in {'rejected', 'invariant'}:
            pending['outcome'] = 'rejected'
            _retire(cursor, pending, 'rejected', completion, str(error))
            status = 'rejected'
        else:
            pending['outcome'] = 'retryable' if error.disposition == 'retryable' else 'uncertain'
            status = pending['outcome']
        persist()
        log.warning('Notification delivery %s: %s', status, error)
        return status
    completion = completed()
    cursor.next_distinct_at = max(cursor.next_distinct_at, completion + GLOBAL_INTERVAL_SECONDS)
    pending['last_completion_at'] = completion
    pending['acknowledgement_id'] = identifier
    pending['acknowledgement_at'] = completion
    pending['outcome'] = 'acknowledged'
    _retire(
        cursor, pending, 'acknowledged', completion, 'Resend acknowledged the immutable request.'
    )
    persist()
    log.info('Notification batch %s acknowledged by Resend.', pending['batch_id'])
    return 'acknowledged'


def _object(value: object, name: str) -> dict[str, object]:
    if not isinstance(value, dict):
        raise ValueError(f'{name} must be an object.')
    return cast(dict[str, object], value)


def _string(data: dict[str, object], key: str, *, nullable: bool = False) -> str | None:
    value = data.get(key)
    if value is None and nullable and key in data:
        return None
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f'Private notification {key} must be a nonempty string.')
    return value


def _number(data: dict[str, object], key: str, *, nullable: bool = False) -> float | None:
    value = data.get(key)
    if value is None and nullable and key in data:
        return None
    if isinstance(value, bool) or not isinstance(value, (int, float)) or not math.isfinite(value):
        raise ValueError(f'Private notification {key} must be a finite number.')
    return float(value)


def _payload(pending: PendingNotification) -> bytes:
    try:
        payload = base64.b64decode(pending['payload_base64'], validate=True)
    except ValueError as error:
        raise ValueError('Private notification payload is not valid base64.') from error
    if hashlib.sha256(payload).hexdigest() != pending['payload_sha256']:
        raise ValueError('Private notification payload hash mismatch; no POST permitted.')
    if len(payload) > CURSOR_BYTES:
        raise ValueError('Private notification payload exceeds its byte bound.')
    document = _object(json.loads(payload), 'Private notification payload')
    for key in ('from', 'subject', 'html', 'text'):
        value = _string(document, key)
        if key in {'html', 'text'} and value is not None and len(value.encode()) > BODY_BYTES:
            raise ValueError('Private notification body exceeds 64 KiB.')
    recipients = document.get('to')
    if not isinstance(recipients, list):
        raise ValueError('Private notification recipients must be a list.')
    recipient_list = cast(list[object], recipients)
    if not recipient_list or any(not isinstance(item, str) or not item for item in recipient_list):
        raise ValueError('Private notification recipients must be nonempty strings.')
    return payload


def validate_lost_intervals(value: object) -> list[LostInterval]:
    if not isinstance(value, list):
        raise ValueError('Notification loss intervals must be a list.')
    entries = cast(list[object], value)
    if len(entries) > MAX_LOST_INTERVALS:
        raise ValueError('Notification loss intervals must be a list of at most 32 intervals.')
    result: list[LostInterval] = []
    for entry in entries:
        data = _object(entry, 'LostInterval')
        for key in ('start', 'end', 'reason'):
            _string(data, key)
        interval = cast(LostInterval, data)
        if len(interval['reason']) > 512:
            raise ValueError('Notification loss reason exceeds its character bound.')
        if datetime.fromisoformat(interval['start']) > datetime.fromisoformat(interval['end']):
            raise ValueError('Notification loss interval ends before it starts.')
        result.append(interval.copy())
    return result


def validate_pending(value: object) -> PendingNotification:
    data = _object(value, 'PendingNotification')
    for key in ('batch_id', 'idempotency_key', 'endpoint', 'payload_base64', 'payload_sha256'):
        _string(data, key)
    for key in ('evidence_start', 'evidence_end', 'acknowledgement_id'):
        _string(data, key, nullable=True)
    for key in ('prepared_at', 'expires_at', 'next_attempt_at'):
        _number(data, key)
    for key in (
        'first_dispatch_at',
        'attempt_started_at',
        'attempt_deadline',
        'last_completion_at',
        'acknowledgement_at',
    ):
        _number(data, key, nullable=True)
    attempts = data.get('attempts')
    if isinstance(attempts, bool) or not isinstance(attempts, int) or attempts < 0:
        raise ValueError('Notification attempt count must be a nonnegative integer.')
    if data.get('outcome') not in {'pending', 'uncertain', 'retryable', 'rejected', 'acknowledged'}:
        raise ValueError('Private notification has an unknown outcome.')
    pending = cast(PendingNotification, data)
    if pending['expires_at'] != pending['prepared_at'] + GLOBAL_INTERVAL_SECONDS:
        raise ValueError('Notification expiry must remain fixed at preparation plus one hour.')
    if (
        not 1 <= len(pending['idempotency_key']) <= 256
        or '\r' in pending['idempotency_key']
        or '\n' in pending['idempotency_key']
    ):
        raise ValueError('Notification idempotency key exceeds its bound.')
    if len(json.dumps(data, ensure_ascii=False).encode()) > CURSOR_BYTES:
        raise ValueError('Private notification envelope exceeds 256 KiB.')
    _payload(pending)
    if 'disclosed_intervals' in data:
        pending['disclosed_intervals'] = validate_lost_intervals(data['disclosed_intervals'])
    if 'acceptance_uncertain' in data and not isinstance(data['acceptance_uncertain'], bool):
        raise ValueError('Private notification uncertainty flag must be boolean.')
    if 'digest_date' in data:
        digest_date = data['digest_date']
        if digest_date is not None and (
            not isinstance(digest_date, str)
            or datetime.fromisoformat(digest_date).date().isoformat() != digest_date
        ):
            raise ValueError('Private notification digest date is invalid.')
    for key in ('evidence_start', 'evidence_end'):
        value = data[key]
        if isinstance(value, str) and datetime.fromisoformat(value).tzinfo is None:
            raise ValueError('Private notification evidence bounds require an explicit timezone.')
    if pending['attempts'] == 0 and any(
        pending[key] is not None
        for key in (
            'first_dispatch_at',
            'attempt_started_at',
            'attempt_deadline',
            'last_completion_at',
        )
    ):
        raise ValueError('Unattempted notification contains attempt timestamps.')
    if pending['attempts'] > 0:
        started = pending['attempt_started_at']
        if (
            started is None
            or pending['first_dispatch_at'] is None
            or pending['attempt_deadline'] != started + REQUEST_TIMEOUT_SECONDS
        ):
            raise ValueError('Attempted notification lacks a valid durable reservation.')
    if pending['outcome'] == 'acknowledged' and (
        not pending['acknowledgement_id'] or pending['acknowledgement_at'] is None
    ):
        raise ValueError('Acknowledged notification lacks its provider receipt.')
    return pending.copy()


def validate_delivery_receipt(value: object) -> DeliveryReceipt:
    data = _object(value, 'DeliveryReceipt')
    for key in ('batch_id', 'reason'):
        _string(data, key)
    for key in ('evidence_start', 'evidence_end', 'acknowledgement_id'):
        _string(data, key, nullable=True)
    _number(data, 'completed_at')
    if data.get('outcome') not in {'rejected', 'uncertain', 'acknowledged'}:
        raise ValueError('Delivery receipt has an unknown outcome.')
    return cast(DeliveryReceipt, data).copy()


resume_delivery = attempt_delivery
