"""Recompute mixed-trial capacity from source-scoped minute observations.

These records are experiment evidence, not a second operational store. An elapsed
six-hour timer, successful request count, or author-supplied summary is insufficient.
"""

from __future__ import annotations

import math
from collections.abc import Mapping, Sequence
from datetime import UTC, datetime, timedelta
from typing import cast

MINUTE = timedelta(minutes=1)


class TrialEvidenceError(ValueError):
    """The retained trial observations do not establish the claimed measurement."""


def object_value(value: object, name: str) -> dict[str, object]:
    if not isinstance(value, dict):
        raise TrialEvidenceError(f'{name} must be an object.')
    return cast(dict[str, object], value)


def list_value(value: object, name: str) -> list[object]:
    if not isinstance(value, list):
        raise TrialEvidenceError(f'{name} must be an array.')
    return cast(list[object], value)


def instant(value: object, name: str) -> datetime:
    if not isinstance(value, str):
        raise TrialEvidenceError(f'{name} must be a UTC timestamp.')
    try:
        parsed = datetime.fromisoformat(value)
    except ValueError as error:
        raise TrialEvidenceError(f'{name} is not a timestamp.') from error
    if parsed.tzinfo is None or parsed.utcoffset() != timedelta(0):
        raise TrialEvidenceError(f'{name} must be timezone-aware UTC.')
    return parsed.astimezone(UTC)


def finite(value: object, name: str) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise TrialEvidenceError(f'{name} must be a finite number.')
    if not math.isfinite(value) or value < 0:
        raise TrialEvidenceError(f'{name} must be finite and nonnegative.')
    return float(value)


def count_value(value: object, name: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise TrialEvidenceError(f'{name} must be a nonnegative integer.')
    return value


def minutes(value: object, name: str) -> dict[datetime, dict[str, object]]:
    result: dict[datetime, dict[str, object]] = {}
    for item in list_value(value, name):
        row = object_value(item, name + '.row')
        minute = instant(row.get('minute'), name + '.minute')
        if minute.second or minute.microsecond or minute in result:
            raise TrialEvidenceError(f'{name} contains a non-minute or duplicate selected minute.')
        count_value(row.get('rows'), name + '.rows')
        digest = row.get('raw_sha256')
        if not isinstance(digest, str) or len(digest) != 64:
            raise TrialEvidenceError(f'{name} lacks an independently comparable raw digest.')
        try:
            int(digest, 16)
        except ValueError as error:
            raise TrialEvidenceError(f'{name} has an invalid digest.') from error
        result[minute] = row
    return result


def derive_capacity(
    progress: Mapping[str, object], *, source_keys: Sequence[str]
) -> dict[str, object]:
    """Return SS-05 measurements, deduplicating accepted units and excluding canonical credit.

    A progress sample holds the actual complete provisional minute inventory, oracle
    row/digest comparisons, a committed publication endpoint and a source clock.
    The source clock must advance at real time; accelerated replay cannot qualify.
    """
    if progress.get('kind') != 'steady_state_trial_progress' or progress.get('schema_version') != 1:
        raise TrialEvidenceError('Version-1 raw trial progress is required.')
    if progress.get('environment') != 'isolated':
        raise TrialEvidenceError('A controlled workload may run only in an isolated environment.')
    if progress.get('clock_rate') != 1:
        raise TrialEvidenceError(
            'Accelerated or paused input time is not real-time capacity proof.'
        )
    plan = object_value(progress.get('source_plan'), 'source_plan')
    if set(plan) != set(source_keys):
        raise TrialEvidenceError('The complete fixed source inventory is required.')
    samples = [
        object_value(item, 'sample') for item in list_value(progress.get('samples'), 'samples')
    ]
    if len(samples) < 2:
        raise TrialEvidenceError('At least two actual progress observations are required.')
    began = instant(samples[0].get('observed_at'), 'first observation')
    ended = instant(samples[-1].get('observed_at'), 'last observation')
    if finite(samples[0].get('elapsed_seconds'), 'initial elapsed') != 0:
        raise TrialEvidenceError('The initial observation must establish elapsed zero.')
    previous = -1.0
    for sample in samples:
        elapsed = finite(sample.get('elapsed_seconds'), 'elapsed_seconds')
        wall = (instant(sample.get('observed_at'), 'observed_at') - began).total_seconds()
        if abs(elapsed - wall) > 2 or elapsed <= previous:
            raise TrialEvidenceError('Monotonic elapsed and wall-clock observations disagree.')
        if previous >= 0 and elapsed - previous > 90:
            raise TrialEvidenceError(
                'A missing minute observation cannot count as healthy progress.'
            )
        states = object_value(sample.get('sources'), 'sample.sources')
        if set(states) != set(source_keys):
            raise TrialEvidenceError('Every observation must contain every required source.')
        previous = elapsed
    duration = finite(samples[-1].get('elapsed_seconds'), 'final elapsed')
    computed: dict[str, object] = {}
    normal_after: list[float] = []
    for key in source_keys:
        declaration = object_value(plan[key], key)
        origin = instant(declaration.get('source_time_at_start'), key + '.source_time_at_start')
        withheld = [
            instant(value, key + '.withheld')
            for value in list_value(declaration.get('withheld'), key + '.withheld')
        ]
        if not withheld or len(set(withheld)) != len(withheld):
            raise TrialEvidenceError(f'{key} must identify its distinct withheld interval.')
        withheld.sort()
        if any(value.second or value.microsecond for value in withheld):
            raise TrialEvidenceError('Withheld units must be closed UTC minutes.')
        if withheld != [withheld[0] + index * MINUTE for index in range(len(withheld))]:
            raise TrialEvidenceError('The withheld interval must be contiguous.')
        if withheld[-1] + MINUTE != origin.replace(second=0, microsecond=0):
            raise TrialEvidenceError('The withheld interval must end at the initial due boundary.')
        oracle = minutes(declaration.get('oracle'), key + '.oracle')
        if not set(withheld) <= set(oracle):
            raise TrialEvidenceError('Withheld source input is absent from the input oracle.')
        accepted: set[datetime] = set()
        drain: float | None = None
        recovered_useful = 0
        recovered_arrival = 0.0
        integrity_loss = 0
        extra_rows = 0
        canonical_credit: set[datetime] = set()
        last_bad = 0.0
        request_weight = 0.0
        rows_completed = 0
        expected_due = origin.replace(second=0, microsecond=0)
        for index, sample in enumerate(samples):
            elapsed = finite(sample['elapsed_seconds'], 'elapsed_seconds')
            state = object_value(object_value(sample['sources'], 'sources')[key], key)
            due = instant(state.get('due'), key + '.due')
            expected_due = (origin + timedelta(seconds=elapsed)).replace(second=0, microsecond=0)
            if due != expected_due:
                raise TrialEvidenceError(
                    'The due clock was accelerated, paused or moved to hide debt.'
                )
            current = minutes(state.get('accepted_minutes'), key + '.accepted_minutes')
            if index == 0 and current:
                raise TrialEvidenceError(
                    'The trial must start before its withheld work is admitted.'
                )
            if any(minute >= due for minute in current):
                raise TrialEvidenceError('An unclosed minute was counted as useful service.')
            expected = set()
            minute = withheld[0]
            while minute < due:
                expected.add(minute)
                minute += MINUTE
            if not expected <= set(oracle):
                raise TrialEvidenceError('The oracle does not cover all arriving source minutes.')
            for minute, row in current.items():
                if minute not in expected:
                    raise TrialEvidenceError('Unrequested input cannot increase useful service.')
                target = oracle[minute]
                if row['raw_sha256'] != target['raw_sha256']:
                    raise TrialEvidenceError(
                        'An accepted minute differs from independently retained input.'
                    )
                expected_rows = count_value(target['rows'], 'expected rows')
                actual_rows = count_value(row['rows'], 'actual rows')
                if minute not in accepted:
                    integrity_loss += max(0, expected_rows - actual_rows)
                    extra_rows += max(0, actual_rows - expected_rows)
                    rows_completed += actual_rows
            canonical = {
                instant(value, 'canonical minute')
                for value in list_value(state.get('canonical_minutes'), 'canonical_minutes')
            }
            canonical_credit.update(canonical)
            # Already verified provisional work remains useful after a canonical replacement.
            accepted.update(current)
            missing = expected - accepted
            reported = {
                instant(value, 'reported missing')
                for value in list_value(state.get('missing_minutes'), 'missing_minutes')
            }
            if missing != reported:
                raise TrialEvidenceError(
                    'Backlog reporting hid or invented required source minutes.'
                )
            cumulative_weight = finite(state.get('request_weight'), 'request_weight')
            if cumulative_weight < request_weight:
                raise TrialEvidenceError('Provider request accounting regressed.')
            request_weight = cumulative_weight
            if drain is None and index and not missing:
                drain = elapsed / 60
                recovered_useful = len(accepted)
                recovered_arrival = elapsed / 60
            frontier = withheld[0]
            while frontier < due and frontier in accepted:
                frontier += MINUTE
            publication = instant(state.get('published_through'), 'published_through')
            if publication > frontier:
                raise TrialEvidenceError(
                    'Publication claims coverage beyond verified contiguous input.'
                )
            # Require normal freshness after recovery, not just one momentary empty queue.
            if (due - frontier).total_seconds() > 120 or (due - publication).total_seconds() > 180:
                last_bad = elapsed
        normal_after.append(
            max(0.0, duration - max(last_bad, (drain or duration / 60) * 60)) / 3600
        )
        computed[key] = {
            'arrival_minutes': recovered_arrival if drain is not None else duration / 60,
            'useful_minutes': recovered_useful if drain is not None else len(accepted),
            'arrival_rows': sum(
                count_value(row['rows'], 'oracle rows')
                for minute, row in oracle.items()
                if withheld[0] <= minute < expected_due
            ),
            'useful_rows': rows_completed,
            'request_weight': request_weight,
            'canonical_replacement_minutes': len(canonical_credit),
            'withheld_minutes': len(withheld),
            'drain_minutes': drain,
            'lost_rows': integrity_loss,
            'duplicate_selected_rows': extra_rows,
            'hidden_backlog_minutes': 0,
        }
    return {
        'trial_start': began.isoformat(),
        'trial_end': ended.isoformat(),
        'normal_freshness_hours_after_recovery': min(normal_after),
        'sources': computed,
        'observation_count': len(samples),
    }
