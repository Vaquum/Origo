"""Offline verification of a sealed steady-state evidence bundle (PRD-0017 SS-01..SS-12).

The verifier reads the bundle written by ``capture.py`` (layout documented there) plus
the isolated trial/deploy artifacts, recomputes every frontier, lag, percentile and
counter from the retained raw records, and compares them with the code-owned policy.
It trusts no precomputed verdict: ``contiguous_end`` is rebuilt from the recorded
intervals, percentiles are nearest-rank over the complete bucket population, and any
bucket, entity, identity or artifact that is missing, unreadable, replayed or bound to
a different identity yields UNKNOWN or FAIL, never PASS.

Trial artifacts (``trials/*.json``; every document carries ``kind``, ``schema_version``,
``environment``, ``code_sha``, ``policy_sha256`` and ``inventory_sha256``)::

    capacity_trial.json      kind steady_state_capacity_trial: trial_start, trial_end,
                             normal_freshness_hours_after_recovery, sources{<key>:
                             {arrival_minutes, useful_minutes, arrival_rows, useful_rows,
                             request_weight, canonical_replacement_minutes, withheld_minutes,
                             drain_minutes, lost_rows, duplicate_selected_rows,
                             hidden_backlog_minutes}}
    fault_cases.json         kind steady_state_fault_cases: cases[{name, kind (worker |
                             provider | clickhouse | dagster | dead_owner | unseeded_render |
                             interrupted_render), interruption_minutes, resume_minutes,
                             debt_clear_minutes, reconcile_minutes, false_dead_attempts,
                             unaffected_freshness_breaches, commit_minutes,
                             restarted_from_zero}]
    deploy_verification.json kind steady_state_deploy_verification: deploy_id,
                             compose_ready_at, verified_at, required_daemons_ready,
                             credentials_and_mounts_usable, fresh_source_minute_advanced,
                             mount_publication_advanced, launched_maintenance_run{run_id,
                             status, ended_at}, skipped_stages[]
    monitor_detection.json   kind steady_state_monitor_detection: cases[{name, breach_at,
                             visible_at, alert_delivered_at}]

Operator host artifacts (``host/*.json``)::

    oom_events.json          {"observed_from", "observed_to", "events": [{"at", "container"}]}
    incidents.json           {"observed_from", "observed_to", "data_loss_events": [...]}
    declared_limits.json     {"limits": {<live feed asset>: <memory limit bytes>}}
"""

from __future__ import annotations

import gzip
import json
import math
import re
import statistics
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from itertools import pairwise
from pathlib import Path
from typing import cast

from origo.sources.contracts import Partition, SourceError

from .contracts import AcceptanceReport, MetricVerdict, Verdict
from .coverage import contiguous_end, coverage_from_intervals, selected_intervals
from .evidence_io import file_digest, validate_inventory
from .policy import (
    Inventory,
    Policy,
    Profile,
    bucket_of,
    canonical_json,
    nearest_rank,
    required_entities,
    rolling_extremes,
    sha256_bytes,
)

REPORT_SCHEMA_VERSION = 1
TERMINAL_RUN_STATUSES = frozenset({'SUCCESS', 'FAILURE', 'CANCELED'})
INTERRUPTION_CASES = ('worker', 'provider', 'clickhouse', 'dagster')
REQUIRED_FAULT_KINDS = (*INTERRUPTION_CASES, 'dead_owner', 'unseeded_render', 'interrupted_render')
DEPTH_LOOKBACK = timedelta(minutes=15)


class EvidenceError(Exception):
    """The bundle cannot be evaluated; the verdict is UNKNOWN."""


def _object(value: object, what: str) -> dict[str, object]:
    if not isinstance(value, dict):
        raise EvidenceError(f'{what} must be an object.')
    return cast(dict[str, object], value)


def _when(value: object, what: str) -> datetime:
    if not isinstance(value, str):
        raise EvidenceError(f'{what} must be an ISO-8601 instant.')
    parsed = datetime.fromisoformat(value)
    if parsed.tzinfo is None:
        raise EvidenceError(f'{what} must be timezone-aware.')
    return parsed.astimezone(UTC)


def _number(value: object) -> float | None:
    if isinstance(value, bool) or not isinstance(value, (int, float)) or not math.isfinite(value):
        return None
    return float(value)


def _optional_object(value: object, what: str) -> dict[str, object] | None:
    return None if value is None else _object(value, what)


def _observed(probe: object) -> dict[str, object] | None:
    if not isinstance(probe, dict):
        return None
    data = cast(dict[str, object], probe)
    return data if data.get('status') == 'observed' else None


@dataclass
class Bundle:
    root: Path
    manifest_sha256: str
    identities: dict[str, dict[str, object]]
    samples: dict[datetime, dict[str, object]]
    rejected: list[str]
    duplicates: int
    activations: list[tuple[datetime, list[dict[str, object]]]]
    trials: dict[str, dict[str, object]]
    host: dict[str, dict[str, object]]
    files: dict[str, str] = field(default_factory=dict[str, str])


def load_bundle(root: Path, period: timedelta) -> Bundle:
    """Read and hash-check every artifact; reject replayed, duplicate or unbound samples."""
    manifest_path = root / 'manifest.json'
    if not manifest_path.is_file():
        raise EvidenceError(f'{manifest_path} is missing.')
    payload = manifest_path.read_bytes()
    manifest = _object(json.loads(payload), 'manifest')
    if manifest.get('kind') != 'steady_state_evidence' or manifest.get('schema_version') != 1:
        raise EvidenceError('manifest.json is not a version-1 steady_state_evidence manifest.')
    listed = _object(manifest.get('files'), 'manifest.files')
    try:
        validated = validate_inventory(root, list(listed))
    except (ValueError, OSError) as error:
        raise EvidenceError(str(error)) from error
    present = {
        path.relative_to(root).as_posix()
        for path in root.rglob('*')
        if path.is_file()
        and path.relative_to(root).as_posix() not in ('manifest.json', 'capture_state.json')
    }
    if set(listed) != present:
        missing = sorted(set(listed) - present)[:5]
        extra = sorted(present - set(listed))[:5]
        raise EvidenceError(f'manifest/file mismatch: missing {missing}, unlisted {extra}.')
    files: dict[str, str] = {}
    for relative, entry in listed.items():
        expected = str(_object(entry, relative).get('sha256'))
        digest = file_digest(validated[relative])
        if digest != expected:
            raise EvidenceError(f'{relative} does not match its manifest digest.')
        files[relative] = digest
    identities: dict[str, dict[str, object]] = {}
    for relative in sorted(files):
        if relative.startswith('identity/') and relative.endswith('.json'):
            document = _object(json.loads((root / relative).read_bytes()), relative)
            digest = str(document.get('runtime_identity_sha256'))
            if (
                relative != f'identity/{digest}.json'
                or document.get('kind') != 'steady_state_identity'
            ):
                raise EvidenceError(f'{relative} is not a bound identity record.')
            claimed = {
                key: value for key, value in document.items() if key != 'runtime_identity_sha256'
            }
            if sha256_bytes(canonical_json(claimed)) != digest:
                raise EvidenceError(f'{relative} identity content does not match its fingerprint.')
            identities[digest] = document
    samples: dict[datetime, dict[str, object]] = {}
    rejected: list[str] = []
    duplicates = 0
    for relative in sorted(files):
        if not (relative.startswith('samples/') and relative.endswith('.jsonl')):
            continue
        for index, line in enumerate((root / relative).read_bytes().splitlines(), start=1):
            where = f'{relative}:{index}'
            sample = _object(json.loads(line), where)
            if sample.get('kind') != 'steady_state_sample' or sample.get('schema_version') != 1:
                rejected.append(f'{where}: not a version-1 sample')
                continue
            bucket = _when(sample.get('bucket'), f'{where}.bucket')
            started = _when(sample.get('observed_start'), f'{where}.observed_start')
            finished = _when(sample.get('observed_end'), f'{where}.observed_end')
            expected_id = sha256_bytes(
                f'{sample["bucket"]}|{sample["observed_start"]}|{sample.get("host")}'.encode()
            )
            if bucket_of(started, int(period.total_seconds())) != bucket or finished < started:
                rejected.append(f'{where}: observed outside its bucket (replayed or backfilled)')
                continue
            if sample.get('sample_id') != expected_id:
                rejected.append(f'{where}: sample_id does not bind bucket, observation and host')
                continue
            if str(sample.get('identity_sha256')) not in identities:
                rejected.append(f'{where}: identity is not in the bundle')
                continue
            if bucket in samples:
                duplicates += 1
                continue
            sample['_ref'] = where
            samples[bucket] = sample
    activations: list[tuple[datetime, list[dict[str, object]]]] = []
    for relative in sorted(files):
        if relative.startswith('activations/') and relative.endswith('.jsonl.gz'):
            lines = gzip.decompress((root / relative).read_bytes()).splitlines()
            if not lines:
                raise EvidenceError(f'{relative} is empty.')
            header = _object(json.loads(lines[0]), relative)
            if header.get('kind') != 'steady_state_activations':
                raise EvidenceError(f'{relative} is not an activation snapshot.')
            rows = [_object(json.loads(line), relative) for line in lines[1:]]
            if len(rows) != header.get('rows'):
                raise EvidenceError(f'{relative} row count does not match its header.')
            activations.append((_when(header.get('bucket'), relative), rows))
    trials = {
        Path(relative).stem: _object(json.loads((root / relative).read_bytes()), relative)
        for relative in files
        if relative.startswith('trials/') and relative.endswith('.json')
    }
    host = {
        Path(relative).stem: _object(json.loads((root / relative).read_bytes()), relative)
        for relative in files
        if relative.startswith('host/') and relative.endswith('.json')
    }
    return Bundle(
        root,
        sha256_bytes(payload),
        identities,
        samples,
        rejected,
        duplicates,
        activations,
        trials,
        host,
        files,
    )


class Evaluator:
    def __init__(
        self, bundle: Bundle, policy: Policy, inventory: Inventory, profile: Profile
    ) -> None:
        self.bundle = bundle
        self.policy = policy
        self.inventory = inventory
        self.profile: Profile = profile
        self.period = timedelta(seconds=policy.sample_period_seconds)
        self.buckets = sorted(bundle.samples)
        self.results: list[MetricVerdict] = []
        self.denominators: dict[str, object] = {}
        self.entities = required_entities(inventory)
        if self.buckets:
            self.window_start, self.window_end = self.buckets[0], self.buckets[-1]
            self.grid = [
                self.window_start + self.period * index for index in range(self._expected())
            ]
        else:
            self.window_start = self.window_end = datetime(1970, 1, 1, tzinfo=UTC)
            self.grid = []

    def _expected(self) -> int:
        return int((self.window_end - self.window_start) / self.period) + 1

    def _record(
        self,
        metric_id: str,
        entity: str,
        statistic: str,
        observed: float | None,
        refs: Iterable[str],
        reason: str,
    ) -> None:
        bound = self.policy.bound(metric_id, statistic)
        verdict: Verdict
        if observed is None:
            verdict = 'UNKNOWN'
        else:
            verdict = 'PASS' if bound.holds(observed) else 'FAIL'
        self.results.append(
            MetricVerdict(
                metric_id,
                entity,
                statistic,
                observed,
                bound.bound,
                bound.unit,
                verdict,
                tuple(sorted(set(refs)))[:50],
                reason,
            )
        )

    def _ref(self, bucket: datetime) -> str:
        return str(self.bundle.samples[bucket].get('_ref'))

    def _series(
        self,
        metric_id: str,
        entity: str,
        values: Sequence[tuple[datetime, float | None]],
        names: Mapping[str, str],
        refs: Iterable[str],
        expected: int | None = None,
    ) -> None:
        """Percentile/max statistics over a per-bucket series on the full grid and every
        contained rolling window; a hole or unknown bucket makes them UNKNOWN."""
        expected = len(self.grid) if expected is None else expected
        unknown = [bucket for bucket, value in values if value is None]
        known = [(bucket, value) for bucket, value in values if value is not None]
        holes = expected - len(values)
        if holes or unknown or not known:
            reason = f'{holes} missing buckets, {len(unknown)} unknown buckets of {expected}'
            if unknown:
                reason += f'; first unknown {unknown[0].isoformat()}'
            for statistic in names.values():
                self._record(metric_id, entity, statistic, None, refs, reason)
            return
        population = [value for _, value in known]
        worst: dict[str, float] = {}
        if 'p95' in names:
            worst['p95'] = nearest_rank(population, 95)
        if 'p99' in names:
            worst['p99'] = nearest_rank(population, 99)
        worst['max'] = max(population)
        windows = 1
        rolling = timedelta(hours=self.policy.rolling_window_hours)
        if 'rolling_24h' in self.policy.metrics[metric_id].windows and len(known) >= 2:
            for _, ranks, top in rolling_extremes(
                known, window=rolling, period=self.period, percentiles=(95, 99)
            ):
                windows += 1
                if 'p95' in names:
                    worst['p95'] = max(worst['p95'], ranks[95])
                if 'p99' in names:
                    worst['p99'] = max(worst['p99'], ranks[99])
                worst['max'] = max(worst['max'], top)
        reason = f'{len(population)} buckets, {windows} windows (full + rolling {self.policy.rolling_window_hours} h)'
        for key, statistic in names.items():
            if key in worst:
                self._record(metric_id, entity, statistic, worst[key], refs, reason)

    def _count(
        self,
        metric_id: str,
        entity: str,
        statistic: str,
        values: Sequence[tuple[datetime, float | None]],
        refs: Iterable[str],
        *,
        reduce: str = 'max',
    ) -> None:
        unknown = [bucket for bucket, value in values if value is None]
        holes = len(self.grid) - len(values)
        if holes or unknown or not values:
            self._record(
                metric_id,
                entity,
                statistic,
                None,
                refs,
                f'{holes} missing, {len(unknown)} unknown buckets of {len(self.grid)}',
            )
            return
        population = [cast(float, value) for _, value in values]
        observed = (
            max(population)
            if reduce == 'max'
            else (min(population) if reduce == 'min' else sum(population))
        )
        self._record(metric_id, entity, statistic, observed, refs, f'{len(population)} buckets')

    # SS-01 -----------------------------------------------------------------------------
    def _source_frontier(self, bucket: datetime, key: str) -> tuple[datetime, datetime] | None:
        probe = _observed(_object(self.bundle.samples[bucket].get('sources'), 'sources').get(key))
        if probe is None:
            return None
        due = _when(probe.get('due'), 'due')
        if due != bucket:
            raise EvidenceError(
                f'{self._ref(bucket)}: {key} due {due.isoformat()} is not the bucket.'
            )
        prefix_end = _when(probe.get('prefix_end'), 'prefix_end')
        tail = tuple(
            Partition(str(item[0]), _when(item[1], 'start'), _when(item[2], 'end'), bool(item[3]))
            for item in cast(list[list[object]], probe.get('tail_intervals') or [])
        )
        frontier = contiguous_end(prefix_end, tail)
        if frontier != _when(probe.get('contiguous_end'), 'contiguous_end'):
            raise EvidenceError(
                f'{self._ref(bucket)}: {key} contiguous_end is not reproducible from its intervals.'
            )
        return due, frontier

    def evaluate_ss01(self) -> None:
        refs = [self._ref(bucket) for bucket in self.buckets]
        snapshots = self._canonical_gap_minutes()
        for key in self.entities['source']:
            lags: list[tuple[datetime, float | None]] = []
            incomplete: list[tuple[datetime, float | None]] = []
            for bucket in self.buckets:
                found = self._source_frontier(bucket, key)
                probe = _observed(
                    _object(self.bundle.samples[bucket].get('sources'), 'sources').get(key)
                )
                if found is None or probe is None:
                    lags.append((bucket, None))
                    incomplete.append((bucket, None))
                    continue
                due, frontier = found
                lags.append((bucket, max(0.0, (due - frontier).total_seconds())))
                incomplete.append((bucket, _number(probe.get('incomplete_partition_count'))))
            self._series(
                'SS-01',
                key,
                lags,
                {'p95': 'lag_seconds_p95', 'p99': 'lag_seconds_p99', 'max': 'lag_seconds_max'},
                refs,
            )
            self._count('SS-01', key, 'uncovered_due_minute_age_seconds_max', lags, refs)
            self._count('SS-01', key, 'incomplete_component_partitions_max', incomplete, refs)
            gap = snapshots.get(key)
            self._record(
                'SS-01',
                key,
                'historical_canonical_gap_minutes_max',
                None if gap is None else float(gap[0]),
                [] if gap is None else gap[1],
                'no activation snapshot in the bundle'
                if gap is None
                else f'{len(gap[1])} activation snapshots',
            )
            self.denominators[f'SS-01:{key}'] = {
                'buckets': len(lags),
                'known': sum(1 for _, value in lags if value is not None),
            }

    def _canonical_gap_minutes(self) -> dict[str, tuple[int, list[str]]]:
        found: dict[str, tuple[int, list[str]]] = {}
        for bucket, rows in self.bundle.activations:
            for key in self.entities['source']:
                anchors = [
                    row
                    for row in rows
                    if row.get('table') == 'source_anchor_log' and row.get('source_key') == key
                ]
                if (
                    len(anchors) != 1
                    or len(cast(list[object], anchors[0].get('anchors') or [])) != 1
                ):
                    continue
                anchor = _when(cast(list[object], anchors[0]['anchors'])[0], 'anchor')
                canonical = tuple(
                    Partition(
                        str(row['partition_key']),
                        _when(row['partition_start'], 'start'),
                        _when(row['partition_end'], 'end'),
                    )
                    for row in rows
                    if row.get('table') == 'source_active_partitions'
                    and row.get('source_key') == key
                    and not row.get('provisional')
                )
                if not canonical:
                    continue
                latest = max(item.end for item in canonical)
                coverage = coverage_from_intervals(anchor, canonical, latest)
                previous = found.get(key, (0, []))
                found[key] = (
                    max(previous[0], coverage.missing_minutes),
                    [*previous[1], f'activations/{bucket:%Y%m%dT%H%M%SZ}.jsonl.gz'],
                )
        return found

    # SS-02 -----------------------------------------------------------------------------
    def _consumer_state(
        self, bucket: datetime, name: str
    ) -> tuple[dict[str, object] | None, int, int]:
        """(manifest, invalid series count, listed series count) or (None, 12, 0) when unknown."""
        probe = _observed(
            _object(self.bundle.samples[bucket].get('consumers'), 'consumers').get(name)
        )
        if probe is None:
            return None, 12, 0
        manifest = _object(probe.get('manifest'), 'manifest')
        series = _object(probe.get('series'), 'series')
        invalid = 0
        for declared in self.inventory.sources[name.split(':')[0]].series:
            checks = _object(series.get(declared.name) or {}, declared.name)
            artifacts = [
                checks.get(part) for part in ('month', 'arrow', 'snapshot') if part in checks
            ]
            if not artifacts or not all(self._artifact_valid(artifact) for artifact in artifacts):
                invalid += 1
        return manifest, invalid, len(series)

    @staticmethod
    def _artifact_valid(artifact: object) -> bool:
        checks = cast(dict[str, object], artifact) if isinstance(artifact, dict) else {}
        return (
            bool(checks.get('listed'))
            and bool(checks.get('exists'))
            and bool(checks.get('sha256_verified'))
            and bool(checks.get('readable'))
        )

    def evaluate_ss02(self) -> None:
        refs = [self._ref(bucket) for bucket in self.buckets]
        for name in self.entities['mount']:
            key = name.split(':')[0]
            lags: list[tuple[datetime, float | None]] = []
            invalid_buckets: list[tuple[datetime, float | None]] = []
            forward: list[tuple[datetime, float | None]] = []
            listed: list[tuple[datetime, float | None]] = []
            for bucket in self.buckets:
                manifest, invalid, count = self._consumer_state(bucket, name)
                listed.append((bucket, float(count) if manifest is not None else None))
                if manifest is None or not manifest.get('exists'):
                    lags.append((bucket, None))
                    invalid_buckets.append((bucket, None if manifest is None else 12.0))
                    forward.append((bucket, None if manifest is None else 0.0))
                    continue
                delivered = _when(manifest.get('active_through'), 'active_through')
                source = self._source_frontier(bucket, key)
                probe = _observed(
                    _object(self.bundle.samples[bucket].get('sources'), 'sources').get(key)
                )
                newest = _when(probe.get('newest_end'), 'newest_end') if probe is not None else None
                claims = (
                    1.0
                    if delivered > bucket or (newest is not None and delivered > newest)
                    else 0.0
                )
                invalid_buckets.append((bucket, float(invalid)))
                forward.append((bucket, claims if source is not None else None))
                lags.append(
                    (
                        bucket,
                        max(0.0, (bucket - delivered).total_seconds())
                        if invalid == 0 and claims == 0.0
                        else None,
                    )
                )
            self._series(
                'SS-02',
                name,
                lags,
                {'p95': 'lag_seconds_p95', 'p99': 'lag_seconds_p99', 'max': 'lag_seconds_max'},
                refs,
            )
            self._count('SS-02', name, 'series_missing_or_unreadable_max', invalid_buckets, refs)
            self._count('SS-02', name, 'required_series_count', listed, refs, reduce='min')
            self._count(
                'SS-02', name, 'unsupported_forward_claims_max', forward, refs, reduce='sum'
            )
            self.denominators[f'SS-02:{name}'] = {
                'buckets': len(lags),
                'known': sum(1 for _, value in lags if value is not None),
            }

    # SS-03 -----------------------------------------------------------------------------
    def evaluate_ss03(self) -> None:
        refs = [self._ref(bucket) for bucket in self.buckets]
        for key in self.entities['depth']:
            first_seen: dict[datetime, tuple[float, bool]] = {}
            unknown_buckets: set[datetime] = set()
            for bucket in self.buckets:
                probe = _observed(
                    _object(self.bundle.samples[bucket].get('depth'), 'depth').get(key)
                )
                if probe is None:
                    unknown_buckets.add(bucket)
                    continue
                for stamp, entry in _object(probe.get('minutes'), 'minutes').items():
                    minute = _when(stamp, 'minute')
                    data = _object(entry, stamp)
                    mtime = _number(data.get('chunk_mtime_ns'))
                    if data.get('chunk_exists') and mtime is not None and minute not in first_seen:
                        valid = (_number(data.get('snapshot_rows')) or 0) > 0 and (
                            _number(data.get('projection_rows')) or 0
                        ) > 0
                        first_seen[minute] = (
                            mtime / 1e9 - (minute + self.period).timestamp(),
                            valid,
                        )
            closed = [minute for minute in self.grid if minute + DEPTH_LOOKBACK <= self.window_end]
            delays: list[tuple[datetime, float | None]] = []
            invalid = 0
            unknown = 0
            for minute in closed:
                seen = first_seen.get(minute)
                observed_all = all(
                    minute + self.period * offset not in unknown_buckets
                    for offset in range(1, int(DEPTH_LOOKBACK / self.period) + 1)
                )
                if seen is None:
                    if observed_all:
                        invalid += 1
                        delays.append((minute, None))
                    else:
                        unknown += 1
                        delays.append((minute, None))
                    continue
                delay, valid = seen
                if not valid:
                    invalid += 1
                delays.append((minute, max(0.0, delay)))
            self._series(
                'SS-03',
                key,
                delays,
                {'p95': 'close_to_arrow_seconds_p95', 'max': 'close_to_arrow_seconds_max'},
                refs,
                expected=len(closed),
            )
            self._record(
                'SS-03',
                key,
                'invalid_closed_minutes_max',
                None if unknown else float(invalid),
                refs,
                f'{len(closed)} closed minutes, {invalid} invalid, {unknown} unobserved',
            )
            self.denominators[f'SS-03:{key}'] = {
                'closed_minutes': len(closed),
                'measured': len(first_seen),
                'invalid': invalid,
                'unobserved': unknown,
            }

    # SS-04 -----------------------------------------------------------------------------
    def evaluate_ss04(self) -> None:
        refs = [self._ref(bucket) for bucket in self.buckets]
        latest = max(self.bundle.activations, key=lambda item: item[0], default=None)
        for key in self.entities['source']:
            daily = [
                f'{key}:{consumer.key}'
                for consumer in self.inventory.sources[key].consumers
                if consumer.cadence == 'daily'
            ]
            ages: list[tuple[datetime, float | None]] = []
            for bucket in self.buckets:
                worst: float | None = 0.0
                for name in daily:
                    manifest, _, _ = self._consumer_state(bucket, name)
                    if manifest is None or not manifest.get('exists'):
                        worst = None
                        break
                    age = (
                        bucket - _when(manifest.get('active_through'), 'active_through')
                    ).total_seconds() / 3600
                    worst = max(worst, age)
                ages.append((bucket, worst))
            self._count('SS-04', key, 'delivered_age_hours_max', ages, refs)
            if latest is None:
                for statistic in (
                    'archive_detection_minutes_max',
                    'activation_minutes_max',
                    'consumer_completion_minutes_max',
                    'missing_available_days_max',
                    'availability_poll_interval_minutes_max',
                ):
                    self._record(
                        'SS-04', key, statistic, None, [], 'no activation snapshot in the bundle'
                    )
                continue
            bucket, rows = latest
            snapshot_ref = f'activations/{bucket:%Y%m%dT%H%M%SZ}.jsonl.gz'
            days = [
                row
                for row in rows
                if row.get('table') == 'source_active_partitions'
                and row.get('source_key') == key
                and not row.get('provisional')
            ]
            observations = [
                row
                for row in rows
                if row.get('table') == 'source_observation_log' and row.get('source_key') == key
            ]
            self._record(
                'SS-04',
                key,
                'missing_available_days_max',
                float(self._missing_days(days)),
                [snapshot_ref],
                f'{len(days)} canonical days in the newest snapshot',
            )
            activated = [
                (
                    str(row['partition_key']),
                    _when(row['partition_end'], 'end'),
                    _when(row['activated_at'], 'activated_at'),
                )
                for row in days
                if self.window_start
                <= _when(row['activated_at'], 'activated_at')
                <= self.window_end
            ]
            detection: list[float] = []
            activation: list[float] = []
            completion: list[float] = []
            pending = 0
            missing_observation = 0
            for partition, end, activated_at in activated:
                seen = sorted(
                    (_when(row['observed_at'], 'observed_at'), bool(row.get('complete')))
                    for row in observations
                    if row.get('partition_key') == partition
                )
                available = next((stamp for stamp, complete in seen if complete), None)
                unavailable = [
                    stamp
                    for stamp, complete in seen
                    if not complete and available is not None and stamp < available
                ]
                if available is None:
                    missing_observation += 1
                else:
                    activation.append((activated_at - available).total_seconds() / 60)
                    if unavailable:
                        detection.append((available - unavailable[-1]).total_seconds() / 60)
                    else:
                        missing_observation += 1
                done = next(
                    (
                        candidate
                        for candidate in self.buckets
                        if candidate >= activated_at
                        and all(
                            (state := self._consumer_state(candidate, name))[0] is not None
                            and bool(state[0].get('exists'))
                            and _when(state[0].get('active_through'), 'active_through') >= end
                            for name in daily
                        )
                    ),
                    None,
                )
                if done is not None:
                    completion.append(
                        (
                            _when(self.bundle.samples[done].get('observed_start'), 'observed_start')
                            - activated_at
                        ).total_seconds()
                        / 60
                    )
                elif self.window_end - activated_at < timedelta(minutes=60):
                    pending += 1
                else:
                    completion.append((self.window_end - activated_at).total_seconds() / 60)
            reason = f'{len(activated)} canonical activations in window, {missing_observation} without availability observations, {pending} pending'
            self._record(
                'SS-04',
                key,
                'archive_detection_minutes_max',
                max(detection) if detection and not missing_observation else None,
                [snapshot_ref],
                reason,
            )
            self._record(
                'SS-04',
                key,
                'activation_minutes_max',
                max(activation) if activation and not missing_observation else None,
                [snapshot_ref],
                reason,
            )
            self._record(
                'SS-04',
                key,
                'consumer_completion_minutes_max',
                max(completion) if completion and not pending else None,
                refs,
                reason,
            )
            polls = sorted(_when(row['observed_at'], 'observed_at') for row in observations)
            gaps = [(later - earlier).total_seconds() / 60 for earlier, later in pairwise(polls)]
            self._record(
                'SS-04',
                key,
                'availability_poll_interval_minutes_max',
                max(gaps) if gaps else None,
                [snapshot_ref],
                f'{len(polls)} canonical availability observations in the snapshot',
            )
            self.denominators[f'SS-04:{key}'] = {
                'activations': len(activated),
                'observations': len(polls),
            }

    @staticmethod
    def _missing_days(days: Sequence[dict[str, object]]) -> int:
        if not days:
            return 0
        ordered = sorted(_when(row['partition_start'], 'start').date() for row in days)
        return (ordered[-1] - ordered[0]).days + 1 - len(set(ordered))

    # SS-07 -----------------------------------------------------------------------------
    def _dagster(self, bucket: datetime) -> dict[str, object] | None:
        return _observed(self.bundle.samples[bucket].get('dagster'))

    def evaluate_ss07(self) -> None:
        refs = [self._ref(bucket) for bucket in self.buckets]
        queued: list[tuple[datetime, float | None]] = []
        maintenance: list[tuple[datetime, float | None]] = []
        duplicates: list[tuple[datetime, float | None]] = []
        runs: dict[str, dict[str, object]] = {}
        terminal_claims: dict[tuple[str, str], list[datetime]] = {}
        pools_unknown = False
        for bucket in self.buckets:
            probe = self._dagster(bucket)
            if probe is None:
                queued.append((bucket, None))
                maintenance.append((bucket, None))
                duplicates.append((bucket, None))
                pools_unknown = True
                continue
            queued.append((bucket, _number(probe.get('queued_runs'))))
            listed = [
                _object(item, 'run') for item in cast(list[object], probe.get('queued') or [])
            ]
            identities = [
                (run.get('job_name'), run.get('partition'), run.get('backfill')) for run in listed
            ]
            duplicates.append((bucket, float(len(identities) - len(set(identities)))))
            success = [
                _object(item, 'run')
                for item in cast(
                    list[object],
                    _object(probe.get('maintenance'), 'maintenance').get('latest_success') or [],
                )
            ]
            ended = _number(success[0].get('end_time')) if success else None
            maintenance.append(
                (bucket, None if ended is None else (bucket.timestamp() - ended) / 60)
            )
            for run in [
                *listed,
                *(_object(item, 'run') for item in cast(list[object], probe.get('recent') or [])),
            ]:
                runs[str(run.get('run_id'))] = run
            for pool in cast(list[object], probe.get('pools') or []):
                for slot in cast(list[object], _object(pool, 'pool').get('claimed') or []):
                    claim = _object(slot, 'slot')
                    run = runs.get(str(claim.get('run_id')))
                    if run is not None and run.get('status') in TERMINAL_RUN_STATUSES:
                        terminal_claims.setdefault(
                            (str(claim.get('run_id')), str(claim.get('step'))), []
                        ).append(bucket)
        self._count('SS-07', 'dagster', 'queued_runs_max', queued, refs)
        self._count('SS-07', 'dagster', 'maintenance_success_age_minutes_max', maintenance, refs)
        self._count('SS-07', 'dagster', 'duplicate_queued_identities_max', duplicates, refs)
        abandoned = sum(
            1 for seen in terminal_claims.values() if max(seen) - min(seen) > timedelta(minutes=5)
        )
        self._record(
            'SS-07',
            'dagster',
            'abandoned_pool_claims_over_5m_max',
            None if pools_unknown else float(abandoned),
            refs,
            f'{len(terminal_claims)} terminal claims observed',
        )
        dispatched = [
            (
                datetime.fromtimestamp(cast(float, _number(run.get('created_at'))), UTC),
                cast(float, _number(run.get('start_time')))
                - cast(float, _number(run.get('created_at'))),
            )
            for run in runs.values()
            if _number(run.get('created_at')) is not None
            and _number(run.get('start_time')) is not None
            and self.window_start
            <= datetime.fromtimestamp(cast(float, _number(run.get('created_at'))), UTC)
            <= self.window_end
        ]
        dispatched.sort()
        if not dispatched or pools_unknown:
            reason = (
                'no started runs observed'
                if not dispatched
                else 'dagster probe unknown in some buckets'
            )
            self._record('SS-07', 'dagster', 'dispatch_delay_seconds_p95', None, refs, reason)
            self._record('SS-07', 'dagster', 'dispatch_delay_seconds_max', None, refs, reason)
        else:
            delays = [max(0.0, delay) for _, delay in dispatched]
            p95, top = nearest_rank(delays, 95), max(delays)
            rolling = timedelta(hours=self.policy.rolling_window_hours)
            for end in self.grid[::60]:
                window = [delay for created, delay in dispatched if end - rolling < created <= end]
                if window:
                    p95, top = max(p95, nearest_rank(window, 95)), max(top, max(window))
            self._record(
                'SS-07',
                'dagster',
                'dispatch_delay_seconds_p95',
                p95,
                refs,
                f'{len(delays)} started runs',
            )
            self._record(
                'SS-07',
                'dagster',
                'dispatch_delay_seconds_max',
                top,
                refs,
                f'{len(delays)} started runs',
            )
        routine = [
            datetime.fromtimestamp(cast(float, _number(run.get('created_at'))), UTC)
            for run in runs.values()
            if _number(run.get('created_at')) is not None
            and run.get('operation') != 'backfill'
            and not run.get('backfill')
        ]
        if pools_unknown or self.window_end - self.window_start < timedelta(hours=24):
            self._record(
                'SS-07',
                'dagster',
                'routine_runs_per_24h_max',
                None,
                refs,
                'window shorter than 24 h or dagster probe unknown',
            )
        else:
            worst = 0
            for end in self.grid[::60]:
                if end - self.window_start >= timedelta(hours=24):
                    worst = max(
                        worst,
                        sum(1 for created in routine if end - timedelta(hours=24) < created <= end),
                    )
            self._record(
                'SS-07',
                'dagster',
                'routine_runs_per_24h_max',
                float(worst),
                refs,
                f'{len(routine)} routine runs observed',
            )
        self.denominators['SS-07:dagster'] = {
            'buckets': len(queued),
            'runs': len(runs),
            'started': len(dispatched),
        }

    # SS-09 -----------------------------------------------------------------------------
    def evaluate_ss09(self) -> None:
        refs = [self._ref(bucket) for bucket in self.buckets]
        oom = self.bundle.host.get('oom_events')
        incidents = self.bundle.host.get('incidents')
        limits = self.bundle.host.get('declared_limits')
        self._record(
            'SS-09',
            'host',
            'oom_kills_max',
            self._host_events(oom, 'events'),
            ['host/oom_events.json'],
            'host/oom_events.json' if oom else 'host/oom_events.json is absent',
        )
        self._record(
            'SS-09',
            'host',
            'data_loss_events_max',
            self._host_events(incidents, 'data_loss_events'),
            ['host/incidents.json'],
            'host/incidents.json' if incidents else 'host/incidents.json is absent',
        )
        watchdog: list[tuple[datetime, float | None]] = []
        ram: list[tuple[datetime, float | None]] = []
        disk: list[tuple[datetime, float | None]] = []
        working: list[tuple[datetime, float | None]] = []
        orphans: set[str] = set()
        orphan_unknown = False
        for bucket in self.buckets:
            logs = _observed(self.bundle.samples[bucket].get('container_log'))
            watchdog.append(
                (
                    bucket,
                    None
                    if logs is None
                    else float(
                        sum(
                            _number(_object(entry, 'service').get('watchdog_exits')) or 0.0
                            for entry in _object(logs.get('services'), 'services').values()
                        )
                    ),
                )
            )
            resources = _observed(self.bundle.samples[bucket].get('resources'))
            if resources is None:
                ram.append((bucket, None))
                disk.append((bucket, None))
                working.append((bucket, None))
                orphan_unknown = True
                continue
            memory = _observed(resources.get('host_memory'))
            ram.append((bucket, None if memory is None else _number(memory.get('available_ratio'))))
            ratios: list[float | None] = []
            for entry in _object(resources.get('disks'), 'disks').values():
                observed_disk = _observed(entry)
                ratios.append(
                    None if observed_disk is None else _number(observed_disk.get('reserve_ratio'))
                )
            known_ratios = [ratio for ratio in ratios if ratio is not None]
            disk.append(
                (bucket, min(known_ratios) if ratios and len(known_ratios) == len(ratios) else None)
            )
            working.append((bucket, self._working_set_ratio(bucket, limits)))
            dagster = self._dagster(bucket)
            success = None
            if dagster is not None:
                latest = [
                    _object(item, 'run')
                    for item in cast(
                        list[object],
                        _object(dagster.get('maintenance'), 'maintenance').get('latest_success')
                        or [],
                    )
                ]
                success = _number(latest[0].get('end_time')) if latest else None
            for orphan in cast(list[object], resources.get('staging_orphans') or []):
                entry = _object(orphan, 'orphan')
                age = _number(entry.get('age_seconds')) or 0.0
                if age <= 3600:
                    continue
                if success is None:
                    orphan_unknown = True
                elif success >= bucket.timestamp() - age + 3600:
                    orphans.add(str(entry.get('path')))
        self._count(
            'SS-09', 'host', 'unplanned_watchdog_restarts_max', watchdog, refs, reduce='sum'
        )
        self._count('SS-09', 'host', 'host_ram_reserve_ratio_min', ram, refs, reduce='min')
        self._count('SS-09', 'host', 'disk_reserve_ratio_min', disk, refs, reduce='min')
        self._count('SS-09', 'host', 'container_working_set_ratio_max', working, refs)
        self._record(
            'SS-09',
            'host',
            'staging_orphans_over_60m_surviving_maintenance_max',
            None if orphan_unknown else float(len(orphans)),
            refs,
            f'{len(orphans)} orphans survived a later maintenance pass',
        )
        self.denominators['SS-09:host'] = {'buckets': len(self.buckets)}

    def _host_events(self, document: dict[str, object] | None, key: str) -> float | None:
        if document is None:
            return None
        covered_from = _when(document.get('observed_from'), 'observed_from')
        covered_to = _when(document.get('observed_to'), 'observed_to')
        if covered_from > self.window_start or covered_to < self.window_end:
            return None
        return float(len(cast(list[object], document.get(key) or [])))

    def _working_set_ratio(
        self, bucket: datetime, limits: dict[str, object] | None
    ) -> float | None:
        dagster = self._dagster(bucket)
        if dagster is None or limits is None:
            return None
        declared = _object(limits.get('limits'), 'limits')
        worst = 0.0
        for asset, event in _object(dagster.get('live_feeds'), 'live_feeds').items():
            limit = _number(declared.get(asset))
            latest = _optional_object(event, asset)
            rss = (
                None
                if latest is None
                else _number(_object(latest.get('metadata'), 'metadata').get('rss_bytes'))
            )
            if limit is None or rss is None or limit <= 0:
                return None
            worst = max(worst, rss / limit)
        return worst

    # SS-10 -----------------------------------------------------------------------------
    def evaluate_ss10(self) -> None:
        refs = [self._ref(bucket) for bucket in self.buckets]
        identity = self._identity()
        discrepancies = (
            cast(list[object], identity.get('inventory_discrepancies') or []) if identity else None
        )
        for key in self.entities['source']:
            invalid = conflicting = missing = 0
            snapshots = 0
            for _, rows in self.bundle.activations:
                snapshots += 1
                active = [
                    row
                    for row in rows
                    if row.get('table') == 'source_active_partitions'
                    and row.get('source_key') == key
                ]
                identities = [
                    (row.get('partition_key'), bool(row.get('provisional'))) for row in active
                ]
                conflicting += len(identities) - len(set(identities))
                intervals = tuple(
                    Partition(
                        str(row['partition_key']),
                        _when(row['partition_start'], 'start'),
                        _when(row['partition_end'], 'end'),
                        bool(row.get('provisional')),
                    )
                    for row in active
                )
                try:
                    selected_intervals(intervals)
                except SourceError:
                    invalid += 1
                expected = {
                    provisional: {
                        component.key
                        for component in self.inventory.sources[key].components
                        if component.provisional == provisional
                    }
                    for provisional in (False, True)
                }
                for row in active:
                    names = {
                        str(cast(list[object], pair)[0])
                        for pair in cast(list[object], row.get('component_hashes') or [])
                    }
                    if names != expected[bool(row.get('provisional'))]:
                        missing += 1
            forward = 0
            unverified = 0
            unknown = 0
            for bucket in self.buckets:
                for consumer in self.inventory.sources[key].consumers:
                    manifest, bad, _ = self._consumer_state(bucket, f'{key}:{consumer.key}')
                    if manifest is None:
                        unknown += 1
                        continue
                    unverified += bad
                    if manifest.get('exists') and consumer.key == 'mount':
                        probe = _observed(
                            _object(self.bundle.samples[bucket].get('sources'), 'sources').get(key)
                        )
                        if probe is None:
                            unknown += 1
                        elif _when(manifest.get('active_through'), 'active_through') > _when(
                            probe.get('newest_end'), 'newest_end'
                        ):
                            forward += 1
            snapshot_refs = [
                f'activations/{bucket:%Y%m%dT%H%M%SZ}.jsonl.gz'
                for bucket, _ in self.bundle.activations
            ]
            reason = f'{snapshots} activation snapshots'
            self._record(
                'SS-10',
                key,
                'invalid_selected_generations_max',
                float(invalid) if snapshots else None,
                snapshot_refs,
                reason,
            )
            self._record(
                'SS-10',
                key,
                'conflicting_native_identities_max',
                float(conflicting) if snapshots else None,
                snapshot_refs,
                reason,
            )
            self._record(
                'SS-10',
                key,
                'missing_required_components_max',
                float(missing) if snapshots else None,
                snapshot_refs,
                reason,
            )
            self._record(
                'SS-10',
                key,
                'falsely_current_manifests_max',
                None if unknown else float(forward),
                refs,
                f'{unknown} unknown consumer/source buckets',
            )
            self._record(
                'SS-10',
                key,
                'unverified_expected_products_max',
                None if unknown else float(unverified),
                refs,
                f'{unknown} unknown consumer buckets',
            )
            self._record(
                'SS-10',
                key,
                'inventory_discrepancies_max',
                None if discrepancies is None else float(len(discrepancies)),
                ['identity'],
                '; '.join(str(item) for item in (discrepancies or [])[:5])
                or 'inventory matches the registry',
            )

    # SS-11 -----------------------------------------------------------------------------
    def evaluate_ss11(self) -> None:
        refs = [self._ref(bucket) for bucket in self.buckets]
        missed: list[tuple[datetime, float | None]] = []
        elapsed: list[tuple[datetime, float | None]] = []
        checker: list[tuple[datetime, float | None]] = []
        stamps: dict[str, set[float]] = {name: set() for name in self.inventory.monitor_checks}
        for bucket in self.buckets:
            capture = _object(self.bundle.samples[bucket].get('capture'), 'capture')
            checker.append((bucket, _number(capture.get('elapsed_seconds'))))
            dagster = self._dagster(bucket)
            if dagster is None:
                missed.append((bucket, None))
                elapsed.append((bucket, None))
                continue
            checks = _object(dagster.get('monitor_checks'), 'monitor_checks')
            deadline = (
                bucket - self.period - timedelta(seconds=self.policy.delivery_lag_seconds)
            ).timestamp()
            fresh = True
            worst: float | None = 0.0
            for name in self.inventory.monitor_checks:
                raw_entry: object = checks.get(name)
                entry = _optional_object(raw_entry, name) or {}
                stamp = _number(entry.get('timestamp'))
                if stamp is None or stamp < deadline:
                    fresh = False
                    continue
                stamps[name].add(stamp)
                metadata = _object(entry.get('metadata') or {}, 'metadata')
                seconds = _number(metadata.get('elapsed_seconds'))
                worst = None if worst is None or seconds is None else max(worst, seconds)
            missed.append((bucket, 0.0 if fresh else 1.0))
            elapsed.append((bucket, worst))
        self._count('SS-11', 'monitor', 'missed_evaluation_buckets_max', missed, refs, reduce='sum')
        self._count('SS-11', 'monitor', 'evaluation_seconds_max', elapsed, refs)
        self._count('SS-11', 'monitor', 'checker_elapsed_seconds_max', checker, refs)
        spacings: list[float] = []
        for values in stamps.values():
            ordered = sorted(values)
            spacings.extend(later - earlier for earlier, later in pairwise(ordered))
        self._record(
            'SS-11',
            'monitor',
            'evaluation_period_seconds',
            round(statistics.median(spacings)) if spacings else None,
            refs,
            f'{len(spacings)} evaluation spacings',
        )
        detection = self.bundle.trials.get('monitor_detection')
        if detection is None or detection.get('kind') != 'steady_state_monitor_detection':
            self._record(
                'SS-11',
                'monitor',
                'detection_seconds_max',
                None,
                ['trials/monitor_detection.json'],
                'trials/monitor_detection.json is absent',
            )
        else:
            self._bind_trial('SS-11', 'monitor', detection, 'trials/monitor_detection.json')
            cases = [
                _object(item, 'case') for item in cast(list[object], detection.get('cases') or [])
            ]
            latencies = [
                (
                    _when(case.get('alert_delivered_at'), 'alert_delivered_at')
                    - _when(case.get('breach_at'), 'breach_at')
                ).total_seconds()
                for case in cases
            ]
            self._record(
                'SS-11',
                'monitor',
                'detection_seconds_max',
                max(latencies) if latencies else None,
                ['trials/monitor_detection.json'],
                f'{len(cases)} controlled breaches',
            )

    # SS-12 -----------------------------------------------------------------------------
    def evaluate_ss12(self) -> None:
        refs = [self._ref(bucket) for bucket in self.buckets]
        hours = (
            (self.window_end - self.window_start + self.period).total_seconds() / 3600
            if self.buckets
            else 0.0
        )
        self._record(
            'SS-12',
            'window',
            'window_hours_min',
            hours,
            refs,
            f'{self.window_start.isoformat()} .. {self.window_end.isoformat()}',
        )
        counts = self._entity_bucket_counts()
        least = min(counts.values()) if counts else 0
        self._record(
            'SS-12',
            'window',
            'buckets_per_entity_min',
            float(least),
            refs,
            ', '.join(
                f'{name}={count}'
                for name, count in sorted(counts.items(), key=lambda item: item[1])[:6]
            ),
        )
        self._record(
            'SS-12',
            'window',
            'missing_or_unknown_buckets_max',
            float(self._expected() - least) if self.buckets else None,
            refs,
            f'expected {self._expected()}, rejected {len(self.bundle.rejected)}, duplicates {self.bundle.duplicates}',
        )
        shas = {str(self.bundle.samples[bucket].get('identity_sha256')) for bucket in self.buckets}
        self._record(
            'SS-12',
            'window',
            'identity_changes_max',
            float(len(shas) - 1) if shas else None,
            refs,
            f'{len(shas)} runtime identities across the window',
        )
        last = self.buckets[-1] if self.buckets else None
        blocking = (
            _observed(self.bundle.samples[last].get('blocking')) if last is not None else None
        )
        if blocking is None:
            self._record(
                'SS-12',
                'window',
                'unresolved_blocking_work_max',
                None,
                refs[-1:],
                'blocking-work probe unknown at closeout',
            )
        else:
            failures = sum(
                _number(_object(item, 'failure').get('count')) or 0.0
                for item in cast(list[object], blocking.get('open_failures') or [])
            )
            outstanding = sum(
                _number(value) or 0.0
                for value in _object(blocking.get('outstanding_attempts'), 'outstanding').values()
            )
            self._record(
                'SS-12',
                'window',
                'unresolved_blocking_work_max',
                failures + outstanding,
                refs[-1:],
                f'{failures:.0f} open blocking failures, {outstanding:.0f} outstanding attempts at closeout',
            )
        self.denominators['SS-12:window'] = {
            'expected_buckets': self._expected(),
            'entity_buckets': counts,
            'rejected': self.bundle.rejected[:20],
        }

    def _entity_bucket_counts(self) -> dict[str, int]:
        counts: dict[str, int] = {}
        for bucket in self.buckets:
            sample = self.bundle.samples[bucket]
            for kind, section in (
                ('source', 'sources'),
                ('consumer', 'consumers'),
                ('depth', 'depth'),
            ):
                probes = _object(sample.get(section), section)
                for name in self.entities[kind]:
                    counts[name] = counts.get(name, 0) + (1 if _observed(probes.get(name)) else 0)
            for name in ('dagster', 'monitor', 'resources', 'receipts', 'blocking'):
                counts[name] = counts.get(name, 0) + (1 if _observed(sample.get(name)) else 0)
        return counts

    # Trials ---------------------------------------------------------------------------
    def _identity(self) -> dict[str, object] | None:
        shas = {str(self.bundle.samples[bucket].get('identity_sha256')) for bucket in self.buckets}
        if len(shas) == 1:
            return self.bundle.identities[next(iter(shas))]
        if not shas and len(self.bundle.identities) == 1:
            return next(iter(self.bundle.identities.values()))
        return None

    def _bind_trial(
        self, metric_id: str, entity: str, document: dict[str, object], ref: str
    ) -> bool:
        """A trial artifact counts only on the same code/policy/inventory identities."""
        identity = self._identity()
        expected_code = str(identity.get('code_sha')) if identity else str(document.get('code_sha'))
        problems: list[str] = []
        if document.get('environment') != 'isolated' and metric_id != 'SS-08':
            problems.append('environment is not isolated')
        if str(document.get('code_sha') or '') != expected_code or not expected_code:
            problems.append('code_sha differs from the observed runtime')
        if document.get('policy_sha256') != self.policy.sha256:
            problems.append('policy_sha256 differs from the loaded policy')
        if document.get('inventory_sha256') != self.inventory.sha256:
            problems.append('inventory_sha256 differs from the loaded inventory')
        if problems:
            for statistic in self.policy.metrics[metric_id].statistics:
                self._record(metric_id, entity, statistic, None, [ref], '; '.join(problems))
            return False
        return True

    def evaluate_ss05(self) -> None:
        ref = 'trials/capacity_trial.json'
        trial = self.bundle.trials.get('capacity_trial')
        if trial is None or trial.get('kind') != 'steady_state_capacity_trial':
            for statistic in self.policy.metrics['SS-05'].statistics:
                self._record('SS-05', 'trial', statistic, None, [ref], f'{ref} is absent')
            return
        if not self._bind_trial('SS-05', 'trial', trial, ref):
            return
        hours = (
            _when(trial.get('trial_end'), 'trial_end')
            - _when(trial.get('trial_start'), 'trial_start')
        ).total_seconds() / 3600
        self._record('SS-05', 'trial', 'trial_hours_min', hours, [ref], 'trial_start .. trial_end')
        self._record(
            'SS-05',
            'trial',
            'normal_freshness_hours_after_recovery_min',
            _number(trial.get('normal_freshness_hours_after_recovery')),
            [ref],
            'declared by the trial runner',
        )
        sources = _object(trial.get('sources'), 'sources')
        for key in self.entities['source']:
            entry = _object(sources.get(key) or {}, key)
            arrival = _number(entry.get('arrival_minutes'))
            useful = _number(entry.get('useful_minutes'))
            ratio = None if arrival is None or useful is None or arrival <= 0 else useful / arrival
            self._record(
                'SS-05',
                key,
                'useful_service_to_arrival_ratio_min',
                ratio,
                [ref],
                f'useful {useful} / arrival {arrival} distinct minutes; canonical replacement {entry.get("canonical_replacement_minutes")} credited separately',
            )
            for statistic, name in (
                ('withheld_minutes', 'withheld_minutes'),
                ('drain_minutes_max', 'drain_minutes'),
                ('lost_rows_max', 'lost_rows'),
                ('duplicate_selected_rows_max', 'duplicate_selected_rows'),
                ('hidden_backlog_minutes_max', 'hidden_backlog_minutes'),
            ):
                self._record(
                    'SS-05',
                    key,
                    statistic,
                    _number(entry.get(name)),
                    [ref],
                    f'{name} from the trial record',
                )

    def evaluate_ss06(self) -> None:
        ref = 'trials/fault_cases.json'
        trial = self.bundle.trials.get('fault_cases')
        if trial is None or trial.get('kind') != 'steady_state_fault_cases':
            for statistic in self.policy.metrics['SS-06'].statistics:
                self._record('SS-06', 'fault_cases', statistic, None, [ref], f'{ref} is absent')
            return
        if not self._bind_trial('SS-06', 'fault_cases', trial, ref):
            return
        cases = [_object(item, 'case') for item in cast(list[object], trial.get('cases') or [])]
        by_kind: dict[str, list[dict[str, object]]] = {}
        for case in cases:
            by_kind.setdefault(str(case.get('kind')), []).append(case)
        missing = [kind for kind in REQUIRED_FAULT_KINDS if kind not in by_kind]

        def worst(kinds: Sequence[str], name: str) -> float | None:
            values = [_number(case.get(name)) for kind in kinds for case in by_kind.get(kind, [])]
            if (
                any(kind not in by_kind for kind in kinds)
                or not values
                or any(value is None for value in values)
            ):
                return None
            return max(cast(float, value) for value in values)

        reason = (
            f'{len(cases)} cases; missing kinds {missing}' if missing else f'{len(cases)} cases'
        )
        for case in [case for kind in INTERRUPTION_CASES for case in by_kind.get(kind, [])]:
            self._record(
                'SS-06',
                f'fault_cases:{case.get("name")}',
                'interruption_minutes',
                _number(case.get('interruption_minutes')),
                [ref],
                'controlled outage length',
            )
        if missing:
            for kind in missing:
                if kind in INTERRUPTION_CASES:
                    self._record(
                        'SS-06',
                        f'fault_cases:{kind}',
                        'interruption_minutes',
                        None,
                        [ref],
                        f'no {kind} interruption case',
                    )
        self._record(
            'SS-06',
            'fault_cases',
            'resume_minutes_max',
            worst(INTERRUPTION_CASES, 'resume_minutes'),
            [ref],
            reason,
        )
        self._record(
            'SS-06',
            'fault_cases',
            'debt_clear_minutes_max',
            worst(INTERRUPTION_CASES, 'debt_clear_minutes'),
            [ref],
            reason,
        )
        self._record(
            'SS-06',
            'fault_cases',
            'unaffected_source_freshness_breaches_max',
            worst(INTERRUPTION_CASES, 'unaffected_freshness_breaches'),
            [ref],
            reason,
        )
        self._record(
            'SS-06',
            'fault_cases',
            'dead_owner_reconcile_minutes_max',
            worst(('dead_owner',), 'reconcile_minutes'),
            [ref],
            reason,
        )
        self._record(
            'SS-06',
            'fault_cases',
            'false_dead_attempts_max',
            worst(('dead_owner',), 'false_dead_attempts'),
            [ref],
            reason,
        )
        self._record(
            'SS-06',
            'fault_cases',
            'unseeded_mount_commit_minutes_max',
            worst(('unseeded_render',), 'commit_minutes'),
            [ref],
            reason,
        )
        self._record(
            'SS-06',
            'fault_cases',
            'interrupted_render_commit_minutes_max',
            worst(('interrupted_render',), 'commit_minutes'),
            [ref],
            reason,
        )
        self._record(
            'SS-06',
            'fault_cases',
            'restarted_from_zero_renders_max',
            worst(('interrupted_render',), 'restarted_from_zero'),
            [ref],
            reason,
        )

    def evaluate_ss08(self) -> None:
        ref = 'trials/deploy_verification.json'
        trial = self.bundle.trials.get('deploy_verification')
        if trial is None or trial.get('kind') != 'steady_state_deploy_verification':
            for statistic in self.policy.metrics['SS-08'].statistics:
                self._record('SS-08', 'deployment', statistic, None, [ref], f'{ref} is absent')
            return
        if not self._bind_trial('SS-08', 'deployment', trial, ref):
            return
        minutes = (
            _when(trial.get('verified_at'), 'verified_at')
            - _when(trial.get('compose_ready_at'), 'compose_ready_at')
        ).total_seconds() / 60
        reason = f'deploy {trial.get("deploy_id")}'
        self._record('SS-08', 'deployment', 'verification_minutes_max', minutes, [ref], reason)
        for statistic in (
            'required_daemons_ready',
            'credentials_and_mounts_usable',
            'fresh_source_minute_advanced',
            'mount_publication_advanced',
        ):
            value = trial.get(statistic)
            self._record(
                'SS-08',
                'deployment',
                statistic,
                None if not isinstance(value, bool) else float(value),
                [ref],
                reason,
            )
        run: object = trial.get('launched_maintenance_run')
        launched = _optional_object(run, 'launched_maintenance_run')
        self._record(
            'SS-08',
            'deployment',
            'launched_maintenance_run_success',
            None
            if launched is None or not launched.get('run_id')
            else float(launched.get('status') == 'SUCCESS'),
            [ref],
            f'run {launched.get("run_id") if launched else "absent"}',
        )
        skipped = trial.get('skipped_stages')
        self._record(
            'SS-08',
            'deployment',
            'skipped_stages_max',
            float(len(cast(list[object], skipped))) if isinstance(skipped, list) else None,
            [ref],
            reason,
        )

    # Overall --------------------------------------------------------------------------
    def evaluate(self) -> tuple[AcceptanceReport, dict[str, object]]:
        identity = self._identity()
        if self.profile == 'production':
            if not self.buckets:
                raise EvidenceError('The production profile requires minute samples.')
            if identity is None:
                raise EvidenceError(
                    'The window spans more than one runtime identity; no single identity can be certified.'
                )
            if identity.get('environment') != 'production':
                raise EvidenceError(
                    'The production profile cannot be satisfied by isolated or replayed evidence.'
                )
            if not re.fullmatch(r'[0-9a-f]{40}', str(identity.get('code_sha') or '')):
                raise EvidenceError('The observed runtime did not record a code identity.')
            if (
                identity.get('policy_sha256') != self.policy.sha256
                or identity.get('inventory_sha256') != self.inventory.sha256
            ):
                raise EvidenceError(
                    'The evidence was captured under a different policy/inventory than the verifier holds.'
                )
            self.evaluate_ss01()
            self.evaluate_ss02()
            self.evaluate_ss03()
            self.evaluate_ss04()
            self.evaluate_ss05()
            self.evaluate_ss06()
            self.evaluate_ss07()
            self.evaluate_ss08()
            self.evaluate_ss09()
            self.evaluate_ss10()
            self.evaluate_ss11()
            self.evaluate_ss12()
        else:
            if identity is not None and identity.get('environment') != 'isolated':
                raise EvidenceError('The isolated profile evaluates isolated evidence only.')
            self.evaluate_ss05()
            self.evaluate_ss06()
            self.evaluate_ss08()
            if self.buckets:
                self.evaluate_ss01()
                self.evaluate_ss02()
                self.evaluate_ss03()
                self.evaluate_ss11()
        verdict: Verdict = 'PASS'
        if any(result.verdict == 'FAIL' for result in self.results):
            verdict = 'FAIL'
        elif any(result.verdict == 'UNKNOWN' for result in self.results) or not self.results:
            verdict = 'UNKNOWN'
        trials = [
            document
            for document in self.bundle.trials.values()
            if isinstance(document.get('trial_start'), str)
        ]
        if not self.buckets and trials:
            self.window_start = min(
                _when(document['trial_start'], 'trial_start') for document in trials
            )
            self.window_end = max(
                _when(document.get('trial_end'), 'trial_end') for document in trials
            )
        report = AcceptanceReport(
            REPORT_SCHEMA_VERSION,
            self.profile,
            str(identity.get('code_sha')) if identity else '',
            str(identity.get('runtime_identity_sha256')) if identity else '',
            self.policy.sha256,
            self.inventory.sha256,
            self.bundle.manifest_sha256,
            self.window_start,
            self.window_end,
            len(self.buckets),
            self._expected() if self.buckets else 0,
            tuple(self.results),
            verdict,
        )
        return report, dict(self.denominators)


def verify_bundle(
    root: Path, *, profile: Profile, policy: Policy, inventory: Inventory
) -> tuple[AcceptanceReport, dict[str, object]]:
    bundle = load_bundle(root, timedelta(seconds=policy.sample_period_seconds))
    return Evaluator(bundle, policy, inventory, profile).evaluate()


def report_document(
    report: AcceptanceReport, denominators: Mapping[str, object]
) -> dict[str, object]:
    return {
        'schema_version': report.schema_version,
        'environment': report.environment,
        'code_sha': report.code_sha,
        'runtime_identity_sha256': report.runtime_identity_sha256,
        'policy_sha256': report.policy_sha256,
        'inventory_sha256': report.inventory_sha256,
        'evidence_manifest_sha256': report.evidence_manifest_sha256,
        'window_start': report.window_start.isoformat(),
        'window_end': report.window_end.isoformat(),
        'observed_buckets': report.observed_buckets,
        'expected_buckets': report.expected_buckets,
        'verdict': report.verdict,
        'results': [
            {
                'metric_id': result.metric_id,
                'entity': result.entity,
                'statistic': result.statistic,
                'observed': result.observed,
                'threshold': result.threshold,
                'unit': result.unit,
                'verdict': result.verdict,
                'evidence_refs': list(result.evidence_refs),
                'reason': result.reason,
            }
            for result in report.results
        ],
        'summary': {
            metric_id: {
                'PASS': sum(
                    1
                    for result in report.results
                    if result.metric_id == metric_id and result.verdict == 'PASS'
                ),
                'FAIL': sum(
                    1
                    for result in report.results
                    if result.metric_id == metric_id and result.verdict == 'FAIL'
                ),
                'UNKNOWN': sum(
                    1
                    for result in report.results
                    if result.metric_id == metric_id and result.verdict == 'UNKNOWN'
                ),
            }
            for metric_id in sorted({result.metric_id for result in report.results})
        },
        'denominators': dict(denominators),
    }


def exit_code(verdict: Verdict) -> int:
    return {'PASS': 0, 'FAIL': 1, 'UNKNOWN': 2}[verdict]
