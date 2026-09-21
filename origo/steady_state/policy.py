"""Code-owned SS-01..SS-12 policy and the fixed product inventory (PRD-0017, #438).

``policy.json`` holds every numeric acceptance bound; ``inventory.json`` holds the literal
sources, components, consumers, file series, depth paths and briefing products whose
denominators cannot shrink. Both are hashed as committed bytes so evidence binds to them.
"""

from __future__ import annotations

import hashlib
import json
import math
from bisect import bisect_left, insort
from collections.abc import Iterator, Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Literal, cast

from origo.sources.contracts import RevisionedSourceSpec
from origo.sources.profiles.formulas.spot_series import MountKlineSpec

POLICY_PATH = Path(__file__).with_name('policy.json')
INVENTORY_PATH = Path(__file__).with_name('inventory.json')
METRIC_IDS = tuple(f'SS-{index:02d}' for index in range(1, 13))
Comparison = Literal['<=', '<', '>=', '>', '==']
Profile = Literal['production', 'isolated']
_COMPARISONS: tuple[str, ...] = ('<=', '<', '>=', '>', '==')


def _object(value: object, what: str) -> dict[str, object]:
    if not isinstance(value, dict):
        raise ValueError(f'{what} must be a JSON object.')
    return cast(dict[str, object], value)


def _string(value: object, what: str) -> str:
    if not isinstance(value, str) or not value:
        raise ValueError(f'{what} must be a non-empty string.')
    return value


def _number(value: object, what: str) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)) or not math.isfinite(value):
        raise ValueError(f'{what} must be a finite number.')
    return float(value)


def _integer(value: object, what: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        raise ValueError(f'{what} must be an integer.')
    return value


def _strings(value: object, what: str) -> tuple[str, ...]:
    if not isinstance(value, list):
        raise ValueError(f'{what} must be a list.')
    return tuple(_string(item, what) for item in cast(list[object], value))


def sha256_bytes(payload: bytes) -> str:
    return hashlib.sha256(payload).hexdigest()


def canonical_json(document: object) -> bytes:
    return json.dumps(document, sort_keys=True, separators=(',', ':')).encode() + b'\n'


@dataclass(frozen=True)
class Bound:
    metric_id: str
    statistic: str
    bound: float
    comparison: Comparison
    unit: str

    def holds(self, observed: float) -> bool:
        if self.comparison == '<=':
            return observed <= self.bound
        if self.comparison == '<':
            return observed < self.bound
        if self.comparison == '>=':
            return observed >= self.bound
        if self.comparison == '>':
            return observed > self.bound
        return observed == self.bound


@dataclass(frozen=True)
class MetricPolicy:
    metric_id: str
    title: str
    entity: str
    profile: Profile
    windows: tuple[str, ...]
    statistics: dict[str, Bound]


@dataclass(frozen=True)
class Policy:
    schema_version: int
    percentile_method: str
    sample_period_seconds: int
    delivery_lag_seconds: int
    minimum_hours: int
    minimum_buckets_per_entity: int
    rolling_window_hours: int
    fixed_identities: tuple[str, ...]
    worker_lookback_hours: int
    metrics: dict[str, MetricPolicy]
    sha256: str

    def bound(self, metric_id: str, statistic: str) -> Bound:
        return self.metrics[metric_id].statistics[statistic]


def _metric(metric_id: str, raw: object) -> MetricPolicy:
    data = _object(raw, metric_id)
    profile = _profile(_string(data.get('profile'), f'{metric_id}.profile'), metric_id)
    statistics: dict[str, Bound] = {}
    for name, entry in _object(data.get('statistics'), f'{metric_id}.statistics').items():
        item = _object(entry, f'{metric_id}.{name}')
        comparison = _string(item.get('comparison'), f'{metric_id}.{name}.comparison')
        if comparison not in _COMPARISONS:
            raise ValueError(f'{metric_id}.{name}.comparison is not a known comparison.')
        statistics[name] = Bound(
            metric_id,
            name,
            _number(item.get('bound'), f'{metric_id}.{name}.bound'),
            cast(Comparison, comparison),
            _string(item.get('unit'), f'{metric_id}.{name}.unit'),
        )
    if not statistics:
        raise ValueError(f'{metric_id} declares no statistics.')
    return MetricPolicy(
        metric_id,
        _string(data.get('title'), f'{metric_id}.title'),
        _string(data.get('entity'), f'{metric_id}.entity'),
        profile,
        _strings(data.get('windows'), f'{metric_id}.windows'),
        statistics,
    )


def _profile(value: str, what: str) -> Profile:
    if value == 'production':
        return 'production'
    if value == 'isolated':
        return 'isolated'
    raise ValueError(f'{what}.profile must be production or isolated.')


def load_policy(path: Path = POLICY_PATH) -> Policy:
    payload = path.read_bytes()
    data = _object(json.loads(payload), 'policy')
    window = _object(data.get('production_window'), 'production_window')
    metrics = {
        metric_id: _metric(metric_id, raw)
        for metric_id, raw in _object(data.get('metrics'), 'metrics').items()
    }
    if tuple(metrics) != METRIC_IDS:
        raise ValueError('The policy must declare exactly SS-01..SS-12 in order.')
    period = _integer(data.get('sample_period_seconds'), 'sample_period_seconds')
    hours = _integer(window.get('minimum_hours'), 'minimum_hours')
    buckets = _integer(window.get('minimum_buckets_per_entity'), 'minimum_buckets_per_entity')
    if period != 60 or buckets != hours * 3600 // period:
        raise ValueError('The minute grid and the 72-hour bucket count must agree.')
    return Policy(
        _integer(data.get('schema_version'), 'schema_version'),
        _string(data.get('percentile_method'), 'percentile_method'),
        period,
        _integer(data.get('delivery_lag_seconds'), 'delivery_lag_seconds'),
        hours,
        buckets,
        _integer(window.get('rolling_window_hours'), 'rolling_window_hours'),
        _strings(window.get('fixed_identities'), 'fixed_identities'),
        _integer(data.get('worker_lookback_hours'), 'worker_lookback_hours'),
        metrics,
        sha256_bytes(payload),
    )


def policy_document(policy: Policy, inventory: Inventory) -> dict[str, object]:
    """The exact policy as JSON for ``check_steady_state.py policy``; no runtime state."""
    return {
        'schema_version': policy.schema_version,
        'policy_sha256': policy.sha256,
        'inventory_sha256': inventory.sha256,
        'percentile_method': policy.percentile_method,
        'sample_period_seconds': policy.sample_period_seconds,
        'production_window': {
            'minimum_hours': policy.minimum_hours,
            'minimum_buckets_per_entity': policy.minimum_buckets_per_entity,
            'rolling_window_hours': policy.rolling_window_hours,
            'fixed_identities': list(policy.fixed_identities),
        },
        'metrics': {
            metric_id: {
                'title': metric.title,
                'entity': metric.entity,
                'profile': metric.profile,
                'windows': list(metric.windows),
                'statistics': {
                    name: {'bound': bound.bound, 'comparison': bound.comparison, 'unit': bound.unit}
                    for name, bound in metric.statistics.items()
                },
            }
            for metric_id, metric in policy.metrics.items()
        },
        'required_entities': {
            kind: list(names) for kind, names in required_entities(inventory).items()
        },
    }


@dataclass(frozen=True)
class ComponentInventory:
    key: str
    provisional: bool
    current_target: str | None
    time_column: str
    primary_key: tuple[str, ...]
    columns: tuple[str, ...]


@dataclass(frozen=True)
class ConsumerInventory:
    key: str
    canonical_only: bool
    public: bool
    cadence: Literal['minute', 'daily']
    destination: Literal['local', 'remote']


@dataclass(frozen=True)
class SeriesInventory:
    name: str
    family: Literal['time', 'dollar']
    size: int
    sub_path: str
    dataset: str
    repo_id_env: str | None
    file_prefix: str


@dataclass(frozen=True)
class SourceInventory:
    key: str
    rollout_stage: str
    schema_version: int
    prefix: str
    first_day: str
    canonical_cron: str
    aliases: dict[str, str]
    components: tuple[ComponentInventory, ...]
    consumers: tuple[ConsumerInventory, ...]
    export_start_date: str
    series: tuple[SeriesInventory, ...]

    def consumer(self, key: str) -> ConsumerInventory:
        return next(item for item in self.consumers if item.key == key)


@dataclass(frozen=True)
class DepthInventory:
    key: str
    series: str
    label: str
    depth: int
    snapshot_table: str
    projection_table: str
    arrow_manifest: str
    chunk_pattern: str
    chunk_retention_minutes: int
    collector_base_url_env: str
    collector_auth_token_env: str
    sync_asset: str
    projection_asset: str


@dataclass(frozen=True)
class Inventory:
    schema_version: int
    sources: dict[str, SourceInventory]
    depth: dict[str, DepthInventory]
    workers: dict[str, str]
    monitor_checks: tuple[str, ...]
    briefing: dict[str, object]
    sha256: str


def _component(raw: object) -> ComponentInventory:
    data = _object(raw, 'component')
    target = data.get('current_target')
    if target is not None and not isinstance(target, str):
        raise ValueError('component.current_target must be a string or null.')
    return ComponentInventory(
        _string(data.get('key'), 'component.key'),
        bool(data.get('provisional')),
        target,
        _string(data.get('time_column'), 'component.time_column'),
        _strings(data.get('primary_key'), 'component.primary_key'),
        _strings(data.get('columns'), 'component.columns'),
    )


def _consumer(raw: object) -> ConsumerInventory:
    data = _object(raw, 'consumer')
    cadence = _string(data.get('cadence'), 'consumer.cadence')
    destination = _string(data.get('destination'), 'consumer.destination')
    if cadence == 'minute':
        rate: Literal['minute', 'daily'] = 'minute'
    elif cadence == 'daily':
        rate = 'daily'
    else:
        raise ValueError('consumer.cadence must be minute or daily.')
    if destination == 'local':
        target: Literal['local', 'remote'] = 'local'
    elif destination == 'remote':
        target = 'remote'
    else:
        raise ValueError('consumer.destination must be local or remote.')
    return ConsumerInventory(
        _string(data.get('key'), 'consumer.key'),
        bool(data.get('canonical_only')),
        bool(data.get('public')),
        rate,
        target,
    )


def _series(raw: object) -> SeriesInventory:
    data = _object(raw, 'series')
    raw_family = _string(data.get('family'), 'series.family')
    if raw_family == 'time':
        family: Literal['time', 'dollar'] = 'time'
    elif raw_family == 'dollar':
        family = 'dollar'
    else:
        raise ValueError('series.family must be time or dollar.')
    env = data.get('repo_id_env')
    if env is not None and not isinstance(env, str):
        raise ValueError('series.repo_id_env must be a string or null.')
    return SeriesInventory(
        _string(data.get('name'), 'series.name'),
        family,
        _integer(data.get('size'), 'series.size'),
        _string(data.get('sub_path'), 'series.sub_path'),
        _string(data.get('dataset'), 'series.dataset'),
        env,
        _string(data.get('file_prefix'), 'series.file_prefix'),
    )


def _source(key: str, raw: object) -> SourceInventory:
    data = _object(raw, key)
    aliases = {
        _string(alias, f'{key}.alias'): _string(component, f'{key}.alias target')
        for alias, component in _object(data.get('aliases'), f'{key}.aliases').items()
    }
    components = tuple(
        _component(item) for item in cast(list[object], data.get('components') or [])
    )
    consumers = tuple(_consumer(item) for item in cast(list[object], data.get('consumers') or []))
    series = tuple(_series(item) for item in cast(list[object], data.get('series') or []))
    if len(series) != 12 or len({item.name for item in series}) != 12:
        raise ValueError(f'{key} must pin exactly twelve distinct file series.')
    if not any(consumer.key == 'mount' for consumer in consumers):
        raise ValueError(f'{key} must pin its mount consumer.')
    canonical = {component.key for component in components if not component.provisional}
    if canonical != {'raw', 'time', 'dollar', 'volume', 'tick', 'imbalance', 'aligned'}:
        raise ValueError(f'{key} must pin the seven canonical components.')
    provisional = {component.key for component in components if component.provisional}
    if provisional != {'raw_latest', 'time_latest', 'dollar_latest'}:
        raise ValueError(f'{key} must pin the three provisional components.')
    return SourceInventory(
        key,
        _string(data.get('rollout_stage'), f'{key}.rollout_stage'),
        _integer(data.get('schema_version'), f'{key}.schema_version'),
        _string(data.get('prefix'), f'{key}.prefix'),
        _string(data.get('first_day'), f'{key}.first_day'),
        _string(data.get('canonical_cron'), f'{key}.canonical_cron'),
        aliases,
        components,
        consumers,
        _string(data.get('export_start_date'), f'{key}.export_start_date'),
        series,
    )


def _depth(key: str, raw: object) -> DepthInventory:
    data = _object(raw, key)
    return DepthInventory(
        key,
        _string(data.get('series'), f'{key}.series'),
        _string(data.get('label'), f'{key}.label'),
        _integer(data.get('depth'), f'{key}.depth'),
        _string(data.get('snapshot_table'), f'{key}.snapshot_table'),
        _string(data.get('projection_table'), f'{key}.projection_table'),
        _string(data.get('arrow_manifest'), f'{key}.arrow_manifest'),
        _string(data.get('chunk_pattern'), f'{key}.chunk_pattern'),
        _integer(data.get('chunk_retention_minutes'), f'{key}.chunk_retention_minutes'),
        _string(data.get('collector_base_url_env'), f'{key}.collector_base_url_env'),
        _string(data.get('collector_auth_token_env'), f'{key}.collector_auth_token_env'),
        _string(data.get('sync_asset'), f'{key}.sync_asset'),
        _string(data.get('projection_asset'), f'{key}.projection_asset'),
    )


def load_inventory(path: Path = INVENTORY_PATH) -> Inventory:
    payload = path.read_bytes()
    data = _object(json.loads(payload), 'inventory')
    sources = {
        key: _source(key, raw) for key, raw in _object(data.get('sources'), 'sources').items()
    }
    if tuple(sources) != (
        'binance_spot_trades',
        'binance_perp_trades',
        'binance_spot_aggtrades',
        'binance_perp_aggtrades',
    ):
        raise ValueError('The inventory must pin the four enabled sources in registry order.')
    depth = {key: _depth(key, raw) for key, raw in _object(data.get('depth'), 'depth').items()}
    if tuple(depth) != ('depth20', 'depth200'):
        raise ValueError('The inventory must pin both depth paths.')
    workers = {
        _string(name, 'worker'): _string(_object(raw, name).get('heartbeat'), f'{name}.heartbeat')
        for name, raw in _object(data.get('workers'), 'workers').items()
    }
    return Inventory(
        _integer(data.get('schema_version'), 'schema_version'),
        sources,
        depth,
        workers,
        _strings(data.get('monitor_checks'), 'monitor_checks'),
        _object(data.get('briefing'), 'briefing'),
        sha256_bytes(payload),
    )


def required_entities(inventory: Inventory) -> dict[str, tuple[str, ...]]:
    """Every entity a verdict must account for, keyed by the policy's entity kinds."""
    sources = tuple(inventory.sources)
    consumers = tuple(
        f'{source.key}:{consumer.key}'
        for source in inventory.sources.values()
        for consumer in source.consumers
    )
    return {
        'source': sources,
        'consumer': consumers,
        'mount': tuple(name for name in consumers if name.endswith(':mount')),
        'depth': tuple(inventory.depth),
        'dagster': ('dagster',),
        'host': ('host',),
        'monitor': ('monitor',),
        'window': ('window',),
        'deployment': ('deployment',),
        'fault_case': ('fault_cases',),
    }


def registry_discrepancies(
    inventory: Inventory, registry: Sequence[RevisionedSourceSpec]
) -> tuple[str, ...]:
    """Where the live registry departs from the pinned inventory; the inventory wins."""
    found: list[str] = []
    specs = {spec.key: spec for spec in registry}
    for key, source in inventory.sources.items():
        spec = specs.get(key)
        if spec is None:
            found.append(f'{key}: missing from the registry')
            continue
        if spec.rollout_stage.value != source.rollout_stage:
            found.append(
                f'{key}: rollout stage {spec.rollout_stage.value} != {source.rollout_stage}'
            )
        if spec.partitions.first_day.isoformat() != source.first_day:
            found.append(f'{key}: first day {spec.partitions.first_day} != {source.first_day}')
        if spec.schema_version != source.schema_version or spec.names.prefix != source.prefix:
            found.append(f'{key}: schema version or prefix changed')
        if spec.orchestration.canonical_cron != source.canonical_cron:
            found.append(f'{key}: canonical cron {spec.orchestration.canonical_cron!r} changed')
        if dict(spec.aliases) != source.aliases:
            found.append(f'{key}: aliases changed')
        declared = {
            component.key: ComponentInventory(
                component.key,
                component.provisional,
                component.current_target,
                component.time_column,
                tuple(component.primary_key),
                tuple(f'{column.name}:{column.sql_type}' for column in component.columns),
            )
            for component in spec.components
        }
        for component in source.components:
            if declared.get(component.key) != component:
                found.append(f'{key}: component {component.key} differs from the inventory')
        for extra in set(declared) - {component.key for component in source.components}:
            found.append(f'{key}: undeclared component {extra}')
        consumers = {consumer.key: consumer for consumer in spec.consumers}
        for consumer in source.consumers:
            actual = consumers.get(consumer.key)
            if (
                actual is None
                or actual.canonical_only != consumer.canonical_only
                or actual.public != consumer.public
            ):
                found.append(f'{key}: consumer {consumer.key} differs from the inventory')
        for extra in set(consumers) - {consumer.key for consumer in source.consumers}:
            found.append(f'{key}: undeclared consumer {extra}')
    for extra in set(specs) - set(inventory.sources):
        found.append(f'{extra}: registered but not pinned in the inventory')
    return tuple(found)


def series_discrepancies(
    source: SourceInventory,
    specs: Sequence[MountKlineSpec],
    datasets: Mapping[str, tuple[str, str | None, str, str]],
    export_start_date: str,
) -> tuple[str, ...]:
    """Where a consumer module's twelve series depart from the pinned inventory."""
    found: list[str] = []
    if export_start_date != source.export_start_date:
        found.append(
            f'{source.key}: export start {export_start_date} != {source.export_start_date}'
        )
    declared = {spec.name: spec for spec in specs}
    for series in source.series:
        spec = declared.get(series.name)
        dataset = datasets.get(series.name)
        if spec is None or dataset is None:
            found.append(f'{source.key}: series {series.name} is not declared')
            continue
        if (spec.family, spec.size, spec.sub_path) != (series.family, series.size, series.sub_path):
            found.append(f'{source.key}: series {series.name} shape changed')
        if (dataset[0], dataset[1], dataset[2]) != (
            series.dataset,
            series.repo_id_env,
            series.file_prefix,
        ):
            found.append(f'{source.key}: series {series.name} destination changed')
    for extra in set(declared) - {series.name for series in source.series}:
        found.append(f'{source.key}: undeclared series {extra}')
    return tuple(found)


def nearest_rank(values: Sequence[float], percentile: float) -> float:
    """Nearest-rank percentile over the complete population; no interpolation."""
    if not values:
        raise ValueError('A percentile needs at least one sample.')
    if not 0 < percentile <= 100:
        raise ValueError('Percentile must be in (0, 100].')
    ordered = sorted(values)
    rank = math.ceil(percentile / 100 * len(ordered))
    return ordered[rank - 1]


def rolling_extremes(
    samples: Sequence[tuple[datetime, float]],
    *,
    window: timedelta,
    period: timedelta,
    percentiles: Sequence[float],
) -> Iterator[tuple[datetime, dict[float, float], float]]:
    """(window end, nearest-rank percentiles, max) for every contained rolling window.

    Samples must be one per bucket on the fixed grid with no holes; the caller has
    already turned holes into UNKNOWN. A sorted list slides one bucket at a time.
    """
    if len(samples) < 2:
        raise ValueError('A rolling window needs at least two samples.')
    width = int(window / period)
    if width < 1 or len(samples) < width:
        return
    ordered: list[float] = []
    for index, (_, value) in enumerate(samples):
        insort(ordered, value)
        if index >= width:
            del ordered[bisect_left(ordered, samples[index - width][1])]
        if index >= width - 1:
            ranks = {
                percentile: ordered[math.ceil(percentile / 100 * width) - 1]
                for percentile in percentiles
            }
            yield samples[index][0], ranks, ordered[-1]


def bucket_of(instant: datetime, period_seconds: int = 60) -> datetime:
    """The fixed UTC grid bucket that contains ``instant``: U(t)."""
    if instant.tzinfo is None:
        raise ValueError('Bucket instants must be timezone-aware.')
    aware = instant.astimezone(UTC)
    seconds = (aware - datetime(1970, 1, 1, tzinfo=UTC)).total_seconds()
    return datetime.fromtimestamp(seconds - seconds % period_seconds, UTC)
