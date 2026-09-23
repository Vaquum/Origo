"""Owner-bound descriptions and serialized evidence; importing schemas performs no I/O."""

from __future__ import annotations

import ast
import hashlib
import importlib
import inspect
import json
import logging
import os
import re
import time
from collections.abc import Callable, Mapping
from datetime import UTC, datetime, timedelta
from pathlib import Path
from types import ModuleType
from typing import Literal, NotRequired, Protocol, TypedDict, cast

Scalar = str | int | float | bool | None
Threshold = str | int | float | bool
ProjectionStatus = Literal['CURRENT', 'STALE', 'WAITING', 'FAILED', 'UNKNOWN', 'INACTIVE']
GateOutcome = Literal['PASS', 'FAIL', 'EXPECTED_WAIT', 'UNKNOWN', 'NOT_EVALUATED']
GateRole = Literal['blocking', 'defer', 'observe']
GateCadence = Literal['periodic', 'event-driven']


class CodeLocation(TypedDict):
    deployed_sha: str
    path: str
    line: int
    url: str


class ProjectionDescriptor(TypedDict):
    id: str
    source_key: str
    name: str
    lane: Literal['canonical', 'provisional', 'depth', 'consumer']
    current_target: str | None
    code: CodeLocation | None


class SourceDescriptor(TypedDict):
    id: str
    name: str
    rollout_stage: str
    projections: list[ProjectionDescriptor]


class GateDescriptor(TypedDict):
    id: str
    definition_version: str
    scope: list[str]
    name: str
    purpose: str
    governed_action: str
    role: GateRole
    condition: str
    thresholds: dict[str, Threshold]
    cadence: GateCadence
    evidence_source: str
    code: CodeLocation | None


class LawCatalog(TypedDict):
    schema_version: int
    version: str
    deployed_sha: str
    sources: list[SourceDescriptor]
    gates: list[GateDescriptor]


class ProjectionObservation(TypedDict):
    id: str
    status: ProjectionStatus
    observed_at: str
    evidence_at: str | None
    evidence_id: str | None
    data_through: str | None
    reason: str
    gate_ids: list[str]
    dagit_url: str | None


class GateEvaluation(TypedDict):
    gate_id: str
    deployed_sha: NotRequired[str]
    catalog_version: NotRequired[str]
    definition_version: str
    evidence_id: str
    evaluated_at: str
    outcome: GateOutcome
    evidence: dict[str, Scalar]
    effect: str
    affected_ids: list[str]
    reason: str
    dagit_url: str | None


GATE_FAMILIES = (
    'monitor',
    'law',
    'source.rollout',
    'source.lock_domain',
    'source.component_integrity',
    'source.retained_integrity',
    'source.activation_generation',
    'source.certification',
    'source.health',
    'source.capacity',
    'source.reader_coverage',
    'provider.archive_availability',
    'provider.request_budget',
    'provider.rate_circuit',
    'provider.response_completeness',
    'worker.minute_admission',
    'publication.ownership',
    'publication.canonical_readiness',
    'publication.current',
    'publication.full_history_cap',
    'sensor.retry',
    'sensor.reconciliation',
    'worker.watchdog',
    'worker.dead_attempt',
    'depth.admission',
    'depth.completion',
    'locks.contention',
    'orchestration.admission',
    'orchestration.runtime',
)
PACKAGE_ROOT = Path(__file__).resolve().parent
log = logging.getLogger('origo.law_catalog')


def _resolve(reference: str) -> object:
    module, _, qualified = reference.partition(':')
    value: object = importlib.import_module('origo.' + module)
    for part in qualified.split('.') if qualified else ():
        value = getattr(value, part)
    return value


def _threshold(reference: str) -> Threshold:
    value = _resolve(reference)
    if not isinstance(value, (str, int, float, bool)):
        raise TypeError(f'Gate threshold is not scalar: {reference}')
    return value


def _hash(value: object) -> str:
    return hashlib.sha256(
        json.dumps(value, sort_keys=True, separators=(',', ':')).encode()
    ).hexdigest()


def _implementation(module_name: str) -> str:
    module = _resolve(module_name)
    if not isinstance(module, ModuleType):
        raise TypeError('Gate implementation owner must be a module.')
    tree = ast.parse(inspect.getsource(module))
    # Include local helpers and SQL, excluding documentation and source locations.
    for node in ast.walk(tree):
        if isinstance(node, (ast.Module, ast.ClassDef, ast.FunctionDef, ast.AsyncFunctionDef)):
            if (
                node.body
                and isinstance(node.body[0], ast.Expr)
                and isinstance(node.body[0].value, ast.Constant)
                and isinstance(node.body[0].value.value, str)
            ):
                del node.body[0]
    return _hash(ast.dump(tree, include_attributes=False))


def code_location(owner: object, deployed_sha: str) -> CodeLocation | None:
    if not re.fullmatch(r'[0-9a-f]{40}', deployed_sha):
        return None
    if inspect.isfunction(owner) or inspect.ismethod(owner):
        owner = inspect.unwrap(owner)
    if not (
        inspect.isfunction(owner)
        or inspect.ismethod(owner)
        or inspect.isclass(owner)
        or isinstance(owner, ModuleType)
    ):
        return None
    filename = inspect.getsourcefile(owner)
    if filename is None:
        return None
    path = Path(filename).resolve()
    if not path.is_file() or not path.is_relative_to(PACKAGE_ROOT):
        return None
    line = inspect.getsourcelines(owner)[1] or 1
    relative = (Path('origo') / path.relative_to(PACKAGE_ROOT)).as_posix()
    return {
        'deployed_sha': deployed_sha,
        'path': relative,
        'line': line,
        'url': f'https://github.com/Vaquum/Origo/blob/{deployed_sha}/{relative}#L{line}',
    }


def build_catalog(deployed_sha: str) -> LawCatalog:
    from origo.sources.registry import SOURCE_REGISTRY
    from origo.workers.depth import DEPTH_SPECS

    gates: list[GateDescriptor] = []
    sources: list[SourceDescriptor] = []
    implementations: dict[str, str] = {}

    def add(
        identity: str,
        scope: list[str],
        owner: str,
        action: str,
        condition: str,
        thresholds: Mapping[str, Threshold] | None = None,
        *,
        role: GateRole = 'blocking',
        cadence: GateCadence = 'event-driven',
        evidence: str = 'source_failure_log',
    ) -> None:
        descriptor: GateDescriptor = {
            'id': identity,
            'definition_version': '',
            'scope': scope,
            'name': identity.split(':')[0].replace('.', ' ').replace('_', ' '),
            'purpose': action,
            'governed_action': action,
            'role': role,
            'condition': condition,
            'thresholds': dict(thresholds or {}),
            'cadence': cadence,
            'evidence_source': evidence,
            'code': code_location(_resolve(owner), deployed_sha),
        }
        module = owner.partition(':')[0]
        if module not in implementations:
            implementations[module] = _implementation(module)
        descriptor['definition_version'] = _hash(
            {
                **{
                    key: value
                    for key, value in descriptor.items()
                    if key not in ('definition_version', 'code')
                },
                'owner': owner,
                'implementation': implementations[module],
            }
        )
        gates.append(descriptor)

    for spec in SOURCE_REGISTRY:
        key = spec.key
        nodes: list[ProjectionDescriptor] = [
            {
                'id': f'{key}:{component.key}',
                'source_key': key,
                'name': component.key,
                'lane': 'provisional' if component.provisional else 'canonical',
                'current_target': f'{key}:{component.current_target}'
                if component.current_target
                else None,
                'code': code_location(component.build, deployed_sha),
            }
            for component in spec.components
        ]
        nodes.extend(
            {
                'id': f'{key}:consumer:{consumer.key}',
                'source_key': key,
                'name': consumer.key,
                'lane': 'consumer',
                'current_target': None,
                'code': code_location(consumer.publish, deployed_sha),
            }
            for consumer in spec.consumers
        )
        sources.append(
            {
                'id': key,
                'name': key,
                'rollout_stage': spec.rollout_stage.value,
                'projections': nodes,
            }
        )
        add(
            f'source.rollout.enabled:{key}',
            [key],
            'sources.contracts:RevisionedSourceSpec.require_enabled',
            'Source operation',
            'DORMANT blocks every operation except setup.',
            {'rollout_stage': spec.rollout_stage.value},
        )
        add(
            f'source.lock_domain:{key}',
            [key],
            'sources.lifecycle:SourceRuntime.require_shared_mount',
            'Source operation',
            'The shared lock mount must match the stored source lock-domain identity.',
            evidence='source_lock_domain; source_failure_log',
        )
        for component in spec.components:
            scope = [f'{key}:{component.key}']
            for condition, description in (
                (
                    'row_identity',
                    'Duplicate primary keys or non-finite values reject component content.',
                ),
                ('time_bounds', 'Every component row must lie inside its partition bounds.'),
            ):
                add(
                    f'source.component_integrity.{condition}:{key}:{component.key}',
                    scope,
                    'sources.storage:SourceStore.validate_component',
                    'Component validation',
                    description,
                    evidence='source_component_log; exact-build source_activation_log; source_failure_log',
                )
            add(
                f'source.component_integrity.row_count:{key}:{component.key}',
                scope,
                'sources.lifecycle:SourceRuntime._build_components',
                'Component validation',
                'Raw row count must equal the adapter count; every required component is nonempty for a nonempty revision.',
                evidence='source_component_log; source_failure_log',
            )
            add(
                f'source.retained_integrity:{key}:{component.key}',
                scope,
                'sources.lifecycle:SourceRuntime._validate_retained',
                'Retained-build validation',
                'Exactly one component proof must match physical row count/hash and the activation component hash.',
                evidence='source_component_log; source_activation_log; source_failure_log',
            )
        add(
            f'source.activation_generation:{key}',
            [key],
            'sources.lifecycle:SourceRuntime._activate',
            'Activation',
            'The current generation must equal the build expected generation.',
            evidence='source_activation_log; source_failure_log',
        )
        for check, condition in (
            ('official_revision', 'The official archive must revalidate the activated revision.'),
            ('components', 'All retained component proofs must match the active build.'),
            (
                'review_state',
                'Record the explicit PENDING or APPROVED review state; PENDING is not approval.',
            ),
        ):
            add(
                f'source.certification.{check}:{key}',
                [key],
                'sources.lifecycle:SourceRuntime.certify',
                'Source certification',
                condition,
                role='observe' if check == 'review_state' else 'blocking',
                evidence='source_certification_log.check_results/review_state; source_failure_log',
            )
        add(
            f'source.health:{key}',
            [key],
            'sources.dagit:observe_source',
            'Source health observation',
            'Unresolved non-reconciliation source, partition or consumer failures produce an unhealthy source result.',
            role='observe',
            evidence='source_failure_log; Dagit source_health evaluation',
        )
        for condition, description, thresholds in (
            (
                'unresolved_failure',
                'An unresolved SOURCE-scoped non-capacity failure blocks historical work.',
                {},
            ),
            (
                'volume_identity',
                'The volume path must be absolute; its mounted UUID and supported local disk must match the ClickHouse server.',
                {},
            ),
            (
                'measured_capacity',
                'A range backfill requires a successful representative working-set measurement.',
                {},
            ),
            (
                'byte_reserve',
                'Free bytes must cover max(ceil(total * reserve_tenths / 10), measured * working_set_factor * concurrency).',
                {
                    'reserve_tenths': _threshold('sources.capacity:CAPACITY_TOTAL_RESERVE_TENTHS'),
                    'working_set_factor': _threshold(
                        'sources.capacity:CAPACITY_WORKING_SET_FACTOR'
                    ),
                    'concurrency': spec.orchestration.canonical_concurrency,
                },
            ),
            (
                'inode_reserve',
                'Total inode count must be positive and free inodes times denominator must cover total inodes.',
                {'denominator': _threshold('sources.capacity:CAPACITY_FREE_INODE_DENOMINATOR')},
            ),
        ):
            add(
                f'source.capacity.{condition}:{key}',
                [key],
                'sources.capacity:_volumes'
                if condition == 'volume_identity'
                else 'sources.capacity:CapacityMonitor.check',
                'Historical ingestion admission',
                description,
                thresholds,
                evidence='source_capacity_log; source_failure_log',
            )
        for condition, description in (
            ('canonical_mask', 'Canonical days replace overlapping provisional minutes.'),
            (
                'first_gap',
                'Provisional partitions after the first missing interval are excluded from current readers.',
            ),
        ):
            add(
                f'source.reader_coverage.{condition}:{key}',
                [key],
                'sources.storage:SourceStore.setup',
                'Current reader eligibility',
                description,
                evidence='source_current_partitions; source_anchor_log',
                cadence='periodic',
            )
        add(
            f'provider.archive_availability:{key}',
            [key],
            'sources.lifecycle:SourceRuntime.discover',
            'Canonical archive ingestion',
            'Latest-day archive absence waits; an older missing archive fails.',
            role='defer',
            evidence='source_observation_log; source_failure_log',
        )
        for condition, owner, description, limits in (
            (
                'archive_checksum',
                'sources.adapters.binance_archive:BinanceArchiveDaily.fetch',
                'Archive bytes must match the official SHA-256 sidecar.',
                {},
            ),
            (
                'archive_member',
                'sources.adapters.binance_archive:BinanceArchiveDaily.fetch',
                'The ZIP must contain exactly the expected dated CSV member.',
                {},
            ),
            (
                'archive_rows',
                'sources.adapters.binance_archive:parse_archive_rows',
                'The archive must be nonempty, have its declared field count, unique increasing IDs, ordered timestamps and rows inside its UTC day.',
                {'field_count': int(str(getattr(spec.canonical, 'FIELD_COUNT')))},
            ),
            (
                'aggregate_anomalies',
                'sources.adapters.binance_archive:BinanceArchiveDaily.clean_agg_rows',
                'Only identical duplicate aggregate rows and declared sentinel rows may be removed; conflicting or unmatched backward IDs fail.',
                {
                    'max_anomaly_ids': _threshold(
                        'sources.adapters.binance_archive:_MAX_ANOMALY_IDS'
                    )
                },
            ),
        ):
            if condition == 'aggregate_anomalies' and 'aggtrades' not in key:
                continue
            add(
                f'provider.response_completeness.{condition}:{key}',
                [key],
                owner,
                'Canonical archive validation',
                description,
                limits,
                evidence='source_observation_log archive evidence; source_failure_log',
            )
        add(
            f'sensor.retry.terminal_state:{key}',
            [key],
            'sources.bundle:build_source_bundle',
            'Source run retry',
            'An existing nonterminal or successful run suppresses another attempt; after run retirement the durable receipt must be failed or canceled before retry.',
            role='defer',
            evidence='Dagster runs; source_run_log',
        )
        add(
            f'sensor.retry.budget:{key}',
            [key],
            'sources.bundle:build_source_bundle',
            'Source run retry',
            'Retry is withheld after the configured retry count.',
            {'retry_count': spec.orchestration.retry_count},
            role='defer',
            evidence='Dagster run tags; source_run_log',
        )
        add(
            f'sensor.retry.backoff:{key}',
            [key],
            'sources.bundle:build_source_bundle',
            'Source run retry',
            'Retry waits until retry_delay seconds after the last terminal attempt.',
            {'retry_delay_seconds': spec.orchestration.retry_delay},
            role='defer',
            evidence='Dagster runs; source_run_log',
        )
        for condition, description, limits in (
            ('active_run', 'An active health run prevents another health run.', {}),
            (
                'native_run',
                'An active reconciliation batch, native source backfill or run for the selected partition defers canonical reconciliation.',
                {},
            ),
            (
                'backoff',
                'An unchanged failed or canceled reconciliation without a terminal failure verdict waits for the delay indexed by its capped attempt.',
                {
                    f'attempt_{index + 1}_seconds': delay
                    for index, delay in enumerate(
                        cast(
                            tuple[int, ...],
                            _resolve('sources.dagit:HEALTH_RECONCILIATION_RETRY_DELAYS'),
                        )
                    )
                },
            ),
            (
                'terminal_verdict',
                'An unchanged authority with a terminal automatic failure verdict waits for operator retry or a state change; removed parity verdicts are excluded.',
                {},
            ),
            (
                'batch',
                'Reconciliation rotates only changed or urgent partitions up to the batch limit.',
                {'partitions': _threshold('sources.dagit:HEALTH_RECONCILIATION_BATCH_SIZE')},
            ),
            (
                'cadence',
                'Health runs wait for minimum interval; at idle interval they run even without new failures.',
                {
                    'minimum_seconds': _threshold('sources.dagit:HEALTH_MIN_INTERVAL_SECONDS'),
                    'idle_seconds': _threshold('sources.dagit:HEALTH_IDLE_INTERVAL_SECONDS'),
                },
            ),
        ):
            add(
                f'sensor.reconciliation.{condition}:{key}',
                [key],
                'sources.dagit:_reconciliation_selection'
                if condition == 'batch'
                else 'sources.dagit:build_reconciliation_sensor'
                if condition in ('native_run', 'terminal_verdict', 'backoff')
                else 'sources.dagit:_health_due',
                'Source reconciliation',
                description,
                limits,
                role='defer',
                evidence='Dagster runs/ticks; source_run_log',
            )
        add(
            f'orchestration.admission.source_pool:{key}',
            [key],
            'sources.prepare:configure_source_pool',
            'Canonical run admission',
            'Concurrent source canonical operations cannot exceed their pool limit.',
            {'limit': spec.orchestration.canonical_concurrency},
            role='defer',
            evidence='Dagster source pool configuration/state',
        )
        for consumer in spec.consumers:
            node = f'{key}:consumer:{consumer.key}'
            if consumer.public:
                add(
                    f'source.rollout.public:{node}',
                    [node],
                    'sources.contracts:RevisionedSourceSpec.require_enabled',
                    'Public publication',
                    'Public consumers require LIVE rollout.',
                    {'rollout_stage': spec.rollout_stage.value},
                )
            for condition, owner, description in (
                (
                    'ownership',
                    'sources.prepare:backfill_owns_publication',
                    'An active backfill or publication run defers competing worker publication.',
                ),
                (
                    'canonical_readiness',
                    'sources.storage:SourceStore.canonical_ready',
                    'Every active canonical generation must have complete component evidence and no unresolved partition failure; this does not certify absent calendar days.',
                ),
                (
                    'backfill_completion',
                    'sources.backfill:publication_ready',
                    'Every selected native backfill day must complete before publication.',
                ),
                (
                    'current.canonical_drift',
                    'sources.profiles.consumer_base:_require_canonical',
                    'The canonical state token must still match the render snapshot when the local publication manifest is committed.',
                ),
                (
                    'current',
                    'sources.publication:publication_current',
                    'A manifest must match the requested state token and its declared output files must exist.',
                ),
            ):
                family = (
                    'publication.canonical_readiness'
                    if condition == 'backfill_completion'
                    else f'publication.{condition}'
                )
                identity = (
                    f'{family}.backfill:{node}'
                    if condition == 'backfill_completion'
                    else f'{family}:{node}'
                )
                add(
                    identity,
                    [node],
                    owner,
                    'Consumer publication',
                    description,
                    role='defer',
                    evidence='Dagster runs/backfills; source_backfill_log; publication manifest; source_failure_log',
                )
            if consumer.key == 'mount':
                add(
                    f'publication.full_history_cap:{node}',
                    [node],
                    'sources.profiles.consumer_base:mount',
                    'Worker mount render',
                    'Changed months above the cap defer to full-history publication unless allow_full is set.',
                    {
                        'month_cap': _threshold(
                            'sources.profiles.consumer_base:MOUNT_WORKER_MONTH_CAP'
                        )
                    },
                    role='defer',
                    evidence='source_failure_log RENDER_DEFERRED',
                )
        if spec.provisional is not None:
            add(
                f'provider.response_completeness.request_contract:{key}',
                [key],
                'sources.adapters.binance_provisional:BinanceProvisionalBase.fetch',
                'Minute request admission',
                'Only closed provisional BTCUSDT minutes are accepted; the declared API credential must be present when required.',
                {'credential_required': bool(getattr(spec.provisional, 'CREDENTIAL_REQUIRED'))},
                evidence='source_failure_log; worker_minute_log',
            )
            for condition, description, limits in (
                (
                    'closed',
                    'Only fully closed minutes at or after the source anchor are eligible.',
                    {},
                ),
                ('covered', 'Already covered minutes are excluded.', {}),
                (
                    'catchup',
                    'Admit latest minute and missing lookback minutes up to the catch-up cap; frontier repair can precede this window.',
                    {
                        'lookback_hours': _threshold(
                            'sources.adapters.binance_provisional:PROVISIONAL_CATCHUP_LOOKBACK_HOURS'
                        ),
                        'catchup_minutes': _threshold(
                            'sources.adapters.binance_provisional:PROVISIONAL_CATCHUP_MINUTES'
                        ),
                    },
                ),
                (
                    'retry',
                    'Failed minute attempts wait exponential backoff capped by source retry delay.',
                    {'retry_delay_seconds': spec.orchestration.retry_delay},
                ),
                (
                    'concurrent',
                    'Minute work cannot exceed the worker thread limit.',
                    {'workers': _threshold('workers.provisional:PROVISIONAL_MAX_WORKERS')},
                ),
            ):
                owner = (
                    'workers.provisional:ProvisionalFeed._may_attempt'
                    if condition == 'retry'
                    else (
                        'workers.provisional:ProvisionalFeed._build_intervals'
                        if condition == 'concurrent'
                        else 'sources.adapters.binance_provisional:BinanceProvisionalBase.candidates'
                    )
                )
                add(
                    f'worker.minute_admission.{condition}:{key}',
                    [key],
                    owner,
                    'Minute admission',
                    description,
                    limits,
                    role='defer',
                    evidence='worker_minute_log; attributable worker attempt',
                )
            add(
                f'provider.response_completeness.page_cap:{key}',
                [key],
                'sources.adapters.binance_provisional:BinanceProvisionalBase.fetch',
                'Minute activation',
                'A closed minute must reach its end boundary within the page request cap, including a discarded initial page.',
                {'pages': _threshold('sources.adapters.binance_provisional:PROVISIONAL_PAGE_CAP')},
                evidence='source_observation_log; source_failure_log; worker_minute_log',
            )
            for condition, description in (
                (
                    'boundary',
                    'A nonempty response must prove both required minute boundaries; exhausted paging is rejected.',
                ),
                (
                    'ordering',
                    'Trade identifiers and timestamps must retain their declared ordering.',
                ),
                ('row_shape', 'Every row must satisfy the adapter field and value contract.'),
                (
                    'empty_minute',
                    'An empty minute requires locator observations on two different clock minutes for that partition and a later trade proving the end boundary.',
                ),
            ):
                add(
                    f'provider.response_completeness.{condition}:{key}',
                    [key],
                    'sources.adapters.binance_provisional:BinanceProvisionalBase.fetch',
                    'Minute activation',
                    description,
                    evidence='source_observation_log; source_failure_log; worker_minute_log',
                )

    for spec in DEPTH_SPECS:
        key = spec.projection_table_name
        nodes = [
            ProjectionDescriptor(
                id=f'{key}:{suffix}',
                source_key=key,
                name=name,
                lane=cast(Literal['depth', 'consumer'], lane),
                current_target=None,
                code=code_location(_resolve(owner), deployed_sha),
            )
            for suffix, name, lane, owner in (
                ('raw', spec.snapshot_table_name, 'depth', 'workers.depth:source_has_rows'),
                (
                    'minute',
                    spec.projection_table_name,
                    'depth',
                    'workers.depth:DepthFeed.process_minute',
                ),
                (
                    'consumer:arrow',
                    spec.series,
                    'consumer',
                    'workers.depth:DepthFeed.process_minute',
                ),
            )
        ]
        sources.append(
            {'id': key, 'name': spec.label, 'rollout_stage': 'LIVE', 'projections': nodes}
        )
        for condition, owner, description in (
            (
                'closed_window',
                'workers.depth:candidate_minutes',
                'Only closed minutes inside the configured lookback and partition calendar are eligible.',
            ),
            (
                'source_rows',
                'workers.depth:source_has_rows',
                'When stored raw snapshots are absent, the collector must serve a nonempty history response for the selected minute.',
            ),
        ):
            add(
                f'depth.admission.{condition}:{key}',
                [key],
                owner,
                'Depth minute admission',
                description,
                {'lookback_minutes': _threshold('workers.depth:DEPTH_SOURCE_LOOKBACK_MINUTES')},
                role='defer',
                evidence='worker_minute_log; depth snapshot tables',
            )
        for node in nodes:
            add(
                f'depth.completion:{node["id"]}',
                [node['id']],
                'workers.depth:DepthFeed.process_minute',
                'Depth minute completion',
                'Raw snapshots, the minute projection and Arrow chunk must exist; the Arrow manifest must cover the selected minute.',
                evidence='worker_minute_log; depth projection/chunk/manifest evidence',
            )

    _operational_gates(add)
    identifiers = [gate['id'] for gate in gates]
    if len(identifiers) != len(set(identifiers)):
        raise ValueError('Gate catalog contains duplicate IDs.')
    catalog: LawCatalog = {
        'schema_version': 1,
        'version': '',
        'deployed_sha': deployed_sha,
        'sources': sources,
        'gates': gates,
    }
    catalog['version'] = _hash(catalog)
    return catalog


class _AddGate(Protocol):
    def __call__(
        self,
        identity: str,
        scope: list[str],
        owner: str,
        action: str,
        condition: str,
        thresholds: Mapping[str, Threshold] | None = None,
        *,
        role: GateRole = 'blocking',
        cadence: GateCadence = 'event-driven',
        evidence: str = 'source_failure_log',
    ) -> None: ...


def _mapping(value: object) -> dict[str, object]:
    if not isinstance(value, Mapping):
        raise TypeError('Gate owner configuration must be a mapping.')
    return {str(key): item for key, item in cast(Mapping[object, object], value).items()}


def _operational_gates(add: _AddGate) -> None:
    from origo.sources.adapters.binance_daily import REST_DEFAULT_BUDGET, REST_HOST_BUDGETS
    from origo.sources.registry import SOURCE_REGISTRY
    from origo.workers.depth import DEPTH_SPECS

    monitor = _resolve('workers.monitor')
    names = cast(tuple[str, ...], getattr(monitor, 'CHECK_NAMES'))
    monitor_owners = {
        'dagster_reachable': (
            '_dagster_findings',
            'Dagit and every required daemon must be reachable and healthy.',
        ),
        'queue_bounded': (
            '_dagster_findings',
            'Queue depth, stuck runs, failed checks and job failures must stay within monitor policy.',
        ),
        'workers_alive': (
            '_worker_findings',
            'Expected worker heartbeats must be fresh and new failed receipts absent.',
        ),
        'collectors_serving': (
            '_collector_findings',
            'Each configured collector must serve the selected closed minute.',
        ),
        'no_error_logs': (
            '_log_findings',
            'The sampled delivery-lagged log window must contain no error records.',
        ),
        'publication_current': (
            '_publication_findings',
            'Published outputs must remain inside their canonical or pinned grace interval.',
        ),
        'data_current': (
            'tick',
            'Every applicable core reader law must pass; NOT_DUE is not a successful obligation.',
        ),
    }
    for name in names:
        owner, condition = monitor_owners[name]
        limits: dict[str, Threshold] = {}
        if name == 'workers_alive':
            limits['heartbeat_seconds'] = _threshold('workers.runtime:HEARTBEAT_MAX_AGE_SECONDS')
        elif name == 'collectors_serving':
            limits['timeout_seconds'] = _threshold('workers.monitor:PROBE_TIMEOUT_SECONDS')
        elif name == 'queue_bounded':
            default = (
                inspect.signature(
                    cast(Callable[..., object], _resolve('workers.monitor:Monitor.__init__'))
                )
                .parameters['queue_threshold']
                .default
            )
            limits['queue_threshold'] = int(
                os.environ.get('ORIGO_ALERT_QUEUE_THRESHOLD', str(default))
            )
            limits['stuck_seconds'] = cast(
                timedelta, getattr(monitor, 'QUEUE_STUCK_AFTER')
            ).total_seconds()
        elif name == 'publication_current':
            limits['pinned_seconds'] = cast(
                timedelta, getattr(monitor, 'PINNED_PUBLICATION_STALE_AFTER')
            ).total_seconds()
            limits['canonical_seconds'] = cast(
                timedelta, getattr(monitor, 'CANONICAL_PUBLICATION_STALE_AFTER')
            ).total_seconds()
        elif name == 'no_error_logs':
            limits['delivery_lag_seconds'] = _threshold('workers.monitor:DELIVERY_LAG_SECONDS')
        add(
            f'monitor.{name}',
            [],
            'workers.monitor:Monitor.' + owner,
            'Monitor finding and alert',
            condition,
            limits,
            role='observe',
            cadence='periodic',
            evidence='Dagit origo_monitor asset-check evaluation',
        )
    anchors = _mapping(_resolve('law:LAW_ANCHORS'))
    for spec in SOURCE_REGISTRY:
        if spec.key not in anchors:
            continue
        market = 'SPOT' if '_spot_' in spec.key else 'PERP'
        deadline = cast(tuple[int, int], _resolve(f'law:C1_{market}_DEADLINE'))
        for predicate, condition, thresholds in (
            (
                'R1',
                'Reader age must be within budget and the selected fully closed covered minute must contain active raw rows.',
                {'budget_seconds': _threshold(f'law:R1_{market}_BUDGET_SECONDS')},
            ),
            (
                'C1',
                'Yesterday must have complete validated canonical proofs; absence is NOT_DUE before deadline and FAIL afterward.',
                {'deadline_utc': f'{deadline[0]:02}:{deadline[1]:02}'},
            ),
            (
                'C2',
                'The frozen anchor through day-before-yesterday must contain every distinct canonical day and active component proof.',
                {'anchor': str(anchors[spec.key])},
            ),
        ):
            add(
                f'law.{predicate}:{spec.key}',
                [spec.key],
                'law:evaluate',
                'Core reader law observation',
                condition,
                thresholds,
                role='observe',
                cadence='periodic',
                evidence='law sample independent reader/proof query',
            )
    for spec in DEPTH_SPECS:
        add(
            f'law.D1:{spec.projection_table_name}',
            [spec.projection_table_name],
            'law:evaluate',
            'Core depth coverage observation',
            'Distinct timestamps in the closed minute slots ending before the delivery grace may miss at most the configured allowance.',
            {
                'expected_slots': _threshold('law:D1_EXPECTED_SLOTS'),
                'max_missing': _threshold('law:D1_MAX_MISSING_SLOTS'),
                'delivery_grace_seconds': _threshold('law:D1_DELIVERY_GRACE_SECONDS'),
            },
            role='observe',
            cadence='periodic',
            evidence='law sample independent depth query',
        )
    add(
        'law.inventory',
        [],
        'law:evaluate',
        'Core law coverage observation',
        'Frozen law members remain required; every LIVE registered source needs an explicit law mapping.',
        role='observe',
        cadence='periodic',
        evidence='registry; law sample',
    )
    for host, (rate, backstop) in {**REST_HOST_BUDGETS, 'default': REST_DEFAULT_BUDGET}.items():
        for condition, text, threshold in (
            (
                'pace',
                'Each host/egress-IP pair waits weight divided by rate between requests.',
                {'weight_per_second': rate},
            ),
            (
                'backstop',
                'Used one-minute weight at or above the backstop delays the next request.',
                {'used_weight_1m': backstop},
            ),
        ):
            add(
                f'provider.request_budget.{condition}:{host}',
                [],
                'sources.adapters.binance_daily:_weighted_request',
                'Provider request admission',
                text,
                threshold,
                role='defer',
                evidence='recorded request evidence; current shared budget deadline',
            )
        for condition, text in (
            ('retry_after', 'HTTP 429 waits the valid Retry-After deadline; invalid headers fail.'),
            (
                'ban',
                'HTTP 418 opens the circuit until Retry-After; requests while open are rejected.',
            ),
        ):
            add(
                f'provider.rate_circuit.{condition}:{host}',
                [],
                'sources.adapters.binance_daily:_weighted_request',
                'Provider request admission',
                text,
                role='defer',
                evidence='source_failure_log; original provider response evidence',
            )
    add(
        'worker.watchdog',
        [],
        'workers.runtime:start_watchdog',
        'Worker process lifetime',
        'The watchdog exits a process when its progress heartbeat exceeds maximum age.',
        {'max_age_seconds': _threshold('workers.runtime:HEARTBEAT_MAX_AGE_SECONDS')},
        evidence='heartbeat sample; worker process exit',
    )
    add(
        'worker.dead_attempt',
        [],
        'workers.receipts:reconcile_died_receipts',
        'Abandoned minute reconciliation',
        'An old host STARTED attempt without a terminal receipt is recorded WORKER_DIED.',
        {'stale_after_seconds': _threshold('workers.receipts:DIED_RECEIPT_STALE_AFTER_SECONDS')},
        evidence='worker_minute_log',
    )
    add(
        'locks.contention.source',
        [],
        'sources.locking:source_lock',
        'Source operation admission',
        'A conflicting shared/exclusive lock blocks; nonblocking acquisition records SOURCE_LOCK_BUSY.',
        role='defer',
        evidence='source_failure_log SOURCE_LOCK_BUSY',
    )
    add(
        'locks.contention.orchestration',
        [],
        'orchestration.policy:admission_lock',
        'Run admission serialization',
        'Run admission waits for the shared orchestration lock; a lock file alone is not an observation.',
        role='defer',
        evidence='attributable admission lock acquisition/wait evidence',
    )
    loader = cast(Callable[[str], object], getattr(importlib.import_module('yaml'), 'safe_load'))
    config_path = Path(os.environ.get('DAGSTER_HOME', str(Path.cwd()))) / 'dagster.yaml'
    config = _mapping(loader(config_path.read_text()))
    coordinator = _mapping(_mapping(config['run_coordinator'])['config'])
    add(
        'orchestration.admission.global',
        [],
        'orchestration.coordinator:OrigoQueuedRunCoordinator',
        'Run launch admission',
        'Total active runs cannot exceed the configured global limit.',
        {'limit': cast(int, coordinator['max_concurrent_runs'])},
        role='defer',
        evidence='dagster.yaml; Dagster queue/runs',
    )
    configured_limits = coordinator['tag_concurrency_limits']
    if not isinstance(configured_limits, list):
        raise TypeError('Configured tag concurrency limits must be a list.')
    for index, raw in enumerate(cast(list[object], configured_limits)):
        limit = _mapping(raw)
        value = limit['value']
        identity = str(value) if isinstance(value, str) else 'per_unique_value'
        add(
            f'orchestration.admission.tag:{index}',
            [],
            'orchestration.coordinator:OrigoQueuedRunCoordinator',
            'Run launch admission',
            f'Tag {limit["key"]} ({identity}) cannot exceed its configured active-run limit.',
            {'limit': cast(int, limit['limit']), 'key': str(limit['key']), 'value': identity},
            role='defer',
            evidence='dagster.yaml; Dagster run tags/queue',
        )
    pool = _mapping(_mapping(config['concurrency'])['pools'])
    add(
        'orchestration.admission.default_pool',
        [],
        'sources.prepare:configure_source_pool',
        'Operation admission',
        'Operations without an explicit pool limit use the configured default.',
        {'limit': cast(int, pool['default_limit']), 'granularity': str(pool['granularity'])},
        role='defer',
        evidence='dagster.yaml; Dagster pool state',
    )
    add(
        'orchestration.admission.duplicate',
        [],
        'orchestration.coordinator:OrigoQueuedRunCoordinator.submit_run',
        'Run submission',
        'Equivalent outstanding request identities suppress duplicate runs.',
        role='defer',
        evidence='Dagster request identity/redundant tags and runs',
    )
    monitoring = _mapping(config['run_monitoring'])
    for key in ('start_timeout_seconds', 'cancel_timeout_seconds', 'max_runtime_seconds'):
        add(
            f'orchestration.runtime.{key}',
            [],
            'orchestration.launcher:OrigoRunLauncher',
            'Run lifetime',
            f'Run monitoring enforces configured {key}; an explicit per-run runtime overrides its default.',
            {'seconds': cast(int, monitoring[key])},
            evidence='dagster.yaml; Dagster run events',
        )
    add(
        'orchestration.runtime.job_default',
        [],
        'orchestration.policy:execution_tags',
        'Per-run maximum runtime',
        'Short non-bulk jobs use the short default; other runs use the general default; an explicit nonzero tag is preserved.',
        {
            'short_seconds': _threshold('orchestration.policy:SHORT_JOB_MAX_RUNTIME_SECONDS'),
            'default_seconds': _threshold('orchestration.policy:DEFAULT_JOB_MAX_RUNTIME_SECONDS'),
        },
        evidence='Dagster execution tags',
    )
    add(
        'orchestration.admission.redundant_launch',
        [],
        'orchestration.launcher:OrigoRunLauncher.launch_run',
        'Run worker launch',
        'A run whose redundancy tag names that run is canceled before worker launch.',
        role='defer',
        evidence='Dagster run tags and cancellation events',
    )
    add(
        'orchestration.runtime.launch_health',
        [],
        'orchestration.launcher:OrigoRunLauncher.check_run_worker_health',
        'Run worker health',
        'The worker must remain reachable and report the launched run as active.',
        evidence='Dagster run worker health/events',
    )


def unknown_observations(catalog: LawCatalog, now: datetime) -> list[ProjectionObservation]:
    return [
        {
            'id': node['id'],
            'status': 'UNKNOWN',
            'observed_at': now.isoformat(),
            'evidence_at': None,
            'evidence_id': None,
            'data_through': None,
            'reason': 'not_observed',
            'gate_ids': [gate['id'] for gate in catalog['gates'] if node['id'] in gate['scope']],
            'dagit_url': None,
        }
        for source in catalog['sources']
        for node in source['projections']
    ]


def gate_evaluation(
    descriptor: GateDescriptor,
    *,
    evidence_id: str,
    evaluated_at: str,
    outcome: GateOutcome,
    evidence: dict[str, Scalar],
    reason: str,
    affected_ids: list[str] | None = None,
    dagit_url: str | None = None,
    catalog_version: str | None = None,
) -> GateEvaluation:
    event: GateEvaluation = {
        'gate_id': descriptor['id'],
        'definition_version': descriptor['definition_version'],
        'evidence_id': evidence_id,
        'evaluated_at': evaluated_at,
        'outcome': outcome,
        'evidence': evidence,
        'effect': descriptor['governed_action'],
        'affected_ids': affected_ids if affected_ids is not None else descriptor['scope'],
        'reason': reason,
        'dagit_url': dagit_url,
    }
    if descriptor['code'] is not None:
        event['deployed_sha'] = descriptor['code']['deployed_sha']
    if catalog_version is not None:
        event['catalog_version'] = catalog_version
    return event


# Map only unambiguous persisted codes. A generic RuntimeError/ValueError cannot identify a guard.
_FAILURE_GATES = {
    'PROVIDER_CREDENTIAL_MISSING': ('provider.response_completeness.request_contract',),
    'CAPACITY_VOLUME_INVALID': ('source.capacity.volume_identity',),
    'ARCHIVE_CHECKSUM_MISMATCH': ('provider.response_completeness.archive_checksum',),
    'ARCHIVE_MEMBER_INVALID': ('provider.response_completeness.archive_member',),
    'ARCHIVE_ROWS_INVALID': ('provider.response_completeness.archive_rows',),
    'RETAINED_CONTENT_INVALID': ('source.retained_integrity',),
    'SOURCE_HEALTH_BLOCKED': ('source.capacity.unresolved_failure',),
    'CAPACITY_VOLUME_MISMATCH': ('source.capacity.volume_identity',),
    'CAPACITY_DISKS_UNSUPPORTED': ('source.capacity.volume_identity',),
    'CAPACITY_MEASUREMENT_REQUIRED': ('source.capacity.measured_capacity',),
    'RENDER_DEFERRED': ('publication.full_history_cap',),
    'SOURCE_LOCK_BUSY': ('locks.contention.source',),
    'PROVIDER_RATE_CIRCUIT': ('provider.rate_circuit.ban',),
}


def source_failure_evaluations(
    catalog: LawCatalog,
    *,
    source_key: str,
    error_code: str,
    event_id: str,
    event_time: str,
    event_type: str,
    component: str | None = None,
    consumer: str | None = None,
    evidence: dict[str, Scalar] | None = None,
) -> list[GateEvaluation]:
    """Convert attributable FAILED events without treating recovery as a fresh gate evaluation."""
    if event_type != 'FAILED':
        return []
    families = _FAILURE_GATES.get(error_code, ())
    affected = (
        f'{source_key}:consumer:{consumer}'
        if consumer
        else (f'{source_key}:{component}' if component else source_key)
    )
    result: list[GateEvaluation] = []
    for gate in catalog['gates']:
        family = gate['id'].split(':')[0]
        if family not in families or (gate['scope'] and affected not in gate['scope']):
            continue
        # A provider circuit receipt without host attribution cannot select a host gate.
        if family.startswith('provider.rate_circuit.') and gate['id'].partition(':')[2] != (
            evidence or {}
        ).get('host'):
            continue
        result.append(
            gate_evaluation(
                gate,
                catalog_version=catalog['version'],
                evidence_id=event_id,
                evaluated_at=event_time,
                outcome='EXPECTED_WAIT' if gate['role'] == 'defer' else 'FAIL',
                evidence=evidence or {},
                reason=error_code,
                affected_ids=[affected],
            )
        )
    return result


def certification_evaluations(
    catalog: LawCatalog,
    *,
    source_key: str,
    event_id: str,
    recorded_at: str,
    check_results: str,
    review_state: str,
    evidence: dict[str, Scalar],
) -> list[GateEvaluation]:
    checks = _mapping(json.loads(check_results))
    result: list[GateEvaluation] = []
    for gate in catalog['gates']:
        if not gate['id'].startswith('source.certification.') or gate['scope'] != [source_key]:
            continue
        check = gate['id'].split(':')[0].rsplit('.', 1)[1]
        if check == 'components':
            expected = {
                node['name']
                for source in catalog['sources']
                if source['id'] == source_key
                for node in source['projections']
                if node['lane'] == 'canonical'
            }
            components = checks.get('components')
            proven = (
                isinstance(components, dict)
                and set(cast(dict[str, object], components)) == expected
                and bool(expected)
            )
            if proven:
                proven = all(
                    isinstance(value, str) and bool(value)
                    for value in cast(dict[str, object], components).values()
                )
        elif check == 'official_revision':
            proven = bool(evidence.get('revision')) and checks.get(check) == evidence['revision']
        else:
            proven = review_state in ('PENDING', 'APPROVED')
        outcome: GateOutcome = 'PASS' if proven else 'UNKNOWN'
        if check == 'review_state' and review_state == 'PENDING':
            outcome = 'EXPECTED_WAIT'
        result.append(
            gate_evaluation(
                gate,
                catalog_version=catalog['version'],
                evidence_id=event_id,
                evaluated_at=recorded_at,
                outcome=outcome,
                evidence={**evidence, 'review_state': review_state},
                reason='recorded_certification' if proven else 'certification_evidence_missing',
            )
        )
    return result


class GateEventClient(Protocol):
    def execute(
        self, query: str, params: object | None = None, settings: Mapping[str, object] | None = None
    ) -> list[tuple[object, ...]]: ...


def import_gate_events(
    client: GateEventClient,
    database: str,
    catalog: LawCatalog,
    now: datetime,
    *,
    known_since: datetime,
    cursor: str | None = None,
) -> tuple[list[GateEvaluation], str | None]:
    """One bounded page; callers establish when this exact definition became attributable."""
    if not re.fullmatch(r'[a-z][a-z0-9_]{0,119}', database):
        raise ValueError('Invalid evidence database identifier.')
    if known_since.tzinfo is None or now.tzinfo is None:
        raise ValueError('Historical evidence bounds must be timezone-aware.')
    since = max(known_since, now - timedelta(days=30))
    position = _mapping(json.loads(cursor)) if cursor is not None else {}
    after = datetime.fromisoformat(str(position.get('at', since.isoformat())))
    after_id = str(position.get('id', ''))
    if after.tzinfo is None or after < since:
        after, after_id = since, ''
    if len(after_id) > 128:
        raise ValueError('Invalid historical evidence cursor.')
    rows = client.execute(
        f"""
        SELECT kind, source_key, event_time, event_id, error_code, event_type,
               component, consumer, check_results, review_state, partition_key, revision, build_id
        FROM (
            SELECT 'failure' AS kind, source_key, event_time, toString(event_id) AS event_id,
                   error_code, event_type, ifNull(component,'') AS component,
                   ifNull(consumer,'') AS consumer, '' AS check_results, '' AS review_state,
                   ifNull(partition_key,'') AS partition_key, ifNull(revision,'') AS revision,
                   ifNull(toString(build_id),'') AS build_id
            FROM {database}.source_failure_log
            WHERE event_time >= toDateTime64(%(since)s, 6, 'UTC')
              AND event_time <= toDateTime64(%(until)s, 6, 'UTC')
            UNION ALL
            SELECT 'certification', source_key, recorded_at,
                   hex(SHA256(concat(source_key, partition_key, revision, toString(build_id),
                       dagster_run_id, toString(recorded_at)))), '', '', '', '', check_results,
                   review_state, partition_key, revision, toString(build_id)
            FROM {database}.source_certification_log
            WHERE recorded_at >= toDateTime64(%(since)s, 6, 'UTC')
              AND recorded_at <= toDateTime64(%(until)s, 6, 'UTC')
        ) WHERE (event_time, event_id) > (toDateTime64(%(after)s, 6, 'UTC'), %(after_id)s)
        ORDER BY event_time, event_id LIMIT 1000
        """,
        {
            'since': since.astimezone(UTC).strftime('%Y-%m-%d %H:%M:%S.%f'),
            'until': now.astimezone(UTC).strftime('%Y-%m-%d %H:%M:%S.%f'),
            'after': after.astimezone(UTC).strftime('%Y-%m-%d %H:%M:%S.%f'),
            'after_id': after_id,
        },
        settings={
            'max_memory_usage': 536870912,
            'max_execution_time': 5,
            'max_threads': 1,
            'read_overflow_mode': 'throw',
            'timeout_overflow_mode': 'throw',
        },
    )
    events: list[GateEvaluation] = []
    for row in rows:
        (
            kind,
            source,
            stamp,
            event_id,
            code,
            state,
            component,
            consumer,
            checks,
            review,
            partition,
            revision,
            build,
        ) = row
        if not isinstance(stamp, datetime):
            raise TypeError('Stored gate event time must be a datetime.')
        stamp = stamp.replace(tzinfo=UTC) if stamp.tzinfo is None else stamp.astimezone(UTC)
        at = stamp.isoformat()
        evidence: dict[str, Scalar] = {
            'partition_key': str(partition),
            'revision': str(revision),
            'build_id': str(build),
        }
        if kind == 'failure':
            events.extend(
                source_failure_evaluations(
                    catalog,
                    source_key=str(source),
                    error_code=str(code),
                    event_id=str(event_id),
                    event_time=at,
                    event_type=str(state),
                    component=str(component) or None,
                    consumer=str(consumer) or None,
                    evidence=evidence,
                )
            )
        elif kind == 'certification':
            events.extend(
                certification_evaluations(
                    catalog,
                    source_key=str(source),
                    event_id=str(event_id),
                    recorded_at=at,
                    check_results=str(checks),
                    review_state=str(review),
                    evidence=evidence,
                )
            )
        else:
            raise ValueError('Unknown gate evidence kind.')
        cursor = json.dumps({'at': at, 'id': str(event_id)}, separators=(',', ':'))
    return events, cursor


def projection_gate_evaluations(
    catalog: LawCatalog, observations: list[ProjectionObservation]
) -> list[GateEvaluation]:
    """Component logs prove their completed validation, not a new check at sampling time."""
    result: list[GateEvaluation] = []
    for observation in observations:
        event_id, stamp = observation['evidence_id'], observation['evidence_at']
        if observation['reason'] != 'validated_activation' or event_id is None or stamp is None:
            continue
        for gate in catalog['gates']:
            if gate['id'].startswith('source.component_integrity.') and gate['scope'] == [
                observation['id']
            ]:
                result.append(
                    gate_evaluation(
                        gate,
                        catalog_version=catalog['version'],
                        evidence_id=event_id,
                        evaluated_at=stamp,
                        outcome='PASS' if gate['code'] is not None else 'UNKNOWN',
                        evidence={'data_through': observation['data_through']},
                        reason='recorded_component_validation'
                        if gate['code']
                        else 'definition_unversioned',
                    )
                )
    return result


def observe_publications(
    client: GateEventClient,
    database: str,
    catalog: LawCatalog,
    now: datetime,
    publication_root: Path,
    *,
    deadline: float,
) -> list[ProjectionObservation]:
    """Compare manifests to the real reader-selected state; inspect file metadata only."""
    from uuid import UUID

    from origo.sources.contracts import Partition, StateRecord, identifier
    from origo.sources.hashing import state_token
    from origo.sources.registry import SOURCE_REGISTRY

    remaining = deadline - time.monotonic()
    if remaining <= 0:
        raise TimeoutError('Publication observation budget exhausted.')
    rows = client.execute(
        f'SELECT source_key, partition_key, provisional, partition_start, partition_end, '
        f'generation, revision, build_id, component_hashes FROM {identifier(database)}.source_current_partitions',
        settings={
            'max_memory_usage': 536870912,
            'max_execution_time': min(5, remaining),
            'max_threads': 1,
            'read_overflow_mode': 'throw',
            'timeout_overflow_mode': 'throw',
        },
    )
    selected: dict[str, list[StateRecord]] = {}
    for row in rows:
        if time.monotonic() >= deadline:
            raise TimeoutError('Publication observation budget exhausted.')
        source, key, provisional, start, end, generation, revision, build, encoded = row
        if not isinstance(start, datetime) or not isinstance(end, datetime):
            raise TypeError('Publication coverage requires timestamped active records.')
        pairs: object = json.loads(str(encoded))
        if not isinstance(pairs, list):
            raise TypeError('Active component hashes must be pairs.')
        hashes: list[tuple[str, str]] = []
        for pair in cast(list[object], pairs):
            if not isinstance(pair, list):
                raise TypeError('Active component hash must be a pair.')
            values = cast(list[object], pair)
            if len(values) != 2 or not all(isinstance(value, str) for value in values):
                raise TypeError('Active component hash must contain two strings.')
            hashes.append((str(values[0]), str(values[1])))
        selected.setdefault(str(source), []).append(
            StateRecord(
                Partition(
                    str(key),
                    start.replace(tzinfo=UTC) if start.tzinfo is None else start.astimezone(UTC),
                    end.replace(tzinfo=UTC) if end.tzinfo is None else end.astimezone(UTC),
                    bool(provisional),
                ),
                int(str(generation)),
                str(revision),
                UUID(str(build)),
                tuple(hashes),
            )
        )
    result: list[ProjectionObservation] = []
    for spec in SOURCE_REGISTRY:
        records = tuple(selected.get(spec.key, ()))
        canonical = tuple(record for record in records if not record.partition.provisional)
        canonical_token = state_token(spec.key, canonical)
        pinned_token = state_token(spec.key, records)
        for consumer in spec.consumers:
            if time.monotonic() >= deadline:
                raise TimeoutError('Publication observation budget exhausted.')
            node = f'{spec.key}:consumer:{consumer.key}'
            observation: ProjectionObservation = {
                'id': node,
                'status': 'UNKNOWN',
                'observed_at': now.isoformat(),
                'evidence_at': None,
                'evidence_id': None,
                'data_through': None,
                'reason': 'publication_manifest_missing',
                'gate_ids': [gate['id'] for gate in catalog['gates'] if node in gate['scope']],
                'dagit_url': None,
            }
            path = publication_root / spec.key / consumer.key / 'latest.json'
            if not path.is_file():
                result.append(observation)
                continue
            try:
                with path.open('rb') as stream:
                    payload = stream.read(1024 * 1024 + 1)
                if len(payload) > 1024 * 1024:
                    raise ValueError('Publication manifest exceeds metadata budget.')
                data = _mapping(json.loads(payload))
                version, files = data['version'], data['files']
                if (
                    not isinstance(version, str)
                    or not version
                    or not isinstance(files, list)
                    or not files
                    or len(cast(list[object], files)) > 10000
                ):
                    raise ValueError('Publication manifest lacks bounded version/file evidence.')
                through = datetime.fromisoformat(str(data['active_through']))
                if through.tzinfo is None:
                    raise ValueError('Publication coverage requires a timezone.')
                if data.get('source_key') != spec.key:
                    raise ValueError('Publication manifest source does not match.')
                observation.update(
                    evidence_at=datetime.fromtimestamp(path.stat().st_mtime, UTC).isoformat(),
                    evidence_id=version,
                    data_through=through.astimezone(UTC).isoformat(),
                )
                missing = False
                for item in cast(list[object], files):
                    if time.monotonic() >= deadline:
                        raise TimeoutError('Publication observation budget exhausted.')
                    entry = _mapping(item)
                    relative = entry['path']
                    if not isinstance(relative, str) or not relative:
                        raise ValueError('Publication entry requires a path.')
                    target = Path(relative)
                    if not target.is_absolute():
                        target = path.parent / 'versions' / version / target
                    if not target.is_file():
                        missing = True
                if missing:
                    observation.update(status='FAILED', reason='publication_artifact_missing')
                elif not (canonical if consumer.canonical_only else records):
                    observation.update(status='UNKNOWN', reason='publication_active_state_missing')
                elif data.get('state_token') != canonical_token or (
                    not consumer.canonical_only and data.get('pinned_token') != pinned_token
                ):
                    observation.update(status='STALE', reason='publication_state_changed')
                else:
                    observation.update(status='CURRENT', reason='publication_state_and_files')
            except (OSError, ValueError, KeyError, TypeError) as error:
                log.warning(
                    'Publication observation %s unavailable: %s', node, type(error).__name__
                )
                observation.update(status='UNKNOWN', reason='publication_evidence_invalid')
            result.append(observation)
    if time.monotonic() > deadline:
        raise TimeoutError('Publication observation budget exhausted.')
    return result
