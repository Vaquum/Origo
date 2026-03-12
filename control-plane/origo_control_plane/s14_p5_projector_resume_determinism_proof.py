from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, cast

from clickhouse_driver import Client as ClickHouseClient

from origo.events.aligned_projector import (
    AlignedProjectionPolicyInput,
    CanonicalAligned1sProjector,
    CanonicalAlignedPolicyStore,
)
from origo.events.ingest_state import CanonicalStreamKey
from origo.events.projector import CanonicalProjectorRuntime
from origo.events.writer import CanonicalEventWriteInput, CanonicalEventWriter
from origo_control_plane.migrations import MigrationRunner, MigrationSettings

_PROOF_DB_SUFFIX_RESUME = '_s14_p5_resume'
_PROOF_DB_SUFFIX_SINGLE = '_s14_p5_single'
_VIEW_ID = 's14_p5_aligned_spot_trades'
_VIEW_VERSION = 1
_PROJECTOR_ID = 's14_p5_projector_v1'
_PROJECTED_AT_UTC = datetime(2024, 1, 3, 0, 10, 0, tzinfo=UTC)


@dataclass(frozen=True)
class _FixtureEvent:
    source_offset_or_equivalent: str
    source_event_time_utc: datetime
    payload_raw: bytes


def _build_clickhouse_client(settings: MigrationSettings) -> ClickHouseClient:
    return ClickHouseClient(
        host=settings.host,
        port=settings.port,
        user=settings.user,
        password=settings.password,
    )


def _fixture_events() -> tuple[_FixtureEvent, ...]:
    return (
        _FixtureEvent(
            source_offset_or_equivalent='3001',
            source_event_time_utc=datetime(2024, 1, 3, 0, 0, 0, 100000, tzinfo=UTC),
            payload_raw=(
                b'{"is_best_match":true,"is_buyer_maker":false,'
                b'"price":"43200.10000000","qty":"0.01000000",'
                b'"quote_qty":"432.00100000","trade_id":"3001"}'
            ),
        ),
        _FixtureEvent(
            source_offset_or_equivalent='3002',
            source_event_time_utc=datetime(2024, 1, 3, 0, 0, 0, 700000, tzinfo=UTC),
            payload_raw=(
                b'{"is_best_match":true,"is_buyer_maker":true,'
                b'"price":"43200.20000000","qty":"0.02000000",'
                b'"quote_qty":"864.00400000","trade_id":"3002"}'
            ),
        ),
        _FixtureEvent(
            source_offset_or_equivalent='3003',
            source_event_time_utc=datetime(2024, 1, 3, 0, 0, 1, 100000, tzinfo=UTC),
            payload_raw=(
                b'{"is_best_match":true,"is_buyer_maker":false,'
                b'"price":"43200.30000000","qty":"0.03000000",'
                b'"quote_qty":"1296.00900000","trade_id":"3003"}'
            ),
        ),
        _FixtureEvent(
            source_offset_or_equivalent='3004',
            source_event_time_utc=datetime(2024, 1, 3, 0, 0, 1, 900000, tzinfo=UTC),
            payload_raw=(
                b'{"is_best_match":true,"is_buyer_maker":true,'
                b'"price":"43200.40000000","qty":"0.04000000",'
                b'"quote_qty":"1728.01600000","trade_id":"3004"}'
            ),
        ),
        _FixtureEvent(
            source_offset_or_equivalent='3005',
            source_event_time_utc=datetime(2024, 1, 3, 0, 0, 2, 100000, tzinfo=UTC),
            payload_raw=(
                b'{"is_best_match":true,"is_buyer_maker":false,'
                b'"price":"43200.50000000","qty":"0.05000000",'
                b'"quote_qty":"2160.02500000","trade_id":"3005"}'
            ),
        ),
    )


def _seed_events(
    *,
    client: ClickHouseClient,
    database: str,
    stream_key: CanonicalStreamKey,
    events: tuple[_FixtureEvent, ...],
) -> list[str]:
    writer = CanonicalEventWriter(client=client, database=database)
    statuses: list[str] = []
    for event in events:
        result = writer.write_event(
            CanonicalEventWriteInput(
                source_id=stream_key.source_id,
                stream_id=stream_key.stream_id,
                partition_id=stream_key.partition_id,
                source_offset_or_equivalent=event.source_offset_or_equivalent,
                source_event_time_utc=event.source_event_time_utc,
                ingested_at_utc=event.source_event_time_utc,
                payload_content_type='application/json',
                payload_encoding='utf-8',
                payload_raw=event.payload_raw,
            )
        )
        statuses.append(result.status)
    return statuses


def _record_policy(
    *,
    client: ClickHouseClient,
    database: str,
    stream_key: CanonicalStreamKey,
    run_id: str,
) -> dict[str, Any]:
    policy_store = CanonicalAlignedPolicyStore(client=client, database=database)
    policy_result = policy_store.record_policy(
        AlignedProjectionPolicyInput(
            view_id=_VIEW_ID,
            view_version=_VIEW_VERSION,
            stream_key=stream_key,
            bucket_size_seconds=1,
            tier_policy='hot_1s_warm_1m',
            retention_hot_days=7,
            retention_warm_days=90,
            recorded_by_run_id=run_id,
            recorded_at_utc=_PROJECTED_AT_UTC,
        )
    )
    return {
        'policy_status': policy_result.status,
        'policy_state': policy_result.policy_state,
    }


def _fetch_aligned_fingerprint(
    *,
    client: ClickHouseClient,
    database: str,
    stream_key: CanonicalStreamKey,
) -> dict[str, Any]:
    rows = client.execute(
        f'''
        SELECT
            aligned_at_utc,
            bucket_event_count,
            first_source_offset_or_equivalent,
            last_source_offset_or_equivalent,
            bucket_sha256
        FROM {database}.canonical_aligned_1s_aggregates
        WHERE view_id = %(view_id)s
            AND view_version = %(view_version)s
            AND source_id = %(source_id)s
            AND stream_id = %(stream_id)s
            AND partition_id = %(partition_id)s
        ORDER BY aligned_at_utc ASC
        ''',
        {
            'view_id': _VIEW_ID,
            'view_version': _VIEW_VERSION,
            'source_id': stream_key.source_id,
            'stream_id': stream_key.stream_id,
            'partition_id': stream_key.partition_id,
        },
    )
    normalized_rows: list[dict[str, Any]] = []
    for row in rows:
        aligned_at_value = cast(datetime, row[0]).astimezone(UTC)
        normalized_rows.append(
            {
                'aligned_at_utc': aligned_at_value.isoformat(),
                'bucket_event_count': int(row[1]),
                'first_source_offset_or_equivalent': str(row[2]),
                'last_source_offset_or_equivalent': str(row[3]),
                'bucket_sha256': str(row[4]),
            }
        )
    return {
        'row_count': len(normalized_rows),
        'rows': normalized_rows,
        'fingerprint_hash_sha256': hashlib.sha256(
            json.dumps(
                normalized_rows,
                sort_keys=True,
                separators=(',', ':'),
            ).encode('utf-8')
        ).hexdigest(),
    }


def _latest_checkpoint_summary(
    *,
    runtime: CanonicalProjectorRuntime,
) -> dict[str, Any]:
    latest = runtime.fetch_latest_checkpoint()
    if latest is None:
        raise RuntimeError('S14-P5 proof expected projector checkpoint to exist')
    return {
        'checkpoint_revision': latest.checkpoint_revision,
        'last_source_offset_or_equivalent': latest.last_source_offset_or_equivalent,
        'last_event_id': str(latest.last_event_id),
        'last_ingested_at_utc': latest.last_ingested_at_utc.astimezone(UTC).isoformat(),
    }


def _run_resume_scenario(
    *,
    client: ClickHouseClient,
    database: str,
    stream_key: CanonicalStreamKey,
) -> dict[str, Any]:
    policy_details = _record_policy(
        client=client,
        database=database,
        stream_key=stream_key,
        run_id='s14-p5-policy-resume',
    )
    policy_state = policy_details['policy_state']
    aligned_projector = CanonicalAligned1sProjector(client=client, database=database)

    runtime_1 = CanonicalProjectorRuntime(
        client=client,
        database=database,
        projector_id=_PROJECTOR_ID,
        stream_key=stream_key,
        batch_size=2,
    )
    runtime_1.start()
    first_batch = runtime_1.fetch_next_batch()
    if len(first_batch) != 2:
        raise RuntimeError(
            f'S14-P5 proof expected first resume batch size=2, got {len(first_batch)}'
        )
    first_rows = aligned_projector.project_batch(
        events=first_batch,
        policy=policy_state,
        projector_id=_PROJECTOR_ID,
        projected_at_utc=_PROJECTED_AT_UTC,
    )
    aligned_projector.insert_aggregate_rows(rows=first_rows)
    first_commit = runtime_1.commit_checkpoint(
        last_event=first_batch[-1],
        run_id='s14-p5-resume-phase-1',
        checkpointed_at_utc=_PROJECTED_AT_UTC,
        state={'phase': 'pre-crash', 'rows_written': len(first_rows)},
    )
    runtime_1.stop()

    runtime_2 = CanonicalProjectorRuntime(
        client=client,
        database=database,
        projector_id=_PROJECTOR_ID,
        stream_key=stream_key,
        batch_size=2,
    )
    backfill_result = aligned_projector.backfill_from_runtime(
        runtime=runtime_2,
        policy=policy_state,
        run_id='s14-p5-resume-phase-2',
        projector_id=_PROJECTOR_ID,
        projected_at_utc=_PROJECTED_AT_UTC,
    )
    runtime_2.stop()

    fingerprint = _fetch_aligned_fingerprint(
        client=client,
        database=database,
        stream_key=stream_key,
    )
    checkpoint = _latest_checkpoint_summary(runtime=runtime_2)
    return {
        'policy_status': policy_details['policy_status'],
        'phase_1_commit_status': first_commit.status,
        'phase_2_backfill': {
            'batches_processed': backfill_result.batches_processed,
            'events_processed': backfill_result.events_processed,
            'rows_written': backfill_result.rows_written,
            'last_checkpoint_status': backfill_result.last_checkpoint_status,
        },
        'checkpoint': checkpoint,
        'fingerprint': fingerprint,
    }


def _run_single_pass_scenario(
    *,
    client: ClickHouseClient,
    database: str,
    stream_key: CanonicalStreamKey,
) -> dict[str, Any]:
    policy_details = _record_policy(
        client=client,
        database=database,
        stream_key=stream_key,
        run_id='s14-p5-policy-single-pass',
    )
    policy_state = policy_details['policy_state']
    aligned_projector = CanonicalAligned1sProjector(client=client, database=database)

    runtime = CanonicalProjectorRuntime(
        client=client,
        database=database,
        projector_id=_PROJECTOR_ID,
        stream_key=stream_key,
        batch_size=2,
    )
    backfill_result = aligned_projector.backfill_from_runtime(
        runtime=runtime,
        policy=policy_state,
        run_id='s14-p5-single-pass',
        projector_id=_PROJECTOR_ID,
        projected_at_utc=_PROJECTED_AT_UTC,
    )
    runtime.stop()

    fingerprint = _fetch_aligned_fingerprint(
        client=client,
        database=database,
        stream_key=stream_key,
    )
    checkpoint = _latest_checkpoint_summary(runtime=runtime)
    return {
        'policy_status': policy_details['policy_status'],
        'backfill': {
            'batches_processed': backfill_result.batches_processed,
            'events_processed': backfill_result.events_processed,
            'rows_written': backfill_result.rows_written,
            'last_checkpoint_status': backfill_result.last_checkpoint_status,
        },
        'checkpoint': checkpoint,
        'fingerprint': fingerprint,
    }


def run_s14_p5_proof() -> dict[str, Any]:
    base_settings = MigrationSettings.from_env()
    resume_database = f'{base_settings.database}{_PROOF_DB_SUFFIX_RESUME}'
    single_database = f'{base_settings.database}{_PROOF_DB_SUFFIX_SINGLE}'
    stream_key = CanonicalStreamKey(
        source_id='binance',
        stream_id='binance_spot_trades',
        partition_id='btcusdt',
    )
    fixture_events = _fixture_events()

    admin_client = _build_clickhouse_client(base_settings)
    resume_runner = MigrationRunner(
        settings=MigrationSettings(
            host=base_settings.host,
            port=base_settings.port,
            user=base_settings.user,
            password=base_settings.password,
            database=resume_database,
        )
    )
    single_runner = MigrationRunner(
        settings=MigrationSettings(
            host=base_settings.host,
            port=base_settings.port,
            user=base_settings.user,
            password=base_settings.password,
            database=single_database,
        )
    )
    try:
        admin_client.execute(f'DROP DATABASE IF EXISTS {resume_database} SYNC')
        admin_client.execute(f'DROP DATABASE IF EXISTS {single_database} SYNC')
        resume_runner.migrate()
        single_runner.migrate()

        resume_seed_statuses = _seed_events(
            client=admin_client,
            database=resume_database,
            stream_key=stream_key,
            events=fixture_events,
        )
        single_seed_statuses = _seed_events(
            client=admin_client,
            database=single_database,
            stream_key=stream_key,
            events=fixture_events,
        )
        if resume_seed_statuses != ['inserted'] * len(fixture_events):
            raise RuntimeError(
                'S14-P5 proof expected resume scenario seed inserts only, '
                f'got {resume_seed_statuses}'
            )
        if single_seed_statuses != ['inserted'] * len(fixture_events):
            raise RuntimeError(
                'S14-P5 proof expected single-pass scenario seed inserts only, '
                f'got {single_seed_statuses}'
            )

        resume_run = _run_resume_scenario(
            client=admin_client,
            database=resume_database,
            stream_key=stream_key,
        )
        single_run = _run_single_pass_scenario(
            client=admin_client,
            database=single_database,
            stream_key=stream_key,
        )

        resume_fingerprint = cast(dict[str, Any], resume_run['fingerprint'])
        single_fingerprint = cast(dict[str, Any], single_run['fingerprint'])
        if (
            resume_fingerprint['fingerprint_hash_sha256']
            != single_fingerprint['fingerprint_hash_sha256']
        ):
            raise RuntimeError(
                'S14-P5 proof failed: projector replay fingerprint differs between '
                'resume and single-pass scenarios'
            )
        if resume_fingerprint['rows'] != single_fingerprint['rows']:
            raise RuntimeError(
                'S14-P5 proof failed: projector replay rows differ between resume '
                'and single-pass scenarios'
            )

        resume_checkpoint = cast(dict[str, Any], resume_run['checkpoint'])
        single_checkpoint = cast(dict[str, Any], single_run['checkpoint'])
        if (
            resume_checkpoint['checkpoint_revision']
            != single_checkpoint['checkpoint_revision']
        ):
            raise RuntimeError(
                'S14-P5 proof failed: checkpoint revision mismatch between '
                'resume and single-pass scenarios'
            )

        return {
            'proof_scope': (
                'Slice 14 S14-P5 projector replay determinism with checkpoint '
                'resume behavior'
            ),
            'stream_key': {
                'source_id': stream_key.source_id,
                'stream_id': stream_key.stream_id,
                'partition_id': stream_key.partition_id,
            },
            'fixture_event_count': len(fixture_events),
            'resume_run': {
                'proof_database': resume_database,
                'seed_statuses': resume_seed_statuses,
                'details': resume_run,
            },
            'single_pass_run': {
                'proof_database': single_database,
                'seed_statuses': single_seed_statuses,
                'details': single_run,
            },
            'deterministic_match': True,
        }
    finally:
        resume_runner.close()
        single_runner.close()
        admin_client.execute(f'DROP DATABASE IF EXISTS {resume_database} SYNC')
        admin_client.execute(f'DROP DATABASE IF EXISTS {single_database} SYNC')
        admin_client.disconnect()


def main() -> None:
    payload = run_s14_p5_proof()
    repo_root = Path.cwd().resolve()
    if repo_root.name == 'control-plane':
        repo_root = repo_root.parent
    output_path = (
        repo_root
        / 'spec'
        / 'slices'
        / 'slice-14-event-sourcing-core'
        / 'proof-s14-p5-projector-resume-determinism.json'
    )
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(
        json.dumps(payload, indent=2, sort_keys=True) + '\n',
        encoding='utf-8',
    )
    print(json.dumps(payload, indent=2, sort_keys=True))


if __name__ == '__main__':
    main()
