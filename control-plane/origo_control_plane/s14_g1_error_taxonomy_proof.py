from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, cast

from origo.events.aligned_projector import (
    AlignedProjectionPolicyInput,
    CanonicalAlignedPolicyStore,
)
from origo.events.errors import EventWriterError, ProjectorRuntimeError
from origo.events.ingest_state import CanonicalStreamKey
from origo.events.projector import CanonicalProjectorRuntime
from origo.events.writer import CanonicalEventWriteInput, build_canonical_event_row


@dataclass(frozen=True)
class _FakeClickHouseClient:
    def execute(
        self,
        _query: str,
        _params: dict[str, object] | None = None,
    ) -> list[tuple[object, ...]]:
        return []


def run_s14_g1_proof() -> dict[str, Any]:
    writer_error_codes: list[str] = []
    projector_error_codes: list[str] = []
    fake_client = cast(Any, _FakeClickHouseClient())

    try:
        build_canonical_event_row(
            CanonicalEventWriteInput(
                source_id='binance',
                stream_id='binance_spot_trades',
                partition_id='btcusdt',
                source_offset_or_equivalent='100',
                source_event_time_utc=datetime(2024, 1, 1, 0, 0, 1, tzinfo=UTC),
                ingested_at_utc=datetime(2024, 1, 1, 0, 0, 2),
                payload_content_type='application/json',
                payload_encoding='utf-8',
                payload_raw=b'{"trade_id":"1","price":"1.00000000","qty":"1.00000000","quote_qty":"1.00000000"}',
            )
        )
    except EventWriterError as exc:
        writer_error_codes.append(exc.code)

    try:
        CanonicalProjectorRuntime(
            client=fake_client,
            database='origo',
            projector_id='projector',
            stream_key=CanonicalStreamKey(
                source_id='binance',
                stream_id='binance_spot_trades',
                partition_id='btcusdt',
            ),
            batch_size=0,
        )
    except ProjectorRuntimeError as exc:
        projector_error_codes.append(exc.code)

    runtime = CanonicalProjectorRuntime(
        client=fake_client,
        database='origo',
        projector_id='projector',
        stream_key=CanonicalStreamKey(
            source_id='binance',
            stream_id='binance_spot_trades',
            partition_id='btcusdt',
        ),
        batch_size=1,
    )
    try:
        runtime.stop()
    except ProjectorRuntimeError as exc:
        projector_error_codes.append(exc.code)

    try:
        CanonicalAlignedPolicyStore(client=fake_client, database='origo').record_policy(
            AlignedProjectionPolicyInput(
                view_id='aligned_1s_raw',
                view_version=0,
                stream_key=CanonicalStreamKey(
                    source_id='binance',
                    stream_id='binance_spot_trades',
                    partition_id='btcusdt',
                ),
                bucket_size_seconds=1,
                tier_policy='hot_1s_warm_1m',
                retention_hot_days=7,
                retention_warm_days=90,
                recorded_by_run_id='run-1',
                recorded_at_utc=datetime(2024, 1, 1, 0, 0, 0, tzinfo=UTC),
            )
        )
    except ProjectorRuntimeError as exc:
        projector_error_codes.append(exc.code)

    expected_writer_codes = {'WRITER_INVALID_TIMESTAMP'}
    expected_projector_codes = {
        'PROJECTOR_INVALID_BATCH_SIZE',
        'PROJECTOR_NOT_STARTED',
        'ALIGNED_POLICY_INVALID_INPUT',
    }
    if set(writer_error_codes) != expected_writer_codes:
        raise RuntimeError(
            'S14-G1 proof expected typed writer error codes '
            f'{sorted(expected_writer_codes)}, got {sorted(set(writer_error_codes))}'
        )
    if set(projector_error_codes) != expected_projector_codes:
        raise RuntimeError(
            'S14-G1 proof expected typed projector error codes '
            f'{sorted(expected_projector_codes)}, got {sorted(set(projector_error_codes))}'
        )

    return {
        'proof_scope': (
            'Slice 14 S14-G1 fail-loud invariants and typed error taxonomy '
            'for event writer/projectors'
        ),
        'writer_error_codes': writer_error_codes,
        'projector_error_codes': projector_error_codes,
        'typed_error_taxonomy_verified': True,
    }


def main() -> None:
    payload = run_s14_g1_proof()
    repo_root = Path.cwd().resolve()
    if repo_root.name == 'control-plane':
        repo_root = repo_root.parent
    output_path = (
        repo_root
        / 'spec'
        / 'slices'
        / 'slice-14-event-sourcing-core'
        / 'guardrails-proof-s14-g1-error-taxonomy.json'
    )
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(
        json.dumps(payload, indent=2, sort_keys=True) + '\n',
        encoding='utf-8',
    )
    print(json.dumps(payload, indent=2, sort_keys=True))


if __name__ == '__main__':
    main()
