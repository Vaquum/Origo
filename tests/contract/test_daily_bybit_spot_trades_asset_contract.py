from __future__ import annotations

import gzip
from dataclasses import dataclass
from types import SimpleNamespace
from typing import Any

import pytest

from origo.events.backfill_state import PartitionCanonicalProof


def _build_bybit_gzip_payload() -> bytes:
    return (
        b'timestamp,symbol,side,size,price,tickDirection,trdMatchID,grossValue,homeNotional,foreignNotional\n'
        b'1606197806.8067,BTCUSDT,Sell,0.007,18275.5,PlusTick,8f725b02-daf4-5af4-aa66-da3e04563a5a,12792850000.0,0.007,127.9285\n'
        b'1606197806.8067,BTCUSDT,Sell,0.007,18275.5,PlusTick,8f725b02-daf4-5af4-aa66-da3e04563a5a,12792850000.0,0.007,127.9285\n'
    )


@dataclass
class _FakeLog:
    messages: list[str]

    def info(self, message: str) -> None:
        self.messages.append(message)

    def warning(self, message: str) -> None:
        self.messages.append(message)


class _FakeContext:
    def __init__(self, *, partition_id: str) -> None:
        self._partition_id = partition_id
        self.run_id = 'run-bybit-asset-contract'
        self.log = _FakeLog(messages=[])

    def asset_partition_key_for_output(self) -> str:
        return self._partition_id


class _FakeResponse:
    def __init__(self, *, content: bytes) -> None:
        self.content = content
        self.headers = {'ETag': 'etag-bybit'}

    def raise_for_status(self) -> None:
        return None


class _FakeClickHouseClient:
    def disconnect(self) -> None:
        return None


@dataclass(frozen=True)
class _FakeProjectionSummary:
    payload: dict[str, int]

    def to_dict(self) -> dict[str, int]:
        return self.payload


def test_daily_bybit_asset_uses_proof_only_reconcile_when_duplicate_offsets_are_allowed(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv('CLICKHOUSE_HOST', 'clickhouse')
    monkeypatch.setenv('CLICKHOUSE_PORT', '9000')
    monkeypatch.setenv('CLICKHOUSE_USER', 'default')
    monkeypatch.setenv('CLICKHOUSE_PASSWORD', 'password')
    monkeypatch.setenv('CLICKHOUSE_DATABASE', 'origo')
    from origo_control_plane.assets import daily_bybit_spot_trades_to_origo as module

    from origo.events.ingest_state import CanonicalStreamKey

    gzip_payload = gzip.compress(_build_bybit_gzip_payload())
    context = _FakeContext(partition_id='2020-11-24')
    writer_calls: list[int] = []

    class _FakeStateStore:
        def __init__(self, *_args: Any, **_kwargs: Any) -> None:
            return None

        def assert_partition_can_execute_or_raise(self, **_kwargs: Any) -> None:
            return None

        def assess_partition_execution(self, **_kwargs: Any) -> SimpleNamespace:
            return SimpleNamespace(
                latest_proof_state='quarantined',
                canonical_row_count=72676,
                active_quarantine=True,
            )

        def record_source_manifest(self, **_kwargs: Any) -> None:
            return None

        def record_partition_state(self, **_kwargs: Any) -> None:
            return None

        def compute_canonical_partition_proof_or_raise(
            self,
            **_kwargs: Any,
        ) -> PartitionCanonicalProof:
            return PartitionCanonicalProof(
                canonical_row_count=2,
                canonical_unique_offset_count=1,
                first_offset_or_equivalent='8f725b02-daf4-5af4-aa66-da3e04563a5a',
                last_offset_or_equivalent='8f725b02-daf4-5af4-aa66-da3e04563a5a',
                canonical_offset_digest_sha256='offset-digest',
                canonical_identity_digest_sha256='identity-digest',
                gap_count=0,
                duplicate_count=1,
            )

        def prove_partition_or_quarantine(self, **_kwargs: Any) -> SimpleNamespace:
            return SimpleNamespace(
                state='proved_complete',
                proof_digest_sha256='digest-bybit-2020-11-24',
            )

    def _fake_build_bybit_partition_source_proof(**_kwargs: Any) -> SimpleNamespace:
        return SimpleNamespace(
            stream_key=CanonicalStreamKey(
                source_id='bybit',
                stream_id='bybit_spot_trades',
                partition_id='2020-11-24',
            ),
            source_row_count=2,
            source_offset_digest_sha256='offset-digest',
            source_identity_digest_sha256='identity-digest',
            first_offset_or_equivalent='8f725b02-daf4-5af4-aa66-da3e04563a5a',
            last_offset_or_equivalent='8f725b02-daf4-5af4-aa66-da3e04563a5a',
            allow_duplicate_offsets=True,
        )

    def _fake_write_bybit_spot_trades_to_canonical(**kwargs: Any) -> dict[str, int]:
        writer_calls.append(len(kwargs['events']))
        return {
            'rows_processed': 2,
            'rows_inserted': 2,
            'rows_duplicate': 0,
        }

    monkeypatch.setattr(
        module,
        'load_backfill_runtime_contract_or_raise',
        lambda _context: SimpleNamespace(
            runtime_audit_mode='summary',
            projection_mode='deferred',
            execution_mode='reconcile',
        ),
    )
    monkeypatch.setattr(module, 'apply_runtime_audit_mode_or_raise', lambda **_kwargs: None)
    monkeypatch.setattr(
        module,
        'resolve_bybit_daily_file_url',
        lambda *, date_str: (f'BTCUSDT{date_str}.csv.gz', 'https://example.test/bybit.csv.gz'),
    )
    monkeypatch.setattr(
        module.requests,
        'get',
        lambda *_args, **_kwargs: _FakeResponse(content=gzip_payload),
    )
    monkeypatch.setattr(module, 'ClickhouseClient', lambda **_kwargs: _FakeClickHouseClient())
    monkeypatch.setattr(module, 'CanonicalBackfillStateStore', _FakeStateStore)
    monkeypatch.setattr(
        module,
        'build_bybit_partition_source_proof',
        _fake_build_bybit_partition_source_proof,
    )
    monkeypatch.setattr(
        module,
        'write_bybit_spot_trades_to_canonical',
        _fake_write_bybit_spot_trades_to_canonical,
    )
    monkeypatch.setattr(
        module,
        'project_bybit_spot_trades_native',
        lambda **_kwargs: _FakeProjectionSummary(
            payload={
                'partitions_processed': 0,
                'batches_processed': 0,
                'events_processed': 0,
                'rows_written': 0,
            }
        ),
    )
    monkeypatch.setattr(
        module,
        'project_bybit_spot_trades_aligned',
        lambda **_kwargs: _FakeProjectionSummary(
            payload={
                'partitions_processed': 0,
                'policies_recorded': 0,
                'policies_duplicate': 0,
                'batches_processed': 0,
                'events_processed': 0,
                'rows_written': 0,
            }
        ),
    )

    result = module.insert_daily_bybit_spot_trades_to_origo.op.compute_fn.decorated_fn(
        context
    )

    assert writer_calls == []
    assert result['write_path'] == 'reconcile_proof_only'
