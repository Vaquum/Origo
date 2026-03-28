from __future__ import annotations

import base64
import csv
import hashlib
import io
import zipfile
from dataclasses import dataclass
from types import SimpleNamespace
from typing import Any

import pytest

from origo.events.backfill_state import PartitionCanonicalProof


def _build_okx_zip_payload() -> tuple[bytes, str]:
    csv_buffer = io.StringIO()
    writer = csv.writer(csv_buffer)
    writer.writerow(
        (
            'instrument_name',
            'trade_id',
            'side',
            'price',
            'size',
            'created_time',
        )
    )
    writer.writerow(('BTC-USDT', '251770796', 'buy', '54883.2', '0.001', '1633651200000'))
    writer.writerow(('BTC-USDT', '251770796', 'buy', '54883.2', '0.001', '1633651200000'))
    writer.writerow(('BTC-USDT', '251770797', 'sell', '54883.3', '0.002', '1633651201000'))
    csv_payload = csv_buffer.getvalue().encode('utf-8')

    zip_buffer = io.BytesIO()
    with zipfile.ZipFile(zip_buffer, 'w', compression=zipfile.ZIP_DEFLATED) as archive:
        archive.writestr('BTC-USDT-trades-2021-10-08.csv', csv_payload)

    zip_payload = zip_buffer.getvalue()
    content_md5_b64 = base64.b64encode(hashlib.md5(zip_payload).digest()).decode('ascii')
    return zip_payload, content_md5_b64


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
        self.run_id = 'run-okx-asset-contract'
        self.log = _FakeLog(messages=[])

    def asset_partition_key_for_output(self) -> str:
        return self._partition_id


class _FakeResponse:
    def __init__(self, *, content: bytes, content_md5_b64: str) -> None:
        self.content = content
        self.headers = {'Content-MD5': content_md5_b64}

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


def test_daily_okx_asset_passes_raw_duplicate_rows_to_proof_and_writer(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv('CLICKHOUSE_HOST', 'clickhouse')
    monkeypatch.setenv('CLICKHOUSE_PORT', '9000')
    monkeypatch.setenv('CLICKHOUSE_USER', 'default')
    monkeypatch.setenv('CLICKHOUSE_PASSWORD', 'password')
    monkeypatch.setenv('CLICKHOUSE_DATABASE', 'origo')
    from origo_control_plane.assets import daily_okx_spot_trades_to_origo as module

    from origo.events.ingest_state import CanonicalStreamKey

    zip_payload, content_md5_b64 = _build_okx_zip_payload()
    context = _FakeContext(partition_id='2021-10-08')
    proof_seen_event_count: list[int] = []
    writer_seen_event_count: list[int] = []

    class _FakeStateStore:
        def __init__(self, *_args: Any, **_kwargs: Any) -> None:
            return None

        def assert_partition_can_execute_or_raise(self, **_kwargs: Any) -> None:
            return None

        def assess_partition_execution(self, **_kwargs: Any) -> SimpleNamespace:
            return SimpleNamespace(
                latest_proof_state=None,
                canonical_row_count=0,
                active_quarantine=False,
            )

        def record_source_manifest(self, **_kwargs: Any) -> None:
            return None

        def record_partition_state(self, **_kwargs: Any) -> None:
            return None

        def prove_partition_or_quarantine(self, **kwargs: Any) -> SimpleNamespace:
            assert kwargs['canonical_proof'] is None
            return SimpleNamespace(
                state='proved_complete',
                proof_digest_sha256='digest-okx-2021-10-08',
            )

    def _fake_build_okx_partition_source_proof(**kwargs: Any) -> SimpleNamespace:
        events = kwargs['events']
        proof_seen_event_count.append(len(events))
        return SimpleNamespace(
            stream_key=CanonicalStreamKey(
                source_id='okx',
                stream_id='okx_spot_trades',
                partition_id='2021-10-08',
            ),
            source_row_count=2,
            source_artifact_identity={
                'raw_csv_row_count': 3,
                'deduplicated_exact_duplicate_rows': 1,
            },
        )

    def _fake_write_okx_spot_trades_to_canonical(**kwargs: Any) -> dict[str, int]:
        events = kwargs['events']
        writer_seen_event_count.append(len(events))
        return {
            'raw_row_count': 3,
            'deduplicated_exact_duplicate_rows': 1,
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
            execution_mode='backfill',
        ),
    )
    monkeypatch.setattr(module, 'apply_runtime_audit_mode_or_raise', lambda **_kwargs: None)
    monkeypatch.setattr(module, 'wait_for_source_rate_gate_or_raise', lambda **_kwargs: None)
    monkeypatch.setattr(
        module,
        'resolve_okx_daily_file_url_or_raise',
        lambda *, date_str: (f'BTC-USDT-trades-{date_str}.zip', 'https://example.test/okx.zip'),
    )
    monkeypatch.setattr(
        module.requests,
        'get',
        lambda *_args, **_kwargs: _FakeResponse(
            content=zip_payload,
            content_md5_b64=content_md5_b64,
        ),
    )
    monkeypatch.setattr(module, 'ClickhouseClient', lambda **_kwargs: _FakeClickHouseClient())
    monkeypatch.setattr(module, 'CanonicalBackfillStateStore', _FakeStateStore)
    monkeypatch.setattr(
        module,
        'build_okx_partition_source_proof',
        _fake_build_okx_partition_source_proof,
    )
    monkeypatch.setattr(
        module,
        'write_okx_spot_trades_to_canonical',
        _fake_write_okx_spot_trades_to_canonical,
    )
    monkeypatch.setattr(
        module,
        'project_okx_spot_trades_native',
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
        'project_okx_spot_trades_aligned',
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

    result = module.insert_daily_okx_spot_trades_to_origo.op.compute_fn.decorated_fn(
        context
    )

    assert proof_seen_event_count == [3]
    assert writer_seen_event_count == [3]
    assert result['raw_row_count'] == 3
    assert result['deduplicated_exact_duplicate_rows'] == 1
    assert result['rows_processed'] == 2
    assert result['rows_inserted'] == 2
    assert result['rows_duplicate'] == 0


def test_daily_okx_asset_uses_writer_repair_when_reconcile_detects_partial_canonical_rows(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv('CLICKHOUSE_HOST', 'clickhouse')
    monkeypatch.setenv('CLICKHOUSE_PORT', '9000')
    monkeypatch.setenv('CLICKHOUSE_USER', 'default')
    monkeypatch.setenv('CLICKHOUSE_PASSWORD', 'password')
    monkeypatch.setenv('CLICKHOUSE_DATABASE', 'origo')
    from origo_control_plane.assets import daily_okx_spot_trades_to_origo as module

    from origo.events.ingest_state import CanonicalStreamKey

    zip_payload, content_md5_b64 = _build_okx_zip_payload()
    context = _FakeContext(partition_id='2021-10-08')
    writer_calls: list[int] = []

    class _FakeStateStore:
        def __init__(self, *_args: Any, **_kwargs: Any) -> None:
            return None

        def assert_partition_can_execute_or_raise(self, **_kwargs: Any) -> None:
            return None

        def assess_partition_execution(self, **_kwargs: Any) -> SimpleNamespace:
            return SimpleNamespace(
                latest_proof_state='source_manifested',
                canonical_row_count=100000,
                active_quarantine=False,
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
                canonical_row_count=100000,
                canonical_unique_offset_count=100000,
                first_offset_or_equivalent='1',
                last_offset_or_equivalent='100000',
                canonical_offset_digest_sha256='offset-digest',
                canonical_identity_digest_sha256='identity-digest-mismatch',
                gap_count=0,
                duplicate_count=0,
            )

        def prove_partition_or_quarantine(self, **_kwargs: Any) -> SimpleNamespace:
            return SimpleNamespace(
                state='proved_complete',
                proof_digest_sha256='digest-okx-2021-10-08',
            )

    def _fake_build_okx_partition_source_proof(**_kwargs: Any) -> SimpleNamespace:
        return SimpleNamespace(
            stream_key=CanonicalStreamKey(
                source_id='okx',
                stream_id='okx_spot_trades',
                partition_id='2021-10-08',
            ),
            source_row_count=2,
            source_artifact_identity_json='{"raw_csv_row_count": 3, "deduplicated_exact_duplicate_rows": 1}',
            source_offset_digest_sha256='offset-digest-source',
            source_identity_digest_sha256='identity-digest-source',
            first_offset_or_equivalent='251770796',
            last_offset_or_equivalent='251770797',
            allow_duplicate_offsets=False,
        )

    def _fake_write_okx_spot_trades_to_canonical(**kwargs: Any) -> dict[str, int]:
        writer_calls.append(len(kwargs['events']))
        return {
            'raw_row_count': 3,
            'deduplicated_exact_duplicate_rows': 1,
            'rows_processed': 2,
            'rows_inserted': 1,
            'rows_duplicate': 1,
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
    monkeypatch.setattr(module, 'wait_for_source_rate_gate_or_raise', lambda **_kwargs: None)
    monkeypatch.setattr(
        module,
        'resolve_okx_daily_file_url_or_raise',
        lambda *, date_str: (f'BTC-USDT-trades-{date_str}.zip', 'https://example.test/okx.zip'),
    )
    monkeypatch.setattr(
        module.requests,
        'get',
        lambda *_args, **_kwargs: _FakeResponse(
            content=zip_payload,
            content_md5_b64=content_md5_b64,
        ),
    )
    monkeypatch.setattr(module, 'ClickhouseClient', lambda **_kwargs: _FakeClickHouseClient())
    monkeypatch.setattr(module, 'CanonicalBackfillStateStore', _FakeStateStore)
    monkeypatch.setattr(
        module,
        'build_okx_partition_source_proof',
        _fake_build_okx_partition_source_proof,
    )
    monkeypatch.setattr(
        module,
        'write_okx_spot_trades_to_canonical',
        _fake_write_okx_spot_trades_to_canonical,
    )
    monkeypatch.setattr(
        module,
        'project_okx_spot_trades_native',
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
        'project_okx_spot_trades_aligned',
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

    result = module.insert_daily_okx_spot_trades_to_origo.op.compute_fn.decorated_fn(
        context
    )

    assert writer_calls == [3]
    assert result['write_path'] == 'reconcile_writer_repair'


def test_daily_okx_asset_uses_proof_only_reconcile_when_canonical_matches_source(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv('CLICKHOUSE_HOST', 'clickhouse')
    monkeypatch.setenv('CLICKHOUSE_PORT', '9000')
    monkeypatch.setenv('CLICKHOUSE_USER', 'default')
    monkeypatch.setenv('CLICKHOUSE_PASSWORD', 'password')
    monkeypatch.setenv('CLICKHOUSE_DATABASE', 'origo')
    from origo_control_plane.assets import daily_okx_spot_trades_to_origo as module

    from origo.events.ingest_state import CanonicalStreamKey

    zip_payload, content_md5_b64 = _build_okx_zip_payload()
    context = _FakeContext(partition_id='2021-10-08')
    writer_calls: list[int] = []

    class _FakeStateStore:
        def __init__(self, *_args: Any, **_kwargs: Any) -> None:
            return None

        def assert_partition_can_execute_or_raise(self, **_kwargs: Any) -> None:
            return None

        def assess_partition_execution(self, **_kwargs: Any) -> SimpleNamespace:
            return SimpleNamespace(
                latest_proof_state='source_manifested',
                canonical_row_count=2,
                active_quarantine=False,
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
                canonical_unique_offset_count=2,
                first_offset_or_equivalent='251770796',
                last_offset_or_equivalent='251770797',
                canonical_offset_digest_sha256='offset-digest-source',
                canonical_identity_digest_sha256='identity-digest-source',
                gap_count=0,
                duplicate_count=0,
            )

        def prove_partition_or_quarantine(self, **kwargs: Any) -> SimpleNamespace:
            assert kwargs['canonical_proof'].canonical_identity_digest_sha256 == (
                'identity-digest-source'
            )
            return SimpleNamespace(
                state='proved_complete',
                proof_digest_sha256='digest-okx-2021-10-08',
            )

    def _fake_build_okx_partition_source_proof(**_kwargs: Any) -> SimpleNamespace:
        return SimpleNamespace(
            stream_key=CanonicalStreamKey(
                source_id='okx',
                stream_id='okx_spot_trades',
                partition_id='2021-10-08',
            ),
            source_row_count=2,
            source_artifact_identity_json='{"raw_csv_row_count": 3, "deduplicated_exact_duplicate_rows": 1}',
            source_offset_digest_sha256='offset-digest-source',
            source_identity_digest_sha256='identity-digest-source',
            first_offset_or_equivalent='251770796',
            last_offset_or_equivalent='251770797',
            allow_duplicate_offsets=False,
        )

    def _fake_write_okx_spot_trades_to_canonical(**kwargs: Any) -> dict[str, int]:
        writer_calls.append(len(kwargs['events']))
        return {
            'raw_row_count': 3,
            'deduplicated_exact_duplicate_rows': 1,
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
    monkeypatch.setattr(module, 'wait_for_source_rate_gate_or_raise', lambda **_kwargs: None)
    monkeypatch.setattr(
        module,
        'resolve_okx_daily_file_url_or_raise',
        lambda *, date_str: (f'BTC-USDT-trades-{date_str}.zip', 'https://example.test/okx.zip'),
    )
    monkeypatch.setattr(
        module.requests,
        'get',
        lambda *_args, **_kwargs: _FakeResponse(
            content=zip_payload,
            content_md5_b64=content_md5_b64,
        ),
    )
    monkeypatch.setattr(module, 'ClickhouseClient', lambda **_kwargs: _FakeClickHouseClient())
    monkeypatch.setattr(module, 'CanonicalBackfillStateStore', _FakeStateStore)
    monkeypatch.setattr(
        module,
        'build_okx_partition_source_proof',
        _fake_build_okx_partition_source_proof,
    )
    monkeypatch.setattr(
        module,
        'write_okx_spot_trades_to_canonical',
        _fake_write_okx_spot_trades_to_canonical,
    )
    monkeypatch.setattr(
        module,
        'project_okx_spot_trades_native',
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
        'project_okx_spot_trades_aligned',
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

    result = module.insert_daily_okx_spot_trades_to_origo.op.compute_fn.decorated_fn(
        context
    )

    assert writer_calls == []
    assert result['write_path'] == 'reconcile_proof_only'
    assert result['raw_row_count'] == 3
    assert result['deduplicated_exact_duplicate_rows'] == 1
    assert result['rows_processed'] == 2
    assert result['rows_inserted'] == 0
    assert result['rows_duplicate'] == 2
