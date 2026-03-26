from __future__ import annotations

import hashlib

from origo.events.backfill_state import (
    CanonicalBackfillStateStore,
    SourceIdentityMaterial,
    build_partition_source_proof,
)
from origo.events.ingest_state import CanonicalStreamKey


def _sha256_lines(lines: list[str]) -> str:
    digest = hashlib.sha256()
    for line in lines:
        digest.update(line.encode('utf-8'))
        digest.update(b'\n')
    return digest.hexdigest()


class _FakeCanonicalRowsClient:
    def __init__(self, *, rows: list[tuple[object, ...]]) -> None:
        self._rows = rows

    def execute(
        self,
        query: str,
        _params: dict[str, object] | None = None,
    ) -> list[tuple[object, ...]]:
        if 'canonical_event_log' in query:
            return self._rows
        raise AssertionError(f'unexpected query: {query}')


def test_build_partition_source_proof_orders_numeric_offsets_numerically() -> None:
    proof = build_partition_source_proof(
        stream_key=CanonicalStreamKey(
            source_id='binance',
            stream_id='binance_spot_trades',
            partition_id='2024-01-01',
        ),
        offset_ordering='numeric',
        source_artifact_identity={'source_file': 'binance-2024-01-01.csv.gz'},
        materials=[
            SourceIdentityMaterial(
                source_offset_or_equivalent='10',
                event_id='event-10',
                payload_sha256_raw='sha-10',
            ),
            SourceIdentityMaterial(
                source_offset_or_equivalent='2',
                event_id='event-2',
                payload_sha256_raw='sha-2',
            ),
            SourceIdentityMaterial(
                source_offset_or_equivalent='1',
                event_id='event-1',
                payload_sha256_raw='sha-1',
            ),
        ],
        allow_empty_partition=False,
    )

    assert proof.first_offset_or_equivalent == '1'
    assert proof.last_offset_or_equivalent == '10'
    assert proof.source_offset_digest_sha256 == _sha256_lines(['1', '2', '10'])
    assert proof.source_identity_digest_sha256 == _sha256_lines(
        [
            '1|event-1|sha-1',
            '2|event-2|sha-2',
            '10|event-10|sha-10',
        ]
    )


def test_compute_canonical_partition_proof_orders_numeric_offsets_numerically() -> None:
    source_proof = build_partition_source_proof(
        stream_key=CanonicalStreamKey(
            source_id='binance',
            stream_id='binance_spot_trades',
            partition_id='2024-01-01',
        ),
        offset_ordering='numeric',
        source_artifact_identity={'source_file': 'binance-2024-01-01.csv.gz'},
        materials=[
            SourceIdentityMaterial(
                source_offset_or_equivalent='1',
                event_id='event-1',
                payload_sha256_raw='sha-1',
            ),
            SourceIdentityMaterial(
                source_offset_or_equivalent='2',
                event_id='event-2',
                payload_sha256_raw='sha-2',
            ),
            SourceIdentityMaterial(
                source_offset_or_equivalent='10',
                event_id='event-10',
                payload_sha256_raw='sha-10',
            ),
        ],
        allow_empty_partition=False,
    )
    store = CanonicalBackfillStateStore(
        client=_FakeCanonicalRowsClient(
            rows=[
                ('10', 'event-10', 'sha-10'),
                ('1', 'event-1', 'sha-1'),
                ('2', 'event-2', 'sha-2'),
            ]
        ),
        database='origo',
    )

    proof = store.compute_canonical_partition_proof_or_raise(source_proof=source_proof)

    assert proof.first_offset_or_equivalent == '1'
    assert proof.last_offset_or_equivalent == '10'
    assert proof.canonical_offset_digest_sha256 == _sha256_lines(['1', '2', '10'])
    assert proof.canonical_identity_digest_sha256 == _sha256_lines(
        [
            '1|event-1|sha-1',
            '2|event-2|sha-2',
            '10|event-10|sha-10',
        ]
    )
    assert proof.gap_count == 7
