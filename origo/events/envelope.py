from __future__ import annotations

from dataclasses import dataclass
from typing import Final, TypedDict


@dataclass(frozen=True)
class CanonicalEventEnvelopeField:
    name: str
    logical_type: str
    clickhouse_type: str
    nullable: bool
    is_identity_key: bool
    description: str


class CanonicalEventEnvelopeFieldRecord(TypedDict):
    name: str
    logical_type: str
    clickhouse_type: str
    nullable: bool
    is_identity_key: bool
    description: str


class CanonicalEventEnvelopeContract(TypedDict):
    envelope_version: int
    identity_fields: list[str]
    fields: list[CanonicalEventEnvelopeFieldRecord]


CANONICAL_EVENT_ENVELOPE_VERSION: Final[int] = 1
CANONICAL_EVENT_IDENTITY_FIELDS: Final[tuple[str, str, str, str]] = (
    'source_id',
    'stream_id',
    'partition_id',
    'source_offset_or_equivalent',
)

CANONICAL_EVENT_ENVELOPE_FIELDS: Final[tuple[CanonicalEventEnvelopeField, ...]] = (
    CanonicalEventEnvelopeField(
        name='envelope_version',
        logical_type='uint16',
        clickhouse_type='UInt16',
        nullable=False,
        is_identity_key=False,
        description='Canonical event-envelope schema version.',
    ),
    CanonicalEventEnvelopeField(
        name='event_id',
        logical_type='uuid',
        clickhouse_type='UUID',
        nullable=False,
        is_identity_key=False,
        description='Deterministic canonical event identifier from writer contract.',
    ),
    CanonicalEventEnvelopeField(
        name='source_id',
        logical_type='string',
        clickhouse_type='LowCardinality(String)',
        nullable=False,
        is_identity_key=True,
        description='Canonical source key (for example binance, fred, or bitcoin_core).',
    ),
    CanonicalEventEnvelopeField(
        name='stream_id',
        logical_type='string',
        clickhouse_type='LowCardinality(String)',
        nullable=False,
        is_identity_key=True,
        description='Logical source stream key inside source_id.',
    ),
    CanonicalEventEnvelopeField(
        name='partition_id',
        logical_type='string',
        clickhouse_type='String',
        nullable=False,
        is_identity_key=True,
        description='Source partition/shard identifier for exactly-once identity.',
    ),
    CanonicalEventEnvelopeField(
        name='source_offset_or_equivalent',
        logical_type='string',
        clickhouse_type='String',
        nullable=False,
        is_identity_key=True,
        description='Source offset or deterministic source event key.',
    ),
    CanonicalEventEnvelopeField(
        name='source_event_time_utc',
        logical_type='datetime64_utc_ns',
        clickhouse_type="Nullable(DateTime64(9, 'UTC'))",
        nullable=True,
        is_identity_key=False,
        description='Highest-precision source event timestamp when available.',
    ),
    CanonicalEventEnvelopeField(
        name='ingested_at_utc',
        logical_type='datetime64_utc_ms',
        clickhouse_type="DateTime64(3, 'UTC')",
        nullable=False,
        is_identity_key=False,
        description='Canonical ingest timestamp written by Origo writer.',
    ),
    CanonicalEventEnvelopeField(
        name='payload_content_type',
        logical_type='string',
        clickhouse_type='LowCardinality(String)',
        nullable=False,
        is_identity_key=False,
        description='Raw payload media type (for example application/json).',
    ),
    CanonicalEventEnvelopeField(
        name='payload_encoding',
        logical_type='string',
        clickhouse_type='LowCardinality(String)',
        nullable=False,
        is_identity_key=False,
        description='Raw payload text/binary encoding identifier.',
    ),
    CanonicalEventEnvelopeField(
        name='payload_raw',
        logical_type='bytes',
        clickhouse_type='String',
        nullable=False,
        is_identity_key=False,
        description='Canonical immutable raw source payload bytes.',
    ),
    CanonicalEventEnvelopeField(
        name='payload_sha256_raw',
        logical_type='sha256_hex',
        clickhouse_type='FixedString(64)',
        nullable=False,
        is_identity_key=False,
        description='SHA256 digest of payload_raw bytes.',
    ),
    CanonicalEventEnvelopeField(
        name='payload_json',
        logical_type='json_string',
        clickhouse_type='String',
        nullable=False,
        is_identity_key=False,
        description='Deterministic parsed payload representation derived from payload_raw.',
    ),
)


def canonical_event_envelope_contract() -> CanonicalEventEnvelopeContract:
    return {
        'envelope_version': CANONICAL_EVENT_ENVELOPE_VERSION,
        'identity_fields': list(CANONICAL_EVENT_IDENTITY_FIELDS),
        'fields': [
            {
                'name': field.name,
                'logical_type': field.logical_type,
                'clickhouse_type': field.clickhouse_type,
                'nullable': field.nullable,
                'is_identity_key': field.is_identity_key,
                'description': field.description,
            }
            for field in CANONICAL_EVENT_ENVELOPE_FIELDS
        ],
    }
