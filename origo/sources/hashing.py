from __future__ import annotations

import hashlib
import math
from collections.abc import Iterable
from datetime import UTC, date, datetime
from decimal import Decimal
from uuid import UUID

from .contracts import Row, StateRecord


def _field(value: object) -> bytes:
    if isinstance(value, bool):
        tag, text = 'b', str(int(value))
    elif isinstance(value, int):
        tag, text = 'i', str(value)
    elif isinstance(value, (Decimal, float)):
        if isinstance(value, float) and not math.isfinite(value):
            raise ValueError('Non-finite values have no canonical encoding.')
        decimal = value if isinstance(value, Decimal) else Decimal(str(value))
        if not decimal.is_finite():
            raise ValueError('Non-finite values have no canonical encoding.')
        tag, text = 'd', format(decimal, 'f')
        if '.' in text:
            text = text.rstrip('0').rstrip('.')
        if decimal == 0:
            text = '0'
    elif isinstance(value, datetime):
        aware = value.replace(tzinfo=UTC) if value.tzinfo is None else value.astimezone(UTC)
        delta = aware - datetime(1970, 1, 1, tzinfo=UTC)
        tag, text = 't', str((delta.days * 86400 + delta.seconds) * 1000000 + delta.microseconds)
    elif isinstance(value, date):
        tag, text = 'D', value.isoformat()
    elif isinstance(value, (str, UUID)):
        tag, text = 's', str(value)
    elif value is None:
        tag, text = 'n', ''
    else:
        raise TypeError(f'Unsupported canonical field type: {type(value).__name__}')
    encoded = text.encode('utf-8')
    return tag.encode() + str(len(encoded)).encode() + b':' + encoded


def content_hash(rows: Iterable[Row], *, schema_version: int) -> str:
    digest = hashlib.sha256(f'origo-source-content-v{schema_version}\n'.encode())
    for row in rows:
        digest.update(str(len(row)).encode() + b':')
        for value in row:
            digest.update(_field(value))
    return digest.hexdigest()


def activation_id(source: str, record: StateRecord) -> str:
    return content_hash(
        ((source, record.partition.key, record.generation, record.revision, record.build_id),),
        schema_version=1,
    )


def state_token(source: str, records: tuple[StateRecord, ...]) -> str:
    rows: list[Row] = [('origo-source-state-v1', source)]
    for record in sorted(
        records, key=lambda item: (item.partition.provisional, item.partition.key)
    ):
        rows.append(
            (record.partition.key, record.partition.provisional, record.generation, record.revision)
        )
        rows.extend((name, digest) for name, digest in sorted(record.component_hashes))
    return content_hash(rows, schema_version=1)
