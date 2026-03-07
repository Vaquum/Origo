from __future__ import annotations

import hashlib
import json
from datetime import UTC, datetime
from typing import Any, cast

from .contracts import MetricValue, ParsedRecord, RawArtifact

_PARSER_NAME = 'json_parser'
_PARSER_VERSION = '1.0.0'


def _value_to_metric(value: Any) -> MetricValue:
    if value is None:
        return None
    if isinstance(value, (str, int, float, bool)):
        return value
    return json.dumps(value, sort_keys=True, separators=(',', ':'), ensure_ascii=True)


def _flatten_payload(value: Any) -> dict[str, MetricValue]:
    if isinstance(value, dict):
        source = cast(dict[object, object], value)
        if len(source) == 0:
            return {'value': '{}'}
        payload: dict[str, MetricValue] = {}
        for key, item_value in source.items():
            if not isinstance(key, str) or key.strip() == '':
                raise RuntimeError('JSON object keys must be non-empty strings')
            payload[key] = _value_to_metric(item_value)
        return payload
    return {'value': _value_to_metric(value)}


def _build_record_id(
    *,
    artifact_id: str,
    row_index: int,
    payload: dict[str, MetricValue],
) -> str:
    digest = hashlib.sha256()
    digest.update(artifact_id.encode('utf-8'))
    digest.update(b'|')
    digest.update(str(row_index).encode('utf-8'))
    digest.update(b'|')
    digest.update(
        json.dumps(
            payload, sort_keys=True, separators=(',', ':'), ensure_ascii=True
        ).encode('utf-8')
    )
    return digest.hexdigest()


def parse_json_artifact(*, artifact: RawArtifact) -> list[ParsedRecord]:
    if artifact.artifact_format != 'json':
        raise ValueError(
            f'parse_json_artifact requires artifact_format=json, got {artifact.artifact_format}'
        )

    try:
        decoded = cast(object, json.loads(artifact.content.decode('utf-8')))
    except json.JSONDecodeError as exc:
        raise RuntimeError(f'Invalid JSON artifact: {exc.msg}') from exc

    parsed_at_utc = datetime.now(UTC)
    records: list[ParsedRecord] = []

    if isinstance(decoded, list):
        rows = cast(list[object], decoded)
        if len(rows) == 0:
            raise RuntimeError('JSON parser cannot parse empty top-level list')
        for row_index, item in enumerate(rows, start=1):
            payload = _flatten_payload(item)
            payload['row_index'] = row_index
            record_id = _build_record_id(
                artifact_id=artifact.artifact_id,
                row_index=row_index,
                payload=payload,
            )
            records.append(
                ParsedRecord(
                    record_id=record_id,
                    artifact_id=artifact.artifact_id,
                    payload=payload,
                    parser_name=_PARSER_NAME,
                    parser_version=_PARSER_VERSION,
                    parsed_at_utc=parsed_at_utc,
                )
            )
    else:
        payload = _flatten_payload(decoded)
        payload['row_index'] = 1
        record_id = _build_record_id(
            artifact_id=artifact.artifact_id,
            row_index=1,
            payload=payload,
        )
        records.append(
            ParsedRecord(
                record_id=record_id,
                artifact_id=artifact.artifact_id,
                payload=payload,
                parser_name=_PARSER_NAME,
                parser_version=_PARSER_VERSION,
                parsed_at_utc=parsed_at_utc,
            )
        )

    return records
