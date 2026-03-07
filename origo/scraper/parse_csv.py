from __future__ import annotations

import csv
import hashlib
import io
import json
import re
from datetime import UTC, datetime

from .contracts import MetricValue, ParsedRecord, RawArtifact

_PARSER_NAME = 'csv_parser'
_PARSER_VERSION = '1.0.0'
_HEADER_PATTERN = re.compile(r'[^a-zA-Z0-9]+')


def _normalize_header_name(value: str, fallback_index: int) -> str:
    lowered = value.strip().lower()
    if lowered == '':
        return f'column_{fallback_index}'
    normalized = _HEADER_PATTERN.sub('_', lowered).strip('_')
    if normalized == '':
        return f'column_{fallback_index}'
    if normalized[0].isdigit():
        return f'column_{normalized}'
    return normalized


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


def parse_csv_artifact(*, artifact: RawArtifact) -> list[ParsedRecord]:
    if artifact.artifact_format != 'csv':
        raise ValueError(
            f'parse_csv_artifact requires artifact_format=csv, got {artifact.artifact_format}'
        )

    csv_text = artifact.content.decode('utf-8-sig')
    reader = csv.reader(io.StringIO(csv_text))
    rows = list(reader)
    if len(rows) < 2:
        raise RuntimeError('CSV parser requires a header row and at least one data row')

    raw_headers = rows[0]
    headers = [
        _normalize_header_name(value, idx)
        for idx, value in enumerate(raw_headers, start=1)
    ]
    parsed_at_utc = datetime.now(UTC)
    records: list[ParsedRecord] = []

    for row_index, row in enumerate(rows[1:], start=1):
        if len(row) == 0:
            continue
        payload: dict[str, MetricValue] = {'row_index': str(row_index)}
        for col_index, header in enumerate(headers):
            if col_index >= len(row):
                payload[header] = None
                continue
            value = row[col_index].strip()
            payload[header] = value if value != '' else None

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

    if len(records) == 0:
        raise RuntimeError('CSV parser did not produce any records')

    return records
