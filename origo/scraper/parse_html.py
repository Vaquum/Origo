from __future__ import annotations

import hashlib
import importlib
import json
import re
from datetime import UTC, datetime
from typing import Any, cast

from .contracts import MetricValue, ParsedRecord, RawArtifact

_PARSER_NAME = 'html_table_parser'
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


def _ensure_unique_headers(raw_headers: list[str]) -> list[str]:
    unique_headers: list[str] = []
    seen_counts: dict[str, int] = {}
    for index, raw_header in enumerate(raw_headers, start=1):
        base = _normalize_header_name(raw_header, fallback_index=index)
        count = seen_counts.get(base, 0) + 1
        seen_counts[base] = count
        if count == 1:
            unique_headers.append(base)
        else:
            unique_headers.append(f'{base}_{count}')
    return unique_headers


def _load_beautiful_soup() -> Any:
    try:
        bs4_module = importlib.import_module('bs4')
    except ModuleNotFoundError as exc:
        raise RuntimeError(
            'HTML parsing requires beautifulsoup4. Install with `uv add beautifulsoup4`.'
        ) from exc

    beautiful_soup = getattr(bs4_module, 'BeautifulSoup', None)
    if beautiful_soup is None:
        raise RuntimeError('bs4.BeautifulSoup was not found')
    return beautiful_soup


def _ensure_lxml_installed() -> None:
    try:
        importlib.import_module('lxml')
    except ModuleNotFoundError as exc:
        raise RuntimeError(
            'HTML parsing requires lxml. Install with `uv add lxml`.'
        ) from exc


def _to_text(value: Any) -> str:
    if isinstance(value, str):
        return value
    return str(value)


def _build_record_id(
    *,
    artifact_id: str,
    table_index: int,
    row_index: int,
    payload: dict[str, MetricValue],
) -> str:
    digest = hashlib.sha256()
    digest.update(artifact_id.encode('utf-8'))
    digest.update(b'|')
    digest.update(str(table_index).encode('utf-8'))
    digest.update(b'|')
    digest.update(str(row_index).encode('utf-8'))
    digest.update(b'|')
    digest.update(
        json.dumps(
            payload, sort_keys=True, separators=(',', ':'), ensure_ascii=True
        ).encode('utf-8')
    )
    return digest.hexdigest()


def parse_html_artifact(*, artifact: RawArtifact) -> list[ParsedRecord]:
    if artifact.artifact_format != 'html':
        raise ValueError(
            f'parse_html_artifact requires artifact_format=html, got {artifact.artifact_format}'
        )

    _ensure_lxml_installed()
    BeautifulSoup = _load_beautiful_soup()
    html_text = artifact.content.decode('utf-8')
    soup = BeautifulSoup(html_text, 'lxml')

    records: list[ParsedRecord] = []
    parsed_at_utc = datetime.now(UTC)
    tables = cast(list[Any], soup.find_all('table'))

    for table_index, table in enumerate(tables, start=1):
        rows = cast(list[Any], table.find_all('tr'))
        if len(rows) == 0:
            continue

        header_cells = cast(list[Any], rows[0].find_all(['th', 'td']))
        raw_headers = [_to_text(cell.get_text(strip=True)) for cell in header_cells]
        headers = _ensure_unique_headers(raw_headers)

        data_rows = rows[1:]
        for row_index, row in enumerate(data_rows, start=1):
            cells = cast(list[Any], row.find_all(['th', 'td']))
            values = [_to_text(cell.get_text(strip=True)) for cell in cells]
            if len(values) == 0:
                continue

            payload: dict[str, MetricValue] = {
                'table_index': table_index,
                'row_index': row_index,
            }
            for col_index, header in enumerate(headers):
                payload[header] = values[col_index] if col_index < len(values) else None

            record_id = _build_record_id(
                artifact_id=artifact.artifact_id,
                table_index=table_index,
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
        raise RuntimeError(
            'HTML parser did not produce any records. '
            'Ensure source includes at least one table with data rows.'
        )

    return records
