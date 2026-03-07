from __future__ import annotations

import hashlib
import importlib
import json
from datetime import UTC, datetime
from io import BytesIO
from typing import Any, cast

from .contracts import MetricValue, ParsedRecord, RawArtifact

_PARSER_NAME = 'pdf_text_parser'
_PARSER_VERSION = '1.0.0'


def _load_pdf_reader() -> Any:
    try:
        pypdf_module = importlib.import_module('pypdf')
    except ModuleNotFoundError as exc:
        raise RuntimeError(
            'PDF parsing requires pypdf. Install with `uv add pypdf`.'
        ) from exc
    reader_class = getattr(pypdf_module, 'PdfReader', None)
    if reader_class is None:
        raise RuntimeError('pypdf.PdfReader was not found')
    return reader_class


def _build_record_id(
    *,
    artifact_id: str,
    page_index: int,
    line_index: int,
    payload: dict[str, MetricValue],
) -> str:
    digest = hashlib.sha256()
    digest.update(artifact_id.encode('utf-8'))
    digest.update(b'|')
    digest.update(str(page_index).encode('utf-8'))
    digest.update(b'|')
    digest.update(str(line_index).encode('utf-8'))
    digest.update(b'|')
    digest.update(
        json.dumps(
            payload, sort_keys=True, separators=(',', ':'), ensure_ascii=True
        ).encode('utf-8')
    )
    return digest.hexdigest()


def parse_pdf_artifact(*, artifact: RawArtifact) -> list[ParsedRecord]:
    if artifact.artifact_format != 'pdf':
        raise ValueError(
            f'parse_pdf_artifact requires artifact_format=pdf, got {artifact.artifact_format}'
        )

    PdfReader = _load_pdf_reader()
    reader = PdfReader(BytesIO(artifact.content))
    pages = cast(list[Any], reader.pages)
    if len(pages) == 0:
        raise RuntimeError('PDF parser found zero pages')

    parsed_at_utc = datetime.now(UTC)
    records: list[ParsedRecord] = []

    for page_index, page in enumerate(pages, start=1):
        extracted = page.extract_text()
        text = extracted if isinstance(extracted, str) else ''
        lines = [line.strip() for line in text.splitlines() if line.strip() != '']
        for line_index, line in enumerate(lines, start=1):
            payload: dict[str, MetricValue] = {
                'page_index': page_index,
                'line_index': line_index,
                'text': line,
            }
            record_id = _build_record_id(
                artifact_id=artifact.artifact_id,
                page_index=page_index,
                line_index=line_index,
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
            'PDF parser did not produce any records. '
            'Ensure source PDF has extractable text content.'
        )

    return records
