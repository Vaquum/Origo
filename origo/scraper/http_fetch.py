from __future__ import annotations

import hashlib
from datetime import UTC, datetime
from typing import Final
from urllib.error import HTTPError, URLError
from urllib.parse import urlparse
from urllib.request import Request, urlopen

from .contracts import ArtifactFormat, RawArtifact, ScrapeRunContext, SourceDescriptor

_DEFAULT_TIMEOUT_SECONDS: Final[float] = 30.0


def _normalize_content_type(raw_content_type: str | None) -> str:
    if raw_content_type is None:
        return 'application/octet-stream'
    normalized = raw_content_type.split(';', maxsplit=1)[0].strip().lower()
    if normalized == '':
        return 'application/octet-stream'
    return normalized


def _detect_artifact_format(*, source_uri: str, content_type: str) -> ArtifactFormat:
    if content_type in {'text/html', 'application/xhtml+xml'}:
        return 'html'
    if content_type in {'application/json', 'text/json'}:
        return 'json'
    if content_type in {'text/csv', 'application/csv'}:
        return 'csv'
    if content_type == 'application/pdf':
        return 'pdf'

    path = urlparse(source_uri).path.lower()
    if path.endswith('.html') or path.endswith('.htm'):
        return 'html'
    if path.endswith('.json'):
        return 'json'
    if path.endswith('.csv'):
        return 'csv'
    if path.endswith('.pdf'):
        return 'pdf'
    return 'binary'


def _hash_bytes(content: bytes) -> str:
    digest = hashlib.sha256()
    digest.update(content)
    return digest.hexdigest()


def _build_artifact_id(
    *,
    source_id: str,
    source_uri: str,
    content_sha256: str,
) -> str:
    digest = hashlib.sha256()
    digest.update(source_id.encode('utf-8'))
    digest.update(b'|')
    digest.update(source_uri.encode('utf-8'))
    digest.update(b'|')
    digest.update(content_sha256.encode('utf-8'))
    return digest.hexdigest()


def fetch_http_source(
    *,
    source: SourceDescriptor,
    run_context: ScrapeRunContext,
    timeout_seconds: float = _DEFAULT_TIMEOUT_SECONDS,
    request_headers: dict[str, str] | None = None,
) -> RawArtifact:
    if timeout_seconds <= 0:
        raise ValueError(f'timeout_seconds must be > 0, got {timeout_seconds}')

    request = Request(
        source.source_uri,
        headers=request_headers if request_headers is not None else {},
        method='GET',
    )
    final_url = source.source_uri

    try:
        with urlopen(request, timeout=timeout_seconds) as response:
            status_code = response.status
            final_url = response.geturl()
            content_type = _normalize_content_type(response.headers.get('Content-Type'))
            content = response.read()
    except HTTPError as exc:
        detail = exc.read().decode('utf-8', errors='replace')
        raise RuntimeError(
            f'HTTP fetch failed with status {exc.code} for {source.source_uri}: {detail}'
        ) from exc
    except URLError as exc:
        raise RuntimeError(
            f'HTTP fetch connection failed for {source.source_uri}: {exc.reason}'
        ) from exc

    if status_code < 200 or status_code >= 300:
        raise RuntimeError(
            f'HTTP fetch returned non-success status {status_code} for {source.source_uri}'
        )
    if len(content) == 0:
        raise RuntimeError(f'HTTP fetch returned empty body for {source.source_uri}')

    content_sha256 = _hash_bytes(content)
    fetched_at_utc = datetime.now(UTC)
    artifact_format = _detect_artifact_format(
        source_uri=final_url if final_url.strip() != '' else source.source_uri,
        content_type=content_type,
    )
    artifact_id = _build_artifact_id(
        source_id=source.source_id,
        source_uri=source.source_uri,
        content_sha256=content_sha256,
    )

    return RawArtifact(
        artifact_id=artifact_id,
        source_id=source.source_id,
        source_uri=source.source_uri,
        fetched_at_utc=fetched_at_utc,
        fetch_method='http',
        artifact_format=artifact_format,
        content_sha256=content_sha256,
        content=content,
        metadata={
            'content_type': content_type,
            'final_url': final_url if final_url.strip() != '' else source.source_uri,
            'run_id': run_context.run_id,
            'source_name': source.source_name,
            'status_code': str(status_code),
        },
    )
