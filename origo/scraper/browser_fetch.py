from __future__ import annotations

import hashlib
import importlib
from datetime import UTC, datetime
from types import ModuleType
from typing import Any, Final, Literal

from .contracts import RawArtifact, ScrapeRunContext, SourceDescriptor

_DEFAULT_TIMEOUT_MS: Final[int] = 30_000
BrowserEngine = Literal['chromium', 'firefox', 'webkit']
_ALLOWED_BROWSER_ENGINES: Final[frozenset[BrowserEngine]] = frozenset(
    {'chromium', 'firefox', 'webkit'}
)


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


def _load_playwright_sync_api() -> ModuleType:
    try:
        return importlib.import_module('playwright.sync_api')
    except ModuleNotFoundError as exc:
        raise RuntimeError(
            'Browser fetch requires playwright. Install with: '
            '`uv add playwright` and run `playwright install chromium`.'
        ) from exc


def fetch_browser_source(
    *,
    source: SourceDescriptor,
    run_context: ScrapeRunContext,
    wait_selector: str | None = None,
    timeout_ms: int = _DEFAULT_TIMEOUT_MS,
    browser_engine: BrowserEngine = 'chromium',
) -> RawArtifact:
    if timeout_ms <= 0:
        raise ValueError(f'timeout_ms must be > 0, got {timeout_ms}')
    if wait_selector is not None and wait_selector.strip() == '':
        raise ValueError('wait_selector must be non-empty when set')
    if browser_engine not in _ALLOWED_BROWSER_ENGINES:
        raise ValueError(
            f'browser_engine must be one of {sorted(_ALLOWED_BROWSER_ENGINES)}, '
            f'got {browser_engine}'
        )

    playwright_module = _load_playwright_sync_api()
    sync_playwright = getattr(playwright_module, 'sync_playwright', None)
    if sync_playwright is None:
        raise RuntimeError('playwright.sync_api.sync_playwright was not found')

    final_url = source.source_uri
    status_code: int | None = None
    html_content: str | None = None

    with sync_playwright() as playwright:
        engine: Any = getattr(playwright, browser_engine, None)
        if engine is None:
            raise RuntimeError(f'playwright engine "{browser_engine}" was not found')
        browser = engine.launch(headless=True)
        try:
            page = browser.new_page()
            response = page.goto(
                source.source_uri,
                timeout=timeout_ms,
                wait_until='networkidle',
            )
            if wait_selector is not None:
                page.wait_for_selector(wait_selector, timeout=timeout_ms)
            final_url_value = page.url
            if not isinstance(final_url_value, str) or final_url_value.strip() == '':
                raise RuntimeError(
                    f'Browser fetch did not produce a valid URL for {source.source_uri}'
                )
            final_url = final_url_value
            html_content = page.content()
            if response is not None:
                status_value = response.status
                if isinstance(status_value, int):
                    status_code = status_value
        finally:
            browser.close()

    if html_content is None or html_content.strip() == '':
        raise RuntimeError(
            f'Browser fetch returned empty content for {source.source_uri}'
        )

    content = html_content.encode('utf-8')
    content_sha256 = _hash_bytes(content)
    artifact_id = _build_artifact_id(
        source_id=source.source_id,
        source_uri=source.source_uri,
        content_sha256=content_sha256,
    )

    return RawArtifact(
        artifact_id=artifact_id,
        source_id=source.source_id,
        source_uri=source.source_uri,
        fetched_at_utc=datetime.now(UTC),
        fetch_method='browser',
        artifact_format='html',
        content_sha256=content_sha256,
        content=content,
        metadata={
            'browser_engine': browser_engine,
            'final_url': final_url,
            'run_id': run_context.run_id,
            'source_name': source.source_name,
            'status_code': (str(status_code) if status_code is not None else 'unknown'),
        },
    )
