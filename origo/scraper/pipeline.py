from __future__ import annotations

from dataclasses import dataclass

from .audit import get_scraper_audit_log
from .clickhouse_staging import persist_normalized_records_to_clickhouse
from .contracts import (
    NormalizedMetricRecord,
    ParsedRecord,
    PersistedRawArtifact,
    RawArtifact,
    ScraperAdapter,
    ScrapeRunContext,
    SourceDescriptor,
)
from .errors import ScraperError, as_scraper_error
from .object_store import persist_raw_artifact
from .retry import RetryHookEvent, load_fetch_retry_policy_from_env, retry_with_backoff
from .rights import resolve_scraper_rights


@dataclass(frozen=True)
class PipelineSourceResult:
    source: SourceDescriptor
    artifact: RawArtifact
    persisted_artifact: PersistedRawArtifact
    parsed_records: list[ParsedRecord]
    normalized_records: list[NormalizedMetricRecord]
    inserted_row_count: int


@dataclass(frozen=True)
class PipelineRunResult:
    run_context: ScrapeRunContext
    source_results: list[PipelineSourceResult]

    @property
    def total_sources(self) -> int:
        return len(self.source_results)

    @property
    def total_parsed_records(self) -> int:
        return sum(len(item.parsed_records) for item in self.source_results)

    @property
    def total_normalized_records(self) -> int:
        return sum(len(item.normalized_records) for item in self.source_results)

    @property
    def total_inserted_rows(self) -> int:
        return sum(item.inserted_row_count for item in self.source_results)


def _audit_or_raise(
    *,
    event_type: str,
    run_id: str,
    source_id: str | None,
    payload: dict[str, str | int | float | bool | None],
) -> None:
    try:
        get_scraper_audit_log().append_event(
            event_type=event_type,
            run_id=run_id,
            source_id=source_id,
            payload=payload,
        )
    except Exception as exc:
        raise as_scraper_error(
            code='SCRAPER_AUDIT_WRITE_ERROR',
            message='Failed to write scraper audit event',
            details={
                'event_type': event_type,
                'run_id': run_id,
                'source_id': source_id if source_id is not None else '',
            },
            cause=exc,
        ) from exc


def _on_retry(event: RetryHookEvent, run_id: str, source_id: str) -> None:
    _audit_or_raise(
        event_type='scraper_retry',
        run_id=run_id,
        source_id=source_id,
        payload={
            'operation_name': event.operation_name,
            'attempt': event.attempt,
            'max_attempts': event.max_attempts,
            'next_backoff_seconds': event.next_backoff_seconds,
            'error_type': event.error_type,
            'error_message': event.error_message,
        },
    )


def _resolve_source_key(source: SourceDescriptor) -> str:
    rights_source = source.metadata.get('rights_source')
    if rights_source is not None and rights_source.strip() != '':
        return rights_source
    return source.source_id


def run_scraper_pipeline(
    *,
    adapter: ScraperAdapter,
    run_context: ScrapeRunContext,
) -> PipelineRunResult:
    _audit_or_raise(
        event_type='scraper_run_started',
        run_id=run_context.run_id,
        source_id=None,
        payload={'adapter_name': adapter.adapter_name},
    )

    try:
        sources = list(adapter.discover_sources(run_context=run_context))
    except Exception as exc:
        _audit_or_raise(
            event_type='scraper_run_failed',
            run_id=run_context.run_id,
            source_id=None,
            payload={
                'error_code': 'SCRAPER_DISCOVER_ERROR',
                'error_message': str(exc),
            },
        )
        raise as_scraper_error(
            code='SCRAPER_DISCOVER_ERROR',
            message='Adapter discover_sources failed',
            details={'adapter_name': adapter.adapter_name},
            cause=exc,
        ) from exc

    if len(sources) == 0:
        raise as_scraper_error(
            code='SCRAPER_DISCOVER_ERROR',
            message='discover_sources returned no sources',
            details={'adapter_name': adapter.adapter_name},
        )

    fetch_policy = load_fetch_retry_policy_from_env()
    source_results: list[PipelineSourceResult] = []

    try:
        for source in sources:
            _audit_or_raise(
                event_type='scraper_source_discovered',
                run_id=run_context.run_id,
                source_id=source.source_id,
                payload={
                    'source_uri': source.source_uri,
                    'source_name': source.source_name,
                },
            )

            source_key = _resolve_source_key(source)
            rights_decision = resolve_scraper_rights(
                source_key=source_key,
                source_id=source.source_id,
            )
            _audit_or_raise(
                event_type='scraper_rights_resolved',
                run_id=run_context.run_id,
                source_id=source.source_id,
                payload={
                    'source_key': rights_decision.source_key,
                    'source_id': rights_decision.source_id,
                    'rights_state': rights_decision.rights_state,
                },
            )

            try:
                artifact = retry_with_backoff(
                    operation_name='fetch',
                    operation=lambda: adapter.fetch(
                        source=source, run_context=run_context
                    ),
                    policy=fetch_policy,
                    on_retry=lambda event: _on_retry(
                        event, run_context.run_id, source.source_id
                    ),
                )
            except ScraperError:
                raise
            except Exception as exc:
                raise as_scraper_error(
                    code='SCRAPER_FETCH_ERROR',
                    message=f'Adapter fetch failed for source_id={source.source_id}',
                    details={'source_id': source.source_id},
                    cause=exc,
                ) from exc

            _audit_or_raise(
                event_type='scraper_fetch_succeeded',
                run_id=run_context.run_id,
                source_id=source.source_id,
                payload={
                    'artifact_id': artifact.artifact_id,
                    'artifact_format': artifact.artifact_format,
                    'content_sha256': artifact.content_sha256,
                },
            )

            try:
                persisted = persist_raw_artifact(
                    artifact=artifact,
                    run_context=run_context,
                )
            except Exception as exc:
                raise as_scraper_error(
                    code='SCRAPER_OBJECT_STORE_ERROR',
                    message=(
                        'Raw artifact persistence failed '
                        f'for source_id={source.source_id}'
                    ),
                    details={'source_id': source.source_id},
                    cause=exc,
                ) from exc

            try:
                parsed_records = list(
                    adapter.parse(
                        artifact=artifact,
                        source=source,
                        run_context=run_context,
                    )
                )
            except Exception as exc:
                raise as_scraper_error(
                    code='SCRAPER_PARSE_ERROR',
                    message=f'Adapter parse failed for source_id={source.source_id}',
                    details={'source_id': source.source_id},
                    cause=exc,
                ) from exc

            if len(parsed_records) == 0:
                raise as_scraper_error(
                    code='SCRAPER_PARSE_ERROR',
                    message=(
                        'Adapter parse returned no records '
                        f'for source_id={source.source_id}'
                    ),
                    details={'source_id': source.source_id},
                )

            try:
                normalized_records = list(
                    adapter.normalize(
                        parsed_records=parsed_records,
                        source=source,
                        run_context=run_context,
                    )
                )
            except Exception as exc:
                raise as_scraper_error(
                    code='SCRAPER_NORMALIZE_ERROR',
                    message=(
                        f'Adapter normalize failed for source_id={source.source_id}'
                    ),
                    details={'source_id': source.source_id},
                    cause=exc,
                ) from exc

            if len(normalized_records) == 0:
                raise as_scraper_error(
                    code='SCRAPER_NORMALIZE_ERROR',
                    message=(
                        'Adapter normalize returned no records '
                        f'for source_id={source.source_id}'
                    ),
                    details={'source_id': source.source_id},
                )

            try:
                inserted_row_count = persist_normalized_records_to_clickhouse(
                    records=normalized_records,
                    run_id=run_context.run_id,
                )
            except Exception as exc:
                raise as_scraper_error(
                    code='SCRAPER_STAGING_PERSIST_ERROR',
                    message=(
                        'ClickHouse staging persistence failed '
                        f'for source_id={source.source_id}'
                    ),
                    details={'source_id': source.source_id},
                    cause=exc,
                ) from exc

            _audit_or_raise(
                event_type='scraper_source_completed',
                run_id=run_context.run_id,
                source_id=source.source_id,
                payload={
                    'artifact_id': artifact.artifact_id,
                    'parsed_records': len(parsed_records),
                    'normalized_records': len(normalized_records),
                    'inserted_rows': inserted_row_count,
                },
            )

            source_results.append(
                PipelineSourceResult(
                    source=source,
                    artifact=artifact,
                    persisted_artifact=persisted,
                    parsed_records=parsed_records,
                    normalized_records=normalized_records,
                    inserted_row_count=inserted_row_count,
                )
            )
    except ScraperError as exc:
        _audit_or_raise(
            event_type='scraper_run_failed',
            run_id=run_context.run_id,
            source_id=None,
            payload={
                'error_code': exc.code,
                'error_message': exc.message,
            },
        )
        raise
    except Exception as exc:
        wrapped = as_scraper_error(
            code='SCRAPER_RUN_ERROR',
            message='Unhandled scraper pipeline error',
            cause=exc,
        )
        _audit_or_raise(
            event_type='scraper_run_failed',
            run_id=run_context.run_id,
            source_id=None,
            payload={
                'error_code': wrapped.code,
                'error_message': wrapped.message,
            },
        )
        raise wrapped from exc

    result = PipelineRunResult(
        run_context=run_context,
        source_results=source_results,
    )
    _audit_or_raise(
        event_type='scraper_run_completed',
        run_id=run_context.run_id,
        source_id=None,
        payload={
            'total_sources': result.total_sources,
            'total_parsed_records': result.total_parsed_records,
            'total_normalized_records': result.total_normalized_records,
            'total_inserted_rows': result.total_inserted_rows,
        },
    )
    return result
