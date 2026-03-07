from __future__ import annotations

import argparse
import hashlib
import json
import math
from dataclasses import dataclass
from datetime import UTC, date, datetime
from numbers import Real
from pathlib import Path
from typing import Any, cast

from clickhouse_connect import get_client

from origo.query.native_core import resolve_clickhouse_http_settings

from .clickhouse_staging import persist_normalized_records_to_clickhouse
from .contracts import (
    MetricValue,
    NormalizedMetricRecord,
    RawArtifact,
    ScrapeRunContext,
    SourceDescriptor,
)
from .etf_adapters import build_s4_03_issuer_adapters

_ETF_DAILY_TABLE = 'etf_daily_metrics_long'
_MANDATORY_METRICS = (
    'issuer',
    'ticker',
    'as_of_date',
    'btc_units',
    'btc_market_value_usd',
    'total_net_assets_usd',
    'holdings_row_count',
)
_PARITY_THRESHOLD_PCT = 99.5
_FLOAT_ABS_TOLERANCE = 1e-9
_PROVENANCE_REQUIRED_KEYS = (
    'source_id',
    'source_uri',
    'artifact_id',
    'artifact_sha256',
    'fetch_method',
    'parser_name',
    'parser_version',
    'fetched_at_utc',
    'parsed_at_utc',
    'normalized_at_utc',
)


@dataclass(frozen=True)
class _SourceRunResult:
    adapter_name: str
    source: SourceDescriptor
    artifact: RawArtifact
    as_of_date: str
    normalized_records: list[NormalizedMetricRecord]
    inserted_row_count: int
    expected_mandatory_metrics: dict[str, MetricValue]


def _utc_datetime_literal(value: datetime) -> str:
    utc_value = value.astimezone(UTC)
    return utc_value.strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]


def _sql_quote(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def _coerce_metric_value(
    *,
    metric_value_string: str | None,
    metric_value_int: int | None,
    metric_value_float: float | None,
    metric_value_bool: int | None,
) -> MetricValue:
    if metric_value_bool is not None:
        return bool(metric_value_bool)
    if metric_value_int is not None:
        return int(metric_value_int)
    if metric_value_float is not None:
        return float(metric_value_float)
    if metric_value_string is not None:
        return metric_value_string
    return None


def _expect_string_key_dict(*, value: Any, label: str) -> dict[str, Any]:
    if not isinstance(value, dict):
        raise RuntimeError(f'{label} must be a JSON object')
    raw_map = cast(dict[Any, Any], value)
    normalized: dict[str, Any] = {}
    for raw_key, raw_value in raw_map.items():
        if not isinstance(raw_key, str):
            raise RuntimeError(f'{label} keys must be strings')
        normalized[raw_key] = raw_value
    return normalized


def _require_non_empty_str_field(
    *,
    payload: dict[str, Any],
    key: str,
    label: str,
) -> str:
    raw = payload.get(key)
    if not isinstance(raw, str) or raw.strip() == '':
        raise RuntimeError(f'{label} must be a non-empty string')
    return raw


def _metric_values_match(expected: MetricValue, actual: MetricValue) -> bool:
    if expected is None or actual is None:
        return expected is actual

    if isinstance(expected, bool) or isinstance(actual, bool):
        return expected == actual

    if isinstance(expected, Real) and isinstance(actual, Real):
        return math.isclose(
            float(expected),
            float(actual),
            rel_tol=0.0,
            abs_tol=_FLOAT_ABS_TOLERANCE,
        )

    return str(expected) == str(actual)


def _extract_as_of_date(records: list[NormalizedMetricRecord], *, adapter_name: str) -> str:
    as_of_values = sorted(
        {
            str(record.metric_value)
            for record in records
            if record.metric_name == 'as_of_date'
        }
    )
    if len(as_of_values) != 1:
        raise RuntimeError(
            f'{adapter_name} must produce exactly one as_of_date metric, got {len(as_of_values)}'
        )
    return as_of_values[0]


def _build_metric_map(
    *,
    records: list[NormalizedMetricRecord],
    metric_names: tuple[str, ...],
    context_label: str,
) -> dict[str, MetricValue]:
    allowed = set(metric_names)
    values: dict[str, MetricValue] = {}
    for record in records:
        if record.metric_name not in allowed:
            continue
        if record.metric_name in values:
            raise RuntimeError(
                f'{context_label} contains duplicate metric_name={record.metric_name}'
            )
        values[record.metric_name] = record.metric_value

    missing = sorted(allowed.difference(values.keys()))
    if len(missing) > 0:
        raise RuntimeError(f'{context_label} missing required metrics: {missing}')
    return values


def _normalized_fingerprint(records: list[NormalizedMetricRecord]) -> str:
    canonical_rows: list[dict[str, Any]] = []
    for record in records:
        canonical_rows.append(
            {
                'source_id': record.source_id,
                'metric_name': record.metric_name,
                'metric_value': record.metric_value,
                'metric_unit': record.metric_unit,
                'observed_at_utc': record.observed_at_utc.isoformat(),
                'dimensions': record.dimensions,
            }
        )

    canonical_rows.sort(
        key=lambda item: (
            str(item['source_id']),
            str(item['metric_name']),
            str(item['observed_at_utc']),
            json.dumps(item['dimensions'], sort_keys=True, separators=(',', ':')),
            json.dumps(item['metric_value'], sort_keys=True, separators=(',', ':')),
        )
    )

    serialized = json.dumps(
        canonical_rows,
        ensure_ascii=True,
        sort_keys=True,
        separators=(',', ':'),
    )
    return hashlib.sha256(serialized.encode('utf-8')).hexdigest()


def _fetch_and_ingest_source_rows(*, run_context: ScrapeRunContext) -> list[_SourceRunResult]:
    source_results: list[_SourceRunResult] = []
    for adapter in build_s4_03_issuer_adapters():
        sources = list(adapter.discover_sources(run_context=run_context))
        if len(sources) != 1:
            raise RuntimeError(
                f'{adapter.adapter_name} expected exactly one source, got {len(sources)}'
            )

        source = sources[0]
        artifact = adapter.fetch(source=source, run_context=run_context)
        parsed_records = list(
            adapter.parse(
                artifact=artifact,
                source=source,
                run_context=run_context,
            )
        )
        normalized_records = list(
            adapter.normalize(
                parsed_records=parsed_records,
                source=source,
                run_context=run_context,
            )
        )
        as_of_date = _extract_as_of_date(
            normalized_records,
            adapter_name=adapter.adapter_name,
        )
        expected_metrics = _build_metric_map(
            records=normalized_records,
            metric_names=_MANDATORY_METRICS,
            context_label=f'{adapter.adapter_name} normalized records',
        )
        inserted = persist_normalized_records_to_clickhouse(records=normalized_records)
        source_results.append(
            _SourceRunResult(
                adapter_name=adapter.adapter_name,
                source=source,
                artifact=artifact,
                as_of_date=as_of_date,
                normalized_records=normalized_records,
                inserted_row_count=inserted,
                expected_mandatory_metrics=expected_metrics,
            )
        )
    return source_results


def _load_mandatory_metrics_from_clickhouse(
    *,
    source_id: str,
    as_of_date: str,
    ingest_start_utc: datetime,
    ingest_end_utc: datetime,
) -> dict[str, MetricValue]:
    settings = resolve_clickhouse_http_settings()
    client = get_client(
        host=settings.host,
        port=settings.port,
        username=settings.username,
        password=settings.password,
        database=settings.database,
        compression=True,
    )
    try:
        start_literal = _utc_datetime_literal(ingest_start_utc)
        end_literal = _utc_datetime_literal(ingest_end_utc)
        required = ','.join(_sql_quote(name) for name in sorted(_MANDATORY_METRICS))
        sql = (
            f'SELECT metric_name, metric_value_string, metric_value_int, '
            f'metric_value_float, metric_value_bool '
            f'FROM {settings.database}.{_ETF_DAILY_TABLE} '
            f'WHERE source_id = {_sql_quote(source_id)} '
            f"AND toDate(observed_at_utc) = toDate('{as_of_date}') "
            f"AND ingested_at_utc >= toDateTime64('{start_literal}', 3, 'UTC') "
            f"AND ingested_at_utc <= toDateTime64('{end_literal}', 3, 'UTC') "
            f'AND metric_name IN ({required}) '
            f'ORDER BY metric_name'
        )
        rows = client.query(sql).result_rows
    finally:
        client.close()

    values_by_metric: dict[str, set[str]] = {}
    parsed_values: dict[str, MetricValue] = {}

    for metric_name_raw, value_string, value_int, value_float, value_bool in rows:
        metric_name = str(metric_name_raw)
        parsed = _coerce_metric_value(
            metric_value_string=(str(value_string) if value_string is not None else None),
            metric_value_int=(int(value_int) if value_int is not None else None),
            metric_value_float=(float(value_float) if value_float is not None else None),
            metric_value_bool=(int(value_bool) if value_bool is not None else None),
        )
        canonical = json.dumps(parsed, sort_keys=True, separators=(',', ':'))
        values_by_metric.setdefault(metric_name, set()).add(canonical)
        parsed_values[metric_name] = parsed

    ambiguous = sorted(
        metric_name
        for metric_name, canonical_values in values_by_metric.items()
        if len(canonical_values) > 1
    )
    if len(ambiguous) > 0:
        raise RuntimeError(
            f'Ambiguous canonical values detected for source_id={source_id} '
            f'as_of_date={as_of_date}: {ambiguous}'
        )

    return parsed_values


def _run_parity_proof(
    *,
    source_results: list[_SourceRunResult],
    ingest_start_utc: datetime,
    ingest_end_utc: datetime,
) -> dict[str, Any]:
    total_metrics = 0
    total_matches = 0
    per_source: list[dict[str, Any]] = []

    for source_result in source_results:
        actual_metrics = _load_mandatory_metrics_from_clickhouse(
            source_id=source_result.source.source_id,
            as_of_date=source_result.as_of_date,
            ingest_start_utc=ingest_start_utc,
            ingest_end_utc=ingest_end_utc,
        )

        matched = 0
        mismatches: list[dict[str, Any]] = []
        for metric_name in _MANDATORY_METRICS:
            total_metrics += 1
            expected_value = source_result.expected_mandatory_metrics.get(metric_name)
            actual_value = actual_metrics.get(metric_name)
            if _metric_values_match(expected_value, actual_value):
                matched += 1
                total_matches += 1
            else:
                mismatches.append(
                    {
                        'metric_name': metric_name,
                        'expected': expected_value,
                        'actual': actual_value,
                    }
                )

        source_parity_pct = (
            (matched / len(_MANDATORY_METRICS)) * 100.0
            if len(_MANDATORY_METRICS) > 0
            else 100.0
        )

        per_source.append(
            {
                'adapter_name': source_result.adapter_name,
                'source_id': source_result.source.source_id,
                'as_of_date': source_result.as_of_date,
                'artifact_id': source_result.artifact.artifact_id,
                'artifact_sha256': source_result.artifact.content_sha256,
                'mandatory_total': len(_MANDATORY_METRICS),
                'mandatory_matched': matched,
                'parity_pct': source_parity_pct,
                'mismatches': mismatches,
            }
        )

    parity_pct = (total_matches / total_metrics) * 100.0 if total_metrics > 0 else 100.0
    threshold_passed = parity_pct >= _PARITY_THRESHOLD_PCT
    if not threshold_passed:
        raise RuntimeError(
            f'S4-P2 parity threshold failed: parity_pct={parity_pct:.6f}, '
            f'required>={_PARITY_THRESHOLD_PCT:.1f}'
        )

    return {
        'threshold_pct': _PARITY_THRESHOLD_PCT,
        'mandatory_metrics_total': total_metrics,
        'mandatory_metrics_matched': total_matches,
        'parity_pct': parity_pct,
        'threshold_passed': threshold_passed,
        'per_source': per_source,
    }


def _load_inserted_metric_rows_fingerprint(
    *,
    source_ids: list[str],
    ingest_start_utc: datetime,
    ingest_end_utc: datetime,
) -> dict[str, Any]:
    if len(source_ids) == 0:
        return {
            'row_count': 0,
            'fingerprint_sha256': hashlib.sha256(b'[]').hexdigest(),
            'rows_by_source': [],
        }

    settings = resolve_clickhouse_http_settings()
    client = get_client(
        host=settings.host,
        port=settings.port,
        username=settings.username,
        password=settings.password,
        database=settings.database,
        compression=True,
    )
    try:
        start_literal = _utc_datetime_literal(ingest_start_utc)
        end_literal = _utc_datetime_literal(ingest_end_utc)
        quoted_sources = ','.join(_sql_quote(source_id) for source_id in sorted(source_ids))
        sql = (
            f'SELECT source_id, observed_at_utc, metric_name, metric_value_string, '
            f'metric_value_int, metric_value_float, metric_value_bool '
            f'FROM {settings.database}.{_ETF_DAILY_TABLE} '
            f'WHERE source_id IN ({quoted_sources}) '
            f"AND ingested_at_utc >= toDateTime64('{start_literal}', 3, 'UTC') "
            f"AND ingested_at_utc <= toDateTime64('{end_literal}', 3, 'UTC') "
            f'ORDER BY source_id, observed_at_utc, metric_name'
        )
        rows = client.query(sql).result_rows
    finally:
        client.close()

    canonical_rows: list[dict[str, Any]] = []
    rows_by_source: dict[str, int] = {}
    for (
        source_id_raw,
        observed_at_raw,
        metric_name_raw,
        value_string,
        value_int,
        value_float,
        value_bool,
    ) in rows:
        source_id = str(source_id_raw)
        rows_by_source[source_id] = rows_by_source.get(source_id, 0) + 1
        canonical_rows.append(
            {
                'source_id': source_id,
                'observed_at_utc': str(observed_at_raw),
                'metric_name': str(metric_name_raw),
                'metric_value': _coerce_metric_value(
                    metric_value_string=(
                        str(value_string) if value_string is not None else None
                    ),
                    metric_value_int=(int(value_int) if value_int is not None else None),
                    metric_value_float=(
                        float(value_float) if value_float is not None else None
                    ),
                    metric_value_bool=(int(value_bool) if value_bool is not None else None),
                ),
            }
        )

    serialized = json.dumps(
        canonical_rows,
        ensure_ascii=True,
        sort_keys=True,
        separators=(',', ':'),
    )
    return {
        'row_count': len(canonical_rows),
        'fingerprint_sha256': hashlib.sha256(serialized.encode('utf-8')).hexdigest(),
        'rows_by_source': [
            {
                'source_id': source_id,
                'row_count': row_count,
            }
            for source_id, row_count in sorted(rows_by_source.items())
        ],
    }


def _run_replay_determinism_proof(
    *,
    source_results: list[_SourceRunResult],
    replay_context: ScrapeRunContext,
    run1_ingest_start_utc: datetime,
    run1_ingest_end_utc: datetime,
) -> dict[str, Any]:
    run2_ingest_start_utc = datetime.now(UTC)
    run2_per_source: list[dict[str, Any]] = []
    run2_source_ids: list[str] = []

    for source_result in source_results:
        adapter = next(
            candidate
            for candidate in build_s4_03_issuer_adapters()
            if candidate.adapter_name == source_result.adapter_name
        )
        replay_parsed = list(
            adapter.parse(
                artifact=source_result.artifact,
                source=source_result.source,
                run_context=replay_context,
            )
        )
        replay_normalized = list(
            adapter.normalize(
                parsed_records=replay_parsed,
                source=source_result.source,
                run_context=replay_context,
            )
        )
        replay_inserted = persist_normalized_records_to_clickhouse(records=replay_normalized)

        run1_fingerprint = _normalized_fingerprint(source_result.normalized_records)
        run2_fingerprint = _normalized_fingerprint(replay_normalized)

        run2_per_source.append(
            {
                'adapter_name': source_result.adapter_name,
                'source_id': source_result.source.source_id,
                'run1_fingerprint_sha256': run1_fingerprint,
                'run2_fingerprint_sha256': run2_fingerprint,
                'deterministic_match': run1_fingerprint == run2_fingerprint,
                'run1_record_count': len(source_result.normalized_records),
                'run2_record_count': len(replay_normalized),
                'run2_inserted_row_count': replay_inserted,
            }
        )
        run2_source_ids.append(source_result.source.source_id)

    run2_ingest_end_utc = datetime.now(UTC)
    run1_db = _load_inserted_metric_rows_fingerprint(
        source_ids=run2_source_ids,
        ingest_start_utc=run1_ingest_start_utc,
        ingest_end_utc=run1_ingest_end_utc,
    )
    run2_db = _load_inserted_metric_rows_fingerprint(
        source_ids=run2_source_ids,
        ingest_start_utc=run2_ingest_start_utc,
        ingest_end_utc=run2_ingest_end_utc,
    )

    deterministic_match = run1_db['fingerprint_sha256'] == run2_db['fingerprint_sha256']
    if not deterministic_match:
        raise RuntimeError(
            'S4-P3 replay determinism failed: run1 and run2 inserted row fingerprints differ'
        )

    return {
        'run2_ingest_start_utc': run2_ingest_start_utc.isoformat(),
        'run2_ingest_end_utc': run2_ingest_end_utc.isoformat(),
        'per_source': run2_per_source,
        'run1_inserted': run1_db,
        'run2_inserted': run2_db,
        'deterministic_match': deterministic_match,
    }


def _run_provenance_reference_proof(
    *,
    source_results: list[_SourceRunResult],
    run1_ingest_start_utc: datetime,
    run1_ingest_end_utc: datetime,
) -> dict[str, Any]:
    source_ids = sorted({result.source.source_id for result in source_results})
    if len(source_ids) == 0:
        return {
            'rows_checked': 0,
            'missing_provenance_rows': 0,
            'invalid_provenance_rows': 0,
            'artifact_reference_mismatch_rows': 0,
            'all_references_valid': True,
            'invalid_row_samples': [],
        }

    expected_by_source = {
        result.source.source_id: (
            result.artifact.artifact_id,
            result.artifact.content_sha256,
        )
        for result in source_results
    }

    settings = resolve_clickhouse_http_settings()
    client = get_client(
        host=settings.host,
        port=settings.port,
        username=settings.username,
        password=settings.password,
        database=settings.database,
        compression=True,
    )
    try:
        start_literal = _utc_datetime_literal(run1_ingest_start_utc)
        end_literal = _utc_datetime_literal(run1_ingest_end_utc)
        quoted_sources = ','.join(_sql_quote(source_id) for source_id in source_ids)
        sql = (
            f'SELECT source_id, metric_name, provenance_json '
            f'FROM {settings.database}.{_ETF_DAILY_TABLE} '
            f'WHERE source_id IN ({quoted_sources}) '
            f"AND ingested_at_utc >= toDateTime64('{start_literal}', 3, 'UTC') "
            f"AND ingested_at_utc <= toDateTime64('{end_literal}', 3, 'UTC') "
            f'ORDER BY source_id, metric_name'
        )
        rows = client.query(sql).result_rows
    finally:
        client.close()

    rows_checked = 0
    missing_provenance_rows = 0
    invalid_provenance_rows = 0
    artifact_reference_mismatch_rows = 0
    invalid_row_samples: list[dict[str, str]] = []

    for source_id_raw, metric_name_raw, provenance_json_raw in rows:
        rows_checked += 1
        source_id = str(source_id_raw)
        metric_name = str(metric_name_raw)
        provenance_raw = (
            str(provenance_json_raw)
            if provenance_json_raw is not None
            else ''
        )

        if provenance_raw.strip() == '':
            missing_provenance_rows += 1
            if len(invalid_row_samples) < 20:
                invalid_row_samples.append(
                    {
                        'source_id': source_id,
                        'metric_name': metric_name,
                        'reason': 'missing_provenance_json',
                    }
                )
            continue

        try:
            provenance_payload_raw = json.loads(provenance_raw)
        except json.JSONDecodeError:
            invalid_provenance_rows += 1
            if len(invalid_row_samples) < 20:
                invalid_row_samples.append(
                    {
                        'source_id': source_id,
                        'metric_name': metric_name,
                        'reason': 'invalid_provenance_json',
                    }
                )
            continue

        try:
            provenance_payload = _expect_string_key_dict(
                value=provenance_payload_raw,
                label='provenance_json',
            )
        except RuntimeError:
            invalid_provenance_rows += 1
            if len(invalid_row_samples) < 20:
                invalid_row_samples.append(
                    {
                        'source_id': source_id,
                        'metric_name': metric_name,
                        'reason': 'provenance_json_not_object',
                    }
                )
            continue

        missing_keys: list[str] = []
        for key in _PROVENANCE_REQUIRED_KEYS:
            value = provenance_payload.get(key)
            if not isinstance(value, str) or value.strip() == '':
                missing_keys.append(key)
        if len(missing_keys) > 0:
            invalid_provenance_rows += 1
            if len(invalid_row_samples) < 20:
                invalid_row_samples.append(
                    {
                        'source_id': source_id,
                        'metric_name': metric_name,
                        'reason': f'missing_required_keys={missing_keys}',
                    }
                )
            continue

        provenance_source_id = _require_non_empty_str_field(
            payload=provenance_payload,
            key='source_id',
            label='provenance_json.source_id',
        )
        if provenance_source_id != source_id:
            invalid_provenance_rows += 1
            if len(invalid_row_samples) < 20:
                invalid_row_samples.append(
                    {
                        'source_id': source_id,
                        'metric_name': metric_name,
                        'reason': (
                            f'provenance_source_id_mismatch={provenance_source_id}'
                        ),
                    }
                )
            continue

        expected_artifact_id, expected_artifact_sha = expected_by_source[source_id]
        artifact_id = _require_non_empty_str_field(
            payload=provenance_payload,
            key='artifact_id',
            label='provenance_json.artifact_id',
        )
        artifact_sha256 = _require_non_empty_str_field(
            payload=provenance_payload,
            key='artifact_sha256',
            label='provenance_json.artifact_sha256',
        )
        if (
            artifact_id != expected_artifact_id
            or artifact_sha256 != expected_artifact_sha
        ):
            artifact_reference_mismatch_rows += 1
            if len(invalid_row_samples) < 20:
                invalid_row_samples.append(
                    {
                        'source_id': source_id,
                        'metric_name': metric_name,
                        'reason': 'artifact_reference_mismatch',
                    }
                )

    all_valid = (
        missing_provenance_rows == 0
        and invalid_provenance_rows == 0
        and artifact_reference_mismatch_rows == 0
    )
    if not all_valid:
        raise RuntimeError(
            'S4-P4 provenance reference verification failed '
            f'(missing={missing_provenance_rows}, invalid={invalid_provenance_rows}, '
            f'artifact_mismatch={artifact_reference_mismatch_rows})'
        )

    return {
        'rows_checked': rows_checked,
        'missing_provenance_rows': missing_provenance_rows,
        'invalid_provenance_rows': invalid_provenance_rows,
        'artifact_reference_mismatch_rows': artifact_reference_mismatch_rows,
        'all_references_valid': all_valid,
        'invalid_row_samples': invalid_row_samples,
    }


def run_s4_p1_p4_proof_suite() -> dict[str, Any]:
    run_started_at_utc = datetime.now(UTC)
    run_context = ScrapeRunContext(
        run_id=f's4-proof-{run_started_at_utc.strftime("%Y%m%dT%H%M%SZ")}',
        started_at_utc=run_started_at_utc,
    )

    run1_ingest_start_utc = datetime.now(UTC)
    run1_source_results = _fetch_and_ingest_source_rows(run_context=run_context)
    run1_ingest_end_utc = datetime.now(UTC)

    parity = _run_parity_proof(
        source_results=run1_source_results,
        ingest_start_utc=run1_ingest_start_utc,
        ingest_end_utc=run1_ingest_end_utc,
    )

    replay_context = ScrapeRunContext(
        run_id=f'{run_context.run_id}-replay',
        started_at_utc=datetime.now(UTC),
    )
    replay = _run_replay_determinism_proof(
        source_results=run1_source_results,
        replay_context=replay_context,
        run1_ingest_start_utc=run1_ingest_start_utc,
        run1_ingest_end_utc=run1_ingest_end_utc,
    )

    provenance = _run_provenance_reference_proof(
        source_results=run1_source_results,
        run1_ingest_start_utc=run1_ingest_start_utc,
        run1_ingest_end_utc=run1_ingest_end_utc,
    )

    proof_window_days = sorted(
        {
            source_result.as_of_date
            for source_result in run1_source_results
        }
    )
    proof_window_start = min(date.fromisoformat(day) for day in proof_window_days)
    proof_window_end = max(date.fromisoformat(day) for day in proof_window_days)

    return {
        'generated_at_utc': datetime.now(UTC).isoformat(),
        'proof_scope': 'Slice 4 S4-P1..S4-P4 ETF proof suite',
        'run_id': run_context.run_id,
        'proof_window': {
            'as_of_dates': proof_window_days,
            'start_date': proof_window_start.isoformat(),
            'end_date': proof_window_end.isoformat(),
        },
        'run_windows': {
            'run1_ingest_start_utc': run1_ingest_start_utc.isoformat(),
            'run1_ingest_end_utc': run1_ingest_end_utc.isoformat(),
            'run2_ingest_start_utc': replay['run2_ingest_start_utc'],
            'run2_ingest_end_utc': replay['run2_ingest_end_utc'],
        },
        'source_results': [
            {
                'adapter_name': result.adapter_name,
                'source_id': result.source.source_id,
                'source_uri': result.source.source_uri,
                'as_of_date': result.as_of_date,
                'artifact_id': result.artifact.artifact_id,
                'artifact_format': result.artifact.artifact_format,
                'artifact_sha256': result.artifact.content_sha256,
                'normalized_record_count': len(result.normalized_records),
                'inserted_row_count': result.inserted_row_count,
                'normalized_fingerprint_sha256': _normalized_fingerprint(
                    result.normalized_records
                ),
            }
            for result in run1_source_results
        ],
        'p1_p2_parity': parity,
        'p3_replay': replay,
        'p4_provenance': provenance,
    }


def write_s4_p1_p4_proof_suite(*, output_path: Path) -> dict[str, Any]:
    payload = run_s4_p1_p4_proof_suite()
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(
        json.dumps(payload, indent=2, sort_keys=True) + '\n',
        encoding='utf-8',
    )
    return payload


def main() -> None:
    parser = argparse.ArgumentParser(
        description='Run Slice 4 proof suite for ETF parity, replay determinism, and provenance checks.',
    )
    parser.add_argument(
        '--output',
        type=Path,
        default=Path('spec/slices/slice-4-etf-use-case/proof-s4-p1-p4.json'),
        help='Path to write proof-suite JSON artifact.',
    )
    args = parser.parse_args()
    payload = write_s4_p1_p4_proof_suite(output_path=args.output)
    print(json.dumps(payload, indent=2, sort_keys=True))


if __name__ == '__main__':
    main()
