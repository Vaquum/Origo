from __future__ import annotations

import json
from datetime import UTC, datetime

from origo.events import CanonicalEventWriteInput, build_canonical_event_row
from origo.scraper.contracts import NormalizedMetricRecord, ProvenanceMetadata
from origo.scraper.etf_canonical_event_ingest import build_etf_canonical_payload


def _sample_provenance() -> ProvenanceMetadata:
    return ProvenanceMetadata(
        source_id='etf_ishares_ibit_daily',
        source_uri='https://example.com/ibit.json',
        artifact_id='artifact-1',
        artifact_sha256='abc123',
        fetch_method='http',
        parser_name='json',
        parser_version='1.0.0',
        fetched_at_utc=datetime(2026, 3, 10, 12, 0, 0, tzinfo=UTC),
        parsed_at_utc=datetime(2026, 3, 10, 12, 0, 1, tzinfo=UTC),
        normalized_at_utc=datetime(2026, 3, 10, 12, 0, 2, tzinfo=UTC),
    )


def test_build_etf_canonical_payload_encodes_float_as_text() -> None:
    record = NormalizedMetricRecord(
        metric_id='metric-1',
        source_id='etf_ishares_ibit_daily',
        metric_name='btc_units',
        metric_value=123.456789,
        metric_unit='BTC',
        observed_at_utc=datetime(2026, 3, 10, 0, 0, 0, tzinfo=UTC),
        dimensions={'issuer': 'ishares', 'ticker': 'IBIT'},
        provenance=_sample_provenance(),
    )

    payload = build_etf_canonical_payload(record=record)
    assert payload['metric_value_kind'] == 'float'
    assert payload['metric_value_text'] == '123.456789'
    assert payload['metric_value_bool'] is None
    assert payload['record_source_id'] == 'etf_ishares_ibit_daily'
    assert payload['dimensions'] == {'issuer': 'ishares', 'ticker': 'IBIT'}


def test_etf_payload_is_valid_under_canonical_precision_contract() -> None:
    record = NormalizedMetricRecord(
        metric_id='metric-2',
        source_id='etf_fidelity_fbtc_daily',
        metric_name='as_of_date',
        metric_value='2026-03-10',
        metric_unit=None,
        observed_at_utc=datetime(2026, 3, 10, 0, 0, 0, tzinfo=UTC),
        dimensions={'issuer': 'fidelity', 'ticker': 'FBTC'},
        provenance=_sample_provenance(),
    )

    payload_raw = json.dumps(
        build_etf_canonical_payload(record=record),
        ensure_ascii=True,
        sort_keys=True,
        separators=(',', ':'),
    ).encode('utf-8')
    row = build_canonical_event_row(
        CanonicalEventWriteInput(
            source_id='etf',
            stream_id='etf_daily_metrics',
            partition_id='2026-03-10',
            source_offset_or_equivalent='metric-2',
            source_event_time_utc=datetime(2026, 3, 10, 0, 0, 0, tzinfo=UTC),
            ingested_at_utc=datetime(2026, 3, 10, 0, 0, 1, tzinfo=UTC),
            payload_content_type='application/json',
            payload_encoding='utf-8',
            payload_raw=payload_raw,
        )
    )
    payload_json = json.loads(row.payload_json)
    assert payload_json['metric_value_kind'] == 'string'
    assert payload_json['metric_value_text'] == '2026-03-10'
