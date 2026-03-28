from __future__ import annotations

import json
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Any
from uuid import uuid4

from origo_control_plane.backfill.s34_contract import get_s34_dataset_contract
from origo_control_plane.s34_exchange_backfill_runner import (
    _build_partition_proof_payload_or_raise,
    _build_range_proof_payload_or_raise,
    _build_source_manifest_payload_or_raise,
)
from origo_control_plane.s34_g1_g2_closeout_prep import (
    _fetch_ambiguous_canonical_partition_ids_or_raise,
    _load_manifest_event_summary_or_raise,
    _render_s34_stream_pair_in_clause_or_raise,
    build_s34_closeout_prep_summary,
)

from origo.events import (
    CanonicalStreamKey,
    PartitionProofState,
    RangeProofState,
    SourceManifestState,
)


def test_s34_manifest_event_summary_counts_partition_and_range_events(
    tmp_path: Path,
) -> None:
    manifest_path = tmp_path / 'manifest.jsonl'
    manifest_path.write_text(
        '\n'.join(
            (
                json.dumps(
                    {
                        'event_type': 's34_backfill_partition_completed',
                        'dataset': 'binance_spot_trades',
                        'partition_id': '2017-08-17',
                    },
                    sort_keys=True,
                ),
                json.dumps(
                    {
                        'event_type': 's34_backfill_range_proved',
                        'dataset': 'binance_spot_trades',
                        'range_proof': {
                            'range_start_partition_id': '2017-08-17',
                            'range_end_partition_id': '2017-08-17',
                        },
                    },
                    sort_keys=True,
                ),
            )
        )
        + '\n',
        encoding='utf-8',
    )

    summary = _load_manifest_event_summary_or_raise(
        path=manifest_path,
        known_datasets={'binance_spot_trades'},
    )

    assert summary['exists'] is True
    assert summary['event_counts_by_dataset']['binance_spot_trades'] == {
        's34_backfill_partition_completed': 1,
        's34_backfill_range_proved': 1,
    }
    assert (
        summary['latest_partition_event_by_dataset']['binance_spot_trades']['partition_id']
        == '2017-08-17'
    )
    assert (
        summary['latest_range_event_by_dataset']['binance_spot_trades']['range_proof'][
            'range_end_partition_id'
        ]
        == '2017-08-17'
    )


def test_s34_closeout_stream_filter_is_limited_to_slice_34_streams() -> None:
    clause = _render_s34_stream_pair_in_clause_or_raise(
        source_field='proofs.source_id',
        stream_field='proofs.stream_id',
    )

    assert "(proofs.source_id, proofs.stream_id) IN (" in clause
    assert "'binance', 'binance_spot_trades'" in clause
    assert "'bitcoin_core', 'bitcoin_block_headers'" in clause


def test_fetch_ambiguous_canonical_partition_ids_uses_authoritative_partition_helper(
    monkeypatch: Any,
) -> None:
    captured: dict[str, Any] = {}

    def _fake_helper(**kwargs: Any) -> dict[tuple[str, str], list[str]]:
        captured.update(kwargs)
        return {('etf', 'etf_daily_metrics'): ['2024-01-11']}

    monkeypatch.setattr(
        'origo_control_plane.s34_g1_g2_closeout_prep.load_grouped_nonterminal_partition_ids_or_raise',
        _fake_helper,
    )

    result = _fetch_ambiguous_canonical_partition_ids_or_raise(
        client=object(),  # type: ignore[arg-type]
        database='origo',
    )

    assert result == {('etf', 'etf_daily_metrics'): ['2024-01-11']}
    assert captured['database'] == 'origo'
    assert 'stream_pair_filter_sql' in captured
    assert "'binance', 'binance_spot_trades'" in captured['stream_pair_filter_sql']
    assert captured['terminal_states'] == ('empty_proved', 'proved_complete')


def test_s34_closeout_prep_summary_surfaces_remaining_gaps_and_terminal_evidence() -> None:
    contract = get_s34_dataset_contract('binance_spot_trades')
    stream_key = CanonicalStreamKey(
        source_id='binance',
        stream_id='binance_spot_trades',
        partition_id='2017-08-17',
    )
    source_manifest = SourceManifestState(
        stream_key=stream_key,
        manifest_revision=1,
        manifest_id=uuid4(),
        offset_ordering='numeric',
        source_artifact_identity_json=json.dumps(
            {
                'file_url': 'https://data.binance.vision/example.zip',
                'checksum_url': 'https://data.binance.vision/example.zip.CHECKSUM',
                'zip_sha256': '1' * 64,
                'csv_sha256': '2' * 64,
                'csv_filename': 'BTCUSDT-trades-2017-08-17.csv',
            },
            sort_keys=True,
        ),
        source_row_count=42,
        first_offset_or_equivalent='100',
        last_offset_or_equivalent='141',
        source_offset_digest_sha256='3' * 64,
        source_identity_digest_sha256='4' * 64,
        allow_empty_partition=False,
        allow_duplicate_offsets=False,
        manifested_by_run_id='s34-test',
        manifested_at_utc=datetime(2026, 3, 26, 12, 0, tzinfo=UTC),
    )
    partition_proof = PartitionProofState(
        stream_key=stream_key,
        proof_revision=2,
        proof_id=uuid4(),
        state='proved_complete',
        reason='canonical_matches_source',
        offset_ordering='numeric',
        source_row_count=42,
        canonical_row_count=42,
        canonical_unique_offset_count=42,
        first_offset_or_equivalent='100',
        last_offset_or_equivalent='141',
        source_offset_digest_sha256='3' * 64,
        source_identity_digest_sha256='4' * 64,
        canonical_offset_digest_sha256='5' * 64,
        canonical_identity_digest_sha256='6' * 64,
        gap_count=0,
        duplicate_count=0,
        proof_digest_sha256='7' * 64,
        proof_details_json=json.dumps({'status': 'ok'}, sort_keys=True),
        recorded_by_run_id='s34-test',
        recorded_at_utc=datetime(2026, 3, 26, 12, 1, tzinfo=UTC),
    )
    range_proof = RangeProofState(
        source_id='binance',
        stream_id='binance_spot_trades',
        range_start_partition_id='2017-08-17',
        range_end_partition_id='2017-08-17',
        range_revision=1,
        range_proof_id=uuid4(),
        partition_count=1,
        range_digest_sha256='8' * 64,
        range_details_json=json.dumps(
            {
                'partition_ids': ['2017-08-17'],
                'partition_proof_digests': ['7' * 64],
            },
            sort_keys=True,
        ),
        recorded_by_run_id='s34-test',
        recorded_at_utc=datetime(2026, 3, 26, 12, 2, tzinfo=UTC),
    )
    summary = build_s34_closeout_prep_summary(
        contracts=(contract,),
        manifest_summary={
            'path': 'spec/runtime/manifest.jsonl',
            'exists': True,
            'event_counts_by_dataset': {
                'binance_spot_trades': {
                    's34_backfill_partition_completed': 1,
                    's34_backfill_range_proved': 1,
                }
            },
            'latest_partition_event_by_dataset': {
                'binance_spot_trades': {'partition_id': '2017-08-17'}
            },
            'latest_range_event_by_dataset': {
                'binance_spot_trades': {'range_proof': {'range_end_partition_id': '2017-08-17'}}
            },
        },
        latest_source_manifests={
            ('binance', 'binance_spot_trades', '2017-08-17'): source_manifest
        },
        latest_partition_proofs={
            ('binance', 'binance_spot_trades', '2017-08-17'): partition_proof
        },
        latest_quarantines={},
        latest_range_proofs={('binance', 'binance_spot_trades'): [range_proof]},
        ambiguous_partition_ids={('binance', 'binance_spot_trades'): ['2017-08-18']},
        plan_end_date=date(2017, 8, 18),
    )

    dataset_summary = summary['dataset_summaries'][0]
    assert dataset_summary['dataset'] == 'binance_spot_trades'
    assert dataset_summary['latest_terminal_partition_id'] == '2017-08-17'
    assert (
        dataset_summary['latest_terminal_partition_evidence']['source_manifest'][
            'source_artifact_identity'
        ]['zip_sha256']
        == '1' * 64
    )
    assert dataset_summary['closeout_ready'] is False
    assert 'ambiguous_canonical_partitions_present' in dataset_summary['remaining_closeout_gaps']
    assert (
        dataset_summary['daily_boundary_assessment']['missing_partition_count'] == 1
    )


def test_s34_runner_manifest_payload_helpers_include_checksum_and_digest_evidence() -> None:
    stream_key = CanonicalStreamKey(
        source_id='binance',
        stream_id='binance_spot_trades',
        partition_id='2017-08-17',
    )
    source_manifest = SourceManifestState(
        stream_key=stream_key,
        manifest_revision=1,
        manifest_id=uuid4(),
        offset_ordering='numeric',
        source_artifact_identity_json=json.dumps(
            {'zip_sha256': '1' * 64, 'csv_sha256': '2' * 64},
            sort_keys=True,
        ),
        source_row_count=42,
        first_offset_or_equivalent='100',
        last_offset_or_equivalent='141',
        source_offset_digest_sha256='3' * 64,
        source_identity_digest_sha256='4' * 64,
        allow_empty_partition=False,
        allow_duplicate_offsets=False,
        manifested_by_run_id='s34-test',
        manifested_at_utc=datetime(2026, 3, 26, 12, 0, tzinfo=UTC),
    )
    partition_proof = PartitionProofState(
        stream_key=stream_key,
        proof_revision=2,
        proof_id=uuid4(),
        state='proved_complete',
        reason='canonical_matches_source',
        offset_ordering='numeric',
        source_row_count=42,
        canonical_row_count=42,
        canonical_unique_offset_count=42,
        first_offset_or_equivalent='100',
        last_offset_or_equivalent='141',
        source_offset_digest_sha256='3' * 64,
        source_identity_digest_sha256='4' * 64,
        canonical_offset_digest_sha256='5' * 64,
        canonical_identity_digest_sha256='6' * 64,
        gap_count=0,
        duplicate_count=0,
        proof_digest_sha256='7' * 64,
        proof_details_json=json.dumps({'status': 'ok'}, sort_keys=True),
        recorded_by_run_id='s34-test',
        recorded_at_utc=datetime(2026, 3, 26, 12, 1, tzinfo=UTC),
    )
    range_proof = RangeProofState(
        source_id='binance',
        stream_id='binance_spot_trades',
        range_start_partition_id='2017-08-17',
        range_end_partition_id='2017-08-17',
        range_revision=1,
        range_proof_id=uuid4(),
        partition_count=1,
        range_digest_sha256='8' * 64,
        range_details_json=json.dumps(
            {'partition_ids': ['2017-08-17']},
            sort_keys=True,
        ),
        recorded_by_run_id='s34-test',
        recorded_at_utc=datetime(2026, 3, 26, 12, 2, tzinfo=UTC),
    )

    source_manifest_payload = _build_source_manifest_payload_or_raise(source_manifest)
    partition_proof_payload = _build_partition_proof_payload_or_raise(partition_proof)
    range_proof_payload = _build_range_proof_payload_or_raise(range_proof)

    assert source_manifest_payload['source_artifact_identity']['csv_sha256'] == '2' * 64
    assert source_manifest_payload['allow_duplicate_offsets'] is False
    assert partition_proof_payload['canonical_identity_digest_sha256'] == '6' * 64
    assert partition_proof_payload['proof_details'] == {'status': 'ok'}
    assert range_proof_payload['range_details'] == {'partition_ids': ['2017-08-17']}
