from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from clickhouse_driver import Client as ClickHouseClient

from origo_control_plane.migrations import MigrationRunner, MigrationSettings
from origo_control_plane.s20_bitcoin_proof_common import (
    BITCOIN_DERIVED_ALIGNED_DATASETS,
    BITCOIN_NATIVE_DATASETS,
    FIXTURE_DAY,
    INGESTED_AT_UTC,
    WINDOW_END_ISO,
    WINDOW_START_ISO,
    build_clickhouse_client,
    build_fixture_canonical_events,
    build_fixture_rows_by_dataset,
    fingerprint_rows,
    fixture_events_by_stream,
    legacy_aligned_expected_rows,
    query_aligned_rows,
    query_all_legacy_native_rows,
    query_native_rows,
    rows_hash,
    run_native_and_aligned_projectors,
    seed_legacy_tables,
    source_checksums_from_events,
)
from origo_control_plane.utils.bitcoin_canonical_event_ingest import (
    write_bitcoin_events_to_canonical,
)

_PROOF_DB_SUFFIX_RUN_1 = '_s20_p1_p3_proof_run_1'
_PROOF_DB_SUFFIX_RUN_2 = '_s20_p1_p3_proof_run_2'
_SLICE_DIR = (
    Path(__file__).resolve().parents[2]
    / 'spec'
    / 'slices'
    / 'slice-20-bitcoin-event-sourcing-port'
)


def _assert_all_parity_true(
    *,
    native_parity: dict[str, bool],
    aligned_parity: dict[str, bool],
    canonical_native_rows: dict[str, list[dict[str, Any]]],
    legacy_native_rows: dict[str, list[dict[str, Any]]],
    canonical_aligned_rows: dict[str, list[dict[str, Any]]],
    legacy_aligned_rows: dict[str, list[dict[str, Any]]],
) -> None:
    if all(native_parity.values()) and all(aligned_parity.values()):
        return

    native_mismatch_hashes: dict[str, dict[str, str]] = {}
    for dataset, parity_ok in native_parity.items():
        if parity_ok:
            continue
        native_mismatch_hashes[dataset] = {
            'canonical_rows_hash_sha256': rows_hash(canonical_native_rows[dataset]),
            'legacy_rows_hash_sha256': rows_hash(legacy_native_rows[dataset]),
        }

    aligned_mismatch_hashes: dict[str, dict[str, str]] = {}
    for dataset, parity_ok in aligned_parity.items():
        if parity_ok:
            continue
        aligned_mismatch_hashes[dataset] = {
            'canonical_rows_hash_sha256': rows_hash(canonical_aligned_rows[dataset]),
            'legacy_rows_hash_sha256': rows_hash(legacy_aligned_rows[dataset]),
        }

    raise RuntimeError(
        'S20-P2 parity mismatch against Bitcoin fixture baselines: '
        f'native_parity={native_parity} aligned_parity={aligned_parity} '
        f'native_mismatch_hashes={native_mismatch_hashes} '
        f'aligned_mismatch_hashes={aligned_mismatch_hashes}'
    )


def _run_single_database(
    *,
    settings: MigrationSettings,
    proof_database: str,
) -> dict[str, Any]:
    proof_settings = MigrationSettings(
        host=settings.host,
        port=settings.port,
        user=settings.user,
        password=settings.password,
        database=proof_database,
    )
    runner = MigrationRunner(settings=proof_settings)
    admin_client: ClickHouseClient = build_clickhouse_client(settings)

    try:
        admin_client.execute(f'DROP DATABASE IF EXISTS {proof_database} SYNC')
        runner.migrate()

        rows_by_dataset = build_fixture_rows_by_dataset()
        events = build_fixture_canonical_events(rows_by_dataset=rows_by_dataset)
        events_by_stream = fixture_events_by_stream(rows_by_dataset=rows_by_dataset)

        seed_legacy_tables(
            client=admin_client,
            database=proof_database,
            rows_by_dataset=rows_by_dataset,
        )

        write_summary = write_bitcoin_events_to_canonical(
            client=admin_client,
            database=proof_database,
            events=events,
            run_id='s20-p1-p3-proof-write',
            ingested_at_utc=INGESTED_AT_UTC,
        )
        expected_write_summary = {
            'rows_processed': len(events),
            'rows_inserted': len(events),
            'rows_duplicate': 0,
        }
        if write_summary != expected_write_summary:
            raise RuntimeError(
                'S20-P1 expected all fixture events to be inserted once; '
                f'observed={write_summary} expected={expected_write_summary}'
            )

        projection_summaries = run_native_and_aligned_projectors(
            client=admin_client,
            database=proof_database,
            events_by_stream=events_by_stream,
            run_id_prefix='s20-p1-p3-proof',
            projected_at_utc=datetime(2026, 3, 10, 23, 30, 5, tzinfo=UTC),
        )

        canonical_native_rows = {
            dataset: query_native_rows(dataset=dataset, database=proof_database)
            for dataset in BITCOIN_NATIVE_DATASETS
        }
        canonical_aligned_rows = {
            dataset: query_aligned_rows(dataset=dataset, database=proof_database)
            for dataset in BITCOIN_DERIVED_ALIGNED_DATASETS
        }

        legacy_native_rows = query_all_legacy_native_rows(
            client=admin_client,
            database=proof_database,
        )
        legacy_aligned_rows = legacy_aligned_expected_rows(rows_by_dataset=rows_by_dataset)

        native_parity = {
            dataset: canonical_native_rows[dataset] == legacy_native_rows[dataset]
            for dataset in BITCOIN_NATIVE_DATASETS
        }
        aligned_parity = {
            dataset: canonical_aligned_rows[dataset] == legacy_aligned_rows[dataset]
            for dataset in BITCOIN_DERIVED_ALIGNED_DATASETS
        }
        _assert_all_parity_true(
            native_parity=native_parity,
            aligned_parity=aligned_parity,
            canonical_native_rows=canonical_native_rows,
            legacy_native_rows=legacy_native_rows,
            canonical_aligned_rows=canonical_aligned_rows,
            legacy_aligned_rows=legacy_aligned_rows,
        )

        native_fingerprints = {
            dataset: fingerprint_rows(
                dataset=dataset,
                rows=canonical_native_rows[dataset],
                aligned=False,
            )
            for dataset in BITCOIN_NATIVE_DATASETS
        }
        aligned_fingerprints = {
            dataset: fingerprint_rows(
                dataset=dataset,
                rows=canonical_aligned_rows[dataset],
                aligned=True,
            )
            for dataset in BITCOIN_DERIVED_ALIGNED_DATASETS
        }

        return {
            'write_summary': write_summary,
            'projection_summaries': projection_summaries,
            'native_rows': canonical_native_rows,
            'aligned_rows': canonical_aligned_rows,
            'native_fingerprints': native_fingerprints,
            'aligned_fingerprints': aligned_fingerprints,
            'native_parity': native_parity,
            'aligned_parity': aligned_parity,
            'source_checksums': source_checksums_from_events(events=events),
        }
    finally:
        runner.close()
        admin_client.execute(f'DROP DATABASE IF EXISTS {proof_database} SYNC')
        admin_client.disconnect()


def _deterministic_match(run_1: dict[str, Any], run_2: dict[str, Any]) -> bool:
    for dataset in BITCOIN_NATIVE_DATASETS:
        if run_1['native_fingerprints'][dataset] != run_2['native_fingerprints'][dataset]:
            return False
    for dataset in BITCOIN_DERIVED_ALIGNED_DATASETS:
        if run_1['aligned_fingerprints'][dataset] != run_2['aligned_fingerprints'][dataset]:
            return False
    return True


def run_s20_p1_p3_proof() -> dict[str, Any]:
    base_settings = MigrationSettings.from_env()
    run_1_database = f'{base_settings.database}{_PROOF_DB_SUFFIX_RUN_1}'
    run_2_database = f'{base_settings.database}{_PROOF_DB_SUFFIX_RUN_2}'

    run_1 = _run_single_database(settings=base_settings, proof_database=run_1_database)
    run_2 = _run_single_database(settings=base_settings, proof_database=run_2_database)

    deterministic_match = _deterministic_match(run_1, run_2)
    if not deterministic_match:
        raise RuntimeError('S20-P3 determinism failure: run fingerprints differ')

    acceptance_payload = {
        'proof_scope': (
            'Slice 20 S20-P1 fixed-window acceptance for Bitcoin native '
            '(all 7 datasets) and aligned_1s (4 derived datasets) '
            'from canonical projection serving paths'
        ),
        'window': {
            'start_utc': WINDOW_START_ISO,
            'end_utc': WINDOW_END_ISO,
        },
        'row_counts': {
            'native': {
                dataset: run_1['native_fingerprints'][dataset]['row_count']
                for dataset in BITCOIN_NATIVE_DATASETS
            },
            'aligned_1s': {
                dataset: run_1['aligned_fingerprints'][dataset]['row_count']
                for dataset in BITCOIN_DERIVED_ALIGNED_DATASETS
            },
        },
        'native_fingerprints': run_1['native_fingerprints'],
        'aligned_fingerprints': run_1['aligned_fingerprints'],
        'source_checksums': run_1['source_checksums'],
        'acceptance_verified': True,
    }

    parity_payload = {
        'proof_scope': (
            'Slice 20 S20-P2 parity checks versus Bitcoin fixture baselines '
            '(legacy native tables + deterministic aligned fixture expectations)'
        ),
        'window': {
            'start_utc': WINDOW_START_ISO,
            'end_utc': WINDOW_END_ISO,
        },
        'native_parity': run_1['native_parity'],
        'aligned_parity': run_1['aligned_parity'],
        'parity_verified': True,
    }

    determinism_payload = {
        'proof_scope': (
            'Slice 20 S20-P3 replay determinism for Bitcoin native and aligned_1s '
            'after event-sourcing cutover'
        ),
        'run_1_fingerprints': {
            dataset: {
                'native': run_1['native_fingerprints'][dataset],
                **(
                    {'aligned_1s': run_1['aligned_fingerprints'][dataset]}
                    if dataset in BITCOIN_DERIVED_ALIGNED_DATASETS
                    else {}
                ),
            }
            for dataset in BITCOIN_NATIVE_DATASETS
        },
        'run_2_fingerprints': {
            dataset: {
                'native': run_2['native_fingerprints'][dataset],
                **(
                    {'aligned_1s': run_2['aligned_fingerprints'][dataset]}
                    if dataset in BITCOIN_DERIVED_ALIGNED_DATASETS
                    else {}
                ),
            }
            for dataset in BITCOIN_NATIVE_DATASETS
        },
        'deterministic_match': deterministic_match,
    }

    baseline_fixture = {
        'fixture_window': {
            'start_utc': WINDOW_START_ISO,
            'end_utc': WINDOW_END_ISO,
        },
        'source_checksums': run_1['source_checksums'],
        'run_1_fingerprints': {
            dataset: {
                'native': run_1['native_fingerprints'][dataset],
                **(
                    {'aligned_1s': run_1['aligned_fingerprints'][dataset]}
                    if dataset in BITCOIN_DERIVED_ALIGNED_DATASETS
                    else {}
                ),
            }
            for dataset in BITCOIN_NATIVE_DATASETS
        },
        'run_2_fingerprints': {
            dataset: {
                'native': run_2['native_fingerprints'][dataset],
                **(
                    {'aligned_1s': run_2['aligned_fingerprints'][dataset]}
                    if dataset in BITCOIN_DERIVED_ALIGNED_DATASETS
                    else {}
                ),
            }
            for dataset in BITCOIN_NATIVE_DATASETS
        },
        'deterministic_match': deterministic_match,
        'column_key': {
            'row_count': 'Total rows returned in the fixture window for this run.',
            'first_offset': 'Deterministic first source offset identity in result rows.',
            'last_offset': 'Deterministic last source offset identity in result rows.',
            'value_sum': 'Deterministic sum across configured numeric fingerprint fields.',
            'hash_per_day': 'Per-day SHA256 of canonicalized rows in that UTC day bucket.',
            'rows_hash_sha256': 'SHA256 of canonicalized full result rows for this run.',
            'source_checksums': (
                'Daily canonical source payload checksums. zip_sha256/csv_sha256 fields '
                'are populated uniformly for fixture schema even though Bitcoin source '
                'format is RPC JSON.'
            ),
        },
    }

    return {
        'proof_s20_p1': acceptance_payload,
        'proof_s20_p2': parity_payload,
        'proof_s20_p3': determinism_payload,
        'baseline_fixture': baseline_fixture,
    }


def main() -> None:
    payload = run_s20_p1_p3_proof()
    _SLICE_DIR.mkdir(parents=True, exist_ok=True)

    p1_path = _SLICE_DIR / 'proof-s20-p1-acceptance.json'
    p2_path = _SLICE_DIR / 'proof-s20-p2-parity.json'
    p3_path = _SLICE_DIR / 'proof-s20-p3-determinism.json'
    baseline_path = _SLICE_DIR / f'baseline-fixture-{FIXTURE_DAY}_{FIXTURE_DAY}.json'

    p1_path.write_text(
        json.dumps(payload['proof_s20_p1'], indent=2, sort_keys=True) + '\n',
        encoding='utf-8',
    )
    p2_path.write_text(
        json.dumps(payload['proof_s20_p2'], indent=2, sort_keys=True) + '\n',
        encoding='utf-8',
    )
    p3_path.write_text(
        json.dumps(payload['proof_s20_p3'], indent=2, sort_keys=True) + '\n',
        encoding='utf-8',
    )
    baseline_path.write_text(
        json.dumps(payload['baseline_fixture'], indent=2, sort_keys=True) + '\n',
        encoding='utf-8',
    )

    print(json.dumps(payload, indent=2, sort_keys=True))


if __name__ == '__main__':
    main()
