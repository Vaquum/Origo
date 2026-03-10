from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from clickhouse_driver import Client as ClickHouseClient

from origo.events.envelope import canonical_event_envelope_contract
from origo_control_plane.migrations import MigrationRunner, MigrationSettings

_PROOF_DB_SUFFIX = '_s14_c1_proof'
_EVENT_LOG_TABLE = 'canonical_event_log'
_ENVELOPE_SCHEMA_TABLE = 'canonical_event_envelope_schema'


def _build_clickhouse_client(settings: MigrationSettings) -> ClickHouseClient:
    return ClickHouseClient(
        host=settings.host,
        port=settings.port,
        user=settings.user,
        password=settings.password,
    )


def _fetch_table_columns(
    client: ClickHouseClient, *, database: str, table: str
) -> list[tuple[str, str]]:
    rows = client.execute(
        '''
        SELECT
            name,
            type
        FROM system.columns
        WHERE database = %(database)s AND table = %(table)s
        ORDER BY position ASC
        ''',
        {'database': database, 'table': table},
    )
    return [(name, column_type) for name, column_type in rows]


def _fetch_migration_rows(
    client: ClickHouseClient, *, database: str
) -> list[tuple[int, str, str, str]]:
    rows = client.execute(
        f'''
        SELECT
            version,
            name,
            checksum,
            toString(applied_at)
        FROM {database}.schema_migrations
        WHERE version IN (17, 18)
        ORDER BY version ASC
        '''
    )
    return [
        (int(version), name, checksum, applied_at)
        for version, name, checksum, applied_at in rows
    ]


def run_s14_c1_proof() -> dict[str, Any]:
    base_settings = MigrationSettings.from_env()
    proof_database = f'{base_settings.database}{_PROOF_DB_SUFFIX}'
    proof_settings = MigrationSettings(
        host=base_settings.host,
        port=base_settings.port,
        user=base_settings.user,
        password=base_settings.password,
        database=proof_database,
    )

    admin_client = _build_clickhouse_client(base_settings)
    runner = MigrationRunner(settings=proof_settings)
    try:
        admin_client.execute(f'DROP DATABASE IF EXISTS {proof_database} SYNC')
        applied_statuses = runner.migrate()
        status_rows = runner.status()

        required_versions = {17, 18}
        applied_versions = {
            status.version
            for status in status_rows
            if status.state == 'applied' and status.version in required_versions
        }
        if applied_versions != required_versions:
            raise RuntimeError(
                'S14-C1 proof expected migrations 0017 and 0018 to be applied with ledger tracking'
            )

        event_log_columns = _fetch_table_columns(
            admin_client, database=proof_database, table=_EVENT_LOG_TABLE
        )
        envelope_schema_columns = _fetch_table_columns(
            admin_client, database=proof_database, table=_ENVELOPE_SCHEMA_TABLE
        )
        if event_log_columns == []:
            raise RuntimeError('S14-C1 proof expected canonical_event_log table columns')
        if envelope_schema_columns == []:
            raise RuntimeError(
                'S14-C1 proof expected canonical_event_envelope_schema table columns'
            )

        contract = canonical_event_envelope_contract()
        contract_field_names = [field['name'] for field in contract['fields']]
        contract_field_types = {
            field['name']: field['clickhouse_type'] for field in contract['fields']
        }
        event_column_names = [name for name, _ in event_log_columns]
        if event_column_names != contract_field_names:
            raise RuntimeError(
                'S14-C1 proof detected mismatch between contract field names and canonical_event_log columns'
            )

        for column_name, column_type in event_log_columns:
            expected_type = contract_field_types[column_name]
            if column_type != expected_type:
                raise RuntimeError(
                    'S14-C1 proof detected ClickHouse type mismatch for '
                    f'column={column_name} expected={expected_type} actual={column_type}'
                )

        migration_rows = _fetch_migration_rows(admin_client, database=proof_database)
        if len(migration_rows) != 2:
            raise RuntimeError(
                'S14-C1 proof expected schema_migrations ledger rows for versions 0017 and 0018'
            )

        return {
            'proof_scope': 'Slice 14 S14-C1 canonical event envelope + append-only log migrations',
            'proof_database': proof_database,
            'applied_migration_count': len(applied_statuses),
            'required_versions_applied': sorted(applied_versions),
            'migration_ledger_rows_17_18': [
                {
                    'version': version,
                    'name': name,
                    'checksum': checksum,
                    'applied_at': applied_at,
                }
                for version, name, checksum, applied_at in migration_rows
            ],
            'canonical_event_log_columns': [
                {'name': name, 'clickhouse_type': column_type}
                for name, column_type in event_log_columns
            ],
            'canonical_event_envelope_schema_columns': [
                {'name': name, 'clickhouse_type': column_type}
                for name, column_type in envelope_schema_columns
            ],
            'contract_identity_fields': contract['identity_fields'],
        }
    finally:
        runner.close()
        admin_client.execute(f'DROP DATABASE IF EXISTS {proof_database} SYNC')
        admin_client.disconnect()


def main() -> None:
    payload = run_s14_c1_proof()
    repo_root = Path.cwd().resolve()
    if repo_root.name == 'control-plane':
        repo_root = repo_root.parent
    output_path = (
        repo_root
        / 'spec'
        / 'slices'
        / 'slice-14-event-sourcing-core'
        / 'capability-proof-s14-c1-migrations.json'
    )
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(
        json.dumps(payload, indent=2, sort_keys=True) + '\n', encoding='utf-8'
    )
    print(json.dumps(payload, indent=2, sort_keys=True))


if __name__ == '__main__':
    main()
