from __future__ import annotations

import json
from pathlib import Path

from origo.events import (
    CANONICAL_EVENT_IDENTITY_FIELDS,
    canonical_event_envelope_contract,
)

_REPO_ROOT = Path(__file__).resolve().parents[2]
_CONTRACT_PATH = _REPO_ROOT / 'contracts' / 'canonical-event-envelope-v1.json'
_MIGRATIONS_DIR = _REPO_ROOT / 'control-plane' / 'migrations' / 'sql'


def _extract_create_table_columns(sql: str) -> list[str]:
    columns: list[str] = []
    in_columns = False

    for raw_line in sql.splitlines():
        line = raw_line.strip()
        if line == '':
            continue
        if line.startswith('CREATE TABLE'):
            in_columns = True
            continue
        if not in_columns:
            continue
        if line.startswith(')'):
            break
        if line.startswith('CONSTRAINT'):
            continue
        column_name = line.split()[0].rstrip(',')
        columns.append(column_name)

    return columns


def test_canonical_event_contract_json_matches_python_contract() -> None:
    payload = json.loads(_CONTRACT_PATH.read_text(encoding='utf-8'))
    assert payload == canonical_event_envelope_contract()


def test_canonical_event_identity_fields_match_contract_rule() -> None:
    assert CANONICAL_EVENT_IDENTITY_FIELDS == (
        'source_id',
        'stream_id',
        'partition_id',
        'source_offset_or_equivalent',
    )


def test_canonical_event_migration_files_exist() -> None:
    assert (_MIGRATIONS_DIR / '0017__create_canonical_event_envelope_schema.sql').exists()
    assert (_MIGRATIONS_DIR / '0018__create_canonical_event_log.sql').exists()


def test_canonical_event_log_sql_columns_match_envelope_contract() -> None:
    sql = (_MIGRATIONS_DIR / '0018__create_canonical_event_log.sql').read_text(
        encoding='utf-8'
    )
    sql_columns = _extract_create_table_columns(sql)
    expected_columns = [
        field['name'] for field in canonical_event_envelope_contract()['fields']
    ]
    assert sql_columns == expected_columns


def test_envelope_schema_table_has_required_contract_metadata_columns() -> None:
    sql = (_MIGRATIONS_DIR / '0017__create_canonical_event_envelope_schema.sql').read_text(
        encoding='utf-8'
    )
    sql_columns = _extract_create_table_columns(sql)
    assert sql_columns == [
        'envelope_version',
        'field_position',
        'field_name',
        'logical_type',
        'clickhouse_type',
        'nullable',
        'is_identity_key',
        'description',
        'created_at_utc',
    ]
