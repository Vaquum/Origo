from __future__ import annotations

from pathlib import Path

_REPO_ROOT = Path(__file__).resolve().parents[2]
_MIGRATIONS_DIR = _REPO_ROOT / 'control-plane' / 'migrations' / 'sql'


def test_canonical_event_reset_boundary_migration_files_exist() -> None:
    assert (_MIGRATIONS_DIR / '0045__create_canonical_partition_reset_boundaries.sql').exists()
    assert (_MIGRATIONS_DIR / '0046__create_canonical_event_log_active_v1.sql').exists()


def test_canonical_event_active_view_uses_reset_boundaries() -> None:
    sql = (
        _MIGRATIONS_DIR / '0046__create_canonical_event_log_active_v1.sql'
    ).read_text(encoding='utf-8')

    assert 'canonical_event_log_active_v1' in sql
    assert 'canonical_partition_reset_boundaries' in sql
    assert 'event_log.ingested_at_utc > reset_boundaries.latest_reset_at_utc' in sql
