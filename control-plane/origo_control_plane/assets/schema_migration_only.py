from __future__ import annotations

from dagster import AssetExecutionContext

_MIGRATION_COMMAND = 'uv run python -m origo_control_plane.migrations.cli migrate'


def raise_schema_migration_only(
    *, context: AssetExecutionContext, asset_name: str
) -> None:
    message = (
        f'{asset_name} is disabled. Schema management is SQL-migrations-only. '
        f'Run `{_MIGRATION_COMMAND}` before ingestion/export jobs.'
    )
    context.log.error(message)
    raise RuntimeError(message)
