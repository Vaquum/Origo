# Origo SQL Migrations

This directory is the source of truth for ClickHouse schema migrations.

## Rules
1. File naming: `NNNN__snake_case_name.sql` (example: `0001__create_table.sql`).
2. Versions are contiguous and start at `0001`.
3. Each file must contain exactly one SQL statement.
4. Applied migrations are immutable: never edit an applied file, add a new version instead.
5. Use `{{DATABASE}}` placeholder for database names inside SQL files.

## Commands
Run from `control-plane/` with ClickHouse env vars set:

```bash
uv run python -m origo_control_plane.migrations status
uv run python -m origo_control_plane.migrations migrate
```

Required env vars:
1. `CLICKHOUSE_HOST`
2. `CLICKHOUSE_PORT`
3. `CLICKHOUSE_USER`
4. `CLICKHOUSE_PASSWORD`
5. `CLICKHOUSE_DATABASE`

Missing or empty values fail loudly by design.
