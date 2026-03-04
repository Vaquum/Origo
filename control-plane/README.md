<h1 align="center">
  <br>
  <a href="https://github.com/Vaquum"><img src="https://github.com/Vaquum/Home/raw/main/assets/Logo.png" alt="Vaquum" width="150"></a>
  <br>
</h1>

<h3 align="center">ORIGO Control Plane</h3>

<p align="center">
  <a href="#description">Description</a> •
  <a href="#owner">Owner</a> •
  <a href="#integrations">Integrations</a> •
  <a href="#docs">Docs</a>
</p>
<hr>

## Description

Control plane for Origo.

## Owner

- [@mikkokotila](https://github.com/mikkokotila)

## Integrations

- https://github.com/Vaquum/origo-docker

## Docs

No documentation.

## Development (uv)

This project uses `uv` with a committed lockfile for deterministic dependency resolution.

```bash
cp ../.env.example .env
uv lock
uv sync --frozen
uv run python -c "import origo_control_plane; print('ok')"
```

For Docker builds, dependencies are installed from `pyproject.toml` + `uv.lock` via `uv sync --frozen`.

## Environment Contract

All required runtime config and secrets are defined in repository root `.env.example`.

Required values must be set and non-empty:
1. `CLICKHOUSE_HOST`
2. `CLICKHOUSE_PORT`
3. `CLICKHOUSE_HTTP_PORT`
4. `CLICKHOUSE_USER`
5. `CLICKHOUSE_PASSWORD`
6. `CLICKHOUSE_DATABASE`

Missing or empty values fail loudly by design.

## SQL Migrations

Versioned ClickHouse SQL migrations live in `control-plane/migrations/sql`.

```bash
uv run python -m origo_control_plane.migrations status
uv run python -m origo_control_plane.migrations migrate
```
