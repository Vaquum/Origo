# Slice 0 Bootstrap Run Notes

- Date (UTC): 2026-03-04
- Scope: Control-plane migration bootstrap, TDW -> Origo rename pass, Binance ingest smoke + replay proof
- Fixture window: 2017-08-17 to 2017-08-19 (`BTCUSDT` spot daily files)
- ClickHouse runtime: container `control-plane-clickhouse-1` with `CLICKHOUSE_PASSWORD=origo`
- Config hardening applied during proof run: `docker-compose.yml` now requires `CLICKHOUSE_PASSWORD` explicitly (fail-fast).
- Evidence artifact: `spec/slices/slice-0-bootstrap/baseline-fixture-2017-08-17_2017-08-19.json`
- Determinism: two replay runs produced identical row counts and fingerprints (`deterministic_match=true`)
- Known warnings: `ALTER DATABASE ... MODIFY SETTING` not supported for Atomic DB engine on current ClickHouse; warnings surfaced intentionally.
- Open guardrails in Slice 0: TLS enforcement and immutable audit-log sink not yet implemented.
- Guardrail update: `S0-G4` completed by adopting `uv.lock` in `control-plane`, validating `uv sync --frozen`, and wiring Docker builds to the lockfile.
- Follow-up fix during `S0-G4`: runtime image no longer exposed `dagit`; resolved by making `dagster-webserver` a runtime dependency and updating compose command accordingly.
- Guardrail update: `S0-G5` completed by scaffolding SQL migrations (`control-plane/migrations/sql`), adding strict `status`/`migrate` runner (`origo_control_plane.migrations`), and validating migration apply + ledger state on ClickHouse.
- Guardrail update: `S0-G6` completed by introducing root `.env.example`, centralizing fail-loud env validation, and removing deployment-default ClickHouse fallbacks from runtime code paths.
- `S0-G6` closeout question: "was any deployment-specific value hard-coded?" -> fixed in active runtime paths for ClickHouse host/port/user/password/database.
- Confirmation: `spec/2-itemized-work-plan.md` checkboxes, `control-plane` version, `control-plane/CHANGELOG.md`, and `.env.example` were updated for `S0-G6`.
