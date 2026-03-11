# S24 Historical Spot Cutover Runbook

- Owner: Origo API
- Last updated: 2026-03-11
- Slice reference: S24

## Purpose
Retire old historical spot endpoints and migrate internal consumers to the six canonical historical routes.

## Migration mapping

Old Python methods (removed):
- `get_spot_trades` -> use explicit exchange methods:
  - `get_binance_spot_trades`
  - `get_okx_spot_trades`
  - `get_bybit_spot_trades`
- `get_spot_klines` -> use:
  - `get_binance_spot_klines`
  - `get_okx_spot_klines`
  - `get_bybit_spot_klines`
- `get_spot_agg_trades` -> removed, no replacement in this slice
- `get_futures_trades` -> removed, out of scope
- `get_futures_klines` -> removed, out of scope

Old parameter names (removed, no aliases):
- `month_year` -> `start_date` / `end_date`
- `n_rows` -> `n_latest_rows`
- `n_random` -> `n_random_rows`
- `start_date_limit` -> `start_date` / `end_date`
- `show_summary` -> removed from historical trades interface

HTTP migration target:
- `/v1/historical/binance/spot/trades`
- `/v1/historical/binance/spot/klines`
- `/v1/historical/okx/spot/trades`
- `/v1/historical/okx/spot/klines`
- `/v1/historical/bybit/spot/trades`
- `/v1/historical/bybit/spot/klines`

## Cutover checklist
1. Replace all old Python historical method calls with exchange-explicit methods.
2. Replace old request payload keys with the new keys.
3. Verify each call uses at most one window mode, or intentionally uses no selector for full-history mode.
4. Validate strict date formatting (`YYYY-MM-DD`) and UTC day assumptions.
5. Validate rights metadata and status/error behavior in client handling.
6. Remove references to dropped methods from docs/scripts/tests.

## Guardrails
- No fallback aliases.
- No silent compatibility layer.
- Contract failures must return loud errors (`409`).
