# Slice 1 Raw Query Native Run Notes

- Date (UTC): 2026-03-04
- Scope: `S1-C1..C7`, `S1-P1..P3`, `S1-G1..G7`
- Fixture windows:
  - spot trades: `2017-08`
  - spot agg-trades: `2019-01`
  - futures trades: `2019-09`
- Runtime environment:
  - Query execution + proofs: `control-plane` `uv run` with `PYTHONPATH=..`
  - ClickHouse: local container on `localhost:8123/9000`
- API adapter validation: `uv run --with fastapi --with uvicorn --with httpx`
- API route smoke validation also confirmed with minimal runtime deps (`fastapi`, `httpx`, `clickhouse-connect`, `polars`, `pyarrow`) after removing eager package import side effects.

## System changes made as side effects of proof run

- Created/ensured local ClickHouse tables:
  - `origo.binance_agg_trades`
  - `origo.binance_futures_trades`
- Loaded local fixture sample rows (`500` each) from Binance monthly files for agg/futures proof windows.
- Generated artifacts:
  - `spec/slices/slice-1-raw-query-native/acceptance-proof.json`
  - `spec/slices/slice-1-raw-query-native/baseline-fixture-2017-08_2019-01_2019-09.json`

## Known warnings

- `n_rows` window mode intentionally emits warning `WINDOW_LATEST_ROWS_MUTABLE`; this is expected and used by strict-mode behavior.
- Binance spot agg-trades source CSV contains an extra boolean column that is ignored to fit current table schema; this is intentional in Slice 1.

## Deferred guardrails

- None for Slice 1 (`S1-G1..G7` completed).

## Completion confirmation

- `spec/2-itemized-work-plan.md` Slice 1 checkboxes were updated.
- `control-plane/pyproject.toml` version was bumped to `1.2.7` after docs closeout.
- `control-plane/CHANGELOG.md` was updated for Slice 1 docs closeout and Slice 2 kickoff.
- `.env.example` has no additional vars required by Slice 1 docs closeout.
- Developer docs closeout added:
  - `docs/Developer/s1-native-query-core.md`
  - `docs/Developer/s1-raw-query-api.md`
- User docs closeout added:
  - `docs/raw-query-reference.md`
  - `docs/data-taxonomy.md`
