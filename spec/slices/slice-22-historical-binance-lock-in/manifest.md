## What was done
- Refactored `HistoricalData` Binance historical interface to explicit methods and removed legacy historical method names from runtime.
- Implemented strict historical window/date contract in shared query helpers (`date-window` vs `n_latest_rows` vs `n_random_rows`) with fail-loud validation.
- Added Binance historical HTTP routes for spot trades and spot klines with raw-envelope parity, auth/rights guardrails, and strict warning behavior.
- Added contract tests for method signature/dropped-method checks, route registration, envelope shape, and fail-loud status behavior.

## Current state
- Binance historical spot data is available over Python and HTTP using only the new contract keys.
- Date-window supports open bounds (resolved from source table min/max day) and strict `YYYY-MM-DD` inputs.
- No alias fallbacks exist for removed methods or removed request parameters.

## Watch out
- Proof fixtures were produced retrospectively with contract-focused deterministic checks; source checksum fields include placeholders where historical archive checksums were not captured in this pass.
- `n_latest_rows` and `n_random_rows` intentionally emit warnings and can fail in `strict=true` mode by contract.
