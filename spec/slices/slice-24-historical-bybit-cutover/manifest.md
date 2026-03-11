## What was done
- Added Bybit historical Python methods and HTTP routes with full cohesion to Binance/OKX contracts.
- Normalized Bybit trades into shared schema and enforced fail-loud side mapping guardrail (`buy -> 0`, `sell -> 1`).
- Completed cutover mapping runbook for replacing old historical methods/endpoints with the six canonical historical spot routes.
- Added final cohesion checks across all six historical routes and six Python methods.

## Current state
- Historical operational surface now exposes exactly six spot endpoints across Binance/OKX/Bybit.
- Python historical interface is fully explicit and exchange-scoped with identical method families.
- Old generic/futures historical methods are removed from runtime.

## Watch out
- Historical baseline fixture was generated retrospectively from deterministic contract outputs and may have partial checksum metadata for original files.
- Consumers must migrate request payloads to the new keys (`start_date`, `end_date`, `n_latest_rows`, `n_random_rows`) with no fallback aliases.
