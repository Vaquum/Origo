## What was done
- Added historical exchange-spot parity so in-scope exchange trades and klines execute both `native` and `aligned_1s` through shared historical Python and HTTP contracts.
- Implemented deterministic aligned-kline aggregation from canonical aligned 1s buckets (`open`, `high`, `low`, `close`, `volume`, `no_of_trades`, `liquidity_sum`).
- Added contract and replay test coverage for exchange historical spot routes in aligned mode.
- Explicitly documented and tested that legacy Binance non-spot dataset keys are removed from historical Python/HTTP scope.

## Current state
- Historical spot-trades routes now execute `native` and `aligned_1s` for:
  - `binance_spot_trades`
  - `okx_spot_trades`
  - `bybit_spot_trades`
- Historical spot-kline routes execute both `native` and `aligned_1s`.
- Shared historical request contract and window semantics remain uniform across exchange historical routes and methods.

## Watch out
- Baseline fixture checksums for source archives are retrospective placeholders in this slice closeout.
- Aligned kline rows intentionally expose only fields derivable from aligned buckets; maker-derived kline metrics remain native-only.
- No compatibility aliases are allowed for deferred historical dataset routes or removed method names.
