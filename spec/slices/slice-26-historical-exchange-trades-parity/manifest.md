## What was done
- Added historical exchange-trades parity so all in-scope exchange trade datasets execute both `native` and `aligned_1s` through shared historical Python and HTTP contracts.
- Kept spot-kline historical routes operational under shared historical selector/filter/strict semantics while keeping `aligned_1s` fail-loud for klines in this tranche.
- Added contract and replay test coverage for three exchange historical trade routes/methods in both modes.
- Explicitly documented and tested that `spot_agg_trades` and `futures_trades` remain deferred from historical Python/HTTP scope.

## Current state
- Historical spot-trades routes now execute `native` and `aligned_1s` for:
  - `spot_trades`
  - `okx_spot_trades`
  - `bybit_spot_trades`
- Historical spot-kline routes remain `native` only and reject `aligned_1s` with contract error.
- Shared historical request contract and window semantics remain uniform across exchange historical routes and methods.

## Watch out
- Baseline fixture checksums for source archives are retrospective placeholders in this slice closeout.
- Historical `aligned_1s` for spot-kline routes is intentionally not enabled in this tranche and remains fail-loud by contract.
- No compatibility aliases are allowed for deferred historical dataset routes or removed method names.
