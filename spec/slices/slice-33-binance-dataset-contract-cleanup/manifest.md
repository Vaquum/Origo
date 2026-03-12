## What was done
- Enforced `binance_spot_trades` as the only Binance dataset identifier across runtime contracts, planners, rights/export tags, historical surfaces, and control-plane paths.
- Removed legacy Binance non-spot dataset identifiers from active contracts and fail-loud validation paths.
- Updated precision and rights contracts to match the cleaned Binance scope.
- Cleaned docs/spec references so active references no longer advertise removed Binance non-spot dataset keys.

## Current state
- Active Binance dataset key is `binance_spot_trades` only.
- `spot_agg_trades`, `futures_trades`, and legacy `spot_trades` identifiers are absent from repository code and active docs/contracts.
- PR gates pass on the cleanup branch: `style`, `type`, `contract`, `replay`, `integrity`.
- Historical and raw surfaces remain available for Binance spot in both `native` and `aligned_1s` modes.

## Watch out
- Slice-15 proof artifacts are retained for provenance and were retroactively normalized; treat Slice-33 artifacts as the authoritative current contract source.
- Backfill work (Slice 34) must run from canonical event/log contracts already cleaned in this slice.
- Any new Binance dataset additions must use explicit new identifiers and must not reintroduce fallback aliases.
