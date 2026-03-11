## What was done
- Added OKX historical Python methods and HTTP routes with signature parity to Binance contracts.
- Normalized OKX trades into shared schema (`trade_id`, `timestamp`, `price`, `quantity`, `is_buyer_maker`, `datetime`).
- Implemented fail-loud side mapping guardrail for OKX (`buy -> 0`, `sell -> 1`) in historical trades/klines path.
- Extended contract tests to cover route and interface cohesion.

## Current state
- OKX historical trades/klines now use the same request/response interface as Binance.
- Shared historical guardrails (auth/rights/error taxonomy/strict warnings) are active on OKX routes.
- Legacy parameter and method aliases remain unsupported by design.

## Watch out
- Proof artifacts for this slice are retrospective and focused on deterministic contract-level behavior; source checksums are partially populated placeholders in baseline fixture.
- Any side values outside `buy|sell` are hard failures by design.
