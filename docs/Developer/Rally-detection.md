# Rally detection

`origo.query.rally_detection.detect_rallies` detects completed upward price events from native trade columns. It has no database, HTTP, grid or display dependency. [Slice #488](https://github.com/Vaquum/Origo/issues/488) specifies its contract; [#489](https://github.com/Vaquum/Origo/issues/489) supplies cube admission, reads and publication. Consumers apply result filters locally.

## Inputs

`RallyTrades` holds five equal-length one-dimensional NumPy columns:

| Column | Exact dtype | Domain |
|---|---|---|
| `trade_id` | `uint64` | Strictly increasing; native ID zero is valid |
| `timestamp_us` | `int64` | Nondecreasing Unix microseconds |
| `price` | `float64` | Finite and positive |
| `quote_quantity` | `float64` | Finite and nonnegative |
| `is_buyer_maker` | `bool` | False identifies taker-buy trades |

The detector preserves native ordering and rejects invalid columns. Timestamp ties are resolved by native ID. It retains column buffers, rather than creating a Python object per trade or a trade-by-rally table.

`analysis_start`, `analysis_end` and `known_at` are timezone-aware datetimes. Observation is exclusive through `min(analysis_end, known_at)`; this edge must follow the origin. Source keys match `[a-z][a-z0-9_]*`, instruments `[A-Z0-9]+`.

`coverage` is an explicit caller decision. Cube callers must supply actual source coverage, including required reference and bar context. They cannot infer completeness from returned rows. Only the fixed legacy spot preset may pass `None`, preserving its existing unverified gap policy. Pre-origin trades provide reference or ATR context; they never seed swing state.

`RallyDefinition` contains a mode, scale, Decimal target and mode-applicable parameters:

| Mode | Required fields | Extent |
|---|---|---|
| `first_hit` | `target`, `anchor_minutes` | First trade at/after anchor through first target hit |
| `controlled_advance` | `target`, `pullback`, `anchor_minutes` | First hit, provided the running-maximum pullback barrier was never exceeded |
| `swing` | `target`, `reversal` | Observed trough through final peak; confirmed by a later reversal |

Irrelevant fields are rejected. Anchored cadence is an integer `1..1440`, excluding Boolean values. Bps target/pullback are `(0,10000]`; bps reversal is `(0,10000)`. ATR quantities are `(0,100]`. Decimal values must be finite, convert to a positive finite Float64 and change the relevant bps multiplier. Bps reversal must also remain below `10000.0` after Float64 conversion. Intermediate thresholds and distances must remain finite and positive.

Anchors are UTC minutes aligned to the Unix epoch cadence. Optional `anchors_us` selects a strictly increasing aligned subset inside the analysis interval; it is unavailable for swing. Ordinary cube discovery supplies no explicit subset.

## Literal arithmetic

Each Decimal parameter converts to Float64 once. Comparisons use native prices directly:

```text
bps target = reference * (1.0 + float(target) / 10000.0)
bps swing reversal threshold = peak * (1.0 - float(reversal) / 10000.0)
bps controlled pullback distance = reference * float(pullback) / 10000.0
ATR target = reference + frozen_ATR * float(target)
ATR pullback/reversal distance = frozen_ATR * float(parameter)
```

Target equality succeeds. Controlled pullback equality is permitted; a greater running-maximum drawdown rejects the candidate before testing its target. Swing reversal equality confirms. Reported `return_bps` never decides eligibility. There is no epsilon or display rounding in these comparisons.

## Anchored modes

The reference is the last actual trade in `[anchor-24h, anchor)`. Membership begins at the first actual trade at/after the anchor. A hit must precede both the observation edge and `anchor+240min`. A completed hit does not wait for the full deadline.

Controlled advance starts its running maximum at the reference price. Each member updates that maximum, tests the frozen pullback distance, then tests the target. The bps distance is fixed from the reference, not recalculated from a later maximum; ATR target and pullback freeze at the anchor.

A missing reference is `unknown_context`. Uncovered reference or member context is also unknown. A fully observed miss or pullback rejection produces no event; an unfinished observed horizon is `right_censored`. A future coverage gap cannot change an already completed event.

## Swing

The first native trade at/after the origin seeds a running high. A strict new high replaces it; equal prices retain the earliest ID. A reversal-sized retreat proves the preceding down leg and starts a running trough. The unresolved initial leg is never emitted as a rally.

A strict new low replaces the trough. Its first target-reaching rise freezes that trough and starts the running peak. Strict new highs replace the peak; equal highs retain the earliest ID. The first reversal-sized retreat emits the frozen trough-to-peak event and seeds the next down leg with the confirming trade. The confirmation is evidence, not a member.

In ATR mode, each strict seed high and running trough freezes its own causal ATR. A missing value cannot be borrowed from future bars or replaced by an equal-priced trade. A later strict extreme may freeze its own available ATR. Qualified events retain the trough ATR, including for reversal. Coverage gaps break state; the first trade after coverage resumes seeds a new initial high.

At the observation edge an unresolved initial leg is `left_censored`; an established down/up leg awaiting completion is `right_censored`. Missing causal ATR or raw coverage is `unknown_context`. Diagnostics describe the discovery ceiling, not a retained candidate history.

## Causal ATR

`ATR_NAME` is `ATR14-SMA15min`. Supply `RallyBar` records built from native trades in UTC 15-minute intervals. At each freeze, use exactly 15 consecutive completed, nonempty, covered bars ending at the last boundary at/before that instant:

1. The first bar supplies the predecessor close.
2. Each subsequent bar supplies `max(high-low, abs(high-previous_close), abs(low-previous_close))`.
3. Sum those 14 Float64 values chronologically with `math.fsum` and divide by `14.0`.

An uncovered required interval yields `uncovered_atr_bar`; a covered interval with no bar yields `empty_atr_bar`. Fifteen flat bars yielding zero ATR produce `unknown_context` with reason `zero_atr`; other completed events remain available. There is no forward fill, search for earlier nonempty bars, Wilder smoothing, future bar or Explorer indicator dependency.

## Events and identity

`RallyDetection` returns immutable tuples of `RallyEvent` and `RallyDiagnostic`. Events include reference, member start/end and confirmation IDs/times/prices; duration, return, drawdown, quote volume/count and taker-buy volume/count.

Membership is inclusive in native ID order. Trades after the endpoint ID are excluded even if their timestamps tie. Anchored reference/lead-in and swing confirmation are excluded. Quote sums use chronological `math.fsum`. `max_drawdown` is a quote-currency price difference measured across members; anchored running maxima include the conceptual reference without counting it as a member. Swing excludes everything after the final peak. Anchored duration starts at the anchor; swing duration starts at the trough.

`definition_fingerprint` hashes `b'rally_v1\n'` followed by compact, key-sorted ASCII JSON of mode, scale and applicable parameters. Decimal fixed-point strings lose only fractional trailing zeros and the trailing dot, without Decimal-context rounding. Cadence remains an integer. The fixed spot preset hashes to `ae5ca8e31e5a92964556ed189f085f79a0ae69c2def9c97ed7eceb21bfa6960c`.

General anchored IDs are `<source>:<instrument>:rally_v1:<fingerprint>:a<anchor_us>`. Swing IDs are `<source>:<instrument>:rally_v1:<fingerprint>:o<origin_us>:t<trough_id>`. Grid, filters, source revisions, known edge, peak and confirmation do not enter semantic identity.

Exactly first hit / bps / target 30 / cadence 1 with `binance_spot_trades` and `BTCUSDT` retains `binance:spot:BTCUSDT:r30v1:t<anchor_seconds>`. Every detector event reports `definition_version='rally_v1'` and the new fingerprint; the legacy exporter keeps its existing `r30v1` metadata and schemas.

Completed event identities/extents are causal across later prefixes. Local replay may select events with `confirmed_at < rally_known_at <= discovery_ceiling`. Ceiling diagnostics must be hidden during earlier replay; they do not describe arbitrary prefixes.

## Legacy adapter and verification

`export_binance_rallies` passes its original predecessor and bulk trade frames to the shared first-hit preset, with its exact requested anchors and original exclusive limits. It independently derives exported lead-in/before/after bounds and retains the existing book query, loaded-window cap and atomic publication. `boundary='before'` may export the reference as context without making it a detector member.

The gated source-native suite contains the nine detector MVC tests and the unchanged exporter tests. Numerical evidence uses unchanged committed Binance records, the complete official 2017-08-17 archive and the captured 2026 spot window. Provenance binds provider/capture checksums, native ranges, units, independently computed expectations and parameter choices. Equality cases search parameter values against authentic prices; market records are never edited or synthesized.

```bash
pytest tests/origo_source_native/test_rally_detection.py -q
pytest tests/origo_source_native/test_binance_rallies.py -q
pytest tests/origo_source_native -q --maxfail=1
```
