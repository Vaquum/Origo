-- Per-minute book state for the UTC day {day:Date} from the depth200 1m
-- projection. minute_start is declared as UTC epoch seconds (toUnixTimestamp)
-- so the feed's time representation is fixed by this query, not inherited
-- from the server's Arrow serialization of DateTime. FINAL collapses
-- ReplacingMergeTree duplicates to the latest snapshot per minute.
SELECT
    toUnixTimestamp(datetime) AS minute_start,
    book_mid_price,
    book_spread_bps,
    book_bid_depth_200_notional,
    book_ask_depth_200_notional,
    book_imbalance_200
FROM origo.binance_spot_depth200_1m FINAL
WHERE datetime >= toDateTime({day:Date})
  AND datetime < toDateTime({day:Date}) + 86400
ORDER BY minute_start
