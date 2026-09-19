-- Daily percentiles of the depth200 1m book metrics for the UTC day {day:Date}.
-- quantileExact keeps the feed deterministic: the same day always yields the
-- same numbers. minutes counts the 1m rows behind each metric so thin book
-- coverage is visible in the feed.
SELECT
    metric,
    quantileExact(0.05)(value) AS p05,
    quantileExact(0.25)(value) AS p25,
    quantileExact(0.50)(value) AS p50,
    quantileExact(0.75)(value) AS p75,
    quantileExact(0.95)(value) AS p95,
    count() AS minutes
FROM
(
    SELECT
        arrayJoin([
            ('book_mid_price', book_mid_price),
            ('book_spread_bps', book_spread_bps),
            ('book_bid_depth_200_notional', book_bid_depth_200_notional),
            ('book_ask_depth_200_notional', book_ask_depth_200_notional),
            ('book_imbalance_200', book_imbalance_200)
        ]) AS metric_value,
        metric_value.1 AS metric,
        metric_value.2 AS value
    FROM origo.binance_spot_depth200_1m FINAL
    WHERE datetime >= toDateTime({day:Date})
      AND datetime < toDateTime({day:Date} + 1)
)
GROUP BY metric
ORDER BY metric
