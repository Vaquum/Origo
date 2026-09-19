-- 8-hour session aggregates (00-08, 08-16, 16-24 of the day) of the depth200
-- 1m book for the UTC day {day:Date}. session_start is declared as UTC epoch
-- seconds (toUnixTimestamp) so the feed's time representation is fixed by
-- this query, not inherited from the server's Arrow serialization of
-- DateTime. The session grid is anchored to the day window itself, so window
-- and grid stay aligned whatever timezone the server resolves the naive
-- datetimes in. minutes counts the 1m rows behind each session so a thinly
-- covered session is visible instead of passing as a quiet one.
WITH toDateTime({day:Date}) AS day_start
SELECT
    toUnixTimestamp(day_start) + 28800 * intDiv(toUnixTimestamp(datetime) - toUnixTimestamp(day_start), 28800) AS session_start,
    count() AS minutes,
    argMin(book_mid_price, datetime) AS open_mid_price,
    argMax(book_mid_price, datetime) AS close_mid_price,
    avg(book_spread_bps) AS avg_spread_bps,
    avg(book_bid_depth_200_notional) AS avg_bid_depth_200_notional,
    avg(book_ask_depth_200_notional) AS avg_ask_depth_200_notional,
    avg(book_imbalance_200) AS avg_imbalance_200
FROM origo.binance_spot_depth200_1m FINAL
WHERE datetime >= day_start
  AND datetime < day_start + 86400
GROUP BY session_start
ORDER BY session_start
