-- One OHLCV bar per {bucket_seconds:UInt32} bucket of the UTC day {day:Date},
-- rolled up from the 1m origo.binance_spot_trades_time_current projection. bar_start is
-- declared as UTC epoch seconds (toUnixTimestamp) so the feed's time
-- representation is fixed by this query, not inherited from the server's
-- Arrow serialization of DateTime. The bar grid is anchored to the day
-- window itself rather than to the raw epoch, so window and grid stay
-- aligned whatever timezone the server resolves the naive datetimes in.
-- source_minutes counts distinct 1m rows and source_rows all 1m rows behind
-- each bar, so the caller can refuse a short or duplicated bar instead of
-- publishing it.
WITH toDateTime({day:Date}) AS day_start
SELECT
    toUnixTimestamp(day_start) + {bucket_seconds:UInt32} * intDiv(toUnixTimestamp(datetime) - toUnixTimestamp(day_start), {bucket_seconds:UInt32}) AS bar_start,
    argMin(open, datetime) AS open,
    max(high) AS high,
    min(low) AS low,
    argMax(close, datetime) AS close,
    sumKahan(volume) AS volume,
    sum(no_of_trades) AS no_of_trades,
    uniqExact(datetime) AS source_minutes,
    count() AS source_rows
FROM origo.binance_spot_trades_time_current
WHERE datetime >= day_start
  AND datetime < day_start + 86400
GROUP BY bar_start
ORDER BY bar_start
