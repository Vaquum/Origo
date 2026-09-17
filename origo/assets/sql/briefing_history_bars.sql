-- One OHLCV bar per {bucket_seconds:UInt32} bucket of the UTC span that runs
-- back {days:UInt32} whole days from midnight opening {through_day:Date},
-- rolled up from the 1m origo.binance_spot_trades_time_current projection. The span ends
-- where {through_day:Date} begins, so the history composes with that day's
-- own briefing feed file without overlapping it.
--
-- bar_start is declared as UTC epoch seconds (toUnixTimestamp) so the
-- history's time representation is fixed by this query, not inherited from
-- the server's Arrow serialization of DateTime. The bar grid is anchored to
-- span_start, which subtractDays puts on a midnight, and both published
-- bucket sizes (900 and 86400) divide a day: every bar in the span therefore
-- lands on the same midnight-aligned grid, and no bar straddles a day
-- boundary. Anchoring to the window rather than the raw epoch keeps window
-- and grid aligned whatever timezone the server resolves the naive
-- datetimes in.
--
-- source_minutes counts distinct 1m rows and source_rows all 1m rows behind
-- each bar, so the caller can refuse a short or duplicated bar instead of
-- publishing it.
WITH toDateTime(subtractDays({through_day:Date}, {days:UInt32})) AS span_start,
     toDateTime({through_day:Date}) AS span_end
SELECT
    toUnixTimestamp(span_start) + {bucket_seconds:UInt32} * intDiv(toUnixTimestamp(datetime) - toUnixTimestamp(span_start), {bucket_seconds:UInt32}) AS bar_start,
    argMin(open, datetime) AS open,
    max(high) AS high,
    min(low) AS low,
    argMax(close, datetime) AS close,
    sumKahan(volume) AS volume,
    sum(no_of_trades) AS no_of_trades,
    uniqExact(datetime) AS source_minutes,
    count() AS source_rows
FROM origo.binance_spot_trades_time_current
WHERE datetime >= span_start
  AND datetime < span_end
GROUP BY bar_start
ORDER BY bar_start
