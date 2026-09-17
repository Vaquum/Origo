-- Measured volume-at-price for the UTC day {day:Date}: every trade at its own
-- price, split by taker side. Quantities are summed as integer satoshis
-- (1e-8 BTC, exact for Binance quantities of at most 8 decimals) and
-- total_sats is formed as the sum of the two sides rather than counted a
-- third time, so buy + sell = total holds exactly.
-- is_buyer_maker = 1 means the resting order bought, so the taker sold.
WITH toUInt64(round(quantity * 100000000)) AS quantity_sats
SELECT
    price,
    sumIf(quantity_sats, is_buyer_maker = 0) AS taker_buy_sats,
    sumIf(quantity_sats, is_buyer_maker = 1) AS taker_sell_sats,
    taker_buy_sats + taker_sell_sats AS total_sats,
    count() AS trades
FROM origo.binance_spot_trades_raw_current
WHERE datetime >= toDateTime64({day:Date}, 6)
  AND datetime < toDateTime64({day:Date} + 1, 6)
GROUP BY price
ORDER BY price
