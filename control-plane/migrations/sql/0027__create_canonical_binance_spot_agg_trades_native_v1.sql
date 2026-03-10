CREATE TABLE IF NOT EXISTS {{DATABASE}}.canonical_binance_spot_agg_trades_native_v1 (
    agg_trade_id UInt64,
    timestamp UInt64,
    price Float64,
    quantity Float64,
    first_trade_id UInt64,
    last_trade_id UInt64,
    is_buyer_maker UInt8,
    datetime DateTime64(3, 'UTC'),
    event_id UUID,
    source_offset_or_equivalent String,
    source_event_time_utc Nullable(DateTime64(9, 'UTC')),
    ingested_at_utc DateTime64(3, 'UTC')
)
ENGINE = MergeTree()
PARTITION BY toYYYYMM(datetime)
ORDER BY (
    toStartOfDay(datetime),
    agg_trade_id,
    event_id
)
SETTINGS
    index_granularity = 8192,
    enable_mixed_granularity_parts = 1,
    min_rows_for_wide_part = 1000000,
    min_bytes_for_wide_part = 10000000,
    min_rows_for_compact_part = 10000,
    write_final_mark = 0,
    optimize_on_insert = 1,
    max_partitions_per_insert_block = 1000;
