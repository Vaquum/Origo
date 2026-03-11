CREATE TABLE IF NOT EXISTS {{DATABASE}}.canonical_bybit_spot_trades_native_v1 (
    symbol LowCardinality(String),
    trade_id UInt64,
    trd_match_id String,
    side LowCardinality(String),
    price Float64,
    size Float64,
    quote_quantity Float64,
    timestamp UInt64,
    datetime DateTime64(3, 'UTC'),
    tick_direction LowCardinality(String),
    gross_value Float64,
    home_notional Float64,
    foreign_notional Float64,
    event_id UUID,
    source_offset_or_equivalent String,
    source_event_time_utc Nullable(DateTime64(9, 'UTC')),
    ingested_at_utc DateTime64(3, 'UTC')
)
ENGINE = MergeTree()
PARTITION BY toYYYYMM(datetime)
ORDER BY (
    toStartOfDay(datetime),
    trade_id,
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
