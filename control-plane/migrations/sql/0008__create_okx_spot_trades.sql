CREATE TABLE IF NOT EXISTS {{DATABASE}}.okx_spot_trades (
    instrument_name LowCardinality(String) CODEC(ZSTD(3)),
    trade_id UInt64 CODEC(Delta(8), ZSTD(3)),
    side LowCardinality(String) CODEC(ZSTD(3)),
    price Float64 CODEC(Delta, ZSTD(3)),
    size Float64 CODEC(ZSTD(3)),
    quote_quantity Float64 CODEC(ZSTD(3)),
    timestamp UInt64 CODEC(Delta, ZSTD(3)),
    datetime DateTime64(3, 'UTC') CODEC(Delta, ZSTD(3))
)
ENGINE = MergeTree()
PARTITION BY toYYYYMM(datetime)
ORDER BY (toStartOfDay(datetime), trade_id)
SAMPLE BY trade_id
SETTINGS
    index_granularity = 8192,
    enable_mixed_granularity_parts = 1,
    min_rows_for_wide_part = 1000000,
    min_bytes_for_wide_part = 10000000,
    min_rows_for_compact_part = 10000,
    write_final_mark = 0,
    optimize_on_insert = 1,
    max_partitions_per_insert_block = 1000;
