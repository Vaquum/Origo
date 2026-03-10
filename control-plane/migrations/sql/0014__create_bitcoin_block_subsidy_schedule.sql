CREATE TABLE IF NOT EXISTS {{DATABASE}}.bitcoin_block_subsidy_schedule (
    block_height UInt32 CODEC(Delta(4), ZSTD(3)),
    block_hash String CODEC(ZSTD(3)),
    block_timestamp UInt64 CODEC(Delta, ZSTD(3)),
    halving_interval UInt32 CODEC(Delta(4), ZSTD(3)),
    subsidy_sats UInt64 CODEC(Delta(8), ZSTD(3)),
    subsidy_btc Float64 CODEC(Delta, ZSTD(3)),
    datetime DateTime64(3, 'UTC') CODEC(Delta, ZSTD(3)),
    source_chain LowCardinality(String) CODEC(ZSTD(3))
)
ENGINE = MergeTree()
PARTITION BY toYYYYMM(datetime)
ORDER BY (block_height)
SETTINGS
    index_granularity = 8192,
    enable_mixed_granularity_parts = 1,
    min_rows_for_wide_part = 1000000,
    min_bytes_for_wide_part = 10000000,
    min_rows_for_compact_part = 10000,
    write_final_mark = 0,
    optimize_on_insert = 1,
    max_partitions_per_insert_block = 1000;
