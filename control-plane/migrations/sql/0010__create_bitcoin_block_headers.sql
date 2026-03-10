CREATE TABLE IF NOT EXISTS {{DATABASE}}.bitcoin_block_headers (
    height UInt32 CODEC(Delta(4), ZSTD(3)),
    block_hash String CODEC(ZSTD(3)),
    prev_hash String CODEC(ZSTD(3)),
    merkle_root String CODEC(ZSTD(3)),
    version Int32 CODEC(Delta(4), ZSTD(3)),
    nonce UInt32 CODEC(Delta(4), ZSTD(3)),
    difficulty Float64 CODEC(Delta, ZSTD(3)),
    timestamp UInt64 CODEC(Delta, ZSTD(3)),
    datetime DateTime64(3, 'UTC') CODEC(Delta, ZSTD(3)),
    source_chain LowCardinality(String) CODEC(ZSTD(3))
)
ENGINE = MergeTree()
PARTITION BY toYYYYMM(datetime)
ORDER BY (height)
SETTINGS
    index_granularity = 8192,
    enable_mixed_granularity_parts = 1,
    min_rows_for_wide_part = 1000000,
    min_bytes_for_wide_part = 10000000,
    min_rows_for_compact_part = 10000,
    write_final_mark = 0,
    optimize_on_insert = 1,
    max_partitions_per_insert_block = 1000;
