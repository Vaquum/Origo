CREATE TABLE IF NOT EXISTS {{DATABASE}}.bitcoin_block_transactions (
    block_height UInt32 CODEC(Delta(4), ZSTD(3)),
    block_hash String CODEC(ZSTD(3)),
    block_timestamp UInt64 CODEC(Delta, ZSTD(3)),
    transaction_index UInt32 CODEC(Delta(4), ZSTD(3)),
    txid String CODEC(ZSTD(3)),
    inputs String CODEC(ZSTD(3)),
    outputs String CODEC(ZSTD(3)),
    values String CODEC(ZSTD(3)),
    scripts String CODEC(ZSTD(3)),
    witness_data String CODEC(ZSTD(3)),
    coinbase UInt8 CODEC(T64, ZSTD(3)),
    datetime DateTime64(3, 'UTC') CODEC(Delta, ZSTD(3)),
    source_chain LowCardinality(String) CODEC(ZSTD(3))
)
ENGINE = MergeTree()
PARTITION BY toYYYYMM(datetime)
ORDER BY (block_height, transaction_index)
SETTINGS
    index_granularity = 8192,
    enable_mixed_granularity_parts = 1,
    min_rows_for_wide_part = 1000000,
    min_bytes_for_wide_part = 10000000,
    min_rows_for_compact_part = 10000,
    write_final_mark = 0,
    optimize_on_insert = 1,
    max_partitions_per_insert_block = 1000;
