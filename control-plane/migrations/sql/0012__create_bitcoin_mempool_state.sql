CREATE TABLE IF NOT EXISTS {{DATABASE}}.bitcoin_mempool_state (
    snapshot_at DateTime64(3, 'UTC') CODEC(Delta, ZSTD(3)),
    snapshot_at_unix_ms UInt64 CODEC(Delta, ZSTD(3)),
    txid String CODEC(ZSTD(3)),
    fee_rate_sat_vb Float64 CODEC(Delta, ZSTD(3)),
    vsize UInt64 CODEC(Delta(8), ZSTD(3)),
    first_seen_timestamp UInt64 CODEC(Delta, ZSTD(3)),
    rbf_flag UInt8 CODEC(T64, ZSTD(3)),
    source_chain LowCardinality(String) CODEC(ZSTD(3))
)
ENGINE = MergeTree()
PARTITION BY toYYYYMM(snapshot_at)
ORDER BY (snapshot_at, txid)
SETTINGS
    index_granularity = 8192,
    enable_mixed_granularity_parts = 1,
    min_rows_for_wide_part = 1000000,
    min_bytes_for_wide_part = 10000000,
    min_rows_for_compact_part = 10000,
    write_final_mark = 0,
    optimize_on_insert = 1,
    max_partitions_per_insert_block = 1000;
