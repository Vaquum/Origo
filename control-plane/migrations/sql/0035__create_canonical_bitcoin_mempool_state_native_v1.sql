CREATE TABLE IF NOT EXISTS {{DATABASE}}.canonical_bitcoin_mempool_state_native_v1 (
    snapshot_at DateTime64(3, 'UTC'),
    snapshot_at_unix_ms UInt64,
    txid String,
    fee_rate_sat_vb Float64,
    vsize UInt32,
    first_seen_timestamp UInt64,
    rbf_flag UInt8,
    source_chain LowCardinality(String),
    event_id UUID,
    source_offset_or_equivalent String,
    source_event_time_utc Nullable(DateTime64(9, 'UTC')),
    ingested_at_utc DateTime64(3, 'UTC')
)
ENGINE = MergeTree()
PARTITION BY toYYYYMM(snapshot_at)
ORDER BY (
    toStartOfDay(snapshot_at),
    snapshot_at_unix_ms,
    txid,
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
