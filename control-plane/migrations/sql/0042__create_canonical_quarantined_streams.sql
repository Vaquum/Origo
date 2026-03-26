CREATE TABLE IF NOT EXISTS {{DATABASE}}.canonical_quarantined_streams (
    source_id LowCardinality(String),
    stream_id LowCardinality(String),
    partition_id String,
    quarantine_revision UInt64,
    status LowCardinality(String),
    reason String,
    details_json String,
    recorded_by_run_id String,
    recorded_at_utc DateTime64(3, 'UTC')
)
ENGINE = MergeTree()
PARTITION BY toYYYYMM(recorded_at_utc)
ORDER BY (
    source_id,
    stream_id,
    partition_id,
    quarantine_revision
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
