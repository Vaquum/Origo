CREATE TABLE IF NOT EXISTS {{DATABASE}}.canonical_aligned_projection_policies (
    view_id String,
    view_version UInt32,
    source_id LowCardinality(String),
    stream_id LowCardinality(String),
    partition_id String,
    policy_revision UInt64,
    policy_id UUID,
    bucket_size_seconds UInt16,
    tier_policy LowCardinality(String),
    retention_hot_days UInt16,
    retention_warm_days UInt16,
    recorded_by_run_id String,
    recorded_at_utc DateTime64(3, 'UTC')
)
ENGINE = MergeTree()
PARTITION BY toYYYYMM(recorded_at_utc)
ORDER BY (
    view_id,
    view_version,
    source_id,
    stream_id,
    partition_id,
    policy_revision
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
