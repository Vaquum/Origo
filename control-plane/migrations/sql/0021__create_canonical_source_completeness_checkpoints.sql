CREATE TABLE IF NOT EXISTS {{DATABASE}}.canonical_source_completeness_checkpoints (
    source_id LowCardinality(String),
    stream_id LowCardinality(String),
    partition_id String,
    checkpoint_revision UInt64,
    checkpoint_id UUID,
    offset_ordering LowCardinality(String),
    check_scope_start_offset Nullable(String),
    check_scope_end_offset Nullable(String),
    last_checked_source_offset_or_equivalent String,
    expected_event_count UInt64,
    observed_event_count UInt64,
    gap_count UInt64,
    status LowCardinality(String),
    gap_details_json String,
    checked_by_run_id String,
    checked_at_utc DateTime64(3, 'UTC')
)
ENGINE = MergeTree()
PARTITION BY toYYYYMM(checked_at_utc)
ORDER BY (
    source_id,
    stream_id,
    partition_id,
    checkpoint_revision
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
