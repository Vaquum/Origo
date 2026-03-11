CREATE TABLE IF NOT EXISTS {{DATABASE}}.canonical_ingest_cursor_state (
    source_id LowCardinality(String),
    stream_id LowCardinality(String),
    partition_id String,
    cursor_revision UInt64,
    last_source_offset_or_equivalent String,
    last_event_id UUID,
    last_source_event_time_utc Nullable(DateTime64(9, 'UTC')),
    last_ingested_at_utc DateTime64(3, 'UTC'),
    offset_ordering LowCardinality(String),
    updated_by_run_id String,
    updated_at_utc DateTime64(3, 'UTC')
)
ENGINE = MergeTree()
PARTITION BY toYYYYMM(updated_at_utc)
ORDER BY (
    source_id,
    stream_id,
    partition_id,
    cursor_revision
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
