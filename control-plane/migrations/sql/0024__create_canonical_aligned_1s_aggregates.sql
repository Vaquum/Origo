CREATE TABLE IF NOT EXISTS {{DATABASE}}.canonical_aligned_1s_aggregates (
    view_id String,
    view_version UInt32,
    source_id LowCardinality(String),
    stream_id LowCardinality(String),
    partition_id String,
    aligned_at_utc DateTime64(3, 'UTC'),
    bucket_event_count UInt32,
    first_event_id UUID,
    last_event_id UUID,
    first_source_offset_or_equivalent String,
    last_source_offset_or_equivalent String,
    latest_source_event_time_utc Nullable(DateTime64(9, 'UTC')),
    latest_ingested_at_utc DateTime64(3, 'UTC'),
    payload_rows_json String,
    bucket_sha256 FixedString(64),
    projector_id String,
    projected_at_utc DateTime64(3, 'UTC')
)
ENGINE = MergeTree()
PARTITION BY toYYYYMM(aligned_at_utc)
ORDER BY (
    view_id,
    view_version,
    source_id,
    stream_id,
    partition_id,
    aligned_at_utc
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
