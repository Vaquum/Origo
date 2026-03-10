CREATE TABLE IF NOT EXISTS {{DATABASE}}.canonical_event_log (
    envelope_version UInt16,
    event_id UUID,
    source_id LowCardinality(String),
    stream_id LowCardinality(String),
    partition_id String,
    source_offset_or_equivalent String,
    source_event_time_utc Nullable(DateTime64(9, 'UTC')),
    ingested_at_utc DateTime64(3, 'UTC'),
    payload_content_type LowCardinality(String),
    payload_encoding LowCardinality(String),
    payload_raw String,
    payload_sha256_raw FixedString(64),
    payload_json String
)
ENGINE = MergeTree()
PARTITION BY toYYYYMM(ingested_at_utc)
ORDER BY (
    source_id,
    stream_id,
    partition_id,
    source_offset_or_equivalent,
    ingested_at_utc,
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
