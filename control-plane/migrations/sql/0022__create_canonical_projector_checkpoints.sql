CREATE TABLE IF NOT EXISTS {{DATABASE}}.canonical_projector_checkpoints (
    projector_id String,
    source_id LowCardinality(String),
    stream_id LowCardinality(String),
    partition_id String,
    checkpoint_revision UInt64,
    last_event_id UUID,
    last_source_offset_or_equivalent String,
    last_source_event_time_utc Nullable(DateTime64(9, 'UTC')),
    last_ingested_at_utc DateTime64(3, 'UTC'),
    run_id String,
    state_json String,
    checkpointed_at_utc DateTime64(3, 'UTC')
)
ENGINE = MergeTree()
PARTITION BY toYYYYMM(checkpointed_at_utc)
ORDER BY (
    projector_id,
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
