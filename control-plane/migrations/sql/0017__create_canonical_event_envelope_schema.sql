CREATE TABLE IF NOT EXISTS {{DATABASE}}.canonical_event_envelope_schema (
    envelope_version UInt16,
    field_position UInt16,
    field_name LowCardinality(String),
    logical_type LowCardinality(String),
    clickhouse_type String,
    nullable UInt8,
    is_identity_key UInt8,
    description String,
    created_at_utc DateTime64(3, 'UTC') DEFAULT now64(3)
)
ENGINE = MergeTree()
ORDER BY (envelope_version, field_position)
SETTINGS
    index_granularity = 8192,
    enable_mixed_granularity_parts = 1,
    min_rows_for_wide_part = 1000000,
    min_bytes_for_wide_part = 10000000,
    min_rows_for_compact_part = 10000,
    write_final_mark = 0,
    optimize_on_insert = 1;
