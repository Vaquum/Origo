CREATE TABLE IF NOT EXISTS {{DATABASE}}.canonical_etf_daily_metrics_native_v1 (
    metric_id String,
    source_id LowCardinality(String),
    metric_name LowCardinality(String),
    metric_unit Nullable(String),
    metric_value_string Nullable(String),
    metric_value_int Nullable(Int64),
    metric_value_float Nullable(Float64),
    metric_value_bool Nullable(UInt8),
    observed_at_utc DateTime64(3, 'UTC'),
    dimensions_json String,
    provenance_json Nullable(String),
    ingested_at_utc DateTime64(3, 'UTC'),
    event_id UUID,
    source_offset_or_equivalent String,
    source_event_time_utc Nullable(DateTime64(9, 'UTC'))
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(observed_at_utc)
ORDER BY (
    source_id,
    observed_at_utc,
    metric_name,
    metric_id,
    event_id
);
