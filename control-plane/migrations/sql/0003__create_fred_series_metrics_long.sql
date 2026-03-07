CREATE TABLE IF NOT EXISTS {{DATABASE}}.fred_series_metrics_long (
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
    ingested_at_utc DateTime64(3, 'UTC')
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(observed_at_utc)
ORDER BY (source_id, observed_at_utc, metric_name, metric_id);
