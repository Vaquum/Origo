CREATE TABLE IF NOT EXISTS {{DATABASE}}.canonical_backfill_partition_proofs (
    source_id LowCardinality(String),
    stream_id LowCardinality(String),
    partition_id String,
    proof_revision UInt64,
    proof_id UUID,
    state LowCardinality(String),
    reason String,
    offset_ordering LowCardinality(String),
    source_row_count UInt64,
    canonical_row_count UInt64,
    canonical_unique_offset_count UInt64,
    first_offset_or_equivalent Nullable(String),
    last_offset_or_equivalent Nullable(String),
    source_offset_digest_sha256 FixedString(64),
    source_identity_digest_sha256 FixedString(64),
    canonical_offset_digest_sha256 FixedString(64),
    canonical_identity_digest_sha256 FixedString(64),
    gap_count UInt64,
    duplicate_count UInt64,
    proof_digest_sha256 FixedString(64),
    proof_details_json String,
    recorded_by_run_id String,
    recorded_at_utc DateTime64(3, 'UTC')
)
ENGINE = MergeTree()
PARTITION BY toYYYYMM(recorded_at_utc)
ORDER BY (
    source_id,
    stream_id,
    partition_id,
    proof_revision
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
