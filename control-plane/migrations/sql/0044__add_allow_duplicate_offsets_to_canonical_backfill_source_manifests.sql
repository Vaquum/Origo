ALTER TABLE {{DATABASE}}.canonical_backfill_source_manifests
ADD COLUMN IF NOT EXISTS allow_duplicate_offsets Bool
AFTER allow_empty_partition;
