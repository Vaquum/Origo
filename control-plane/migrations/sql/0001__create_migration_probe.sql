CREATE TABLE IF NOT EXISTS {{DATABASE}}.origo_migration_probe (
    id UInt8,
    created_at DateTime DEFAULT now()
)
ENGINE = MergeTree()
ORDER BY (id);

