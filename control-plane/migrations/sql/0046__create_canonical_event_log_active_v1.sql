CREATE VIEW IF NOT EXISTS {{DATABASE}}.canonical_event_log_active_v1 AS
SELECT event_log.*
FROM {{DATABASE}}.canonical_event_log AS event_log
LEFT JOIN
(
    SELECT
        source_id,
        stream_id,
        partition_id,
        max(recorded_at_utc) AS latest_reset_at_utc
    FROM {{DATABASE}}.canonical_partition_reset_boundaries
    GROUP BY
        source_id,
        stream_id,
        partition_id
) AS reset_boundaries
ON event_log.source_id = reset_boundaries.source_id
AND event_log.stream_id = reset_boundaries.stream_id
AND event_log.partition_id = reset_boundaries.partition_id
WHERE
    reset_boundaries.latest_reset_at_utc IS NULL
    OR event_log.ingested_at_utc > reset_boundaries.latest_reset_at_utc;
