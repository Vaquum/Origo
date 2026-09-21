CREATE OR REPLACE VIEW origo.source_current_partitions AS
            WITH eligible AS (
                SELECT a.* FROM origo.source_active_partitions a
                LEFT JOIN (
                    SELECT source_key, groupArray((partition_start, partition_end)) AS intervals
                    FROM origo.source_active_partitions WHERE NOT provisional GROUP BY source_key
                ) c ON a.source_key=c.source_key
                WHERE NOT a.provisional OR NOT arrayExists(
                    interval -> interval.1<=a.partition_start AND interval.2>a.partition_start, c.intervals)
            ), ranked AS (
                SELECT e.*, anchor,
                    max(partition_end) OVER (PARTITION BY e.source_key ORDER BY partition_start, partition_end
                        ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING) AS prior_end
                FROM eligible e INNER JOIN origo.source_anchor_log n ON e.source_key=n.source_key
            ), frontiers AS (
                SELECT source_key,
                    if(countIf(partition_start>greatest(prior_end, anchor))=0,
                       max(partition_end), minIf(partition_start, partition_start>greatest(prior_end, anchor))) AS frontier
                FROM ranked GROUP BY source_key
            )
            SELECT e.* FROM eligible e INNER JOIN frontiers f ON e.source_key=f.source_key
            WHERE NOT e.provisional OR e.partition_end<=f.frontier
