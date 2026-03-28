from __future__ import annotations

from collections import defaultdict
from typing import Any, cast

from clickhouse_driver import Client as ClickHouseClient


def load_nonterminal_partition_ids_for_stream_or_raise(
    *,
    client: ClickHouseClient,
    database: str,
    source_id: str,
    stream_id: str,
    terminal_states: tuple[str, ...],
) -> tuple[str, ...]:
    rows = cast(
        list[tuple[Any, ...]],
        client.execute(
            f'''
            SELECT
                candidate_partitions.partition_id
            FROM
            (
                SELECT DISTINCT partition_id
                FROM
                (
                    SELECT partition_id
                    FROM {database}.canonical_event_log
                    WHERE source_id = %(source_id)s
                      AND stream_id = %(stream_id)s
                    UNION DISTINCT
                    SELECT partition_id
                    FROM {database}.canonical_backfill_source_manifests
                    WHERE source_id = %(source_id)s
                      AND stream_id = %(stream_id)s
                    UNION DISTINCT
                    SELECT partition_id
                    FROM {database}.canonical_backfill_partition_proofs
                    WHERE source_id = %(source_id)s
                      AND stream_id = %(stream_id)s
                )
            ) AS candidate_partitions
            LEFT JOIN
            (
                SELECT
                    partition_id,
                    argMax(state, proof_revision) AS state
                FROM {database}.canonical_backfill_partition_proofs
                WHERE source_id = %(source_id)s
                  AND stream_id = %(stream_id)s
                GROUP BY partition_id
            ) AS proofs
            ON candidate_partitions.partition_id = proofs.partition_id
            WHERE proofs.state IS NULL OR proofs.state NOT IN %(terminal_states)s
            ORDER BY candidate_partitions.partition_id ASC
            ''',
            {
                'source_id': source_id,
                'stream_id': stream_id,
                'terminal_states': terminal_states,
            },
        ),
    )
    return tuple(str(row[0]) for row in rows)


def load_grouped_nonterminal_partition_ids_or_raise(
    *,
    client: ClickHouseClient,
    database: str,
    stream_pair_filter_sql: str,
    terminal_states: tuple[str, ...],
) -> dict[tuple[str, str], list[str]]:
    rows = cast(
        list[tuple[Any, ...]],
        client.execute(
            f'''
            SELECT
                candidate_partitions.source_id,
                candidate_partitions.stream_id,
                candidate_partitions.partition_id
            FROM
            (
                SELECT DISTINCT source_id, stream_id, partition_id
                FROM
                (
                    SELECT source_id, stream_id, partition_id
                    FROM {database}.canonical_event_log
                    WHERE {stream_pair_filter_sql}
                    UNION DISTINCT
                    SELECT source_id, stream_id, partition_id
                    FROM {database}.canonical_backfill_source_manifests
                    WHERE {stream_pair_filter_sql}
                    UNION DISTINCT
                    SELECT source_id, stream_id, partition_id
                    FROM {database}.canonical_backfill_partition_proofs
                    WHERE {stream_pair_filter_sql}
                )
            ) AS candidate_partitions
            LEFT JOIN
            (
                SELECT
                    source_id,
                    stream_id,
                    partition_id,
                    argMax(state, proof_revision) AS state
                FROM {database}.canonical_backfill_partition_proofs
                WHERE {stream_pair_filter_sql}
                GROUP BY source_id, stream_id, partition_id
            ) AS proofs
            ON candidate_partitions.source_id = proofs.source_id
            AND candidate_partitions.stream_id = proofs.stream_id
            AND candidate_partitions.partition_id = proofs.partition_id
            WHERE proofs.state IS NULL OR proofs.state NOT IN %(terminal_states)s
            ORDER BY
                candidate_partitions.source_id,
                candidate_partitions.stream_id,
                candidate_partitions.partition_id
            ''',
            {'terminal_states': terminal_states},
        ),
    )
    grouped: dict[tuple[str, str], list[str]] = defaultdict(list)
    for row in rows:
        grouped[(str(row[0]), str(row[1]))].append(str(row[2]))
    return dict(grouped)
