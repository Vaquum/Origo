"""Native retention of an explicit set of ClickHouse system logs."""

import os
import re
import time
from dataclasses import dataclass
from pathlib import Path
from typing import cast

from origo.sources.contracts import Client, Row

from .protocol import OperationalMetadataMaintenanceConfig
from .sqlite import allocated

DIAGNOSTIC_LOGS = (
    'text_log',
    'query_log',
    'trace_log',
    'part_log',
    'metric_log',
    'asynchronous_metric_log',
    'processors_profile_log',
    'latency_log',
    'query_metric_log',
    'error_log',
    'query_thread_log',
    'query_views_log',
    'asynchronous_insert_log',
    'opentelemetry_span_log',
    'crash_log',
    'blob_storage_log',
    's3queue_log',
    'backup_log',
)


def diagnostic_root(table: str) -> str | None:
    return next(
        (
            root
            for root in DIAGNOSTIC_LOGS
            if table == root or re.fullmatch(re.escape(root) + r'_[0-9]+', table)
        ),
        None,
    )


def timestamp_expression(root: str) -> str:
    return (
        'toDateTime(intDiv(finish_time_us, 1000000))'
        if root == 'opentelemetry_span_log'
        else 'event_time'
    )


def ttl_expression(root: str) -> str:
    return timestamp_expression(root) + ' + INTERVAL 14 DAY DELETE'


@dataclass(frozen=True)
class DiagnosticInventory:
    active_bytes: int
    inactive_bytes: int
    entirely_expired_bytes: int
    oldest_date: str
    tables: tuple[str, ...]
    drift: tuple[str, ...]
    errors: tuple[str, ...]
    pending: tuple[str, ...]
    scheduled_action: str = ''


def _execute(
    client: Client, query: str, deadline: float, params: object | None = None
) -> list[Row]:
    remaining = deadline - time.monotonic()
    if remaining <= 0:
        raise TimeoutError('ClickHouse maintenance deadline reached.')
    return client.execute(
        query,
        params,
        settings={
            'max_execution_time': max(1, min(10, int(remaining))),
            'max_threads': 1,
            'use_query_cache': 0,
            'materialize_ttl_after_modify': 0,
        },
    )


def maintain_diagnostics(
    client: Client, config: OperationalMetadataMaintenanceConfig, deadline: float
) -> DiagnosticInventory:
    version = _execute(client, 'SELECT version()', deadline)
    if version != [('25.3.2.39',)]:
        raise RuntimeError(f'Diagnostic retention has not been validated on {version}.')
    rows = _execute(
        client,
        "SELECT name,engine,create_table_query FROM system.tables WHERE database='system' AND engine LIKE '%MergeTree' ORDER BY name LIMIT 501",
        deadline,
    )
    if len(rows) > 500:
        raise RuntimeError('System table inventory exceeds the validated bound.')
    tables: list[str] = []
    drift: list[str] = []
    errors: list[str] = []
    pending: list[str] = []
    for row in rows:
        table, engine, ddl = (str(value) for value in row)
        root = diagnostic_root(table)
        if root is None:
            unknown = _execute(
                client,
                "SELECT sum(bytes_on_disk) FROM system.parts WHERE database='system' AND table=%(table)s",
                deadline,
                {'table': table},
            )
            if int(str(unknown[0][0])):
                errors.append(f'unmanaged_system_table:{table}')
            continue
        if engine != 'MergeTree':
            raise RuntimeError(f'Unexpected diagnostic engine: system.{table} {engine}')
        column = 'finish_time_us' if root == 'opentelemetry_span_log' else 'event_time'
        columns = _execute(
            client,
            "SELECT name,type FROM system.columns WHERE database='system' AND table=%(table)s AND name IN (%(timestamp)s,'hostname','span_id')",
            deadline,
            {'table': table, 'timestamp': column},
        )
        schema = {str(name): str(kind) for name, kind in columns}
        identity_valid = (
            schema.get('span_id') == 'UInt64'
            if root == 'opentelemetry_span_log'
            else schema.get('hostname') in ('String', 'LowCardinality(String)')
        )
        timestamp_valid = (
            schema.get(column) == 'UInt64'
            if root == 'opentelemetry_span_log'
            else schema.get(column, '').startswith('DateTime')
        )
        if not identity_valid or not timestamp_valid:
            raise RuntimeError(f'Unrecognized diagnostic schema: system.{table}: {schema}')
        tables.append(table)
        unquoted = re.sub(r"'(?:[^'\\]|\\.)*'|`[^`]*`", '', ddl)
        ttl_line = re.search(r'\bTTL\s+(.*?)(?:\s+SETTINGS\b|\s+COMMENT\b|$)', unquoted, re.DOTALL)
        expected = re.sub(r'\s+', '', timestamp_expression(root)) + '+toIntervalDay(14)'
        ttl = re.sub(r'\s+', '', ttl_line.group(1)) if ttl_line else ''
        if ttl not in (expected, expected + 'DELETE'):
            if config.dry_run:
                drift.append(table)
            else:
                _execute(
                    client,
                    f'ALTER TABLE system.{table} MODIFY TTL {ttl_expression(root)}',
                    deadline,
                )
                # TTL metadata backfill is controlled separately, one bounded operation at a time.
                _execute(
                    client,
                    f'ALTER TABLE system.{table} MODIFY SETTING merge_with_ttl_timeout=3600',
                    deadline,
                )
    present = {root for table in tables if (root := diagnostic_root(table)) is not None}
    missing = set[str](DIAGNOSTIC_LOGS) - present
    errors.extend(f'enabled_diagnostic_log_missing:{root}' for root in sorted(missing))
    if not tables:
        raise RuntimeError('No managed diagnostic logs were found.')
    table_parameters = {'tables': tuple(tables)}
    parts = _execute(
        client,
        "SELECT table,name,partition_id,active,bytes_on_disk,toString(min_date),toString(max_date),max_date<today()-14 FROM system.parts WHERE database='system' AND table IN %(tables)s ORDER BY min_date,table,name LIMIT 5001",
        deadline,
        table_parameters,
    )
    if len(parts) > 5000:
        raise RuntimeError('Diagnostic part inventory exceeds 5000; maintenance must be paginated.')
    mutations = _execute(
        client,
        "SELECT table,mutation_id,latest_fail_reason FROM system.mutations WHERE database='system' AND table IN %(tables)s AND NOT is_done LIMIT 101",
        deadline,
        table_parameters,
    )
    for table, mutation_id, reason in mutations:
        pending.append(f'mutation:{table}:{mutation_id}')
        if reason:
            errors.append(f'mutation_failed:{table}:{mutation_id}:{reason}')
    merges = _execute(
        client,
        "SELECT table,result_part_name FROM system.merges WHERE database='system' AND table IN %(tables)s LIMIT 101",
        deadline,
        table_parameters,
    )
    pending.extend(f'merge:{table}:{part}' for table, part in merges)
    if 'part_log' in tables:
        failures = _execute(
            client,
            'SELECT table,part_name,argMax(error,event_time_microseconds),argMax(exception,event_time_microseconds) '
            "FROM system.part_log WHERE database='system' AND table IN %(tables)s "
            'AND event_date>=today()-1 AND event_time>=now()-INTERVAL 1 DAY '
            "AND event_type IN ('MergeParts','MutatePart') GROUP BY table,part_name "
            'HAVING argMax(error,event_time_microseconds)!=0 LIMIT 101',
            deadline,
            table_parameters,
        )
        errors.extend(
            f'merge_failed:{table}:{part}:{code}:{reason}' for table, part, code, reason in failures
        )
    active_bytes = sum(int(str(row[4])) for row in parts if row[3])
    inactive_bytes = sum(int(str(row[4])) for row in parts if not row[3])
    expired_bytes = sum(int(str(row[4])) for row in parts if row[3] and row[7])
    active = [row for row in parts if row[3]]
    oldest = min((str(row[5]) for row in active), default='')
    lagging = _execute(
        client,
        "SELECT table,toString(min(min_date)) FROM system.parts WHERE database='system' AND table IN %(tables)s AND active GROUP BY table HAVING min(min_date)<toDate(now()-INTERVAL 14 DAY)-%(lag_days)s",
        deadline,
        {**table_parameters, 'lag_days': (config.diagnostic_max_lag_seconds + 86399) // 86400},
    )
    errors.extend(f'expiry_lag:{table}:{oldest_date}' for table, oldest_date in lagging)
    action = ''
    busy_tables = {str(row[0]) for row in mutations + merges}
    lagging_tables = {str(row[0]) for row in lagging}
    for part in active:
        if config.dry_run or str(part[0]) in busy_tables or time.monotonic() >= deadline - 5:
            continue
        if not part[7] and str(part[0]) not in lagging_tables:
            continue
        table, name, partition_id = str(part[0]), str(part[1]), str(part[2])
        root = diagnostic_root(table)
        if (
            root is None
            or not re.fullmatch(r'[A-Za-z0-9_-]+', name)
            or not re.fullmatch(r'[A-Za-z0-9_-]+', partition_id)
        ):
            raise ValueError('Invalid diagnostic part identity.')
        timestamp = timestamp_expression(root)
        bounds = _execute(
            client,
            f'SELECT count(),max(toUnixTimestamp({timestamp})),toUnixTimestamp(now()-INTERVAL 14 DAY) FROM system.{table} WHERE _part=%(part)s SETTINGS max_bytes_to_read={config.diagnostic_max_partition_bytes}',
            deadline,
            {'part': name},
        )
        count, maximum, cutoff = (int(str(value)) for value in bounds[0])
        if count and maximum < cutoff:
            _execute(client, f"ALTER TABLE system.{table} DROP PART '{name}'", deadline)
            action += f'drop_expired_part:{table}:{name};'
        elif table in lagging_tables and not mutations:
            size = _execute(
                client,
                "SELECT sum(bytes_on_disk) FROM system.parts WHERE database='system' AND table=%(table)s AND partition_id=%(partition)s AND active",
                deadline,
                {'table': table, 'partition': partition_id},
            )
            partition_bytes = int(str(size[0][0]))
            free = int(
                str(_execute(client, 'SELECT min(free_space) FROM system.disks', deadline)[0][0])
            )
            if (
                partition_bytes > config.diagnostic_max_partition_bytes
                or free < config.diagnostic_min_free_bytes + 2 * partition_bytes
            ):
                errors.append(f'catch_up_capacity:{table}:{partition_id}:{partition_bytes}')
            else:
                _execute(
                    client,
                    f"ALTER TABLE system.{table} MATERIALIZE TTL IN PARTITION ID '{partition_id}' SETTINGS mutations_sync=0",
                    deadline,
                )
                action += f'materialize_ttl:{table}:{partition_id};'
                break
    if action:
        observed = maintain_diagnostics(
            client, config.model_copy(update={'dry_run': True}), deadline
        )
        return DiagnosticInventory(
            observed.active_bytes,
            observed.inactive_bytes,
            observed.entirely_expired_bytes,
            observed.oldest_date,
            observed.tables,
            observed.drift,
            tuple(dict.fromkeys(errors + list(observed.errors)))
            if action.startswith('materialize')
            else observed.errors,
            observed.pending,
            action,
        )
    return DiagnosticInventory(
        active_bytes,
        inactive_bytes,
        expired_bytes,
        oldest,
        tuple(tables),
        tuple(drift),
        tuple(errors),
        tuple(pending),
        action,
    )


def diagnostic_allocated_bytes(client: Client, root: Path, deadline: float) -> int:
    root = root.resolve(strict=True)
    rows = _execute(
        client,
        "SELECT name,data_paths FROM system.tables WHERE database='system' AND engine LIKE '%MergeTree' LIMIT 501",
        deadline,
    )
    if len(rows) > 500:
        raise RuntimeError('Diagnostic filesystem inventory exceeds its table bound.')
    directories: set[Path] = set()
    for _, raw_paths in rows:
        if not isinstance(raw_paths, list):
            raise TypeError('ClickHouse data_paths must be an array.')
        for raw in cast(list[object], raw_paths):
            relative = Path(str(raw)).relative_to('/var/lib/clickhouse')
            directory = (root / relative).resolve()
            if not directory.is_relative_to(root):
                raise ValueError('Diagnostic data path escapes the read-only ClickHouse mount.')
            directories.add(directory)
    total = 0
    for directory in directories:
        total += allocated(directory)
        for current, subdirectories, files in os.walk(directory, followlinks=False):
            if time.monotonic() >= deadline:
                raise TimeoutError('Diagnostic allocated-block inventory exceeded its deadline.')
            total += sum(allocated(Path(current) / name) for name in subdirectories + files)
    return total
