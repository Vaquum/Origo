from __future__ import annotations

import importlib
import io
import json
from collections.abc import Callable
from dataclasses import replace
from typing import cast

from dagster import get_dagster_logger

from origo.assets.create_origo_database import ClickHouseSettings, get_clickhouse_settings

from ..adapters import binance_daily
from ..archive import verified_archive
from ..arrow_types import ArrowCompute, ArrowCSV, ArrowModule, ArrowTable
from ..cleanup import preserve_primary_failure
from ..columnar import binary_hash, insert_arrow
from ..contracts import Client, SourceError, StateRecord, identifier
from ..hashing import content_hash
from ..storage import ordered_component_rows
from .spot import SPOT_COMPONENTS

pa = cast(ArrowModule, importlib.import_module('pyarrow'))
pc = cast(ArrowCompute, importlib.import_module('pyarrow.compute'))
csv = cast(ArrowCSV, importlib.import_module('pyarrow.csv'))

_LEGACY = {
    'raw': ('binance_daily_spot_trades', 'create_binance_trades_table_origo', '_create_raw_table'),
    'time': (
        'binance_spot_klines',
        'create_binance_spot_klines_table_origo',
        '_create_klines_table',
    ),
    'dollar': (
        'binance_spot_dollar_klines',
        'create_binance_spot_dollar_klines_table_origo',
        '_create_dollar_klines_table',
    ),
    'volume': (
        'binance_spot_volume_klines',
        'create_binance_spot_volume_klines_table_origo',
        '_create_volume_klines_table',
    ),
    'tick': (
        'binance_spot_tick_klines',
        'create_binance_spot_tick_klines_table_origo',
        '_create_tick_klines_table',
    ),
    'imbalance': (
        'binance_spot_dollar_imbalance_klines',
        'create_binance_spot_dollar_imbalance_klines_table_origo',
        '_create_dollar_imbalance_klines_table',
    ),
    'aligned': (
        'aligned_1m_exchange',
        'create_aligned_1m_exchange_table_origo',
        '_create_aligned_table',
    ),
}


def verify_spot_legacy(client: Client, database: str, record: StateRecord) -> dict[str, object]:
    """Compare one retained spot generation with independent legacy archive execution."""
    database = identifier(database)
    day = record.partition.key
    evidence_rows = client.execute(
        f"""SELECT evidence_json FROM {database}.source_component_log
        WHERE source_key='binance_spot_trades' AND partition_key=%(day)s
          AND build_id=%(build)s AND component='raw' AND revision=%(revision)s""",
        {'day': day, 'build': record.build_id, 'revision': record.revision},
    )
    if len(evidence_rows) != 1:
        raise SourceError(
            'PARITY_INPUT_MISSING', 'The retained generation has no unique archive evidence.'
        )
    evidence: object = json.loads(str(evidence_rows[0][0]))
    if not isinstance(evidence, dict):
        raise SourceError('PARITY_INPUT_INVALID', 'Archive evidence must be an object.')
    url = cast(dict[str, object], evidence).get('object_url')
    if not isinstance(url, str):
        raise SourceError('PARITY_INPUT_INVALID', 'Archive evidence has no object URL.')
    # Independently decode exactly the retained official ZIP revision.
    archive = verified_archive(
        url,
        record.revision,
        lambda address: binance_daily.get_response(address).body,
        code='PARITY_REVISION_CHANGED',
    )
    raw = importlib.import_module('origo.assets.daily_trades_to_origo')
    extract = cast(Callable[[bytes], tuple[str, bytes]], raw._extract_csv)
    _, csv_body = extract(archive)
    reference = identifier(f'{database}_source_parity_binance_spot_trades_{record.build_id.hex}')
    settings = replace(get_clickhouse_settings(), database=reference)
    checks: dict[str, object] = {}
    # The runtime holds this partition's exclusive lock, so a prior workspace
    # for this exact build can only belong to an interrupted verification.
    client.execute(f'DROP DATABASE IF EXISTS {reference} SYNC')
    client.execute(f'CREATE DATABASE {reference}')
    with preserve_primary_failure(
        'legacy comparison database', lambda: client.execute(f'DROP DATABASE {reference} SYNC')
    ):
        for _table, module_name, function in _LEGACY.values():
            module = importlib.import_module('origo.assets.' + module_name)
            create = cast(Callable[[Client, ClickHouseSettings], None], getattr(module, function))
            create(client, settings)
        insert_arrow(f'{reference}.binance_daily_spot_trades', _reference_table(csv_body))
        for component, (table, _module, _create) in _LEGACY.items():
            if component == 'raw':
                continue
            module_name = (
                'refresh_aligned_1m_exchange_from_binance_spot_origo'
                if component == 'aligned'
                else f'refresh_{table}_origo'
            )
            module = importlib.import_module('origo.assets.' + module_name)
            if component == 'imbalance':
                calculate_arrow = cast(
                    Callable[[ClickHouseSettings, str], None], module._insert_partition_rows
                )
                calculate_arrow(settings, day)
            else:
                calculate = cast(Callable[[Client, str, str], None], module._insert_partition_rows)
                calculate(client, reference, day)
        for component in (item for item in SPOT_COMPONENTS if not item.provisional):
            table = _LEGACY[component.key][0]
            legacy_schema = client.execute(f'DESCRIBE TABLE {reference}.{table}')
            expected_schema = [(column.name, column.sql_type) for column in component.columns]
            if [(row[0], row[1]) for row in legacy_schema] != expected_schema:
                raise SourceError(
                    'LEGACY_SCHEMA_MISMATCH', f'Legacy schema differs: {component.key}.'
                )
            legacy_count = client.execute(f'SELECT count() FROM {reference}.{table}')[0][0]
            actual_hash = dict(record.component_hashes)[component.key]
            legacy_hash = (
                binary_hash(client, component, f'{reference}.{table}')
                if actual_hash.startswith('v2:')
                else content_hash(
                    ordered_component_rows(client.execute, component, f'{reference}.{table}'),
                    schema_version=1,
                )
            )
            # The runtime validates the retained rows against these immutable hashes
            # after the independent legacy computation, under the source heavy lock.
            actual_hash = dict(record.component_hashes)[component.key]
            evidence = client.execute(
                f"""SELECT row_count FROM {database}.source_component_log
                WHERE source_key='binance_spot_trades' AND partition_key=%(day)s
                AND build_id=%(build)s AND revision=%(revision)s AND component=%(component)s""",
                {
                    'day': day,
                    'build': record.build_id,
                    'revision': record.revision,
                    'component': component.key,
                },
            )
            if len(evidence) != 1:
                raise SourceError('PARITY_INPUT_MISSING', 'Component evidence must be unique.')
            actual_count = evidence[0][0]
            get_dagster_logger('origo.sources').info(
                'source=binance_spot_trades partition=%s component=%s phase=legacy_comparison '
                'legacy_rows=%s current_rows=%s legacy_hash=%s current_hash=%s',
                day,
                component.key,
                legacy_count,
                actual_count,
                legacy_hash,
                actual_hash,
            )
            if (actual_count, actual_hash) != (legacy_count, legacy_hash):
                raise SourceError(
                    'LEGACY_PARITY_MISMATCH', f'Legacy output differs: {component.key}.'
                )
            checks[component.key] = {'row_count': actual_count, 'sha256': actual_hash}
        return checks


def _reference_table(body: bytes) -> ArrowTable:
    # Independent Arrow CSV decoder; the production adapter uses validated Polars expressions.
    names = [
        'trade_id',
        'price',
        'quantity',
        'quote_quantity',
        'timestamp',
        'is_buyer_maker',
        'is_best_match',
    ]
    table = csv.read_csv(
        io.BytesIO(body),
        read_options=csv.ReadOptions(column_names=names),
        convert_options=csv.ConvertOptions(
            column_types={
                'trade_id': pa.uint64(),
                'price': pa.float64(),
                'quantity': pa.float64(),
                'quote_quantity': pa.float64(),
                'timestamp': pa.uint64(),
                'is_buyer_maker': pa.bool_(),
                'is_best_match': pa.bool_(),
            },
            true_values=['True', 'true'],
            false_values=['False', 'false'],
        ),
    )
    stamp = table['timestamp']
    micros = pc.if_else(pc.less(stamp, 10**13), pc.multiply(stamp, 1000), stamp)
    table = table.set_column(5, names[5], pc.cast(table[5], pa.uint8()))
    table = table.set_column(6, names[6], pc.cast(table[6], pa.uint8()))
    return table.append_column(
        'datetime', pc.cast(pc.cast(micros, pa.int64()), pa.timestamp('us', tz='UTC'))
    )
