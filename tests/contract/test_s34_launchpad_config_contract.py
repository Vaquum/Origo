from __future__ import annotations

import importlib
from typing import Any

import pytest


def _set_clickhouse_env(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv('CLICKHOUSE_HOST', 'clickhouse')
    monkeypatch.setenv('CLICKHOUSE_PORT', '9000')
    monkeypatch.setenv('CLICKHOUSE_USER', 'default')
    monkeypatch.setenv('CLICKHOUSE_PASSWORD', 'password')
    monkeypatch.setenv('CLICKHOUSE_DATABASE', 'origo')


def _assert_runtime_defaults(
    fields: dict[str, Any],
    *,
    projection_mode: str,
) -> None:
    assert fields['projection_mode'].default_value == projection_mode
    assert fields['execution_mode'].default_value == 'backfill'
    assert fields['runtime_audit_mode'].default_value == 'summary'


@pytest.mark.parametrize(
    ('module_name', 'asset_name'),
    [
        (
            'bitcoin_block_headers_to_origo',
            'insert_bitcoin_block_headers_to_origo',
        ),
        (
            'bitcoin_block_transactions_to_origo',
            'insert_bitcoin_block_transactions_to_origo',
        ),
        (
            'bitcoin_block_fee_totals_to_origo',
            'insert_bitcoin_block_fee_totals_to_origo',
        ),
        (
            'bitcoin_block_subsidy_schedule_to_origo',
            'insert_bitcoin_block_subsidy_schedule_to_origo',
        ),
        (
            'bitcoin_network_hashrate_estimate_to_origo',
            'insert_bitcoin_network_hashrate_estimate_to_origo',
        ),
        (
            'bitcoin_circulating_supply_to_origo',
            'insert_bitcoin_circulating_supply_to_origo',
        ),
    ],
)
def test_height_based_bitcoin_assets_expose_launchpad_height_window_contract(
    monkeypatch: pytest.MonkeyPatch,
    module_name: str,
    asset_name: str,
) -> None:
    _set_clickhouse_env(monkeypatch)

    package = __import__('origo_control_plane.assets', fromlist=[module_name])
    module = getattr(package, module_name)
    asset = getattr(module, asset_name)

    fields = asset.op.config_schema.as_field().config_type.fields

    assert set(fields) >= {
        'projection_mode',
        'execution_mode',
        'runtime_audit_mode',
        'height_start',
        'height_end',
    }
    _assert_runtime_defaults(fields, projection_mode='deferred')


def test_bitcoin_mempool_asset_exposes_runtime_launchpad_contract(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _set_clickhouse_env(monkeypatch)

    module = importlib.import_module(
        'origo_control_plane.assets.bitcoin_mempool_state_to_origo'
    )
    asset = module.insert_bitcoin_mempool_state_to_origo

    fields = asset.op.config_schema.as_field().config_type.fields

    assert set(fields) >= {'projection_mode', 'execution_mode', 'runtime_audit_mode'}
    _assert_runtime_defaults(fields, projection_mode='inline')


@pytest.mark.parametrize(
    ('module_name', 'op_name'),
    [
        ('etf_daily_ingest', 'origo_etf_daily_backfill_step'),
        ('fred_daily_ingest', 'origo_fred_daily_backfill_step'),
    ],
)
def test_etf_and_fred_backfill_ops_expose_manual_launchpad_window_controls(
    monkeypatch: pytest.MonkeyPatch,
    module_name: str,
    op_name: str,
) -> None:
    _set_clickhouse_env(monkeypatch)

    module = importlib.import_module(f'origo_control_plane.jobs.{module_name}')
    op = getattr(module, op_name)

    fields = op.config_schema.as_field().config_type.fields

    assert set(fields) >= {
        'projection_mode',
        'execution_mode',
        'runtime_audit_mode',
        'start_date',
        'end_date',
        'partition_ids_csv',
    }
    _assert_runtime_defaults(fields, projection_mode='deferred')
