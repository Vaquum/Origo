from __future__ import annotations

import pytest


@pytest.mark.parametrize(
    ('module_name', 'asset_name'),
    [
        ('daily_trades_to_origo', 'insert_daily_binance_trades_to_origo'),
        ('daily_okx_spot_trades_to_origo', 'insert_daily_okx_spot_trades_to_origo'),
        ('daily_bybit_spot_trades_to_origo', 'insert_daily_bybit_spot_trades_to_origo'),
    ],
)
def test_exchange_assets_declare_launchpad_runtime_config_defaults(
    monkeypatch: pytest.MonkeyPatch,
    module_name: str,
    asset_name: str,
) -> None:
    monkeypatch.setenv('CLICKHOUSE_HOST', 'clickhouse')
    monkeypatch.setenv('CLICKHOUSE_PORT', '9000')
    monkeypatch.setenv('CLICKHOUSE_USER', 'default')
    monkeypatch.setenv('CLICKHOUSE_PASSWORD', 'password')
    monkeypatch.setenv('CLICKHOUSE_DATABASE', 'origo')

    package = __import__('origo_control_plane.assets', fromlist=[module_name])
    module = getattr(package, module_name)
    asset = getattr(module, asset_name)

    field = asset.op.config_schema.as_field()
    config_type = field.config_type
    fields = config_type.fields

    assert set(fields) >= {'projection_mode', 'execution_mode', 'runtime_audit_mode'}
    assert fields['projection_mode'].default_value == 'deferred'
    assert fields['execution_mode'].default_value == 'backfill'
    assert fields['runtime_audit_mode'].default_value == 'summary'
