from __future__ import annotations

import pytest

from api.origo_api.export_tags import read_export_tags


def test_read_export_tags_requires_rights_metadata_fields() -> None:
    with pytest.raises(RuntimeError, match='rights_state'):
        read_export_tags(
            {
                'origo.export.mode': 'native',
                'origo.export.format': 'parquet',
                'origo.export.dataset': 'binance_spot_trades',
                'origo.export.source': 'binance',
                'origo.export.rights_provisional': 'false',
            }
        )

    with pytest.raises(RuntimeError, match='rights_provisional'):
        read_export_tags(
            {
                'origo.export.mode': 'native',
                'origo.export.format': 'parquet',
                'origo.export.dataset': 'binance_spot_trades',
                'origo.export.source': 'binance',
                'origo.export.rights_state': 'Hosted Allowed',
            }
        )


def test_read_export_tags_accepts_rights_metadata_fields() -> None:
    (
        mode,
        export_format,
        dataset,
        source,
        rights_state,
        rights_provisional,
        view_id,
        view_version,
    ) = read_export_tags(
        {
            'origo.export.mode': 'native',
            'origo.export.format': 'parquet',
            'origo.export.dataset': 'binance_spot_trades',
            'origo.export.source': 'binance',
            'origo.export.rights_state': 'Hosted Allowed',
            'origo.export.rights_provisional': 'false',
        }
    )
    assert mode == 'native'
    assert export_format == 'parquet'
    assert dataset == 'binance_spot_trades'
    assert source == 'binance'
    assert rights_state == 'Hosted Allowed'
    assert rights_provisional is False
    assert view_id is None
    assert view_version is None
