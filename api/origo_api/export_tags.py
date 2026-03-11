from __future__ import annotations

from typing import cast

from .schemas import (
    ExportFormat,
    RawExportMode,
    RawQueryDataset,
    RightsState,
)


def read_export_tags(
    tags: dict[str, str],
) -> tuple[
    RawExportMode,
    ExportFormat,
    RawQueryDataset,
    str,
    RightsState,
    bool,
    str | None,
    int | None,
]:
    mode = tags.get('origo.export.mode')
    export_format = tags.get('origo.export.format')
    dataset = tags.get('origo.export.dataset')
    source = tags.get('origo.export.source')
    rights_state = tags.get('origo.export.rights_state')
    rights_provisional_raw = tags.get('origo.export.rights_provisional')

    if mode not in {'native', 'aligned_1s'}:
        raise RuntimeError(f'Unsupported or missing export mode tag: {mode}')
    if export_format not in {'parquet', 'csv'}:
        raise RuntimeError(f'Unsupported or missing export format tag: {export_format}')
    if dataset not in {
        'spot_trades',
        'spot_agg_trades',
        'futures_trades',
        'okx_spot_trades',
        'bybit_spot_trades',
        'etf_daily_metrics',
        'fred_series_metrics',
        'bitcoin_block_headers',
        'bitcoin_block_transactions',
        'bitcoin_mempool_state',
        'bitcoin_block_fee_totals',
        'bitcoin_block_subsidy_schedule',
        'bitcoin_network_hashrate_estimate',
        'bitcoin_circulating_supply',
    }:
        raise RuntimeError(f'Unsupported or missing export dataset tag: {dataset}')
    if source is None or source.strip() == '':
        raise RuntimeError('Missing export source tag: origo.export.source')
    if rights_state not in {'Hosted Allowed', 'BYOK Required', 'Ingest Only'}:
        raise RuntimeError(
            f'Unsupported or missing export rights_state tag: {rights_state}'
        )
    if rights_provisional_raw not in {'true', 'false'}:
        raise RuntimeError(
            'Unsupported or missing export rights_provisional tag: '
            f'{rights_provisional_raw}'
        )
    rights_provisional = rights_provisional_raw == 'true'
    view_id = tags.get('origo.export.view_id')
    view_version_token = tags.get('origo.export.view_version')
    if view_id is None and view_version_token is None:
        parsed_view_id = None
        parsed_view_version = None
    else:
        if view_id is None or view_version_token is None:
            raise RuntimeError(
                'Export view metadata tags must include both '
                'origo.export.view_id and origo.export.view_version when set'
            )
        if view_id.strip() == '':
            raise RuntimeError('Export view_id tag must be non-empty when set')
        try:
            parsed_view_version = int(view_version_token)
        except ValueError as exc:
            raise RuntimeError(
                'Export view_version tag must be an integer when set'
            ) from exc
        if parsed_view_version <= 0:
            raise RuntimeError('Export view_version tag must be > 0 when set')
        parsed_view_id = view_id
    return (
        cast(RawExportMode, mode),
        cast(ExportFormat, export_format),
        cast(RawQueryDataset, dataset),
        source,
        cast(RightsState, rights_state),
        rights_provisional,
        parsed_view_id,
        parsed_view_version,
    )
