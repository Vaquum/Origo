from __future__ import annotations

import json
from dataclasses import dataclass
from decimal import Decimal, InvalidOperation, localcontext
from typing import Any, Final, Literal, TypedDict, cast

CanonicalNumericKind = Literal['int', 'decimal']


@dataclass(frozen=True)
class CanonicalNumericFieldRule:
    field_path: str
    numeric_kind: CanonicalNumericKind
    scale: int | None
    description: str


class CanonicalNumericFieldRuleRecord(TypedDict):
    field_path: str
    numeric_kind: CanonicalNumericKind
    scale: int | None
    description: str


class CanonicalSourcePrecisionEntry(TypedDict):
    source_id: str
    stream_id: str
    numeric_fields: list[CanonicalNumericFieldRuleRecord]


class CanonicalSourcePrecisionRegistryContract(TypedDict):
    registry_version: int
    entries: list[CanonicalSourcePrecisionEntry]


CANONICAL_SOURCE_PRECISION_REGISTRY_VERSION: Final[int] = 1

_CANONICAL_SOURCE_PRECISION_RULES: Final[
    dict[tuple[str, str], tuple[CanonicalNumericFieldRule, ...]]
] = {
    (
        'binance',
        'spot_trades',
    ): (
        CanonicalNumericFieldRule(
            field_path='trade_id',
            numeric_kind='int',
            scale=None,
            description='Exchange trade identifier.',
        ),
        CanonicalNumericFieldRule(
            field_path='price',
            numeric_kind='decimal',
            scale=8,
            description='Trade execution price.',
        ),
        CanonicalNumericFieldRule(
            field_path='qty',
            numeric_kind='decimal',
            scale=8,
            description='Base-asset trade quantity.',
        ),
        CanonicalNumericFieldRule(
            field_path='quote_qty',
            numeric_kind='decimal',
            scale=8,
            description='Quote-asset trade quantity.',
        ),
    ),
    (
        'binance',
        'spot_agg_trades',
    ): (
        CanonicalNumericFieldRule(
            field_path='agg_trade_id',
            numeric_kind='int',
            scale=None,
            description='Exchange aggregate trade identifier.',
        ),
        CanonicalNumericFieldRule(
            field_path='price',
            numeric_kind='decimal',
            scale=8,
            description='Aggregate trade price.',
        ),
        CanonicalNumericFieldRule(
            field_path='qty',
            numeric_kind='decimal',
            scale=8,
            description='Aggregate trade quantity.',
        ),
        CanonicalNumericFieldRule(
            field_path='first_trade_id',
            numeric_kind='int',
            scale=None,
            description='First underlying trade identifier in aggregate trade.',
        ),
        CanonicalNumericFieldRule(
            field_path='last_trade_id',
            numeric_kind='int',
            scale=None,
            description='Last underlying trade identifier in aggregate trade.',
        ),
    ),
    (
        'binance',
        'futures_trades',
    ): (
        CanonicalNumericFieldRule(
            field_path='trade_id',
            numeric_kind='int',
            scale=None,
            description='Futures trade identifier.',
        ),
        CanonicalNumericFieldRule(
            field_path='price',
            numeric_kind='decimal',
            scale=8,
            description='Futures trade execution price.',
        ),
        CanonicalNumericFieldRule(
            field_path='qty',
            numeric_kind='decimal',
            scale=8,
            description='Futures trade quantity.',
        ),
        CanonicalNumericFieldRule(
            field_path='quote_qty',
            numeric_kind='decimal',
            scale=8,
            description='Futures trade quote-asset quantity.',
        ),
    ),
    (
        'bybit',
        'bybit_spot_trades',
    ): (
        CanonicalNumericFieldRule(
            field_path='trade_id',
            numeric_kind='int',
            scale=None,
            description='Bybit trade identifier.',
        ),
        CanonicalNumericFieldRule(
            field_path='price',
            numeric_kind='decimal',
            scale=15,
            description='Bybit trade execution price.',
        ),
        CanonicalNumericFieldRule(
            field_path='size',
            numeric_kind='decimal',
            scale=15,
            description='Bybit base-asset trade quantity.',
        ),
        CanonicalNumericFieldRule(
            field_path='quote_quantity',
            numeric_kind='decimal',
            scale=15,
            description='Bybit quote-asset trade quantity.',
        ),
        CanonicalNumericFieldRule(
            field_path='timestamp',
            numeric_kind='int',
            scale=None,
            description='Bybit event timestamp in epoch milliseconds.',
        ),
        CanonicalNumericFieldRule(
            field_path='gross_value',
            numeric_kind='decimal',
            scale=15,
            description='Bybit gross value.',
        ),
        CanonicalNumericFieldRule(
            field_path='home_notional',
            numeric_kind='decimal',
            scale=15,
            description='Bybit home notional.',
        ),
        CanonicalNumericFieldRule(
            field_path='foreign_notional',
            numeric_kind='decimal',
            scale=15,
            description='Bybit foreign notional.',
        ),
    ),
    (
        'bitcoin_core',
        'bitcoin_block_headers',
    ): (
        CanonicalNumericFieldRule(
            field_path='height',
            numeric_kind='int',
            scale=None,
            description='Bitcoin block height.',
        ),
        CanonicalNumericFieldRule(
            field_path='version',
            numeric_kind='int',
            scale=None,
            description='Bitcoin block version.',
        ),
        CanonicalNumericFieldRule(
            field_path='nonce',
            numeric_kind='int',
            scale=None,
            description='Bitcoin block nonce.',
        ),
        CanonicalNumericFieldRule(
            field_path='difficulty',
            numeric_kind='decimal',
            scale=18,
            description='Bitcoin block difficulty.',
        ),
        CanonicalNumericFieldRule(
            field_path='timestamp_ms',
            numeric_kind='int',
            scale=None,
            description='Bitcoin block timestamp in epoch milliseconds.',
        ),
    ),
    (
        'bitcoin_core',
        'bitcoin_block_transactions',
    ): (
        CanonicalNumericFieldRule(
            field_path='block_height',
            numeric_kind='int',
            scale=None,
            description='Bitcoin block height for transaction.',
        ),
        CanonicalNumericFieldRule(
            field_path='block_timestamp_ms',
            numeric_kind='int',
            scale=None,
            description='Bitcoin block timestamp in epoch milliseconds.',
        ),
        CanonicalNumericFieldRule(
            field_path='transaction_index',
            numeric_kind='int',
            scale=None,
            description='Transaction index within block.',
        ),
    ),
    (
        'bitcoin_core',
        'bitcoin_mempool_state',
    ): (
        CanonicalNumericFieldRule(
            field_path='snapshot_at_unix_ms',
            numeric_kind='int',
            scale=None,
            description='Mempool snapshot timestamp in epoch milliseconds.',
        ),
        CanonicalNumericFieldRule(
            field_path='fee_rate_sat_vb',
            numeric_kind='decimal',
            scale=18,
            description='Mempool transaction fee rate in sat/vB.',
        ),
        CanonicalNumericFieldRule(
            field_path='vsize',
            numeric_kind='int',
            scale=None,
            description='Mempool transaction virtual size in bytes.',
        ),
        CanonicalNumericFieldRule(
            field_path='first_seen_timestamp',
            numeric_kind='int',
            scale=None,
            description='Mempool transaction first-seen timestamp (epoch seconds).',
        ),
    ),
    (
        'bitcoin_core',
        'bitcoin_block_fee_totals',
    ): (
        CanonicalNumericFieldRule(
            field_path='block_height',
            numeric_kind='int',
            scale=None,
            description='Bitcoin block height.',
        ),
        CanonicalNumericFieldRule(
            field_path='block_timestamp_ms',
            numeric_kind='int',
            scale=None,
            description='Bitcoin block timestamp in epoch milliseconds.',
        ),
        CanonicalNumericFieldRule(
            field_path='fee_total_btc',
            numeric_kind='decimal',
            scale=18,
            description='Total transaction fees in BTC for block.',
        ),
        CanonicalNumericFieldRule(
            field_path='metric_value',
            numeric_kind='decimal',
            scale=18,
            description='Canonical aligned metric value for block fee totals.',
        ),
    ),
    (
        'bitcoin_core',
        'bitcoin_block_subsidy_schedule',
    ): (
        CanonicalNumericFieldRule(
            field_path='block_height',
            numeric_kind='int',
            scale=None,
            description='Bitcoin block height.',
        ),
        CanonicalNumericFieldRule(
            field_path='block_timestamp_ms',
            numeric_kind='int',
            scale=None,
            description='Bitcoin block timestamp in epoch milliseconds.',
        ),
        CanonicalNumericFieldRule(
            field_path='halving_interval',
            numeric_kind='int',
            scale=None,
            description='Bitcoin halving interval index.',
        ),
        CanonicalNumericFieldRule(
            field_path='subsidy_sats',
            numeric_kind='int',
            scale=None,
            description='Bitcoin block subsidy in satoshis.',
        ),
        CanonicalNumericFieldRule(
            field_path='subsidy_btc',
            numeric_kind='decimal',
            scale=18,
            description='Bitcoin block subsidy in BTC.',
        ),
        CanonicalNumericFieldRule(
            field_path='metric_value',
            numeric_kind='decimal',
            scale=18,
            description='Canonical aligned metric value for subsidy.',
        ),
    ),
    (
        'bitcoin_core',
        'bitcoin_network_hashrate_estimate',
    ): (
        CanonicalNumericFieldRule(
            field_path='block_height',
            numeric_kind='int',
            scale=None,
            description='Bitcoin block height.',
        ),
        CanonicalNumericFieldRule(
            field_path='block_timestamp_ms',
            numeric_kind='int',
            scale=None,
            description='Bitcoin block timestamp in epoch milliseconds.',
        ),
        CanonicalNumericFieldRule(
            field_path='difficulty',
            numeric_kind='decimal',
            scale=18,
            description='Bitcoin block difficulty.',
        ),
        CanonicalNumericFieldRule(
            field_path='observed_interval_seconds',
            numeric_kind='int',
            scale=None,
            description='Observed block interval in seconds.',
        ),
        CanonicalNumericFieldRule(
            field_path='hashrate_hs',
            numeric_kind='decimal',
            scale=18,
            description='Estimated Bitcoin network hashrate in H/s.',
        ),
        CanonicalNumericFieldRule(
            field_path='metric_value',
            numeric_kind='decimal',
            scale=18,
            description='Canonical aligned metric value for hashrate.',
        ),
    ),
    (
        'bitcoin_core',
        'bitcoin_circulating_supply',
    ): (
        CanonicalNumericFieldRule(
            field_path='block_height',
            numeric_kind='int',
            scale=None,
            description='Bitcoin block height.',
        ),
        CanonicalNumericFieldRule(
            field_path='block_timestamp_ms',
            numeric_kind='int',
            scale=None,
            description='Bitcoin block timestamp in epoch milliseconds.',
        ),
        CanonicalNumericFieldRule(
            field_path='circulating_supply_sats',
            numeric_kind='int',
            scale=None,
            description='Bitcoin circulating supply in satoshis.',
        ),
        CanonicalNumericFieldRule(
            field_path='circulating_supply_btc',
            numeric_kind='decimal',
            scale=18,
            description='Bitcoin circulating supply in BTC.',
        ),
        CanonicalNumericFieldRule(
            field_path='metric_value',
            numeric_kind='decimal',
            scale=18,
            description='Canonical aligned metric value for circulating supply.',
        ),
    ),
    (
        'etf',
        'etf_daily_metrics',
    ): (),
    (
        'fred',
        'fred_series_metrics',
    ): (),
    (
        'okx',
        'okx_spot_trades',
    ): (
        CanonicalNumericFieldRule(
            field_path='trade_id',
            numeric_kind='int',
            scale=None,
            description='OKX trade identifier.',
        ),
        CanonicalNumericFieldRule(
            field_path='price',
            numeric_kind='decimal',
            scale=8,
            description='OKX trade execution price.',
        ),
        CanonicalNumericFieldRule(
            field_path='size',
            numeric_kind='decimal',
            scale=8,
            description='OKX base-asset trade quantity.',
        ),
        CanonicalNumericFieldRule(
            field_path='timestamp',
            numeric_kind='int',
            scale=None,
            description='OKX event timestamp in epoch milliseconds.',
        ),
    ),
}


def _parse_json_constant(value: str) -> None:
    raise RuntimeError(f'Non-finite numeric constant is not allowed: {value}')


def _is_numeric_value(value: Any) -> bool:
    if isinstance(value, bool):
        return False
    return isinstance(value, (int, Decimal))


def _normalize_int_value(
    *,
    raw_value: Any,
    source_id: str,
    stream_id: str,
    field_path: str,
) -> int:
    normalized_decimal: Decimal
    if isinstance(raw_value, bool):
        raise RuntimeError(
            f'Boolean value is not valid for int field path={field_path} '
            f'source_id={source_id} stream_id={stream_id}'
        )
    if isinstance(raw_value, int):
        return raw_value
    if isinstance(raw_value, Decimal):
        normalized_decimal = raw_value
    elif isinstance(raw_value, str):
        try:
            normalized_decimal = Decimal(raw_value)
        except InvalidOperation as exc:
            raise RuntimeError(
                f'Invalid integer value for field path={field_path} '
                f'source_id={source_id} stream_id={stream_id}: {raw_value}'
            ) from exc
    else:
        raise RuntimeError(
            f'Unsupported integer value type for field path={field_path} '
            f'source_id={source_id} stream_id={stream_id}: {type(raw_value).__name__}'
        )
    if normalized_decimal != normalized_decimal.to_integral_value():
        raise RuntimeError(
            f'Non-integer value for int field path={field_path} '
            f'source_id={source_id} stream_id={stream_id}: {raw_value}'
        )
    return int(normalized_decimal)


def _normalize_decimal_value(
    *,
    raw_value: Any,
    scale: int,
    source_id: str,
    stream_id: str,
    field_path: str,
) -> str:
    normalized_decimal: Decimal
    if isinstance(raw_value, bool):
        raise RuntimeError(
            f'Boolean value is not valid for decimal field path={field_path} '
            f'source_id={source_id} stream_id={stream_id}'
        )
    if isinstance(raw_value, int):
        normalized_decimal = Decimal(raw_value)
    elif isinstance(raw_value, Decimal):
        normalized_decimal = raw_value
    elif isinstance(raw_value, str):
        try:
            normalized_decimal = Decimal(raw_value)
        except InvalidOperation as exc:
            raise RuntimeError(
                f'Invalid decimal value for field path={field_path} '
                f'source_id={source_id} stream_id={stream_id}: {raw_value}'
            ) from exc
    else:
        raise RuntimeError(
            f'Unsupported decimal value type for field path={field_path} '
            f'source_id={source_id} stream_id={stream_id}: {type(raw_value).__name__}'
        )

    scale_quantizer = Decimal(1).scaleb(-scale)
    with localcontext() as decimal_context:
        decimal_context.prec = max(decimal_context.prec, 80)
        quantized = normalized_decimal.quantize(scale_quantizer)
    if normalized_decimal != quantized:
        raise RuntimeError(
            f'Decimal value exceeds configured scale for field path={field_path} '
            f'source_id={source_id} stream_id={stream_id}: value={raw_value} scale={scale}'
        )
    return format(quantized, f'.{scale}f')


def _build_rules_by_path(
    *, source_id: str, stream_id: str
) -> dict[str, CanonicalNumericFieldRule]:
    stream_key = (source_id.strip(), stream_id.strip())
    rules = _CANONICAL_SOURCE_PRECISION_RULES.get(stream_key)
    if rules is None:
        raise RuntimeError(
            f'No source precision rules found for source_id={stream_key[0]} '
            f'stream_id={stream_key[1]}'
        )

    by_path: dict[str, CanonicalNumericFieldRule] = {}
    for rule in rules:
        if rule.field_path in by_path:
            raise RuntimeError(
                f'Duplicate precision rule for source_id={stream_key[0]} '
                f'stream_id={stream_key[1]} field_path={rule.field_path}'
            )
        if rule.numeric_kind == 'decimal':
            if rule.scale is None or rule.scale < 0:
                raise RuntimeError(
                    'Decimal precision rules must define non-negative scale: '
                    f'source_id={stream_key[0]} stream_id={stream_key[1]} '
                    f'field_path={rule.field_path}'
                )
        elif rule.scale is not None:
            raise RuntimeError(
                f'Int precision rule cannot define scale: source_id={stream_key[0]} '
                f'stream_id={stream_key[1]} field_path={rule.field_path}'
            )
        by_path[rule.field_path] = rule
    return by_path


def _normalize_payload_value(
    *,
    source_id: str,
    stream_id: str,
    value: Any,
    field_path: str,
    rules_by_path: dict[str, CanonicalNumericFieldRule],
) -> Any:
    if isinstance(value, dict):
        raw_map = cast(dict[Any, Any], value)
        normalized_map: dict[str, Any] = {}
        for raw_key, child_value in raw_map.items():
            if not isinstance(raw_key, str):
                raise RuntimeError(
                    f'JSON object keys must be strings: source_id={source_id} '
                    f'stream_id={stream_id} field_path={field_path}'
                )
            child_path = f'{field_path}.{raw_key}' if field_path != '' else raw_key
            normalized_map[raw_key] = _normalize_payload_value(
                source_id=source_id,
                stream_id=stream_id,
                value=child_value,
                field_path=child_path,
                rules_by_path=rules_by_path,
            )
        return {key: normalized_map[key] for key in sorted(normalized_map)}

    if isinstance(value, list):
        list_items = cast(list[Any], value)
        return [
            _normalize_payload_value(
                source_id=source_id,
                stream_id=stream_id,
                value=item,
                field_path=f'{field_path}[]' if field_path != '' else '[]',
                rules_by_path=rules_by_path,
            )
            for item in list_items
        ]

    rule = rules_by_path.get(field_path)
    if rule is not None:
        if rule.numeric_kind == 'int':
            return _normalize_int_value(
                raw_value=value,
                source_id=source_id,
                stream_id=stream_id,
                field_path=field_path,
            )
        scale = rule.scale
        if scale is None:
            raise RuntimeError(
                f'Missing scale for decimal rule field path={field_path} '
                f'source_id={source_id} stream_id={stream_id}'
            )
        return _normalize_decimal_value(
            raw_value=value,
            scale=scale,
            source_id=source_id,
            stream_id=stream_id,
            field_path=field_path,
        )

    if _is_numeric_value(value):
        raise RuntimeError(
            f'Numeric field path={field_path} is missing precision rule '
            f'for source_id={source_id} stream_id={stream_id}'
        )
    return value


def canonicalize_payload_json_with_precision(
    *,
    source_id: str,
    stream_id: str,
    payload_raw: bytes,
    payload_encoding: str,
) -> str:
    decoded = payload_raw.decode(payload_encoding)
    parsed = json.loads(
        decoded,
        parse_float=Decimal,
        parse_int=int,
        parse_constant=_parse_json_constant,
    )
    normalized = _normalize_payload_value(
        source_id=source_id.strip(),
        stream_id=stream_id.strip(),
        value=parsed,
        field_path='',
        rules_by_path=_build_rules_by_path(source_id=source_id, stream_id=stream_id),
    )
    return json.dumps(
        normalized,
        ensure_ascii=False,
        sort_keys=True,
        separators=(',', ':'),
    )


def assert_payload_json_has_no_float_values(
    *, source_id: str, stream_id: str, payload_json: str
) -> None:
    parsed = json.loads(payload_json)

    def _visit(value: Any, *, field_path: str) -> None:
        if isinstance(value, float):
            raise RuntimeError(
                f'Float value found in canonical payload_json field path={field_path} '
                f'source_id={source_id} stream_id={stream_id}'
            )
        if isinstance(value, dict):
            for raw_key, raw_child in cast(dict[Any, Any], value).items():
                if not isinstance(raw_key, str):
                    raise RuntimeError(
                        f'Canonical payload_json keys must be strings at field path={field_path} '
                        f'source_id={source_id} stream_id={stream_id}'
                    )
                child_path = (
                    f'{field_path}.{raw_key}' if field_path != '' else raw_key
                )
                _visit(raw_child, field_path=child_path)
            return
        if isinstance(value, list):
            for index, item in enumerate(cast(list[Any], value)):
                child_path = f'{field_path}[{index}]' if field_path != '' else f'[{index}]'
                _visit(item, field_path=child_path)

    _visit(parsed, field_path='')


def canonical_source_precision_registry_contract() -> CanonicalSourcePrecisionRegistryContract:
    entries: list[CanonicalSourcePrecisionEntry] = []
    for source_id, stream_id in sorted(_CANONICAL_SOURCE_PRECISION_RULES.keys()):
        rules = _CANONICAL_SOURCE_PRECISION_RULES[(source_id, stream_id)]
        entries.append(
            {
                'source_id': source_id,
                'stream_id': stream_id,
                'numeric_fields': [
                    {
                        'field_path': rule.field_path,
                        'numeric_kind': rule.numeric_kind,
                        'scale': rule.scale,
                        'description': rule.description,
                    }
                    for rule in rules
                ],
            }
        )
    return {
        'registry_version': CANONICAL_SOURCE_PRECISION_REGISTRY_VERSION,
        'entries': entries,
    }
