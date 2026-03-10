from __future__ import annotations

import json
import math
import re
from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from typing import Any, Literal, cast

type BitcoinStreamDataset = Literal[
    'bitcoin_block_headers',
    'bitcoin_block_transactions',
    'bitcoin_mempool_state',
]
type BitcoinDerivedDataset = Literal[
    'bitcoin_block_fee_totals',
    'bitcoin_block_subsidy_schedule',
    'bitcoin_network_hashrate_estimate',
    'bitcoin_circulating_supply',
]

_HASH_HEX_64_PATTERN = re.compile(r'^[0-9a-f]{64}$')
_SATS_PER_BTC = 100_000_000
_INITIAL_SUBSIDY_SATS = 50 * _SATS_PER_BTC
_HALVING_INTERVAL_BLOCKS = 210_000
_MAX_HALVINGS = 64
_HASHRATE_CONSTANT = float(2**32)


@dataclass(frozen=True)
class BitcoinIntegrityReport:
    dataset: str
    rows_checked: int
    anomaly_checks_performed: int
    min_height: int | None
    max_height: int | None

    def to_dict(self) -> dict[str, str | int | None]:
        return {
            'dataset': self.dataset,
            'rows_checked': self.rows_checked,
            'anomaly_checks_performed': self.anomaly_checks_performed,
            'min_height': self.min_height,
            'max_height': self.max_height,
        }


def _expect_int(*, value: Any, label: str, minimum: int | None = None) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        raise ValueError(f'Bitcoin integrity schema/type check failed for {label}')
    if minimum is not None and value < minimum:
        raise ValueError(
            f'Bitcoin integrity anomaly check failed for {label}: '
            f'expected >= {minimum}, got={value}'
        )
    return value


def _expect_number(*, value: Any, label: str, minimum: float | None = None) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise ValueError(f'Bitcoin integrity schema/type check failed for {label}')
    numeric = float(value)
    if minimum is not None and numeric < minimum:
        raise ValueError(
            f'Bitcoin integrity anomaly check failed for {label}: '
            f'expected >= {minimum}, got={numeric}'
        )
    return numeric


def _expect_bool_like(*, value: Any, label: str) -> bool:
    if isinstance(value, bool):
        return value
    if isinstance(value, int) and value in {0, 1}:
        return value == 1
    raise ValueError(f'Bitcoin integrity schema/type check failed for {label}')


def _expect_hash_hex_64(*, value: Any, label: str) -> str:
    if not isinstance(value, str):
        raise ValueError(f'Bitcoin integrity schema/type check failed for {label}')
    if _HASH_HEX_64_PATTERN.fullmatch(value) is None:
        raise ValueError(
            f'Bitcoin integrity schema/type check failed for {label}: '
            f'expected 64-char lowercase hash, got={value}'
        )
    return value


def _expect_optional_prev_hash(*, value: Any, label: str) -> str:
    if not isinstance(value, str):
        raise ValueError(f'Bitcoin integrity schema/type check failed for {label}')
    if value == '':
        return value
    if _HASH_HEX_64_PATTERN.fullmatch(value) is None:
        raise ValueError(
            f'Bitcoin integrity schema/type check failed for {label}: '
            f'expected empty string or 64-char lowercase hash, got={value}'
        )
    return value


def _expect_json_list(*, value: Any, label: str) -> list[Any]:
    if not isinstance(value, str):
        raise ValueError(f'Bitcoin integrity schema/type check failed for {label}')
    try:
        parsed = json.loads(value)
    except json.JSONDecodeError as exc:
        raise ValueError(
            f'Bitcoin integrity schema/type check failed for {label}: invalid JSON'
        ) from exc
    if not isinstance(parsed, list):
        raise ValueError(f'Bitcoin integrity schema/type check failed for {label}')
    return cast(list[Any], parsed)


def _expect_json_dict(*, value: Any, label: str) -> dict[str, Any]:
    if not isinstance(value, str):
        raise ValueError(f'Bitcoin integrity schema/type check failed for {label}')
    try:
        parsed = json.loads(value)
    except json.JSONDecodeError as exc:
        raise ValueError(
            f'Bitcoin integrity schema/type check failed for {label}: invalid JSON'
        ) from exc
    if not isinstance(parsed, dict):
        raise ValueError(f'Bitcoin integrity schema/type check failed for {label}')
    parsed_map = cast(dict[Any, Any], parsed)
    normalized: dict[str, Any] = {}
    for key, raw_value in parsed_map.items():
        if not isinstance(key, str):
            raise ValueError(
                f'Bitcoin integrity schema/type check failed for {label}: keys must be strings'
            )
        normalized[key] = raw_value
    return normalized


def _normalize_rows(rows: Iterable[Mapping[str, Any]], *, label: str) -> list[dict[str, Any]]:
    normalized: list[dict[str, Any]] = []
    for row_index, row in enumerate(rows, start=1):
        row_map = cast(Mapping[Any, Any], row)
        normalized_row: dict[str, Any] = {}
        for key, value in row_map.items():
            if not isinstance(key, str):
                raise ValueError(
                    f'Bitcoin integrity schema/type check failed for '
                    f'{label}.row[{row_index}]: keys must be strings'
                )
            normalized_row[key] = value
        normalized.append(normalized_row)
    return normalized


def run_bitcoin_block_header_integrity(
    *, rows: Iterable[Mapping[str, Any]]
) -> BitcoinIntegrityReport:
    dataset: BitcoinStreamDataset = 'bitcoin_block_headers'
    normalized_rows = _normalize_rows(rows, label=dataset)
    if len(normalized_rows) == 0:
        raise ValueError(
            'Bitcoin integrity schema/type check failed for bitcoin_block_headers: '
            'no rows found'
        )

    previous_height: int | None = None
    previous_block_hash: str | None = None
    previous_timestamp: int | None = None
    anomaly_checks_performed = 0
    min_height: int | None = None
    max_height: int | None = None

    for row_index, row in enumerate(normalized_rows, start=1):
        height = _expect_int(
            value=row.get('height'),
            label=f'{dataset}.row[{row_index}].height',
            minimum=0,
        )
        block_hash = _expect_hash_hex_64(
            value=row.get('block_hash'),
            label=f'{dataset}.row[{row_index}].block_hash',
        )
        prev_hash = _expect_optional_prev_hash(
            value=row.get('prev_hash'),
            label=f'{dataset}.row[{row_index}].prev_hash',
        )
        difficulty = _expect_number(
            value=row.get('difficulty'),
            label=f'{dataset}.row[{row_index}].difficulty',
            minimum=0.0,
        )
        timestamp_ms = _expect_int(
            value=row.get('timestamp_ms'),
            label=f'{dataset}.row[{row_index}].timestamp_ms',
            minimum=1,
        )
        anomaly_checks_performed += 2

        if difficulty <= 0:
            raise ValueError(
                f'Bitcoin integrity anomaly check failed for {dataset}.row[{row_index}].difficulty: '
                f'expected > 0, got={difficulty}'
            )
        anomaly_checks_performed += 1

        if previous_height is not None:
            if height != previous_height + 1:
                raise ValueError(
                    f'Bitcoin integrity linkage check failed for {dataset}: '
                    f'row={row_index}, expected_height={previous_height + 1}, got={height}'
                )
            if previous_block_hash is None or prev_hash != previous_block_hash:
                raise ValueError(
                    f'Bitcoin integrity linkage check failed for {dataset}: '
                    f'row={row_index}, prev_hash={prev_hash}, '
                    f'expected={previous_block_hash}'
                )
            if previous_timestamp is not None and timestamp_ms <= previous_timestamp:
                raise ValueError(
                    f'Bitcoin integrity anomaly check failed for {dataset}: '
                    f'row={row_index}, timestamp must be strictly increasing '
                    f'(previous={previous_timestamp}, current={timestamp_ms})'
                )
            anomaly_checks_performed += 3
        elif height == 0 and prev_hash != '':
            raise ValueError(
                f'Bitcoin integrity linkage check failed for {dataset}: '
                'genesis prev_hash must be empty'
            )

        min_height = height if min_height is None else min(min_height, height)
        max_height = height if max_height is None else max(max_height, height)
        previous_height = height
        previous_block_hash = block_hash
        previous_timestamp = timestamp_ms

    return BitcoinIntegrityReport(
        dataset=dataset,
        rows_checked=len(normalized_rows),
        anomaly_checks_performed=anomaly_checks_performed,
        min_height=min_height,
        max_height=max_height,
    )


def run_bitcoin_block_transaction_integrity(
    *, rows: Iterable[Mapping[str, Any]]
) -> BitcoinIntegrityReport:
    dataset: BitcoinStreamDataset = 'bitcoin_block_transactions'
    normalized_rows = _normalize_rows(rows, label=dataset)
    if len(normalized_rows) == 0:
        raise ValueError(
            'Bitcoin integrity schema/type check failed for bitcoin_block_transactions: '
            'no rows found'
        )

    previous_height: int | None = None
    previous_transaction_index = -1
    current_block_hash: str | None = None
    observed_txids: set[str] = set()
    anomaly_checks_performed = 0
    min_height: int | None = None
    max_height: int | None = None

    for row_index, row in enumerate(normalized_rows, start=1):
        block_height = _expect_int(
            value=row.get('block_height'),
            label=f'{dataset}.row[{row_index}].block_height',
            minimum=0,
        )
        block_hash = _expect_hash_hex_64(
            value=row.get('block_hash'),
            label=f'{dataset}.row[{row_index}].block_hash',
        )
        transaction_index = _expect_int(
            value=row.get('transaction_index'),
            label=f'{dataset}.row[{row_index}].transaction_index',
            minimum=0,
        )
        txid = _expect_hash_hex_64(
            value=row.get('txid'),
            label=f'{dataset}.row[{row_index}].txid',
        )
        _expect_int(
            value=row.get('block_timestamp_ms'),
            label=f'{dataset}.row[{row_index}].block_timestamp_ms',
            minimum=1,
        )
        _expect_bool_like(
            value=row.get('coinbase'),
            label=f'{dataset}.row[{row_index}].coinbase',
        )
        inputs = _expect_json_list(
            value=row.get('inputs_json'),
            label=f'{dataset}.row[{row_index}].inputs_json',
        )
        outputs = _expect_json_list(
            value=row.get('outputs_json'),
            label=f'{dataset}.row[{row_index}].outputs_json',
        )
        values_payload = _expect_json_dict(
            value=row.get('values_json'),
            label=f'{dataset}.row[{row_index}].values_json',
        )
        _expect_json_dict(
            value=row.get('scripts_json'),
            label=f'{dataset}.row[{row_index}].scripts_json',
        )
        _expect_json_list(
            value=row.get('witness_data_json'),
            label=f'{dataset}.row[{row_index}].witness_data_json',
        )
        anomaly_checks_performed += 10

        if txid in observed_txids:
            raise ValueError(
                f'Bitcoin integrity linkage check failed for {dataset}: '
                f'duplicate txid={txid}'
            )
        observed_txids.add(txid)
        anomaly_checks_performed += 1

        if len(inputs) == 0 or len(outputs) == 0:
            raise ValueError(
                f'Bitcoin integrity anomaly check failed for {dataset}.row[{row_index}]: '
                'inputs and outputs must be non-empty'
            )
        anomaly_checks_performed += 1

        input_sum = _expect_number(
            value=values_payload.get('input_value_btc_sum'),
            label=f'{dataset}.row[{row_index}].values_json.input_value_btc_sum',
            minimum=0.0,
        )
        output_sum = _expect_number(
            value=values_payload.get('output_value_btc_sum'),
            label=f'{dataset}.row[{row_index}].values_json.output_value_btc_sum',
            minimum=0.0,
        )
        if output_sum < 0 or input_sum < 0:
            raise ValueError(
                f'Bitcoin integrity anomaly check failed for {dataset}.row[{row_index}]: '
                'value sums must be non-negative'
            )
        anomaly_checks_performed += 2

        if previous_height is None:
            if transaction_index != 0:
                raise ValueError(
                    f'Bitcoin integrity linkage check failed for {dataset}: '
                    f'first row transaction_index must be 0, got={transaction_index}'
                )
        elif block_height == previous_height:
            if current_block_hash is not None and block_hash != current_block_hash:
                raise ValueError(
                    f'Bitcoin integrity linkage check failed for {dataset}: '
                    f'row={row_index}, block_hash mismatch within block_height={block_height}'
                )
            if transaction_index != previous_transaction_index + 1:
                raise ValueError(
                    f'Bitcoin integrity linkage check failed for {dataset}: '
                    f'row={row_index}, expected transaction_index='
                    f'{previous_transaction_index + 1}, '
                    f'got={transaction_index}'
                )
            anomaly_checks_performed += 2
        else:
            if block_height <= previous_height:
                raise ValueError(
                    f'Bitcoin integrity linkage check failed for {dataset}: '
                    f'row={row_index}, previous_block_height={previous_height}, '
                    f'current_block_height={block_height}'
                )
            if transaction_index != 0:
                raise ValueError(
                    f'Bitcoin integrity linkage check failed for {dataset}: '
                    f'row={row_index}, first transaction for block_height={block_height} '
                    f'must have transaction_index=0, got={transaction_index}'
                )
            anomaly_checks_performed += 2

        min_height = block_height if min_height is None else min(min_height, block_height)
        max_height = block_height if max_height is None else max(max_height, block_height)
        previous_height = block_height
        previous_transaction_index = transaction_index
        current_block_hash = block_hash

    return BitcoinIntegrityReport(
        dataset=dataset,
        rows_checked=len(normalized_rows),
        anomaly_checks_performed=anomaly_checks_performed,
        min_height=min_height,
        max_height=max_height,
    )


def run_bitcoin_mempool_state_integrity(
    *, rows: Iterable[Mapping[str, Any]]
) -> BitcoinIntegrityReport:
    dataset: BitcoinStreamDataset = 'bitcoin_mempool_state'
    normalized_rows = _normalize_rows(rows, label=dataset)

    if len(normalized_rows) == 0:
        return BitcoinIntegrityReport(
            dataset=dataset,
            rows_checked=0,
            anomaly_checks_performed=0,
            min_height=None,
            max_height=None,
        )

    snapshot_at_unix_ms: int | None = None
    observed_txids: set[str] = set()
    anomaly_checks_performed = 0

    for row_index, row in enumerate(normalized_rows, start=1):
        row_snapshot_ms = _expect_int(
            value=row.get('snapshot_at_unix_ms'),
            label=f'{dataset}.row[{row_index}].snapshot_at_unix_ms',
            minimum=0,
        )
        txid = _expect_hash_hex_64(
            value=row.get('txid'),
            label=f'{dataset}.row[{row_index}].txid',
        )
        _expect_number(
            value=row.get('fee_rate_sat_vb'),
            label=f'{dataset}.row[{row_index}].fee_rate_sat_vb',
            minimum=0.0,
        )
        _expect_int(
            value=row.get('vsize'),
            label=f'{dataset}.row[{row_index}].vsize',
            minimum=1,
        )
        _expect_int(
            value=row.get('first_seen_timestamp'),
            label=f'{dataset}.row[{row_index}].first_seen_timestamp',
            minimum=0,
        )
        _expect_bool_like(
            value=row.get('rbf_flag'),
            label=f'{dataset}.row[{row_index}].rbf_flag',
        )
        anomaly_checks_performed += 6

        if txid in observed_txids:
            raise ValueError(
                f'Bitcoin integrity linkage check failed for {dataset}: '
                f'duplicate txid in snapshot={txid}'
            )
        observed_txids.add(txid)
        anomaly_checks_performed += 1

        if snapshot_at_unix_ms is None:
            snapshot_at_unix_ms = row_snapshot_ms
        elif row_snapshot_ms != snapshot_at_unix_ms:
            raise ValueError(
                f'Bitcoin integrity linkage check failed for {dataset}: '
                f'mixed snapshot_at_unix_ms values found '
                f'(expected={snapshot_at_unix_ms}, got={row_snapshot_ms})'
            )

    return BitcoinIntegrityReport(
        dataset=dataset,
        rows_checked=len(normalized_rows),
        anomaly_checks_performed=anomaly_checks_performed,
        min_height=None,
        max_height=None,
    )


def _subsidy_sats_for_height(height: int) -> tuple[int, int]:
    halving_interval = height // _HALVING_INTERVAL_BLOCKS
    if halving_interval >= _MAX_HALVINGS:
        return halving_interval, 0
    return halving_interval, _INITIAL_SUBSIDY_SATS >> halving_interval


def _circulating_supply_sats_at_height(height: int) -> int:
    remaining_blocks = height + 1
    total_sats = 0
    for halving_interval in range(_MAX_HALVINGS):
        subsidy_sats = _INITIAL_SUBSIDY_SATS >> halving_interval
        if subsidy_sats <= 0:
            break
        blocks_in_era = min(remaining_blocks, _HALVING_INTERVAL_BLOCKS)
        total_sats += blocks_in_era * subsidy_sats
        remaining_blocks -= blocks_in_era
        if remaining_blocks == 0:
            break
    return total_sats


def run_bitcoin_block_fee_total_integrity(
    *, rows: Iterable[Mapping[str, Any]]
) -> BitcoinIntegrityReport:
    dataset: BitcoinDerivedDataset = 'bitcoin_block_fee_totals'
    normalized_rows = _normalize_rows(rows, label=dataset)
    if len(normalized_rows) == 0:
        raise ValueError(
            'Bitcoin integrity schema/type check failed for bitcoin_block_fee_totals: '
            'no rows found'
        )

    previous_height: int | None = None
    anomaly_checks_performed = 0
    min_height: int | None = None
    max_height: int | None = None

    for row_index, row in enumerate(normalized_rows, start=1):
        height = _expect_int(
            value=row.get('block_height'),
            label=f'{dataset}.row[{row_index}].block_height',
            minimum=0,
        )
        _expect_hash_hex_64(
            value=row.get('block_hash'),
            label=f'{dataset}.row[{row_index}].block_hash',
        )
        _expect_int(
            value=row.get('block_timestamp_ms'),
            label=f'{dataset}.row[{row_index}].block_timestamp_ms',
            minimum=1,
        )
        _expect_number(
            value=row.get('fee_total_btc'),
            label=f'{dataset}.row[{row_index}].fee_total_btc',
            minimum=0.0,
        )
        anomaly_checks_performed += 4

        if previous_height is not None and height != previous_height + 1:
            raise ValueError(
                f'Bitcoin integrity linkage check failed for {dataset}: '
                f'row={row_index}, expected_height={previous_height + 1}, got={height}'
            )
        anomaly_checks_performed += 1
        previous_height = height
        min_height = height if min_height is None else min(min_height, height)
        max_height = height if max_height is None else max(max_height, height)

    return BitcoinIntegrityReport(
        dataset=dataset,
        rows_checked=len(normalized_rows),
        anomaly_checks_performed=anomaly_checks_performed,
        min_height=min_height,
        max_height=max_height,
    )


def run_bitcoin_subsidy_schedule_integrity(
    *, rows: Iterable[Mapping[str, Any]]
) -> BitcoinIntegrityReport:
    dataset: BitcoinDerivedDataset = 'bitcoin_block_subsidy_schedule'
    normalized_rows = _normalize_rows(rows, label=dataset)
    if len(normalized_rows) == 0:
        raise ValueError(
            'Bitcoin integrity schema/type check failed for bitcoin_block_subsidy_schedule: '
            'no rows found'
        )

    previous_height: int | None = None
    anomaly_checks_performed = 0
    min_height: int | None = None
    max_height: int | None = None

    for row_index, row in enumerate(normalized_rows, start=1):
        height = _expect_int(
            value=row.get('block_height'),
            label=f'{dataset}.row[{row_index}].block_height',
            minimum=0,
        )
        _expect_hash_hex_64(
            value=row.get('block_hash'),
            label=f'{dataset}.row[{row_index}].block_hash',
        )
        _expect_int(
            value=row.get('block_timestamp_ms'),
            label=f'{dataset}.row[{row_index}].block_timestamp_ms',
            minimum=1,
        )
        halving_interval = _expect_int(
            value=row.get('halving_interval'),
            label=f'{dataset}.row[{row_index}].halving_interval',
            minimum=0,
        )
        subsidy_sats = _expect_int(
            value=row.get('subsidy_sats'),
            label=f'{dataset}.row[{row_index}].subsidy_sats',
            minimum=0,
        )
        subsidy_btc = _expect_number(
            value=row.get('subsidy_btc'),
            label=f'{dataset}.row[{row_index}].subsidy_btc',
            minimum=0.0,
        )
        anomaly_checks_performed += 6

        expected_halving_interval, expected_subsidy_sats = _subsidy_sats_for_height(height)
        if halving_interval != expected_halving_interval:
            raise ValueError(
                f'Bitcoin integrity formula check failed for {dataset}: '
                f'row={row_index}, expected_halving_interval='
                f'{expected_halving_interval}, got={halving_interval}'
            )
        if subsidy_sats != expected_subsidy_sats:
            raise ValueError(
                f'Bitcoin integrity formula check failed for {dataset}: '
                f'row={row_index}, expected_subsidy_sats={expected_subsidy_sats}, '
                f'got={subsidy_sats}'
            )
        expected_subsidy_btc = float(expected_subsidy_sats) / float(_SATS_PER_BTC)
        if not math.isclose(subsidy_btc, expected_subsidy_btc, rel_tol=1e-12, abs_tol=0.0):
            raise ValueError(
                f'Bitcoin integrity formula check failed for {dataset}: '
                f'row={row_index}, expected_subsidy_btc={expected_subsidy_btc}, '
                f'got={subsidy_btc}'
            )
        anomaly_checks_performed += 3

        if previous_height is not None and height != previous_height + 1:
            raise ValueError(
                f'Bitcoin integrity linkage check failed for {dataset}: '
                f'row={row_index}, expected_height={previous_height + 1}, got={height}'
            )
        anomaly_checks_performed += 1
        previous_height = height
        min_height = height if min_height is None else min(min_height, height)
        max_height = height if max_height is None else max(max_height, height)

    return BitcoinIntegrityReport(
        dataset=dataset,
        rows_checked=len(normalized_rows),
        anomaly_checks_performed=anomaly_checks_performed,
        min_height=min_height,
        max_height=max_height,
    )


def run_bitcoin_network_hashrate_integrity(
    *, rows: Iterable[Mapping[str, Any]]
) -> BitcoinIntegrityReport:
    dataset: BitcoinDerivedDataset = 'bitcoin_network_hashrate_estimate'
    normalized_rows = _normalize_rows(rows, label=dataset)
    if len(normalized_rows) == 0:
        raise ValueError(
            'Bitcoin integrity schema/type check failed for bitcoin_network_hashrate_estimate: '
            'no rows found'
        )

    previous_height: int | None = None
    anomaly_checks_performed = 0
    min_height: int | None = None
    max_height: int | None = None

    for row_index, row in enumerate(normalized_rows, start=1):
        height = _expect_int(
            value=row.get('block_height'),
            label=f'{dataset}.row[{row_index}].block_height',
            minimum=0,
        )
        _expect_hash_hex_64(
            value=row.get('block_hash'),
            label=f'{dataset}.row[{row_index}].block_hash',
        )
        _expect_int(
            value=row.get('block_timestamp_ms'),
            label=f'{dataset}.row[{row_index}].block_timestamp_ms',
            minimum=1,
        )
        difficulty = _expect_number(
            value=row.get('difficulty'),
            label=f'{dataset}.row[{row_index}].difficulty',
            minimum=0.0,
        )
        observed_interval_seconds = _expect_int(
            value=row.get('observed_interval_seconds'),
            label=f'{dataset}.row[{row_index}].observed_interval_seconds',
            minimum=1,
        )
        hashrate_hs = _expect_number(
            value=row.get('hashrate_hs'),
            label=f'{dataset}.row[{row_index}].hashrate_hs',
            minimum=0.0,
        )
        anomaly_checks_performed += 6

        expected_hashrate = difficulty * _HASHRATE_CONSTANT / float(observed_interval_seconds)
        if not math.isclose(hashrate_hs, expected_hashrate, rel_tol=1e-12, abs_tol=0.0):
            raise ValueError(
                f'Bitcoin integrity formula check failed for {dataset}: '
                f'row={row_index}, expected_hashrate_hs={expected_hashrate}, '
                f'got={hashrate_hs}'
            )
        anomaly_checks_performed += 1

        if previous_height is not None and height != previous_height + 1:
            raise ValueError(
                f'Bitcoin integrity linkage check failed for {dataset}: '
                f'row={row_index}, expected_height={previous_height + 1}, got={height}'
            )
        anomaly_checks_performed += 1
        previous_height = height
        min_height = height if min_height is None else min(min_height, height)
        max_height = height if max_height is None else max(max_height, height)

    return BitcoinIntegrityReport(
        dataset=dataset,
        rows_checked=len(normalized_rows),
        anomaly_checks_performed=anomaly_checks_performed,
        min_height=min_height,
        max_height=max_height,
    )


def run_bitcoin_circulating_supply_integrity(
    *, rows: Iterable[Mapping[str, Any]]
) -> BitcoinIntegrityReport:
    dataset: BitcoinDerivedDataset = 'bitcoin_circulating_supply'
    normalized_rows = _normalize_rows(rows, label=dataset)
    if len(normalized_rows) == 0:
        raise ValueError(
            'Bitcoin integrity schema/type check failed for bitcoin_circulating_supply: '
            'no rows found'
        )

    previous_height: int | None = None
    previous_supply_sats: int | None = None
    anomaly_checks_performed = 0
    min_height: int | None = None
    max_height: int | None = None

    for row_index, row in enumerate(normalized_rows, start=1):
        height = _expect_int(
            value=row.get('block_height'),
            label=f'{dataset}.row[{row_index}].block_height',
            minimum=0,
        )
        _expect_hash_hex_64(
            value=row.get('block_hash'),
            label=f'{dataset}.row[{row_index}].block_hash',
        )
        _expect_int(
            value=row.get('block_timestamp_ms'),
            label=f'{dataset}.row[{row_index}].block_timestamp_ms',
            minimum=1,
        )
        supply_sats = _expect_int(
            value=row.get('circulating_supply_sats'),
            label=f'{dataset}.row[{row_index}].circulating_supply_sats',
            minimum=0,
        )
        supply_btc = _expect_number(
            value=row.get('circulating_supply_btc'),
            label=f'{dataset}.row[{row_index}].circulating_supply_btc',
            minimum=0.0,
        )
        anomaly_checks_performed += 5

        expected_supply_sats = _circulating_supply_sats_at_height(height)
        if supply_sats != expected_supply_sats:
            raise ValueError(
                f'Bitcoin integrity formula check failed for {dataset}: '
                f'row={row_index}, expected_supply_sats={expected_supply_sats}, '
                f'got={supply_sats}'
            )
        expected_supply_btc = float(expected_supply_sats) / float(_SATS_PER_BTC)
        if not math.isclose(supply_btc, expected_supply_btc, rel_tol=1e-12, abs_tol=0.0):
            raise ValueError(
                f'Bitcoin integrity formula check failed for {dataset}: '
                f'row={row_index}, expected_supply_btc={expected_supply_btc}, '
                f'got={supply_btc}'
            )
        anomaly_checks_performed += 2

        if previous_height is not None:
            if height != previous_height + 1:
                raise ValueError(
                    f'Bitcoin integrity linkage check failed for {dataset}: '
                    f'row={row_index}, expected_height={previous_height + 1}, got={height}'
                )
            if previous_supply_sats is not None and supply_sats < previous_supply_sats:
                raise ValueError(
                    f'Bitcoin integrity anomaly check failed for {dataset}: '
                    f'row={row_index}, supply must be monotonic '
                    f'(previous={previous_supply_sats}, current={supply_sats})'
                )
            anomaly_checks_performed += 2

        previous_height = height
        previous_supply_sats = supply_sats
        min_height = height if min_height is None else min(min_height, height)
        max_height = height if max_height is None else max(max_height, height)

    return BitcoinIntegrityReport(
        dataset=dataset,
        rows_checked=len(normalized_rows),
        anomaly_checks_performed=anomaly_checks_performed,
        min_height=min_height,
        max_height=max_height,
    )
