from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from typing import Final, Literal

S34BackfillDataset = Literal[
    'binance_spot_trades',
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
]

S34BackfillPhase = Literal[
    'exchange_primary',
    'exchange_parallel',
    'etf',
    'fred',
    'bitcoin_base',
    'bitcoin_derived',
]

S34PartitionScheme = Literal['daily', 'height_range']
S34OffsetOrdering = Literal['numeric', 'lexicographic', 'opaque']


@dataclass(frozen=True)
class S34DatasetBackfillContract:
    dataset: S34BackfillDataset
    source_id: str
    stream_id: str
    execution_rank: int
    phase: S34BackfillPhase
    partition_scheme: S34PartitionScheme
    earliest_partition_date: date | None
    earliest_height: int | None
    offset_ordering: S34OffsetOrdering
    aligned_capable: bool


_S34_EXPECTED_DATASET_ORDER: Final[tuple[S34BackfillDataset, ...]] = (
    'binance_spot_trades',
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
)

_S34_CONTRACTS: Final[tuple[S34DatasetBackfillContract, ...]] = (
    S34DatasetBackfillContract(
        dataset='binance_spot_trades',
        source_id='binance',
        stream_id='binance_spot_trades',
        execution_rank=1,
        phase='exchange_primary',
        partition_scheme='daily',
        earliest_partition_date=date(2017, 8, 17),
        earliest_height=None,
        offset_ordering='numeric',
        aligned_capable=True,
    ),
    S34DatasetBackfillContract(
        dataset='okx_spot_trades',
        source_id='okx',
        stream_id='okx_spot_trades',
        execution_rank=2,
        phase='exchange_parallel',
        partition_scheme='daily',
        earliest_partition_date=date(2021, 9, 1),
        earliest_height=None,
        offset_ordering='numeric',
        aligned_capable=True,
    ),
    S34DatasetBackfillContract(
        dataset='bybit_spot_trades',
        source_id='bybit',
        stream_id='bybit_spot_trades',
        execution_rank=3,
        phase='exchange_parallel',
        partition_scheme='daily',
        earliest_partition_date=date(2020, 3, 25),
        earliest_height=None,
        offset_ordering='lexicographic',
        aligned_capable=True,
    ),
    S34DatasetBackfillContract(
        dataset='etf_daily_metrics',
        source_id='etf',
        stream_id='etf_daily_metrics',
        execution_rank=4,
        phase='etf',
        partition_scheme='daily',
        earliest_partition_date=date(2024, 1, 11),
        earliest_height=None,
        offset_ordering='lexicographic',
        aligned_capable=True,
    ),
    S34DatasetBackfillContract(
        dataset='fred_series_metrics',
        source_id='fred',
        stream_id='fred_series_metrics',
        execution_rank=5,
        phase='fred',
        partition_scheme='daily',
        earliest_partition_date=None,
        earliest_height=None,
        offset_ordering='lexicographic',
        aligned_capable=True,
    ),
    S34DatasetBackfillContract(
        dataset='bitcoin_block_headers',
        source_id='bitcoin_core',
        stream_id='bitcoin_block_headers',
        execution_rank=6,
        phase='bitcoin_base',
        partition_scheme='height_range',
        earliest_partition_date=None,
        earliest_height=0,
        offset_ordering='numeric',
        aligned_capable=True,
    ),
    S34DatasetBackfillContract(
        dataset='bitcoin_block_transactions',
        source_id='bitcoin_core',
        stream_id='bitcoin_block_transactions',
        execution_rank=7,
        phase='bitcoin_base',
        partition_scheme='height_range',
        earliest_partition_date=None,
        earliest_height=0,
        offset_ordering='numeric',
        aligned_capable=True,
    ),
    S34DatasetBackfillContract(
        dataset='bitcoin_mempool_state',
        source_id='bitcoin_core',
        stream_id='bitcoin_mempool_state',
        execution_rank=8,
        phase='bitcoin_base',
        partition_scheme='height_range',
        earliest_partition_date=None,
        earliest_height=0,
        offset_ordering='numeric',
        aligned_capable=True,
    ),
    S34DatasetBackfillContract(
        dataset='bitcoin_block_fee_totals',
        source_id='bitcoin_core',
        stream_id='bitcoin_block_fee_totals',
        execution_rank=9,
        phase='bitcoin_derived',
        partition_scheme='height_range',
        earliest_partition_date=None,
        earliest_height=0,
        offset_ordering='numeric',
        aligned_capable=True,
    ),
    S34DatasetBackfillContract(
        dataset='bitcoin_block_subsidy_schedule',
        source_id='bitcoin_core',
        stream_id='bitcoin_block_subsidy_schedule',
        execution_rank=10,
        phase='bitcoin_derived',
        partition_scheme='height_range',
        earliest_partition_date=None,
        earliest_height=0,
        offset_ordering='numeric',
        aligned_capable=True,
    ),
    S34DatasetBackfillContract(
        dataset='bitcoin_network_hashrate_estimate',
        source_id='bitcoin_core',
        stream_id='bitcoin_network_hashrate_estimate',
        execution_rank=11,
        phase='bitcoin_derived',
        partition_scheme='height_range',
        earliest_partition_date=None,
        earliest_height=0,
        offset_ordering='numeric',
        aligned_capable=True,
    ),
    S34DatasetBackfillContract(
        dataset='bitcoin_circulating_supply',
        source_id='bitcoin_core',
        stream_id='bitcoin_circulating_supply',
        execution_rank=12,
        phase='bitcoin_derived',
        partition_scheme='height_range',
        earliest_partition_date=None,
        earliest_height=0,
        offset_ordering='numeric',
        aligned_capable=True,
    ),
)


def list_s34_dataset_contracts() -> tuple[S34DatasetBackfillContract, ...]:
    return _S34_CONTRACTS


def get_s34_dataset_contract(dataset: S34BackfillDataset) -> S34DatasetBackfillContract:
    for contract in _S34_CONTRACTS:
        if contract.dataset == dataset:
            return contract
    raise RuntimeError(f'Unknown S34 dataset contract: {dataset}')


def assert_s34_backfill_contract_consistency_or_raise() -> None:
    if len(_S34_CONTRACTS) != len(_S34_EXPECTED_DATASET_ORDER):
        raise RuntimeError(
            'S34 backfill contract count mismatch: '
            f'expected={len(_S34_EXPECTED_DATASET_ORDER)} '
            f'actual={len(_S34_CONTRACTS)}'
        )

    observed_dataset_order = tuple(contract.dataset for contract in _S34_CONTRACTS)
    if observed_dataset_order != _S34_EXPECTED_DATASET_ORDER:
        raise RuntimeError(
            'S34 backfill dataset order mismatch: '
            f'expected={_S34_EXPECTED_DATASET_ORDER} '
            f'actual={observed_dataset_order}'
        )

    observed_ranks = tuple(contract.execution_rank for contract in _S34_CONTRACTS)
    expected_ranks = tuple(range(1, len(_S34_CONTRACTS) + 1))
    if observed_ranks != expected_ranks:
        raise RuntimeError(
            'S34 execution rank sequence mismatch: '
            f'expected={expected_ranks} actual={observed_ranks}'
        )

    dataset_set = {contract.dataset for contract in _S34_CONTRACTS}
    if len(dataset_set) != len(_S34_CONTRACTS):
        raise RuntimeError('S34 backfill contracts contain duplicate dataset entries')

    for contract in _S34_CONTRACTS:
        if contract.source_id.strip() == '':
            raise RuntimeError(
                f'S34 contract source_id must be non-empty for dataset={contract.dataset}'
            )
        if contract.stream_id.strip() == '':
            raise RuntimeError(
                f'S34 contract stream_id must be non-empty for dataset={contract.dataset}'
            )
        if contract.partition_scheme == 'daily':
            if contract.earliest_partition_date is None:
                if contract.dataset != 'fred_series_metrics':
                    raise RuntimeError(
                        'S34 daily dataset contract missing earliest_partition_date for '
                        f'dataset={contract.dataset}'
                    )
            if contract.earliest_height is not None:
                raise RuntimeError(
                    'S34 daily dataset must not define earliest_height for '
                    f'dataset={contract.dataset}'
                )
        if contract.partition_scheme == 'height_range':
            if contract.earliest_height is None:
                raise RuntimeError(
                    'S34 height_range contract missing earliest_height for '
                    f'dataset={contract.dataset}'
                )
            if contract.earliest_height < 0:
                raise RuntimeError(
                    'S34 height_range contract earliest_height must be >= 0 for '
                    f'dataset={contract.dataset}'
                )
