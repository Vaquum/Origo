from __future__ import annotations

from copy import deepcopy
from datetime import UTC, datetime

import pytest
from origo_control_plane.assets.bitcoin_mempool_state_to_origo import (
    normalize_mempool_state_or_raise,
)


def _sample_mempool_payload() -> dict[str, object]:
    return {
        'f' * 64: {
            'vsize': 900,
            'time': 1713600100,
            'bip125-replaceable': False,
            'fees': {'base': 0.000225},
        },
        '0' * 63 + '1': {
            'vsize': 250,
            'time': 1713600001,
            'bip125-replaceable': True,
            'fees': {'base': 0.00001},
        },
    }


def test_normalize_mempool_state_is_deterministic() -> None:
    snapshot_at_utc = datetime(2026, 3, 8, 0, 0, 0, tzinfo=UTC)
    payload = _sample_mempool_payload()

    run_1 = normalize_mempool_state_or_raise(
        mempool_payload=deepcopy(payload),
        snapshot_at_utc=snapshot_at_utc,
        source_chain='main',
    )
    run_2 = normalize_mempool_state_or_raise(
        mempool_payload=deepcopy(payload),
        snapshot_at_utc=snapshot_at_utc,
        source_chain='main',
    )

    assert [row.as_canonical_map() for row in run_1] == [
        row.as_canonical_map() for row in run_2
    ]


def test_normalize_mempool_state_fee_rate_and_order() -> None:
    snapshot_at_utc = datetime(2026, 3, 8, 0, 0, 0, tzinfo=UTC)

    rows = normalize_mempool_state_or_raise(
        mempool_payload=_sample_mempool_payload(),
        snapshot_at_utc=snapshot_at_utc,
        source_chain='main',
    )

    assert [row.txid for row in rows] == ['0' * 63 + '1', 'f' * 64]
    assert rows[0].fee_rate_sat_vb == pytest.approx(4.0)
    assert rows[1].fee_rate_sat_vb == pytest.approx(25.0)
    assert rows[0].rbf_flag is True
    assert rows[1].rbf_flag is False


def test_normalize_mempool_state_rejects_invalid_txid() -> None:
    payload = _sample_mempool_payload()
    payload['not-a-hash'] = payload.pop('f' * 64)
    snapshot_at_utc = datetime(2026, 3, 8, 0, 0, 0, tzinfo=UTC)

    with pytest.raises(RuntimeError, match='mempool txid must be a 64-char lowercase hexadecimal hash'):
        normalize_mempool_state_or_raise(
            mempool_payload=payload,
            snapshot_at_utc=snapshot_at_utc,
            source_chain='main',
        )
