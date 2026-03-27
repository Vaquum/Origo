from __future__ import annotations

import re

_HEIGHT_RANGE_PATTERN = re.compile(r'^(?P<start>\d{12})-(?P<end>\d{12})$')
_HEIGHT_RANGE_WIDTH = 12


def format_bitcoin_height_range_partition_id_or_raise(
    *,
    start_height: int,
    end_height: int,
) -> str:
    if start_height is True or start_height is False:
        raise RuntimeError('start_height must be int')
    if end_height is True or end_height is False:
        raise RuntimeError('end_height must be int')
    if start_height < 0:
        raise RuntimeError(f'start_height must be >= 0, got {start_height}')
    if end_height < start_height:
        raise RuntimeError(
            'end_height must be >= start_height, '
            f'got start_height={start_height} end_height={end_height}'
        )
    return (
        f'{start_height:0{_HEIGHT_RANGE_WIDTH}d}-'
        f'{end_height:0{_HEIGHT_RANGE_WIDTH}d}'
    )


def parse_bitcoin_height_range_partition_id_or_raise(value: str) -> tuple[int, int]:
    normalized = value.strip()
    if normalized == '':
        raise RuntimeError('height_range partition_id must be non-empty')
    match = _HEIGHT_RANGE_PATTERN.fullmatch(normalized)
    if match is None:
        raise RuntimeError(
            'height_range partition_id must match '
            f'{_HEIGHT_RANGE_WIDTH}-digit start/end format, got {value!r}'
        )
    start_height = int(match.group('start'))
    end_height = int(match.group('end'))
    if end_height < start_height:
        raise RuntimeError(
            'height_range partition_id end must be >= start, '
            f'got partition_id={value!r}'
        )
    return (start_height, end_height)
