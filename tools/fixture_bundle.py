"""Fetch, pack, and verify daily-archive fixture bundles (PRD-0013 row 7).

A bundle is three files beside each other in one leaf directory::

    <stem>.zip               the official Binance vision archive, unmodified
    <stem>.zip.CHECKSUM      the official checksum sidecar, unmodified
    <stem>.provenance.json   capture record: source URL, digests, row selection

Leaf directories are symbol-scoped (``.../daily/trades/BTCUSDT/``); the ``revisioned``
leaf under the spot fixtures is legacy. New sources use their symbol.

Schema notes (from the two sources that prove the shape):
- ``csv_header`` is always written; ``null`` means the member carries no header
  line. Bundles packed before the key existed (spot) read as ``null``.
- ``selected_sha256`` is the sha256 of the header line (when the member carries
  one) followed by data lines [start:stop] with their line endings. A full-file
  selection hashes the whole member.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import urllib.request
import zipfile
from datetime import UTC, datetime
from pathlib import Path

PACKAGING = 'unmodified CSV rows extracted from checksum-verified official ZIP'


def _sha256(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def _download(url: str) -> bytes:
    request = urllib.request.Request(url, headers={'User-Agent': 'origo-fixture-bundle'})
    with urllib.request.urlopen(request, timeout=120) as response:
        return response.read()


def _read_provenance(path: Path) -> dict[str, object]:
    try:
        data = json.loads(path.read_text(encoding='utf-8'))
    except (OSError, ValueError) as exc:
        raise RuntimeError(f'Cannot read provenance {path}: {exc}') from exc
    if not isinstance(data, dict):
        raise RuntimeError(f'Provenance {path} must be a JSON object.')
    return data


def cmd_fetch(args: argparse.Namespace) -> int:
    leaf: Path = args.dir
    url: str = args.url.rstrip('/')
    if not url.endswith('.zip'):
        raise ValueError('Fixture URL must point at the official .zip file.')
    stem = url.rsplit('/', 1)[1][: -len('.zip')]
    leaf.mkdir(parents=True, exist_ok=True)
    body = _download(url)
    sidecar = _download(url + '.CHECKSUM')
    (leaf / f'{stem}.zip').write_bytes(body)
    (leaf / f'{stem}.zip.CHECKSUM').write_bytes(sidecar)
    print(json.dumps({'stem': stem, 'zip_bytes': len(body), 'dir': str(leaf)}))
    return 0


def _member_lines(body: bytes) -> list[bytes]:
    return body.splitlines(keepends=True)


def cmd_pack(args: argparse.Namespace) -> int:
    leaf: Path = args.dir
    stem: str = args.stem
    blob = leaf / f'{stem}.zip'
    sidecar = leaf / f'{stem}.zip.CHECKSUM'
    if not blob.is_file():
        raise RuntimeError(f'Missing archive: {blob} (run fetch first).')
    if not sidecar.is_file():
        raise RuntimeError(f'Missing checksum sidecar: {sidecar} (run fetch first).')
    digest = _sha256(blob.read_bytes())
    sidecar_text = sidecar.read_text(encoding='utf-8').strip()
    if not sidecar_text.startswith(digest):
        raise RuntimeError(f'Official checksum mismatch for {blob}.')
    member_name: str | None = args.csv_member
    with zipfile.ZipFile(blob) as archive:
        names = archive.namelist()
        if member_name is None:
            if len(names) != 1:
                raise RuntimeError(f'{blob} holds {len(names)} members; pass --csv-member.')
            member_name = names[0]
        try:
            raw = archive.read(member_name)
        except KeyError as exc:
            raise RuntimeError(f'{blob} has no member {member_name}.') from exc
    lines = _member_lines(raw)
    header: str | None = args.header
    prologue = b''
    if header is not None:
        if not lines or lines[0].decode('utf-8').rstrip('\r\n') != header:
            raise RuntimeError(f'{member_name} first line is not the declared header.')
        prologue = lines[0]
        lines = lines[1:]
    start: int = args.rows_start
    stop: int = args.rows_stop if args.rows_stop is not None else len(lines)
    if not 0 <= start <= stop <= len(lines):
        raise ValueError(f'Row selection [{start}:{stop}] outside 0..{len(lines)}.')
    selected = prologue + b''.join(lines[start:stop])
    provenance = {
        'url': args.url,
        'date': args.date,
        'zip_sha256': digest,
        'checksum_sidecar': sidecar_text,
        'csv_member': member_name,
        'csv_header': header,
        'official_row_count': len(lines),
        'selected_row_start': start,
        'selected_row_stop': stop,
        'selected_sha256': _sha256(selected),
        'captured_at': datetime.now(UTC).isoformat(),
        'packaging': PACKAGING,
    }
    (leaf / f'{stem}.provenance.json').write_text(
        json.dumps(provenance, indent=2) + '\n', encoding='utf-8'
    )
    if args.extract_csv:
        (leaf / f'{stem}.csv').write_bytes(selected)
    print(json.dumps({'stem': stem, 'rows': len(lines), 'selected': stop - start}))
    return 0


def cmd_verify(args: argparse.Namespace) -> int:
    leaf: Path = args.dir
    stem: str = args.stem
    blob = leaf / f'{stem}.zip'
    sidecar = leaf / f'{stem}.zip.CHECKSUM'
    document = leaf / f'{stem}.provenance.json'
    failures: list[str] = []
    try:
        recorded = _read_provenance(document)
    except RuntimeError as exc:
        return _report(stem, [str(exc)])
    digest = _sha256(blob.read_bytes()) if blob.is_file() else None
    if digest is None:
        failures.append('missing archive')
    elif digest != recorded.get('zip_sha256'):
        failures.append('zip_sha256 mismatch')
    if not sidecar.is_file():
        failures.append('missing checksum sidecar')
    else:
        sidecar_text = sidecar.read_text(encoding='utf-8').strip()
        if sidecar_text != recorded.get('checksum_sidecar'):
            failures.append('checksum sidecar drifted')
        if digest is not None and not sidecar_text.startswith(digest):
            failures.append('official checksum mismatch')
    member = recorded.get('csv_member')
    if digest is not None and isinstance(member, str):
        with zipfile.ZipFile(blob) as archive:
            if member not in archive.namelist():
                failures.append('csv member missing')
            else:
                lines = _member_lines(archive.read(member))
                header = recorded.get('csv_header')
                prologue = b''
                if header is not None:
                    if not lines or lines[0].decode('utf-8').rstrip('\r\n') != header:
                        failures.append('csv header mismatch')
                    else:
                        prologue = lines[0]
                        lines = lines[1:]
                if recorded.get('official_row_count') != len(lines):
                    failures.append('official_row_count mismatch')
                start = recorded.get('selected_row_start')
                stop = recorded.get('selected_row_stop')
                if (
                    isinstance(start, int)
                    and isinstance(stop, int)
                    and 0 <= start <= stop <= len(lines)
                ):
                    selected = prologue + b''.join(lines[start:stop])
                    if _sha256(selected) != recorded.get('selected_sha256'):
                        failures.append('selected_sha256 mismatch')
                else:
                    failures.append('row selection outside member')
    elif not isinstance(member, str):
        failures.append('csv_member not recorded')
    return _report(stem, failures)


def _report(stem: str, failures: list[str]) -> int:
    print(json.dumps({'stem': stem, 'ok': not failures, 'failures': failures}))
    return 1 if failures else 0


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest='command', required=True)
    fetch = sub.add_parser('fetch', help='Download the official zip and checksum sidecar.')
    fetch.add_argument('--url', required=True, help='Official .../NAME.zip URL.')
    fetch.add_argument('--dir', type=Path, required=True, help='Symbol-scoped leaf directory.')
    fetch.set_defaults(func=cmd_fetch)
    pack = sub.add_parser('pack', help='Verify a fetched zip and write its provenance.')
    pack.add_argument('--dir', type=Path, required=True)
    pack.add_argument('--stem', required=True, help='Bundle stem, e.g. BTCUSDT-trades-2024-04-20.')
    pack.add_argument('--url', required=True, help='Official URL, recorded in provenance.')
    pack.add_argument('--date', required=True, help='Trading date, YYYY-MM-DD.')
    pack.add_argument('--csv-member', default=None)
    pack.add_argument('--header', default=None, help='Expected header line text, if any.')
    pack.add_argument('--rows-start', type=int, default=0)
    pack.add_argument('--rows-stop', type=int, default=None)
    pack.add_argument('--extract-csv', action='store_true')
    pack.set_defaults(func=cmd_pack)
    verify = sub.add_parser('verify', help='Recompute a bundle and compare with provenance.')
    verify.add_argument('--dir', type=Path, required=True)
    verify.add_argument('--stem', required=True)
    verify.set_defaults(func=cmd_verify)
    args = parser.parse_args(argv)
    return args.func(args)


if __name__ == '__main__':
    raise SystemExit(main())
