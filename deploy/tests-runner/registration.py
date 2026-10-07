from __future__ import annotations

import argparse
import base64
import json
import os
import subprocess
import sys
import time
from datetime import datetime
from pathlib import Path
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen

REPOSITORY = 'Vaquum/Origo'


def encode(value: bytes) -> str:
    return base64.urlsafe_b64encode(value).decode().rstrip('=')


def request(endpoint: str, token: str, body: dict[str, object] | None = None) -> dict[str, object]:
    message = Request(
        f'https://api.github.com/{endpoint}',
        data=json.dumps(body).encode() if body is not None else None,
        headers={'Authorization': f'Bearer {token}', 'Accept': 'application/vnd.github+json'},
        method='POST' if body is not None else 'GET',
    )
    attempt = 0
    while True:
        try:
            with urlopen(message, timeout=30) as response:
                return json.load(response)
        except (URLError, TimeoutError) as error:
            if isinstance(error, HTTPError) and not 500 <= error.code < 600:
                raise
            attempt += 1
            print(f'GitHub request failed ({attempt}/3): {error}', file=sys.stderr)
            if attempt == 3:
                raise
            time.sleep(2 ** attempt)


def save_cache(cache: Path, access: dict[str, object]) -> None:
    pending = cache.with_name(cache.name + '.new')
    fd = os.open(pending, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
    os.fchmod(fd, 0o600)
    with os.fdopen(fd, 'w') as handle:
        json.dump(access, handle)
    os.replace(pending, cache)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument('--status', action='store_true')
    args = parser.parse_args()
    now = int(time.time())
    header = encode(json.dumps({'alg': 'RS256', 'typ': 'JWT'}).encode())
    claims = encode(json.dumps({'iat': now - 60, 'exp': now + 540, 'iss': os.environ['APP_ID']}).encode())
    payload = f'{header}.{claims}'
    signature = subprocess.run(
        ['openssl', 'dgst', '-sha256', '-sign', '/etc/origo-tests-runner/app.pem'],
        input=payload.encode(), check=True, capture_output=True,
    ).stdout
    jwt = f'{payload}.{encode(signature)}'
    cache = Path('/run/origo-tests-runner-token.json')
    access = json.loads(cache.read_text()) if cache.exists() else {'expires_at': '1970-01-01T00:00:00Z'}
    if datetime.fromisoformat(access['expires_at'].replace('Z', '+00:00')).timestamp() < now + 60:
        access = request(f"app/installations/{os.environ['INSTALLATION_ID']}/access_tokens", jwt, {
            'repositories': ['Origo'], 'permissions': {'administration': 'write'},
        })
        save_cache(cache, access)
    token = access['token']
    if not isinstance(token, str):
        raise TypeError('GitHub did not return an installation token')
    if args.status:
        runners = request(f'repos/{REPOSITORY}/actions/runners', token)['runners']
        matches = [runner for runner in runners if runner['name'] == 'origo-tests']
        if not matches:
            print('absent')
        elif len(matches) != 1:
            raise ValueError('More than one Origo tests runner is registered')
        else:
            print('busy' if matches[0]['busy'] else matches[0]['status'])
        return
    registration = request(f'repos/{REPOSITORY}/actions/runners/registration-token', token, {})
    print(registration['token'])


if __name__ == '__main__':
    main()
