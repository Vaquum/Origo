import zipfile
from io import BytesIO

import requests


def check_if_has_header(url: str, encoding: str = 'utf-8') -> bool:
    response = requests.get(url, timeout=120)
    response.raise_for_status()

    with zipfile.ZipFile(BytesIO(response.content)) as zf:
        name = zf.namelist()[0]
        with zf.open(name) as member:
            line = member.readline(256_000)
            line = line.decode(encoding, 'replace').rstrip()

    try:
        int(line.split(',')[0])
        return False
    except ValueError:
        return True
