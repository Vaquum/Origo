"""Alert e-mail through the Resend HTTP API.

The monitor worker is the only sender. Delivery is one POST with a bounded timeout;
any failure raises so a mail outage fails the monitor tick visibly and is retried on
the next minute. The API key lives only in the deployment environment.
"""

from __future__ import annotations

import json
import logging
import urllib.error
import urllib.request
from collections.abc import Mapping
from dataclasses import dataclass
from typing import cast

DEFAULT_RESEND_API_URL = 'https://api.resend.com/emails'
REQUIRED_VARIABLES = (
    'ORIGO_ALERT_RESEND_API_KEY',
    'ORIGO_ALERT_EMAIL_FROM',
    'ORIGO_ALERT_EMAIL_TO',
)
REQUEST_TIMEOUT_SECONDS = 10
log = logging.getLogger('origo.alerts')


def _integer(environ: Mapping[str, str], name: str, default: int, *, maximum: int) -> int:
    raw = environ.get(name, '').strip()
    if not raw:
        return default
    try:
        value = int(raw)
    except ValueError as error:
        raise RuntimeError(f'{name} must be an integer, got {raw!r}.') from error
    if not 0 <= value <= maximum:
        raise RuntimeError(f'{name} must be between 0 and {maximum}, got {value}.')
    return value


@dataclass(frozen=True)
class AlertSettings:
    resend_api_key: str
    resend_api_url: str
    email_from: str
    email_to: tuple[str, ...]
    cooldown_seconds: int
    queue_threshold: int
    digest_hour_utc: int

    @classmethod
    def from_environment(cls, environ: Mapping[str, str]) -> AlertSettings | None:
        """Settings from ``ORIGO_ALERT_*``, or ``None`` when no alert variable is set at all.

        A partial set is a configuration error: alerts never degrade silently.
        """
        present = [name for name in REQUIRED_VARIABLES if environ.get(name, '').strip()]
        if not present:
            return None
        missing = [name for name in REQUIRED_VARIABLES if name not in present]
        if missing:
            raise RuntimeError(f'Alert configuration is incomplete; missing {", ".join(missing)}.')
        recipients = tuple(
            part.strip() for part in environ['ORIGO_ALERT_EMAIL_TO'].split(',') if part.strip()
        )
        if not recipients:
            raise RuntimeError('ORIGO_ALERT_EMAIL_TO names no recipient.')
        return cls(
            resend_api_key=environ['ORIGO_ALERT_RESEND_API_KEY'].strip(),
            resend_api_url=environ.get('ORIGO_ALERT_RESEND_API_URL', '').strip()
            or DEFAULT_RESEND_API_URL,
            email_from=environ['ORIGO_ALERT_EMAIL_FROM'].strip(),
            email_to=recipients,
            cooldown_seconds=_integer(environ, 'ORIGO_ALERT_COOLDOWN_SECONDS', 21600, maximum=7 * 86400),
            queue_threshold=_integer(environ, 'ORIGO_ALERT_QUEUE_THRESHOLD', 200, maximum=1_000_000),
            digest_hour_utc=_integer(environ, 'ORIGO_ALERT_DIGEST_HOUR_UTC', 7, maximum=23),
        )


def send_alert(settings: AlertSettings, subject: str, body: str) -> None:
    """Deliver one plain-text message to every configured recipient; raise on any failure."""
    payload = json.dumps(
        {
            'from': settings.email_from,
            'to': list(settings.email_to),
            'subject': subject,
            'text': body,
        }
    ).encode()
    request = urllib.request.Request(
        settings.resend_api_url,
        data=payload,
        method='POST',
        headers={
            'Authorization': f'Bearer {settings.resend_api_key}',
            'Content-Type': 'application/json',
        },
    )
    try:
        with urllib.request.urlopen(request, timeout=REQUEST_TIMEOUT_SECONDS) as response:
            status = int(response.status)
            raw = response.read()
    except urllib.error.HTTPError as error:
        detail = error.read()[:200].decode('utf-8', 'replace')
        raise RuntimeError(f'Alert delivery failed: HTTP {error.code} {detail}') from error
    except (urllib.error.URLError, TimeoutError, OSError) as error:
        raise RuntimeError(f'Alert delivery failed: {error}') from error
    if not 200 <= status < 300:
        raise RuntimeError(f'Alert delivery failed: HTTP {status}')
    document: object = json.loads(raw or b'{}')
    identifier = ''
    if isinstance(document, dict):
        identifier = str(cast(dict[str, object], document).get('id', ''))
    log.info('alert delivered subject=%r id=%s', subject, identifier)
