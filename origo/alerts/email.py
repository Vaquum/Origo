"""Bounded, exact-byte Resend transport for the monitor's private delivery cursor."""

from __future__ import annotations

import http.client
import ipaddress
import json
import logging
import re
import time
import urllib.error
import urllib.parse
import urllib.request
from collections.abc import Mapping
from dataclasses import dataclass
from email.utils import parsedate_to_datetime
from typing import Literal, cast

DEFAULT_RESEND_API_URL = 'https://api.resend.com/emails'
REQUIRED_VARIABLES = (
    'ORIGO_ALERT_RESEND_API_KEY',
    'ORIGO_ALERT_EMAIL_FROM',
    'ORIGO_ALERT_EMAIL_TO',
)
REQUEST_TIMEOUT_SECONDS = 10
USER_AGENT = 'origo-monitor'
RESPONSE_BYTES = 64 * 1024
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


def validate_dashboard_url(raw: str) -> str | None:
    """Validate only syntax and address class; never resolve or request the URL."""
    value = raw.strip()
    if not value:
        return None
    if any(character.isspace() or ord(character) < 32 for character in value):
        raise ValueError('Dashboard URL contains whitespace or control characters.')
    try:
        parsed = urllib.parse.urlsplit(value)
        host = parsed.hostname
        port = parsed.port
    except ValueError as error:
        raise ValueError('Dashboard URL has an invalid host or port.') from error
    if parsed.scheme not in {'http', 'https'} or not host:
        raise ValueError('Dashboard URL requires HTTP or HTTPS and an operator-facing host.')
    if parsed.username is not None or parsed.password is not None:
        raise ValueError('Dashboard URL must not contain credentials.')
    if '?' in value or '#' in value or parsed.path not in {'/law', '/law/'}:
        raise ValueError('Dashboard URL must use /law without query parameters or a fragment.')
    host = host.rstrip('.').lower()
    if host == 'localhost' or host.endswith('.localhost') or '%' in host or '\\' in host:
        raise ValueError('Dashboard URL must not use a local-only host.')
    try:
        address = ipaddress.ip_address(host)
    except ValueError:
        if re.fullmatch(r'[0-9.]+', host) or '.' not in host:
            raise ValueError('Dashboard URL requires a valid IP address or dotted DNS name.')
        try:
            ascii_host = host.encode('idna').decode('ascii')
        except UnicodeError as error:
            raise ValueError('Dashboard URL has an invalid DNS name.') from error
        if len(ascii_host) > 253 or any(
            not re.fullmatch(r'[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?', label)
            for label in ascii_host.split('.')
        ):
            raise ValueError('Dashboard URL has an invalid DNS name.')
        host = ascii_host
    else:
        if (
            address.is_unspecified
            or address.is_loopback
            or address.is_link_local
            or address.is_multicast
        ):
            raise ValueError('Dashboard URL uses an address unavailable to operators.')
        if isinstance(address, ipaddress.IPv6Address):
            mapped = address.ipv4_mapped
            if mapped and (
                mapped.is_unspecified
                or mapped.is_loopback
                or mapped.is_link_local
                or mapped.is_multicast
            ):
                raise ValueError('Dashboard URL uses an address unavailable to operators.')
            host = f'[{address}]'
        else:
            host = str(address)
    authority = host if port is None else f'{host}:{port}'
    return urllib.parse.urlunsplit((parsed.scheme, authority, '/law', '', ''))


@dataclass(frozen=True)
class AlertSettings:
    resend_api_key: str
    resend_api_url: str
    email_from: str
    email_to: tuple[str, ...]
    cooldown_seconds: int
    queue_threshold: int
    digest_hour_utc: int
    public_dashboard_url: str | None = None
    dashboard_url_fault: str | None = None

    @classmethod
    def from_environment(cls, environ: Mapping[str, str]) -> AlertSettings | None:
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
        dashboard_url = None
        dashboard_url_fault = None
        try:
            dashboard_url = validate_dashboard_url(environ.get('ORIGO_ALERT_DASHBOARD_URL', ''))
        except ValueError as error:
            dashboard_url_fault = f'ORIGO_ALERT_DASHBOARD_URL: {error}'
            log.warning(
                '%s Links omitted; monitoring and email remain enabled.', dashboard_url_fault
            )
        return cls(
            resend_api_key=environ['ORIGO_ALERT_RESEND_API_KEY'].strip(),
            resend_api_url=environ.get('ORIGO_ALERT_RESEND_API_URL', '').strip()
            or DEFAULT_RESEND_API_URL,
            email_from=environ['ORIGO_ALERT_EMAIL_FROM'].strip(),
            email_to=recipients,
            cooldown_seconds=_integer(
                environ, 'ORIGO_ALERT_COOLDOWN_SECONDS', 21600, maximum=7 * 86400
            ),
            queue_threshold=_integer(
                environ, 'ORIGO_ALERT_QUEUE_THRESHOLD', 200, maximum=1_000_000
            ),
            digest_hour_utc=_integer(environ, 'ORIGO_ALERT_DIGEST_HOUR_UTC', 7, maximum=23),
            public_dashboard_url=dashboard_url,
            dashboard_url_fault=dashboard_url_fault,
        )


class DeliveryError(RuntimeError):
    def __init__(
        self,
        message: str,
        *,
        disposition: Literal['uncertain', 'retryable', 'rejected', 'invariant'],
        status: int | None = None,
        retry_after: float | None = None,
    ) -> None:
        super().__init__(message)
        self.disposition = disposition
        self.status = status
        self.retry_after = retry_after


def _retry_after(value: str | None) -> float | None:
    if not value:
        return None
    try:
        seconds = float(value)
    except ValueError:
        try:
            timestamp = parsedate_to_datetime(value).timestamp()
        except (TypeError, ValueError, OverflowError) as error:
            log.warning('Resend supplied an invalid Retry-After header: %s', type(error).__name__)
            return None
        return max(0.0, timestamp - time.time())
    return max(0.0, seconds) if seconds < float('inf') else None


def _response_object(raw: bytes) -> dict[str, object]:
    try:
        value: object = json.loads(raw)
    except (ValueError, UnicodeDecodeError) as error:
        raise DeliveryError(
            'Resend response is not valid JSON; acceptance is uncertain.', disposition='uncertain'
        ) from error
    if not isinstance(value, dict):
        raise DeliveryError(
            'Resend response is not an object; acceptance is uncertain.', disposition='uncertain'
        )
    return cast(dict[str, object], value)


def _http_error(status: int, raw: bytes, retry_after: float | None) -> DeliveryError:
    code = ''
    try:
        value = _response_object(raw)
    except DeliveryError:
        log.warning('Resend HTTP %s response body is unavailable or malformed.', status)
    else:
        code = str(value.get('name', value.get('code', '')))
    if status == 429 or (status == 409 and code == 'concurrent_idempotent_requests'):
        return DeliveryError(
            f'Resend HTTP {status}; retrying the reserved batch.',
            disposition='retryable',
            status=status,
            retry_after=retry_after,
        )
    if status == 409 and code == 'invalid_idempotent_request':
        return DeliveryError(
            'Resend rejected an invalid idempotent request; immutable delivery invariant failed.',
            disposition='invariant',
            status=status,
        )
    if 400 <= status < 500:
        return DeliveryError(
            f'Resend HTTP {status}; delivery rejected.', disposition='rejected', status=status
        )
    return DeliveryError(
        f'Resend HTTP {status}; acceptance is uncertain.',
        disposition='uncertain',
        status=status,
        retry_after=retry_after,
    )


def send_alert(
    payload: bytes,
    *,
    api_key: str,
    api_url: str,
    idempotency_key: str,
) -> str:
    """Post the stored bytes once and return only an acknowledged provider ID."""
    if not 1 <= len(idempotency_key) <= 256 or '\r' in idempotency_key or '\n' in idempotency_key:
        raise ValueError('Resend Idempotency-Key must contain 1-256 safe characters.')
    request = urllib.request.Request(
        api_url,
        data=payload,
        method='POST',
        headers={
            'Authorization': f'Bearer {api_key}',
            'Content-Type': 'application/json',
            'User-Agent': USER_AGENT,
            'Idempotency-Key': idempotency_key,
        },
    )
    try:
        with urllib.request.urlopen(request, timeout=REQUEST_TIMEOUT_SECONDS) as response:
            status = int(response.status)
            raw = response.read(RESPONSE_BYTES + 1)
            retry_after = _retry_after(response.headers.get('Retry-After'))
    except urllib.error.HTTPError as error:
        raw = error.read(RESPONSE_BYTES)
        raise _http_error(
            error.code, raw, _retry_after(error.headers.get('Retry-After'))
        ) from error
    except (urllib.error.URLError, http.client.HTTPException, TimeoutError, OSError) as error:
        raise DeliveryError(
            f'Resend transport failed ({type(error).__name__}); acceptance is uncertain.',
            disposition='uncertain',
        ) from error
    if not 200 <= status < 300:
        raise _http_error(status, raw, retry_after)
    if len(raw) > RESPONSE_BYTES:
        raise DeliveryError(
            'Resend response exceeds its byte limit; acceptance is uncertain.',
            disposition='uncertain',
        )
    document = _response_object(raw)
    identifier = document.get('id')
    if not isinstance(identifier, str) or not identifier.strip():
        raise DeliveryError(
            'Resend response has no acknowledgement ID; acceptance is uncertain.',
            disposition='uncertain',
        )
    return identifier.strip()
