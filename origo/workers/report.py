"""Reporting worker facts into Dagster through the webserver's external-asset endpoints.

Every method returns ``True`` when Dagit accepted the event and ``False`` when it did not
answer or refused, within a bounded timeout: a webserver outage never blocks a tick, and
the caller records the outcome instead of pretending the write happened.
"""

from __future__ import annotations

import json
import logging
import urllib.error
import urllib.request
from collections.abc import Mapping
from datetime import datetime

LAST_UPDATED_TIMESTAMP_METADATA_KEY = 'dagster/last_updated_timestamp'
log = logging.getLogger('origo.workers.report')


class Reporter:
    def __init__(self, base_url: str, *, timeout_seconds: float = 5.0) -> None:
        self.base_url = base_url.rstrip('/')
        self.timeout_seconds = timeout_seconds

    def materialized(
        self, asset_key: str, *, partition: str | None, metadata: Mapping[str, object]
    ) -> bool:
        payload: dict[str, object] = {'metadata': dict(metadata)}
        if partition is not None:
            payload['partition'] = partition
        return self._post(f'/report_asset_materialization/{asset_key}', payload)

    def observed(
        self, asset_key: str, *, last_updated: datetime, metadata: Mapping[str, object]
    ) -> bool:
        return self._post(
            f'/report_asset_observation/{asset_key}',
            {
                'metadata': {
                    **dict(metadata),
                    LAST_UPDATED_TIMESTAMP_METADATA_KEY: last_updated.timestamp(),
                }
            },
        )

    def check(
        self,
        asset_key: str,
        check_name: str,
        *,
        passed: bool,
        metadata: Mapping[str, object],
        severity: str = 'ERROR',
    ) -> bool:
        return self._post(
            f'/report_asset_check/{asset_key}',
            {
                'check_name': check_name,
                'passed': passed,
                'severity': severity,
                'metadata': dict(metadata),
            },
        )

    def _post(self, path: str, payload: Mapping[str, object]) -> bool:
        request = urllib.request.Request(
            self.base_url + path,
            data=json.dumps(payload).encode(),
            method='POST',
            headers={'Content-Type': 'application/json'},
        )
        try:
            with urllib.request.urlopen(request, timeout=self.timeout_seconds) as response:
                status = int(response.status)
        except urllib.error.HTTPError as error:
            detail = error.read()[:300].decode('utf-8', 'replace')
            log.error('dagster refused %s: HTTP %s %s', path, error.code, detail)
            return False
        except (urllib.error.URLError, TimeoutError, OSError) as error:
            log.error('dagster unreachable for %s: %s', path, error)
            return False
        if not 200 <= status < 300:
            log.error('dagster answered %s with HTTP %s', path, status)
            return False
        return True
