from __future__ import annotations

import hashlib
import json
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from .contracts import NormalizedMetricRecord, ScrapeRunContext
from .etf_adapters import build_s4_02_issuer_adapters


def _records_fingerprint(records: list[NormalizedMetricRecord]) -> str:
    canonical_rows: list[dict[str, Any]] = []
    for record in records:
        canonical_rows.append(
            {
                'source_id': record.source_id,
                'metric_name': record.metric_name,
                'metric_value': record.metric_value,
                'metric_unit': record.metric_unit,
                'observed_at_utc': record.observed_at_utc.isoformat(),
                'dimensions': record.dimensions,
            }
        )

    canonical_rows.sort(
        key=lambda item: (
            str(item['source_id']),
            str(item['metric_name']),
            str(item['observed_at_utc']),
            json.dumps(item['dimensions'], sort_keys=True, separators=(',', ':')),
            json.dumps(item['metric_value'], sort_keys=True, separators=(',', ':')),
        )
    )

    serialized = json.dumps(
        canonical_rows,
        sort_keys=True,
        separators=(',', ':'),
        ensure_ascii=True,
    )
    return hashlib.sha256(serialized.encode('utf-8')).hexdigest()


def run_s4_02_capability_proof() -> dict[str, Any]:
    started_at_utc = datetime.now(UTC)
    run_context = ScrapeRunContext(
        run_id=f's4-02-proof-{started_at_utc.strftime("%Y%m%dT%H%M%SZ")}',
        started_at_utc=started_at_utc,
    )

    adapter_results: list[dict[str, Any]] = []
    for adapter in build_s4_02_issuer_adapters():
        sources = list(adapter.discover_sources(run_context=run_context))
        if len(sources) != 1:
            raise RuntimeError(
                f'{adapter.adapter_name} expected exactly one source, got {len(sources)}'
            )

        source = sources[0]
        artifact = adapter.fetch(source=source, run_context=run_context)
        parsed_records = list(
            adapter.parse(
                artifact=artifact,
                source=source,
                run_context=run_context,
            )
        )
        normalized_records = list(
            adapter.normalize(
                parsed_records=parsed_records,
                source=source,
                run_context=run_context,
            )
        )

        as_of_values = sorted(
            {
                str(record.metric_value)
                for record in normalized_records
                if record.metric_name == 'as_of_date'
            }
        )
        observed_days = sorted(
            {
                record.observed_at_utc.isoformat()
                for record in normalized_records
            }
        )

        adapter_results.append(
            {
                'adapter_name': adapter.adapter_name,
                'source_id': source.source_id,
                'source_uri': source.source_uri,
                'artifact_id': artifact.artifact_id,
                'artifact_format': artifact.artifact_format,
                'artifact_sha256': artifact.content_sha256,
                'parsed_record_count': len(parsed_records),
                'normalized_record_count': len(normalized_records),
                'normalized_fingerprint_sha256': _records_fingerprint(
                    normalized_records
                ),
                'as_of_dates': as_of_values,
                'observed_at_utc_values': observed_days,
            }
        )

    return {
        'generated_at_utc': datetime.now(UTC).isoformat(),
        'proof_scope': 'Slice 4 S4-02 issuer adapters 1-7 capability proof',
        'run_id': run_context.run_id,
        'adapter_results': adapter_results,
    }


def write_s4_02_acceptance_proof(*, output_path: Path) -> dict[str, Any]:
    proof = run_s4_02_capability_proof()
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(
        json.dumps(proof, indent=2, sort_keys=True) + '\n',
        encoding='utf-8',
    )
    return proof


if __name__ == '__main__':
    default_path = Path('spec/slices/slice-4-etf-use-case/acceptance-proof-s4-02.json')
    payload = write_s4_02_acceptance_proof(output_path=default_path)
    print(json.dumps(payload, indent=2, sort_keys=True))
