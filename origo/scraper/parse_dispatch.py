from __future__ import annotations

from .contracts import ParsedRecord, RawArtifact
from .parse_csv import parse_csv_artifact
from .parse_html import parse_html_artifact
from .parse_json import parse_json_artifact
from .parse_pdf import parse_pdf_artifact


def parse_artifact(*, artifact: RawArtifact) -> list[ParsedRecord]:
    if artifact.artifact_format == 'html':
        return parse_html_artifact(artifact=artifact)
    if artifact.artifact_format == 'json':
        return parse_json_artifact(artifact=artifact)
    if artifact.artifact_format == 'csv':
        return parse_csv_artifact(artifact=artifact)
    if artifact.artifact_format == 'pdf':
        return parse_pdf_artifact(artifact=artifact)
    raise RuntimeError(
        f'No parser implementation for artifact_format={artifact.artifact_format}'
    )
